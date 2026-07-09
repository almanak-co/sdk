"""Gateway-side PoolAnalyticsService tests (VIB-4727).

CoinGecko Onchain is the sole external pool-analytics lane (the legacy
catalog-matching lane was structurally dead — its upstream catalog keys
pools by opaque UUIDs, never by address — and was deleted).
Tests patch the upstream provider seam (``_query_coingecko_onchain_pool``)
with recorded JSON fixtures from
``tests/gateway/services/fixtures/pool_analytics/`` — no live external API
is reached. Patching at this seam (the function returning the parsed JSON
dict) is the "equivalent in-test HTTP mocking" the UAT card permits
in lieu of ``aioresponses`` (which is not a project dependency).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import time
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import aiohttp
import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.pool_analytics_service import (
    PoolAnalyticsServiceServicer,
    _parse_coingecko_onchain_pool,
)

_FIXTURES = Path(__file__).parent / "fixtures" / "pool_analytics"
_ANTONIS_POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"  # USDC/WETH 0.05% Arbitrum
_ETH_USDC_WETH = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"  # USDC/WETH 0.05% Ethereum


def _load_fixture(name: str) -> Any:
    with (_FIXTURES / name).open("r") as f:
        return json.load(f)


class _MockContext:
    """Captures grpc status code + details set on a ServicerContext."""

    def __init__(self) -> None:
        self.code: grpc.StatusCode | None = None
        self.details: str = ""

    def set_code(self, code: grpc.StatusCode) -> None:
        self.code = code

    def set_details(self, details: str) -> None:
        self.details = details


def _make_servicer() -> PoolAnalyticsServiceServicer:
    return PoolAnalyticsServiceServicer(settings=GatewaySettings())


def _request(
    *,
    pool_address: str = _ANTONIS_POOL,
    chain: str = "arbitrum",
    protocol: str = "uniswap_v3",
) -> gateway_pb2.PoolAnalyticsRequest:
    return gateway_pb2.PoolAnalyticsRequest(
        pool_address=pool_address,
        chain=chain,
        protocol=protocol,
    )


# ============================================================================
# D1.S1 — Arbitrum univ3 happy path through CoinGecko Onchain (primary lane)
# ============================================================================


def test_get_pool_analytics_arbitrum_univ3():
    """D1.S1: returns string-decimal envelope for a known Arbitrum univ3 pool.

    Exact APR math from the fixture: fee_apr = volume_24h * fee * 365 /
    tvl * 100 = 820000 * 0.0005 * 365 / 1185000 * 100.
    """
    servicer = _make_servicer()
    payload = _load_fixture("geckoterminal_arbitrum_univ3.json")
    ctx = _MockContext()
    before = int(time.time())

    with patch.object(
        servicer,
        "_query_coingecko_onchain_pool",
        new=AsyncMock(return_value=payload),
    ):
        response = asyncio.run(servicer.GetPoolAnalytics(_request(), ctx))

    after = int(time.time())

    assert response.success is True
    assert response.tvl_usd == "1185000.0"
    assert response.volume_24h_usd == "820000.0"
    expected_apr = 820000.0 * 0.0005 * 365 / 1185000.0 * 100
    assert response.fee_apr == str(expected_apr) == "12.628691983122362"
    assert response.source == "coingecko_onchain"
    assert response.chain == "arbitrum"
    assert response.protocol == "uniswap_v3"
    assert before <= response.observed_at <= after + 1
    # gRPC OK = no code set on the mock context.
    assert ctx.code is None
    assert servicer.health()["coingecko_onchain"]["successes"] == 1


def _coingecko_pool_payload(attrs: dict[str, Any]) -> dict[str, Any]:
    return {"data": {"attributes": attrs}}


def test_parse_coingecko_pool_fee_percentage_is_percent_units():
    """pool_fee_percentage is percent units, so 0.3 means a 0.003 fee rate."""
    record = _parse_coingecko_onchain_pool(
        _coingecko_pool_payload(
            {
                "reserve_in_usd": "10000",
                "volume_usd": {"h24": "1000"},
                "pool_fee_percentage": "0.3",
                "dex_id": "uniswap_v3",
            }
        ),
        pool_address=_ETH_USDC_WETH,
        chain="ethereum",
        protocol="",
    )

    assert record.protocol == "uniswap_v3"
    assert record.fee_apr == "10.95"


def test_parse_coingecko_pool_fee_fraction_remains_direct_rate():
    """Legacy pool_fee is already a fraction, so 0.003 is used as-is."""
    record = _parse_coingecko_onchain_pool(
        _coingecko_pool_payload(
            {
                "reserve_in_usd": "10000",
                "volume_usd": {"h24": "1000"},
                "pool_fee": "0.003",
            }
        ),
        pool_address=_ETH_USDC_WETH,
        chain="ethereum",
        protocol="uniswap_v3",
    )

    assert record.fee_apr == "10.95"


def test_parse_coingecko_pool_fee_missing_stays_unmeasured():
    """Missing fee data is unmeasured, not silently substituted into APR."""
    record = _parse_coingecko_onchain_pool(
        _coingecko_pool_payload({"reserve_in_usd": "10000", "volume_usd": {"h24": "1000"}}),
        pool_address=_ETH_USDC_WETH,
        chain="ethereum",
        protocol="uniswap_v3",
    )

    assert record.fee_apr == ""


# ============================================================================
# API-key resolution — gateway-canonical name wins; bare name stays valid
# ============================================================================


def test_pool_analytics_uses_bare_coingecko_api_key_fallback(monkeypatch: pytest.MonkeyPatch):
    """Bare COINGECKO_API_KEY remains valid for local gateway operators."""
    monkeypatch.delenv("ALMANAK_GATEWAY_COINGECKO_API_KEY", raising=False)
    monkeypatch.setenv("COINGECKO_API_KEY", "bare-key")
    servicer = PoolAnalyticsServiceServicer(settings=GatewaySettings())

    assert servicer._coingecko_api_key == "bare-key"


def test_pool_analytics_prefers_gateway_canonical_coingecko_key(monkeypatch: pytest.MonkeyPatch):
    """When both env vars are set, ALMANAK_GATEWAY_COINGECKO_API_KEY wins."""
    monkeypatch.setenv("ALMANAK_GATEWAY_COINGECKO_API_KEY", "gateway-key")
    monkeypatch.setenv("COINGECKO_API_KEY", "bare-key")
    servicer = PoolAnalyticsServiceServicer(settings=GatewaySettings())

    assert servicer._coingecko_api_key == "gateway-key"


def test_missing_coingecko_key_is_nonfatal_and_names_env_vars(monkeypatch: pytest.MonkeyPatch):
    """Key absent → honest UNAVAILABLE (not a crash) naming both env vars."""
    monkeypatch.delenv("ALMANAK_GATEWAY_COINGECKO_API_KEY", raising=False)
    monkeypatch.delenv("COINGECKO_API_KEY", raising=False)
    servicer = PoolAnalyticsServiceServicer(settings=GatewaySettings())
    assert servicer._coingecko_api_key is None
    ctx = _MockContext()

    # No HTTP seam patched: the key check must trip BEFORE any session /
    # URL construction, so this test would fail loudly if a network call
    # were attempted (no aiohttp session exists to answer it).
    response = asyncio.run(servicer.GetPoolAnalytics(_request(), ctx))

    assert response.success is False
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert "COINGECKO_API_KEY" in response.error
    assert "ALMANAK_GATEWAY_COINGECKO_API_KEY" in response.error
    assert servicer.health()["coingecko_onchain"]["failures"] == 1


# ============================================================================
# D2.M1 — Chain matrix (Arbitrum / Ethereum) — network slug mapping
# ============================================================================


def test_chain_matrix_coingecko_onchain_url_includes_correct_network():
    """D2.M1: the CoinGecko Onchain URL embeds the per-chain network slug
    (``arbitrum`` for arbitrum, ``eth`` for ethereum)."""
    servicer = _make_servicer()
    captured_networks: list[str] = []
    fixtures = {
        "arbitrum": _load_fixture("geckoterminal_arbitrum_univ3.json"),
        "ethereum": _load_fixture("geckoterminal_ethereum_univ3.json"),
    }
    expected_tvl = {"arbitrum": "1185000.0", "ethereum": "210000000.0"}

    for chain in ("arbitrum", "ethereum"):

        async def fake_cg(network: str, pool_address: str, *, _chain: str = chain) -> dict[str, Any]:
            captured_networks.append(network)
            return fixtures[_chain]

        with patch.object(servicer, "_query_coingecko_onchain_pool", new=fake_cg):
            ctx = _MockContext()
            # Use a different pool address per chain so the cache doesn't short-circuit.
            pool = _ANTONIS_POOL if chain == "arbitrum" else _ETH_USDC_WETH
            response = asyncio.run(
                servicer.GetPoolAnalytics(
                    _request(pool_address=pool, chain=chain),
                    ctx,
                ),
            )
            assert response.success is True, response.error
            assert response.source == "coingecko_onchain"
            assert response.tvl_usd == expected_tvl[chain]

    # ``arbitrum`` maps to network slug ``arbitrum``;
    # ``ethereum`` maps to network slug ``eth``.
    assert captured_networks == ["arbitrum", "eth"]


def test_unsupported_chain_returns_invalid_argument():
    """A chain outside the CoinGecko Onchain network map fails fast."""
    servicer = _make_servicer()
    ctx = _MockContext()

    response = asyncio.run(
        servicer.GetPoolAnalytics(
            _request(chain="not-a-real-chain"),
            ctx,
        ),
    )

    assert response.success is False
    assert "unsupported chain" in response.error
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT


# ============================================================================
# D2.M4 — Cache hit on second call within TTL skips upstream HTTP;
#         third call with different ``protocol`` triggers a new HTTP call.
# ============================================================================


def test_cache_hit_skips_upstream_http():
    """D2.M4: second identical request does not re-hit the upstream; a
    third request with a *different* protocol misses the per-pool cache
    (proves protocol is in the cache key) and re-fetches."""
    servicer = _make_servicer()
    payload = _load_fixture("geckoterminal_arbitrum_univ3.json")
    call_count = 0

    async def counting_query(network: str, pool_address: str) -> dict[str, Any]:
        nonlocal call_count
        call_count += 1
        return payload

    with patch.object(servicer, "_query_coingecko_onchain_pool", new=counting_query):
        # 1st call — fresh per-pool cache miss.
        r1 = asyncio.run(servicer.GetPoolAnalytics(_request(), _MockContext()))
        assert r1.success is True
        assert r1.is_live_data is True
        assert call_count == 1

        # 2nd call — identical params, per-pool cache hit (no upstream).
        r2 = asyncio.run(servicer.GetPoolAnalytics(_request(), _MockContext()))
        assert r2.success is True
        assert r2.is_live_data is False  # served from per-pool cache
        assert r2.tvl_usd == r1.tvl_usd
        assert call_count == 1, "second identical call must NOT re-fetch"

        # 3rd call — different protocol. Per-pool cache key (chain, pool,
        # 'aerodrome') is DIFFERENT from the cached (chain, pool,
        # 'uniswap_v3'), so the per-pool cache misses and the upstream is
        # re-queried. The caller-supplied protocol is echoed back — the
        # cached uniswap_v3 record must NOT be served under the aerodrome
        # key.
        r3 = asyncio.run(
            servicer.GetPoolAnalytics(
                _request(protocol="aerodrome"),
                _MockContext(),
            ),
        )
        assert r3.success is True
        assert r3.is_live_data is True
        assert r3.protocol == "aerodrome"
        assert call_count == 2, "different protocol must be a per-pool cache miss"


# ============================================================================
# D3.F2 — Provider fails -> gRPC UNAVAILABLE, success=False
# ============================================================================


def test_provider_unavailable():
    """D3.F2 (gateway side): the provider raises -> response is
    success=False with gRPC status UNAVAILABLE and error names the lane."""
    servicer = _make_servicer()
    ctx = _MockContext()

    with patch.object(
        servicer,
        "_query_coingecko_onchain_pool",
        new=AsyncMock(side_effect=aiohttp.ClientError("upstream 503")),
    ):
        response = asyncio.run(servicer.GetPoolAnalytics(_request(), ctx))

    assert response.success is False
    assert "coingecko_onchain" in response.error
    assert "upstream 503" in response.error
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert servicer.health()["coingecko_onchain"]["failures"] == 1


def test_malformed_json_body_surfaces_as_provider_error():
    """A 200 response with a malformed body makes ``response.json()`` raise
    ``json.JSONDecodeError`` (a ``ValueError``). That garbage payload is a
    provider failure, not an unhandled server error: it must be mapped into
    the ``_ProviderError`` taxonomy so GetPoolAnalytics returns its structured
    UNAVAILABLE envelope rather than crashing the gRPC call with UNKNOWN.

    Drives the real ``_query_coingecko_onchain_pool`` (and thus the real
    ``response.json()``) via a fake HTTP session, so the assertion fails
    loudly if the ``ValueError`` catch is ever dropped from the seam."""

    class _FakeResponse:
        status = 200

        async def json(self) -> Any:
            raise json.JSONDecodeError("Expecting value", "<not json>", 0)

        async def text(self) -> str:
            return "<not json>"

    class _FakeGetCtx:
        async def __aenter__(self) -> _FakeResponse:
            return _FakeResponse()

        async def __aexit__(self, *_exc: object) -> bool:
            return False

    class _FakeSession:
        def get(self, _url: str, headers: dict[str, str] | None = None) -> _FakeGetCtx:
            return _FakeGetCtx()

    servicer = PoolAnalyticsServiceServicer(settings=GatewaySettings(coingecko_api_key="test-key"))
    ctx = _MockContext()

    with patch.object(servicer, "_get_http_session", new=AsyncMock(return_value=_FakeSession())):
        response = asyncio.run(servicer.GetPoolAnalytics(_request(), ctx))

    assert response.success is False
    assert "coingecko_onchain" in response.error
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert servicer.health()["coingecko_onchain"]["failures"] == 1


def test_local_rate_limit_surfaces_as_provider_error():
    """An empty local token bucket is a provider-side 'rate limited'
    failure (pre-existing CoinGecko-lane semantics)."""
    servicer = _make_servicer()
    servicer._rate_limiter_cg._tokens = 0.0
    ctx = _MockContext()

    response = asyncio.run(servicer.GetPoolAnalytics(_request(), ctx))

    assert response.success is False
    assert "coingecko_onchain: rate limited" in response.error
    assert ctx.code == grpc.StatusCode.UNAVAILABLE


# ============================================================================
# Regression guards (multi-auditor findings on PR #2389)
# ============================================================================


def test_invalid_evm_pool_address_returns_invalid_argument():
    """Blocker #3 from the multi-auditor audit: malformed pool_address
    must be rejected before any upstream URL is constructed (the
    CoinGecko Onchain URL template embeds the address segment)."""
    servicer = _make_servicer()
    ctx = _MockContext()

    response = asyncio.run(
        servicer.GetPoolAnalytics(
            _request(pool_address="../../v1/admin", chain="arbitrum"),
            ctx,
        ),
    )

    assert response.success is False
    assert "invalid pool_address" in response.error
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT


def test_short_address_prefix_rejected_as_invalid_argument():
    """A short address prefix (e.g. an attacker probing with ``0xc696``)
    fails syntactic validation — it can never reach the upstream URL."""
    servicer = _make_servicer()
    ctx = _MockContext()

    response = asyncio.run(
        servicer.GetPoolAnalytics(
            _request(pool_address="0xc696"),
            ctx,
        ),
    )

    assert response.success is False
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT


def test_solana_pool_address_preserves_case():
    """Blocker #1: Solana base58 addresses are case-sensitive. Lowercasing
    them produces a different address. ``solana`` is in the CoinGecko
    Onchain chain map, so the case-preservation contract must hold
    end-to-end."""
    servicer = _make_servicer()
    # Real-looking Solana base58 address with mixed case.
    solana_addr = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    ctx = _MockContext()
    # Stub the CoinGecko Onchain seam to capture the address it receives.
    captured_addresses: list[str] = []

    async def fake_cg(network: str, pool_address: str) -> dict[str, Any]:
        captured_addresses.append(pool_address)
        return _load_fixture("geckoterminal_arbitrum_univ3.json")

    with patch.object(servicer, "_query_coingecko_onchain_pool", new=fake_cg):
        asyncio.run(
            servicer.GetPoolAnalytics(
                _request(pool_address=solana_addr, chain="solana"),
                ctx,
            ),
        )

    # Case preserved end-to-end; would have been lowercased pre-fix.
    assert captured_addresses == [solana_addr]
    assert any(c.isupper() for c in captured_addresses[0])


def test_per_pool_cache_evicts_expired_on_write_and_caps_size():
    """Unique-key traffic over long uptime must not
    leak memory. Each ``_cache_put`` evicts expired entries and a hard
    cap caps the dict at ``_CACHE_MAX_ENTRIES``."""
    from almanak.gateway.services import pool_analytics_service as svc_mod

    servicer = _make_servicer()
    payload = _load_fixture("geckoterminal_arbitrum_univ3.json")

    # Pre-seed many "expired" entries so the eviction logic has work to do.
    expired_at = time.monotonic() - svc_mod._CACHE_TTL_SECONDS - 10
    for i in range(50):
        key = ("arbitrum", f"0x{'0' * 38}{i:02x}", "uniswap_v3")
        servicer._public_cache[key] = svc_mod._CacheEntry(
            record=svc_mod._PoolAnalyticsRecord(
                pool_address=key[1],
                chain=key[0],
                protocol=key[2],
            ),
            cached_at=expired_at,
        )
    assert len(servicer._public_cache) == 50

    # A live put triggers eviction → all 50 expired entries dropped.
    with patch.object(servicer, "_query_coingecko_onchain_pool", new=AsyncMock(return_value=payload)):
        asyncio.run(servicer.GetPoolAnalytics(_request(), _MockContext()))

    # Old expired keys gone; only the fresh one remains.
    assert all(k[1] == _ANTONIS_POOL for k in servicer._public_cache.keys()), (
        f"expired entries should have been evicted; got {list(servicer._public_cache)}"
    )


# ============================================================================
# D3.F6 — Silent-error guard: pool not found must NEVER yield a
#         success=True zero-filled envelope.
# ============================================================================


def test_pool_not_found_does_not_return_zero_envelope(caplog):
    """D3.F6: deterministic "not found" (CoinGecko Onchain 404) must
    surface as success=False UNAVAILABLE — never as a zero-filled
    success=True envelope."""
    import logging

    servicer = _make_servicer()
    ctx = _MockContext()

    with (
        # CoinGecko Onchain returns None on 404 (per servicer convention).
        patch.object(servicer, "_query_coingecko_onchain_pool", new=AsyncMock(return_value=None)),
        caplog.at_level(logging.WARNING, logger="almanak.gateway.services.pool_analytics_service"),
    ):
        response = asyncio.run(servicer.GetPoolAnalytics(_request(), ctx))

    # Hard guard: NEVER return success=True with empty/zero fields.
    assert response.success is False, "must not silently mask 'no data' as 'zero data'"
    assert response.tvl_usd == ""
    assert ctx.code == grpc.StatusCode.UNAVAILABLE
    assert "not found" in response.error.lower()
    # User-visible audit-log signal.
    assert any("not found" in record.getMessage().lower() for record in caplog.records)


# ============================================================================
# The structurally-dead catalog-matcher lane is GONE from
# this module, and the shared registry infrastructure it used to consume
# is untouched (other lanes still own it).
# ============================================================================


def test_dead_catalog_matcher_lane_removed_from_module():
    """Source guard: no DefiLlama matcher path survives in this module.

    The matcher was structurally dead (upstream catalog ids are UUIDs, not
    addresses — an address-equality match can never hit) and misleading:
    it burned a fetch of a multi-MB catalog per TTL for zero data. Any
    reappearance of the lane in this module is a regression.
    """
    from almanak.gateway.services import pool_analytics_service as svc_mod

    source = inspect.getsource(svc_mod).lower()
    assert "defillama" not in source
    assert "llama" not in source
    assert "yields.llama.fi" not in source

    for dead_symbol in (
        "_fetch_from_defillama",
        "_get_defillama_catalog",
        "_refresh_catalog",
        "_query_defillama_pools",
        "_parse_llama_pool",
        "_PROTOCOL_TO_LLAMA",
        "_LazyProtocolToLlama",
        "_build_protocol_to_llama",
        "_NOT_ATTEMPTED",
    ):
        assert not hasattr(svc_mod, dead_symbol), f"dead matcher symbol resurfaced: {dead_symbol}"

    servicer = _make_servicer()
    assert set(servicer.health()) == {"coingecko_onchain"}
    assert not hasattr(servicer, "_catalog_cache")
    assert not hasattr(servicer, "_rate_limiter_llama")


def test_shared_defillama_infrastructure_untouched():
    """The matcher was deleted FROM THIS SERVICE ONLY: the shared
    DefiLlama slug capability + chain map are still owned and consumed by
    the pool-history lanes and must keep resolving."""
    from almanak.connectors._base.gateway_capabilities import (
        GatewayDefillamaSlugCapability,
    )
    from almanak.gateway.data._history_common import _CHAIN_TO_LLAMA_DISPLAY
    from almanak.gateway.data.pool_history.dispatcher import (
        _defillama_slug_table,
        _resolve_defillama_slug,
    )

    assert GatewayDefillamaSlugCapability is not None
    assert "ethereum" in _CHAIN_TO_LLAMA_DISPLAY
    assert _resolve_defillama_slug("uniswap_v3") == "uniswap-v3"
    assert _defillama_slug_table()["uniswap_v3"] == "uniswap-v3"
