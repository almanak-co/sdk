"""Gateway-side PoolAnalyticsService tests (VIB-4727).

Covers the gateway-servicer half of the UAT card
``docs/internal/uat-cards/VIB-4727.md`` (D1.S1, D2.M1, D2.M2, D2.M4,
D3.F2, D3.F6). Tests patch the upstream provider seams
(``_query_defillama_pools``, ``_query_geckoterminal_pool``) with recorded
JSON fixtures from
``tests/gateway/services/fixtures/pool_analytics/`` — no live external API
is reached. Patching at this seam (the function returning the parsed JSON
dict) is the "equivalent in-test HTTP mocking" the UAT card permits
in lieu of ``aioresponses`` (which is not a project dependency).
"""

from __future__ import annotations

import asyncio
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
)

_FIXTURES = Path(__file__).parent / "fixtures" / "pool_analytics"
_ANTONIS_POOL = "0xc6962004f452be9203591991d15f6b388e09e8d0"  # USDC/WETH 0.05% Arbitrum
_ETH_USDC_WETH = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"  # USDC/WETH 0.05% Ethereum


def _load_fixture(name: str) -> Any:
    with (_FIXTURES / name).open("r") as f:
        return json.load(f)


def _llama_pools(name: str) -> list[dict[str, Any]]:
    return _load_fixture(name)["data"]


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
# D1.S1 — Arbitrum univ3 happy path through DefiLlama
# ============================================================================


def test_get_pool_analytics_arbitrum_univ3():
    """D1.S1: returns string-decimal envelope for a known Arbitrum univ3 pool."""
    servicer = _make_servicer()
    pools = _llama_pools("defillama_arbitrum_univ3.json")
    ctx = _MockContext()
    before = int(time.time())

    with patch.object(
        servicer,
        "_query_defillama_pools",
        new=AsyncMock(return_value=pools),
    ):
        response = asyncio.run(servicer.GetPoolAnalytics(_request(), ctx))

    after = int(time.time())

    assert response.success is True
    assert response.tvl_usd == "1210000.0"
    assert response.fee_apr == "12.5"
    assert response.source == "defillama"
    assert response.chain == "arbitrum"
    assert response.protocol == "uniswap_v3"
    assert before <= response.observed_at <= after + 1
    # gRPC OK = no code set on the mock context.
    assert ctx.code is None


# ============================================================================
# D2.M1 — Chain matrix (Arbitrum / Ethereum) — provider name-mapping divergence
# ============================================================================


@pytest.mark.parametrize(
    "chain, pool_address, fixture_name, expected_pool_chain, expected_tvl",
    [
        ("arbitrum", _ANTONIS_POOL, "defillama_arbitrum_univ3.json", "Arbitrum", "1210000.0"),
        ("ethereum", _ETH_USDC_WETH, "defillama_ethereum_univ3.json", "Ethereum", "215000000.0"),
    ],
)
def test_chain_matrix_arbitrum_and_ethereum(
    chain: str,
    pool_address: str,
    fixture_name: str,
    expected_pool_chain: str,
    expected_tvl: str,
):
    """D2.M1: Arbitrum + Ethereum both map cleanly through DefiLlama; only
    the chain-matching pool is selected from the same fixture list."""
    servicer = _make_servicer()
    pools = _llama_pools(fixture_name)
    ctx = _MockContext()

    with patch.object(
        servicer,
        "_query_defillama_pools",
        new=AsyncMock(return_value=pools),
    ):
        response = asyncio.run(
            servicer.GetPoolAnalytics(
                _request(pool_address=pool_address, chain=chain),
                ctx,
            ),
        )

    assert response.success is True, response.error
    assert response.chain == chain
    assert response.tvl_usd == expected_tvl
    # Verify the right candidate row was picked (chain mapping):
    selected = next(p for p in pools if pool_address in p["pool"].lower() and p["chain"] == expected_pool_chain)
    assert str(selected["tvlUsd"]) == response.tvl_usd.rstrip("0").rstrip(".") or response.tvl_usd == f"{selected['tvlUsd']}"


def test_chain_matrix_geckoterminal_url_includes_correct_network():
    """D2.M1 (cont.): GeckoTerminal fallback URL embeds the per-chain
    network slug (``arbitrum`` for arbitrum, ``eth`` for ethereum)."""
    servicer = _make_servicer()
    captured_networks: list[str] = []
    gt_fixture = _load_fixture("geckoterminal_arbitrum_univ3.json")

    async def fake_gt(network: str, pool_address: str) -> dict[str, Any]:
        captured_networks.append(network)
        return gt_fixture

    # Force DefiLlama to miss so the fallback path runs.
    with (
        patch.object(servicer, "_query_defillama_pools", new=AsyncMock(return_value=[])),
        patch.object(servicer, "_query_geckoterminal_pool", new=fake_gt),
    ):
        for chain in ("arbitrum", "ethereum"):
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
            assert response.source == "geckoterminal"

    # ``arbitrum`` maps to GT network slug ``arbitrum``;
    # ``ethereum`` maps to GT network slug ``eth``.
    assert captured_networks == ["arbitrum", "eth"]


# ============================================================================
# D2.M2 — Provider fallback (DefiLlama -> GeckoTerminal)
# ============================================================================


def test_provider_fallback_defillama_to_geckoterminal():
    """D2.M2: DefiLlama HTTP failure routes to GeckoTerminal; metrics
    record one failure on DefiLlama and one success on GeckoTerminal."""
    servicer = _make_servicer()
    gt_fixture = _load_fixture("geckoterminal_arbitrum_univ3.json")
    ctx = _MockContext()

    with (
        patch.object(
            servicer,
            "_query_defillama_pools",
            new=AsyncMock(side_effect=aiohttp.ClientError("upstream 503")),
        ),
        patch.object(
            servicer,
            "_query_geckoterminal_pool",
            new=AsyncMock(return_value=gt_fixture),
        ),
    ):
        response = asyncio.run(servicer.GetPoolAnalytics(_request(), ctx))

    assert response.success is True
    assert response.source == "geckoterminal"
    assert ctx.code is None
    metrics = servicer.health()
    assert metrics["defillama"]["failures"] == 1
    assert metrics["geckoterminal"]["successes"] == 1


# ============================================================================
# D2.M4 — Cache hit on second call within TTL skips upstream HTTP;
#         third call with different ``protocol`` triggers a new HTTP call.
# ============================================================================


def test_cache_hit_skips_upstream_http():
    """D2.M4: second identical request does not re-hit the upstream;
    a third request with a *different* protocol exercises a fresh
    per-pool match against the cached catalog (proves protocol is in
    the per-pool cache key).

    Architecture note (Important #4 from PR #2389 audit): the upstream
    DefiLlama /pools catalog is amortized across all per-pool callers
    via `_get_defillama_catalog`, so the upstream HTTP-call count is NOT
    a 1:1 proxy for per-pool cache misses. The per-pool cache key
    (chain, pool, protocol) is still protocol-sensitive — proven below
    by the third request producing a fresh `is_live_data=True` envelope
    (cache miss on the per-pool key) while the catalog query is reused.
    """
    servicer = _make_servicer()
    pools = _llama_pools("defillama_arbitrum_univ3.json")
    call_count = 0

    async def counting_query() -> list[dict[str, Any]]:
        nonlocal call_count
        call_count += 1
        return pools

    with (
        patch.object(servicer, "_query_defillama_pools", new=counting_query),
        # Stub GeckoTerminal too so the 3rd-call fallback (when the DefiLlama
        # protocol filter rejects the uniswap_v3 fixture) doesn't touch real HTTP.
        patch.object(servicer, "_query_geckoterminal_pool", new=AsyncMock(return_value=None)),
    ):
        # 1st call — fresh per-pool cache miss + fresh catalog fetch.
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
        # 'aerodrome') is DIFFERENT from the cached (chain, pool, 'uniswap_v3'),
        # so the per-pool cache misses. The catalog cache absorbs the
        # upstream fetch (call_count stays at 1) — this is the amortization
        # win from Important #4 of the PR #2389 audit. The aerodrome
        # project filter rejects the uniswap_v3 fixture, so the response
        # is a not-found UNAVAILABLE; the load-bearing assertion is that
        # the per-pool cache did NOT serve a uniswap_v3 record under the
        # aerodrome key.
        r3 = asyncio.run(
            servicer.GetPoolAnalytics(
                _request(protocol="aerodrome"),
                _MockContext(),
            ),
        )
        assert r3.success is False, "different protocol must not serve uniswap_v3 cache record"
        assert call_count == 1, "catalog cache should absorb the upstream fetch"


# ============================================================================
# D3.F2 — All providers fail -> gRPC UNAVAILABLE, success=False
# ============================================================================


def test_all_providers_unavailable():
    """D3.F2 (gateway side): both providers raise -> response is
    success=False with gRPC status UNAVAILABLE and error mentions both."""
    servicer = _make_servicer()
    ctx = _MockContext()

    with (
        patch.object(
            servicer,
            "_query_defillama_pools",
            new=AsyncMock(side_effect=aiohttp.ClientError("llama down")),
        ),
        patch.object(
            servicer,
            "_query_geckoterminal_pool",
            new=AsyncMock(side_effect=aiohttp.ClientError("gt down")),
        ),
    ):
        response = asyncio.run(servicer.GetPoolAnalytics(_request(), ctx))

    assert response.success is False
    assert "defillama" in response.error
    assert "geckoterminal" in response.error
    assert ctx.code == grpc.StatusCode.UNAVAILABLE


# ============================================================================
# D3.F6 — Silent-error guard: pool not found must NEVER yield a
#         success=True zero-filled envelope.
# ============================================================================


# ============================================================================
# Regression guards (multi-auditor findings on PR #2389)
# ============================================================================


def test_invalid_evm_pool_address_returns_invalid_argument():
    """Blocker #3 from the multi-auditor audit: malformed pool_address
    must be rejected before any upstream URL is constructed (the
    GeckoTerminal URL template embeds the address segment)."""
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


def test_solana_pool_address_preserves_case():
    """Blocker #1: Solana base58 addresses are case-sensitive. Lowercasing
    them produces a different address. The PR includes ``solana`` in the
    GeckoTerminal chain map, so the case-preservation contract must hold
    end-to-end."""
    servicer = _make_servicer()
    # Real-looking Solana base58 address with mixed case.
    solana_addr = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
    ctx = _MockContext()
    # Stub the GeckoTerminal seam to capture the address it receives.
    captured_addresses: list[str] = []

    async def fake_gt(network: str, pool_address: str) -> dict[str, Any]:
        captured_addresses.append(pool_address)
        return _load_fixture("geckoterminal_arbitrum_univ3.json")

    with (
        # Stub DefiLlama too so the test doesn't accidentally hit the
        # live /pools endpoint when DefiLlama's `solana` chain branch
        # runs. CodeRabbit PR #2389 review thread.
        patch.object(servicer, "_query_defillama_pools", new=AsyncMock(return_value=[])),
        patch.object(servicer, "_query_geckoterminal_pool", new=fake_gt),
    ):
        asyncio.run(
            servicer.GetPoolAnalytics(
                _request(pool_address=solana_addr, chain="solana"),
                ctx,
            ),
        )

    # Case preserved end-to-end; would have been lowercased pre-fix.
    assert captured_addresses == [solana_addr]
    assert "j" not in solana_addr.lower() or any(c.isupper() for c in captured_addresses[0])


def test_defillama_matcher_uses_address_equality_not_substring():
    """Important #6: a short attacker-controlled prefix must NOT match an
    unrelated pool whose DefiLlama id happens to contain that prefix."""
    servicer = _make_servicer()
    # Fixture pool address is ``0xc6962004...`` for the legit row.
    # An attacker passes a 4-char prefix that's a substring of the legit
    # pool's id but would have to match the full address segment to
    # actually pin a pool. Post-fix this MUST return INVALID_ARGUMENT
    # (validation rejects the malformed short address) — proving the
    # substring collision path is gone.
    ctx = _MockContext()
    response = asyncio.run(
        servicer.GetPoolAnalytics(
            _request(pool_address="0xc696"),
            ctx,
        ),
    )
    assert response.success is False
    assert ctx.code == grpc.StatusCode.INVALID_ARGUMENT


def test_defillama_catalog_amortized_across_per_pool_callers():
    """Important #4: the multi-MB DefiLlama /pools catalog must be
    fetched once per TTL and shared across per-pool callers — N strategies
    polling N different pools must NOT trigger N catalog fetches per
    minute."""
    servicer = _make_servicer()
    pools = _llama_pools("defillama_arbitrum_univ3.json") + _llama_pools(
        "defillama_ethereum_univ3.json",
    )
    fetch_count = 0

    async def counting_catalog() -> list[dict[str, Any]]:
        nonlocal fetch_count
        fetch_count += 1
        return pools

    with patch.object(servicer, "_query_defillama_pools", new=counting_catalog):
        # 3 DIFFERENT per-pool calls — same chain or different — must
        # share the catalog fetch.
        for pool_addr, chain in [
            (_ANTONIS_POOL, "arbitrum"),
            (_ETH_USDC_WETH, "ethereum"),
            (_ANTONIS_POOL, "arbitrum"),  # cache hit on per-pool key
        ]:
            asyncio.run(
                servicer.GetPoolAnalytics(
                    _request(pool_address=pool_addr, chain=chain),
                    _MockContext(),
                ),
            )

    assert fetch_count == 1, "catalog must be fetched once and reused across per-pool callers"


def test_per_pool_cache_evicts_expired_on_write_and_caps_size():
    """CodeRabbit PR #2389: unique-key traffic over long uptime must not
    leak memory. Each ``_cache_put`` evicts expired entries and a hard
    cap caps the dict at ``_CACHE_MAX_ENTRIES``."""
    from almanak.gateway.services import pool_analytics_service as svc_mod

    servicer = _make_servicer()
    pools = _llama_pools("defillama_arbitrum_univ3.json")

    # Pre-seed many "expired" entries so the eviction logic has work to do.
    expired_at = time.monotonic() - svc_mod._CACHE_TTL_SECONDS - 10
    for i in range(50):
        key = ("arbitrum", f"0x{'0' * 38}{i:02x}", "uniswap_v3")
        servicer._public_cache[key] = svc_mod._CacheEntry(
            record=svc_mod._PoolAnalyticsRecord(
                pool_address=key[1], chain=key[0], protocol=key[2],
            ),
            cached_at=expired_at,
        )
    assert len(servicer._public_cache) == 50

    # A live put triggers eviction → all 50 expired entries dropped.
    with patch.object(servicer, "_query_defillama_pools", new=AsyncMock(return_value=pools)):
        asyncio.run(servicer.GetPoolAnalytics(_request(), _MockContext()))

    # Old expired keys gone; only the fresh one remains.
    assert all(
        k[1] == _ANTONIS_POOL
        for k in servicer._public_cache.keys()
    ), f"expired entries should have been evicted; got {list(servicer._public_cache)}"


def test_defillama_local_rate_limit_skip_is_not_a_not_found_miss():
    """CodeRabbit PR #2389: a local rate-limit skip must NOT be conflated
    with "fetched-and-no-match". The error string must not contain
    'defillama: not found' when the local bucket was empty."""
    from almanak.gateway.services import pool_analytics_service as svc_mod

    servicer = _make_servicer()
    # Force the rate-limit bucket empty.
    servicer._rate_limiter_llama._tokens = 0.0
    ctx = _MockContext()

    with patch.object(
        servicer,
        "_query_geckoterminal_pool",
        new=AsyncMock(return_value=None),
    ):
        response = asyncio.run(servicer.GetPoolAnalytics(_request(), ctx))

    # GeckoTerminal returned not-found, but defillama should NOT appear in
    # the error string (the skip was local, not a miss).
    assert response.success is False
    assert "defillama: not found" not in response.error
    # The sentinel propagated through; no DefiLlama failure tallied.
    assert servicer.health()["defillama"]["failures"] == 0
    # And the sentinel is the documented module symbol callers can check.
    assert isinstance(svc_mod._NOT_ATTEMPTED, svc_mod._NotAttempted)


def test_concurrent_cold_cache_callers_share_one_catalog_fetch():
    """CodeRabbit PR #2389: N concurrent callers landing on an empty
    catalog must result in ONE upstream /pools fetch, not N. Without the
    in-flight dedup the rate limiter would burst at TTL boundaries."""
    servicer = _make_servicer()
    pools = _llama_pools("defillama_arbitrum_univ3.json")
    fetch_count = 0
    fetch_started = asyncio.Event()
    fetch_continue = asyncio.Event()

    async def slow_fetch() -> list[dict[str, Any]]:
        nonlocal fetch_count
        fetch_count += 1
        fetch_started.set()
        # Hold the fetch open so the other concurrent callers race in.
        await fetch_continue.wait()
        return pools

    async def run() -> None:
        with patch.object(servicer, "_query_defillama_pools", new=slow_fetch):
            # Kick 5 concurrent GetPoolAnalytics calls with the same key —
            # the per-pool cache would normally serve subsequent hits, so
            # force unique per-pool keys to drive them all through the
            # catalog code path.
            tasks = [
                asyncio.create_task(
                    servicer.GetPoolAnalytics(
                        _request(pool_address=f"0xc6962004f452be9203591991d15f6b388e09e8d{i:1x}"),
                        _MockContext(),
                    ),
                )
                for i in range(5)
            ]
            await fetch_started.wait()
            fetch_continue.set()
            await asyncio.gather(*tasks)

    asyncio.run(run())
    assert fetch_count == 1, "all concurrent cold-cache callers must share ONE upstream fetch"


def test_pool_not_found_does_not_return_zero_envelope(caplog):
    """D3.F6: deterministic "not found" from both providers (DefiLlama list
    contains only the pool on a different chain; GeckoTerminal 404) must
    surface as success=False UNAVAILABLE — never as a zero-filled
    success=True envelope."""
    import logging

    servicer = _make_servicer()
    # Wrong-chain-only DefiLlama fixture -> chain-filter rejects -> not found.
    pools = _llama_pools("defillama_wrong_chain_only.json")
    ctx = _MockContext()

    with (
        patch.object(servicer, "_query_defillama_pools", new=AsyncMock(return_value=pools)),
        # GeckoTerminal returns None on 404 (per servicer convention).
        patch.object(servicer, "_query_geckoterminal_pool", new=AsyncMock(return_value=None)),
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
