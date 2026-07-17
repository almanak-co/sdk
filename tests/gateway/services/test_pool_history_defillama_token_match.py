"""DefiLlama UUID-id token-set matching tests (ALM-2940).

The live yields catalog's ``pool`` ids are 100% opaque UUIDs (0 of ~15.4k
entries carry the legacy ``chain-0xaddress`` style as of 2026-07-15), so the
address-segment matcher alone is an always-miss. These tests pin the revived
matcher: (project, chain, exact underlying-token set) with the measured
TVL-consistency cross-check for same-token-set twins, refusing on any
ambiguity. Plus the dispatcher-side pool-token resolver that feeds it.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

from almanak.gateway.data.pool_history._base import _TokenBucket
from almanak.gateway.data.pool_history.defillama import (
    DefiLlamaPoolHistoryProvider,
    ResolvedPoolIdentity,
)
from almanak.gateway.data.pool_history.dispatcher import PoolHistoryDispatcher
from almanak.gateway.proto import gateway_pb2

# Real Base-chain addresses (aerodrome classic WETH/USDC vAMM + tokens);
# mixed-case underlyingTokens mirror the live catalog's checksummed entries.
_VAMM_ADDRESS = "0xcdac0d6c6c59727a65f871236188350531885c43"
_WETH = "0x4200000000000000000000000000000000000006"
_USDC_CHECKSUMMED = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
_TOKEN_SET = frozenset({_WETH, _USDC_CHECKSUMMED.lower()})

_LB_ADDRESS = "0x864d4e5ee7318e97483db7eb0912e09f161516ea"
_WAVAX = "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"
_USDC_AVAX = "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"


def _entry(pool_id: str, project: str, chain: str, tokens: list[str], tvl: float) -> dict[str, Any]:
    return {
        "pool": pool_id,
        "project": project,
        "chain": chain,
        "symbol": "X-Y",
        "tvlUsd": tvl,
        "underlyingTokens": tokens,
    }


_CATALOG = [
    # The Solidly twins: volatile ($7.58M) + stable ($24k) share the token set.
    _entry("uuid-vamm", "aerodrome-v1", "Base", [_WETH, _USDC_CHECKSUMMED], 7_583_793),
    _entry("uuid-samm", "aerodrome-v1", "Base", [_WETH, _USDC_CHECKSUMMED], 24_452),
    # Same tokens, different project — must never match through the slug filter.
    _entry("uuid-decoy", "uniswap-v3", "Base", [_WETH, _USDC_CHECKSUMMED], 7_000_000),
    # Lone LB candidate on Avalanche.
    _entry("uuid-joe", "joe-v2.2", "Avalanche", [_WAVAX, _USDC_AVAX], 384_976),
]

_SLUGS = {"aerodrome": "aerodrome-v1", "traderjoe_v2": "joe-v2.2"}


def _provider(resolver: Any) -> DefiLlamaPoolHistoryProvider:
    return DefiLlamaPoolHistoryProvider(
        session_getter=AsyncMock(),
        slug_resolver=lambda protocol: _SLUGS.get(protocol),
        rate_limiter=_TokenBucket(rate=100, period=1.0),
        pool_token_resolver=resolver,
    )


def _match(provider: DefiLlamaPoolHistoryProvider, *, chain: str, llama_chain: str, address: str, protocol: str):
    return asyncio.run(
        provider._match_pool_id_by_tokens(
            _CATALOG,
            chain=chain,
            llama_chain=llama_chain,
            pool_address=address,
            protocol=protocol,
        )
    )


def test_lone_candidate_matches_on_token_set_alone():
    """One (project, chain, token-set) candidate matches without a reserve."""

    async def resolver(_chain: str, _address: str) -> ResolvedPoolIdentity:
        return ResolvedPoolIdentity(tokens=frozenset({_WAVAX, _USDC_AVAX}), reserve_usd=None)

    result = _match(
        _provider(resolver), chain="avalanche", llama_chain="Avalanche", address=_LB_ADDRESS, protocol="traderjoe_v2"
    )
    assert result == "uuid-joe"


def test_lone_candidate_with_reserve_off_band_refuses():
    """A lone candidate is cross-checked too when a live reserve exists: the
    free-tier catalog omits pools, so the only listed same-token-set pool may
    be a SIBLING, not the strategy's pool. Off-band TVL -> refuse, never a
    silent wrong-pool match. (uuid-joe TVL $384,976 vs live reserve $50k ->
    ratio ~7.7, outside [0.5, 2.0].)"""

    async def resolver(_chain: str, _address: str) -> ResolvedPoolIdentity:
        return ResolvedPoolIdentity(tokens=frozenset({_WAVAX, _USDC_AVAX}), reserve_usd=Decimal("50000"))

    result = _match(
        _provider(resolver), chain="avalanche", llama_chain="Avalanche", address=_LB_ADDRESS, protocol="traderjoe_v2"
    )
    assert result is None


def test_lone_candidate_with_reserve_in_band_matches():
    """A lone candidate whose catalog TVL is consistent with the live reserve
    still matches (ratio ~1.01, inside [0.5, 2.0])."""

    async def resolver(_chain: str, _address: str) -> ResolvedPoolIdentity:
        return ResolvedPoolIdentity(tokens=frozenset({_WAVAX, _USDC_AVAX}), reserve_usd=Decimal("380000"))

    result = _match(
        _provider(resolver), chain="avalanche", llama_chain="Avalanche", address=_LB_ADDRESS, protocol="traderjoe_v2"
    )
    assert result == "uuid-joe"


def test_twins_disambiguated_by_tvl_consistency():
    """Solidly twins: only the candidate consistent with the live reserve wins."""

    async def resolver(_chain: str, _address: str) -> ResolvedPoolIdentity:
        return ResolvedPoolIdentity(tokens=_TOKEN_SET, reserve_usd=Decimal("7500000"))

    result = _match(_provider(resolver), chain="base", llama_chain="Base", address=_VAMM_ADDRESS, protocol="aerodrome")
    assert result == "uuid-vamm"


def test_twins_without_reserve_refuse():
    """No live reserve + several same-token-set candidates -> refuse, never guess."""

    async def resolver(_chain: str, _address: str) -> ResolvedPoolIdentity:
        return ResolvedPoolIdentity(tokens=_TOKEN_SET, reserve_usd=None)

    result = _match(_provider(resolver), chain="base", llama_chain="Base", address=_VAMM_ADDRESS, protocol="aerodrome")
    assert result is None


def test_twins_both_in_band_refuse():
    """Two candidates inside the consistency band is still ambiguity -> refuse."""
    catalog = [
        _entry("uuid-a", "aerodrome-v1", "Base", [_WETH, _USDC_CHECKSUMMED], 100_000),
        _entry("uuid-b", "aerodrome-v1", "Base", [_WETH, _USDC_CHECKSUMMED], 150_000),
    ]

    async def resolver(_chain: str, _address: str) -> ResolvedPoolIdentity:
        return ResolvedPoolIdentity(tokens=_TOKEN_SET, reserve_usd=Decimal("120000"))

    provider = _provider(resolver)
    result = asyncio.run(
        provider._match_pool_id_by_tokens(
            catalog, chain="base", llama_chain="Base", pool_address=_VAMM_ADDRESS, protocol="aerodrome"
        )
    )
    assert result is None


def test_no_registry_slug_refuses_token_matching():
    """Without a project slug the token-set path must not run (cross-project collisions)."""

    async def resolver(_chain: str, _address: str) -> ResolvedPoolIdentity:
        return ResolvedPoolIdentity(tokens=_TOKEN_SET, reserve_usd=Decimal("7500000"))

    result = _match(
        _provider(resolver), chain="base", llama_chain="Base", address=_VAMM_ADDRESS, protocol="unknown_protocol"
    )
    assert result is None


def test_resolver_none_and_resolver_crash_yield_no_match():
    """A failed or crashing token resolver degrades to not-found, never raises."""

    async def none_resolver(_chain: str, _address: str) -> None:
        return None

    async def crashing_resolver(_chain: str, _address: str) -> ResolvedPoolIdentity:
        raise RuntimeError("boom")

    for resolver in (none_resolver, crashing_resolver):
        result = _match(
            _provider(resolver), chain="base", llama_chain="Base", address=_VAMM_ADDRESS, protocol="aerodrome"
        )
        assert result is None


def test_no_resolver_disables_token_path():
    """pool_token_resolver=None (default) keeps the legacy-only behaviour."""
    provider = DefiLlamaPoolHistoryProvider(
        session_getter=AsyncMock(),
        slug_resolver=lambda protocol: _SLUGS.get(protocol),
        rate_limiter=_TokenBucket(rate=100, period=1.0),
    )
    result = _match(provider, chain="base", llama_chain="Base", address=_VAMM_ADDRESS, protocol="aerodrome")
    assert result is None


# ===========================================================================
# fetch() end-to-end: catalog (UUID ids) -> token match -> chart
# ===========================================================================


class _FakeResp:
    def __init__(self, status: int, payload: Any) -> None:
        self.status = status
        self._payload = payload

    async def json(self) -> Any:
        return self._payload

    async def text(self) -> str:
        return str(self._payload)

    async def __aenter__(self) -> _FakeResp:
        return self

    async def __aexit__(self, *_a: Any) -> bool:
        return False


class _UrlRoutedSession:
    """Routes GETs by URL substring; records requested URLs."""

    def __init__(self, routes: dict[str, _FakeResp]) -> None:
        self._routes = routes
        self.urls: list[str] = []
        self.closed = False

    def get(self, url: str, **_kwargs: Any) -> _FakeResp:
        self.urls.append(url)
        for fragment, resp in self._routes.items():
            if fragment in url:
                return resp
        raise AssertionError(f"unexpected URL {url}")


def test_fetch_uuid_catalog_end_to_end():
    """Address-segment miss -> token-set match -> /chart fetched -> snapshots."""
    session = _UrlRoutedSession(
        {
            "/pools": _FakeResp(200, {"data": _CATALOG}),
            "/chart/uuid-joe": _FakeResp(
                200,
                {"data": [{"timestamp": 86400, "tvlUsd": 384976.0}]},
            ),
        }
    )

    async def resolver(_chain: str, _address: str) -> ResolvedPoolIdentity:
        return ResolvedPoolIdentity(tokens=frozenset({_WAVAX, _USDC_AVAX}), reserve_usd=Decimal("380000"))

    provider = DefiLlamaPoolHistoryProvider(
        session_getter=AsyncMock(return_value=session),
        slug_resolver=lambda protocol: _SLUGS.get(protocol),
        rate_limiter=_TokenBucket(rate=100, period=1.0),
        pool_token_resolver=resolver,
    )
    result = asyncio.run(
        provider.fetch(
            chain="avalanche",
            pool_address=_LB_ADDRESS,
            protocol="traderjoe_v2",
            start_ts=0,
            end_ts=2 * 86400,
            resolution=gateway_pb2.Resolution.RESOLUTION_1D,
        )
    )
    assert result is not None and len(result) == 1
    assert result[0].timestamp == 86400
    assert Decimal(result[0].tvl) == Decimal("384976")
    assert any("/chart/uuid-joe" in url for url in session.urls)


# ===========================================================================
# Dispatcher pool-token resolver
# ===========================================================================


def _cg_pool_payload(base: str, quote: str, reserve: str, network: str = "base") -> dict[str, Any]:
    return {
        "data": {
            "attributes": {"reserve_in_usd": reserve},
            "relationships": {
                "base_token": {"data": {"id": f"{network}_{base}"}},
                "quote_token": {"data": {"id": f"{network}_{quote}"}},
            },
        }
    }


def _dispatcher(session: Any, *, clock=None) -> PoolHistoryDispatcher:
    dispatcher = PoolHistoryDispatcher(
        thegraph_api_key="test-key",
        thegraph_monthly_budget_max=1000,
        is_supported_fn=lambda _chain, _protocol: True,
        coingecko_api_key="test-cg-key",
        **({"clock": clock} if clock is not None else {}),
    )
    dispatcher._http_session = session
    return dispatcher


def test_resolver_parses_tokens_and_reserve():
    session = _UrlRoutedSession(
        {"/networks/base/pools/": _FakeResp(200, _cg_pool_payload(_WETH, _USDC_CHECKSUMMED, "7583793.5"))}
    )
    dispatcher = _dispatcher(session)
    identity = asyncio.run(dispatcher._resolve_pool_token_set("base", _VAMM_ADDRESS))
    assert identity is not None
    assert identity.tokens == _TOKEN_SET  # EVM addresses normalized lowercase
    assert identity.reserve_usd == Decimal("7583793.5")


def test_resolver_half_resolved_identity_is_none():
    """A missing quote token yields None — one token matches every pool with it."""
    payload = _cg_pool_payload(_WETH, _USDC_CHECKSUMMED, "1")
    del payload["data"]["relationships"]["quote_token"]
    session = _UrlRoutedSession({"/networks/base/pools/": _FakeResp(200, payload)})
    assert asyncio.run(_dispatcher(session)._resolve_pool_token_set("base", _VAMM_ADDRESS)) is None


def test_resolver_http_error_and_missing_key_yield_none():
    session = _UrlRoutedSession({"/networks/base/pools/": _FakeResp(500, {"error": "boom"})})
    assert asyncio.run(_dispatcher(session)._resolve_pool_token_set("base", _VAMM_ADDRESS)) is None

    keyless = PoolHistoryDispatcher(
        thegraph_api_key="test-key",
        thegraph_monthly_budget_max=1000,
        is_supported_fn=lambda _chain, _protocol: True,
        coingecko_api_key="",
    )
    assert asyncio.run(keyless._resolve_pool_token_set("base", _VAMM_ADDRESS)) is None


def test_resolver_caches_identity_and_expires_by_ttl():
    """Second call within TTL is served from cache; past TTL re-fetches."""
    now = {"t": 1000.0}
    session = _UrlRoutedSession(
        {"/networks/base/pools/": _FakeResp(200, _cg_pool_payload(_WETH, _USDC_CHECKSUMMED, "100"))}
    )
    dispatcher = _dispatcher(session, clock=lambda: now["t"])

    first = asyncio.run(dispatcher._resolve_pool_token_set("base", _VAMM_ADDRESS))
    second = asyncio.run(dispatcher._resolve_pool_token_set("base", _VAMM_ADDRESS))
    assert first == second
    assert len(session.urls) == 1  # cache hit — no second HTTP call

    now["t"] += 3601.0  # past _POOL_TOKEN_CACHE_TTL_SECONDS
    third = asyncio.run(dispatcher._resolve_pool_token_set("base", _VAMM_ADDRESS))
    assert third == first
    assert len(session.urls) == 2  # TTL expiry -> live reserve re-measured
