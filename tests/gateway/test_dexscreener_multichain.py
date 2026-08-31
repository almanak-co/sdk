"""Multi-chain dispatch tests for DexScreenerPriceSource (VIB-3259 Phase 2).

Before Phase 2, ``DexScreenerPriceSource`` was constructor-locked to a single
``chain_id``. On a multi-chain gateway, only that one chain's DexScreener
queries worked; everything else either crashed with ``ValueError`` or (worse)
returned pairs from the wrong chain.

Phase 2 makes the source per-call: a single instance dispatches on
``resolved_token.chain`` for every request. Unsupported chains raise
``DataSourceUnavailable(reason="chain_unsupported:...")`` which the
aggregator treats as a non-error skip. These tests pin that behaviour.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.framework.data.interfaces import DataSourceUnavailable
from almanak.framework.data.tokens import ResolvedToken
from almanak.gateway.data.price.dexscreener import DexScreenerPriceSource


_TEST_ADDRESS = "0x1234567890aBcDeF1234567890AbCdEf12345678"


def _pair(
    price: str,
    chain_id: str,
    liquidity_usd: float = 5_000_000,
    base_address: str = _TEST_ADDRESS,
    base_symbol: str = "FOO",
    quote_address: str = "0x000000000000000000000000000000000000dEaD",
    quote_symbol: str = "USDX",
    price_native: str | None = None,
) -> dict:
    pair = {
        "chainId": chain_id,
        "priceUsd": price,
        "liquidity": {"usd": liquidity_usd},
        "volume": {"h24": 1_000_000},
        "baseToken": {"address": base_address, "symbol": base_symbol},
        "quoteToken": {"address": quote_address, "symbol": quote_symbol},
    }
    if price_native is not None:
        pair["priceNative"] = price_native
    return pair


def _pair_json(price: str, chain_id: str, liquidity_usd: float = 5_000_000, **kwargs) -> list[dict]:
    return [_pair(price, chain_id, liquidity_usd, **kwargs)]


def _mock_session(source: DexScreenerPriceSource):
    """Mock session that records every (url, params) and returns configured payloads.

    Returns (captured, set_payload, patcher). ``set_payload(payload)`` installs
    the next JSON body the mocked session will return.
    """
    captured: list[tuple[str, dict]] = []
    payload_ref: dict = {"payload": []}

    async def _json():
        return payload_ref["payload"]

    resp = MagicMock()
    resp.status = 200
    resp.json = _json
    resp.text = AsyncMock(return_value="")

    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=resp)
    cm.__aexit__ = AsyncMock(return_value=False)

    def _get(url, params=None):  # noqa: ANN001
        captured.append((url, dict(params or {})))
        return cm

    session = MagicMock()
    session.get = MagicMock(side_effect=_get)

    def _set_payload(payload):
        payload_ref["payload"] = payload

    patcher = patch.object(source, "_get_session", new_callable=AsyncMock, return_value=session)
    return captured, _set_payload, patcher


@pytest.mark.asyncio
async def test_single_instance_serves_arbitrum_and_base_in_one_process():
    """One DexScreener instance with no default chain dispatches per-call
    to both arbitrum and base in the same process. Cache must be
    partitioned by chain so no cross-chain pollution leaks through."""
    # Multi-chain source: no default chain; dispatch is entirely per-call.
    source = DexScreenerPriceSource(cache_ttl=30, min_liquidity_usd=10_000)

    # Same address, different chains — classic collision scenario.
    same_address = "0x1234567890aBcDeF1234567890AbCdEf12345678"

    arb_token = ResolvedToken(
        symbol="FOO",
        address=same_address,
        decimals=18,
        chain="arbitrum",
        chain_id=42161,
        source="test",
        is_verified=False,
    )
    base_token = ResolvedToken(
        symbol="FOO",
        address=same_address,
        decimals=18,
        chain="base",
        chain_id=8453,
        source="test",
        is_verified=False,
    )

    captured, set_payload, patcher = _mock_session(source)

    with patcher:
        # Request 1: arbitrum → different price per chain so we can prove
        # the dispatch actually chose the right chain.
        set_payload(_pair_json("1.00", chain_id="arbitrum"))
        arb_result = await source.get_price(same_address, "USD", resolved_token=arb_token)

        # Request 2: base → distinct price
        set_payload(_pair_json("9.99", chain_id="base"))
        base_result = await source.get_price(same_address, "USD", resolved_token=base_token)

    # Per-chain prices preserved → no cache collision.
    assert arb_result.price == Decimal("1.00")
    assert base_result.price == Decimal("9.99")

    # URLs carry the chain-scoped platform slug. This is the real
    # correctness proof — per-call dispatch actually happened.
    # ``_fetch_token_pairs`` forwards the address as-given (case preserved),
    # so match on the path prefix (chain slug) rather than a lowercased copy.
    assert any("/token-pairs/v1/arbitrum/" in url for url, _ in captured)
    assert any("/token-pairs/v1/base/" in url for url, _ in captured)

    # Cache must be chain-scoped. If someone collapsed the cache key back
    # to address-only, a second arb request would return 9.99 (the base price).
    set_payload(_pair_json("77.77", chain_id="arbitrum"))  # would be used if cache missed
    with patcher:
        cached_arb = await source.get_price(same_address, "USD", resolved_token=arb_token)
    assert cached_arb.price == Decimal("1.00"), "arbitrum cache was polluted by base"


@pytest.mark.asyncio
async def test_unsupported_chain_raises_chain_unsupported_skip():
    """A chain with no DexScreener platform mapping must raise
    ``DataSourceUnavailable(reason="chain_unsupported:...")`` — NOT
    ``ValueError`` like the old constructor-locked code did. The
    aggregator treats this as a non-error skip.
    """
    source = DexScreenerPriceSource(cache_ttl=30)

    fake_chain = MagicMock()
    fake_chain.value = "no-such-chain"
    fake_token = MagicMock()
    fake_token.address = "0x1234567890aBcDeF1234567890AbCdEf12345678"
    fake_token.chain = fake_chain

    with pytest.raises(DataSourceUnavailable) as exc_info:
        await source.get_price("FOO", "USD", resolved_token=fake_token)

    # The reason must carry the "chain_unsupported:<chain>" prefix so the
    # aggregator (and humans reading logs) can distinguish this from a
    # genuine source failure.
    assert "chain_unsupported:no-such-chain" in str(exc_info.value)


def _resolved(address: str, chain: str, symbol: str) -> MagicMock:
    chain_mock = MagicMock()
    chain_mock.value = chain
    token = MagicMock()
    token.address = address
    token.chain = chain_mock
    token.symbol = symbol
    return token


_BASE_USDC = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
_BASE_AERO = "0x940181a94A35A4569E4529A3CDfB74e38FD98631"
_BASE_USDBC = "0xd9aAEc86B65D86f6A7B5B1b0c42FFA531710b6CA"


@pytest.mark.asyncio
async def test_orientation_prefers_base_side_pair_over_deeper_quote_side():
    """ALM-3147 negative control (Base USDC, 2026-08-31): the deepest Base
    pool has USDC as the QUOTE token, so liquidity-only selection returned
    AERO's $0.48 as USDC's price. A base-side pair must win regardless of
    liquidity. Fails on the pre-orientation code."""
    source = DexScreenerPriceSource(cache_ttl=30)
    usdc = _resolved(_BASE_USDC, "base", "USDC")

    _captured, set_payload, patcher = _mock_session(source)
    set_payload(
        [
            _pair(
                "0.48",
                chain_id="base",
                liquidity_usd=28_000_000,
                base_address=_BASE_AERO,
                base_symbol="AERO",
                quote_address=_BASE_USDC,
                quote_symbol="USDC",
                price_native="0.48",
            ),
            _pair(
                "1.0001",
                chain_id="base",
                liquidity_usd=180_000,
                base_address=_BASE_USDC,
                base_symbol="USDC",
                quote_address=_BASE_USDBC,
                quote_symbol="USDbC",
            ),
        ]
    )

    with patcher:
        result = await source.get_price(_BASE_USDC, "USD", resolved_token=usdc)

    assert result.price == Decimal("1.0001"), (
        f"Base-side pair must win over a deeper quote-side pair — returning the "
        f"quote-side priceUsd is AERO's price, not USDC's. Got: {result.price}"
    )


@pytest.mark.asyncio
async def test_orientation_inverts_quote_side_when_no_base_side_pair():
    """When the requested token appears ONLY as the quote leg, the price is
    recovered by inverting ``priceUsd / priceNative`` — never by returning
    the base token's price. AERO/USDC with AERO at $0.48 and priceNative
    0.48 USDC-per-AERO implies USDC = $1.00."""
    source = DexScreenerPriceSource(cache_ttl=30)
    usdc = _resolved(_BASE_USDC, "base", "USDC")

    _captured, set_payload, patcher = _mock_session(source)
    set_payload(
        [
            _pair(
                "0.48",
                chain_id="base",
                liquidity_usd=28_000_000,
                base_address=_BASE_AERO,
                base_symbol="AERO",
                quote_address=_BASE_USDC,
                quote_symbol="USDC",
                price_native="0.48",
            ),
        ]
    )

    with patcher:
        result = await source.get_price(_BASE_USDC, "USD", resolved_token=usdc)

    assert result.price == Decimal("1"), (
        f"Quote-side-only token must be priced via priceUsd/priceNative inversion. Got: {result.price}"
    )


@pytest.mark.asyncio
async def test_orientation_quote_side_with_unusable_price_native_raises_skip():
    """A quote-side pair whose ``priceNative`` is missing/zero cannot be
    inverted — the source must opt out with DataSourceUnavailable rather
    than fall back to the base token's price."""
    source = DexScreenerPriceSource(cache_ttl=30)
    usdc = _resolved(_BASE_USDC, "base", "USDC")

    _captured, set_payload, patcher = _mock_session(source)
    set_payload(
        [
            _pair(
                "0.48",
                chain_id="base",
                liquidity_usd=28_000_000,
                base_address=_BASE_AERO,
                base_symbol="AERO",
                quote_address=_BASE_USDC,
                quote_symbol="USDC",
                price_native="0",
            ),
        ]
    )

    with patcher:
        with pytest.raises(DataSourceUnavailable, match="No liquid pair"):
            await source.get_price(_BASE_USDC, "USD", resolved_token=usdc)


@pytest.mark.asyncio
async def test_wsteth_ethereum_prices_via_base_side_pair_without_quarantine():
    """The VIB-4439 wstETH incident ($97 vs ~$3500) was this same bug:
    the deepest pool is AAVE/wstETH. The per-token quarantine is deleted;
    the orientation check must pick the base-side wstETH/WETH pair."""
    source = DexScreenerPriceSource(cache_ttl=30)
    wsteth_addr = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
    wsteth = _resolved(wsteth_addr, "ethereum", "WSTETH")

    _captured, set_payload, patcher = _mock_session(source)
    set_payload(
        [
            _pair(
                "122.88",  # AAVE's price — the pre-fix corruption
                chain_id="ethereum",
                liquidity_usd=10_336_070,
                base_address="0x7Fc66500c84A76Ad7e9c93437bFc5Ac33E2DDaE9",
                base_symbol="AAVE",
                quote_address=wsteth_addr,
                quote_symbol="wstETH",
                price_native="0.04058",
            ),
            _pair(
                "3039.32",
                chain_id="ethereum",
                liquidity_usd=6_064_730,
                base_address=wsteth_addr,
                base_symbol="wstETH",
                quote_address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                quote_symbol="WETH",
            ),
        ]
    )

    with patcher:
        result = await source.get_price("WSTETH", "USD", resolved_token=wsteth)

    assert result.price == Decimal("3039.32"), (
        f"wstETH must price from its base-side pair, not the deeper AAVE/wstETH "
        f"pool's base price. Got: {result.price}"
    )


@pytest.mark.asyncio
async def test_symbol_search_requires_symbol_match():
    """The symbol-search fallback (no address available) returns pairs for
    whatever the free-text query matched. A pair whose base AND quote are
    both other tokens must be discarded instead of priced."""
    source = DexScreenerPriceSource(default_chain_id="base", cache_ttl=30)

    _captured, set_payload, patcher = _mock_session(source)
    # The search endpoint wraps results in {"pairs": [...]} (unlike the
    # token-pairs endpoint, which returns a bare list).
    set_payload(
        {
            "pairs": [
                _pair(
                    "5.23",
                    chain_id="base",
                    base_symbol="SOMETOKEN",
                    quote_symbol="WETH",
                ),
            ]
        }
    )

    with patcher:
        with pytest.raises(DataSourceUnavailable, match="No liquid pair"):
            await source.get_price("ZZZUNKNOWN", "USD", resolved_token=None)

    # Same payload, but the base side actually IS the requested symbol.
    set_payload(
        {
            "pairs": [
                _pair(
                    "5.23",
                    chain_id="base",
                    base_symbol="ZZZUNKNOWN",
                    quote_symbol="WETH",
                ),
            ]
        }
    )
    with patcher:
        result = await source.get_price("ZZZUNKNOWN", "USD", resolved_token=None)
    assert result.price == Decimal("5.23")


@pytest.mark.asyncio
async def test_no_chain_context_raises_skip():
    """No default chain AND no resolved_token → skip with a specific reason."""
    source = DexScreenerPriceSource(cache_ttl=30)

    with pytest.raises(DataSourceUnavailable) as exc_info:
        await source.get_price("SOL", "USD", resolved_token=None)

    assert "no_chain_context" in str(exc_info.value)


@pytest.mark.asyncio
async def test_default_chain_preserves_legacy_behavior():
    """Passing ``default_chain_id`` restores single-chain mode — the ctor
    defaults every non-ResolvedToken call to that chain."""
    source = DexScreenerPriceSource(default_chain_id="solana", cache_ttl=30)

    captured, set_payload, patcher = _mock_session(source)
    set_payload(
        _pair_json(
            "84.50",
            chain_id="solana",
            base_address="So11111111111111111111111111111111111111112",
            base_symbol="SOL",
        )
    )

    with patcher:
        # No resolved_token → fall back to the default chain.
        result = await source.get_price("SOL", "USD", resolved_token=None)

    assert result.price == Decimal("84.50")
    assert any("/token-pairs/v1/solana/" in url for url, _ in captured)


@pytest.mark.asyncio
async def test_resolved_token_chain_overrides_default_chain_id():
    """Production path: the source is constructed with a default chain
    (legacy single-chain wiring) AND a ResolvedToken is passed in with a
    DIFFERENT chain. The ResolvedToken must win — otherwise multi-chain
    callers threading chain via ``resolved_token.chain`` would silently
    price on the wrong chain. This is the exact failure mode the other
    tests here would miss if the precedence was reversed."""
    # Default is solana — but the request carries an Ethereum ResolvedToken.
    source = DexScreenerPriceSource(default_chain_id="solana", cache_ttl=30)

    captured, set_payload, patcher = _mock_session(source)
    set_payload(
        _pair_json(
            "3200.00",
            chain_id="ethereum",
            base_address="0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            base_symbol="WETH",
        )
    )

    eth_chain = MagicMock()
    eth_chain.value = "ethereum"
    eth_token = MagicMock()
    eth_token.address = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"  # WETH
    eth_token.chain = eth_chain

    with patcher:
        result = await source.get_price("WETH", "USD", resolved_token=eth_token)

    # Price is the ethereum payload, and the URL is the ethereum platform
    # path — NOT the solana default.
    assert result.price == Decimal("3200.00")
    assert any("/token-pairs/v1/ethereum/" in url for url, _ in captured)
    assert not any("/token-pairs/v1/solana/" in url for url, _ in captured)


@pytest.mark.asyncio
async def test_legacy_chain_id_kwarg_still_accepted():
    """Backward compatibility: the old ``chain_id`` kwarg must still work
    so in-flight callers aren't broken during the migration window."""
    source = DexScreenerPriceSource(chain_id="solana", cache_ttl=30)
    assert source._default_chain_name == "solana"
    assert source._default_platform == "solana"


@pytest.mark.asyncio
async def test_aggregator_treats_chain_unsupported_as_skip_not_failure():
    """The whole point of the ``chain_unsupported`` marker: the aggregator
    records it in ``sources_failed`` but DOES NOT crash — other sources
    continue serving the request. This test drives it through the real
    PriceAggregator path."""
    from almanak.framework.data.interfaces import PriceResult
    from almanak.gateway.data.price.aggregator import PriceAggregator

    dexscreener_source = DexScreenerPriceSource(cache_ttl=30)

    # Stub a second source that succeeds — so the aggregator has a valid
    # result to return while DexScreener cleanly opts out.
    class _StubSource:
        source_name = "stub"
        cache_ttl_seconds = 30
        supported_tokens: list[str] = []

        async def get_price(self, token, quote="USD", *, resolved_token=None):  # noqa: ANN001, ANN201
            from datetime import UTC, datetime

            return PriceResult(
                price=Decimal("42.0"),
                source="stub",
                timestamp=datetime.now(UTC),
                confidence=1.0,
                stale=False,
            )

        async def health_check(self) -> bool:
            return True

        async def close(self) -> None:
            return None

    aggregator = PriceAggregator(sources=[dexscreener_source, _StubSource()])

    fake_chain = MagicMock()
    fake_chain.value = "no-such-chain"
    fake_token = MagicMock()
    fake_token.address = "0x1234567890aBcDeF1234567890AbCdEf12345678"
    fake_token.chain = fake_chain

    result = await aggregator.get_aggregated_price("FOO", "USD", resolved_token=fake_token)

    # Aggregator returned the stub's price — DexScreener's skip did NOT
    # propagate as a failure that kills the request.
    assert result.price == Decimal("42.0")
    # Diagnostics show DexScreener failed with chain_unsupported, proving
    # the marker was recorded (not silently eaten).
    details = aggregator.get_last_details("FOO", "USD")
    assert "dexscreener" in details["sources_failed"]
    assert "chain_unsupported" in details["sources_failed"]["dexscreener"]


def test_double_chain_kwarg_raises() -> None:
    """Passing both ``default_chain_id`` and legacy ``chain_id`` is caller
    misuse (most often a half-done migration). Fail loud so the bug surfaces
    at construction, not inside a mispriced request later.
    """
    with pytest.raises(ValueError, match="not both"):
        DexScreenerPriceSource(default_chain_id="arbitrum", chain_id="base")


def test_bnb_alias_canonicalized_to_bsc_on_ctor() -> None:
    """``default_chain_id="bnb"`` must canonicalize to ``"bsc"`` so that
    internal lookups (_KNOWN_TOKEN_ADDRESSES, TokenResolver, cache keys)
    use the canonical chain name and not the alias. Storing the alias
    directly would push same-chain requests onto different cache keys and
    miss resolver entries that are keyed by canonical names.
    """
    source = DexScreenerPriceSource(default_chain_id="bnb")
    assert source._default_chain_name == "bsc"
    assert source._default_platform == "bsc"


def test_bnb_alias_canonicalized_from_resolved_token(monkeypatch: pytest.MonkeyPatch) -> None:
    """Same canonicalization must apply when the chain comes from
    ``resolved_token.chain`` so a caller that supplies a non-canonical (or
    the string "bnb") via the ResolvedToken path cannot bypass the
    canonicalization done at construction time.
    """
    source = DexScreenerPriceSource(cache_ttl=30)

    fake_chain = MagicMock()
    fake_chain.value = "bnb"
    fake_token = MagicMock()
    fake_token.address = "0xabc"
    fake_token.chain = fake_chain

    chain_name, platform = source._resolve_chain_for_call(fake_token)
    assert chain_name == "bsc"
    assert platform == "bsc"


@pytest.mark.asyncio
async def test_health_check_multichain_probes_search_endpoint() -> None:
    """In multi-chain mode (no default chain) the health check must actually
    probe DexScreener — returning True without any network call would mask
    real outages from hosted readiness probes. We probe the chain-agnostic
    ``/latest/dex/search`` endpoint so liveness is asserted regardless of
    which chain a later request targets.
    """
    source = DexScreenerPriceSource(cache_ttl=30)
    assert source._default_chain_name is None

    # Stub the search method directly so we don't hit the real network.
    # Empty list -> unhealthy; populated list -> healthy.
    with patch.object(source, "_search_pairs", new=AsyncMock(return_value=[{"chainId": "ethereum"}])):
        with patch.object(source, "_get_session", new=AsyncMock(return_value=MagicMock())):
            assert await source.health_check() is True

    with patch.object(source, "_search_pairs", new=AsyncMock(return_value=[])):
        with patch.object(source, "_get_session", new=AsyncMock(return_value=MagicMock())):
            assert await source.health_check() is False

    with patch.object(source, "_search_pairs", new=AsyncMock(side_effect=RuntimeError("api down"))):
        with patch.object(source, "_get_session", new=AsyncMock(return_value=MagicMock())):
            assert await source.health_check() is False
