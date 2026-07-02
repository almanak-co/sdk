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


def _pair_json(price: str, chain_id: str, liquidity_usd: float = 5_000_000) -> list[dict]:
    return [
        {
            "chainId": chain_id,
            "priceUsd": price,
            "liquidity": {"usd": liquidity_usd},
            "volume": {"h24": 1_000_000},
        }
    ]


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


@pytest.mark.asyncio
async def test_lst_quarantine_wsteth_ethereum_raises_skip():
    """VIB-4439 F1 (B2): WSTETH on Ethereum is in the LST quarantine list.

    DexScreener's wstETH/USD price on Ethereum is structurally unreliable
    (the dominant pool is wstETH/WETH, no direct USD liquidity), and the
    fixture run on 2026-05-15 observed DexScreener returning $97.31 vs the
    Chainlink truth ~$3500. The quarantine raises
    ``DataSourceUnavailable(reason="quarantined_lst_token:WSTETH:ethereum")``
    so the aggregator skips this source on this specific token+chain and
    consensus on the working oracles (Chainlink direct + Chainlink derived
    + CoinGecko).
    """
    source = DexScreenerPriceSource(cache_ttl=30)

    eth_chain = MagicMock()
    eth_chain.value = "ethereum"
    wsteth_token = MagicMock()
    wsteth_token.address = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"  # mainnet wstETH
    wsteth_token.chain = eth_chain
    wsteth_token.symbol = "WSTETH"

    with pytest.raises(DataSourceUnavailable) as exc_info:
        await source.get_price("WSTETH", "USD", resolved_token=wsteth_token)

    assert "quarantined_lst_token:WSTETH:ethereum" in str(exc_info.value), (
        f"DexScreener must raise quarantined_lst_token for WSTETH on Ethereum. "
        f"Got: {exc_info.value!r}"
    )


@pytest.mark.asyncio
async def test_lst_quarantine_matches_resolved_symbol_not_raw_token() -> None:
    """CodeRabbit on PR #2323: the quarantine must match against the
    ``resolved_token.symbol`` (not the raw ``token`` argument) so an
    address-based call ("0x7f39..." rather than "WSTETH") cannot bypass it.
    This is the typical path from the aggregator's address-based lookup.
    """
    source = DexScreenerPriceSource(cache_ttl=30)

    eth_chain = MagicMock()
    eth_chain.value = "ethereum"
    wsteth_token = MagicMock()
    wsteth_token.address = "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
    wsteth_token.chain = eth_chain
    wsteth_token.symbol = "WSTETH"

    # Call with the ADDRESS as the ``token`` arg — the path that previously
    # slipped past the quarantine when the check used ``token.upper()``.
    with pytest.raises(DataSourceUnavailable) as exc_info:
        await source.get_price(wsteth_token.address, "USD", resolved_token=wsteth_token)

    assert "quarantined_lst_token:WSTETH:ethereum" in str(exc_info.value), (
        f"Quarantine must match resolved_token.symbol even when token arg is "
        f"an address. Got: {exc_info.value!r}"
    )


@pytest.mark.asyncio
async def test_lst_quarantine_does_not_apply_to_other_chains():
    """The quarantine is per-(token, chain). The same wstETH symbol on a
    different chain is NOT quarantined — DexScreener may have a working
    USD pool there. Today only the WSTETH+ethereum pair is quarantined."""
    from unittest.mock import patch

    source = DexScreenerPriceSource(cache_ttl=30)

    op_chain = MagicMock()
    op_chain.value = "optimism"
    wsteth_op = MagicMock()
    wsteth_op.address = "0x1F32b1c2345538c0c6f582fCB022739c4A194Ebb"  # OP wstETH
    wsteth_op.chain = op_chain
    wsteth_op.symbol = "WSTETH"

    # Patch _fetch_price so the test does not hit the real DexScreener API.
    # The quarantine check fires BEFORE _fetch_price; we just need to prove
    # the call reaches _fetch_price (i.e., the quarantine did NOT trip).
    with patch.object(
        source,
        "_fetch_price",
        side_effect=DataSourceUnavailable(source="dexscreener", reason="mocked_no_data"),
    ):
        with pytest.raises(DataSourceUnavailable) as exc_info:
            await source.get_price("WSTETH", "USD", resolved_token=wsteth_op)

    # Must reach the patched _fetch_price (mocked_no_data) — quarantine
    # must not short-circuit for chains other than ethereum.
    assert "mocked_no_data" in str(exc_info.value), (
        f"Quarantine should NOT apply to wstETH on optimism — get_price must "
        f"reach _fetch_price. Got: {exc_info.value!r}"
    )


@pytest.mark.asyncio
async def test_lst_quarantine_does_not_apply_to_non_lst_tokens_on_ethereum():
    """The quarantine is per-(token, chain). Tokens other than WSTETH on
    Ethereum are NOT quarantined — DexScreener is the correct source for
    most ERC-20s with real USD liquidity."""
    from unittest.mock import patch

    source = DexScreenerPriceSource(cache_ttl=30)

    eth_chain = MagicMock()
    eth_chain.value = "ethereum"
    usdc_token = MagicMock()
    usdc_token.address = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
    usdc_token.chain = eth_chain
    usdc_token.symbol = "USDC"

    with patch.object(
        source,
        "_fetch_price",
        side_effect=DataSourceUnavailable(source="dexscreener", reason="mocked_no_data"),
    ):
        with pytest.raises(DataSourceUnavailable) as exc_info:
            await source.get_price("USDC", "USD", resolved_token=usdc_token)

    assert "mocked_no_data" in str(exc_info.value), (
        f"Quarantine should NOT apply to USDC on ethereum — get_price must "
        f"reach _fetch_price. Got: {exc_info.value!r}"
    )


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
    set_payload(_pair_json("84.50", chain_id="solana"))

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
    set_payload(_pair_json("3200.00", chain_id="ethereum"))

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
