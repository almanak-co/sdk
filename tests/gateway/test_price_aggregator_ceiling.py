"""Tests for PriceAggregator single-source price ceiling.

Verifies that wildly incorrect single-source prices (e.g., $12B from a
misconfigured Chainlink feed) are rejected instead of being passed through
to strategies.
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceUnavailable,
    PriceResult,
)
from almanak.gateway.data.price.aggregator import (
    AllDataSourcesFailed,
    PriceAggregator,
)


class MockPriceSource(BasePriceSource):
    """Mock price source that returns a configurable price."""

    def __init__(self, name: str, price: Decimal | None = None, error: str | None = None):
        self._name = name
        self._price = price
        self._error = error

    @property
    def source_name(self) -> str:
        return self._name

    async def get_price(self, token: str, quote: str = "USD", **kwargs) -> PriceResult:
        if self._error:
            raise DataSourceUnavailable(source=self._name, reason=self._error)
        return PriceResult(
            price=self._price,
            source=self._name,
            timestamp=datetime.now(UTC),
            confidence=0.95,
        )

    async def close(self) -> None:
        pass


class TestSingleSourcePriceCeiling:
    """Tests for the single-source absolute price ceiling."""

    @pytest.mark.asyncio
    async def test_normal_price_passes_through(self):
        """A reasonable single-source price should be accepted."""
        source = MockPriceSource("chainlink", price=Decimal("2500"))
        aggregator = PriceAggregator(sources=[source])

        result = await aggregator.get_aggregated_price("WETH")

        assert result.price == Decimal("2500")
        assert result.confidence > 0

    @pytest.mark.asyncio
    async def test_extreme_price_rejected(self):
        """A single-source price above $10M should be rejected."""
        # Simulates the wstETH/ETH feed decoded as USD: ~$12.28B
        source = MockPriceSource("chainlink", price=Decimal("12285333765"))
        aggregator = PriceAggregator(sources=[source])

        with pytest.raises(AllDataSourcesFailed):
            await aggregator.get_aggregated_price("WSTETH")

    @pytest.mark.asyncio
    async def test_extreme_price_with_failed_second_source(self):
        """When one source returns $12B and the other fails, reject the price."""
        chainlink = MockPriceSource("chainlink", price=Decimal("12285333765"))
        coingecko = MockPriceSource("coingecko", error="rate limited")
        aggregator = PriceAggregator(sources=[chainlink, coingecko])

        with pytest.raises(AllDataSourcesFailed):
            await aggregator.get_aggregated_price("WSTETH")

    @pytest.mark.asyncio
    async def test_btc_high_price_passes(self):
        """BTC at $500K (extreme but plausible) should pass the ceiling."""
        source = MockPriceSource("chainlink", price=Decimal("500000"))
        aggregator = PriceAggregator(sources=[source])

        result = await aggregator.get_aggregated_price("BTC")

        assert result.price == Decimal("500000")

    @pytest.mark.asyncio
    async def test_multi_source_agreement_at_high_price_passes(self):
        """When two sources agree on a high price, it should pass.

        The ceiling only applies to single-source results. Multi-source
        agreement uses the standard median/outlier logic.
        """
        source1 = MockPriceSource("chainlink", price=Decimal("50000000"))
        source2 = MockPriceSource("coingecko", price=Decimal("50000000"))
        aggregator = PriceAggregator(sources=[source1, source2])

        result = await aggregator.get_aggregated_price("SOME_TOKEN")

        assert result.price == Decimal("50000000")


class TestStablecoinFallback:
    """Tests for the ``$1.00`` fallback when all upstream sources fail.

    Address-keyed fallback was added to disambiguate the ``PUSD`` symbol
    (Polymarket vs. ``Pleasing USD`` / ``Palm USD`` / ``Plume USD``); the
    legacy symbol-keyed allowlist is preserved for unambiguous symbols only.
    """

    @pytest.mark.asyncio
    async def test_pusd_with_polymarket_address_falls_back_to_one_dollar(self) -> None:
        """Polymarket pUSD (resolved to its on-chain address) gets $1.00 when
        all sources fail. This is the original PR's intent: pUSD has no
        Chainlink / CoinGecko listing, so the fallback IS the price source."""
        from almanak.framework.data.tokens.models import BridgeType, ResolvedToken
        from almanak.gateway.data.price.aggregator import PriceAggregator

        from almanak import Chain

        sources = [
            MockPriceSource("chainlink", error="no feed"),
            MockPriceSource("coingecko", error="404 not listed"),
        ]
        aggregator = PriceAggregator(sources=sources)

        polymarket_pusd = ResolvedToken(
            symbol="PUSD",
            address="0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
            decimals=6,
            chain=Chain.POLYGON,
            chain_id=137,
            name="Polymarket USD",
            coingecko_id="",
            is_stablecoin=True,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol="USDC",
            bridge_type=BridgeType.NATIVE,
            source="static",
        )
        result = await aggregator.get_aggregated_price("PUSD", "USD", resolved_token=polymarket_pusd)

        assert result.price == Decimal("1.00")
        assert result.source == "stablecoin_fallback"

    @pytest.mark.asyncio
    async def test_pusd_with_other_address_does_not_fall_back(self) -> None:
        """A non-Polymarket token whose symbol happens to be ``PUSD`` (e.g.
        ``Pleasing USD`` on a different chain) must NOT get the $1.00
        fallback when sources fail — that's the security regression Codex
        flagged for the symbol-only allowlist."""
        from almanak.framework.data.tokens.models import BridgeType, ResolvedToken
        from almanak.gateway.data.price.aggregator import PriceAggregator

        from almanak import Chain

        sources = [
            MockPriceSource("chainlink", error="no feed"),
            MockPriceSource("coingecko", error="404 not listed"),
        ]
        aggregator = PriceAggregator(sources=sources)

        pleasing_pusd = ResolvedToken(
            symbol="PUSD",
            address="0x1111111111111111111111111111111111111111",
            decimals=18,
            chain=Chain.ETHEREUM,
            chain_id=1,
            name="Pleasing USD",
            coingecko_id="",
            is_stablecoin=False,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol="PUSD",
            bridge_type=BridgeType.NATIVE,
            source="static",
        )
        with pytest.raises(AllDataSourcesFailed):
            await aggregator.get_aggregated_price("PUSD", "USD", resolved_token=pleasing_pusd)

    @pytest.mark.asyncio
    async def test_pusd_without_resolved_token_does_not_fall_back(self) -> None:
        """Without ``resolved_token`` the address-keyed allowlist can't tell
        which PUSD this is, so it falls through to the symbol allowlist —
        which no longer contains PUSD. Result: no fallback. Callers wanting
        the Polymarket pUSD price must pass ``resolved_token``."""
        from almanak.gateway.data.price.aggregator import PriceAggregator

        sources = [
            MockPriceSource("chainlink", error="no feed"),
            MockPriceSource("coingecko", error="no feed"),
        ]
        aggregator = PriceAggregator(sources=sources)

        with pytest.raises(AllDataSourcesFailed):
            await aggregator.get_aggregated_price("PUSD", "USD")

    @pytest.mark.asyncio
    async def test_usdc_without_resolved_token_still_falls_back(self) -> None:
        """USDC's symbol IS unambiguous across the EVM token set, so it
        remains in the symbol-keyed allowlist and the legacy bare-symbol
        path keeps working with no resolved_token."""
        from almanak.gateway.data.price.aggregator import PriceAggregator

        sources = [
            MockPriceSource("chainlink", error="rate limited"),
            MockPriceSource("coingecko", error="rate limited"),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = await aggregator.get_aggregated_price("USDC", "USD")

        assert result.price == Decimal("1.00")
        assert result.source == "stablecoin_fallback"

    @pytest.mark.asyncio
    async def test_non_usd_quote_does_not_use_fallback(self) -> None:
        """The fallback only kicks in for ``USD`` quotes — pricing PUSD in
        EUR with no working source must surface as ``AllDataSourcesFailed``,
        not silently return $1.00 (which isn't even the right denomination)."""
        from almanak.framework.data.tokens.models import BridgeType, ResolvedToken
        from almanak.gateway.data.price.aggregator import PriceAggregator

        from almanak import Chain

        sources = [MockPriceSource("chainlink", error="no feed")]
        aggregator = PriceAggregator(sources=sources)

        polymarket_pusd = ResolvedToken(
            symbol="PUSD",
            address="0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB",
            decimals=6,
            chain=Chain.POLYGON,
            chain_id=137,
            name="Polymarket USD",
            coingecko_id="",
            is_stablecoin=True,
            is_native=False,
            is_wrapped_native=False,
            canonical_symbol="USDC",
            bridge_type=BridgeType.NATIVE,
            source="static",
        )
        with pytest.raises(AllDataSourcesFailed):
            await aggregator.get_aggregated_price("PUSD", "EUR", resolved_token=polymarket_pusd)


class TestChainlinkFeedConfig:
    """Tests for Chainlink feed configuration correctness."""

    def test_base_wsteth_not_in_usd_feeds(self):
        """Base should NOT have a WSTETH/USD entry (it's an ETH-denominated feed)."""
        from almanak.core.chainlink import BASE_PRICE_FEEDS

        assert "WSTETH/USD" not in BASE_PRICE_FEEDS

    def test_base_wsteth_in_eth_denominated_feeds(self):
        """Base should have WSTETH/ETH in the ETH-denominated feeds."""
        from almanak.core.chainlink import ETH_DENOMINATED_FEEDS

        assert "base" in ETH_DENOMINATED_FEEDS
        assert "WSTETH/ETH" in ETH_DENOMINATED_FEEDS["base"]
        assert ETH_DENOMINATED_FEEDS["base"]["WSTETH/ETH"] == "0x43a5C292A453A3bF3606fa856197f09D7B74251a"

    def test_arbitrum_wsteth_in_eth_denominated_feeds(self):
        """Arbitrum should also use ETH-denominated wstETH feed."""
        from almanak.core.chainlink import ETH_DENOMINATED_FEEDS

        assert "WSTETH/ETH" in ETH_DENOMINATED_FEEDS["arbitrum"]

    def test_ethereum_wsteth_in_eth_denominated_feeds(self):
        """VIB-4439 F1 (B1): Ethereum has a direct WSTETH/USD feed but the
        derived path (WSTETH/ETH * ETH/USD) is an independent Chainlink
        oracle that does NOT depend on the WSTETH/USD aggregator. Adding it
        gives the OnChain source a second working price for wstETH on
        Ethereum even when the WSTETH/USD direct feed is unavailable, so
        the multi-source consensus has enough good data to remain robust
        without DexScreener.
        """
        from almanak.core.chainlink import ETH_DENOMINATED_FEEDS

        assert "ethereum" in ETH_DENOMINATED_FEEDS
        assert "WSTETH/ETH" in ETH_DENOMINATED_FEEDS["ethereum"]
        # Canonical Chainlink WSTETH/ETH price feed on Ethereum mainnet
        # (18 decimals — `_FEED_DECIMALS["WSTETH/ETH"]` in
        # `gateway/data/price/onchain.py:57` handles the decimal scaling).
        assert ETH_DENOMINATED_FEEDS["ethereum"]["WSTETH/ETH"] == "0x86392dC19c0b719886221c78AB11eb8Cf5c52812"

    def test_ethereum_wsteth_keeps_direct_usd_feed_too(self):
        """The B1 derived feed is ADDITIVE — Ethereum keeps the direct
        WSTETH/USD entry so the OnChain source has both paths and median
        consensus is computed from independent Chainlink aggregators."""
        from almanak.core.chainlink import ETHEREUM_PRICE_FEEDS

        assert "WSTETH/USD" in ETHEREUM_PRICE_FEEDS, (
            "Ethereum WSTETH/USD direct feed must remain. B1 added the "
            "ETH-denominated derived path as a SECOND independent oracle, "
            "not a replacement."
        )
