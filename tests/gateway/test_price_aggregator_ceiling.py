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
        assert ETH_DENOMINATED_FEEDS["base"]["WSTETH/ETH"] == "0x43a5C292A453A3bF3606fa856197F09D7B74251a"

    def test_arbitrum_wsteth_in_eth_denominated_feeds(self):
        """Arbitrum should also use ETH-denominated wstETH feed."""
        from almanak.core.chainlink import ETH_DENOMINATED_FEEDS

        assert "WSTETH/ETH" in ETH_DENOMINATED_FEEDS["arbitrum"]
