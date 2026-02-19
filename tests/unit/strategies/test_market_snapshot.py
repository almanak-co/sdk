"""Tests for MarketSnapshot.set_price_data() (VIB-121)."""

from decimal import Decimal

from almanak.framework.strategies.intent_strategy import MarketSnapshot, MultiChainMarketSnapshot, PriceData


class TestMarketSnapshotSetPriceData:
    """MarketSnapshot.set_price_data() should populate price_data() and price()."""

    def test_set_price_data_roundtrip(self):
        """set_price_data() data should be retrievable via price_data()."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        pd = PriceData(
            price=Decimal("3000"),
            price_24h_ago=Decimal("2900"),
            change_24h_pct=Decimal("3.45"),
            high_24h=Decimal("3050"),
            low_24h=Decimal("2850"),
        )
        market.set_price_data("ETH", pd)

        result = market.price_data("ETH")
        assert result.price == Decimal("3000")
        assert result.change_24h_pct == Decimal("3.45")
        assert result.high_24h == Decimal("3050")
        assert result.low_24h == Decimal("2850")
        assert result.price_24h_ago == Decimal("2900")

    def test_set_price_data_readable_via_price(self):
        """price() should also return the price from set_price_data()."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        pd = PriceData(price=Decimal("1850.50"))
        market.set_price_data("ETH", pd)

        assert market.price("ETH") == Decimal("1850.50")

    def test_set_price_data_custom_quote(self):
        """set_price_data() with non-default quote currency."""
        market = MarketSnapshot(chain="ethereum", wallet_address="0xtest")
        pd = PriceData(price=Decimal("0.00033"))
        market.set_price_data("ETH", pd, quote="BTC")

        result = market.price_data("ETH", quote="BTC")
        assert result.price == Decimal("0.00033")

    def test_set_price_data_does_not_affect_other_tokens(self):
        """Setting price data for one token should not affect another."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price_data("ETH", PriceData(price=Decimal("3000")))

        # USDC should still raise ValueError since no data is set
        try:
            market.price("USDC")
            assert False, "Should have raised ValueError"
        except ValueError:
            pass

    def test_set_price_overridden_by_set_price_data(self):
        """set_price_data() should take precedence for price_data() calls."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        # set_price populates _prices (checked first by price())
        market.set_price("ETH", Decimal("2800"))
        # set_price_data populates _price_cache (checked first by price_data())
        market.set_price_data("ETH", PriceData(price=Decimal("3000"), change_24h_pct=Decimal("5.0")))

        # price_data() should return the richer PriceData
        pd = market.price_data("ETH")
        assert pd.price == Decimal("3000")
        assert pd.change_24h_pct == Decimal("5.0")


class TestMultiChainMarketSnapshotSetPriceData:
    """MultiChainMarketSnapshot.set_price_data() for multi-chain scenarios."""

    def test_set_price_data_roundtrip(self):
        """set_price_data() should populate the price cache for the chain."""
        market = MultiChainMarketSnapshot(
            chains=["arbitrum", "ethereum"],
            wallet_address="0xtest",
        )
        pd = PriceData(
            price=Decimal("3000"),
            change_24h_pct=Decimal("2.5"),
        )
        market.set_price_data("ETH", "arbitrum", pd)

        # price() should return the value from the cache
        assert market.price("ETH", chain="arbitrum") == Decimal("3000")
        # The full PriceData should be in the internal cache
        assert market._price_cache["arbitrum"]["ETH/USD"].change_24h_pct == Decimal("2.5")

    def test_set_price_data_chain_isolation(self):
        """Price data set on one chain should not leak to another."""
        market = MultiChainMarketSnapshot(
            chains=["arbitrum", "ethereum"],
            wallet_address="0xtest",
        )
        market.set_price_data("ETH", "arbitrum", PriceData(price=Decimal("3000")))

        try:
            market.price("ETH", chain="ethereum")
            assert False, "Should have raised ValueError"
        except ValueError:
            pass
