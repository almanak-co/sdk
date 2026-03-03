"""Tests for MarketSnapshot.lending_rate() and best_lending_rate() (VIB-437).

Proves the fix for the MarketSnapshot duplication bug where strategies calling
market.lending_rate() would get AttributeError because the simple MarketSnapshot
lacked rate_monitor support. Now the canonical MarketSnapshot (in intent_strategy.py)
supports lending_rate(), best_lending_rate(), and set_lending_rate() directly.
"""

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, patch

import pytest

from almanak.framework.data.rates import BestRateResult, LendingRate, RateMonitor, RateSide
from almanak.framework.strategies.intent_strategy import MarketSnapshot


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_rate_monitor():
    """Create a RateMonitor with mock rates (no RPC needed)."""
    monitor = RateMonitor(chain="ethereum")
    monitor.set_mock_rate("aave_v3", "USDC", "supply", Decimal("4.25"))
    monitor.set_mock_rate("aave_v3", "USDC", "borrow", Decimal("5.75"))
    monitor.set_mock_rate("aave_v3", "WETH", "supply", Decimal("2.15"))
    monitor.set_mock_rate("morpho_blue", "USDC", "supply", Decimal("5.15"))
    monitor.set_mock_rate("morpho_blue", "USDC", "borrow", Decimal("5.25"))
    monitor.set_mock_rate("compound_v3", "USDC", "supply", Decimal("4.85"))
    return monitor


@pytest.fixture
def snapshot_with_rates(mock_rate_monitor):
    """Create a MarketSnapshot with rate_monitor wired in."""
    return MarketSnapshot(
        chain="ethereum",
        wallet_address="0xtest",
        rate_monitor=mock_rate_monitor,
    )


@pytest.fixture
def snapshot_without_rates():
    """Create a MarketSnapshot without rate_monitor (original state)."""
    return MarketSnapshot(
        chain="arbitrum",
        wallet_address="0xtest",
    )


# =============================================================================
# Stage 1: lending_rate() works on the canonical MarketSnapshot
# =============================================================================


class TestLendingRate:
    """Test MarketSnapshot.lending_rate() with RateMonitor."""

    def test_lending_rate_supply(self, snapshot_with_rates):
        """lending_rate() returns supply rate from RateMonitor."""
        rate = snapshot_with_rates.lending_rate("aave_v3", "USDC", "supply")
        assert isinstance(rate, LendingRate)
        assert rate.protocol == "aave_v3"
        assert rate.token == "USDC"
        assert rate.side == "supply"
        assert rate.apy_percent == Decimal("4.25")

    def test_lending_rate_borrow(self, snapshot_with_rates):
        """lending_rate() returns borrow rate from RateMonitor."""
        rate = snapshot_with_rates.lending_rate("aave_v3", "USDC", "borrow")
        assert isinstance(rate, LendingRate)
        assert rate.side == "borrow"
        assert rate.apy_percent == Decimal("5.75")

    def test_lending_rate_different_token(self, snapshot_with_rates):
        """lending_rate() works for different tokens."""
        rate = snapshot_with_rates.lending_rate("aave_v3", "WETH", "supply")
        assert rate.token == "WETH"
        assert rate.apy_percent == Decimal("2.15")

    def test_lending_rate_different_protocol(self, snapshot_with_rates):
        """lending_rate() works for different protocols."""
        rate = snapshot_with_rates.lending_rate("morpho_blue", "USDC", "supply")
        assert rate.protocol == "morpho_blue"
        assert rate.apy_percent == Decimal("5.15")

    def test_lending_rate_caches_result(self, snapshot_with_rates):
        """Second call returns cached result without re-fetching."""
        rate1 = snapshot_with_rates.lending_rate("aave_v3", "USDC", "supply")
        rate2 = snapshot_with_rates.lending_rate("aave_v3", "USDC", "supply")
        assert rate1 is rate2  # Same object from cache

    def test_lending_rate_no_monitor_raises(self, snapshot_without_rates):
        """lending_rate() raises ValueError when no rate_monitor configured."""
        with pytest.raises(ValueError, match="No rate monitor configured"):
            snapshot_without_rates.lending_rate("aave_v3", "USDC")

    def test_lending_rate_default_side_is_supply(self, snapshot_with_rates):
        """Default side parameter is 'supply'."""
        rate = snapshot_with_rates.lending_rate("aave_v3", "USDC")
        assert rate.side == "supply"
        assert rate.apy_percent == Decimal("4.25")


# =============================================================================
# Stage 1: best_lending_rate() works on the canonical MarketSnapshot
# =============================================================================


class TestBestLendingRate:
    """Test MarketSnapshot.best_lending_rate() cross-protocol comparison."""

    def test_best_supply_rate(self, snapshot_with_rates):
        """best_lending_rate() finds highest supply rate across protocols."""
        result = snapshot_with_rates.best_lending_rate("USDC", "supply")
        assert isinstance(result, BestRateResult)
        assert result.best_rate is not None
        # Morpho has highest USDC supply (5.15%)
        assert result.best_rate.protocol == "morpho_blue"
        assert result.best_rate.apy_percent == Decimal("5.15")

    def test_best_borrow_rate(self, snapshot_with_rates):
        """best_lending_rate() finds lowest borrow rate across protocols."""
        result = snapshot_with_rates.best_lending_rate("USDC", "borrow")
        assert isinstance(result, BestRateResult)
        assert result.best_rate is not None
        # Morpho has lowest USDC borrow (5.25%)
        assert result.best_rate.protocol == "morpho_blue"
        assert result.best_rate.apy_percent == Decimal("5.25")

    def test_best_rate_all_rates_populated(self, snapshot_with_rates):
        """all_rates list contains rates from all queried protocols."""
        result = snapshot_with_rates.best_lending_rate("USDC", "supply")
        assert len(result.all_rates) >= 2  # At least aave and morpho

    def test_best_rate_no_monitor_raises(self, snapshot_without_rates):
        """best_lending_rate() raises ValueError when no rate_monitor configured."""
        with pytest.raises(ValueError, match="No rate monitor configured"):
            snapshot_without_rates.best_lending_rate("USDC")


# =============================================================================
# Stage 1: set_lending_rate() pre-population for backtesting
# =============================================================================


class TestSetLendingRate:
    """Test MarketSnapshot.set_lending_rate() pre-population."""

    def test_set_lending_rate_roundtrip(self, snapshot_without_rates):
        """Pre-populated rate is returned by lending_rate() without needing RateMonitor."""
        fake_rate = LendingRate(
            protocol="aave_v3",
            token="USDC",
            side="supply",
            apy_ray=Decimal("0"),
            apy_percent=Decimal("3.50"),
            chain="arbitrum",
        )
        snapshot_without_rates.set_lending_rate("aave_v3", "USDC", "supply", fake_rate)

        # Should return the pre-populated rate without raising
        result = snapshot_without_rates.lending_rate("aave_v3", "USDC", "supply")
        assert result.apy_percent == Decimal("3.50")
        assert result.protocol == "aave_v3"

    def test_set_lending_rate_different_sides(self, snapshot_without_rates):
        """Supply and borrow rates are stored independently."""
        supply_rate = LendingRate(
            protocol="aave_v3", token="USDC", side="supply",
            apy_ray=Decimal("0"), apy_percent=Decimal("4.00"),
        )
        borrow_rate = LendingRate(
            protocol="aave_v3", token="USDC", side="borrow",
            apy_ray=Decimal("0"), apy_percent=Decimal("6.00"),
        )
        snapshot_without_rates.set_lending_rate("aave_v3", "USDC", "supply", supply_rate)
        snapshot_without_rates.set_lending_rate("aave_v3", "USDC", "borrow", borrow_rate)

        assert snapshot_without_rates.lending_rate("aave_v3", "USDC", "supply").apy_percent == Decimal("4.00")
        assert snapshot_without_rates.lending_rate("aave_v3", "USDC", "borrow").apy_percent == Decimal("6.00")

    def test_set_lending_rate_overrides_monitor(self, snapshot_with_rates):
        """Pre-populated rate takes precedence over RateMonitor."""
        custom_rate = LendingRate(
            protocol="aave_v3", token="USDC", side="supply",
            apy_ray=Decimal("0"), apy_percent=Decimal("99.99"),
        )
        snapshot_with_rates.set_lending_rate("aave_v3", "USDC", "supply", custom_rate)

        result = snapshot_with_rates.lending_rate("aave_v3", "USDC", "supply")
        assert result.apy_percent == Decimal("99.99")

    def test_set_lending_rate_does_not_affect_other_keys(self, snapshot_without_rates):
        """Setting one protocol/token/side doesn't affect others."""
        rate = LendingRate(
            protocol="aave_v3", token="USDC", side="supply",
            apy_ray=Decimal("0"), apy_percent=Decimal("4.00"),
        )
        snapshot_without_rates.set_lending_rate("aave_v3", "USDC", "supply", rate)

        # Different protocol should still raise (no rate_monitor)
        with pytest.raises(ValueError, match="No rate monitor configured"):
            snapshot_without_rates.lending_rate("morpho_blue", "USDC", "supply")


# =============================================================================
# Stage 1: create_market_snapshot() wiring
# =============================================================================


class _StubStrategy:
    """Minimal concrete IntentStrategy for testing create_market_snapshot() wiring."""

    @staticmethod
    def _create(chain="ethereum", wallet_address="0xtest"):
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        class _Stub(IntentStrategy):
            def decide(self, market):
                return None

        return _Stub(config={}, chain=chain, wallet_address=wallet_address)


class TestCreateMarketSnapshotWiring:
    """Test that IntentStrategy.create_market_snapshot() passes rate_monitor through."""

    def test_rate_monitor_wired_through_create_market_snapshot(self):
        """When IntentStrategy has _rate_monitor set, it flows to MarketSnapshot."""
        strategy = _StubStrategy._create(chain="ethereum")
        # Simulate what the CLI runner does
        monitor = RateMonitor(chain="ethereum")
        monitor.set_mock_rate("aave_v3", "USDC", "supply", Decimal("4.25"))
        strategy._rate_monitor = monitor

        # create_market_snapshot() should pass rate_monitor through
        snapshot = strategy.create_market_snapshot()
        assert snapshot._rate_monitor is monitor

        # lending_rate() should work on the created snapshot
        rate = snapshot.lending_rate("aave_v3", "USDC", "supply")
        assert rate.apy_percent == Decimal("4.25")

    def test_no_rate_monitor_by_default(self):
        """Without rate_monitor set, create_market_snapshot() still works (rate_monitor=None)."""
        strategy = _StubStrategy._create(chain="arbitrum")
        snapshot = strategy.create_market_snapshot()
        assert snapshot._rate_monitor is None


# =============================================================================
# Stage 2: backward compatibility — existing code still works
# =============================================================================


class TestBackwardCompatibility:
    """Prove that adding rate_monitor doesn't break existing MarketSnapshot usage."""

    def test_price_still_works(self):
        """price() still works after adding rate_monitor param."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price("ETH", Decimal("3000"))
        assert market.price("ETH") == Decimal("3000")

    def test_balance_still_works(self):
        """balance() still works after adding rate_monitor param."""
        from almanak.framework.strategies.intent_strategy import TokenBalance

        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_balance(
            "USDC",
            TokenBalance(symbol="USDC", balance=Decimal("1000"), balance_usd=Decimal("1000")),
        )
        assert market.balance("USDC").balance == Decimal("1000")

    def test_rsi_still_works(self):
        """rsi() still works after adding rate_monitor param."""
        from almanak.framework.strategies.intent_strategy import RSIData

        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_rsi("ETH", RSIData(value=Decimal("45")))
        rsi = market.rsi("ETH")
        assert rsi.value == Decimal("45")

    def test_constructor_without_rate_monitor(self):
        """MarketSnapshot can still be created without rate_monitor (default None)."""
        market = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0xtest",
            price_oracle=lambda token, quote: Decimal("3000"),
        )
        assert market.price("ETH") == Decimal("3000")
        assert market._rate_monitor is None

    def test_macd_still_works(self):
        """MACD indicator still works."""
        from almanak.framework.strategies.intent_strategy import MACDData

        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_macd(
            "ETH",
            MACDData(macd_line=Decimal("0.5"), signal_line=Decimal("0.3"), histogram=Decimal("0.2")),
        )
        macd = market.macd("ETH")
        assert macd.histogram == Decimal("0.2")

    def test_total_portfolio_usd_still_works(self):
        """total_portfolio_usd() still works."""
        from almanak.framework.strategies.intent_strategy import TokenBalance

        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_balance("USDC", TokenBalance(symbol="USDC", balance=Decimal("1000"), balance_usd=Decimal("1000")))
        market.set_balance("ETH", TokenBalance(symbol="ETH", balance=Decimal("1"), balance_usd=Decimal("3000")))
        assert market.total_portfolio_usd() == Decimal("4000")


# =============================================================================
# Stage 3: RateMonitor as a real integration test (mock RPC)
# =============================================================================


class TestRateMonitorIntegration:
    """Integration test: RateMonitor -> MarketSnapshot -> strategy.decide()."""

    def test_strategy_can_use_lending_rate_in_decide(self):
        """End-to-end: strategy calling market.lending_rate() in decide()."""
        from almanak.framework.intents import Intent
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        class LendingRateStrategy(IntentStrategy):
            """Test strategy that uses lending rates."""

            def decide(self, market):
                rate = market.lending_rate("aave_v3", "USDC", "supply")
                if rate.apy_percent > Decimal("3.0"):
                    return Intent.hold(reason=f"Good rate: {rate.apy_percent}%")
                return None

        strategy = LendingRateStrategy(config={}, chain="ethereum", wallet_address="0xtest")
        monitor = RateMonitor(chain="ethereum")
        monitor.set_mock_rate("aave_v3", "USDC", "supply", Decimal("4.25"))
        strategy._rate_monitor = monitor

        market = strategy.create_market_snapshot()
        result = strategy.decide(market)

        # Should have returned a HOLD intent with the rate info
        assert result is not None

    def test_strategy_can_use_best_lending_rate_in_decide(self):
        """End-to-end: strategy calling market.best_lending_rate() in decide()."""
        from almanak.framework.intents import Intent
        from almanak.framework.strategies.intent_strategy import IntentStrategy

        class BestRateStrategy(IntentStrategy):
            """Test strategy that finds best rate."""

            def decide(self, market):
                result = market.best_lending_rate("USDC", "supply")
                if result.best_rate and result.best_rate.apy_percent > Decimal("4.0"):
                    return Intent.hold(reason=f"Best rate: {result.best_rate.protocol}")
                return None

        strategy = BestRateStrategy(config={}, chain="ethereum", wallet_address="0xtest")
        monitor = RateMonitor(chain="ethereum")
        monitor.set_mock_rate("aave_v3", "USDC", "supply", Decimal("4.25"))
        monitor.set_mock_rate("morpho_blue", "USDC", "supply", Decimal("5.15"))
        strategy._rate_monitor = monitor

        market = strategy.create_market_snapshot()
        result = strategy.decide(market)

        assert result is not None

    def test_backtest_with_prepopulated_lending_rates(self):
        """Backtest scenario: pre-populated lending rates without RateMonitor."""
        market = MarketSnapshot(chain="ethereum", wallet_address="0xtest")
        market.set_price("USDC", Decimal("1.00"))
        market.set_price("WETH", Decimal("3000"))

        # Simulate historical lending rates for backtesting
        market.set_lending_rate(
            "aave_v3", "USDC", "supply",
            LendingRate(
                protocol="aave_v3", token="USDC", side="supply",
                apy_ray=Decimal("0"), apy_percent=Decimal("3.50"),
                chain="ethereum",
            ),
        )
        market.set_lending_rate(
            "morpho_blue", "USDC", "supply",
            LendingRate(
                protocol="morpho_blue", token="USDC", side="supply",
                apy_ray=Decimal("0"), apy_percent=Decimal("4.80"),
                chain="ethereum",
            ),
        )

        # Strategy can query rates just like in production
        aave_rate = market.lending_rate("aave_v3", "USDC", "supply")
        morpho_rate = market.lending_rate("morpho_blue", "USDC", "supply")

        assert aave_rate.apy_percent == Decimal("3.50")
        assert morpho_rate.apy_percent == Decimal("4.80")


# =============================================================================
# Edge cases
# =============================================================================


class TestEdgeCases:
    """Edge cases and error handling."""

    def test_lending_rate_unsupported_protocol(self, snapshot_with_rates):
        """Requesting unsupported protocol raises ValueError."""
        with pytest.raises(ValueError):
            snapshot_with_rates.lending_rate("unsupported_proto", "USDC", "supply")

    def test_lending_rate_unsupported_token(self, snapshot_with_rates):
        """Requesting unsupported token raises ValueError."""
        with pytest.raises(ValueError):
            snapshot_with_rates.lending_rate("aave_v3", "UNKNOWN_TOKEN_XYZ", "supply")

    def test_snapshot_to_dict_unchanged(self):
        """to_dict() still works correctly (no regression)."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        market.set_price("ETH", Decimal("3000"))
        d = market.to_dict()
        assert d["chain"] == "arbitrum"
        assert d["prices"]["ETH"] == "3000"

    def test_rate_monitor_attribute_exists_on_snapshot(self):
        """MarketSnapshot has _rate_monitor attribute."""
        market = MarketSnapshot(chain="arbitrum", wallet_address="0xtest")
        assert hasattr(market, "_rate_monitor")
        assert market._rate_monitor is None

    def test_rate_monitor_attribute_exists_on_strategy(self):
        """IntentStrategy has _rate_monitor attribute."""
        strategy = _StubStrategy._create(chain="ethereum")
        assert hasattr(strategy, "_rate_monitor")
        assert strategy._rate_monitor is None
