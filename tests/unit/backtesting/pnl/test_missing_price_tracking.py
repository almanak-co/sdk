"""Tests for missing price tracking in portfolio valuation.

This test suite validates that:
1. Missing prices are logged with warnings including token and timestamp
2. DataQualityTracker records missing price lookups
3. strict_price_mode raises ValueError when prices are missing
4. Missing prices are tracked per unique token/chain combination

Part of US-087a: Replace silent price skip with logged metric (P1-AUDIT).
"""

import logging
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import DataQualityTracker
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPortfolio,
    SimulatedPosition,
)

# Test timestamp for positions
TEST_TIME = datetime(2024, 1, 1, 12, 0, 0)
SIMULATION_TIME = datetime(2024, 6, 15, 10, 30, 0)

# Test chain IDs
CHAIN_ID_ETHEREUM = 1
CHAIN_ID_ARBITRUM = 42161


class TestDataQualityTrackerMissingPrice:
    """Tests for missing price tracking in DataQualityTracker."""

    def test_tracker_has_missing_price_count(self):
        """Test DataQualityTracker has missing_price_count field."""
        tracker = DataQualityTracker()
        assert hasattr(tracker, "missing_price_count")
        assert tracker.missing_price_count == 0

    def test_record_missing_price(self):
        """Test recording a missing price increments count."""
        tracker = DataQualityTracker()
        tracker.record_missing_price(
            token="UNKNOWN",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        assert tracker.missing_price_count == 1

    def test_record_missing_price_unique(self):
        """Test same token is only counted once."""
        tracker = DataQualityTracker()
        # Record same token multiple times
        for _ in range(3):
            tracker.record_missing_price(
                token="UNKNOWN",
                timestamp=TEST_TIME,
                chain_id=CHAIN_ID_ETHEREUM,
            )
        # Should only count once
        assert tracker.missing_price_count == 1

    def test_record_missing_price_different_chains(self):
        """Test same token on different chains counted separately."""
        tracker = DataQualityTracker()
        tracker.record_missing_price(
            token="UNKNOWN",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        tracker.record_missing_price(
            token="UNKNOWN",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ARBITRUM,
        )
        assert tracker.missing_price_count == 2

    def test_record_missing_price_different_tokens(self):
        """Test different tokens counted separately."""
        tracker = DataQualityTracker()
        tracker.record_missing_price(
            token="TOKEN_A",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        tracker.record_missing_price(
            token="TOKEN_B",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        assert tracker.missing_price_count == 2

    def test_record_missing_price_also_increments_failed_lookups(self):
        """Test missing price also records as failed lookup."""
        tracker = DataQualityTracker()
        initial_failed = tracker.failed_lookups
        tracker.record_missing_price(
            token="UNKNOWN",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        assert tracker.failed_lookups == initial_failed + 1

    def test_missing_price_tokens_property(self):
        """Test missing_price_tokens returns list of tracked tokens."""
        tracker = DataQualityTracker()
        tracker.record_missing_price(
            token="TOKEN_A",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        tracker.record_missing_price(
            token="TOKEN_B",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ARBITRUM,
        )
        tokens = tracker.missing_price_tokens
        assert len(tokens) == 2
        assert f"{CHAIN_ID_ETHEREUM}:token_a" in tokens
        assert f"{CHAIN_ID_ARBITRUM}:token_b" in tokens


class TestPortfolioMissingPriceLogging:
    """Tests for portfolio valuation logging when prices are missing."""

    def _create_portfolio_with_tokens(self, tokens: dict[str, Decimal]) -> SimulatedPortfolio:
        """Create a portfolio with token holdings."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            cash_usd=Decimal("5000"),
        )
        portfolio.tokens = tokens
        return portfolio

    def _create_market_state_with_prices(self, prices: dict[str, Decimal]) -> MarketState:
        """Create a mock market state with specified prices."""
        state = MagicMock(spec=MarketState)

        def get_price(token: str) -> Decimal:
            if token in prices:
                return prices[token]
            raise KeyError(f"Price not found for {token}")

        state.get_price = get_price
        return state

    def test_missing_price_logs_warning(self, caplog):
        """Test that missing price logs a warning with token and timestamp."""
        portfolio = self._create_portfolio_with_tokens({"UNKNOWN_TOKEN": Decimal("100")})
        market_state = self._create_market_state_with_prices({})  # No prices

        with caplog.at_level(logging.WARNING):
            portfolio.get_total_value_usd(
                market_state,
                simulation_timestamp=SIMULATION_TIME,
            )

        # Check warning was logged
        assert any("Missing price" in record.message for record in caplog.records)
        assert any("UNKNOWN_TOKEN" in record.message for record in caplog.records)
        assert any("2024-06-15" in record.message for record in caplog.records)

    def test_missing_price_records_in_tracker(self):
        """Test that missing price is recorded in DataQualityTracker."""
        portfolio = self._create_portfolio_with_tokens({"UNKNOWN_TOKEN": Decimal("100")})
        market_state = self._create_market_state_with_prices({})  # No prices
        tracker = DataQualityTracker()

        portfolio.get_total_value_usd(
            market_state,
            data_tracker=tracker,
            simulation_timestamp=SIMULATION_TIME,
            chain_id=CHAIN_ID_ETHEREUM,
        )

        assert tracker.missing_price_count == 1
        assert f"{CHAIN_ID_ETHEREUM}:unknown_token" in tracker.missing_price_tokens

    def test_mixed_prices_tracks_only_missing(self):
        """Test that only missing prices are tracked, not successful ones."""
        portfolio = self._create_portfolio_with_tokens({
            "WETH": Decimal("10"),
            "UNKNOWN_TOKEN": Decimal("100"),
        })
        market_state = self._create_market_state_with_prices({
            "WETH": Decimal("3000"),
        })
        tracker = DataQualityTracker()

        portfolio.get_total_value_usd(
            market_state,
            data_tracker=tracker,
            simulation_timestamp=SIMULATION_TIME,
            chain_id=CHAIN_ID_ETHEREUM,
        )

        # Only UNKNOWN_TOKEN should be tracked as missing
        assert tracker.missing_price_count == 1

    def test_valuation_continues_without_strict_mode(self):
        """Test valuation continues when price is missing and strict_mode is False."""
        portfolio = self._create_portfolio_with_tokens({"UNKNOWN_TOKEN": Decimal("100")})
        market_state = self._create_market_state_with_prices({})

        # Should not raise, should return cash balance
        total = portfolio.get_total_value_usd(
            market_state,
            strict_price_mode=False,
        )

        assert total == Decimal("5000")  # Only cash is counted


class TestPortfolioStrictPriceMode:
    """Tests for strict_price_mode behavior in portfolio valuation."""

    def _create_portfolio_with_tokens(self, tokens: dict[str, Decimal]) -> SimulatedPortfolio:
        """Create a portfolio with token holdings."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            cash_usd=Decimal("5000"),
        )
        portfolio.tokens = tokens
        return portfolio

    def _create_market_state_with_prices(self, prices: dict[str, Decimal]) -> MarketState:
        """Create a mock market state with specified prices."""
        state = MagicMock(spec=MarketState)

        def get_price(token: str) -> Decimal:
            if token in prices:
                return prices[token]
            raise KeyError(f"Price not found for {token}")

        state.get_price = get_price
        return state

    def test_strict_mode_raises_on_missing_price(self):
        """Test strict_price_mode raises ValueError when price is missing."""
        portfolio = self._create_portfolio_with_tokens({"UNKNOWN_TOKEN": Decimal("100")})
        market_state = self._create_market_state_with_prices({})

        with pytest.raises(ValueError) as exc_info:
            portfolio.get_total_value_usd(
                market_state,
                strict_price_mode=True,
                simulation_timestamp=SIMULATION_TIME,
                chain_id=CHAIN_ID_ETHEREUM,
            )

        assert "Missing price for token UNKNOWN_TOKEN" in str(exc_info.value)
        assert "strict_price_mode is enabled" in str(exc_info.value)

    def test_strict_mode_includes_context_in_error(self):
        """Test strict_price_mode error includes timestamp and chain_id."""
        portfolio = self._create_portfolio_with_tokens({"UNKNOWN_TOKEN": Decimal("100")})
        market_state = self._create_market_state_with_prices({})

        with pytest.raises(ValueError) as exc_info:
            portfolio.get_total_value_usd(
                market_state,
                strict_price_mode=True,
                simulation_timestamp=SIMULATION_TIME,
                chain_id=CHAIN_ID_ETHEREUM,
            )

        error_msg = str(exc_info.value)
        assert "2024-06-15" in error_msg
        assert str(CHAIN_ID_ETHEREUM) in error_msg

    def test_strict_mode_passes_when_all_prices_available(self):
        """Test strict_price_mode succeeds when all prices are available."""
        portfolio = self._create_portfolio_with_tokens({
            "WETH": Decimal("10"),
            "USDC": Decimal("5000"),
        })
        market_state = self._create_market_state_with_prices({
            "WETH": Decimal("3000"),
            "USDC": Decimal("1"),
        })

        # Should not raise
        total = portfolio.get_total_value_usd(
            market_state,
            strict_price_mode=True,
        )

        expected = Decimal("5000") + Decimal("10") * Decimal("3000") + Decimal("5000") * Decimal("1")
        assert total == expected


class TestPositionValueMissingPrice:
    """Tests for missing price handling in position valuation."""

    def _create_market_state_with_prices(self, prices: dict[str, Decimal]) -> MarketState:
        """Create a mock market state with specified prices."""
        state = MagicMock(spec=MarketState)

        def get_price(token: str) -> Decimal:
            if token in prices:
                return prices[token]
            raise KeyError(f"Price not found for {token}")

        state.get_price = get_price
        return state

    def test_spot_position_missing_price_uses_entry_price(self, caplog):
        """Test SPOT position falls back to entry price when price missing."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            cash_usd=Decimal("10000"),
        )
        position = SimulatedPosition(
            position_id="test-pos",
            position_type=PositionType.SPOT,
            protocol="test",
            tokens=["UNKNOWN_TOKEN"],
            amounts={"UNKNOWN_TOKEN": Decimal("10")},
            entry_price=Decimal("100"),  # Fallback price
            entry_time=TEST_TIME,
        )
        portfolio.positions.append(position)
        market_state = self._create_market_state_with_prices({})

        with caplog.at_level(logging.WARNING):
            value = portfolio._get_position_value(
                position,
                market_state,
                simulation_timestamp=SIMULATION_TIME,
            )

        # Should use entry price as fallback
        assert value == Decimal("10") * Decimal("100")
        # Should log warning
        assert any("Missing price" in record.message for record in caplog.records)

    def test_spot_position_strict_mode_raises(self):
        """Test SPOT position in strict mode raises on missing price."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            cash_usd=Decimal("10000"),
        )
        position = SimulatedPosition(
            position_id="test-pos",
            position_type=PositionType.SPOT,
            protocol="test",
            tokens=["UNKNOWN_TOKEN"],
            amounts={"UNKNOWN_TOKEN": Decimal("10")},
            entry_price=Decimal("100"),
            entry_time=TEST_TIME,
        )
        portfolio.positions.append(position)
        market_state = self._create_market_state_with_prices({})

        with pytest.raises(ValueError) as exc_info:
            portfolio._get_position_value(
                position,
                market_state,
                strict_price_mode=True,
                simulation_timestamp=SIMULATION_TIME,
            )

        assert "SPOT position" in str(exc_info.value)

    def test_lp_position_missing_price_tracked(self, caplog):
        """Test LP position missing price is tracked."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            cash_usd=Decimal("10000"),
        )
        position = SimulatedPosition(
            position_id="test-lp",
            position_type=PositionType.LP,
            protocol="test",
            tokens=["WETH", "USDC"],
            amounts={"WETH": Decimal("1"), "USDC": Decimal("3000")},
            entry_price=Decimal("3000"),
            entry_time=TEST_TIME,
        )
        portfolio.positions.append(position)
        tracker = DataQualityTracker()
        market_state = self._create_market_state_with_prices({})  # No prices

        with caplog.at_level(logging.WARNING):
            portfolio._get_position_value(
                position,
                market_state,
                data_tracker=tracker,
                simulation_timestamp=SIMULATION_TIME,
                chain_id=CHAIN_ID_ETHEREUM,
            )

        # Both tokens should be tracked as missing
        assert tracker.missing_price_count == 2

    def test_perp_position_missing_price_uses_entry(self, caplog):
        """Test PERP position uses entry price when current price missing."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            cash_usd=Decimal("10000"),
        )
        position = SimulatedPosition(
            position_id="test-perp",
            position_type=PositionType.PERP_LONG,
            protocol="test",
            tokens=["ETH"],
            amounts={"ETH": Decimal("0")},  # Not used for perp
            entry_price=Decimal("3000"),
            entry_time=TEST_TIME,
            collateral_usd=Decimal("1000"),
            notional_usd=Decimal("10000"),
        )
        portfolio.positions.append(position)
        tracker = DataQualityTracker()
        market_state = self._create_market_state_with_prices({})

        with caplog.at_level(logging.WARNING):
            value = portfolio._get_position_value(
                position,
                market_state,
                data_tracker=tracker,
                simulation_timestamp=SIMULATION_TIME,
            )

        # Should track missing price
        assert tracker.missing_price_count == 1
        # With entry price fallback, unrealized PnL is 0
        assert value == position.collateral_usd + position.accumulated_funding

    def test_lending_position_missing_price_uses_entry(self, caplog):
        """Test LENDING position uses entry price when current price missing."""
        portfolio = SimulatedPortfolio(
            initial_capital_usd=Decimal("10000"),
            cash_usd=Decimal("10000"),
        )
        position = SimulatedPosition(
            position_id="test-supply",
            position_type=PositionType.SUPPLY,
            protocol="test",
            tokens=["USDC"],
            amounts={"USDC": Decimal("1000")},
            entry_price=Decimal("1"),
            entry_time=TEST_TIME,
            interest_accrued=Decimal("10"),
        )
        portfolio.positions.append(position)
        tracker = DataQualityTracker()
        market_state = self._create_market_state_with_prices({})

        with caplog.at_level(logging.WARNING):
            value = portfolio._get_position_value(
                position,
                market_state,
                data_tracker=tracker,
                simulation_timestamp=SIMULATION_TIME,
            )

        # Should track missing price
        assert tracker.missing_price_count == 1
        # Value should be principal + interest
        expected = Decimal("1000") * Decimal("1") + Decimal("10")
        assert value == expected


class TestDataQualityReportMissingPrice:
    """Tests for missing price fields in DataQualityReport.

    Part of US-087b: Add missing_price_count to DataQualityReport (P1-AUDIT).
    """

    def test_data_quality_report_has_missing_price_count(self):
        """Test DataQualityReport has missing_price_count field with default 0."""
        from almanak.framework.backtesting.models import DataQualityReport

        report = DataQualityReport()
        assert hasattr(report, "missing_price_count")
        assert report.missing_price_count == 0

    def test_data_quality_report_has_missing_price_tokens(self):
        """Test DataQualityReport has missing_price_tokens field with default empty list."""
        from almanak.framework.backtesting.models import DataQualityReport

        report = DataQualityReport()
        assert hasattr(report, "missing_price_tokens")
        assert report.missing_price_tokens == []

    def test_data_quality_report_to_dict_includes_missing_price_count(self):
        """Test to_dict includes missing_price_count."""
        from almanak.framework.backtesting.models import DataQualityReport

        report = DataQualityReport(missing_price_count=5)
        data = report.to_dict()
        assert "missing_price_count" in data
        assert data["missing_price_count"] == 5

    def test_data_quality_report_to_dict_includes_missing_price_tokens(self):
        """Test to_dict includes missing_price_tokens."""
        from almanak.framework.backtesting.models import DataQualityReport

        tokens = ["1:unknown_token", "42161:weird_token"]
        report = DataQualityReport(missing_price_tokens=tokens)
        data = report.to_dict()
        assert "missing_price_tokens" in data
        assert data["missing_price_tokens"] == tokens

    def test_data_quality_report_from_dict_deserializes_missing_price_count(self):
        """Test from_dict deserializes missing_price_count."""
        from almanak.framework.backtesting.models import DataQualityReport

        data = {"missing_price_count": 7}
        report = DataQualityReport.from_dict(data)
        assert report.missing_price_count == 7

    def test_data_quality_report_from_dict_deserializes_missing_price_tokens(self):
        """Test from_dict deserializes missing_price_tokens."""
        from almanak.framework.backtesting.models import DataQualityReport

        tokens = ["1:token_a", "137:token_b"]
        data = {"missing_price_tokens": tokens}
        report = DataQualityReport.from_dict(data)
        assert report.missing_price_tokens == tokens

    def test_data_quality_report_from_dict_defaults_missing_price_count(self):
        """Test from_dict defaults missing_price_count to 0 when missing."""
        from almanak.framework.backtesting.models import DataQualityReport

        data = {}
        report = DataQualityReport.from_dict(data)
        assert report.missing_price_count == 0

    def test_data_quality_report_from_dict_defaults_missing_price_tokens(self):
        """Test from_dict defaults missing_price_tokens to empty list when missing."""
        from almanak.framework.backtesting.models import DataQualityReport

        data = {}
        report = DataQualityReport.from_dict(data)
        assert report.missing_price_tokens == []

    def test_data_quality_report_roundtrip(self):
        """Test full roundtrip serialization/deserialization with missing price fields."""
        from almanak.framework.backtesting.models import DataQualityReport

        original = DataQualityReport(
            missing_price_count=3,
            missing_price_tokens=["1:weth", "42161:usdc"],
            coverage_ratio=Decimal("0.95"),
            stale_data_count=2,
        )
        data = original.to_dict()
        restored = DataQualityReport.from_dict(data)

        assert restored.missing_price_count == original.missing_price_count
        assert restored.missing_price_tokens == original.missing_price_tokens
        assert restored.coverage_ratio == original.coverage_ratio
        assert restored.stale_data_count == original.stale_data_count


class TestDataQualityTrackerToReport:
    """Tests for DataQualityTracker.to_data_quality_report including missing price fields.

    Part of US-087b: Add missing_price_count to DataQualityReport (P1-AUDIT).
    """

    def test_to_data_quality_report_includes_missing_price_count(self):
        """Test to_data_quality_report populates missing_price_count."""
        tracker = DataQualityTracker()
        tracker.record_missing_price(
            token="UNKNOWN",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        tracker.record_missing_price(
            token="WEIRD",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ARBITRUM,
        )

        report = tracker.to_data_quality_report()
        assert report.missing_price_count == 2

    def test_to_data_quality_report_includes_missing_price_tokens(self):
        """Test to_data_quality_report populates missing_price_tokens."""
        tracker = DataQualityTracker()
        tracker.record_missing_price(
            token="TOKEN_A",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        tracker.record_missing_price(
            token="TOKEN_B",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ARBITRUM,
        )

        report = tracker.to_data_quality_report()
        assert len(report.missing_price_tokens) == 2
        assert f"{CHAIN_ID_ETHEREUM}:token_a" in report.missing_price_tokens
        assert f"{CHAIN_ID_ARBITRUM}:token_b" in report.missing_price_tokens

    def test_missing_price_affects_coverage_ratio(self):
        """Test that missing prices affect coverage_ratio calculation."""
        tracker = DataQualityTracker()

        # Record 10 successful lookups
        for _ in range(10):
            tracker.record_lookup(success=True, source="coingecko")

        # Record 2 missing prices (these also count as failed lookups)
        tracker.record_missing_price(
            token="TOKEN_A",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ETHEREUM,
        )
        tracker.record_missing_price(
            token="TOKEN_B",
            timestamp=TEST_TIME,
            chain_id=CHAIN_ID_ETHEREUM,
        )

        report = tracker.to_data_quality_report()

        # 10 successful / 12 total = 0.833...
        expected_ratio = Decimal("10") / Decimal("12")
        assert report.coverage_ratio == expected_ratio

    def test_empty_tracker_produces_report_with_zero_missing_prices(self):
        """Test empty tracker produces report with zero missing prices."""
        tracker = DataQualityTracker()
        report = tracker.to_data_quality_report()

        assert report.missing_price_count == 0
        assert report.missing_price_tokens == []
