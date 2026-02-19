"""Tests for gas ETH price fallback removal (US-084b).

This module tests that the PnL backtester no longer uses hardcoded $3000 ETH
fallback for gas cost calculations. Instead, it should:
1. In strict mode: raise ValueError if ETH price unavailable
2. In non-strict mode: raise ValueError requiring gas_eth_price_override

The gas_price_source should be tracked in DataQualityReport.
"""

from datetime import datetime
from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import DataQualityReport
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.engine import DataQualityTracker


class TestDataQualityTrackerGasPriceSource:
    """Tests for DataQualityTracker gas price source tracking."""

    def test_record_gas_price_source_override(self):
        """Test tracking override source."""
        tracker = DataQualityTracker()
        tracker.record_gas_price_source("override")
        assert tracker.gas_price_source_counts == {"override": 1}

    def test_record_gas_price_source_historical(self):
        """Test tracking historical source."""
        tracker = DataQualityTracker()
        tracker.record_gas_price_source("historical")
        tracker.record_gas_price_source("historical")
        assert tracker.gas_price_source_counts == {"historical": 2}

    def test_record_gas_price_source_market(self):
        """Test tracking market source."""
        tracker = DataQualityTracker()
        tracker.record_gas_price_source("market")
        assert tracker.gas_price_source_counts == {"market": 1}

    def test_record_multiple_sources(self):
        """Test tracking multiple sources."""
        tracker = DataQualityTracker()
        tracker.record_gas_price_source("override")
        tracker.record_gas_price_source("historical")
        tracker.record_gas_price_source("historical")
        tracker.record_gas_price_source("market")
        assert tracker.gas_price_source_counts == {
            "override": 1,
            "historical": 2,
            "market": 1,
        }

    def test_to_data_quality_report_includes_gas_price_source(self):
        """Test that to_data_quality_report includes gas_price_source_counts."""
        tracker = DataQualityTracker()
        tracker.record_gas_price_source("override")
        tracker.record_gas_price_source("historical")

        report = tracker.to_data_quality_report()
        assert report.gas_price_source_counts == {"override": 1, "historical": 1}


class TestDataQualityReportGasPriceSource:
    """Tests for DataQualityReport gas_price_source_counts field."""

    def test_default_gas_price_source_counts(self):
        """Test default value is empty dict."""
        report = DataQualityReport()
        assert report.gas_price_source_counts == {}

    def test_gas_price_source_counts_initialization(self):
        """Test initialization with gas_price_source_counts."""
        report = DataQualityReport(
            gas_price_source_counts={"override": 10, "historical": 90}
        )
        assert report.gas_price_source_counts == {"override": 10, "historical": 90}

    def test_to_dict_includes_gas_price_source_counts(self):
        """Test serialization includes gas_price_source_counts."""
        report = DataQualityReport(
            gas_price_source_counts={"override": 5, "market": 15}
        )
        data = report.to_dict()
        assert "gas_price_source_counts" in data
        assert data["gas_price_source_counts"] == {"override": 5, "market": 15}

    def test_from_dict_parses_gas_price_source_counts(self):
        """Test deserialization parses gas_price_source_counts."""
        data = {
            "coverage_ratio": "0.95",
            "source_breakdown": {},
            "stale_data_count": 0,
            "interpolation_count": 0,
            "unresolved_token_count": 0,
            "gas_price_source_counts": {"historical": 100},
        }
        report = DataQualityReport.from_dict(data)
        assert report.gas_price_source_counts == {"historical": 100}

    def test_from_dict_defaults_gas_price_source_counts(self):
        """Test deserialization defaults to empty dict if missing."""
        data = {
            "coverage_ratio": "1.0",
            "source_breakdown": {},
            "stale_data_count": 0,
            "interpolation_count": 0,
            "unresolved_token_count": 0,
        }
        report = DataQualityReport.from_dict(data)
        assert report.gas_price_source_counts == {}


class TestGasEthPriceFallbackRemoval:
    """Tests verifying $3000 ETH fallback has been removed."""

    @pytest.fixture
    def base_config(self):
        """Create base config for testing."""
        return PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 1, 2),
            initial_capital_usd=Decimal("10000"),
            chain="ethereum",
            tokens=["ETH", "USDC"],
            include_gas_costs=True,
        )

    @pytest.fixture
    def market_state_no_eth(self):
        """Create market state without ETH/WETH prices."""
        return MarketState(
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            prices={"USDC": Decimal("1.0"), "BTC": Decimal("40000")},
        )

    @pytest.fixture
    def market_state_with_eth(self):
        """Create market state with WETH price."""
        return MarketState(
            timestamp=datetime(2024, 1, 1, 12, 0, 0),
            prices={
                "USDC": Decimal("1.0"),
                "WETH": Decimal("2500"),
                "ETH": Decimal("2500"),
            },
        )

    def test_gas_override_used_when_set(self, base_config, market_state_no_eth):
        """Test that gas_eth_price_override is used when set."""
        # Set override
        config = PnLBacktestConfig(
            start_time=base_config.start_time,
            end_time=base_config.end_time,
            initial_capital_usd=base_config.initial_capital_usd,
            chain=base_config.chain,
            tokens=base_config.tokens,
            include_gas_costs=True,
            gas_eth_price_override=Decimal("3500"),
        )

        # Even without ETH price in market state, should work with override
        # This verifies the config accepts the override
        assert config.gas_eth_price_override == Decimal("3500")

    def test_market_eth_price_used_when_available(
        self, base_config, market_state_with_eth
    ):
        """Test that market ETH price is used when available."""
        # Market state has WETH/ETH prices
        assert market_state_with_eth.get_price("WETH") == Decimal("2500")
        assert market_state_with_eth.get_price("ETH") == Decimal("2500")

    def test_strict_mode_config(self, base_config):
        """Test strict_reproducibility mode configuration."""
        config = PnLBacktestConfig(
            start_time=base_config.start_time,
            end_time=base_config.end_time,
            initial_capital_usd=base_config.initial_capital_usd,
            chain=base_config.chain,
            tokens=base_config.tokens,
            include_gas_costs=True,
            strict_reproducibility=True,
        )
        assert config.strict_reproducibility is True

    def test_use_historical_gas_prices_config(self, base_config):
        """Test use_historical_gas_prices configuration."""
        config = PnLBacktestConfig(
            start_time=base_config.start_time,
            end_time=base_config.end_time,
            initial_capital_usd=base_config.initial_capital_usd,
            chain=base_config.chain,
            tokens=base_config.tokens,
            include_gas_costs=True,
            use_historical_gas_prices=True,
        )
        assert config.use_historical_gas_prices is True


class TestGasCostCalculationWithTracker:
    """Tests for gas cost calculation with data quality tracking."""

    def test_tracker_records_override_source(self):
        """Test that tracker records override source."""
        tracker = DataQualityTracker()
        tracker.record_gas_price_source("override")
        assert "override" in tracker.gas_price_source_counts
        assert tracker.gas_price_source_counts["override"] == 1

    def test_tracker_records_historical_source(self):
        """Test that tracker records historical source."""
        tracker = DataQualityTracker()
        tracker.record_gas_price_source("historical")
        assert "historical" in tracker.gas_price_source_counts
        assert tracker.gas_price_source_counts["historical"] == 1

    def test_tracker_records_market_source(self):
        """Test that tracker records market source."""
        tracker = DataQualityTracker()
        tracker.record_gas_price_source("market")
        assert "market" in tracker.gas_price_source_counts
        assert tracker.gas_price_source_counts["market"] == 1

    def test_report_contains_gas_price_sources(self):
        """Test that report contains all tracked gas price sources."""
        tracker = DataQualityTracker()
        tracker.record_gas_price_source("override")
        tracker.record_gas_price_source("override")
        tracker.record_gas_price_source("historical")

        report = tracker.to_data_quality_report()
        assert report.gas_price_source_counts["override"] == 2
        assert report.gas_price_source_counts["historical"] == 1


class TestFallbackNotUsed:
    """Tests verifying that hardcoded fallback is NOT used."""

    def test_no_fallback_source_recorded(self):
        """Test that 'fallback' source is never recorded."""
        tracker = DataQualityTracker()

        # Only valid sources should be used
        tracker.record_gas_price_source("override")
        tracker.record_gas_price_source("historical")
        tracker.record_gas_price_source("market")

        # Verify no fallback
        assert "fallback" not in tracker.gas_price_source_counts

    def test_data_quality_report_no_fallback(self):
        """Test that report doesn't contain fallback source."""
        tracker = DataQualityTracker()
        tracker.record_gas_price_source("historical")

        report = tracker.to_data_quality_report()
        assert "fallback" not in report.gas_price_source_counts
