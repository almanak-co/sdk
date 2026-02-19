"""Integration tests for data coverage reporting in backtesting.

This module tests the data coverage metrics system to validate that:
1. data_coverage_pct is calculated correctly across position types
2. Confidence flags (HIGH, MEDIUM, LOW) are properly set for each data type
3. Data source strings are populated and tracked correctly
4. Mixed real and fallback data scenarios are handled properly

Tests require THEGRAPH_API_KEY environment variable for subgraph access to
get HIGH confidence data. Tests with fallback scenarios don't require the key.

Example:
    # Run tests
    pytest tests/integration/backtesting/test_data_coverage.py -v -m integration

    # Run with live API (requires THEGRAPH_API_KEY)
    THEGRAPH_API_KEY=your_key pytest -m integration -v
"""

import logging
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.models import (
    DataCoverageMetrics,
    LendingMetrics,
    LPMetrics,
    PerpMetrics,
    SlippageMetrics,
)
from almanak.framework.backtesting.pnl.portfolio import (
    PositionType,
    SimulatedPortfolio,
    SimulatedPosition,
)
from almanak.framework.backtesting.pnl.types import DataConfidence

logger = logging.getLogger(__name__)


# =============================================================================
# Test Constants
# =============================================================================

# Well-known pool/market addresses for testing
WETH_USDC_POOL_ETHEREUM = "0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8"

# Test date range
TEST_TIMESTAMP = datetime(2024, 1, 5, 12, 0, 0, tzinfo=UTC)


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def has_thegraph_api_key() -> bool:
    """Check if The Graph API key is available."""
    return bool(os.environ.get("THEGRAPH_API_KEY"))


@pytest.fixture
def lp_position_high_confidence() -> SimulatedPosition:
    """Create an LP position with HIGH confidence data."""
    position = SimulatedPosition(
        position_type=PositionType.LP,
        protocol="uniswap_v3",
        tokens=["WETH", "USDC"],
        amounts={"WETH": Decimal("1"), "USDC": Decimal("3000")},
        entry_price=Decimal("3000"),
        entry_time=TEST_TIMESTAMP - timedelta(days=1),
        position_id="test_lp_high",
        fee_confidence="high",
        slippage_confidence="high",
    )
    position.metadata["data_source"] = "uniswap_v3_subgraph"
    position.metadata["pool_address"] = WETH_USDC_POOL_ETHEREUM
    position.metadata["chain"] = "ethereum"
    return position


@pytest.fixture
def lp_position_low_confidence() -> SimulatedPosition:
    """Create an LP position with LOW confidence (fallback) data."""
    position = SimulatedPosition(
        position_type=PositionType.LP,
        protocol="unknown_dex",
        tokens=["WETH", "USDC"],
        amounts={"WETH": Decimal("1"), "USDC": Decimal("3000")},
        entry_price=Decimal("3000"),
        entry_time=TEST_TIMESTAMP - timedelta(days=1),
        position_id="test_lp_low",
        fee_confidence="low",
        slippage_confidence="low",
    )
    position.metadata["data_source"] = "fallback"
    position.metadata["pool_address"] = "0x1234567890abcdef"
    position.metadata["chain"] = "ethereum"
    return position


@pytest.fixture
def perp_position_high_confidence() -> SimulatedPosition:
    """Create a perp position with HIGH confidence funding data."""
    return SimulatedPosition(
        position_type=PositionType.PERP_LONG,
        protocol="hyperliquid",
        tokens=["ETH"],
        amounts={"ETH": Decimal("1")},
        entry_price=Decimal("3000"),
        entry_time=TEST_TIMESTAMP - timedelta(days=1),
        position_id="test_perp_high",
        leverage=Decimal("5"),
        collateral_usd=Decimal("1000"),
        notional_usd=Decimal("5000"),
        funding_confidence="high",
        funding_data_source="hyperliquid_funding_history",
    )


@pytest.fixture
def perp_position_medium_confidence() -> SimulatedPosition:
    """Create a perp position with MEDIUM confidence funding data (GMX style)."""
    return SimulatedPosition(
        position_type=PositionType.PERP_SHORT,
        protocol="gmx_v2",
        tokens=["BTC"],
        amounts={"BTC": Decimal("-0.1")},
        entry_price=Decimal("50000"),
        entry_time=TEST_TIMESTAMP - timedelta(days=1),
        position_id="test_perp_medium",
        leverage=Decimal("10"),
        collateral_usd=Decimal("500"),
        notional_usd=Decimal("5000"),
        funding_confidence="medium",
        funding_data_source="gmx_markets_api",
    )


@pytest.fixture
def lending_position_high_confidence() -> SimulatedPosition:
    """Create a lending position with HIGH confidence APY data."""
    return SimulatedPosition(
        position_type=PositionType.SUPPLY,
        protocol="aave_v3",
        tokens=["USDC"],
        amounts={"USDC": Decimal("10000")},
        entry_price=Decimal("1"),
        entry_time=TEST_TIMESTAMP - timedelta(days=1),
        position_id="test_lending_high",
        apy_confidence="high",
        apy_data_source="aave_v3_subgraph",
    )


@pytest.fixture
def lending_position_low_confidence() -> SimulatedPosition:
    """Create a lending position with LOW confidence (fallback) APY data."""
    return SimulatedPosition(
        position_type=PositionType.BORROW,
        protocol="unknown_protocol",
        tokens=["USDC"],
        amounts={"USDC": Decimal("-5000")},
        entry_price=Decimal("1"),
        entry_time=TEST_TIMESTAMP - timedelta(days=1),
        position_id="test_lending_low",
        apy_confidence="low",
        apy_data_source="fallback",
    )


# =============================================================================
# Integration Tests - Data Coverage Metrics
# =============================================================================


@pytest.mark.integration
class TestDataCoverageMetrics:
    """Integration tests for data coverage metrics calculation."""

    def test_portfolio_calculates_data_coverage_with_mixed_confidence(
        self,
        lp_position_high_confidence: SimulatedPosition,
        lp_position_low_confidence: SimulatedPosition,
        perp_position_high_confidence: SimulatedPosition,
        perp_position_medium_confidence: SimulatedPosition,
        lending_position_high_confidence: SimulatedPosition,
        lending_position_low_confidence: SimulatedPosition,
    ) -> None:
        """Test data_coverage_pct calculation with mixed real and fallback data.

        This test validates that the portfolio correctly calculates the overall
        data coverage percentage when some positions use real historical data
        (HIGH confidence) and others use fallback data (LOW confidence).
        """
        # Create portfolio with mixed confidence positions
        portfolio = SimulatedPortfolio(
            positions=[
                lp_position_high_confidence,
                lp_position_low_confidence,
                perp_position_high_confidence,
                perp_position_medium_confidence,
                lending_position_high_confidence,
                lending_position_low_confidence,
            ],
            initial_capital_usd=Decimal("100000"),
        )

        # Calculate data coverage metrics
        metrics = portfolio.calculate_data_coverage_metrics()

        # Validate metrics are returned
        assert isinstance(metrics, DataCoverageMetrics)

        # Log the metrics for debugging
        logger.info(
            "Data Coverage: %.1f%% (total=%d, high=%d)",
            metrics.data_coverage_pct,
            metrics.total_data_points,
            metrics.high_confidence_data_points,
        )

        # Validate LP metrics
        assert metrics.lp_metrics.position_count == 2
        assert metrics.lp_metrics.fee_confidence_breakdown["high"] == 1
        assert metrics.lp_metrics.fee_confidence_breakdown["low"] == 1

        # Validate perp metrics
        assert metrics.perp_metrics.position_count == 2
        assert metrics.perp_metrics.funding_confidence_breakdown["high"] == 1
        assert metrics.perp_metrics.funding_confidence_breakdown["medium"] == 1

        # Validate lending metrics
        assert metrics.lending_metrics.position_count == 2
        assert metrics.lending_metrics.apy_confidence_breakdown["high"] == 1
        assert metrics.lending_metrics.apy_confidence_breakdown["low"] == 1

        # Validate total data points (6 positions + 2 LP slippage calculations)
        # LP positions contribute both fee_confidence and slippage_confidence
        # Total: 2 LP + 2 Perp + 2 Lending + 2 Slippage = 8 data points
        expected_total = 6 + 2  # 6 positions + 2 slippage calculations
        assert metrics.total_data_points == expected_total, (
            f"Expected {expected_total} total data points, got {metrics.total_data_points}"
        )

        # Validate data coverage percentage
        # HIGH confidence: 1 LP fee + 1 perp funding + 1 lending APY + 1 slippage = 4
        expected_high = 4  # 1 LP, 1 perp, 1 lending, 1 slippage
        assert metrics.high_confidence_data_points == expected_high, (
            f"Expected {expected_high} HIGH confidence points, got {metrics.high_confidence_data_points}"
        )

        # Coverage should be ~50% (4 out of 8 data points)
        expected_coverage = (expected_high / expected_total) * 100
        assert abs(metrics.data_coverage_pct - expected_coverage) < 1.0, (
            f"Expected ~{expected_coverage:.1f}% coverage, got {metrics.data_coverage_pct:.1f}%"
        )

    def test_portfolio_with_all_high_confidence_shows_100_percent_coverage(
        self,
        lp_position_high_confidence: SimulatedPosition,
        perp_position_high_confidence: SimulatedPosition,
        lending_position_high_confidence: SimulatedPosition,
    ) -> None:
        """Test that all HIGH confidence positions result in 100% coverage."""
        portfolio = SimulatedPortfolio(
            positions=[
                lp_position_high_confidence,
                perp_position_high_confidence,
                lending_position_high_confidence,
            ],
            initial_capital_usd=Decimal("100000"),
        )

        metrics = portfolio.calculate_data_coverage_metrics()

        # All positions are HIGH confidence
        assert metrics.lp_metrics.fee_confidence_breakdown["high"] == 1
        assert metrics.perp_metrics.funding_confidence_breakdown["high"] == 1
        assert metrics.lending_metrics.apy_confidence_breakdown["high"] == 1

        # Coverage should be 100%
        assert metrics.data_coverage_pct == 100.0, (
            f"Expected 100% coverage, got {metrics.data_coverage_pct:.1f}%"
        )

        logger.info("All HIGH confidence: %.1f%% coverage", metrics.data_coverage_pct)

    def test_portfolio_with_all_low_confidence_shows_0_percent_coverage(
        self,
        lp_position_low_confidence: SimulatedPosition,
        lending_position_low_confidence: SimulatedPosition,
    ) -> None:
        """Test that all LOW confidence positions result in 0% coverage."""
        portfolio = SimulatedPortfolio(
            positions=[
                lp_position_low_confidence,
                lending_position_low_confidence,
            ],
            initial_capital_usd=Decimal("100000"),
        )

        metrics = portfolio.calculate_data_coverage_metrics()

        # All positions are LOW confidence
        assert metrics.lp_metrics.fee_confidence_breakdown["low"] == 1
        assert metrics.lending_metrics.apy_confidence_breakdown["low"] == 1

        # Coverage should be 0%
        assert metrics.data_coverage_pct == 0.0, (
            f"Expected 0% coverage, got {metrics.data_coverage_pct:.1f}%"
        )

        logger.info("All LOW confidence: %.1f%% coverage", metrics.data_coverage_pct)

    def test_empty_portfolio_shows_100_percent_coverage(self) -> None:
        """Test that empty portfolio shows 100% coverage (vacuously true)."""
        portfolio = SimulatedPortfolio(
            positions=[],
            initial_capital_usd=Decimal("100000"),
        )

        metrics = portfolio.calculate_data_coverage_metrics()

        # No data points = no coverage issues
        assert metrics.total_data_points == 0
        assert metrics.data_coverage_pct == 100.0, (
            "Empty portfolio should show 100% coverage (vacuously true)"
        )

        logger.info("Empty portfolio: %.1f%% coverage", metrics.data_coverage_pct)


@pytest.mark.integration
class TestConfidenceFlags:
    """Integration tests for confidence flag tracking per data type."""

    def test_lp_fee_confidence_flag_is_set_correctly(
        self,
        lp_position_high_confidence: SimulatedPosition,
        lp_position_low_confidence: SimulatedPosition,
    ) -> None:
        """Test that LP fee_confidence flags are tracked correctly."""
        portfolio = SimulatedPortfolio(
            positions=[lp_position_high_confidence, lp_position_low_confidence],
            initial_capital_usd=Decimal("100000"),
        )

        metrics = portfolio.calculate_data_coverage_metrics()

        # Validate fee confidence breakdown
        assert metrics.lp_metrics.fee_confidence_breakdown["high"] == 1
        assert metrics.lp_metrics.fee_confidence_breakdown["low"] == 1
        assert metrics.lp_metrics.fee_confidence_breakdown["medium"] == 0

        logger.info(
            "LP fee confidence breakdown: %s",
            metrics.lp_metrics.fee_confidence_breakdown,
        )

    def test_lp_slippage_confidence_flag_is_set_correctly(
        self,
        lp_position_high_confidence: SimulatedPosition,
        lp_position_low_confidence: SimulatedPosition,
    ) -> None:
        """Test that LP slippage_confidence flags are tracked correctly."""
        portfolio = SimulatedPortfolio(
            positions=[lp_position_high_confidence, lp_position_low_confidence],
            initial_capital_usd=Decimal("100000"),
        )

        metrics = portfolio.calculate_data_coverage_metrics()

        # Validate slippage confidence breakdown
        assert metrics.slippage_metrics.slippage_confidence_breakdown["high"] == 1
        assert metrics.slippage_metrics.slippage_confidence_breakdown["low"] == 1

        logger.info(
            "Slippage confidence breakdown: %s",
            metrics.slippage_metrics.slippage_confidence_breakdown,
        )

    def test_perp_funding_confidence_flag_is_set_correctly(
        self,
        perp_position_high_confidence: SimulatedPosition,
        perp_position_medium_confidence: SimulatedPosition,
    ) -> None:
        """Test that perp funding_confidence flags are tracked correctly."""
        portfolio = SimulatedPortfolio(
            positions=[perp_position_high_confidence, perp_position_medium_confidence],
            initial_capital_usd=Decimal("100000"),
        )

        metrics = portfolio.calculate_data_coverage_metrics()

        # Validate funding confidence breakdown
        assert metrics.perp_metrics.funding_confidence_breakdown["high"] == 1
        assert metrics.perp_metrics.funding_confidence_breakdown["medium"] == 1
        assert metrics.perp_metrics.funding_confidence_breakdown["low"] == 0

        logger.info(
            "Perp funding confidence breakdown: %s",
            metrics.perp_metrics.funding_confidence_breakdown,
        )

    def test_lending_apy_confidence_flag_is_set_correctly(
        self,
        lending_position_high_confidence: SimulatedPosition,
        lending_position_low_confidence: SimulatedPosition,
    ) -> None:
        """Test that lending apy_confidence flags are tracked correctly."""
        portfolio = SimulatedPortfolio(
            positions=[lending_position_high_confidence, lending_position_low_confidence],
            initial_capital_usd=Decimal("100000"),
        )

        metrics = portfolio.calculate_data_coverage_metrics()

        # Validate APY confidence breakdown
        assert metrics.lending_metrics.apy_confidence_breakdown["high"] == 1
        assert metrics.lending_metrics.apy_confidence_breakdown["low"] == 1
        assert metrics.lending_metrics.apy_confidence_breakdown["medium"] == 0

        logger.info(
            "Lending APY confidence breakdown: %s",
            metrics.lending_metrics.apy_confidence_breakdown,
        )


@pytest.mark.integration
class TestDataSourceTracking:
    """Integration tests for data source string tracking."""

    def test_lp_data_sources_are_populated(
        self,
        lp_position_high_confidence: SimulatedPosition,
        lp_position_low_confidence: SimulatedPosition,
    ) -> None:
        """Test that LP data sources are populated correctly."""
        portfolio = SimulatedPortfolio(
            positions=[lp_position_high_confidence, lp_position_low_confidence],
            initial_capital_usd=Decimal("100000"),
        )

        metrics = portfolio.calculate_data_coverage_metrics()

        # Both positions should have data sources tracked (extracted from metadata)
        # The calculate_data_coverage_metrics extracts from position.metadata.get("data_source")
        assert len(metrics.lp_metrics.data_sources) > 0, (
            "LP data sources should be populated"
        )

        logger.info("LP data sources: %s", metrics.lp_metrics.data_sources)

    def test_perp_data_sources_are_populated(
        self,
        perp_position_high_confidence: SimulatedPosition,
        perp_position_medium_confidence: SimulatedPosition,
    ) -> None:
        """Test that perp data sources are populated correctly."""
        portfolio = SimulatedPortfolio(
            positions=[perp_position_high_confidence, perp_position_medium_confidence],
            initial_capital_usd=Decimal("100000"),
        )

        metrics = portfolio.calculate_data_coverage_metrics()

        # Both positions have funding_data_source set
        assert len(metrics.perp_metrics.data_sources) == 2, (
            "Expected 2 unique perp data sources"
        )
        assert "hyperliquid_funding_history" in metrics.perp_metrics.data_sources
        assert "gmx_markets_api" in metrics.perp_metrics.data_sources

        logger.info("Perp data sources: %s", metrics.perp_metrics.data_sources)

    def test_lending_data_sources_are_populated(
        self,
        lending_position_high_confidence: SimulatedPosition,
        lending_position_low_confidence: SimulatedPosition,
    ) -> None:
        """Test that lending data sources are populated correctly."""
        portfolio = SimulatedPortfolio(
            positions=[lending_position_high_confidence, lending_position_low_confidence],
            initial_capital_usd=Decimal("100000"),
        )

        metrics = portfolio.calculate_data_coverage_metrics()

        # Both positions have apy_data_source set
        assert len(metrics.lending_metrics.data_sources) == 2, (
            "Expected 2 unique lending data sources"
        )
        assert "aave_v3_subgraph" in metrics.lending_metrics.data_sources
        assert "fallback" in metrics.lending_metrics.data_sources

        logger.info("Lending data sources: %s", metrics.lending_metrics.data_sources)


@pytest.mark.integration
class TestDataCoverageMetricsToDict:
    """Integration tests for DataCoverageMetrics serialization."""

    def test_metrics_to_dict_includes_all_fields(
        self,
        lp_position_high_confidence: SimulatedPosition,
        perp_position_high_confidence: SimulatedPosition,
        lending_position_high_confidence: SimulatedPosition,
    ) -> None:
        """Test that to_dict() includes all required fields."""
        portfolio = SimulatedPortfolio(
            positions=[
                lp_position_high_confidence,
                perp_position_high_confidence,
                lending_position_high_confidence,
            ],
            initial_capital_usd=Decimal("100000"),
        )

        metrics = portfolio.calculate_data_coverage_metrics()
        metrics_dict = metrics.to_dict()

        # Validate all required fields are present
        assert "lp_metrics" in metrics_dict
        assert "perp_metrics" in metrics_dict
        assert "lending_metrics" in metrics_dict
        assert "slippage_metrics" in metrics_dict
        assert "data_coverage_pct" in metrics_dict
        assert "total_data_points" in metrics_dict
        assert "high_confidence_data_points" in metrics_dict

        # Validate nested structure
        assert "fee_confidence_breakdown" in metrics_dict["lp_metrics"]
        assert "data_sources" in metrics_dict["lp_metrics"]
        assert "funding_confidence_breakdown" in metrics_dict["perp_metrics"]
        assert "apy_confidence_breakdown" in metrics_dict["lending_metrics"]
        assert "slippage_confidence_breakdown" in metrics_dict["slippage_metrics"]

        logger.info("Metrics dict: %s", metrics_dict)


@pytest.mark.integration
class TestClosedPositionsIncludedInCoverage:
    """Integration tests for closed positions being included in coverage metrics."""

    def test_closed_positions_contribute_to_data_coverage(
        self,
        lp_position_high_confidence: SimulatedPosition,
        perp_position_medium_confidence: SimulatedPosition,
    ) -> None:
        """Test that closed positions are included in data coverage calculations."""
        portfolio = SimulatedPortfolio(
            positions=[lp_position_high_confidence],
            initial_capital_usd=Decimal("100000"),
        )

        # Simulate closing a position (move to _closed_positions)
        perp_position_medium_confidence.last_updated = TEST_TIMESTAMP
        portfolio._closed_positions.append(perp_position_medium_confidence)

        metrics = portfolio.calculate_data_coverage_metrics()

        # Both open and closed positions should be counted
        assert metrics.lp_metrics.position_count == 1
        assert metrics.perp_metrics.position_count == 1

        # Verify perp funding confidence from closed position is tracked
        assert metrics.perp_metrics.funding_confidence_breakdown["medium"] == 1

        logger.info(
            "Coverage with closed position: %.1f%% (LP=%d, Perp=%d)",
            metrics.data_coverage_pct,
            metrics.lp_metrics.position_count,
            metrics.perp_metrics.position_count,
        )


# =============================================================================
# Test Runner
# =============================================================================

if __name__ == "__main__":
    # Run with pytest
    pytest.main([__file__, "-v", "-m", "integration", "-s"])
