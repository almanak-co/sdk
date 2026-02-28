"""Tests for Price Aggregator.

This test suite covers:
- Single source aggregation
- Multiple source aggregation with median calculation
- Outlier detection (>2% deviation from median)
- Partial failure handling
- Total failure handling
- Source health metrics tracking
"""

import asyncio
from collections.abc import Coroutine
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, TypeVar

import pytest

from almanak.framework.data.interfaces import (
    AllDataSourcesFailed,
    BasePriceSource,
    DataSourceUnavailable,
    PriceResult,
)
from almanak.gateway.data.price.aggregator import (
    PriceAggregator,
    SourceHealthMetrics,
)

T = TypeVar("T")


def run_async[T](coro: Coroutine[Any, Any, T]) -> T:
    """Helper to run async functions in sync tests."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# =============================================================================
# Mock Price Source
# =============================================================================


class MockPriceSource(BasePriceSource):
    """Mock price source for testing."""

    def __init__(
        self,
        name: str,
        price: Decimal,
        confidence: float = 1.0,
        stale: bool = False,
        should_fail: bool = False,
        fail_error: str = "Mock error",
    ) -> None:
        self._name = name
        self._price = price
        self._confidence = confidence
        self._stale = stale
        self._should_fail = should_fail
        self._fail_error = fail_error
        self._call_count = 0

    async def get_price(self, token: str, quote: str = "USD") -> PriceResult:
        self._call_count += 1
        if self._should_fail:
            raise DataSourceUnavailable(
                source=self._name,
                reason=self._fail_error,
            )
        return PriceResult(
            price=self._price,
            source=self._name,
            timestamp=datetime.now(UTC),
            confidence=self._confidence,
            stale=self._stale,
        )

    @property
    def source_name(self) -> str:
        return self._name

    @property
    def supported_tokens(self) -> list[str]:
        return ["ETH", "WETH", "USDC", "ARB"]


# =============================================================================
# SourceHealthMetrics Tests
# =============================================================================


class TestSourceHealthMetrics:
    """Tests for SourceHealthMetrics class."""

    def test_initial_state(self) -> None:
        """Test initial metrics state."""
        metrics = SourceHealthMetrics(source_name="test")

        assert metrics.source_name == "test"
        assert metrics.total_requests == 0
        assert metrics.successful_requests == 0
        assert metrics.failed_requests == 0
        assert metrics.success_rate == 100.0
        assert metrics.average_latency_ms == 0.0

    def test_record_success(self) -> None:
        """Test recording successful requests."""
        metrics = SourceHealthMetrics(source_name="test")

        metrics.record_success(latency_ms=100.0)
        metrics.record_success(latency_ms=200.0)

        assert metrics.total_requests == 2
        assert metrics.successful_requests == 2
        assert metrics.failed_requests == 0
        assert metrics.success_rate == 100.0
        assert metrics.average_latency_ms == 150.0
        assert metrics.last_success_time is not None

    def test_record_failure(self) -> None:
        """Test recording failed requests."""
        metrics = SourceHealthMetrics(source_name="test")

        metrics.record_failure("Connection error")

        assert metrics.total_requests == 1
        assert metrics.successful_requests == 0
        assert metrics.failed_requests == 1
        assert metrics.success_rate == 0.0
        assert metrics.last_error == "Connection error"
        assert metrics.last_error_time is not None

    def test_mixed_success_and_failure(self) -> None:
        """Test metrics with mixed results."""
        metrics = SourceHealthMetrics(source_name="test")

        metrics.record_success(100.0)
        metrics.record_success(100.0)
        metrics.record_failure("Error")

        assert metrics.total_requests == 3
        assert metrics.successful_requests == 2
        assert metrics.failed_requests == 1
        assert metrics.success_rate == pytest.approx(66.67, rel=0.01)

    def test_to_dict(self) -> None:
        """Test metrics serialization."""
        metrics = SourceHealthMetrics(source_name="test")
        metrics.record_success(100.0)

        result = metrics.to_dict()

        assert result["source_name"] == "test"
        assert result["total_requests"] == 1
        assert result["successful_requests"] == 1
        assert result["success_rate"] == 100.0
        assert result["average_latency_ms"] == 100.0
        assert result["last_success_time"] is not None


# =============================================================================
# PriceAggregator Initialization Tests
# =============================================================================


class TestPriceAggregatorInit:
    """Tests for PriceAggregator initialization."""

    def test_single_source_init(self) -> None:
        """Test initialization with single source."""
        source = MockPriceSource(name="mock", price=Decimal("2500"))
        aggregator = PriceAggregator(sources=[source])

        assert len(aggregator.sources) == 1
        assert aggregator.sources[0].source_name == "mock"

    def test_multiple_sources_init(self) -> None:
        """Test initialization with multiple sources."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2505")),
            MockPriceSource(name="source3", price=Decimal("2502")),
        ]
        aggregator = PriceAggregator(sources=sources)

        assert len(aggregator.sources) == 3

    def test_empty_sources_raises(self) -> None:
        """Test that empty sources list raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            PriceAggregator(sources=[])

        assert "At least one price source is required" in str(exc_info.value)

    def test_custom_thresholds(self) -> None:
        """Test custom threshold configuration."""
        source = MockPriceSource(name="mock", price=Decimal("2500"))
        aggregator = PriceAggregator(
            sources=[source],
            outlier_threshold=0.05,  # 5%
            stale_confidence_penalty=0.5,
            partial_failure_penalty=0.2,
        )

        assert aggregator._outlier_threshold == 0.05
        assert aggregator._stale_confidence_penalty == 0.5
        assert aggregator._partial_failure_penalty == 0.2


# =============================================================================
# Single Source Tests
# =============================================================================


class TestSingleSourceAggregation:
    """Tests for single source aggregation."""

    def test_single_source_success(self) -> None:
        """Test successful price fetch from single source."""
        source = MockPriceSource(name="mock", price=Decimal("2500.50"))
        aggregator = PriceAggregator(sources=[source])

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        assert result.price == Decimal("2500.50")
        assert result.source == "aggregated"
        assert result.confidence == 1.0
        assert result.stale is False

    def test_single_source_stale(self) -> None:
        """Test single source with stale data reduces confidence."""
        source = MockPriceSource(name="mock", price=Decimal("2500"), stale=True)
        aggregator = PriceAggregator(sources=[source])

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        assert result.price == Decimal("2500")
        assert result.stale is True
        assert result.confidence < 1.0  # Reduced due to staleness

    def test_single_source_reduced_confidence(self) -> None:
        """Test single source with reduced confidence."""
        source = MockPriceSource(name="mock", price=Decimal("2500"), confidence=0.8)
        aggregator = PriceAggregator(sources=[source])

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        assert result.price == Decimal("2500")
        assert result.confidence <= 0.8

    def test_single_source_failure_raises(self) -> None:
        """Test single source failure raises AllDataSourcesFailed."""
        source = MockPriceSource(
            name="mock",
            price=Decimal("0"),
            should_fail=True,
            fail_error="API down",
        )
        aggregator = PriceAggregator(sources=[source])

        with pytest.raises(AllDataSourcesFailed) as exc_info:
            run_async(aggregator.get_aggregated_price("ETH", "USD"))

        assert "mock" in exc_info.value.errors


# =============================================================================
# Multiple Source Tests
# =============================================================================


class TestMultipleSourceAggregation:
    """Tests for multiple source aggregation."""

    def test_multiple_sources_median(self) -> None:
        """Test median calculation with multiple sources."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2510")),
            MockPriceSource(name="source3", price=Decimal("2505")),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # Median of [2500, 2505, 2510] = 2505
        assert result.price == Decimal("2505")
        assert result.confidence == 1.0

    def test_multiple_sources_even_count(self) -> None:
        """Test median with even number of sources."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2510")),
            MockPriceSource(name="source3", price=Decimal("2505")),
            MockPriceSource(name="source4", price=Decimal("2515")),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # Median of [2500, 2505, 2510, 2515] = (2505 + 2510) / 2 = 2507.5
        assert result.price == Decimal("2507.5")

    def test_multiple_sources_all_same_price(self) -> None:
        """Test aggregation when all sources return same price."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2500")),
            MockPriceSource(name="source3", price=Decimal("2500")),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        assert result.price == Decimal("2500")
        assert result.confidence == 1.0


# =============================================================================
# Outlier Detection Tests
# =============================================================================


class TestOutlierDetection:
    """Tests for outlier detection (>2% deviation from median)."""

    def test_outlier_detected_high(self) -> None:
        """Test outlier detection for high price deviation."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2505")),
            MockPriceSource(name="source3", price=Decimal("2600")),  # ~4% deviation
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # Outlier should be filtered, median of [2500, 2505] = 2502.5
        assert result.price == Decimal("2502.5")

    def test_outlier_detected_low(self) -> None:
        """Test outlier detection for low price deviation."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2400")),  # ~4% deviation
            MockPriceSource(name="source2", price=Decimal("2500")),
            MockPriceSource(name="source3", price=Decimal("2505")),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # Outlier should be filtered, median of [2500, 2505] = 2502.5
        assert result.price == Decimal("2502.5")

    def test_no_outlier_within_threshold(self) -> None:
        """Test no outlier detected when within 2% threshold."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2530")),  # ~1.2% deviation
            MockPriceSource(name="source3", price=Decimal("2510")),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # All within threshold, median of [2500, 2510, 2530] = 2510
        assert result.price == Decimal("2510")
        assert result.confidence == 1.0

    def test_all_outliers_uses_all(self) -> None:
        """Test that if all prices are 'outliers' relative to each other, all are used."""
        # These prices are all >2% apart from each other
        sources = [
            MockPriceSource(name="source1", price=Decimal("2000")),
            MockPriceSource(name="source2", price=Decimal("2500")),
            MockPriceSource(name="source3", price=Decimal("3000")),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # All flagged as outliers from median 2500, but we use all when none remain
        # Median of [2000, 2500, 3000] = 2500
        assert result.price == Decimal("2500")

    def test_outlier_reduces_confidence(self) -> None:
        """Test that outliers slightly reduce confidence."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2505")),
            MockPriceSource(name="source3", price=Decimal("3000")),  # Clear outlier
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # Confidence slightly reduced due to outlier
        assert result.confidence < 1.0


# =============================================================================
# Partial Failure Tests
# =============================================================================


class TestPartialFailure:
    """Tests for partial failure handling."""

    def test_one_source_fails_others_succeed(self) -> None:
        """Test aggregation continues when one source fails."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2505")),
            MockPriceSource(name="source3", price=Decimal("0"), should_fail=True),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # Should still return median of working sources
        assert result.price == Decimal("2502.5")

    def test_partial_failure_reduces_confidence(self) -> None:
        """Test that partial failure reduces confidence."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2505")),
            MockPriceSource(name="source3", price=Decimal("0"), should_fail=True),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # Confidence reduced due to partial failure
        assert result.confidence < 1.0

    def test_multiple_failures_still_succeeds(self) -> None:
        """Test aggregation with multiple failures but at least one success."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("0"), should_fail=True),
            MockPriceSource(name="source3", price=Decimal("0"), should_fail=True),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        assert result.price == Decimal("2500")
        # Confidence significantly reduced
        assert result.confidence < 0.9

    def test_stale_sources_reduce_confidence(self) -> None:
        """Test that stale sources reduce confidence."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2505"), stale=True),
            MockPriceSource(name="source3", price=Decimal("2502"), stale=True),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # Stale data flag should be set
        assert result.stale is True
        # Confidence reduced
        assert result.confidence < 1.0


# =============================================================================
# Total Failure Tests
# =============================================================================


class TestTotalFailure:
    """Tests for total failure handling."""

    def test_all_sources_fail_raises(self) -> None:
        """Test AllDataSourcesFailed when all sources fail."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("0"), should_fail=True, fail_error="Error 1"),
            MockPriceSource(name="source2", price=Decimal("0"), should_fail=True, fail_error="Error 2"),
            MockPriceSource(name="source3", price=Decimal("0"), should_fail=True, fail_error="Error 3"),
        ]
        aggregator = PriceAggregator(sources=sources)

        with pytest.raises(AllDataSourcesFailed) as exc_info:
            run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # All errors should be in the exception
        assert "source1" in exc_info.value.errors
        assert "source2" in exc_info.value.errors
        assert "source3" in exc_info.value.errors

    def test_all_sources_fail_error_messages(self) -> None:
        """Test that error messages are captured correctly."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("0"), should_fail=True, fail_error="API timeout"),
            MockPriceSource(name="source2", price=Decimal("0"), should_fail=True, fail_error="Rate limited"),
        ]
        aggregator = PriceAggregator(sources=sources)

        with pytest.raises(AllDataSourcesFailed) as exc_info:
            run_async(aggregator.get_aggregated_price("ETH", "USD"))

        assert "API timeout" in exc_info.value.errors["source1"]
        assert "Rate limited" in exc_info.value.errors["source2"]


# =============================================================================
# Health Metrics Tests
# =============================================================================


class TestHealthMetrics:
    """Tests for source health metrics tracking."""

    def test_health_metrics_tracked_on_success(self) -> None:
        """Test that health metrics are tracked on success."""
        source = MockPriceSource(name="mock", price=Decimal("2500"))
        aggregator = PriceAggregator(sources=[source])

        run_async(aggregator.get_aggregated_price("ETH", "USD"))

        health = aggregator.get_source_health("mock")
        assert health is not None
        assert health["total_requests"] == 1
        assert health["successful_requests"] == 1
        assert health["success_rate"] == 100.0

    def test_health_metrics_tracked_on_failure(self) -> None:
        """Test that health metrics are tracked on failure."""
        source = MockPriceSource(name="mock", price=Decimal("0"), should_fail=True)
        aggregator = PriceAggregator(sources=[source])

        with pytest.raises(AllDataSourcesFailed):
            run_async(aggregator.get_aggregated_price("ETH", "USD"))

        health = aggregator.get_source_health("mock")
        assert health is not None
        assert health["total_requests"] == 1
        assert health["failed_requests"] == 1
        assert health["success_rate"] == 0.0

    def test_get_all_source_health(self) -> None:
        """Test getting health for all sources."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2505")),
        ]
        aggregator = PriceAggregator(sources=sources)

        run_async(aggregator.get_aggregated_price("ETH", "USD"))

        all_health = aggregator.get_all_source_health()
        assert "source1" in all_health
        assert "source2" in all_health
        assert all_health["source1"]["successful_requests"] == 1
        assert all_health["source2"]["successful_requests"] == 1

    def test_get_source_health_unknown(self) -> None:
        """Test getting health for unknown source returns None."""
        source = MockPriceSource(name="mock", price=Decimal("2500"))
        aggregator = PriceAggregator(sources=[source])

        health = aggregator.get_source_health("unknown")
        assert health is None

    def test_reset_health_metrics_single(self) -> None:
        """Test resetting health metrics for single source."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2505")),
        ]
        aggregator = PriceAggregator(sources=sources)

        run_async(aggregator.get_aggregated_price("ETH", "USD"))
        aggregator.reset_health_metrics("source1")

        health1 = aggregator.get_source_health("source1")
        health2 = aggregator.get_source_health("source2")

        assert health1 is not None
        assert health2 is not None
        assert health1["total_requests"] == 0  # Reset
        assert health2["total_requests"] == 1  # Not reset

    def test_reset_health_metrics_all(self) -> None:
        """Test resetting health metrics for all sources."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2505")),
        ]
        aggregator = PriceAggregator(sources=sources)

        run_async(aggregator.get_aggregated_price("ETH", "USD"))
        aggregator.reset_health_metrics()

        health1 = aggregator.get_source_health("source1")
        health2 = aggregator.get_source_health("source2")

        assert health1 is not None
        assert health2 is not None
        assert health1["total_requests"] == 0
        assert health2["total_requests"] == 0


# =============================================================================
# Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_two_sources_median(self) -> None:
        """Test median with exactly two sources."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2510")),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # Median of [2500, 2510] = 2505
        assert result.price == Decimal("2505")

    def test_concurrent_fetch(self) -> None:
        """Test that sources are fetched concurrently."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("2500")),
            MockPriceSource(name="source2", price=Decimal("2505")),
            MockPriceSource(name="source3", price=Decimal("2502")),
        ]
        aggregator = PriceAggregator(sources=sources)

        run_async(aggregator.get_aggregated_price("ETH", "USD"))

        # All sources should have been called
        for source in sources:
            assert source._call_count == 1

    def test_very_small_prices(self) -> None:
        """Test aggregation with very small prices (like some tokens)."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("0.00001")),
            MockPriceSource(name="source2", price=Decimal("0.000011")),
            MockPriceSource(name="source3", price=Decimal("0.0000105")),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("SHIB", "USD"))

        # Median of [0.00001, 0.0000105, 0.000011] = 0.0000105
        assert result.price == Decimal("0.0000105")

    def test_very_large_prices(self) -> None:
        """Test aggregation with very large prices (like BTC)."""
        sources = [
            MockPriceSource(name="source1", price=Decimal("95000")),
            MockPriceSource(name="source2", price=Decimal("95100")),
            MockPriceSource(name="source3", price=Decimal("95050")),
        ]
        aggregator = PriceAggregator(sources=sources)

        result = run_async(aggregator.get_aggregated_price("BTC", "USD"))

        # Median of [95000, 95050, 95100] = 95050
        assert result.price == Decimal("95050")
