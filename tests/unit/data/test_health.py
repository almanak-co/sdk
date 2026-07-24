"""Unit tests for the health-report dataclasses in ``almanak.framework.data.health``.

Covers every validation branch of ``SourceHealth.__post_init__``,
``CacheStats.__post_init__``, ``HealthReport.__post_init__`` and every status
branch of ``HealthReport.calculate_overall_status``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from almanak.framework.data.health import CacheStats, HealthReport, SourceHealth


def _source(
    name: str = "coingecko",
    success_rate: float = 1.0,
    latency_p50_ms: float = 10.0,
    latency_p95_ms: float = 50.0,
    error_count: int = 0,
    total_requests: int = 100,
) -> SourceHealth:
    return SourceHealth(
        name=name,
        success_rate=success_rate,
        latency_p50_ms=latency_p50_ms,
        latency_p95_ms=latency_p95_ms,
        error_count=error_count,
        last_success=datetime.now(UTC),
        total_requests=total_requests,
    )


class TestSourceHealthValidation:
    def test_valid_boundary_values_accepted(self) -> None:
        # 0.0 and 1.0 are both inside the allowed success-rate range.
        assert _source(success_rate=0.0).success_rate == 0.0
        assert _source(success_rate=1.0).success_rate == 1.0
        zeroed = _source(latency_p50_ms=0.0, latency_p95_ms=0.0, error_count=0, total_requests=0)
        assert zeroed.total_requests == 0

    @pytest.mark.parametrize("rate", [-0.01, 1.01])
    def test_success_rate_out_of_range_rejected(self, rate: float) -> None:
        with pytest.raises(ValueError, match="success_rate must be between 0 and 1"):
            _source(success_rate=rate)

    def test_negative_latency_p50_rejected(self) -> None:
        with pytest.raises(ValueError, match="latency_p50_ms must be non-negative"):
            _source(latency_p50_ms=-1.0)

    def test_negative_latency_p95_rejected(self) -> None:
        with pytest.raises(ValueError, match="latency_p95_ms must be non-negative"):
            _source(latency_p95_ms=-0.5)

    def test_negative_error_count_rejected(self) -> None:
        with pytest.raises(ValueError, match="error_count must be non-negative"):
            _source(error_count=-1)

    def test_negative_total_requests_rejected(self) -> None:
        with pytest.raises(ValueError, match="total_requests must be non-negative"):
            _source(total_requests=-5)


class TestCacheStatsValidation:
    def test_defaults_are_valid(self) -> None:
        stats = CacheStats()
        assert stats.hits == 0
        assert stats.max_size is None
        assert stats.hit_rate == 0.0

    def test_unlimited_cache_max_size_none_accepted(self) -> None:
        stats = CacheStats(hits=3, misses=1, size=4, max_size=None)
        assert stats.hit_rate == 0.75

    def test_negative_hits_rejected(self) -> None:
        with pytest.raises(ValueError, match="hits must be non-negative"):
            CacheStats(hits=-1)

    def test_negative_misses_rejected(self) -> None:
        with pytest.raises(ValueError, match="misses must be non-negative"):
            CacheStats(misses=-2)

    def test_negative_size_rejected(self) -> None:
        with pytest.raises(ValueError, match="size must be non-negative"):
            CacheStats(size=-3)

    def test_negative_max_size_rejected(self) -> None:
        with pytest.raises(ValueError, match="max_size must be non-negative"):
            CacheStats(max_size=-1)


class TestHealthReportValidation:
    def test_valid_statuses_accepted(self) -> None:
        for status in ("healthy", "degraded", "unhealthy"):
            report = HealthReport(
                timestamp=datetime.now(UTC),
                sources={},
                cache_stats=CacheStats(),
                overall_status=status,
            )
            assert report.overall_status == status

    def test_invalid_status_rejected(self) -> None:
        with pytest.raises(ValueError, match="overall_status must be one of"):
            HealthReport(
                timestamp=datetime.now(UTC),
                sources={},
                cache_stats=CacheStats(),
                overall_status="on-fire",
            )


class TestCalculateOverallStatus:
    def test_no_sources_is_healthy(self) -> None:
        assert HealthReport.calculate_overall_status({}) == "healthy"

    def test_all_healthy_sources(self) -> None:
        sources = {
            "a": _source(success_rate=1.0),
            "b": _source(success_rate=0.9),  # 90% boundary is healthy
        }
        assert HealthReport.calculate_overall_status(sources) == "healthy"

    def test_one_degraded_source_degrades_overall(self) -> None:
        sources = {
            "a": _source(success_rate=1.0),
            "b": _source(success_rate=0.7),
        }
        assert HealthReport.calculate_overall_status(sources) == "degraded"

    def test_any_failing_source_is_unhealthy(self) -> None:
        sources = {
            "a": _source(success_rate=1.0),
            "b": _source(success_rate=0.7),
            "c": _source(success_rate=0.1),
        }
        assert HealthReport.calculate_overall_status(sources) == "unhealthy"

    def test_failing_short_circuits_even_after_degraded(self) -> None:
        # Degraded seen first, failing later — failing still wins.
        sources = {
            "degraded": _source(success_rate=0.6),
            "failing": _source(success_rate=0.4),
        }
        assert HealthReport.calculate_overall_status(sources) == "unhealthy"
