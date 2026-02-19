"""Health Report Module.

This module provides data structures for tracking data source health,
including latency metrics, success rates, and error tracking.

Key Components:
    - SourceHealth: Dataclass for individual source health metrics
    - CacheStats: Dataclass for cache performance statistics
    - HealthReport: Dataclass aggregating all health information
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class SourceHealth:
    """Health metrics for an individual data source.

    Tracks success rates, latency percentiles, and error information
    to provide visibility into data source reliability.

    Attributes:
        name: Name of the data source (e.g., "coingecko", "web3_rpc")
        success_rate: Success rate from 0.0 to 1.0 (1.0 = 100% success)
        latency_p50_ms: 50th percentile (median) latency in milliseconds
        latency_p95_ms: 95th percentile latency in milliseconds
        error_count: Total number of errors encountered
        last_success: Timestamp of last successful request
        last_error: Timestamp of last error (None if no errors)
        last_error_message: Message from the last error (None if no errors)
        total_requests: Total number of requests made
    """

    name: str
    success_rate: float
    latency_p50_ms: float
    latency_p95_ms: float
    error_count: int
    last_success: datetime | None
    last_error: datetime | None = None
    last_error_message: str | None = None
    total_requests: int = 0

    def __post_init__(self) -> None:
        """Validate field values."""
        if not 0.0 <= self.success_rate <= 1.0:
            raise ValueError(f"success_rate must be between 0 and 1, got {self.success_rate}")
        if self.latency_p50_ms < 0:
            raise ValueError(f"latency_p50_ms must be non-negative, got {self.latency_p50_ms}")
        if self.latency_p95_ms < 0:
            raise ValueError(f"latency_p95_ms must be non-negative, got {self.latency_p95_ms}")
        if self.error_count < 0:
            raise ValueError(f"error_count must be non-negative, got {self.error_count}")
        if self.total_requests < 0:
            raise ValueError(f"total_requests must be non-negative, got {self.total_requests}")

    @property
    def is_healthy(self) -> bool:
        """Check if source is considered healthy (>= 90% success rate)."""
        return self.success_rate >= 0.9

    @property
    def is_degraded(self) -> bool:
        """Check if source is degraded (50-90% success rate)."""
        return 0.5 <= self.success_rate < 0.9

    @property
    def is_failing(self) -> bool:
        """Check if source is failing (< 50% success rate)."""
        return self.success_rate < 0.5

    @property
    def time_since_last_success_seconds(self) -> float | None:
        """Calculate seconds since last successful request."""
        if self.last_success is None:
            return None
        return (datetime.now(UTC) - self.last_success).total_seconds()

    @property
    def time_since_last_error_seconds(self) -> float | None:
        """Calculate seconds since last error."""
        if self.last_error is None:
            return None
        return (datetime.now(UTC) - self.last_error).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "name": self.name,
            "success_rate": self.success_rate,
            "latency_p50_ms": self.latency_p50_ms,
            "latency_p95_ms": self.latency_p95_ms,
            "error_count": self.error_count,
            "last_success": self.last_success.isoformat() if self.last_success else None,
            "last_error": self.last_error.isoformat() if self.last_error else None,
            "last_error_message": self.last_error_message,
            "total_requests": self.total_requests,
            "is_healthy": self.is_healthy,
            "is_degraded": self.is_degraded,
            "is_failing": self.is_failing,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "SourceHealth":
        """Create SourceHealth from dictionary."""
        return cls(
            name=data["name"],
            success_rate=data["success_rate"],
            latency_p50_ms=data["latency_p50_ms"],
            latency_p95_ms=data["latency_p95_ms"],
            error_count=data["error_count"],
            last_success=(datetime.fromisoformat(data["last_success"]) if data.get("last_success") else None),
            last_error=(datetime.fromisoformat(data["last_error"]) if data.get("last_error") else None),
            last_error_message=data.get("last_error_message"),
            total_requests=data.get("total_requests", 0),
        )


@dataclass
class CacheStats:
    """Statistics for data cache performance.

    Attributes:
        hits: Number of cache hits
        misses: Number of cache misses
        size: Current number of items in cache
        max_size: Maximum cache capacity (None if unlimited)
        hit_rate: Cache hit rate from 0.0 to 1.0
    """

    hits: int = 0
    misses: int = 0
    size: int = 0
    max_size: int | None = None

    def __post_init__(self) -> None:
        """Validate field values."""
        if self.hits < 0:
            raise ValueError(f"hits must be non-negative, got {self.hits}")
        if self.misses < 0:
            raise ValueError(f"misses must be non-negative, got {self.misses}")
        if self.size < 0:
            raise ValueError(f"size must be non-negative, got {self.size}")
        if self.max_size is not None and self.max_size < 0:
            raise ValueError(f"max_size must be non-negative, got {self.max_size}")

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate."""
        total = self.hits + self.misses
        if total == 0:
            return 0.0
        return self.hits / total

    @property
    def total_requests(self) -> int:
        """Total cache requests (hits + misses)."""
        return self.hits + self.misses

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "hits": self.hits,
            "misses": self.misses,
            "size": self.size,
            "max_size": self.max_size,
            "hit_rate": self.hit_rate,
            "total_requests": self.total_requests,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "CacheStats":
        """Create CacheStats from dictionary."""
        return cls(
            hits=data["hits"],
            misses=data["misses"],
            size=data["size"],
            max_size=data.get("max_size"),
        )


# Overall status type
OverallStatus = Literal["healthy", "degraded", "unhealthy"]


@dataclass
class HealthReport:
    """Aggregated health report for all data sources.

    Provides a comprehensive view of data system health, including
    individual source health, cache statistics, and overall status.

    Attributes:
        timestamp: When the health report was generated
        sources: Dictionary mapping source names to their health metrics
        cache_stats: Cache performance statistics
        overall_status: Overall health status ("healthy", "degraded", "unhealthy")
    """

    timestamp: datetime
    sources: dict[str, SourceHealth]
    cache_stats: CacheStats
    overall_status: OverallStatus

    def __post_init__(self) -> None:
        """Validate overall_status value."""
        valid_statuses = ("healthy", "degraded", "unhealthy")
        if self.overall_status not in valid_statuses:
            raise ValueError(f"overall_status must be one of {valid_statuses}, got {self.overall_status}")

    @property
    def healthy_sources(self) -> list[str]:
        """Get names of healthy sources."""
        return [name for name, health in self.sources.items() if health.is_healthy]

    @property
    def degraded_sources(self) -> list[str]:
        """Get names of degraded sources."""
        return [name for name, health in self.sources.items() if health.is_degraded]

    @property
    def failing_sources(self) -> list[str]:
        """Get names of failing sources."""
        return [name for name, health in self.sources.items() if health.is_failing]

    @property
    def source_count(self) -> int:
        """Total number of registered sources."""
        return len(self.sources)

    @property
    def average_success_rate(self) -> float:
        """Calculate average success rate across all sources."""
        if not self.sources:
            return 0.0
        return sum(s.success_rate for s in self.sources.values()) / len(self.sources)

    @classmethod
    def calculate_overall_status(cls, sources: dict[str, SourceHealth]) -> OverallStatus:
        """Calculate overall status based on source health.

        Logic:
        - "healthy": All sources have >= 90% success rate
        - "degraded": At least one source has 50-90% success rate, none failing
        - "unhealthy": At least one source has < 50% success rate

        Args:
            sources: Dictionary of source health metrics

        Returns:
            Overall status string
        """
        if not sources:
            return "healthy"  # No sources = healthy (nothing to fail)

        has_degraded = False
        for health in sources.values():
            if health.is_failing:
                return "unhealthy"
            if health.is_degraded:
                has_degraded = True

        return "degraded" if has_degraded else "healthy"

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "sources": {name: health.to_dict() for name, health in self.sources.items()},
            "cache_stats": self.cache_stats.to_dict(),
            "overall_status": self.overall_status,
            "healthy_sources": self.healthy_sources,
            "degraded_sources": self.degraded_sources,
            "failing_sources": self.failing_sources,
            "source_count": self.source_count,
            "average_success_rate": self.average_success_rate,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HealthReport":
        """Create HealthReport from dictionary."""
        return cls(
            timestamp=datetime.fromisoformat(data["timestamp"]),
            sources={name: SourceHealth.from_dict(health_data) for name, health_data in data["sources"].items()},
            cache_stats=CacheStats.from_dict(data["cache_stats"]),
            overall_status=data["overall_status"],
        )


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "SourceHealth",
    "CacheStats",
    "HealthReport",
    "OverallStatus",
]
