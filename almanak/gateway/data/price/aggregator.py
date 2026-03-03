"""Price Aggregator for multi-source price validation and aggregation.

This module provides a production-ready price aggregator that combines prices
from multiple sources, detects outliers, and handles partial failures gracefully.

Key Features:
    - Single source support with confidence based on staleness
    - Multi-source aggregation using median price
    - Outlier detection (>2% deviation from median)
    - Partial failure handling with adjusted confidence
    - Source health tracking for routing decisions

Example:
    from almanak.gateway.data.price.aggregator import PriceAggregator
    from almanak.gateway.data.price.coingecko import CoinGeckoPriceSource

    sources = [CoinGeckoPriceSource()]
    aggregator = PriceAggregator(sources=sources)
    result = await aggregator.get_aggregated_price("WETH", "USD")
    print(f"Price: {result.price}, Confidence: {result.confidence}")
"""

import asyncio
import logging
import statistics
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.data.interfaces import (
    AllDataSourcesFailed,
    BasePriceSource,
    PriceResult,
)

logger = logging.getLogger(__name__)


# Default configuration constants
DEFAULT_OUTLIER_DEVIATION_THRESHOLD = 0.02  # 2% deviation from median
DEFAULT_STALE_CONFIDENCE_PENALTY = 0.3  # Reduce confidence by 30% for stale data
DEFAULT_PARTIAL_FAILURE_CONFIDENCE_PENALTY = 0.1  # Reduce by 10% per failed source

# Magnitude outlier threshold: if max/min price ratio exceeds this, the sources
# fundamentally disagree (feed misconfiguration, wrong units, decimal mismatch).
# Example: wstETH/ETH exchange rate feed (~1.228) decoded with 8-decimal assumption
# produces ~$12.28B, while CoinGecko returns ~$3,400. Ratio ≈ 3.6M× >> 100×.
# At this scale, averaging produces nonsense -- we must raise AllDataSourcesFailed.
DEFAULT_MAGNITUDE_OUTLIER_RATIO = 100.0

# Stablecoins that fall back to $1.00 when all price sources fail
STABLECOIN_FALLBACK_TOKENS = frozenset({"USDC", "USDT", "DAI", "FRAX", "LUSD", "USDC.E", "USDT.E"})


@dataclass
class SourceHealthMetrics:
    """Health metrics for a single price source.

    Tracks success rate, latency, and error information for making
    routing decisions and observability.

    Attributes:
        source_name: Name of the data source
        total_requests: Total number of price requests
        successful_requests: Number of successful requests
        failed_requests: Number of failed requests
        total_latency_ms: Total latency for successful requests
        last_success_time: Time of last successful request
        last_error_time: Time of last error
        last_error: Last error message
    """

    source_name: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_latency_ms: float = 0.0
    last_success_time: datetime | None = None
    last_error_time: datetime | None = None
    last_error: str | None = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage (0-100)."""
        if self.total_requests == 0:
            return 100.0
        return (self.successful_requests / self.total_requests) * 100

    @property
    def average_latency_ms(self) -> float:
        """Calculate average latency in milliseconds."""
        if self.successful_requests == 0:
            return 0.0
        return self.total_latency_ms / self.successful_requests

    def record_success(self, latency_ms: float) -> None:
        """Record a successful request."""
        self.total_requests += 1
        self.successful_requests += 1
        self.total_latency_ms += latency_ms
        self.last_success_time = datetime.now(UTC)

    def record_failure(self, error: str) -> None:
        """Record a failed request."""
        self.total_requests += 1
        self.failed_requests += 1
        self.last_error = error
        self.last_error_time = datetime.now(UTC)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "source_name": self.source_name,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": round(self.success_rate, 2),
            "average_latency_ms": round(self.average_latency_ms, 2),
            "last_success_time": (self.last_success_time.isoformat() if self.last_success_time else None),
            "last_error_time": (self.last_error_time.isoformat() if self.last_error_time else None),
            "last_error": self.last_error,
        }


@dataclass
class AggregationResult:
    """Internal result from price aggregation including outlier info.

    Attributes:
        price: Aggregated price (median for multiple sources)
        valid_results: List of valid PriceResults used in aggregation
        outliers: List of PriceResults flagged as outliers
        errors: Dict mapping source names to error messages
    """

    price: Decimal
    valid_results: list[PriceResult]
    outliers: list[PriceResult] = field(default_factory=list)
    errors: dict[str, str] = field(default_factory=dict)


class PriceAggregator:
    """Price aggregator with multi-source validation, outlier detection, and graceful degradation.

    This class implements the PriceOracle protocol and provides aggregated prices
    from multiple BasePriceSource implementations with proper error handling.

    Key behaviors:
    - Single source: Returns price with confidence based on staleness
    - Multiple sources: Returns median price, flags outliers (>2% deviation)
    - Partial failure: Continues with available sources, adjusts confidence
    - Total failure: Raises AllDataSourcesFailed with individual error details

    Attributes:
        sources: List of BasePriceSource implementations
        outlier_threshold: Deviation threshold for outlier detection (default 2%)
        stale_confidence_penalty: Confidence reduction for stale data
        partial_failure_penalty: Confidence reduction per failed source

    Example:
        # Single source
        aggregator = PriceAggregator(sources=[CoinGeckoPriceSource()])
        result = await aggregator.get_aggregated_price("ETH")

        # Multiple sources
        aggregator = PriceAggregator(sources=[
            CoinGeckoPriceSource(),
            ChainlinkPriceSource(),
        ])
        result = await aggregator.get_aggregated_price("ETH")
        print(f"Median price: {result.price}, Confidence: {result.confidence}")
    """

    def __init__(
        self,
        sources: Sequence[BasePriceSource],
        outlier_threshold: float = DEFAULT_OUTLIER_DEVIATION_THRESHOLD,
        stale_confidence_penalty: float = DEFAULT_STALE_CONFIDENCE_PENALTY,
        partial_failure_penalty: float = DEFAULT_PARTIAL_FAILURE_CONFIDENCE_PENALTY,
        magnitude_outlier_ratio: float = DEFAULT_MAGNITUDE_OUTLIER_RATIO,
    ) -> None:
        """Initialize the PriceAggregator.

        Args:
            sources: List of BasePriceSource implementations (1 to N sources)
            outlier_threshold: Deviation threshold for outlier detection (default 0.02 = 2%)
            stale_confidence_penalty: Confidence reduction for stale data (default 0.3)
            partial_failure_penalty: Confidence reduction per failed source (default 0.1)
            magnitude_outlier_ratio: When max/min price ratio exceeds this, treat as feed
                misconfiguration (wrong units/decimals) and raise AllDataSourcesFailed
                instead of averaging garbage values. Default 100× (e.g., $3,400 vs $12.28B
                wstETH case triggers at ~3,600,000×). Set higher to allow more divergence.

        Raises:
            ValueError: If sources list is empty
        """
        if not sources:
            raise ValueError("At least one price source is required")

        self._sources = list(sources)
        self._outlier_threshold = outlier_threshold
        self._stale_confidence_penalty = stale_confidence_penalty
        self._partial_failure_penalty = partial_failure_penalty
        self._magnitude_outlier_ratio = magnitude_outlier_ratio

        # Health metrics per source
        self._health_metrics: dict[str, SourceHealthMetrics] = {
            source.source_name: SourceHealthMetrics(source_name=source.source_name) for source in sources
        }

        # Per-call diagnostics: stores last aggregation details per token/quote pair
        self._last_details: dict[str, dict[str, Any]] = {}

        logger.info(
            "Initialized PriceAggregator",
            extra={
                "source_count": len(sources),
                "sources": [s.source_name for s in sources],
                "outlier_threshold": outlier_threshold,
                "magnitude_outlier_ratio": magnitude_outlier_ratio,
            },
        )

    @property
    def sources(self) -> list[BasePriceSource]:
        """Return the list of configured price sources."""
        return self._sources.copy()

    async def get_aggregated_price(
        self,
        token: str,
        quote: str = "USD",
    ) -> PriceResult:
        """Get aggregated price from multiple sources.

        Fetches prices from all configured sources concurrently, filters outliers,
        and returns the median price with adjusted confidence.

        Args:
            token: Token symbol to get price for (e.g., "ETH", "WETH")
            quote: Quote currency (default "USD")

        Returns:
            PriceResult with aggregated price and confidence score

        Raises:
            AllDataSourcesFailed: If all sources fail to provide data
        """
        logger.debug(
            "Getting aggregated price for %s/%s from %d sources",
            token,
            quote,
            len(self._sources),
        )

        # Fetch from all sources concurrently
        results = await self._fetch_all_sources(token, quote)

        # Store per-call diagnostics BEFORE the failure check so that
        # get_last_details() is populated even when all sources fail.
        detail_key = f"{token.upper()}/{quote.upper()}"
        self._last_details[detail_key] = {
            "sources_ok": [r.source for r in results.valid_results],
            "sources_failed": results.errors,
            "outliers": [r.source for r in results.outliers],
        }

        # Check if all sources failed
        if not results.valid_results:
            # Stablecoin fallback: use $1.00 for known stablecoins when all sources fail
            if quote.upper() == "USD" and token.upper() in STABLECOIN_FALLBACK_TOKENS:
                logger.warning(
                    "All price sources failed for stablecoin %s/%s, using $1.00 fallback. Errors: %s",
                    token,
                    quote,
                    results.errors,
                )
                return PriceResult(
                    price=Decimal("1.00"),
                    source="stablecoin_fallback",
                    timestamp=datetime.now(UTC),
                    confidence=0.8,
                    stale=False,
                )

            logger.error(
                "All data sources failed for %s/%s: %s",
                token,
                quote,
                results.errors,
            )
            raise AllDataSourcesFailed(errors=results.errors)

        # Calculate confidence based on results
        confidence = self._calculate_confidence(results)

        # Determine staleness
        stale = any(r.stale for r in results.valid_results)

        # Log aggregation result
        logger.info(
            "Aggregated price for %s/%s: %s (confidence: %.2f, sources: %d/%d, outliers: %d)",
            token,
            quote,
            results.price,
            confidence,
            len(results.valid_results),
            len(self._sources),
            len(results.outliers),
        )

        # Log outliers if any
        if results.outliers:
            for outlier in results.outliers:
                logger.warning(
                    "Outlier detected from %s: %s (median: %s)",
                    outlier.source,
                    outlier.price,
                    results.price,
                )

        return PriceResult(
            price=results.price,
            source="aggregated",
            timestamp=datetime.now(UTC),
            confidence=confidence,
            stale=stale,
        )

    async def _fetch_all_sources(
        self,
        token: str,
        quote: str,
    ) -> AggregationResult:
        """Fetch prices from all sources concurrently.

        Args:
            token: Token symbol
            quote: Quote currency

        Returns:
            AggregationResult with valid results, outliers, and errors
        """
        # Create tasks for all sources
        tasks = [self._fetch_with_metrics(source, token, quote) for source in self._sources]

        # Gather results (don't raise on individual failures)
        task_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Separate successes and failures
        valid_results: list[PriceResult] = []
        errors: dict[str, str] = {}

        for source, result in zip(self._sources, task_results, strict=False):
            if isinstance(result, Exception):
                errors[source.source_name] = str(result)
            elif isinstance(result, PriceResult):
                valid_results.append(result)
            else:
                errors[source.source_name] = f"Unexpected result type: {type(result)}"

        # If no valid results, return early
        if not valid_results:
            return AggregationResult(
                price=Decimal("0"),
                valid_results=[],
                outliers=[],
                errors=errors,
            )

        # Single source: return as-is
        if len(valid_results) == 1:
            return AggregationResult(
                price=valid_results[0].price,
                valid_results=valid_results,
                outliers=[],
                errors=errors,
            )

        # Multiple sources: detect outliers and compute median
        return self._aggregate_multiple(valid_results, errors)

    async def _fetch_with_metrics(
        self,
        source: BasePriceSource,
        token: str,
        quote: str,
    ) -> PriceResult:
        """Fetch price from a source and track metrics.

        Args:
            source: Price source to fetch from
            token: Token symbol
            quote: Quote currency

        Returns:
            PriceResult from the source

        Raises:
            DataSourceError: If the source fails
        """
        metrics = self._health_metrics[source.source_name]
        start_time = time.time()

        try:
            result = await source.get_price(token, quote)
            latency_ms = (time.time() - start_time) * 1000
            metrics.record_success(latency_ms)
            return result
        except Exception as e:
            metrics.record_failure(str(e))
            raise

    def _aggregate_multiple(
        self,
        results: list[PriceResult],
        errors: dict[str, str],
    ) -> AggregationResult:
        """Aggregate multiple price results using median and outlier detection.

        Args:
            results: List of valid PriceResults
            errors: Dict of source errors

        Returns:
            AggregationResult with median price and outlier list
        """
        # Calculate median price
        prices = [float(r.price) for r in results]
        median_price = Decimal(str(statistics.median(prices)))

        # Detect outliers (>2% deviation from median)
        valid_results: list[PriceResult] = []
        outliers: list[PriceResult] = []

        for result in results:
            deviation = abs(float(result.price) - float(median_price)) / float(median_price)
            if deviation > self._outlier_threshold:
                outliers.append(result)
                logger.debug(
                    "Flagged outlier from %s: %s (%.2f%% deviation from median %s)",
                    result.source,
                    result.price,
                    deviation * 100,
                    median_price,
                )
            else:
                valid_results.append(result)

        # If all results are outliers, check whether the divergence is due to feed
        # misconfiguration (magnitude-level disagreement) vs genuine market volatility.
        if not valid_results:
            prices_float = sorted(float(r.price) for r in results)
            min_price = prices_float[0]
            max_price = prices_float[-1]

            ratio = 0.0
            if min_price > 0:
                ratio = max_price / min_price
            elif max_price > 0:  # min_price <= 0: zero or negative price is always extreme
                ratio = float("inf")

            if ratio > self._magnitude_outlier_ratio:
                # Extreme divergence: max/min ratio far exceeds normal market volatility.
                # Likely cause: feed returning price in wrong units (e.g., wstETH/ETH
                # exchange rate decoded as USD via 8-decimal assumption gives ~$12.28B
                # while correct USD price is ~$3,400, ratio ≈ 3,600,000×).
                # Averaging these values produces nonsense -- fail explicitly.
                ratio_str = "inf" if ratio == float("inf") else f"{ratio:.0f}"
                logger.error(
                    "Extreme price divergence detected across %d sources: min=%s, max=%s "
                    "(ratio=%s× exceeds limit of %.0f×). This indicates a feed "
                    "configuration error (wrong units/decimals), not market volatility. "
                    "Raising AllDataSourcesFailed to prevent corrupted price from being used.",
                    len(results),
                    min_price,
                    max_price,
                    ratio_str,
                    self._magnitude_outlier_ratio,
                )
                magnitude_errors = {
                    r.source: (
                        f"Magnitude outlier: price={r.price} (min={min_price:.4g}, "
                        f"max={max_price:.4g}, ratio={ratio_str}×)"
                    )
                    for r in results
                }
                magnitude_errors.update(errors)
                raise AllDataSourcesFailed(errors=magnitude_errors)

            # Normal divergence across all sources (e.g., volatile market with 3 sources
            # each 15% apart). Use all results -- median is still meaningful.
            logger.warning(
                "All prices flagged as outliers, using all %d results",
                len(results),
            )
            valid_results = results
            outliers = []

        # Recalculate median after outlier removal if needed
        if outliers and valid_results:
            prices = [float(r.price) for r in valid_results]
            median_price = Decimal(str(statistics.median(prices)))

        return AggregationResult(
            price=median_price,
            valid_results=valid_results,
            outliers=outliers,
            errors=errors,
        )

    def _calculate_confidence(self, result: AggregationResult) -> float:
        """Calculate confidence score for aggregated result.

        Confidence is calculated based on:
        - Number of sources that succeeded vs failed
        - Whether any results are stale
        - Number of outliers detected

        Args:
            result: AggregationResult from aggregation

        Returns:
            Confidence score from 0.0 to 1.0
        """
        # Start with full confidence
        confidence = 1.0

        # Penalty for failed sources
        failed_count = len(result.errors)
        if failed_count > 0:
            confidence -= failed_count * self._partial_failure_penalty

        # Penalty for stale data
        stale_count = sum(1 for r in result.valid_results if r.stale)
        if stale_count > 0:
            stale_ratio = stale_count / len(result.valid_results)
            confidence -= stale_ratio * self._stale_confidence_penalty

        # Small penalty for outliers (data quality concern)
        if result.outliers:
            outlier_penalty = len(result.outliers) * 0.05
            confidence -= outlier_penalty

        # If single source, use its confidence directly (with stale penalty if applicable)
        if len(result.valid_results) == 1:
            single_confidence = result.valid_results[0].confidence
            if result.valid_results[0].stale:
                single_confidence *= 1 - self._stale_confidence_penalty
            confidence = min(confidence, single_confidence)

        # Clamp to valid range
        return max(0.0, min(1.0, confidence))

    def get_source_health(self, source_name: str) -> dict[str, Any] | None:
        """Get health metrics for a specific source.

        Args:
            source_name: Name of the source to query

        Returns:
            Dictionary with health metrics, or None if source unknown
        """
        metrics = self._health_metrics.get(source_name)
        if metrics is None:
            return None
        return metrics.to_dict()

    def get_all_source_health(self) -> dict[str, dict[str, Any]]:
        """Get health metrics for all sources.

        Returns:
            Dictionary mapping source names to their health metrics
        """
        return {name: metrics.to_dict() for name, metrics in self._health_metrics.items()}

    def get_last_details(self, token: str, quote: str = "USD") -> dict[str, Any] | None:
        """Get per-source diagnostics from the last aggregation call for a token pair.

        Returns:
            Dict with sources_ok, sources_failed, and outliers lists, or None if
            no aggregation has been performed for this pair yet.
        """
        return self._last_details.get(f"{token.upper()}/{quote.upper()}")

    def reset_health_metrics(self, source_name: str | None = None) -> None:
        """Reset health metrics for one or all sources.

        Args:
            source_name: Specific source to reset, or None to reset all
        """
        if source_name is not None:
            if source_name in self._health_metrics:
                self._health_metrics[source_name] = SourceHealthMetrics(source_name=source_name)
        else:
            for name in self._health_metrics:
                self._health_metrics[name] = SourceHealthMetrics(source_name=name)

    async def close(self) -> None:
        """Close all underlying price sources.

        This should be called when the aggregator is no longer needed
        to properly release resources (HTTP sessions, etc.).
        """
        for source in self._sources:
            if hasattr(source, "close"):
                try:
                    await source.close()
                except Exception as e:
                    logger.warning("Error closing source %s: %s", source.source_name, e)


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "PriceAggregator",
    "SourceHealthMetrics",
    "AggregationResult",
]
