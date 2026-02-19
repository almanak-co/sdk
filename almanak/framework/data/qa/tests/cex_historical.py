"""CEX Historical Price Test for QA Framework.

This module provides validation for CEX (Centralized Exchange) historical OHLCV
data using CoinGecko as the data source. It validates data continuity, gap detection,
and overall data quality for historical price analysis.

Example:
    from almanak.framework.data.qa.tests.cex_historical import CEXHistoricalTest
    from almanak.framework.data.qa.config import load_config

    config = load_config()
    test = CEXHistoricalTest(config)
    results = await test.run()

    for result in results:
        print(f"{result.token}: {result.total_candles} candles - {'PASS' if result.passed else 'FAIL'}")
"""

import logging
from dataclasses import dataclass
from datetime import timedelta
from decimal import Decimal
from typing import Any

from almanak.framework.data.indicators.rsi import CoinGeckoOHLCVProvider
from almanak.framework.data.interfaces import DataSourceUnavailable, OHLCVCandle
from almanak.framework.data.qa.config import QAConfig

logger = logging.getLogger(__name__)


@dataclass
class CEXHistoricalResult:
    """Result of a CEX historical price test for a single token.

    Attributes:
        token: Token symbol (e.g., "ETH", "WBTC")
        candles: List of OHLCV candles for plot generation
        total_candles: Number of candles received
        expected_candles: Number of candles expected based on timeframe and days
        missing_count: Number of missing candles detected
        max_gap_hours: Maximum gap between consecutive candles in hours
        price_range: Tuple of (min_price, max_price) during the period
        passed: Whether all validation checks passed
        error: Error message if the test failed, None otherwise
    """

    token: str
    candles: list[OHLCVCandle]
    total_candles: int
    expected_candles: int
    missing_count: int
    max_gap_hours: float
    price_range: tuple[Decimal, Decimal] | None
    passed: bool
    error: str | None

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary for serialization."""
        return {
            "token": self.token,
            "candles": [c.to_dict() for c in self.candles],
            "total_candles": self.total_candles,
            "expected_candles": self.expected_candles,
            "missing_count": self.missing_count,
            "max_gap_hours": self.max_gap_hours,
            "price_range": ((str(self.price_range[0]), str(self.price_range[1])) if self.price_range else None),
            "passed": self.passed,
            "error": self.error,
        }


# Mapping of timeframe to seconds
TIMEFRAME_SECONDS: dict[str, int] = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "4h": 14400,
    "1d": 86400,
}


class CEXHistoricalTest:
    """Test for validating CEX historical OHLCV data from CoinGecko.

    This test validates that:
    1. Historical OHLCV data can be fetched for all configured tokens
    2. Data has sufficient candles for the requested period
    3. Gaps between candles do not exceed the max_gap_hours threshold
    4. Price data is valid (positive values)

    Attributes:
        config: QA configuration with token lists and thresholds
        ohlcv_provider: CoinGecko OHLCV provider instance

    Example:
        config = load_config()
        test = CEXHistoricalTest(config)
        results = await test.run()

        passed = all(r.passed for r in results)
        print(f"CEX Historical Test: {'PASSED' if passed else 'FAILED'}")
    """

    def __init__(
        self,
        config: QAConfig,
        ohlcv_provider: CoinGeckoOHLCVProvider | None = None,
    ) -> None:
        """Initialize the CEX historical price test.

        Args:
            config: QA configuration with token lists and thresholds
            ohlcv_provider: Optional CoinGeckoOHLCVProvider instance. If None,
                           a default instance will be created.
        """
        self.config = config
        self.ohlcv_provider = ohlcv_provider or CoinGeckoOHLCVProvider()

    async def run(self) -> list[CEXHistoricalResult]:
        """Run the CEX historical price test for all configured tokens.

        Returns:
            List of CEXHistoricalResult for each token tested.
        """
        results: list[CEXHistoricalResult] = []

        # Test all tokens (popular + additional)
        tokens = self.config.all_tokens

        logger.info(
            "Running CEX historical test for %d tokens (timeframe=%s, days=%d)",
            len(tokens),
            self.config.timeframe,
            self.config.historical_days,
        )

        for token in tokens:
            result = await self._test_token(token)
            results.append(result)

        # Log summary
        passed_count = sum(1 for r in results if r.passed)
        logger.info(
            "CEX historical test complete: %d/%d passed",
            passed_count,
            len(results),
        )

        return results

    def _calculate_expected_candles(self) -> int:
        """Calculate expected number of candles based on timeframe and days.

        Returns:
            Expected number of candles
        """
        timeframe_seconds = TIMEFRAME_SECONDS.get(self.config.timeframe, 14400)
        total_seconds = self.config.historical_days * 24 * 3600
        return total_seconds // timeframe_seconds

    def _detect_gaps(self, candles: list[OHLCVCandle]) -> tuple[int, float]:
        """Detect gaps in OHLCV data.

        Args:
            candles: List of OHLCV candles sorted by timestamp

        Returns:
            Tuple of (missing_count, max_gap_hours)
        """
        if len(candles) < 2:
            return 0, 0.0

        timeframe_seconds = TIMEFRAME_SECONDS.get(self.config.timeframe, 14400)
        expected_interval = timedelta(seconds=timeframe_seconds)
        tolerance = timedelta(seconds=timeframe_seconds * 0.1)  # 10% tolerance

        missing_count = 0
        max_gap_hours = 0.0

        for i in range(1, len(candles)):
            time_diff = candles[i].timestamp - candles[i - 1].timestamp
            expected_plus_tolerance = expected_interval + tolerance

            if time_diff > expected_plus_tolerance:
                gap_seconds = time_diff.total_seconds()
                gap_hours = gap_seconds / 3600
                max_gap_hours = max(max_gap_hours, gap_hours)

                # Calculate missing candles
                missing = int(gap_seconds / timeframe_seconds) - 1
                missing_count += max(0, missing)

        return missing_count, max_gap_hours

    def _calculate_price_range(self, candles: list[OHLCVCandle]) -> tuple[Decimal, Decimal] | None:
        """Calculate the price range (min, max) from candles.

        Args:
            candles: List of OHLCV candles

        Returns:
            Tuple of (min_price, max_price) or None if no candles
        """
        if not candles:
            return None

        min_price = min(c.low for c in candles)
        max_price = max(c.high for c in candles)

        return (min_price, max_price)

    async def _test_token(self, token: str) -> CEXHistoricalResult:
        """Test historical OHLCV data for a single token.

        Args:
            token: Token symbol to test

        Returns:
            CEXHistoricalResult with validation results
        """
        expected_candles = self._calculate_expected_candles()

        try:
            # Fetch OHLCV data from CoinGecko
            candles = await self.ohlcv_provider.get_ohlcv(
                token=token,
                quote="USD",
                timeframe=self.config.timeframe,
                limit=expected_candles,
            )

            if not candles:
                logger.warning(
                    "No OHLCV data returned for %s",
                    token,
                )
                return CEXHistoricalResult(
                    token=token,
                    candles=[],
                    total_candles=0,
                    expected_candles=expected_candles,
                    missing_count=expected_candles,
                    max_gap_hours=0.0,
                    price_range=None,
                    passed=False,
                    error="No OHLCV data returned",
                )

            # Sort candles by timestamp (should already be sorted, but ensure)
            candles = sorted(candles, key=lambda c: c.timestamp)

            # Detect gaps
            missing_count, max_gap_hours = self._detect_gaps(candles)

            # Calculate price range
            price_range = self._calculate_price_range(candles)

            # Validate prices are positive
            invalid_prices = [c for c in candles if c.open <= 0 or c.high <= 0 or c.low <= 0 or c.close <= 0]

            # Determine if test passed
            gap_threshold_passed = max_gap_hours <= self.config.thresholds.max_gap_hours
            prices_valid = len(invalid_prices) == 0

            passed = gap_threshold_passed and prices_valid

            # Build error message if failed
            error = None
            if not passed:
                errors = []
                if not gap_threshold_passed:
                    errors.append(
                        f"Max gap {max_gap_hours:.1f}h exceeds threshold ({self.config.thresholds.max_gap_hours}h)"
                    )
                if not prices_valid:
                    errors.append(f"Found {len(invalid_prices)} candles with invalid prices")
                error = "; ".join(errors)

            logger.debug(
                "CEX historical for %s: %d candles (expected=%d, missing=%d, max_gap=%.1fh, passed=%s)",
                token,
                len(candles),
                expected_candles,
                missing_count,
                max_gap_hours,
                passed,
            )

            return CEXHistoricalResult(
                token=token,
                candles=candles,
                total_candles=len(candles),
                expected_candles=expected_candles,
                missing_count=missing_count,
                max_gap_hours=max_gap_hours,
                price_range=price_range,
                passed=passed,
                error=error,
            )

        except DataSourceUnavailable as e:
            logger.warning(
                "CEX historical data unavailable for %s: %s",
                token,
                str(e),
            )
            return CEXHistoricalResult(
                token=token,
                candles=[],
                total_candles=0,
                expected_candles=expected_candles,
                missing_count=expected_candles,
                max_gap_hours=0.0,
                price_range=None,
                passed=False,
                error=f"Data source unavailable: {e.reason}",
            )

        except Exception as e:
            logger.error(
                "Unexpected error fetching CEX historical data for %s: %s",
                token,
                str(e),
            )
            return CEXHistoricalResult(
                token=token,
                candles=[],
                total_candles=0,
                expected_candles=expected_candles,
                missing_count=expected_candles,
                max_gap_hours=0.0,
                price_range=None,
                passed=False,
                error=f"Unexpected error: {str(e)}",
            )

    async def close(self) -> None:
        """Close resources (HTTP sessions, etc.)."""
        await self.ohlcv_provider.close()

    async def __aenter__(self) -> "CEXHistoricalTest":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


__all__ = [
    "CEXHistoricalResult",
    "CEXHistoricalTest",
    "TIMEFRAME_SECONDS",
]
