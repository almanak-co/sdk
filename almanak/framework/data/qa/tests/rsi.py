"""RSI Indicator Test for QA Framework.

This module provides validation for RSI (Relative Strength Index) calculations
using the RSICalculator and CoinGeckoOHLCVProvider. It validates RSI bounds,
calculates rolling RSI history, and determines overbought/oversold signals.

Example:
    from almanak.framework.data.qa.tests.rsi import RSITest
    from almanak.framework.data.qa.config import load_config

    config = load_config()
    test = RSITest(config)
    results = await test.run()

    for result in results:
        print(f"{result.token}: RSI={result.current_rsi:.2f} ({result.signal})")
"""

import logging
from dataclasses import dataclass, field
from decimal import Decimal

from almanak.framework.data.indicators.rsi import CoinGeckoOHLCVProvider, RSICalculator
from almanak.framework.data.interfaces import DataSourceUnavailable, InsufficientDataError
from almanak.framework.data.qa.config import QAConfig

logger = logging.getLogger(__name__)


# RSI signal thresholds
RSI_OVERSOLD_THRESHOLD = 30.0
RSI_OVERBOUGHT_THRESHOLD = 70.0


@dataclass
class RSIDataPoint:
    """A single RSI data point in the history.

    Attributes:
        index: Position in the history (0 = oldest)
        rsi: RSI value at this point
    """

    index: int
    rsi: float


@dataclass
class RSIResult:
    """Result of an RSI indicator test for a single token.

    Attributes:
        token: Token symbol (e.g., "ETH", "WBTC")
        current_rsi: Most recent RSI value, None if unavailable
        signal: RSI signal interpretation (Neutral, Overbought, Oversold)
        rsi_history: List of RSI values over the calculation period
        min_rsi: Minimum RSI value in the history
        max_rsi: Maximum RSI value in the history
        avg_rsi: Average RSI value in the history
        passed: Whether all validation checks passed (0 <= RSI <= 100)
        error: Error message if the test failed, None otherwise
    """

    token: str
    current_rsi: float | None
    signal: str
    rsi_history: list[RSIDataPoint] = field(default_factory=list)
    min_rsi: float | None = None
    max_rsi: float | None = None
    avg_rsi: float | None = None
    passed: bool = False
    error: str | None = None

    def to_dict(self) -> dict:
        """Convert result to dictionary for serialization."""
        return {
            "token": self.token,
            "current_rsi": self.current_rsi,
            "signal": self.signal,
            "rsi_history": [{"index": p.index, "rsi": p.rsi} for p in self.rsi_history],
            "min_rsi": self.min_rsi,
            "max_rsi": self.max_rsi,
            "avg_rsi": self.avg_rsi,
            "passed": self.passed,
            "error": self.error,
        }


def get_rsi_signal(rsi: float) -> str:
    """Determine the RSI signal based on thresholds.

    Args:
        rsi: RSI value (0-100)

    Returns:
        Signal string: "Oversold", "Overbought", or "Neutral"
    """
    if rsi < RSI_OVERSOLD_THRESHOLD:
        return "Oversold"
    elif rsi > RSI_OVERBOUGHT_THRESHOLD:
        return "Overbought"
    return "Neutral"


class RSITest:
    """Test for validating RSI indicator calculations.

    This test validates that:
    1. RSI can be calculated for all configured tokens
    2. RSI values are within valid bounds (0-100)
    3. Rolling RSI history is computed correctly
    4. Signal interpretation is accurate

    Attributes:
        config: QA configuration with token lists and RSI period
        ohlcv_provider: CoinGecko OHLCV data provider
        rsi_calculator: RSI calculator instance

    Example:
        config = load_config()
        test = RSITest(config)
        results = await test.run()

        passed = all(r.passed for r in results)
        print(f"RSI Test: {'PASSED' if passed else 'FAILED'}")
    """

    def __init__(
        self,
        config: QAConfig,
        ohlcv_provider: CoinGeckoOHLCVProvider | None = None,
        rsi_calculator: RSICalculator | None = None,
    ) -> None:
        """Initialize the RSI indicator test.

        Args:
            config: QA configuration with token lists and RSI period
            ohlcv_provider: Optional CoinGeckoOHLCVProvider instance.
                           If None, a default instance will be created.
            rsi_calculator: Optional RSICalculator instance. If None,
                           one will be created with the ohlcv_provider.
        """
        self.config = config
        self.ohlcv_provider = ohlcv_provider or CoinGeckoOHLCVProvider()
        self.rsi_calculator = rsi_calculator or RSICalculator(
            ohlcv_provider=self.ohlcv_provider,
            default_period=config.rsi_period,
        )

    async def run(self) -> list[RSIResult]:
        """Run the RSI indicator test for all configured tokens.

        Returns:
            List of RSIResult for each token tested.
        """
        results: list[RSIResult] = []

        # Test all tokens (popular + additional)
        tokens = self.config.all_tokens

        logger.info(
            "Running RSI indicator test for %d tokens (period=%d)",
            len(tokens),
            self.config.rsi_period,
        )

        for token in tokens:
            result = await self._test_token(token)
            results.append(result)

        # Log summary
        passed_count = sum(1 for r in results if r.passed)
        logger.info(
            "RSI indicator test complete: %d/%d passed",
            passed_count,
            len(results),
        )

        return results

    async def _test_token(self, token: str) -> RSIResult:
        """Test RSI calculation for a single token.

        Args:
            token: Token symbol to test

        Returns:
            RSIResult with validation results
        """
        try:
            # Fetch OHLCV data for rolling RSI calculation
            # Need enough data for multiple RSI calculations
            period = self.config.rsi_period
            limit = period + 50  # Extra data for rolling history

            ohlcv_data = await self.ohlcv_provider.get_ohlcv(
                token=token,
                quote="USD",
                timeframe=self.config.timeframe,
                limit=limit,
            )

            if not ohlcv_data or len(ohlcv_data) < period + 1:
                return RSIResult(
                    token=token,
                    current_rsi=None,
                    signal="Unknown",
                    passed=False,
                    error=f"Insufficient data: need {period + 1} candles, got {len(ohlcv_data) if ohlcv_data else 0}",
                )

            # Extract close prices
            close_prices: list[Decimal] = [candle.close for candle in ohlcv_data]

            # Calculate rolling RSI history
            rsi_history: list[RSIDataPoint] = []
            all_valid = True

            # Calculate RSI at each point where we have enough data
            # Start from the earliest point where we have period+1 data points
            for i in range(period + 1, len(close_prices) + 1):
                prices_subset = close_prices[:i]
                try:
                    rsi_value = RSICalculator.calculate_rsi_from_prices(prices_subset, period)
                    # Validate RSI bounds
                    if not (0.0 <= rsi_value <= 100.0):
                        all_valid = False
                        logger.warning(
                            "RSI out of bounds for %s at index %d: %.2f",
                            token,
                            i - 1,
                            rsi_value,
                        )
                    rsi_history.append(RSIDataPoint(index=len(rsi_history), rsi=rsi_value))
                except InsufficientDataError:
                    # Skip this point if not enough data
                    continue

            if not rsi_history:
                return RSIResult(
                    token=token,
                    current_rsi=None,
                    signal="Unknown",
                    passed=False,
                    error="Could not calculate any RSI values",
                )

            # Get current RSI (most recent)
            current_rsi = rsi_history[-1].rsi

            # Calculate statistics
            rsi_values = [p.rsi for p in rsi_history]
            min_rsi = min(rsi_values)
            max_rsi = max(rsi_values)
            avg_rsi = sum(rsi_values) / len(rsi_values)

            # Validate all RSI values are within bounds
            bounds_valid = all(0.0 <= rsi <= 100.0 for rsi in rsi_values)

            # Get signal interpretation
            signal = get_rsi_signal(current_rsi)

            passed = all_valid and bounds_valid

            logger.debug(
                "RSI for %s: current=%.2f (%s), min=%.2f, max=%.2f, avg=%.2f, passed=%s",
                token,
                current_rsi,
                signal,
                min_rsi,
                max_rsi,
                avg_rsi,
                passed,
            )

            return RSIResult(
                token=token,
                current_rsi=current_rsi,
                signal=signal,
                rsi_history=rsi_history,
                min_rsi=min_rsi,
                max_rsi=max_rsi,
                avg_rsi=avg_rsi,
                passed=passed,
                error=None,
            )

        except InsufficientDataError as e:
            logger.warning(
                "Insufficient data for RSI calculation for %s: %s",
                token,
                str(e),
            )
            return RSIResult(
                token=token,
                current_rsi=None,
                signal="Unknown",
                passed=False,
                error=f"Insufficient data: {str(e)}",
            )

        except DataSourceUnavailable as e:
            logger.warning(
                "Data source unavailable for RSI calculation for %s: %s",
                token,
                str(e),
            )
            return RSIResult(
                token=token,
                current_rsi=None,
                signal="Unknown",
                passed=False,
                error=f"Data source unavailable: {e.reason}",
            )

        except Exception as e:
            logger.error(
                "Unexpected error calculating RSI for %s: %s",
                token,
                str(e),
            )
            return RSIResult(
                token=token,
                current_rsi=None,
                signal="Unknown",
                passed=False,
                error=f"Unexpected error: {str(e)}",
            )

    async def close(self) -> None:
        """Close resources (HTTP sessions, etc.)."""
        await self.ohlcv_provider.close()

    async def __aenter__(self) -> "RSITest":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()


__all__ = [
    "RSIResult",
    "RSIDataPoint",
    "RSITest",
    "get_rsi_signal",
    "RSI_OVERSOLD_THRESHOLD",
    "RSI_OVERBOUGHT_THRESHOLD",
]
