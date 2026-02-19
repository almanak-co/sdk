"""Stochastic Oscillator Calculator.

The Stochastic Oscillator is a momentum indicator comparing a token's
closing price to its price range over a given period.

Components:
- %K (Fast): Current position in the price range (0-100)
- %D (Slow): SMA of %K, acts as signal line

Trading Signals:
- %K < 20: Oversold territory (potential buy)
- %K > 80: Overbought territory (potential sell)
- %K crosses above %D: Bullish signal
- %K crosses below %D: Bearish signal

Example:
    from almanak.framework.data.indicators import StochasticCalculator

    stoch_calc = StochasticCalculator(ohlcv_provider=provider)
    stoch = await stoch_calc.calculate_stochastic("WETH", k_period=14, d_period=3)

    if stoch.k_value < 20 and stoch.k_value > stoch.d_value:
        print("Oversold with bullish crossover - potential buy")
"""

import logging
from typing import Any

from ..interfaces import (
    InsufficientDataError,
    OHLCVCandle,
    OHLCVProvider,
)
from .base import StochasticResult

logger = logging.getLogger(__name__)


class StochasticCalculator:
    """Stochastic Oscillator Calculator.

    Calculates the Stochastic Oscillator with configurable %K and %D periods.

    Default Parameters:
        - %K Period: 14 (industry standard)
        - %D Period: 3 (smoothing period)

    Attributes:
        ohlcv_provider: Provider for OHLCV data (implements OHLCVProvider protocol)

    Example:
        provider = CoinGeckoOHLCVProvider()
        stoch_calc = StochasticCalculator(ohlcv_provider=provider)

        stoch = await stoch_calc.calculate_stochastic("WETH", k_period=14, d_period=3)
        print(f"%K: {stoch.k_value:.2f}, %D: {stoch.d_value:.2f}")

        if stoch.k_value < 20:
            print("Oversold territory")
    """

    def __init__(self, ohlcv_provider: OHLCVProvider) -> None:
        """Initialize the Stochastic Calculator.

        Args:
            ohlcv_provider: Provider implementing OHLCVProvider protocol
        """
        self._ohlcv_provider = ohlcv_provider
        logger.info("Initialized StochasticCalculator")

    @property
    def name(self) -> str:
        """Return indicator name."""
        return "Stochastic"

    @property
    def min_data_points(self) -> int:
        """Return minimum data points (k_period + d_period)."""
        return 17  # 14 + 3 for default settings

    @staticmethod
    def calculate_stochastic_from_candles(
        candles: list[OHLCVCandle],
        k_period: int = 14,
        d_period: int = 3,
    ) -> StochasticResult:
        """Calculate Stochastic Oscillator from OHLCV candles.

        Formula:
        %K = 100 * (Close - Lowest Low) / (Highest High - Lowest Low)
        %D = SMA(%K, d_period)

        Args:
            candles: List of OHLCVCandle objects (oldest first)
            k_period: Lookback period for %K (default 14)
            d_period: SMA period for %D (default 3)

        Returns:
            StochasticResult with k_value and d_value

        Raises:
            InsufficientDataError: If not enough candle data
        """
        required = k_period + d_period - 1
        if len(candles) < required:
            raise InsufficientDataError(
                required=required,
                available=len(candles),
                indicator="Stochastic",
            )

        # Calculate %K values for the last d_period candles
        k_values: list[float] = []

        for i in range(len(candles) - d_period + 1, len(candles) + 1):
            if i < k_period:
                continue

            # Get the range of candles for this %K calculation
            period_candles = candles[i - k_period : i]

            # Find highest high and lowest low in the period
            highest_high = max(float(c.high) for c in period_candles)
            lowest_low = min(float(c.low) for c in period_candles)

            # Current close
            current_close = float(period_candles[-1].close)

            # Calculate %K
            price_range = highest_high - lowest_low
            if price_range > 0:
                k = 100 * (current_close - lowest_low) / price_range
            else:
                k = 50.0  # Neutral if no range

            k_values.append(k)

        if len(k_values) < d_period:
            raise InsufficientDataError(
                required=required,
                available=len(candles),
                indicator="Stochastic",
            )

        # Most recent %K
        k_value = k_values[-1]

        # %D is SMA of last d_period %K values
        d_value = sum(k_values[-d_period:]) / d_period

        return StochasticResult(
            k_value=k_value,
            d_value=d_value,
        )

    async def calculate_stochastic(
        self,
        token: str,
        k_period: int = 14,
        d_period: int = 3,
        timeframe: str = "1h",
    ) -> StochasticResult:
        """Calculate Stochastic Oscillator for a token.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            k_period: Lookback period for %K (default 14)
            d_period: SMA period for %D (default 3)
            timeframe: OHLCV candle timeframe (default "1h")

        Returns:
            StochasticResult with k_value and d_value (both 0-100 scale)

        Raises:
            InsufficientDataError: If not enough historical data
            DataSourceError: If data cannot be fetched

        Example:
            stoch = await stoch_calc.calculate_stochastic("WETH", k_period=14, d_period=3)

            # Trading logic
            if stoch.k_value < 20:
                print("Oversold - potential buy signal")
            elif stoch.k_value > 80:
                print("Overbought - potential sell signal")

            # Crossover signals
            if stoch.k_value > stoch.d_value:
                print("Bullish - %K above %D")
        """
        limit = k_period + d_period + 10  # Buffer

        logger.debug(
            "Calculating Stochastic for %s with k_period=%d, d_period=%d, timeframe=%s",
            token,
            k_period,
            d_period,
            timeframe,
        )

        ohlcv_data = await self._ohlcv_provider.get_ohlcv(
            token=token,
            quote="USD",
            timeframe=timeframe,
            limit=limit,
        )

        if not ohlcv_data:
            raise InsufficientDataError(
                required=k_period + d_period,
                available=0,
                indicator="Stochastic",
            )

        stoch = self.calculate_stochastic_from_candles(ohlcv_data, k_period, d_period)

        logger.debug(
            "Calculated Stochastic for %s: %%K=%.2f, %%D=%.2f",
            token,
            stoch.k_value,
            stoch.d_value,
        )

        return stoch

    async def calculate(
        self,
        token: str,
        timeframe: str = "1h",
        **params: Any,
    ) -> dict[str, float]:
        """Calculate Stochastic (BaseIndicator protocol implementation).

        Args:
            token: Token symbol
            timeframe: OHLCV candle timeframe
            **params: k_period (default 14), d_period (default 3)

        Returns:
            Dictionary with k_value and d_value
        """
        k_period = params.get("k_period", 14)
        d_period = params.get("d_period", 3)

        result = await self.calculate_stochastic(token, k_period, d_period, timeframe)
        return result.to_dict()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "StochasticCalculator",
]
