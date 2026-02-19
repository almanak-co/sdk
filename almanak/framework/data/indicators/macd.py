"""MACD (Moving Average Convergence Divergence) Calculator.

MACD is a trend-following momentum indicator showing the relationship
between two exponential moving averages.

Components:
- MACD Line: Fast EMA (12) - Slow EMA (26)
- Signal Line: EMA of MACD Line (9 periods)
- Histogram: MACD Line - Signal Line

Trading Signals:
- MACD crosses above Signal Line: Bullish (buy signal)
- MACD crosses below Signal Line: Bearish (sell signal)
- Histogram expanding: Trend strengthening
- Histogram contracting: Trend weakening

Example:
    from almanak.framework.data.indicators import MACDCalculator

    macd_calc = MACDCalculator(ohlcv_provider=provider)
    macd = await macd_calc.calculate_macd("WETH", timeframe="4h")

    if macd.histogram > 0 and macd.macd_line > macd.signal_line:
        print("Bullish momentum - MACD above signal")
"""

import logging
import math
from decimal import Decimal
from typing import Any

from ..interfaces import (
    InsufficientDataError,
    OHLCVProvider,
)
from .base import MACDResult

logger = logging.getLogger(__name__)


class MACDCalculator:
    """MACD (Moving Average Convergence Divergence) Calculator.

    Calculates MACD with configurable fast, slow, and signal periods.

    Default Parameters (industry standard):
        - Fast EMA: 12 periods
        - Slow EMA: 26 periods
        - Signal EMA: 9 periods

    Attributes:
        ohlcv_provider: Provider for OHLCV data (implements OHLCVProvider protocol)

    Example:
        provider = CoinGeckoOHLCVProvider()
        macd_calc = MACDCalculator(ohlcv_provider=provider)

        macd = await macd_calc.calculate_macd("WETH", fast=12, slow=26, signal=9)
        print(f"MACD: {macd.macd_line}, Signal: {macd.signal_line}")

        # Check for bullish crossover
        if macd.histogram > 0:
            print("MACD above signal - bullish")
    """

    def __init__(self, ohlcv_provider: OHLCVProvider) -> None:
        """Initialize the MACD Calculator.

        Args:
            ohlcv_provider: Provider implementing OHLCVProvider protocol
        """
        self._ohlcv_provider = ohlcv_provider
        logger.info("Initialized MACDCalculator")

    @property
    def name(self) -> str:
        """Return indicator name."""
        return "MACD"

    @property
    def min_data_points(self) -> int:
        """Return minimum data points (slow period + signal + buffer)."""
        return 35  # 26 + 9 for default settings

    @staticmethod
    def _calculate_ema(prices: list[float], period: int, smoothing: float = 2.0) -> list[float]:
        """Calculate EMA series from prices.

        Args:
            prices: List of prices (oldest first)
            period: EMA period
            smoothing: Smoothing factor (default 2.0)

        Returns:
            List of EMA values (same length as prices, first 'period-1' values are NaN)
        """
        if len(prices) < period:
            return []

        k = smoothing / (period + 1)
        ema_values: list[float] = []

        # Start with SMA of first 'period' values
        sma = sum(prices[:period]) / period
        ema_values.extend([float("nan")] * (period - 1))
        ema_values.append(sma)

        # Apply EMA formula
        ema = sma
        for price in prices[period:]:
            ema = price * k + ema * (1 - k)
            ema_values.append(ema)

        return ema_values

    @staticmethod
    def calculate_macd_from_prices(
        close_prices: list[Decimal],
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
    ) -> MACDResult:
        """Calculate MACD from a list of close prices.

        Args:
            close_prices: List of closing prices (oldest first)
            fast_period: Fast EMA period (default 12)
            slow_period: Slow EMA period (default 26)
            signal_period: Signal line EMA period (default 9)

        Returns:
            MACDResult with macd_line, signal_line, and histogram

        Raises:
            InsufficientDataError: If not enough price data
        """
        required = slow_period + signal_period
        if len(close_prices) < required:
            raise InsufficientDataError(
                required=required,
                available=len(close_prices),
                indicator="MACD",
            )

        # Convert to float for calculations
        prices = [float(p) for p in close_prices]

        # Calculate fast and slow EMAs
        fast_ema_series = MACDCalculator._calculate_ema(prices, fast_period)
        slow_ema_series = MACDCalculator._calculate_ema(prices, slow_period)

        # Calculate MACD line (fast EMA - slow EMA)
        # Start from where both EMAs are valid
        start_idx = slow_period - 1
        macd_line_series: list[float] = []

        for i in range(start_idx, len(prices)):
            if i < len(fast_ema_series) and i < len(slow_ema_series):
                fast_val = fast_ema_series[i]
                slow_val = slow_ema_series[i]
                if not (math.isnan(fast_val) or math.isnan(slow_val)):
                    macd_line_series.append(fast_val - slow_val)

        if len(macd_line_series) < signal_period:
            raise InsufficientDataError(
                required=required,
                available=len(close_prices),
                indicator="MACD",
            )

        # Calculate signal line (EMA of MACD line)
        signal_line_series = MACDCalculator._calculate_ema(macd_line_series, signal_period)

        # Get the most recent values
        macd_line = macd_line_series[-1]
        signal_line = signal_line_series[-1] if signal_line_series else macd_line

        # Calculate histogram
        histogram = macd_line - signal_line

        return MACDResult(
            macd_line=macd_line,
            signal_line=signal_line,
            histogram=histogram,
        )

    async def calculate_macd(
        self,
        token: str,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
        timeframe: str = "1h",
    ) -> MACDResult:
        """Calculate MACD for a token.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            fast_period: Fast EMA period (default 12)
            slow_period: Slow EMA period (default 26)
            signal_period: Signal line EMA period (default 9)
            timeframe: OHLCV candle timeframe (default "1h")

        Returns:
            MACDResult with macd_line, signal_line, and histogram

        Raises:
            InsufficientDataError: If not enough historical data
            DataSourceError: If data cannot be fetched

        Example:
            macd = await macd_calc.calculate_macd("WETH", fast=12, slow=26, signal=9)

            # Trading logic
            if macd.histogram > 0:
                # MACD above signal - bullish momentum
                print("Bullish crossover")
            elif macd.histogram < 0:
                # MACD below signal - bearish momentum
                print("Bearish crossover")
        """
        # Request enough data for stable calculations
        limit = slow_period + signal_period + 50

        logger.debug(
            "Calculating MACD for %s with fast=%d, slow=%d, signal=%d, timeframe=%s",
            token,
            fast_period,
            slow_period,
            signal_period,
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
                required=slow_period + signal_period,
                available=0,
                indicator="MACD",
            )

        close_prices = [candle.close for candle in ohlcv_data]
        macd = self.calculate_macd_from_prices(close_prices, fast_period, slow_period, signal_period)

        logger.debug(
            "Calculated MACD for %s: line=%.4f, signal=%.4f, histogram=%.4f",
            token,
            macd.macd_line,
            macd.signal_line,
            macd.histogram,
        )

        return macd

    async def calculate(
        self,
        token: str,
        timeframe: str = "1h",
        **params: Any,
    ) -> dict[str, float]:
        """Calculate MACD (BaseIndicator protocol implementation).

        Args:
            token: Token symbol
            timeframe: OHLCV candle timeframe
            **params: fast_period (12), slow_period (26), signal_period (9)

        Returns:
            Dictionary with all MACD values
        """
        fast = params.get("fast_period", 12)
        slow = params.get("slow_period", 26)
        signal = params.get("signal_period", 9)

        result = await self.calculate_macd(token, fast, slow, signal, timeframe)
        return result.to_dict()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "MACDCalculator",
]
