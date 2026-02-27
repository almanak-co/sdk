"""ATR (Average True Range) Calculator.

The Average True Range (ATR) is a volatility indicator that shows how much
an asset moves on average during a given timeframe.

True Range is the greatest of:
1. Current High - Current Low
2. |Current High - Previous Close|
3. |Current Low - Previous Close|

ATR is typically the 14-period moving average of the True Range.

Common Uses:
- Position sizing: Larger ATR = smaller position size
- Stop-loss placement: Set stops at multiples of ATR
- Volatility filtering: Trade only when ATR above/below threshold
- Trend confirmation: Rising ATR confirms trend strength

Example:
    from almanak.framework.data.indicators import ATRCalculator

    atr_calc = ATRCalculator(ohlcv_provider=provider)
    atr = await atr_calc.calculate_atr("WETH", period=14, timeframe="1h")

    # Use for position sizing (e.g., risk 2 ATR per trade)
    stop_loss = current_price - (2 * atr)
"""

import logging
from typing import Any

from ..interfaces import (
    InsufficientDataError,
    OHLCVCandle,
    OHLCVProvider,
)

logger = logging.getLogger(__name__)


class ATRCalculator:
    """ATR (Average True Range) Calculator.

    Calculates ATR with configurable period using Wilder's smoothing method.

    Default Parameters:
        - Period: 14 (industry standard)

    Attributes:
        ohlcv_provider: Provider for OHLCV data (implements OHLCVProvider protocol)

    Example:
        provider = CoinGeckoOHLCVProvider()
        atr_calc = ATRCalculator(ohlcv_provider=provider)

        atr = await atr_calc.calculate_atr("WETH", period=14, timeframe="4h")
        print(f"ATR: ${atr:.2f}")

        # Position sizing
        if atr > 100:
            print("High volatility - reduce position size")
    """

    def __init__(self, ohlcv_provider: OHLCVProvider) -> None:
        """Initialize the ATR Calculator.

        Args:
            ohlcv_provider: Provider implementing OHLCVProvider protocol
        """
        self._ohlcv_provider = ohlcv_provider
        logger.info("Initialized ATRCalculator")

    @property
    def name(self) -> str:
        """Return indicator name."""
        return "ATR"

    @property
    def min_data_points(self) -> int:
        """Return minimum data points (period + 1)."""
        return 15  # 14 + 1 for default settings

    @staticmethod
    def _calculate_true_range(high: float, low: float, prev_close: float) -> float:
        """Calculate True Range for a single candle.

        True Range = max(
            high - low,
            |high - prev_close|,
            |low - prev_close|
        )

        Args:
            high: Current candle high
            low: Current candle low
            prev_close: Previous candle close

        Returns:
            True Range value
        """
        return max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )

    @staticmethod
    def calculate_atr_from_candles(
        candles: list[OHLCVCandle],
        period: int = 14,
    ) -> float:
        """Calculate ATR from OHLCV candles using Wilder's smoothing.

        Wilder's ATR uses a modified EMA:
        - First ATR = Simple average of first N True Ranges
        - Subsequent ATR = ((Prior ATR * (N-1)) + Current TR) / N

        Args:
            candles: List of OHLCVCandle objects (oldest first)
            period: ATR period (default 14)

        Returns:
            ATR value

        Raises:
            InsufficientDataError: If not enough candle data
        """
        required = period + 1
        if len(candles) < required:
            raise InsufficientDataError(
                required=required,
                available=len(candles),
                indicator="ATR",
            )

        # Calculate True Range series
        true_ranges: list[float] = []
        for i in range(1, len(candles)):
            tr = ATRCalculator._calculate_true_range(
                high=float(candles[i].high),
                low=float(candles[i].low),
                prev_close=float(candles[i - 1].close),
            )
            true_ranges.append(tr)

        # Calculate initial ATR (simple average of first 'period' TRs)
        atr = sum(true_ranges[:period]) / period

        # Apply Wilder's smoothing for remaining values
        for tr in true_ranges[period:]:
            atr = ((atr * (period - 1)) + tr) / period

        return atr

    @staticmethod
    def calculate_atr_from_prices(
        prices: list,
        period: int = 14,
    ) -> float:
        """Calculate ATR from close prices only using Wilder's smoothing.

        When only close prices are available (e.g., synthetic Monte Carlo paths),
        True Range is approximated as |close[i] - close[i-1]|. This is
        mathematically equivalent to ATR computed from OHLCV data where
        open = high = low = close for each candle.

        Args:
            prices: List of close prices (oldest first), as Decimal or float
            period: ATR period (default 14)

        Returns:
            ATR value (float)

        Raises:
            InsufficientDataError: If not enough price data
            ValueError: If period < 1
        """
        if period < 1:
            raise ValueError("ATR period must be at least 1")
        required = period + 1
        if len(prices) < required:
            raise InsufficientDataError(
                required=required,
                available=len(prices),
                indicator="ATR",
            )

        # Approximate True Range as absolute price change between consecutive closes
        true_ranges: list[float] = []
        for i in range(1, len(prices)):
            tr = abs(float(prices[i]) - float(prices[i - 1]))
            true_ranges.append(tr)

        # Calculate initial ATR (simple average of first 'period' TRs)
        atr = sum(true_ranges[:period]) / period

        # Apply Wilder's smoothing for remaining values
        for tr in true_ranges[period:]:
            atr = ((atr * (period - 1)) + tr) / period

        return atr

    async def calculate_atr(
        self,
        token: str,
        period: int = 14,
        timeframe: str = "1h",
    ) -> float:
        """Calculate ATR for a token.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            period: ATR period (default 14)
            timeframe: OHLCV candle timeframe (default "1h")

        Returns:
            ATR value (in the same units as the token price)

        Raises:
            InsufficientDataError: If not enough historical data
            DataSourceError: If data cannot be fetched

        Example:
            atr = await atr_calc.calculate_atr("WETH", period=14, timeframe="4h")

            # Stop-loss calculation
            current_price = 2500
            stop_loss = current_price - (2 * atr)
            print(f"Stop loss at ${stop_loss:.2f} (2 ATR below)")

            # Position sizing with 1% risk
            risk_per_trade = 10000 * 0.01  # $100
            position_size = risk_per_trade / atr
            print(f"Position size: {position_size:.2f} units")
        """
        limit = period + 20  # Buffer

        logger.debug(
            "Calculating ATR for %s with period=%d, timeframe=%s",
            token,
            period,
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
                required=period + 1,
                available=0,
                indicator="ATR",
            )

        atr = self.calculate_atr_from_candles(ohlcv_data, period)

        logger.debug(
            "Calculated ATR for %s: %.4f (period=%d, timeframe=%s)",
            token,
            atr,
            period,
            timeframe,
        )

        return atr

    async def calculate(
        self,
        token: str,
        timeframe: str = "1h",
        **params: Any,
    ) -> dict[str, float]:
        """Calculate ATR (BaseIndicator protocol implementation).

        Args:
            token: Token symbol
            timeframe: OHLCV candle timeframe
            **params: period (default 14)

        Returns:
            Dictionary with atr value
        """
        period = params.get("period", 14)
        atr = await self.calculate_atr(token, period, timeframe)
        return {"atr": atr}


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "ATRCalculator",
]
