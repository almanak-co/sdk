"""Moving Average Calculators (SMA, EMA, WMA).

This module provides moving average calculations for technical analysis.
All moving averages support configurable periods and timeframes.

Indicators:
    - SMA (Simple Moving Average): Equal-weighted average of last N prices
    - EMA (Exponential Moving Average): Weighted average favoring recent prices
    - WMA (Weighted Moving Average): Linearly weighted average

Example:
    from almanak.framework.data.indicators import MovingAverageCalculator, CoinGeckoOHLCVProvider

    provider = CoinGeckoOHLCVProvider()
    ma_calc = MovingAverageCalculator(ohlcv_provider=provider)

    # Calculate different moving averages
    sma_20 = await ma_calc.sma("WETH", period=20, timeframe="1h")
    ema_12 = await ma_calc.ema("WETH", period=12, timeframe="1h")
    wma_20 = await ma_calc.wma("WETH", period=20, timeframe="1h")

    # Use in strategy
    if current_price > sma_20:
        print("Price above 20-period SMA - bullish")
"""

import logging
from decimal import Decimal
from typing import Any

from ..interfaces import (
    InsufficientDataError,
    OHLCVProvider,
)

logger = logging.getLogger(__name__)


class MovingAverageCalculator:
    """Calculator for Simple, Exponential, and Weighted Moving Averages.

    Provides three types of moving averages commonly used in technical analysis:
    - SMA: Simple average of last N closing prices
    - EMA: Exponential moving average with configurable smoothing
    - WMA: Weighted moving average with linear weighting

    Attributes:
        ohlcv_provider: Provider for OHLCV data (implements OHLCVProvider protocol)

    Example:
        provider = CoinGeckoOHLCVProvider()
        ma_calc = MovingAverageCalculator(ohlcv_provider=provider)

        sma = await ma_calc.sma("WETH", period=20, timeframe="4h")
        ema = await ma_calc.ema("WETH", period=12, timeframe="1h")
    """

    def __init__(self, ohlcv_provider: OHLCVProvider) -> None:
        """Initialize the Moving Average Calculator.

        Args:
            ohlcv_provider: Provider implementing OHLCVProvider protocol
        """
        self._ohlcv_provider = ohlcv_provider
        logger.info("Initialized MovingAverageCalculator")

    @property
    def name(self) -> str:
        """Return indicator name."""
        return "MovingAverage"

    @property
    def min_data_points(self) -> int:
        """Return minimum data points (depends on period, default 20)."""
        return 20

    @staticmethod
    def calculate_sma_from_prices(close_prices: list[Decimal], period: int) -> float:
        """Calculate SMA from a list of close prices.

        Args:
            close_prices: List of closing prices (oldest first)
            period: Number of periods for the average

        Returns:
            Simple Moving Average value

        Raises:
            InsufficientDataError: If not enough price data
        """
        if len(close_prices) < period:
            raise InsufficientDataError(
                required=period,
                available=len(close_prices),
                indicator="SMA",
            )

        recent_prices = close_prices[-period:]
        total = sum(float(p) for p in recent_prices)
        return total / period

    @staticmethod
    def calculate_ema_from_prices(close_prices: list[Decimal], period: int, smoothing: float = 2.0) -> float:
        """Calculate EMA from a list of close prices.

        Uses the formula: EMA = Price(t) * k + EMA(y) * (1 - k)
        where k = smoothing / (period + 1)

        Args:
            close_prices: List of closing prices (oldest first)
            period: Number of periods for the average
            smoothing: Smoothing factor (default 2.0 for standard EMA)

        Returns:
            Exponential Moving Average value

        Raises:
            InsufficientDataError: If not enough price data
        """
        if len(close_prices) < period:
            raise InsufficientDataError(
                required=period,
                available=len(close_prices),
                indicator="EMA",
            )

        # Calculate multiplier
        k = smoothing / (period + 1)

        # Start with SMA of first 'period' values
        sma = sum(float(p) for p in close_prices[:period]) / period
        ema = sma

        # Apply EMA formula for remaining values
        for price in close_prices[period:]:
            ema = float(price) * k + ema * (1 - k)

        return ema

    @staticmethod
    def calculate_wma_from_prices(close_prices: list[Decimal], period: int) -> float:
        """Calculate WMA from a list of close prices.

        Weighted Moving Average: More recent prices have higher weights.
        Weight for position i (1-indexed from oldest): i / sum(1..period)

        Args:
            close_prices: List of closing prices (oldest first)
            period: Number of periods for the average

        Returns:
            Weighted Moving Average value

        Raises:
            InsufficientDataError: If not enough price data
        """
        if len(close_prices) < period:
            raise InsufficientDataError(
                required=period,
                available=len(close_prices),
                indicator="WMA",
            )

        recent_prices = close_prices[-period:]

        # Calculate weight sum: 1 + 2 + 3 + ... + period = period * (period + 1) / 2
        weight_sum = period * (period + 1) / 2

        # Calculate weighted sum
        weighted_sum = 0.0
        for i, price in enumerate(recent_prices, start=1):
            weighted_sum += float(price) * i

        return weighted_sum / weight_sum

    async def sma(self, token: str, period: int = 20, timeframe: str = "1h") -> float:
        """Calculate Simple Moving Average for a token.

        SMA is the unweighted mean of the last N closing prices.
        Commonly used periods: 10, 20, 50, 100, 200

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            period: Number of periods (default 20)
            timeframe: OHLCV candle timeframe (default "1h")

        Returns:
            SMA value as float

        Raises:
            InsufficientDataError: If not enough historical data
            DataSourceError: If data cannot be fetched

        Example:
            sma_20 = await ma_calc.sma("WETH", period=20, timeframe="1h")
            sma_200 = await ma_calc.sma("WETH", period=200, timeframe="1d")
        """
        limit = period + 10  # Buffer

        logger.debug(
            "Calculating SMA for %s with period=%d, timeframe=%s",
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
                required=period,
                available=0,
                indicator="SMA",
            )

        close_prices = [candle.close for candle in ohlcv_data]
        sma = self.calculate_sma_from_prices(close_prices, period)

        logger.debug(
            "Calculated SMA for %s: %.6f (period=%d, timeframe=%s)",
            token,
            sma,
            period,
            timeframe,
        )

        return sma

    async def ema(
        self,
        token: str,
        period: int = 12,
        timeframe: str = "1h",
        smoothing: float = 2.0,
    ) -> float:
        """Calculate Exponential Moving Average for a token.

        EMA gives more weight to recent prices using exponential decay.
        Commonly used periods: 12, 26 (for MACD), 9, 21

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            period: Number of periods (default 12)
            timeframe: OHLCV candle timeframe (default "1h")
            smoothing: Smoothing factor (default 2.0)

        Returns:
            EMA value as float

        Raises:
            InsufficientDataError: If not enough historical data
            DataSourceError: If data cannot be fetched

        Example:
            ema_12 = await ma_calc.ema("WETH", period=12, timeframe="1h")
            ema_26 = await ma_calc.ema("WETH", period=26, timeframe="1h")
        """
        # Request more data for EMA to have stable values
        limit = period * 3

        logger.debug(
            "Calculating EMA for %s with period=%d, timeframe=%s",
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
                required=period,
                available=0,
                indicator="EMA",
            )

        close_prices = [candle.close for candle in ohlcv_data]
        ema = self.calculate_ema_from_prices(close_prices, period, smoothing)

        logger.debug(
            "Calculated EMA for %s: %.6f (period=%d, timeframe=%s)",
            token,
            ema,
            period,
            timeframe,
        )

        return ema

    async def wma(self, token: str, period: int = 20, timeframe: str = "1h") -> float:
        """Calculate Weighted Moving Average for a token.

        WMA assigns higher weights to more recent data points linearly.
        Most recent price has weight N, oldest has weight 1.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            period: Number of periods (default 20)
            timeframe: OHLCV candle timeframe (default "1h")

        Returns:
            WMA value as float

        Raises:
            InsufficientDataError: If not enough historical data
            DataSourceError: If data cannot be fetched

        Example:
            wma_20 = await ma_calc.wma("WETH", period=20, timeframe="1h")
        """
        limit = period + 10  # Buffer

        logger.debug(
            "Calculating WMA for %s with period=%d, timeframe=%s",
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
                required=period,
                available=0,
                indicator="WMA",
            )

        close_prices = [candle.close for candle in ohlcv_data]
        wma = self.calculate_wma_from_prices(close_prices, period)

        logger.debug(
            "Calculated WMA for %s: %.6f (period=%d, timeframe=%s)",
            token,
            wma,
            period,
            timeframe,
        )

        return wma

    async def calculate(
        self,
        token: str,
        timeframe: str = "1h",
        **params: Any,
    ) -> dict[str, float]:
        """Calculate moving average (BaseIndicator protocol implementation).

        Args:
            token: Token symbol
            timeframe: OHLCV candle timeframe
            **params: Must include 'type' (sma/ema/wma) and 'period'

        Returns:
            Dictionary with calculated value

        Example:
            result = await ma_calc.calculate("WETH", type="sma", period=20)
            # {"sma": 2500.0}
        """
        ma_type = params.get("type", "sma").lower()
        period = params.get("period", 20)

        if ma_type == "sma":
            value = await self.sma(token, period, timeframe)
            return {"sma": value}
        elif ma_type == "ema":
            smoothing = params.get("smoothing", 2.0)
            value = await self.ema(token, period, timeframe, smoothing)
            return {"ema": value}
        elif ma_type == "wma":
            value = await self.wma(token, period, timeframe)
            return {"wma": value}
        else:
            raise ValueError(f"Unknown moving average type: {ma_type}")


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "MovingAverageCalculator",
]
