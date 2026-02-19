"""Bollinger Bands Calculator.

Bollinger Bands are a volatility indicator consisting of:
- Middle Band: Simple Moving Average (SMA)
- Upper Band: SMA + (standard deviation * multiplier)
- Lower Band: SMA - (standard deviation * multiplier)

Trading Signals:
- Price touching lower band: Potential buy signal (oversold)
- Price touching upper band: Potential sell signal (overbought)
- Band squeeze: Low volatility, potential breakout incoming
- Band expansion: High volatility, strong trend

Example:
    from almanak.framework.data.indicators import BollingerBandsCalculator

    bb_calc = BollingerBandsCalculator(ohlcv_provider=provider)
    bb = await bb_calc.calculate_bollinger_bands("WETH", period=20, std_dev=2.0, timeframe="1h")

    if bb.percent_b < 0:
        print("Price below lower band - oversold!")
    elif bb.percent_b > 1:
        print("Price above upper band - overbought!")
"""

import logging
import math
from decimal import Decimal
from typing import Any

from ..interfaces import (
    InsufficientDataError,
    OHLCVProvider,
)
from .base import BollingerBandsResult

logger = logging.getLogger(__name__)


class BollingerBandsCalculator:
    """Bollinger Bands Calculator.

    Calculates Bollinger Bands with configurable period and standard deviation multiplier.

    Default Parameters:
        - Period: 20 (industry standard)
        - Standard Deviation Multiplier: 2.0 (captures ~95% of price movement)

    Attributes:
        ohlcv_provider: Provider for OHLCV data (implements OHLCVProvider protocol)

    Example:
        provider = CoinGeckoOHLCVProvider()
        bb_calc = BollingerBandsCalculator(ohlcv_provider=provider)

        bb = await bb_calc.calculate_bollinger_bands("WETH", period=20, std_dev=2.0)
        print(f"Upper: {bb.upper_band}, Middle: {bb.middle_band}, Lower: {bb.lower_band}")

        # Check price position
        if bb.percent_b < 0.2:
            print("Near lower band - potential buy")
    """

    def __init__(self, ohlcv_provider: OHLCVProvider) -> None:
        """Initialize the Bollinger Bands Calculator.

        Args:
            ohlcv_provider: Provider implementing OHLCVProvider protocol
        """
        self._ohlcv_provider = ohlcv_provider
        logger.info("Initialized BollingerBandsCalculator")

    @property
    def name(self) -> str:
        """Return indicator name."""
        return "BollingerBands"

    @property
    def min_data_points(self) -> int:
        """Return minimum data points (default period of 20)."""
        return 20

    @staticmethod
    def calculate_bollinger_from_prices(
        close_prices: list[Decimal],
        period: int = 20,
        std_dev_multiplier: float = 2.0,
    ) -> BollingerBandsResult:
        """Calculate Bollinger Bands from a list of close prices.

        Args:
            close_prices: List of closing prices (oldest first)
            period: SMA period (default 20)
            std_dev_multiplier: Standard deviation multiplier (default 2.0)

        Returns:
            BollingerBandsResult with all band values

        Raises:
            InsufficientDataError: If not enough price data
        """
        if len(close_prices) < period:
            raise InsufficientDataError(
                required=period,
                available=len(close_prices),
                indicator="BollingerBands",
            )

        # Get recent prices for calculation
        recent_prices = [float(p) for p in close_prices[-period:]]

        # Calculate SMA (middle band)
        sma = sum(recent_prices) / period

        # Calculate standard deviation
        variance = sum((p - sma) ** 2 for p in recent_prices) / period
        std_dev = math.sqrt(variance)

        # Calculate bands
        upper_band = sma + (std_dev_multiplier * std_dev)
        lower_band = sma - (std_dev_multiplier * std_dev)

        # Calculate bandwidth (volatility measure)
        # Bandwidth = (Upper - Lower) / Middle
        bandwidth = (upper_band - lower_band) / sma if sma > 0 else 0.0

        # Calculate %B (position within bands)
        # %B = (Price - Lower) / (Upper - Lower)
        # %B < 0: below lower band
        # %B > 1: above upper band
        # %B = 0.5: at middle band
        current_price = float(close_prices[-1])
        band_width = upper_band - lower_band
        percent_b = (current_price - lower_band) / band_width if band_width > 0 else 0.5

        return BollingerBandsResult(
            upper_band=upper_band,
            middle_band=sma,
            lower_band=lower_band,
            bandwidth=bandwidth,
            percent_b=percent_b,
        )

    async def calculate_bollinger_bands(
        self,
        token: str,
        period: int = 20,
        std_dev: float = 2.0,
        timeframe: str = "1h",
    ) -> BollingerBandsResult:
        """Calculate Bollinger Bands for a token.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            period: SMA period (default 20)
            std_dev: Standard deviation multiplier (default 2.0)
            timeframe: OHLCV candle timeframe (default "1h")

        Returns:
            BollingerBandsResult with upper_band, middle_band, lower_band,
            bandwidth, and percent_b

        Raises:
            InsufficientDataError: If not enough historical data
            DataSourceError: If data cannot be fetched

        Example:
            bb = await bb_calc.calculate_bollinger_bands("WETH", period=20, std_dev=2.0)

            # Trading logic
            if bb.percent_b < 0:
                # Price below lower band - potential buy
                return SwapIntent(token_in="USDC", token_out="WETH", ...)
            elif bb.percent_b > 1:
                # Price above upper band - potential sell
                return SwapIntent(token_in="WETH", token_out="USDC", ...)

            # Volatility check
            if bb.bandwidth < 0.05:
                print("Low volatility - squeeze detected")
        """
        limit = period + 10  # Buffer

        logger.debug(
            "Calculating Bollinger Bands for %s with period=%d, std_dev=%.1f, timeframe=%s",
            token,
            period,
            std_dev,
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
                indicator="BollingerBands",
            )

        close_prices = [candle.close for candle in ohlcv_data]
        bb = self.calculate_bollinger_from_prices(close_prices, period, std_dev)

        logger.debug(
            "Calculated Bollinger Bands for %s: upper=%.2f, middle=%.2f, lower=%.2f, %%B=%.2f",
            token,
            bb.upper_band,
            bb.middle_band,
            bb.lower_band,
            bb.percent_b,
        )

        return bb

    async def calculate(
        self,
        token: str,
        timeframe: str = "1h",
        **params: Any,
    ) -> dict[str, float]:
        """Calculate Bollinger Bands (BaseIndicator protocol implementation).

        Args:
            token: Token symbol
            timeframe: OHLCV candle timeframe
            **params: period (default 20), std_dev (default 2.0)

        Returns:
            Dictionary with all Bollinger Bands values
        """
        period = params.get("period", 20)
        std_dev = params.get("std_dev", 2.0)

        result = await self.calculate_bollinger_bands(token, period, std_dev, timeframe)
        return result.to_dict()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "BollingerBandsCalculator",
]
