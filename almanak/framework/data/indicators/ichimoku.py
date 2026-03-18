"""Ichimoku Cloud calculator."""

import logging
from typing import Any

from ..interfaces import (
    InsufficientDataError,
    OHLCVCandle,
    OHLCVProvider,
)
from .base import IchimokuResult

logger = logging.getLogger(__name__)


class IchimokuCalculator:
    """Ichimoku Cloud calculator."""

    def __init__(self, ohlcv_provider: OHLCVProvider) -> None:
        self._ohlcv_provider = ohlcv_provider
        logger.debug("Initialized IchimokuCalculator")

    @property
    def name(self) -> str:
        return "Ichimoku"

    @property
    def min_data_points(self) -> int:
        return 52

    @staticmethod
    def _midpoint(candles: list[OHLCVCandle]) -> float:
        highest = max(float(c.high) for c in candles)
        lowest = min(float(c.low) for c in candles)
        return (highest + lowest) / 2.0

    @staticmethod
    def calculate_ichimoku_from_candles(
        candles: list[OHLCVCandle],
        tenkan_period: int = 9,
        kijun_period: int = 26,
        senkou_b_period: int = 52,
    ) -> IchimokuResult:
        """Calculate Ichimoku components from OHLCV candles."""
        required = max(tenkan_period, kijun_period, senkou_b_period)
        if len(candles) < required:
            raise InsufficientDataError(
                required=required,
                available=len(candles),
                indicator="Ichimoku",
            )

        tenkan_sen = IchimokuCalculator._midpoint(candles[-tenkan_period:])
        kijun_sen = IchimokuCalculator._midpoint(candles[-kijun_period:])
        senkou_span_a = (tenkan_sen + kijun_sen) / 2.0
        senkou_span_b = IchimokuCalculator._midpoint(candles[-senkou_b_period:])
        current_price = float(candles[-1].close)

        # Chikou span is the current close plotted 26 periods back.
        chikou_span = current_price

        return IchimokuResult(
            tenkan_sen=tenkan_sen,
            kijun_sen=kijun_sen,
            senkou_span_a=senkou_span_a,
            senkou_span_b=senkou_span_b,
            chikou_span=chikou_span,
            current_price=current_price,
        )

    async def calculate_ichimoku(
        self,
        token: str,
        tenkan_period: int = 9,
        kijun_period: int = 26,
        senkou_b_period: int = 52,
        timeframe: str = "1h",
    ) -> IchimokuResult:
        """Calculate Ichimoku for a token."""
        limit = senkou_b_period + 60

        ohlcv_data = await self._ohlcv_provider.get_ohlcv(
            token=token,
            quote="USD",
            timeframe=timeframe,
            limit=limit,
        )

        if not ohlcv_data:
            raise InsufficientDataError(
                required=max(tenkan_period, kijun_period, senkou_b_period),
                available=0,
                indicator="Ichimoku",
            )

        return self.calculate_ichimoku_from_candles(
            ohlcv_data,
            tenkan_period=tenkan_period,
            kijun_period=kijun_period,
            senkou_b_period=senkou_b_period,
        )

    async def calculate(
        self,
        token: str,
        timeframe: str = "1h",
        **params: Any,
    ) -> dict[str, float]:
        """Calculate Ichimoku (BaseIndicator protocol implementation)."""
        tenkan_period = params.get("tenkan_period", 9)
        kijun_period = params.get("kijun_period", 26)
        senkou_b_period = params.get("senkou_b_period", 52)
        result = await self.calculate_ichimoku(
            token,
            tenkan_period=tenkan_period,
            kijun_period=kijun_period,
            senkou_b_period=senkou_b_period,
            timeframe=timeframe,
        )
        return result.to_dict()


__all__ = ["IchimokuCalculator"]
