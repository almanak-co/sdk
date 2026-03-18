"""ADX (Average Directional Index) calculator.

ADX measures trend strength (0-100) and is commonly used with +DI/-DI to
determine trend direction.
"""

import logging
from typing import Any

from ..interfaces import (
    InsufficientDataError,
    OHLCVCandle,
    OHLCVProvider,
)
from .base import ADXResult

logger = logging.getLogger(__name__)


class ADXCalculator:
    """ADX calculator using Wilder's smoothing method."""

    def __init__(self, ohlcv_provider: OHLCVProvider) -> None:
        self._ohlcv_provider = ohlcv_provider
        logger.debug("Initialized ADXCalculator")

    @property
    def name(self) -> str:
        return "ADX"

    @property
    def min_data_points(self) -> int:
        # ADX needs around 2*period candles for a stable value.
        return 28

    @staticmethod
    def _calculate_dx(plus_di: float, minus_di: float) -> float:
        denom = plus_di + minus_di
        if denom <= 0:
            return 0.0
        return 100.0 * abs(plus_di - minus_di) / denom

    @staticmethod
    def calculate_adx_from_candles(
        candles: list[OHLCVCandle],
        period: int = 14,
    ) -> ADXResult:
        """Calculate ADX/+DI/-DI from OHLCV candles."""
        required = period * 2
        if len(candles) < required:
            raise InsufficientDataError(
                required=required,
                available=len(candles),
                indicator="ADX",
            )

        tr_values: list[float] = []
        plus_dm_values: list[float] = []
        minus_dm_values: list[float] = []

        for i in range(1, len(candles)):
            prev = candles[i - 1]
            curr = candles[i]

            high_diff = float(curr.high) - float(prev.high)
            low_diff = float(prev.low) - float(curr.low)

            plus_dm = high_diff if high_diff > low_diff and high_diff > 0 else 0.0
            minus_dm = low_diff if low_diff > high_diff and low_diff > 0 else 0.0

            tr = max(
                float(curr.high) - float(curr.low),
                abs(float(curr.high) - float(prev.close)),
                abs(float(curr.low) - float(prev.close)),
            )

            tr_values.append(tr)
            plus_dm_values.append(plus_dm)
            minus_dm_values.append(minus_dm)

        tr_smooth = sum(tr_values[:period])
        plus_dm_smooth = sum(plus_dm_values[:period])
        minus_dm_smooth = sum(minus_dm_values[:period])

        if tr_smooth <= 0:
            return ADXResult(adx=0.0, plus_di=0.0, minus_di=0.0)

        plus_di = 100.0 * (plus_dm_smooth / tr_smooth)
        minus_di = 100.0 * (minus_dm_smooth / tr_smooth)
        dx_values: list[float] = [ADXCalculator._calculate_dx(plus_di, minus_di)]

        for idx in range(period, len(tr_values)):
            tr_smooth = tr_smooth - (tr_smooth / period) + tr_values[idx]
            plus_dm_smooth = plus_dm_smooth - (plus_dm_smooth / period) + plus_dm_values[idx]
            minus_dm_smooth = minus_dm_smooth - (minus_dm_smooth / period) + minus_dm_values[idx]

            if tr_smooth <= 0:
                plus_di = 0.0
                minus_di = 0.0
            else:
                plus_di = 100.0 * (plus_dm_smooth / tr_smooth)
                minus_di = 100.0 * (minus_dm_smooth / tr_smooth)

            dx_values.append(ADXCalculator._calculate_dx(plus_di, minus_di))

        if len(dx_values) < period:
            raise InsufficientDataError(
                required=required,
                available=len(candles),
                indicator="ADX",
            )

        adx = sum(dx_values[:period]) / period
        for dx in dx_values[period:]:
            adx = ((adx * (period - 1)) + dx) / period

        return ADXResult(adx=adx, plus_di=plus_di, minus_di=minus_di)

    async def calculate_adx(
        self,
        token: str,
        period: int = 14,
        timeframe: str = "1h",
    ) -> ADXResult:
        """Calculate ADX for a token."""
        limit = period * 3 + 10

        ohlcv_data = await self._ohlcv_provider.get_ohlcv(
            token=token,
            quote="USD",
            timeframe=timeframe,
            limit=limit,
        )

        if not ohlcv_data:
            raise InsufficientDataError(
                required=period * 2,
                available=0,
                indicator="ADX",
            )

        return self.calculate_adx_from_candles(ohlcv_data, period)

    async def calculate(
        self,
        token: str,
        timeframe: str = "1h",
        **params: Any,
    ) -> dict[str, float]:
        """Calculate ADX (BaseIndicator protocol implementation)."""
        period = params.get("period", 14)
        result = await self.calculate_adx(token, period=period, timeframe=timeframe)
        return result.to_dict()


__all__ = ["ADXCalculator"]
