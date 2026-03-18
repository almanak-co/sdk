"""CCI (Commodity Channel Index) calculator."""

import logging
from typing import Any

from ..interfaces import (
    InsufficientDataError,
    OHLCVCandle,
    OHLCVProvider,
)

logger = logging.getLogger(__name__)


class CCICalculator:
    """CCI calculator.

    CCI formula:
        CCI = (TP - SMA(TP)) / (0.015 * MeanDeviation(TP))
    where TP is typical price = (high + low + close) / 3.
    """

    def __init__(self, ohlcv_provider: OHLCVProvider) -> None:
        self._ohlcv_provider = ohlcv_provider
        logger.debug("Initialized CCICalculator")

    @property
    def name(self) -> str:
        return "CCI"

    @property
    def min_data_points(self) -> int:
        return 20

    @staticmethod
    def calculate_cci_from_candles(
        candles: list[OHLCVCandle],
        period: int = 20,
    ) -> float:
        """Calculate CCI from OHLCV candles."""
        if len(candles) < period:
            raise InsufficientDataError(
                required=period,
                available=len(candles),
                indicator="CCI",
            )

        typical_prices = [(float(c.high) + float(c.low) + float(c.close)) / 3.0 for c in candles]
        recent = typical_prices[-period:]

        sma_tp = sum(recent) / period
        mean_deviation = sum(abs(tp - sma_tp) for tp in recent) / period
        if mean_deviation == 0:
            return 0.0

        current_tp = recent[-1]
        return (current_tp - sma_tp) / (0.015 * mean_deviation)

    async def calculate_cci(
        self,
        token: str,
        period: int = 20,
        timeframe: str = "1h",
    ) -> float:
        """Calculate CCI for a token."""
        limit = period + 20

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
                indicator="CCI",
            )

        return self.calculate_cci_from_candles(ohlcv_data, period=period)

    async def calculate(
        self,
        token: str,
        timeframe: str = "1h",
        **params: Any,
    ) -> dict[str, float]:
        """Calculate CCI (BaseIndicator protocol implementation)."""
        period = params.get("period", 20)
        cci = await self.calculate_cci(token, period=period, timeframe=timeframe)
        return {"cci": cci}


__all__ = ["CCICalculator"]
