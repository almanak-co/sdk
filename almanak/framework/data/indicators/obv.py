"""OBV (On-Balance Volume) calculator."""

import logging
from typing import Any

from ..interfaces import (
    InsufficientDataError,
    OHLCVCandle,
    OHLCVProvider,
)
from .base import OBVResult

logger = logging.getLogger(__name__)


class OBVCalculator:
    """OBV calculator with configurable signal line period."""

    def __init__(self, ohlcv_provider: OHLCVProvider) -> None:
        self._ohlcv_provider = ohlcv_provider
        logger.debug("Initialized OBVCalculator")

    @property
    def name(self) -> str:
        return "OBV"

    @property
    def min_data_points(self) -> int:
        return 22  # signal_period=21 + current candle

    @staticmethod
    def calculate_obv_from_candles(
        candles: list[OHLCVCandle],
        signal_period: int = 21,
    ) -> OBVResult:
        """Calculate OBV and OBV SMA signal line from OHLCV candles."""
        required = max(signal_period + 1, 2)
        if len(candles) < required:
            raise InsufficientDataError(
                required=required,
                available=len(candles),
                indicator="OBV",
            )

        obv_values: list[float] = [0.0]

        for i in range(1, len(candles)):
            prev_close = float(candles[i - 1].close)
            curr_close = float(candles[i].close)
            raw_vol = candles[i].volume
            volume = float(raw_vol) if raw_vol is not None else 0.0

            obv = obv_values[-1]
            if curr_close > prev_close:
                obv += volume
            elif curr_close < prev_close:
                obv -= volume
            obv_values.append(obv)

        signal_values = obv_values[-signal_period:]
        signal_line = sum(signal_values) / len(signal_values)
        return OBVResult(obv=obv_values[-1], signal_line=signal_line)

    async def calculate_obv(
        self,
        token: str,
        signal_period: int = 21,
        timeframe: str = "1h",
    ) -> OBVResult:
        """Calculate OBV for a token."""
        limit = signal_period + 200

        ohlcv_data = await self._ohlcv_provider.get_ohlcv(
            token=token,
            quote="USD",
            timeframe=timeframe,
            limit=limit,
        )

        if not ohlcv_data:
            raise InsufficientDataError(
                required=signal_period + 1,
                available=0,
                indicator="OBV",
            )

        return self.calculate_obv_from_candles(ohlcv_data, signal_period=signal_period)

    async def calculate(
        self,
        token: str,
        timeframe: str = "1h",
        **params: Any,
    ) -> dict[str, float]:
        """Calculate OBV (BaseIndicator protocol implementation)."""
        signal_period = params.get("signal_period", 21)
        result = await self.calculate_obv(token, signal_period=signal_period, timeframe=timeframe)
        return result.to_dict()


__all__ = ["OBVCalculator"]
