"""OBV (On-Balance Volume) calculator."""

import logging
from typing import Any

from ..interfaces import (
    InsufficientDataError,
    OHLCVCandle,
    OHLCVProvider,
    VolumeUnavailableError,
)
from ..timeframes import OHLCVTimeframe, parse_ohlcv_timeframe
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

        # ALM-3148 §8.2: refuse rather than substitute. `volume is None` means
        # the source did not measure volume — a perp venue's index candles carry
        # none at all — and the previous `else 0.0` silently turned that into a
        # measured zero. OBV then accumulated nothing and returned a flat 0.0
        # with a 0.0 signal line: a structurally valid number meaning "no
        # accumulation pressure" when the truth was "no volume data exists".
        # Latent until now because only volume-less sources reached it; making
        # a volume-less source the default for perp strategies is exactly what
        # would have converted it into a certain, silent wrong answer.
        unmeasured = sum(1 for candle in candles[1:] if candle.volume is None)
        if unmeasured:
            raise VolumeUnavailableError(indicator="OBV", observed=unmeasured, inspected=len(candles) - 1)

        obv_values: list[float] = [0.0]

        for i in range(1, len(candles)):
            prev_close = float(candles[i - 1].close)
            curr_close = float(candles[i].close)
            volume = float(candles[i].volume)  # type: ignore[arg-type]  # guarded above

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
        timeframe: OHLCVTimeframe = OHLCVTimeframe.ONE_HOUR,
    ) -> OBVResult:
        """Calculate OBV for a token."""
        timeframe = parse_ohlcv_timeframe(timeframe, field_name="OBV timeframe")
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
        timeframe: OHLCVTimeframe = OHLCVTimeframe.ONE_HOUR,
        **params: Any,
    ) -> dict[str, float]:
        """Calculate OBV (BaseIndicator protocol implementation)."""
        signal_period = params.get("signal_period", 21)
        result = await self.calculate_obv(token, signal_period=signal_period, timeframe=timeframe)
        return result.to_dict()


__all__ = ["OBVCalculator"]
