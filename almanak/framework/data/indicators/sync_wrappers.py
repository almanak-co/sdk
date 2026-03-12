"""Synchronous wrapper factories for async indicator calculators.

Each factory creates a sync callable that wraps an async calculator method,
converting calculator result types (float/dataclass) to MarketSnapshot data
types (Decimal-based dataclasses).

Uses the nest_asyncio pattern inherited from the RSI workflow to run
async code from synchronous strategy `decide()` methods.
"""

import asyncio
import logging
from collections.abc import Callable
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ...strategies.intent_strategy import (
        ATRData,
        BollingerBandsData,
        MACDData,
        MAData,
        RSIData,
        StochasticData,
    )
    from .atr import ATRCalculator
    from .bollinger_bands import BollingerBandsCalculator
    from .macd import MACDCalculator
    from .moving_averages import MovingAverageCalculator
    from .rsi import RSICalculator
    from .stochastic import StochasticCalculator

logger = logging.getLogger(__name__)


def _run_async(coro):
    """Run an async coroutine synchronously, handling nested event loops."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop is not None:
        import nest_asyncio

        nest_asyncio.apply()
        return asyncio.get_event_loop().run_until_complete(coro)
    else:
        return asyncio.run(coro)


def create_sync_rsi_func(
    rsi_calculator: "RSICalculator",
) -> Callable[..., "RSIData"]:
    """Create a sync callable wrapper for RSI calculator.

    Args:
        rsi_calculator: RSI calculator instance

    Returns:
        Sync callable (token, period, timeframe=) -> RSIData
    """
    from ...strategies.intent_strategy import RSIData

    def sync_rsi(token: str, period: int = 14, timeframe: str = "4h") -> RSIData:
        rsi_value = _run_async(rsi_calculator.calculate_rsi(token, period, timeframe=timeframe))
        return RSIData(value=Decimal(str(rsi_value)), period=period)

    return sync_rsi


def create_sync_macd_func(
    macd_calculator: "MACDCalculator",
) -> Callable[..., "MACDData"]:
    """Create a sync callable wrapper for MACD calculator.

    Args:
        macd_calculator: MACD calculator instance

    Returns:
        Sync callable (token, fast, slow, signal, timeframe=) -> MACDData
    """
    from ...strategies.intent_strategy import MACDData

    def sync_macd(
        token: str,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
        timeframe: str = "4h",
    ) -> MACDData:
        result = _run_async(
            macd_calculator.calculate_macd(token, fast_period, slow_period, signal_period, timeframe=timeframe)
        )
        return MACDData(
            macd_line=Decimal(str(result.macd_line)),
            signal_line=Decimal(str(result.signal_line)),
            histogram=Decimal(str(result.histogram)),
            fast_period=fast_period,
            slow_period=slow_period,
            signal_period=signal_period,
        )

    return sync_macd


def create_sync_bollinger_func(
    bb_calculator: "BollingerBandsCalculator",
) -> Callable[..., "BollingerBandsData"]:
    """Create a sync callable wrapper for Bollinger Bands calculator.

    Args:
        bb_calculator: Bollinger Bands calculator instance

    Returns:
        Sync callable (token, period, std_dev, timeframe=) -> BollingerBandsData
    """
    from ...strategies.intent_strategy import BollingerBandsData

    def sync_bollinger(
        token: str,
        period: int = 20,
        std_dev: float = 2.0,
        timeframe: str = "4h",
    ) -> BollingerBandsData:
        result = _run_async(bb_calculator.calculate_bollinger_bands(token, period, std_dev, timeframe=timeframe))
        return BollingerBandsData(
            upper_band=Decimal(str(result.upper_band)),
            middle_band=Decimal(str(result.middle_band)),
            lower_band=Decimal(str(result.lower_band)),
            bandwidth=Decimal(str(result.bandwidth)),
            percent_b=Decimal(str(result.percent_b)),
            period=period,
            std_dev=std_dev,
        )

    return sync_bollinger


def create_sync_stochastic_func(
    stoch_calculator: "StochasticCalculator",
) -> Callable[..., "StochasticData"]:
    """Create a sync callable wrapper for Stochastic calculator.

    Args:
        stoch_calculator: Stochastic calculator instance

    Returns:
        Sync callable (token, k_period, d_period, timeframe=) -> StochasticData
    """
    from ...strategies.intent_strategy import StochasticData

    def sync_stochastic(
        token: str,
        k_period: int = 14,
        d_period: int = 3,
        timeframe: str = "4h",
    ) -> StochasticData:
        result = _run_async(stoch_calculator.calculate_stochastic(token, k_period, d_period, timeframe=timeframe))
        return StochasticData(
            k_value=Decimal(str(result.k_value)),
            d_value=Decimal(str(result.d_value)),
            k_period=k_period,
            d_period=d_period,
        )

    return sync_stochastic


def create_sync_atr_func(
    atr_calculator: "ATRCalculator",
    price_oracle: Callable[[str, str], Decimal],
) -> Callable[..., "ATRData"]:
    """Create a sync callable wrapper for ATR calculator.

    Computes derived field value_percent = (atr / price) * 100.

    Args:
        atr_calculator: ATR calculator instance
        price_oracle: Sync price oracle (token, quote) -> Decimal

    Returns:
        Sync callable (token, period, timeframe=) -> ATRData
    """
    from ...strategies.intent_strategy import ATRData

    def sync_atr(
        token: str,
        period: int = 14,
        timeframe: str = "4h",
    ) -> ATRData:
        atr_value = _run_async(atr_calculator.calculate_atr(token, period, timeframe=timeframe))
        atr_decimal = Decimal(str(atr_value))

        price = price_oracle(token, "USD")
        if price <= 0:
            raise ValueError(f"Price oracle returned non-positive price for {token}/USD: {price}")

        value_percent = (atr_decimal / price) * Decimal("100")

        return ATRData(
            value=atr_decimal,
            value_percent=value_percent,
            period=period,
        )

    return sync_atr


def create_sync_sma_func(
    ma_calculator: "MovingAverageCalculator",
    price_oracle: Callable[[str, str], Decimal],
) -> Callable[..., "MAData"]:
    """Create a sync callable wrapper for SMA calculator.

    Sets current_price from price oracle for MAData.is_price_above/below helpers.

    Args:
        ma_calculator: Moving Average calculator instance
        price_oracle: Sync price oracle (token, quote) -> Decimal

    Returns:
        Sync callable (token, period, timeframe=) -> MAData
    """
    from ...strategies.intent_strategy import MAData

    def sync_sma(
        token: str,
        period: int = 20,
        timeframe: str = "4h",
    ) -> MAData:
        ma_value = _run_async(ma_calculator.sma(token, period, timeframe=timeframe))

        current_price = price_oracle(token, "USD")
        if current_price <= 0:
            raise ValueError(f"Price oracle returned non-positive price for {token}/USD: {current_price}")

        return MAData(
            value=Decimal(str(ma_value)),
            ma_type="SMA",
            period=period,
            current_price=current_price,
        )

    return sync_sma


def create_sync_ema_func(
    ma_calculator: "MovingAverageCalculator",
    price_oracle: Callable[[str, str], Decimal],
) -> Callable[..., "MAData"]:
    """Create a sync callable wrapper for EMA calculator.

    Sets current_price from price oracle for MAData.is_price_above/below helpers.

    Args:
        ma_calculator: Moving Average calculator instance
        price_oracle: Sync price oracle (token, quote) -> Decimal

    Returns:
        Sync callable (token, period, timeframe=) -> MAData
    """
    from ...strategies.intent_strategy import MAData

    def sync_ema(
        token: str,
        period: int = 12,
        timeframe: str = "4h",
    ) -> MAData:
        ma_value = _run_async(ma_calculator.ema(token, period, timeframe=timeframe))

        current_price = price_oracle(token, "USD")
        if current_price <= 0:
            raise ValueError(f"Price oracle returned non-positive price for {token}/USD: {current_price}")

        return MAData(
            value=Decimal(str(ma_value)),
            ma_type="EMA",
            period=period,
            current_price=current_price,
        )

    return sync_ema
