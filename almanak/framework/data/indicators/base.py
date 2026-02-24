"""Base Protocol and Types for Technical Indicators.

This module provides the foundational protocol and types for the indicator framework,
enabling consistent implementation of technical analysis indicators.

Key Features:
    - BaseIndicator protocol for structural subtyping
    - Common result dataclasses for multi-value indicators
    - Standardized error types for indicator-specific failures

Example:
    from almanak.framework.data.indicators.base import BaseIndicator

    @runtime_checkable
    class MyIndicator(BaseIndicator):
        @property
        def name(self) -> str:
            return "MyIndicator"

        @property
        def min_data_points(self) -> int:
            return 14

        async def calculate(
            self, token: str, timeframe: str = "1h", **params
        ) -> dict[str, float]:
            # Implementation
            return {"value": 50.0}
"""

from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class BaseIndicator(Protocol):
    """Base protocol for all technical indicators.

    This protocol defines the interface that all technical indicators must implement.
    It uses structural subtyping (duck typing) so any class with matching methods
    will be considered a valid implementation.

    Attributes:
        name: Unique identifier for this indicator (e.g., "RSI", "BollingerBands")
        min_data_points: Minimum historical data points required for calculation

    Example Implementation:
        class RSICalculator:
            @property
            def name(self) -> str:
                return "RSI"

            @property
            def min_data_points(self) -> int:
                return 15  # period + 1

            async def calculate(
                self, token: str, timeframe: str = "1h", **params
            ) -> dict[str, float]:
                period = params.get("period", 14)
                # ... calculation logic
                return {"rsi": 45.5}
    """

    @property
    def name(self) -> str:
        """Return the unique name of this indicator.

        Returns:
            Indicator name (e.g., "RSI", "BollingerBands", "MACD")
        """
        ...

    @property
    def min_data_points(self) -> int:
        """Return the minimum number of OHLCV data points required.

        This is the absolute minimum for the indicator to produce a valid result.
        For RSI with period=14, this would be 15 (period + 1).
        For Bollinger Bands with period=20, this would be 20.

        Returns:
            Minimum required data points
        """
        ...

    async def calculate(
        self,
        token: str,
        timeframe: str = "1h",
        **params: Any,
    ) -> dict[str, float]:
        """Calculate the indicator value(s) for a token.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            timeframe: OHLCV candle timeframe (default "1h")
                Supported: "1m", "5m", "15m", "1h", "4h", "1d"
            **params: Indicator-specific parameters (e.g., period=14 for RSI)

        Returns:
            Dictionary of indicator values. Single-value indicators return
            one key (e.g., {"rsi": 45.5}). Multi-value indicators return
            multiple keys (e.g., {"upper": 3000, "middle": 2800, "lower": 2600}).

        Raises:
            InsufficientDataError: If not enough historical data
            DataSourceError: If data cannot be fetched
        """
        ...


@dataclass(frozen=True)
class BollingerBandsResult:
    """Result from Bollinger Bands calculation.

    Attributes:
        upper_band: Upper band (SMA + std_dev * multiplier)
        middle_band: Middle band (Simple Moving Average)
        lower_band: Lower band (SMA - std_dev * multiplier)
        bandwidth: Band width as ratio ((upper - lower) / middle).
            Typical ranges for crypto assets (1h candles, 20-period, 2 std dev):
                - Squeeze: < 0.02 (low volatility, breakout likely)
                - Normal:  0.02 - 0.06 (typical trading range)
                - Expansion: > 0.06 (high volatility, trend in progress)
            These are starting guidelines; actual thresholds vary by asset and
            timeframe. Shorter timeframes and lower-cap assets trend higher.
        percent_b: Price position relative to bands (0 = lower, 1 = upper).
            Values above 1.0 indicate price above the upper band; values below
            0.0 indicate price below the lower band.
    """

    upper_band: float
    middle_band: float
    lower_band: float
    bandwidth: float
    percent_b: float

    def to_dict(self) -> dict[str, float]:
        """Convert to dictionary format."""
        return {
            "upper_band": self.upper_band,
            "middle_band": self.middle_band,
            "lower_band": self.lower_band,
            "bandwidth": self.bandwidth,
            "percent_b": self.percent_b,
        }


@dataclass(frozen=True)
class MACDResult:
    """Result from MACD calculation.

    Attributes:
        macd_line: MACD line (fast EMA - slow EMA)
        signal_line: Signal line (EMA of MACD line)
        histogram: MACD histogram (MACD line - signal line)
    """

    macd_line: float
    signal_line: float
    histogram: float

    def to_dict(self) -> dict[str, float]:
        """Convert to dictionary format."""
        return {
            "macd_line": self.macd_line,
            "signal_line": self.signal_line,
            "histogram": self.histogram,
        }


@dataclass(frozen=True)
class StochasticResult:
    """Result from Stochastic Oscillator calculation.

    Attributes:
        k_value: %K (fast stochastic) - current position in price range
        d_value: %D (slow stochastic) - SMA of %K
    """

    k_value: float
    d_value: float

    def to_dict(self) -> dict[str, float]:
        """Convert to dictionary format."""
        return {
            "k_value": self.k_value,
            "d_value": self.d_value,
        }


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "BaseIndicator",
    "BollingerBandsResult",
    "MACDResult",
    "StochasticResult",
]
