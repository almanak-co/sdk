"""Technical Indicators Module.

This package provides technical indicators for market analysis,
including RSI, Bollinger Bands, MACD, moving averages, and other common indicators.

Architecture:
    - BaseIndicator: Protocol for all indicators (duck typing)
    - IndicatorRegistry: Registry for indicator discovery and factory creation
    - Individual calculators: RSI, Bollinger Bands, MACD, Stochastic, ATR, Moving Averages

Key Components:
    - RSICalculator: Relative Strength Index using Wilder's smoothing
    - BollingerBandsCalculator: Bollinger Bands with configurable std deviation
    - MACDCalculator: Moving Average Convergence Divergence
    - StochasticCalculator: Stochastic Oscillator (%K and %D)
    - ATRCalculator: Average True Range
    - MovingAverageCalculator: SMA, EMA, WMA

Example:
    from almanak.framework.data.indicators import (
        RSICalculator,
        BollingerBandsCalculator,
        CoinGeckoOHLCVProvider,
        IndicatorRegistry,
    )

    # Create OHLCV provider
    ohlcv_provider = CoinGeckoOHLCVProvider()

    # Create RSI calculator with configurable timeframe
    rsi_calc = RSICalculator(ohlcv_provider=ohlcv_provider)
    rsi = await rsi_calc.calculate_rsi("WETH", period=14, timeframe="1h")
    print(f"RSI: {rsi}")  # 0-100 scale

    # Use registry for discovery
    for name in IndicatorRegistry.list_all():
        print(f"Available: {name}")
"""

from .atr import ATRCalculator
from .base import (
    BaseIndicator,
    BollingerBandsResult,
    MACDResult,
    StochasticResult,
)
from .bollinger_bands import BollingerBandsCalculator
from .macd import MACDCalculator
from .moving_averages import MovingAverageCalculator
from .registry import IndicatorRegistry
from .rsi import (
    CoinGeckoOHLCVProvider,
    OHLCVData,
    RSICalculator,
)
from .stochastic import StochasticCalculator

__all__ = [
    # Base protocol and result types
    "BaseIndicator",
    "BollingerBandsResult",
    "MACDResult",
    "StochasticResult",
    # Registry
    "IndicatorRegistry",
    # RSI
    "RSICalculator",
    "CoinGeckoOHLCVProvider",
    "OHLCVData",
    # Moving Averages
    "MovingAverageCalculator",
    # Bollinger Bands
    "BollingerBandsCalculator",
    # MACD
    "MACDCalculator",
    # Stochastic
    "StochasticCalculator",
    # ATR
    "ATRCalculator",
]
