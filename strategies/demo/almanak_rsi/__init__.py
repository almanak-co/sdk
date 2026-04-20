"""ALMANAK RSI Demo Strategy.

An RSI mean reversion strategy for ALMANAK/USDC on Uniswap V3 (Base chain).
Uses CoinGecko DEX (GeckoTerminal) OHLCV data for RSI calculation.
"""

from .strategy import AlmanakRSIStrategy

__all__ = ["AlmanakRSIStrategy"]
