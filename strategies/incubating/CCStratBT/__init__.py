"""Triple Signal Momentum Strategy (CCStratBT).

A multi-indicator TA strategy combining RSI, MACD, and Bollinger Bands
for higher-conviction swing trades on WETH/USDC via Enso aggregator.
"""

from .strategy import TripleSignalStrategy

__all__ = ["TripleSignalStrategy"]
