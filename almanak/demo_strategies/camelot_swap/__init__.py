"""Camelot Swap Demo Strategy.

Demonstrates a BUY + SELL swap lifecycle on Arbitrum via Camelot
(Algebra V3). The Camelot connector is SWAP-only.
"""

from .strategy import CamelotSwapStrategy

__all__ = ["CamelotSwapStrategy"]
