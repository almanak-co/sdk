"""Morpho Blue Paper Trade Demo Strategy.

A lending strategy designed to exercise the paper trading engine with
Morpho Blue supply/borrow intents on Ethereum. Demonstrates multi-tick
lending decisions for paper trading PnL tracking.
"""

from .strategy import MorphoPaperTradeStrategy

__all__ = ["MorphoPaperTradeStrategy"]
