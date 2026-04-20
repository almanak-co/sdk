"""Aave V3 Paper Trading Lending Demo Strategy.

A lending strategy designed to exercise the paper trading engine with
supply/borrow/repay intents on Aave V3 (Arbitrum). Demonstrates multi-tick
lending decisions with configurable parameters for sweep optimization.
"""

from .strategy import AavePaperLendingStrategy

__all__ = ["AavePaperLendingStrategy"]
