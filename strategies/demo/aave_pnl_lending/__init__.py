"""Aave V3 PnL Lending Demo Strategy.

A lending strategy designed to exercise the PnL backtester with
supply/borrow intents on Aave V3. Demonstrates multi-tick lending
decisions based on price movement and utilization thresholds.
"""

from .strategy import AavePnLLendingStrategy

__all__ = ["AavePnLLendingStrategy"]
