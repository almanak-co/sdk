"""Aerodrome LP Sweep Demo Strategy.

An Aerodrome LP strategy on Base designed for parameter sweep backtesting.
Configurable RSI thresholds, LP sizing, and reentry cooldown provide multiple
sweep dimensions for grid search optimization.
"""

from .strategy import AerodromeSweepLPStrategy

__all__ = ["AerodromeSweepLPStrategy"]
