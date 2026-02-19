"""Momentum Accumulation Strategy.

RSI-based dip buying with wide-range concentrated LP for token accumulation.
Never sells the target token - pure accumulation.
"""

from .strategy import MomentumAccumulation

__all__ = ["MomentumAccumulation"]
