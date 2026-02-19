"""TraderJoe V2 Wide-Range Accumulator Strategy.

A TraderJoe V2 LP strategy with wide 15% range for JOE/AVAX accumulation.
Uses hybrid rebalancing based on both time (7 days) and price movement (7%).
"""

from .strategy import TJWideAccumulatorStrategy

__all__ = ["TJWideAccumulatorStrategy"]
