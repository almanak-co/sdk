"""
Aerodrome Trend-Following LP Strategy.

An Aerodrome volatile pool strategy that exits LP when trend reverses.
Uses EMA(9) and EMA(21) crossovers to determine entry/exit points.
"""

from .strategy import AeroTrendFollowerConfig, AeroTrendFollowerStrategy

__all__ = ["AeroTrendFollowerConfig", "AeroTrendFollowerStrategy"]
