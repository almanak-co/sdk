"""Enso RSI Demo Strategy - Tutorial for RSI trading via Enso aggregator.

This module demonstrates how to use the Enso DEX aggregator for swaps.

Usage:
    almanak strat run -d enso_rsi --once --dry-run
    almanak strat run -d enso_rsi --network anvil --once
"""

from .strategy import EnsoRSIStrategy

__all__ = ["EnsoRSIStrategy"]
