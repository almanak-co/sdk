"""Enso RSI Demo Strategy - Tutorial for RSI trading via Enso aggregator.

This module demonstrates how to use the Enso DEX aggregator for swaps.

Usage:
    python -m src.cli.run --strategy demo_enso_rsi --once --dry-run
    python strategies/demo/enso_rsi/run_anvil.py
"""

from .strategy import EnsoRSIStrategy

__all__ = ["EnsoRSIStrategy"]
