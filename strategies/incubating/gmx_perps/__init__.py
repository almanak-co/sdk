"""GMX Perpetuals Demo Strategy - Tutorial for perpetual futures trading.

This module demonstrates how to trade perpetual futures on GMX V2.

Usage:
    python -m src.cli.run --strategy demo_gmx_perps --once --dry-run
    python strategies/demo/gmx_perps/run_anvil.py
"""

from .strategy import GMXPerpsStrategy

__all__ = ["GMXPerpsStrategy"]
