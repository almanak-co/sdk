"""LiFi Smart DCA Strategy - RSI-based dollar-cost averaging via LiFi aggregator.

Uses LiFi for cross-chain-capable swap routing with RSI-based entry signals.

Usage:
    almanak strat run -d strategies/incubating/lifi_smart_dca --network anvil --once
"""

from .strategy import LiFiSmartDCAStrategy

__all__ = ["LiFiSmartDCAStrategy"]
