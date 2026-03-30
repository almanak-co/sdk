"""TraderJoe V2 LP Crisis Scenario Backtest Strategy.

Stress-tests TraderJoe V2 LP range rebalancing under historical crisis
conditions (Black Thursday, Terra Collapse, FTX Collapse) on Avalanche.
"""

from .strategy import TraderJoeCrisisLPStrategy

__all__ = ["TraderJoeCrisisLPStrategy"]
