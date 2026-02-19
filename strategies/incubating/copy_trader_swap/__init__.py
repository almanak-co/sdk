"""Copy Trader Swap Strategy.

Monitors leader wallets on-chain and replicates their swap trades
using configurable sizing modes and risk caps.

See: config.json for configuration options.
"""

from .strategy import CopyTraderSwapStrategy

__all__ = [
    "CopyTraderSwapStrategy",
]
