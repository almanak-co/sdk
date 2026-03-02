"""Anvil fork management infrastructure.

This package provides Anvil fork lifecycle management used by:
- ``almanak strat run --network anvil`` (live strategy runs on forks)
- Paper trading (``almanak strat backtest paper``)
- Intent tests and integration tests

Moved from ``almanak.framework.backtesting.paper.fork_manager`` to
correctly reflect that Anvil fork management is infrastructure, not
a backtesting concern.
"""

from almanak.framework.anvil.fork_manager import (
    CHAIN_IDS,
    KNOWN_BALANCE_SLOTS,
    TOKEN_ADDRESSES,
    TOKEN_DECIMALS,
    ForkManagerConfig,
    RollingForkManager,
)

__all__ = [
    "RollingForkManager",
    "ForkManagerConfig",
    "CHAIN_IDS",
    "TOKEN_ADDRESSES",
    "TOKEN_DECIMALS",
    "KNOWN_BALANCE_SLOTS",
]
