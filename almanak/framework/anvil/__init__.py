"""Fork management infrastructure for local testing.

This package provides fork lifecycle management used by:
- ``almanak strat run --network anvil`` (live strategy runs on forks)
- Paper trading (``almanak strat backtest paper``)
- Intent tests and integration tests

Supports both EVM (Anvil) and Solana (solana-test-validator) chains.
"""

from almanak.framework.anvil.fork_manager import (
    CHAIN_IDS,
    KNOWN_BALANCE_SLOTS,
    TOKEN_ADDRESSES,
    TOKEN_DECIMALS,
    ForkManagerConfig,
    RollingForkManager,
)
from almanak.framework.anvil.solana_fork_manager import (
    SOLANA_TOKEN_DECIMALS,
    SOLANA_TOKEN_MINTS,
    SolanaForkManager,
)

__all__ = [
    "RollingForkManager",
    "ForkManagerConfig",
    "SolanaForkManager",
    "CHAIN_IDS",
    "TOKEN_ADDRESSES",
    "TOKEN_DECIMALS",
    "KNOWN_BALANCE_SLOTS",
    "SOLANA_TOKEN_MINTS",
    "SOLANA_TOKEN_DECIMALS",
]
