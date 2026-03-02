"""DEPRECATED: Fork manager has moved to ``almanak.framework.anvil.fork_manager``.

This module re-exports all symbols for backward compatibility. Update your
imports to use ``almanak.framework.anvil`` instead::

    # Old (deprecated)
    from almanak.framework.backtesting.paper.fork_manager import RollingForkManager

    # New (preferred)
    from almanak.framework.anvil import RollingForkManager
"""

import warnings

warnings.warn(
    "almanak.framework.backtesting.paper.fork_manager is deprecated. Use almanak.framework.anvil.fork_manager instead.",
    DeprecationWarning,
    stacklevel=2,
)

from almanak.framework.anvil.fork_manager import (  # noqa: E402, F401
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
