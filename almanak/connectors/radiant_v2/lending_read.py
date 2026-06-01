"""Radiant V2 single-reserve lending-read capability.

Radiant V2 is an Aave V2 fork: it exposes the identical
``PoolDataProvider.getUserReserveData(address asset, address user)`` ABI against
its own ``pool_data_provider`` contract. It therefore reuses the shared
:data:`~almanak.connectors._strategy_base.lending_read_base.AAVE_FORK_RESERVE_READ`
spec; only the per-chain data-provider address (owned by this connector's
``addresses.py`` — Ethereum only) differs from Aave's.

Publishing :data:`LENDING_READ_SPEC` here is this connector's opt-in to the
strategy-side
:class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`,
so the framework lending reader can reprice Radiant V2 positions without naming
the protocol or its contract kind.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.lending_read_base import (
    AAVE_FORK_RESERVE_READ,
    LendingReadSpec,
)

#: Single-reserve read capability the registry dispatches for ``radiant_v2``.
LENDING_READ_SPEC: LendingReadSpec = AAVE_FORK_RESERVE_READ

__all__ = ["LENDING_READ_SPEC"]
