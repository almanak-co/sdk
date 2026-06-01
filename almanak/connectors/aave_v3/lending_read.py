"""Aave V3 single-reserve lending-read capability.

Publishes this connector's
:class:`~almanak.connectors._strategy_base.lending_read_base.LendingReadSpec`
so the strategy-side
:class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`
can let the framework lending reader query a wallet's on-chain supply/debt for a
single reserve without the framework hardcoding Aave's ``pool_data_provider``
contract kind or the ``getUserReserveData`` selector.

Aave V3 reads per-user reserve state from
``PoolDataProvider.getUserReserveData(address asset, address user)``; the shared
:data:`~almanak.connectors._strategy_base.lending_read_base.AAVE_FORK_RESERVE_READ`
spec describes that ABI (the same one Spark and Radiant V2 expose).
"""

from __future__ import annotations

from almanak.connectors._strategy_base.lending_read_base import (
    AAVE_FORK_RESERVE_READ,
    LendingReadSpec,
)

#: Single-reserve read capability the registry dispatches for ``aave_v3``.
LENDING_READ_SPEC: LendingReadSpec = AAVE_FORK_RESERVE_READ

__all__ = ["LENDING_READ_SPEC"]
