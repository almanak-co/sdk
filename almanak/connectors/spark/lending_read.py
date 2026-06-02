"""Spark lending-read capabilities (single-reserve + aggregate account-state).

Spark is an Aave V3 fork: it exposes the identical
``PoolDataProvider.getUserReserveData(address asset, address user)`` and
``Pool.getUserAccountData(address user)`` / ``Pool.getUserEMode(address user)``
ABIs against its own ``pool_data_provider`` / ``pool`` contracts. It therefore
reuses the shared
:data:`~almanak.connectors._strategy_base.lending_read_base.AAVE_FORK_RESERVE_READ`
and
:data:`~almanak.connectors._strategy_base.lending_read_base.AAVE_FORK_ACCOUNT_STATE_READ`
specs; only the per-chain addresses (owned by this connector's ``addresses.py``)
differ from Aave's.

Publishing :data:`LENDING_READ_SPEC` and :data:`ACCOUNT_STATE_READ_SPEC` here is
this connector's opt-in to the strategy-side
:class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`,
so the framework lending reader can reprice Spark positions without naming the
protocol or its contract kinds.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.lending_read_base import (
    AAVE_FORK_ACCOUNT_STATE_READ,
    AAVE_FORK_RESERVE_READ,
    AccountStateReadSpec,
    LendingReadSpec,
)

#: Single-reserve read capability the registry dispatches for ``spark``.
LENDING_READ_SPEC: LendingReadSpec = AAVE_FORK_RESERVE_READ

#: Aggregate account-state read capability the registry dispatches for ``spark``.
ACCOUNT_STATE_READ_SPEC: AccountStateReadSpec = AAVE_FORK_ACCOUNT_STATE_READ

__all__ = ["ACCOUNT_STATE_READ_SPEC", "LENDING_READ_SPEC"]
