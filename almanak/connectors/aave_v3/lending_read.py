"""Aave V3 lending-read capabilities (single-reserve + aggregate account-state).

Publishes this connector's read specs so the strategy-side
:class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`
can let the framework lending reader query Aave state without the framework
hardcoding Aave's contract kinds or function selectors:

* :data:`LENDING_READ_SPEC` — single-reserve supply/debt read. Aave V3 reads
  per-user reserve state from
  ``PoolDataProvider.getUserReserveData(address asset, address user)``; the
  shared
  :data:`~almanak.connectors._strategy_base.lending_read_base.AAVE_FORK_RESERVE_READ`
  spec describes that ABI (the same one Spark exposes).
* :data:`ACCOUNT_STATE_READ_SPEC` — aggregate account-state read (VIB-4929):
  total collateral / total debt / health factor / liquidation threshold /
  e-mode category. Aave V3 reads these from
  ``Pool.getUserAccountData(address user)`` (+ ``Pool.getUserEMode(address user)``
  for the e-mode category); the shared
  :data:`~almanak.connectors._strategy_base.lending_read_base.AAVE_FORK_ACCOUNT_STATE_READ`
  spec describes that ABI. The values are USD-denominated on-chain by the Aave
  oracle, so this read needs no external price injection.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.lending_read_base import (
    AAVE_FORK_ACCOUNT_STATE_READ,
    AAVE_FORK_RESERVE_READ,
    AccountStateReadSpec,
    LendingReadSpec,
)

#: Single-reserve read capability the registry dispatches for ``aave_v3``.
LENDING_READ_SPEC: LendingReadSpec = AAVE_FORK_RESERVE_READ

#: Aggregate account-state read capability the registry dispatches for ``aave_v3``.
ACCOUNT_STATE_READ_SPEC: AccountStateReadSpec = AAVE_FORK_ACCOUNT_STATE_READ

__all__ = ["ACCOUNT_STATE_READ_SPEC", "LENDING_READ_SPEC"]
