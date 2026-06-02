"""Morpho Blue lending-read capability (aggregate account-state).

Publishes this connector's account-state read spec so the strategy-side
:class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`
can let the framework lending reader query Morpho state without the framework
hardcoding Morpho's contract kind or function selectors:

* :data:`ACCOUNT_STATE_READ_SPEC` — aggregate account-state read (VIB-4929
  PR-3a): total collateral / total debt / health factor / lltv. Morpho Blue
  reads the per-user ``position(id, user)`` and per-market ``market(id)`` from
  its single per-chain singleton (contract kind ``morpho``); the shared
  :data:`~almanak.connectors._strategy_base.lending_read_base.MORPHO_BLUE_ACCOUNT_STATE_READ`
  spec describes that ABI and decodes it.

Unlike the Aave family, Morpho Blue is **not USD-native** — its on-chain reads
return raw token amounts, so the spec values the position from the
price/decimals/market-params seam injected onto the
:class:`~almanak.connectors._strategy_base.lending_read_base.AccountStateQuery`
by the framework consumer. The spec stays pure (no gateway, no oracle); the
framework reader owns the price resolution + gateway round-trip.

Morpho Blue publishes no single-reserve ``LENDING_READ_SPEC``: its account state
is read per-market through ``position`` / ``market``, not via an Aave-style
``getUserReserveData(asset, user)`` reserve read.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.lending_read_base import (
    MORPHO_BLUE_ACCOUNT_STATE_READ,
    AccountStateReadSpec,
)

#: Aggregate account-state read capability the registry dispatches for ``morpho_blue``.
ACCOUNT_STATE_READ_SPEC: AccountStateReadSpec = MORPHO_BLUE_ACCOUNT_STATE_READ

__all__ = ["ACCOUNT_STATE_READ_SPEC"]
