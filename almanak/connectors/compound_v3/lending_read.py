"""Compound V3 lending-read capability (aggregate account-state).

Publishes this connector's account-state read spec so the strategy-side
:class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`
can let the framework lending reader query Compound V3 state without the framework
hardcoding Compound's Comet selection, function selectors, or HF math:

* :data:`ACCOUNT_STATE_READ_SPEC` — aggregate account-state read (VIB-4929 PR-3b):
  collateral USD / debt USD / health factor. Compound V3 reads per-market from a
  per-*market* Comet (``balanceOf`` for base-asset supply, otherwise
  ``userCollateral`` + ``borrowBalanceOf``); the shared
  :data:`~almanak.connectors._strategy_base.lending_read_base.COMPOUND_V3_ACCOUNT_STATE_READ`
  spec describes that ABI and decodes it.

Unlike the Aave family, Compound V3 is **not USD-native** and its read target is
per-market (not a single per-chain contract). The spec therefore declares empty
``contract_kinds`` (market-scoped target, bound by the registry from the
``COMPOUND_V3_ACCOUNT_STATE_MARKETS`` table's ``comet_address``),
``normalize_market_id=str.lower`` (Compound market ids are base-asset symbols),
and a ``query_inputs_fn`` for the intent-derived collateral token. It stays pure
(no gateway, no oracle); the framework reader owns price resolution + the gateway
round-trip.

Compound V3 publishes no single-reserve ``LENDING_READ_SPEC``: its account state is
read per-market via ``userCollateral`` / ``balanceOf`` / ``borrowBalanceOf``, not an
Aave-style ``getUserReserveData(asset, user)`` reserve read.
"""

from __future__ import annotations

from almanak.connectors._strategy_base.lending_read_base import (
    COMPOUND_V3_ACCOUNT_STATE_READ,
    AccountStateReadSpec,
)

#: Aggregate account-state read capability the registry dispatches for ``compound_v3``.
ACCOUNT_STATE_READ_SPEC: AccountStateReadSpec = COMPOUND_V3_ACCOUNT_STATE_READ

__all__ = ["ACCOUNT_STATE_READ_SPEC"]
