"""Gas-cost USD conversion helpers (VIB-3658 sequel — April 30 audit item #3).

The bug this module exists to close
-----------------------------------
``transaction_ledger.gas_usd`` (and the per-strategy sidecar's ``gas_usd``
field) were always ``""`` / ``None`` for SWAP and LP intents.  The runner
already had the data needed to compute it (``gas_used`` + ``gas_price`` ×
native-token USD price); the only missing piece was a writer that put the
three together.  Lending already did this correctly via
``lending_accounting._amount_to_usd`` (line 916 of that module).

This helper hoists that pattern into a single module so every category
(SWAP, LP, lending, perps, vault, prediction, …) shares the same
implementation and the same chain → native-token map.

Why a dedicated module
----------------------
Three call sites needed it:

  1. ``observability.ledger._extract_tx_and_gas`` — feeds ``transaction_ledger.gas_usd``.
  2. ``accounting.sidecar.AccountingSidecarWriter`` — feeds the per-strategy
     ``.jsonl`` consumed by the dashboard.
  3. Future event builders (LP / swap / perp / vault) — when they grow a
     ``gas_usd`` field on the typed event the same helper is the right path.

Inlining the conversion in any one of those modules would diverge the
chain map and the price-lookup precedence; centralising it here keeps a
single source of truth.

Gateway boundary
----------------
This module performs **no** network egress.  It receives the price oracle
dict (already populated upstream by ``MarketSnapshot`` / ``GatewayPriceOracle``)
and does pure arithmetic on it.  See ``CLAUDE.md`` →
"Gateway boundary: no direct network calls, no bypasses".

Decimal precision
-----------------
``gas_used`` is uint256 wei.  The conversion is::

    gas_usd = Decimal(gas_used) * Decimal(gas_price_wei) / Decimal(10**18) * native_usd

We never use ``float`` — gas costs on L1 ETH at ~50 gwei × 300k gas reach
the 7-significant-digit floor of double precision quickly, and a $0.000001
rounding error on a $10M-AUM strategy compounds visibly in PnL.

If the oracle cannot resolve the native token's price the function returns
``None`` (NOT ``Decimal(0)``).  ``None`` means *unknown*; ``Decimal(0)``
would mean *measured zero* and would silently corrupt downstream gas-cost
totals in PnL reports.  A single WARN is emitted at the call site (not
here — we want one log per ledger write, not one per helper invocation).
"""

from __future__ import annotations

import logging
from decimal import Decimal, InvalidOperation
from typing import Any

logger = logging.getLogger(__name__)

# ─── Chain native gas token ───────────────────────────────────────────────────
# Native-gas-token symbols are owned by :class:`ChainRegistry`
# (``ChainDescriptor.native.symbol``). VIB-4933 deleted the framework-side
# ``_CHAIN_NATIVE_TOKEN`` literal that previously duplicated those symbols and
# drifted from the registry (e.g. plasma="ETH" in the old lending map vs "XPL"
# here — VIB-3805). ``native_token_for_chain`` now reads the registry directly,
# mirroring ``almanak.core.chains._helpers.receipt_timeout_for`` and the gateway
# precedent ``web3_provider.NATIVE_TOKEN_SYMBOLS`` (also a registry-derived view).
#
# The registry covers both EVM and non-EVM chains (Solana), so the historical
# "framework map is a superset of the gateway map" relationship is automatic:
# both views are now projections of the same ``ChainRegistry.all()``.
# Tests: ``tests/unit/framework/accounting/test_gas_pricing.py`` covers this side;
# ``tests/gateway`` covers the gateway side.


def native_token_for_chain(chain: str) -> str:
    """Return the native gas token symbol for *chain*.

    Reads ``ChainDescriptor.native.symbol`` from :class:`ChainRegistry`
    (single source of truth). Defaults to ``"ETH"`` for unknown / empty
    chains — matches the gateway-side fallback in ``GatewayBalanceProvider``
    and the lending event builder. Logs at DEBUG (not WARN) so unknown-chain
    noise doesn't drown out real misconfigurations; the missing-price WARN at
    the call site is the place an operator would notice.

    Aliases (``bnb`` -> ``bsc``, ``avax`` -> ``avalanche``, ``eth`` -> ``ethereum``,
    etc.) are resolved by ``ChainRegistry.try_resolve``, which lowercases and
    strips its input and matches both canonical names and aliases. Without this
    normalization, BSC strategies that pass ``chain='bnb'`` would silently fall
    back to ETH and misprice gas.
    """
    if not chain:
        return "ETH"
    from almanak.core.chains import ChainRegistry

    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None:
        logger.debug("gas_pricing: chain=%s not in ChainRegistry; defaulting to ETH", chain)
        return "ETH"
    return descriptor.native.symbol


def _lookup_price(price_oracle: dict[str, Any] | None, symbol: str) -> Decimal | None:
    """Return the USD price of *symbol* from *price_oracle*, or None.

    Tries upper-case (canonical), then exact, then lower-case — matches the
    precedence used by ``lending_accounting._amount_to_usd`` so we stay
    consistent across the codebase.  ``None`` is returned for any failure
    (missing oracle, missing key, unparseable value).

    Accepts both shapes the runner emits today:
      * Flat ``{symbol: price}`` (legacy `MarketSnapshot.get_price_oracle_dict()`).
      * Nested ``{symbol: {"price_usd": ..., "oracle_source": ..., ...}}`` —
        the AttemptNo17 G12 shape produced by
        ``_portfolio_snapshot_to_price_oracle`` for the teardown lane.
    Without the nested branch, teardown ledger rows lost ``gas_usd`` because
    `compute_gas_usd` couldn't parse the oracle's nested ``price_usd`` field.
    """
    if not price_oracle:
        return None
    candidate = price_oracle.get(symbol.upper())
    if candidate is None:
        candidate = price_oracle.get(symbol)
    if candidate is None:
        candidate = price_oracle.get(symbol.lower())
    if candidate is None:
        return None
    if isinstance(candidate, dict):
        candidate = candidate.get("price_usd")
        if candidate is None:
            return None
    try:
        price = Decimal(str(candidate))
    except (InvalidOperation, ValueError, TypeError):
        return None
    if not price.is_finite():
        # ``Decimal("NaN")`` raises ``InvalidOperation`` on the ``<= 0``
        # comparison below; ``Decimal("Infinity")`` parses cleanly and
        # passes ``> 0`` but is never a legitimate USD price.  Reject
        # non-finite values up front so the rest of the helper can
        # treat ``price`` as a real number.
        return None
    if price <= 0:
        # A zero or negative price is never legitimate for a native gas token.
        # Treat as unavailable rather than fabricating a $0 gas cost.
        return None
    return price


def compute_gas_usd(
    *,
    gas_cost_wei: int | None,
    chain: str,
    price_oracle: dict[str, Any] | None,
) -> Decimal | None:
    """Convert raw wei-gas-cost to USD using the chain's native-token USD price.

    Parameters
    ----------
    gas_cost_wei:
        Total gas cost in wei (``gas_used * gas_price``, summed across all
        transactions in the bundle).  Three input shapes have distinct
        semantics — see Returns.
    chain:
        Chain name (case-insensitive).  Resolved against
        :class:`ChainRegistry` (``ChainDescriptor.native.symbol``) after
        alias normalization.
    price_oracle:
        ``MarketSnapshot.get_price_oracle_dict()`` output — a flat
        ``{symbol: price}`` dict.

    Returns
    -------
    Decimal | None
        - ``None`` when the value is genuinely unknown.  Triggered by:
          (a) ``gas_cost_wei is None`` (caller didn't measure it);
          (b) chain canonicalises to ``"solana"`` (lamports vs wei unit
              mismatch — see the non-EVM short-circuit below);
          (c) ``gas_cost_wei < 0`` (structurally impossible);
          (d) ``price_oracle`` missing the native token's price OR the
              parsed price is non-finite / non-positive.
        - ``Decimal("0")`` when ``gas_cost_wei == 0`` on an EVM chain —
          a measured zero (e.g. dry-run, paper trade).  This is the only
          path that returns a measured-zero; the Solana 0-wei case is
          captured by branch (b) above as "unknown" instead.
        - ``Decimal(...)`` (the actual USD figure) when all four of
          ``gas_cost_wei > 0``, EVM chain, finite positive native price,
          and a successful arithmetic conversion.
        Never ``float`` — Decimal precision is load-bearing for
        accumulated gas figures on long-running strategies.

    Why ``None`` and not ``Decimal(0)`` on missing price
    ----------------------------------------------------
    PnL totals SUM gas_usd across every trade.  If an oracle hiccup
    silently substituted ``0`` we would underreport drag and quietly
    overstate net PnL.  The lending lane already ships ``None`` in this
    case (see ``_amount_to_usd``); this helper preserves that contract.
    """
    if gas_cost_wei is None:
        # Still propagate the measured-zero convention: an unmeasured cost is
        # explicitly "unknown".  callers expecting "0 wei" pass 0, not None.
        return None

    # Non-EVM short-circuit (Solana / future lamport-denominated chains).
    # ``gas_cost_wei`` assumes the EVM wei unit (10**18 / native).  Solana's
    # adapter stores fees in ``fee_lamports`` on the result object and leaves
    # ``total_gas_cost_wei`` at its default ``0``, so the value reaching this
    # helper today is always 0; even if a future code path piped lamports
    # through ``total_gas_cost_wei``, dividing by ``10**18`` would understate
    # the figure by ~10**9.  Return ``None`` (unknown) on Solana — the
    # honest contract until a lamport-aware sibling helper exists.  This
    # preempts both Gemini's concern (silent 10**9 underreport) and Codex's
    # concern (Decimal("0") on 0-wei looking like a measured zero).
    try:
        from almanak.core.constants import resolve_chain_name

        canonical_chain = resolve_chain_name(chain) if chain else ""
    except Exception:  # noqa: BLE001
        canonical_chain = (chain or "").lower()
    # VIB-4803: route SVM chains through the family adapter — they have no
    # wei/gwei accounting (lamports instead), so return None to indicate
    # "no comparable gas-in-USD figure available".
    from almanak.framework.chain_family import SvmFamily, family_for

    if isinstance(family_for(canonical_chain), SvmFamily):
        return None

    if gas_cost_wei == 0:
        return Decimal("0")
    if gas_cost_wei < 0:
        # Defensive: a negative gas figure is structurally impossible.
        # Treat as unknown rather than producing a negative USD value that
        # would silently subtract from PnL drag.
        logger.warning("gas_pricing: refusing to convert negative gas_cost_wei=%s", gas_cost_wei)
        return None

    native_symbol = native_token_for_chain(chain)
    native_price = _lookup_price(price_oracle, native_symbol)
    if native_price is None:
        return None

    try:
        gas_native = Decimal(str(gas_cost_wei)) / Decimal(10**18)
        return gas_native * native_price
    except (InvalidOperation, ValueError, ArithmeticError):
        # Defensive — Decimal arithmetic on legitimate inputs cannot fail,
        # but we prefer "None + log" over a crash on the hot path.
        logger.warning(
            "gas_pricing: arithmetic failure converting gas_cost_wei=%s on chain=%s",
            gas_cost_wei,
            chain,
            exc_info=True,
        )
        return None
