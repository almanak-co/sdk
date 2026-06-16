"""Ambient inventory revaluation lane — the G6 component term for untraded inventory.

Blueprint: ``docs/internal/blueprints/27-accounting.md`` §11.5.

Why this exists
---------------
G6 (the wallet ≡ component reconciliation) sums *typed* component PnL — SWAP /
LP / perp realized PnL, fees, funding, interest, minus gas. Every one of those
terms attributes to a token the strategy **traded**. But the wallet method
(``equity_final − equity_initial``) also moves when a token the strategy is
merely *holding* changes price between the two endpoint snapshots — the price of
idle WETH left in the wallet, the native-gas token's remaining balance, the
unspent half of a single-sided swap. None of those land in a typed bucket, so
the component sum is structurally short by their mark-to-market and G6 reports a
spurious gap.

This module computes that missing term — the **ambient inventory revaluation** —
as ``Σ qty_idle × (mark_final − mark_initial)`` over every token that is NOT
already accounted for by a component term, plus the open swap-lot
mark-to-market ``Σ (remaining × mark_final − basis_remaining)`` for the lots the
FIFO store still holds. Together these two pieces value *all* untraded inventory
exactly once.

The crux: tracked vs ambient
----------------------------
Every token present in either endpoint snapshot's wallet is partitioned into:

* **tracked** — its value delta is already inside a component term. Two sources:
  (a) the canonical token symbols named by *any* accounting-event payload
  (``token_in`` / ``token_out`` / ``token0`` / ``token1`` / ``asset``) — a
  swap/LP/lending leg whose realized PnL the component sum already carries; and
  (b) the tokens of the open swap lots a freshly-replayed ``FIFOBasisStore``
  holds — those get the explicit open-lot MTM term here, not the ambient term.
* **ambient** — everything else. Its ``qty × Δmark`` is the term G6 was missing.

Double-count guard
------------------
A token can be *partially* traded: 30 of 100 WETH consumed by a swap lot, 70
sitting idle. The lot's 30 are valued by the open-lot MTM term; the idle 70 by
``qty_idle × Δmark`` where ``qty_idle = wallet_balance − Σ open_lot_remaining``
floored at zero. The two pieces sum to ``wallet_balance × Δmark`` — the held
quantity is counted exactly once. A token named by an accounting payload (a
fully-traded leg) is excluded from the ambient term entirely; its delta is in
the typed bucket.

Native gas is **not** a special case: the native token's wallet row carries a
``qty`` and a ``Δmark`` just like any other ambient token, so the general rule
captures its revaluation. Gas *spent* stays in the component ``−Σgas`` term and
never enters here (it is a realized cost, not held inventory).

Empty ≠ Zero
------------
A persisted ``price_usd == "0"`` is a *measured* zero and contributes ``0`` to
the term. An absent / empty / unparseable mark for a token that has a non-zero
quantity at an endpoint makes the whole term **unmeasured**: ``total_usd`` is
``None`` and ``confidence == "unmeasured_price"``. The lane never silently
substitutes zero for a missing measurement — a ``None`` total propagates to G6
as a null-bucket FAIL with a diagnostic, never a silent pass.

Boundary
--------
Leaf module under ``accounting/``: imports only ``decimal`` / ``dataclasses`` /
the standard library and ``basis.py``. NO new on-chain reads and NO fresh price
reads — the marks are the exact prices the wallet (equity) method already used,
read off the same persisted snapshot rows.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from almanak.framework.accounting.basis import FIFOBasisStore

# Canonical token-SYMBOL-bearing payload fields, across every primitive's
# accounting-event payload (``payload_schemas.py``): SWAP carries
# ``token_in`` / ``token_out``; LP_OPEN / LP_CLOSE carry ``token0`` / ``token1``;
# SUPPLY / BORROW / WITHDRAW / REPAY carry ``asset``. Perp payloads carry a
# ``market`` *pair identifier* (e.g. "ARB-USDC"), NOT a single wallet token
# symbol, so it is intentionally absent — a held perp-collateral token left idle
# in the wallet is correctly *ambient*, while the realized perp PnL settled into
# that collateral is already in the ``Σ_perp`` component bucket.
#
# This is a field-NAME allowlist (generic across event types), not a token-
# symbol allowlist. No primitive branch, no hardcoded token symbol — adding a
# new primitive whose payload reuses these canonical fields is captured for
# free; a primitive that introduces a genuinely new token-symbol field name adds
# it here once, in one place.
_TOKEN_SYMBOL_PAYLOAD_FIELDS: tuple[str, ...] = (
    "token_in",
    "token_out",
    "token0",
    "token1",
    "asset",
)


@dataclass
class InventoryRevaluation:
    """The ambient-inventory revaluation term for one snapshot bracket.

    ``total_usd`` is the sum of the ambient ``qty_idle × Δmark`` term and the
    open swap-lot ``remaining × mark_final − basis_remaining`` term. It is
    ``None`` (unmeasured — Empty≠Zero) when any held token at either endpoint
    lacks a usable mark or carries an unmeasured balance, or when an open lot is
    missing its basis. ``per_token`` is a per-symbol audit breakdown of the
    *ambient* contributions (the open-lot term is summed under the synthetic
    ``"<open_swap_lots>"`` key). ``confidence`` is ``"measured"`` or one of
    ``"unmeasured_price"`` / ``"unmeasured_balance"`` / ``"unmeasured_basis"``.
    ``excluded_tokens`` lists the symbols partitioned out as tracked.
    """

    total_usd: Decimal | None
    per_token: dict[str, str] = field(default_factory=dict)
    confidence: str = "measured"
    excluded_tokens: list[str] = field(default_factory=list)


def _canonical(symbol: Any) -> str:
    """Case-insensitive canonical key for symbol matching.

    Mirrors the canonicalisation the FIFO store and the swap-symbol resolver use
    (``resolve_swap_token_symbol`` upper-cases EVM symbols); here we only need a
    stable, case-insensitive identity for set membership and netting, so a simple
    upper-case of the trimmed string is sufficient and dependency-free.
    """
    return str(symbol or "").strip().upper()


def _parse_dec(value: Any) -> Decimal | None:
    """Parse to a finite Decimal or return None (no zero-coercion — Empty≠Zero).

    ``None`` / ``""`` / unparseable / non-finite all return ``None`` (unmeasured).
    A literal ``"0"`` parses to ``Decimal("0")`` (a *measured* zero).
    """
    if value is None or value == "":
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:  # noqa: BLE001 — any malformed value is "unmeasured", not zero.
        return None
    return parsed if parsed.is_finite() else None


def _wallet_marks(snapshot: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Extract ``{canonical_symbol: {"balance": Decimal|None, "price": Decimal|None}}``.

    Reads the persisted ``wallet_balances_json`` array
    (``{symbol, balance, value_usd, address, price_usd}`` per the snapshot
    writer). The mark is the per-balance ``price_usd`` — the exact price the
    wallet (equity) method already valued this row with on the same snapshot.

    ``balance`` / ``price`` are ``None`` (unmeasured) when absent or unparseable;
    a literal ``"0"`` price is a measured zero (parses to ``Decimal("0")``).
    A row with no symbol is skipped (cannot be keyed). When a symbol appears more
    than once, balances accumulate and the first usable price wins (snapshots
    emit one row per symbol; this is defensive).
    """
    out: dict[str, dict[str, Any]] = {}
    if not snapshot:
        return out
    raw = snapshot.get("wallet_balances_json")
    balances = _loads_list(raw)
    for entry in balances:
        if not isinstance(entry, dict):
            continue
        sym = _canonical(entry.get("symbol"))
        if not sym:
            continue
        bal = _parse_dec(entry.get("balance"))
        price = _parse_dec(entry.get("price_usd"))
        slot = out.setdefault(sym, {"balance": None, "price": None})
        if bal is not None:
            slot["balance"] = (slot["balance"] or Decimal("0")) + bal
        # First usable price wins; "price_usd": null leaves it unmeasured.
        if slot["price"] is None and price is not None:
            slot["price"] = price
    return out


def _loads_list(raw: Any) -> list[Any]:
    """Best-effort decode of a persisted JSON-array column to a list."""
    if raw is None or raw == "":
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        import json

        try:
            decoded = json.loads(raw)
        except (ValueError, TypeError):
            return []
        return decoded if isinstance(decoded, list) else []
    return []


def _tracked_symbols(accounting_events: list[dict[str, Any]]) -> set[str]:
    """Canonical symbols named by ANY accounting-event payload's token fields.

    These tokens' value deltas are already inside a component term (their typed
    realized-PnL bucket), so they are partitioned OUT of the ambient term. The
    enumeration is generic over the canonical field-name allowlist — no per-
    primitive branch, no hardcoded token symbol.
    """
    tracked: set[str] = set()
    for ev in accounting_events:
        if not isinstance(ev, dict):
            continue
        payload = _event_payload(ev)
        if not payload:
            continue
        for fieldname in _TOKEN_SYMBOL_PAYLOAD_FIELDS:
            sym = _canonical(payload.get(fieldname))
            if sym:
                tracked.add(sym)
    return tracked


def _has_unattributable_trading_activity(accounting_events: list[dict[str, Any]]) -> bool:
    """True if any event is a SWAP — the only event class that produces open
    FIFO swap lots.

    Used solely by the empty-``deployment_id`` fail-closed branch: without a
    deployment scope we cannot replay the FIFO store, so a *partially*-traded
    token's idle remainder would be mis-valued as pure ambient. Only SWAP events
    create entries in ``FIFOBasisStore.iter_open_swap_lots`` — LP / lending /
    perp legs create no swap lots, so their presence alone does NOT make
    ambient-only inventory unmeasurable (an ambient-only window with, say, an
    LP_OPEN event still reconciles exactly). Mirrors the swap-lot reconstruction
    in ``FIFOBasisStore.reconstruct_from_events``, which keys on SWAP events.
    """
    for ev in accounting_events:
        if not isinstance(ev, dict):
            continue
        if str(ev.get("event_type") or "").upper() == "SWAP":
            return True
        if str(_event_payload(ev).get("event_type") or "").upper() == "SWAP":
            return True
    return False


def _event_payload(event: dict[str, Any]) -> dict[str, Any]:
    """Return an accounting-event row's payload dict.

    Tolerates both a pre-parsed ``payload`` dict (fixture / in-memory shape) and
    the persisted ``payload_json`` string column (DB shape). Never raises — a
    malformed payload yields ``{}`` (no tracked symbols contributed; the token,
    if held, then falls into the ambient term, which is the safe default).
    """
    payload = event.get("payload")
    if isinstance(payload, dict):
        return payload
    raw = event.get("payload_json")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw:
        import json

        try:
            decoded = json.loads(raw)
        except (ValueError, TypeError):
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _open_lot_remaining_by_token(store: FIFOBasisStore) -> dict[str, Decimal]:
    """Sum of ``remaining`` across open swap lots, keyed by canonical symbol.

    Used by the double-count guard: a partially-traded token's idle remainder is
    ``wallet_balance − Σ open_lot_remaining`` floored at zero.
    """
    by_token: dict[str, Decimal] = {}
    for _position_key, token, remaining, _cost in store.iter_open_swap_lots():
        sym = _canonical(token)
        if not sym:
            continue
        by_token[sym] = by_token.get(sym, Decimal("0")) + remaining
    return by_token


def _open_lot_basis_by_token(store: FIFOBasisStore) -> dict[str, Decimal | None]:
    """Sum of ``cost_for_remaining`` across open swap lots, keyed by canonical symbol.

    A token's basis is ``None`` (unmeasured — Empty≠Zero) when ANY of its open
    lots is missing its basis: a partial basis cannot be trusted to value the
    held quantity, so the whole token degrades to unmeasured and the caller
    surfaces ``unmeasured_basis``. Reuses VIB-4984's ``cost_for_remaining``
    (the lot's ``cost_usd`` pro-rated by ``remaining / amount``).
    """
    by_token: dict[str, Decimal | None] = {}
    for _position_key, token, _remaining, cost_for_remaining in store.iter_open_swap_lots():
        sym = _canonical(token)
        if not sym:
            continue
        if cost_for_remaining is None:
            by_token[sym] = None
            continue
        prior = by_token.get(sym, Decimal("0"))
        # Once a token is marked unmeasured (None), keep it unmeasured.
        if prior is None:
            continue
        by_token[sym] = prior + cost_for_remaining
    return by_token


def compute_inventory_revaluation(
    *,
    snapshot_initial: dict[str, Any] | None,
    snapshot_final: dict[str, Any] | None,
    accounting_events: list[dict[str, Any]],
    deployment_id: str,
) -> InventoryRevaluation:
    """Compute the ambient-inventory revaluation term for a snapshot bracket.

    See the module docstring for the full contract. Summary:

    * **Ambient term**: for every token held at either endpoint that is NOT a
      tracked (payload-named) symbol, contribute ``qty_idle × (mark_final −
      mark_initial)`` where ``qty_idle = wallet_balance − Σ open_lot_remaining``
      floored at zero. The wallet balance is the *final* balance (the inventory
      the strategy ends holding); ``Δmark`` uses the same token's marks on the
      two endpoint snapshots.
    * **Open swap-lot term** (Q1b): for each open swap lot, contribute
      ``remaining × mark_final − basis_remaining`` (the lot's residual MTM),
      reusing VIB-4984's ``cost_for_remaining`` as the basis.
    * **Confidence**: ``None`` total + ``unmeasured_price`` when a held token has
      a non-zero quantity at an endpoint but no usable mark there;
      ``unmeasured_basis`` when an open lot is missing its basis. Empty ≠ Zero.

    Scoping: open-lot replay is restricted to ``deployment_id`` so a shared
    wallet only marks this strategy's own inventory. When ``deployment_id`` is
    empty the lane fails closed (``unmeasured_basis``) **iff** the event stream
    carries SWAP activity it cannot attribute (the only event class that creates
    open FIFO lots) — a partially-traded token would otherwise be silently
    mis-valued as pure ambient. With no SWAP activity the ambient term is exact
    and the lane measures normally even without a scope, including windows that
    carry only non-swap (LP / lending) events.
    A final wallet row whose balance is unmeasured (``None``) likewise fails
    closed (``unmeasured_balance``) rather than coercing to a zero holding.
    """
    initial_marks = _wallet_marks(snapshot_initial)
    final_marks = _wallet_marks(snapshot_final)
    # ``tracked`` is the set of canonical symbols any accounting payload names —
    # a diagnostic audit field (``excluded_tokens``). It is NOT the partition
    # itself: the partition is by HELD QUANTITY (below). A token can be both
    # payload-named (its realized flow is in a component bucket) AND carry an
    # idle remainder still sitting in the wallet (e.g. the unspent half of a
    # single-sided swap) — that remainder is genuine ambient inventory. Keying
    # the partition off the held wallet balance, not off symbol membership, is
    # what makes "the unspent half of a single-sided swap" reconcile (design
    # §1, blueprint 27 §11.5).
    tracked = _tracked_symbols(accounting_events)

    # Replay this deployment's events into a fresh FIFO store so the open-lot
    # quantities and basis match exactly what VIB-4984's dashboard tile sees.
    # Open-lot replay is scoped to ``deployment_id`` so a shared wallet only
    # marks this strategy's own inventory.
    store = FIFOBasisStore()
    if deployment_id:
        scoped_events = [
            ev for ev in accounting_events if isinstance(ev, dict) and ev.get("deployment_id") == deployment_id
        ]
        store.reconstruct_from_events(scoped_events)
        open_remaining = _open_lot_remaining_by_token(store)
        open_basis = _open_lot_basis_by_token(store)
    elif _has_unattributable_trading_activity(accounting_events):
        # Empty deployment_id but the event stream carries SWAP activity we
        # cannot attribute to a FIFO store (a shared wallet's lots are not ours
        # to replay). Without the open-lot decomposition a *partially*-traded
        # token would be silently mis-valued as pure ambient (qty_idle × Δmark
        # instead of remaining × mark_final − basis), so we fail closed
        # (Empty ≠ Zero) rather than return a measured-but-wrong total. With no
        # SWAP activity there are no open lots to miss — the ambient term is
        # exact and we measure normally (the ``else``), even for windows that
        # carry non-swap (LP / lending) events.
        return InventoryRevaluation(
            total_usd=None,
            per_token={},
            confidence="unmeasured_basis",
            excluded_tokens=sorted(tracked),
        )
    else:
        open_remaining = {}
        open_basis = {}

    per_token: dict[str, str] = {}
    ambient_total = Decimal("0")
    open_lot_total = Decimal("0")
    saw_open_lot = False

    # The partition is over every token the wallet HOLDS at the final endpoint,
    # unioned with the open-lot tokens (a lot whose token left the wallet
    # entirely — deployed into an LP, disposed via a later swap — has
    # ``held_final = 0`` and contributes nothing, which is exactly the
    # double-count guard for a round-trip that ends flat: rule 5, §11.2).
    all_symbols = set(initial_marks) | set(final_marks) | set(open_remaining)
    for sym in sorted(all_symbols):
        # Empty ≠ Zero: a token whose final wallet ROW is present but whose
        # balance is unmeasured (``None``) must fail closed — we cannot assert
        # "holds nothing" from an unmeasured balance. A token genuinely ABSENT
        # from the final wallet (no row at all — deployed into an LP, fully
        # disposed) correctly has held=0; that is NOT the same as a present row
        # with an unmeasured balance.
        final_row = final_marks.get(sym)
        if final_row is not None and final_row.get("balance") is None:
            return InventoryRevaluation(
                total_usd=None,
                per_token=per_token,
                confidence="unmeasured_balance",
                excluded_tokens=sorted(tracked),
            )
        held = (final_row.get("balance") if final_row is not None else None) or Decimal("0")
        lot_remaining = open_remaining.get(sym, Decimal("0"))

        # The held balance partitions into (a) the portion still backed by an
        # open swap lot — valued by the open-lot MTM term — and (b) the idle
        # remainder — valued by the ambient Δmark term. ``lot_held`` is bounded
        # by what is ACTUALLY in the wallet so a FIFO lot whose token was
        # deployed/disposed (held=0) is not re-marked. Each held unit is counted
        # exactly once and ``lot_held + qty_idle == held``.
        lot_held = min(lot_remaining, held) if held > 0 else Decimal("0")
        qty_idle = held - lot_held

        # ── Open swap-lot term (Q1b): remaining × mark_final − basis_remaining,
        # pro-rated to the portion of the lot still held in the wallet.
        if lot_held > 0:
            saw_open_lot = True
            full_remaining = lot_remaining
            basis_remaining = open_basis.get(sym)
            if basis_remaining is None:
                # Missing basis — refuse to mark held inventory as pure profit.
                return InventoryRevaluation(
                    total_usd=None,
                    per_token=per_token,
                    confidence="unmeasured_basis",
                    excluded_tokens=sorted(tracked),
                )
            mark = final_marks.get(sym, {}).get("price")
            if mark is None:
                # No persisted final mark — degrade (never fetch a live price;
                # gateway boundary). Empty ≠ Zero.
                return InventoryRevaluation(
                    total_usd=None,
                    per_token=per_token,
                    confidence="unmeasured_price",
                    excluded_tokens=sorted(tracked),
                )
            # Pro-rate the basis to the held portion of the lot so the open-lot
            # term and the ambient term never overlap on the same quantity.
            held_basis = basis_remaining * (lot_held / full_remaining) if full_remaining > 0 else Decimal("0")
            lot_contribution = (lot_held * mark) - held_basis
            open_lot_total += lot_contribution
            if lot_contribution != 0:
                per_token[f"{sym}:open_lot"] = str(lot_contribution)

        # ── Ambient term: qty_idle × (mark_final − mark_initial). ───────────
        if qty_idle > 0:
            init_price = initial_marks.get(sym, {}).get("price")
            final_price = final_marks.get(sym, {}).get("price")
            # Empty ≠ Zero: a held idle quantity with an unmeasured mark at
            # EITHER endpoint makes the whole term unmeasured. We need both
            # marks to form a Δ; a missing endpoint price is "unknown", not "no
            # change". (Native gas is captured HERE by the general rule — no
            # special case; the native row carries a qty and a Δmark like any
            # other ambient token.)
            if init_price is None or final_price is None:
                return InventoryRevaluation(
                    total_usd=None,
                    per_token=per_token,
                    confidence="unmeasured_price",
                    excluded_tokens=sorted(tracked),
                )
            contribution = qty_idle * (final_price - init_price)
            ambient_total += contribution
            if contribution != 0:
                per_token[sym] = str(contribution)

    if saw_open_lot:
        per_token["<open_swap_lots_total>"] = str(open_lot_total)

    return InventoryRevaluation(
        total_usd=ambient_total + open_lot_total,
        per_token=per_token,
        confidence="measured",
        excluded_tokens=sorted(tracked),
    )
