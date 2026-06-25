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

# The persisted ``positions_json`` row marker the portfolio valuer stamps on a
# held-PT inventory position (``portfolio_valuer._PT_INVENTORY_SOURCE`` /
# ``_classify_pt_inventory``, VIB-5316). A held PT is KNOWN_UNPRICEABLE in the
# spot oracle (the ``PT-`` prefix), so it produces NO ``wallet_balances_json``
# row — its gateway-discounted mark lives ONLY on this position row. The lane
# matches the marker as a literal (VIB-4636 discipline — detect by data-shape
# marker, never by a protocol-name string) so it does not import the valuer;
# mirrors ``accountant_test._open_pt_inventory_rows``.
_PT_INVENTORY_SOURCE = "pt_inventory_lots"

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


def _pt_inventory_marks(snapshot: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    """Held-PT marks read from ``positions_json`` (the SECOND mark source).

    The §11.5 lane's primary mark source — ``_wallet_marks`` over
    ``wallet_balances_json`` — never carries a held PT: a held PT is
    KNOWN_UNPRICEABLE in the spot oracle (the ``PT-`` prefix; ``portfolio_valuer``
    skips it from the wallet/position spot valuation to avoid double-counting), so
    it produces NO wallet-balances row. Its gateway-discounted mark lives ONLY on
    the synthetic ``positions_json`` row tagged ``details.source ==
    "pt_inventory_lots"`` (``_classify_pt_inventory``, VIB-5316). This reader is
    that second mark source.

    Returns ``{canonical_symbol: {"balance", "price", "cost", "unmeasured"}}``,
    keyed on the MATURITY-BEARING canonical symbol (``_canonical`` /
    ``basis.canonical_symbol``) — NOT the maturity-insensitive
    ``canonical_pt_symbol`` (a JOIN/DEDUP identity only, never a pricing key). Two
    maturities of the same underlying therefore stay distinct here, mirroring the
    valuer's pricing key (``portfolio_valuer._aggregate_open_pt_lots`` keys PT
    pricing on the maturity-bearing ``canonical_symbol`` for exactly this reason).

    * ``balance`` — the held PT quantity (``details.quantity``);
    * ``price`` — the PER-UNIT mark ``value_usd / quantity`` (the top-level
      ``value_usd`` is the whole-lot mark);
    * ``cost`` — the buy-time-anchored USD cost basis (top-level
      ``cost_basis_usd``; omitted/falsy on the row → unmeasured);
    * ``unmeasured`` — True when ANY of the row's ``details.mark_unmeasured`` /
      ``cost_basis_unmeasured`` / ``unrealized_pnl_unmeasured`` flags is set, OR
      the quantity / ``value_usd`` is absent/unparseable. Empty ≠ Zero: a missing
      measurement is unmeasured, never a fabricated 0.

    Parse discipline mirrors ``accountant_test._open_pt_inventory_rows`` exactly:
    accept a bare list OR the versioned envelope ``{"positions": [...]}``; a
    malformed / unreadable ``positions_json`` yields no PT marks (the caller's
    Empty ≠ Zero handling then governs — a held PT with no readable mark surfaces
    as unmeasured via the missing-mark path, never a silent zero).

    When the same maturity-bearing canonical PT symbol appears in more than one row (defensive — the
    valuer emits one row per symbol), quantities accumulate and the first usable
    mark/cost wins, with ``unmeasured`` latching True if any contributing row is
    unmeasured.
    """
    out: dict[str, dict[str, Any]] = {}
    if not snapshot:
        return out
    positions = _loads_positions(snapshot.get("positions_json"))
    for p in positions:
        if not isinstance(p, dict):
            continue
        details = p.get("details")
        if not isinstance(details, dict) or details.get("source") != _PT_INVENTORY_SOURCE:
            continue
        # Maturity-BEARING key (``_canonical``, identical to
        # ``basis.canonical_symbol``): two maturities of the same underlying must
        # NOT collapse into one bucket and keep the first mark. The valuer's PT
        # pricing aggregation keys the same way (``_aggregate_open_pt_lots``). The
        # ``positions_json`` ``pt_symbol`` and the FIFO ``pt_token`` are BOTH the
        # maturity-bearing on-chain ledger symbol (same source), so ``_canonical``
        # of both yields the identical join key — but distinct maturities stay
        # distinct. ``_canonical(None) == ""``; the ``if not sym`` below skips it.
        sym = _canonical(details.get("pt_symbol") or details.get("asset"))
        if not sym:
            continue
        qty = _parse_dec(details.get("quantity"))
        value_usd = _parse_dec(p.get("value_usd"))
        cost = _parse_dec(p.get("cost_basis_usd"))
        # Empty ≠ Zero: a flagged-unmeasured row, or one missing its quantity /
        # whole-lot mark, is unmeasured — never coerce to a 0 holding or 0 mark.
        flag_unmeasured = bool(
            details.get("mark_unmeasured")
            or details.get("cost_basis_unmeasured")
            or details.get("unrealized_pnl_unmeasured")
        )
        row_unmeasured = flag_unmeasured or qty is None or qty <= 0 or value_usd is None
        per_unit = (value_usd / qty) if (value_usd is not None and qty is not None and qty > 0) else None

        slot = out.setdefault(sym, {"balance": None, "price": None, "cost": None, "unmeasured": False})
        if qty is not None:
            slot["balance"] = (slot["balance"] or Decimal("0")) + qty
        if slot["price"] is None and per_unit is not None:
            slot["price"] = per_unit
        if slot["cost"] is None and cost is not None:
            slot["cost"] = cost
        if row_unmeasured:
            slot["unmeasured"] = True
    return out


def _loads_positions(raw: Any) -> list[Any]:
    """Decode a persisted ``positions_json`` column to its positions list.

    Accepts the two shapes ``accountant_test._open_pt_inventory_rows`` tolerates:
    a legacy bare list, OR the versioned envelope ``{"positions": [...]}``
    (VIB-3923). An unreadable / unexpected shape yields ``[]`` (no PT marks).
    """
    if raw is None or raw == "" or raw == "[]":
        return []
    decoded: Any = raw
    if isinstance(raw, str):
        import json

        try:
            decoded = json.loads(raw)
        except (ValueError, TypeError):
            return []
    if isinstance(decoded, list):
        return decoded
    if isinstance(decoded, dict) and isinstance(decoded.get("positions"), list):
        return decoded["positions"]
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
    # Fold held-PT lots into the same open-lot remaining map (VIB-5410). A held
    # PT is the unmatched ``PT_BUY`` residual; it is NOT a swap lot (disjoint key
    # namespace + ``remaining_pt`` field), so it must be summed separately. Keyed
    # by the MATURITY-BEARING ``_canonical`` (CR: NOT ``canonical_pt_symbol``,
    # which is a JOIN/DEDUP identity only — never a pricing key): each maturity of
    # the same underlying stays DISTINCT, mirroring the valuer's PT pricing key
    # (``portfolio_valuer._aggregate_open_pt_lots``). The PT lot joins its
    # ``positions_json`` mark (same maturity-bearing on-chain symbol) and never
    # collides with a swap-lane/ambient symbol (PT symbols carry the ``PT-``
    # prefix — disjoint namespace, VIB-5353, basis.py).
    for _position_key, pt_token, remaining_pt, _sy_cost, _usd_cost in store.iter_open_pt_lots():
        sym = _canonical(pt_token)
        if not sym:
            continue
        by_token[sym] = by_token.get(sym, Decimal("0")) + remaining_pt
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

    def _add(sym: str, cost: Decimal | None) -> None:
        """Accumulate a lot's basis with None-poisoning (Empty ≠ Zero)."""
        if cost is None:
            by_token[sym] = None
            return
        prior = by_token.get(sym, Decimal("0"))
        if prior is None:  # already unmeasured — keep it unmeasured.
            return
        by_token[sym] = prior + cost

    for _position_key, token, _remaining, cost_for_remaining in store.iter_open_swap_lots():
        sym = _canonical(token)
        if sym:
            _add(sym, cost_for_remaining)
    # Fold held-PT lots (VIB-5410): the 5th tuple element ``usd_cost`` is the
    # buy-time-anchored USD cost ALREADY pro-rated for ``remaining_pt``
    # (basis.py:iter_open_pt_lots) — the direct analogue of the swap lane's
    # ``cost_for_remaining``. Same None-poisoning discipline; keyed by the
    # MATURITY-BEARING ``_canonical`` (CR: NOT ``canonical_pt_symbol``) so each
    # maturity stays distinct, mirroring the valuer's PT pricing key.
    for _position_key, pt_token, _remaining_pt, _sy_cost, usd_cost in store.iter_open_pt_lots():
        sym = _canonical(pt_token)
        if sym:
            _add(sym, usd_cost)
    return by_token


def _open_pt_lot_symbols(store: FIFOBasisStore) -> set[str]:
    """Maturity-BEARING canonical symbols of the open held-PT lots (VIB-5410).

    A held PT is the unmatched ``PT_BUY`` residual — it carries NO
    ``wallet_balances_json`` row (KNOWN_UNPRICEABLE) and, when the final
    ``positions_json`` carries no ``pt_inventory_lots`` mark, it would otherwise be
    indistinguishable from an ordinary swap-lane token whose balance left the
    wallet (``held = 0`` ⇒ contributes 0). This set lets the caller route such a
    STILL-OPEN PT through the PT branch so the missing-mark case degrades
    (Empty ≠ Zero) instead of silently contributing 0. Keyed identically to the
    PT marks (``_canonical`` of the lot's ``pt_token``), so the two join.
    """
    return {
        sym
        for _position_key, pt_token, _remaining_pt, _sy_cost, _usd_cost in store.iter_open_pt_lots()
        if (sym := _canonical(pt_token))
    }


@dataclass(frozen=True)
class _HeldResolution:
    """Per-symbol resolution of held quantity + open-lot remaining (or degrade/skip).

    Exactly one of three states (mutually exclusive):

    * ``skip`` — the symbol contributes nothing (a held→flat PT disposed by the
      bracket end: no open lot remains, no final mark). The caller ``continue``s.
    * ``degrade`` — a non-empty ``confidence`` string (``unmeasured_*``); the
      caller returns a ``None``-total :class:`InventoryRevaluation` immediately
      (Empty ≠ Zero).
    * **measured** — both ``held`` and ``lot_remaining`` are non-``None`` and the
      caller proceeds to the open-lot / ambient terms.
    """

    held: Decimal | None = None
    lot_remaining: Decimal | None = None
    degrade: str = ""
    skip: bool = False


def _resolve_held_quantity(
    sym: str,
    *,
    is_pt: bool,
    pt_final: dict[str, dict[str, Any]],
    final_marks: dict[str, dict[str, Any]],
    open_remaining: dict[str, Decimal],
) -> _HeldResolution:
    """Resolve a symbol's held quantity and open-lot remaining for one bracket.

    PT path (VIB-5410): held quantity + mark come from the ``positions_json``
    ``pt_inventory_lots`` row (``pt_final``), NOT ``wallet_balances_json``. A PT
    absent from the FINAL snapshot whose FIFO lot is fully matched was
    disposed/redeemed by the bracket end → ``skip`` (held = 0, its realized leg is
    in the SWAP/PEN buckets); a STILL-OPEN PT lot lacking a final mark, or a
    flagged-unmeasured PT row, degrades (``unmeasured_price`` — mirrors PEN3).

    Non-PT path: a present final wallet row with an unmeasured (``None``) balance
    degrades (``unmeasured_balance`` — cannot assert "holds nothing" from an
    unmeasured balance); a token genuinely ABSENT from the final wallet correctly
    has ``held = 0``.
    """
    if is_pt:
        pt_row = pt_final.get(sym)
        if pt_row is None:
            # PT absent from the FINAL snapshot. No open lot remaining ⇒ disposed
            # by the bracket end → contribute 0 (skip); a still-open lot lacking a
            # final mark is genuinely unmeasured.
            if open_remaining.get(sym, Decimal("0")) <= 0:
                return _HeldResolution(skip=True)
            return _HeldResolution(degrade="unmeasured_price")
        if pt_row.get("unmeasured"):
            # Empty ≠ Zero: a flagged-unmeasured PT row degrades the whole term.
            return _HeldResolution(degrade="unmeasured_price")
        held = pt_row.get("balance") or Decimal("0")
        # The held PT is the open-lot residual: ``lot_remaining`` tracks it via the
        # PT-fold in ``_open_lot_remaining_by_token`` (no ambient remainder).
        return _HeldResolution(held=held, lot_remaining=open_remaining.get(sym, held))

    # Non-PT: Empty ≠ Zero on a present-but-unmeasured final balance.
    final_row = final_marks.get(sym)
    if final_row is not None and final_row.get("balance") is None:
        return _HeldResolution(degrade="unmeasured_balance")
    held = (final_row.get("balance") if final_row is not None else None) or Decimal("0")
    return _HeldResolution(held=held, lot_remaining=open_remaining.get(sym, Decimal("0")))


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
    * **Held-PT term** (VIB-5410): a held PT is KNOWN_UNPRICEABLE in the spot
      oracle, so it never enters ``wallet_balances_json``; its discounted mark and
      buy-time cost basis live on the ``positions_json`` ``pt_inventory_lots`` row
      instead. A PT symbol therefore sources its ``held`` quantity and
      ``mark_final`` from :func:`_pt_inventory_marks` (NOT ``final_marks``) and
      takes ONLY the open-lot MTM branch (``lot_held × mark_final − basis_held``;
      ``qty_idle == 0`` — it has no wallet ambient marks). The held-PT valuation
      key is the MATURITY-BEARING canonical symbol (``_canonical``), so two
      maturities of the same underlying stay DISTINCT (each valued with its own
      mark) — mirroring the valuer's PT pricing key
      (``portfolio_valuer._aggregate_open_pt_lots``), NOT the maturity-insensitive
      ``canonical_pt_symbol`` (a JOIN/DEDUP identity only). A PT disposed by the
      final endpoint (fully matched by ``PT_SELL`` / ``PT_REDEEM``, no open lot
      remaining) contributes 0 like any non-PT token absent from the final wallet
      — its realized leg is already in the SWAP / PEN buckets. The whole term
      degrades to ``None`` only when a STILL-OPEN PT row is flagged unmeasured or
      its open-lot basis is missing, mirroring PEN3.
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
    # SECOND mark source (VIB-5410): held PTs are KNOWN_UNPRICEABLE in the spot
    # oracle, so they never appear in ``wallet_balances_json`` — their discounted
    # gateway mark + buy-time cost basis live ONLY on the ``positions_json``
    # ``pt_inventory_lots`` rows. A PT symbol sources its held quantity / mark /
    # cost from these maps, NOT from ``final_marks``.
    pt_final = _pt_inventory_marks(snapshot_final)
    pt_initial = _pt_inventory_marks(snapshot_initial)
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
        open_pt_symbols = _open_pt_lot_symbols(store)
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
        open_pt_symbols = set()

    per_token: dict[str, str] = {}
    ambient_total = Decimal("0")
    open_lot_total = Decimal("0")
    saw_open_lot = False

    # The partition is over every token the wallet HOLDS at the final endpoint,
    # unioned with the open-lot tokens (a lot whose token left the wallet
    # entirely — deployed into an LP, disposed via a later swap — has
    # ``held_final = 0`` and contributes nothing, which is exactly the
    # double-count guard for a round-trip that ends flat: rule 5, §11.2).
    all_symbols = (
        set(initial_marks) | set(final_marks) | set(open_remaining) | set(pt_final) | set(pt_initial) | open_pt_symbols
    )
    for sym in sorted(all_symbols):
        # VIB-5410: a PT symbol (present in either PT map OR as an open held-PT
        # FIFO lot) is sourced ENTIRELY from ``positions_json`` — held quantity and
        # mark from ``pt_final`` (NOT
        # ``final_marks``, which has no PT row), so ``lot_held == held`` and
        # ``qty_idle == 0`` (a PT never takes the ambient ``qty_idle × Δmark``
        # branch — it has no wallet ambient marks). The held-PT valuation key is
        # the MATURITY-BEARING ``_canonical`` symbol (CR), so two distinct
        # maturities of the same underlying stay distinct (each marked from its own
        # ``pt_final`` row) — never collapsed into one bucket. PT symbols carry the
        # ``PT-`` prefix and live in a disjoint namespace from swap/ambient wallet
        # symbols (VIB-5353), so this branch never shadows an ordinary token.
        is_pt = sym in pt_final or sym in pt_initial or sym in open_pt_symbols

        resolution = _resolve_held_quantity(
            sym,
            is_pt=is_pt,
            pt_final=pt_final,
            final_marks=final_marks,
            open_remaining=open_remaining,
        )
        if resolution.skip:
            # Held → flat: a PT disposed/redeemed by the bracket end contributes 0.
            continue
        if resolution.degrade:
            # Empty ≠ Zero: an unmeasured PT row / balance degrades the whole term.
            return InventoryRevaluation(
                total_usd=None,
                per_token=per_token,
                confidence=resolution.degrade,
                excluded_tokens=sorted(tracked),
            )
        assert resolution.held is not None and resolution.lot_remaining is not None
        held = resolution.held
        lot_remaining = resolution.lot_remaining

        # The held balance partitions into (a) the portion still backed by an
        # open swap lot — valued by the open-lot MTM term — and (b) the idle
        # remainder — valued by the ambient Δmark term. ``lot_held`` is bounded
        # by what is ACTUALLY in the wallet so a FIFO lot whose token was
        # deployed/disposed (held=0) is not re-marked. Each held unit is counted
        # exactly once and ``lot_held + qty_idle == held``. (For a PT,
        # ``lot_held == held`` and ``qty_idle == 0`` — the held-PT term is purely
        # the open-lot MTM branch.)
        lot_held = min(lot_remaining, held) if held > 0 else Decimal("0")
        qty_idle = held - lot_held

        # ── Open swap-lot / held-PT term (Q1b): remaining × mark_final −
        # basis_remaining, pro-rated to the portion of the lot still held.
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
            # PT mark comes from the ``positions_json`` row (its discounted
            # gateway mark); a non-PT mark comes from ``wallet_balances_json``.
            mark = pt_final[sym]["price"] if is_pt else final_marks.get(sym, {}).get("price")
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
