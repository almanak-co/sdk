"""Swap category handler for AccountingProcessor (VIB-3473).

Reads all inputs from the ledger row (price_inputs_json, token_in/out, amounts,
effective_price, slippage_bps) — no live chain calls.

FIFO cost basis:
  - token_in:  FIFO-match against previously recorded acquisition lots to compute
               realized_pnl_usd = amount_in_usd - cost_basis_consumed.
  - token_out: record a new acquisition lot so future disposals can match against it.

Connector-specific swap treatments are routed by ``AccountingProcessor`` before
this generic handler is called.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.accounting.category_handlers._price_helpers import parse_price_inputs
from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    SwapAccountingEvent,
    SwapEventType,
)

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore

logger = logging.getLogger(__name__)


def _determine_confidence(
    *,
    has_price_in: bool,
    has_price_out: bool,
    token_in: str,
    token_out: str,
    amounts_unmeasured: bool,
) -> tuple[AccountingConfidence, str]:
    """Compute the SwapAccountingEvent confidence + unavailable_reason.

    HIGH confidence requires that both legs have USD prices in
    ``price_inputs_json`` AND the receipt parser resolved token decimals
    (so ledger ``amount_in`` / ``amount_out`` are not empty strings). Any
    gap drops the row to ESTIMATED with a typed reason composed of all
    applicable causes — important for auditing because both gaps can
    co-occur on the same row and downstream consumers should see all of
    them, not just the first.

    The price-presence signal is passed in as an explicit boolean rather
    than inferred from ``amount_*_usd is None``: when ``amounts_unmeasured``
    is True we deliberately force the USD fields to None (Empty != zero
    propagation), so the absence-by-None test would falsely report
    "missing prices" on a row whose prices were actually present.
    """
    reasons: list[str] = []
    if not has_price_in or not has_price_out:
        missing: list[str] = []
        if not has_price_in:
            missing.append(f"{token_in or 'token_in'} price")
        if not has_price_out:
            missing.append(f"{token_out or 'token_out'} price")
        reasons.append(f"missing prices in price_inputs_json: {', '.join(missing)}")
    if amounts_unmeasured:
        # Surface the parser-side decimals failure so an auditor can see
        # exactly why ``effective_price`` is None on this row.
        reasons.append("swap amounts unmeasured (token decimals could not be resolved by receipt parser)")
    if reasons:
        return AccountingConfidence.ESTIMATED, "; ".join(reasons)
    return AccountingConfidence.HIGH, ""


def _parse_timestamp(raw_ts: Any) -> datetime:
    """Parse the ledger row's timestamp; fall back to ``now(UTC)`` on
    malformed / missing input."""
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        return datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        return datetime.now(UTC)


def _parse_slippage_bps(raw: Any) -> int | None:
    """Parse a slippage_bps ledger value; return None on missing /
    non-coercible input."""
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


def _parse_optional_decimal(raw: Any) -> Decimal | None:
    """Parse a decimal-or-empty ledger value; return None for None / empty."""
    if raw is None or raw == "":
        return None
    return _parse_decimal(raw)


def _select_effective_price(
    raw_ep: Any,
    amount_in: Decimal | None,
    amount_out: Decimal | None,
    amounts_unmeasured: bool,
) -> Decimal | None:
    """Pick the SwapAccountingEvent ``effective_price`` from the ledger row.

    Order of precedence:
    1. If amounts are unmeasured, force None — a stale or non-empty
       ``effective_price`` field cannot rescue an unmeasured row (Empty
       != zero, docs/internal/blueprints/27-accounting.md).
    2. If the ledger row carries a non-empty ``effective_price``, use it.
    3. Otherwise compute ``amount_out / amount_in`` when both sides are
       measured and ``amount_in > 0``.
    4. Otherwise None (unmeasured / unrecoverable).
    """
    if amounts_unmeasured:
        return None
    if raw_ep and raw_ep != "":
        return _parse_decimal(raw_ep)
    if amount_in is not None and amount_out is not None and amount_in > 0:
        return amount_out / amount_in
    return None


@dataclass(frozen=True)
class _SwapMatchOutcome:
    """FIFO matching outcome for one SWAP disposal (VIB-4905 / F1).

    Returned by :func:`_record_basis_lots` so partial-match disposals can
    surface a matched-portion PnL even when the legacy ``realized_pnl_usd``
    field is forced to ``None``.

    Field semantics — Empty ≠ Zero throughout:

    * ``realized_pnl_usd`` (legacy): ``amount_in_usd - cost_basis_consumed``
      ONLY on a full match (``_unmatched == 0``).  ``None`` otherwise.
      Preserved for backward compat with consumers that expect the v1
      contract.
    * ``realized_pnl_usd_matched`` (VIB-4905): matched-portion PnL,
      computed pro-rated for partial matches too.  ``None`` when no prior
      basis was matched at all (the "no acquisition lots" case).
    * ``unmatched_amount_in``: the portion of ``amount_in`` that had no
      matching basis lot.  ``Decimal("0")`` on a full match; the full
      ``amount_in`` when no lots existed; the partial residual otherwise.
      ``None`` when matching was not attempted.
    * ``unmatched_proceeds_usd``: pro-rated USD proceeds attributable to
      ``unmatched_amount_in``.  ``None`` when ``amount_in_usd`` was
      unavailable.
    * ``cost_basis_recorded``: ``True`` iff a new acquisition lot was
      written for ``token_out``.
    """

    realized_pnl_usd: Decimal | None
    realized_pnl_usd_matched: Decimal | None
    unmatched_amount_in: Decimal | None
    unmatched_proceeds_usd: Decimal | None
    cost_basis_recorded: bool


def _split_proceeds(
    amount_in: Decimal,
    amount_in_usd: Decimal | None,
    unmatched: Decimal,
) -> tuple[Decimal | None, Decimal | None]:
    """Pro-rate ``amount_in_usd`` into (matched_proceeds, unmatched_proceeds).

    Return shapes — Empty ≠ Zero throughout:

    * ``(None, None)`` — ``amount_in_usd`` is unavailable or ``amount_in <=
      0``.  Never substitute a zero for an unmeasured price.
    * ``(None, amount_in_usd)`` — ``matched_amount <= 0`` (the "lots
      existed but were exhausted" case).  ALL proceeds attribute to the
      unmatched leg.  Matched proceeds stay ``None`` because the matched
      quantity was zero, so matched proceeds are *unmeasured* (asking
      "how much USD attributable to zero tokens" has no answer), not
      measured-zero.  Empty ≠ Zero.
    * ``(matched_proceeds, unmatched_proceeds)`` — full pro-rated split.
      The sum invariant ``matched + unmatched == amount_in_usd`` holds
      exactly (the unmatched leg is computed as the difference, so
      Decimal-context rounding noise from the multiplication round-trips).
    """
    if amount_in_usd is None or amount_in <= 0:
        return None, None
    matched_amount = amount_in - unmatched
    if matched_amount <= 0:
        # Nothing matched — matched proceeds undefined; all USD value
        # attributes to the unmatched leg.  Returning ``None`` for matched
        # preserves Empty ≠ Zero at the writer boundary; the caller's
        # ``matched_proceeds_usd is not None`` gate at
        # ``_record_basis_lots`` then naturally skips stamping
        # ``realized_pnl_usd_matched`` — defense in depth with the
        # ``matched_amount > 0`` structural guard at the same call site.
        return None, amount_in_usd
    matched_proceeds = amount_in_usd * (matched_amount / amount_in)
    return matched_proceeds, amount_in_usd - matched_proceeds


def _record_basis_lots(
    *,
    basis_store: FIFOBasisStore,
    deployment_id: str,
    cycle_id: str,
    swap_position_key: str,
    token_in: str,
    token_out: str,
    amount_in: Decimal | None,
    amount_out: Decimal | None,
    amount_in_usd: Decimal | None,
    amount_out_usd: Decimal | None,
    timestamp: datetime,
    tx_hash: str,
    ledger_entry_id: str,
) -> _SwapMatchOutcome:
    """Consume token_in lots (FIFO) and record token_out acquisition.

    Returns :class:`_SwapMatchOutcome` carrying both the legacy
    ``realized_pnl_usd`` (full-match-only) and the VIB-4905 matched /
    unmatched bundle.  Skips silently when ``amount_in`` / ``amount_out``
    is ``None`` or the position key is empty — caller filters most of
    these cases up front, this is belt-and-braces.
    """
    realized_pnl_usd: Decimal | None = None
    realized_pnl_usd_matched: Decimal | None = None
    unmatched_amount_in: Decimal | None = None
    unmatched_proceeds_usd: Decimal | None = None
    cost_basis_recorded = False

    # 1. Consume token_in lots to compute realized PnL.
    if amount_in is not None and amount_in > 0 and token_in:
        cost_basis_consumed, _unmatched = basis_store.match_swap_disposal(
            deployment_id=deployment_id,
            position_key=swap_position_key,
            token=token_in,
            amount=amount_in,
        )
        unmatched_amount_in = _unmatched

        matched_proceeds_usd, unmatched_proceeds_usd = _split_proceeds(
            amount_in=amount_in,
            amount_in_usd=amount_in_usd,
            unmatched=_unmatched,
        )

        # Codex P2 (VIB-4905 audit): the "lots exist but exhausted" case —
        # ``match_swap_disposal`` returns ``(Decimal("0"), amount_in)`` when
        # the FIFO key was registered but every lot has ``remaining == 0``.
        # ``cost_basis_consumed`` is then ``Decimal("0")``, not ``None``, so
        # gating on ``is not None`` alone would compute
        # ``matched_pnl = 0 - 0 = Decimal("0")`` and stamp it as a measured
        # zero — conflating with "actually $0 matched PnL".  Per Empty ≠
        # Zero, the matched PnL stays ``None`` when no quantity was matched.
        # ``matched_amount > 0`` is the structural guard.
        matched_amount = amount_in - _unmatched
        if cost_basis_consumed is not None and matched_proceeds_usd is not None and matched_amount > 0:
            # VIB-4905 (F1): matched-portion PnL — populated on partial
            # matches too.  The legacy ``realized_pnl_usd`` field below
            # keeps the v1 "null on partial" contract for consumers that
            # haven't migrated yet.
            realized_pnl_usd_matched = matched_proceeds_usd - cost_basis_consumed
            if _unmatched == Decimal("0"):
                realized_pnl_usd = realized_pnl_usd_matched

    # 2. Record acquisition lot for token_out (only when a positive amount was acquired).
    if token_out and amount_out is not None and amount_out > 0:
        _lot_seed = tx_hash or ledger_entry_id
        lot_id = (
            make_accounting_event_id(deployment_id, cycle_id, "SWAP_LOT", _lot_seed, token_out) if _lot_seed else ""
        )
        basis_store.record_swap_acquisition(
            deployment_id=deployment_id,
            position_key=swap_position_key,
            token=token_out,
            amount=amount_out,
            cost_usd=amount_out_usd,
            timestamp=timestamp,
            lot_id=lot_id,
        )
        cost_basis_recorded = True

    return _SwapMatchOutcome(
        realized_pnl_usd=realized_pnl_usd,
        realized_pnl_usd_matched=realized_pnl_usd_matched,
        unmatched_amount_in=unmatched_amount_in,
        unmatched_proceeds_usd=unmatched_proceeds_usd,
        cost_basis_recorded=cost_basis_recorded,
    )


def handle_swap(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
    basis_store: FIFOBasisStore | None = None,
) -> SwapAccountingEvent | None:
    """Build a SwapAccountingEvent from an outbox + ledger row pair.

    All inputs are read from the ledger row fields — no live chain calls.

    The outbox_row provides: wallet_address, position_key.
    The ledger_row provides: all other fields.

    FIFO lot management:
      - token_in:  match_swap_disposal → realized_pnl_usd (None if no prior lot)
      - token_out: record_swap_acquisition → cost_basis_recorded = True

    Called from AccountingProcessor._dispatch after category routing.
    """
    # ── Identity fields ──────────────────────────────────────────────────────
    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    chain = ledger_row.get("chain") or ""
    protocol = (ledger_row.get("protocol") or "").lower()
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""

    timestamp = _parse_timestamp(ledger_row.get("timestamp"))

    # ── Token / amount fields ────────────────────────────────────────────────
    # ``_raw`` preserves the original case from the ledger row — required for
    # Solana base58 addresses, which are case-sensitive (review feedback on
    # PR #2250 / VIB-4304). The resolver helper sees the raw value and decides
    # per-chain whether to lowercase (EVM) or preserve case (Solana) internally.
    # The uppercased ``token_in`` / ``token_out`` still drive the SwapAccountingEvent
    # identity hash, FIFO basis-store key, and ``unavailable_reason`` text — those
    # consumers expect a canonical uppercase form for EVM symbols + the
    # already-correct-case for Solana base58 (Solana case-corruption in the
    # downstream paths is a pre-existing concern, tracked separately, not
    # in scope for VIB-4304).
    token_in_raw = ledger_row.get("token_in") or ""
    token_out_raw = ledger_row.get("token_out") or ""
    # VIB-4487: resolve token_in/token_out to their canonical symbol BEFORE
    # they seed the FIFO basis-store key and the SwapAccountingEvent identity
    # hash. Four connectors (Aerodrome, PancakeSwap V3, SushiSwap V3, Uniswap
    # V4) historically stamped a raw contract address into
    # ``swap_amounts.token_in`` / ``token_out``, which propagates to
    # ``transaction_ledger.token_in`` / ``token_out`` and then here. With the
    # pre-VIB-4487 ``.upper()``-only path, a SWAP acquiring WETH via an
    # address-emitting connector keyed its FIFO lot under
    # ``...:0xc02a...`` while a symbol-emitting connector (Uniswap V3, Aave V3)
    # keyed the same token under ``...:weth`` — the two lots could never match,
    # silently corrupting cross-connector / cross-event basis reconciliation.
    #
    # ``_resolve_price_lookup_key`` (added by VIB-4304 for the price lookup)
    # already maps an address-shaped value to its canonical uppercase symbol
    # (falling through to the original value on a resolver miss, Empty != zero).
    # Extending that SAME resolution to the identity / FIFO key — exactly what
    # VIB-4487 asks for — unifies all three consumers (price lookup, FIFO key,
    # event identity) onto one canonical token identity, so two swaps of the
    # same token via different connectors land in the same FIFO lot.
    token_in = _resolve_price_lookup_key(token_in_raw, chain)
    token_out = _resolve_price_lookup_key(token_out_raw, chain)

    raw_amount_in = ledger_row.get("amount_in")
    raw_amount_out = ledger_row.get("amount_out")
    parsed_in = _parse_decimal(raw_amount_in)
    parsed_out = _parse_decimal(raw_amount_out)

    # An empty string OR an unparsable string (e.g. ``"NaN"``) in the
    # ledger row means the receipt parser could not resolve a usable
    # decimal-converted amount (see ``observability/ledger.py:_extract_from_swap_amounts``
    # — gated on ``amount_*_decimal_resolved``). Both sides must be flagged
    # as unmeasured because computing USD value, FIFO realized PnL, or
    # ``effective_price`` against ``Decimal(0)`` would silently emit a
    # measured-zero row that auditors cannot distinguish from a real
    # zero-amount swap. Per docs/internal/blueprints/27-accounting.md "Empty != zero" —
    # never conflate.
    amounts_unmeasured = parsed_in is None or parsed_out is None

    # ``amount_in`` / ``amount_out`` flow into the SwapAccountingEvent as
    # ``Decimal | None``. ``None`` propagates the unmeasured signal end-to-
    # end (FIFO matching, USD conversion, lot recording all skip below).
    amount_in: Decimal | None = parsed_in
    amount_out: Decimal | None = parsed_out

    effective_price = _select_effective_price(
        ledger_row.get("effective_price"),
        amount_in,
        amount_out,
        amounts_unmeasured,
    )

    slippage_bps = _parse_slippage_bps(ledger_row.get("slippage_bps"))
    gas_usd = _parse_optional_decimal(ledger_row.get("gas_usd"))

    # ── USD pricing from price_inputs_json (VIB-3885) ───────────────────────
    # ``parse_price_inputs`` accepts both the canonical nested shape
    # ({symbol: {price_usd, oracle_source, ...}}) and the legacy flat
    # shape ({symbol: price}); see ``_price_helpers.py`` for context.
    # Skip USD conversion when amounts are unmeasured — pricing a
    # ``Decimal(0)`` placeholder would produce ``$0`` and conflate with a
    # measured zero-USD swap.
    price_oracle = parse_price_inputs(ledger_row.get("price_inputs_json"))

    # VIB-4304: ``price_inputs_json`` is symbol-keyed (e.g. ``"WETH"``)
    # but several connectors' receipt parsers (Aerodrome confirmed; likely
    # PancakeSwap, Sushi, Uniswap V3, Curve and others) stamp the contract
    # **address** into ``swap_amounts.token_in`` / ``token_out``. That
    # propagates to ``transaction_ledger.token_in`` / ``token_out`` and
    # then to this handler as ``"0X833589FCD6..."`` — which always misses
    # the symbol-keyed ``price_oracle``, flipping every confidence
    # downgrade to ESTIMATED with a misleading "missing prices" reason.
    #
    # Resolve address-shaped values to symbol via the token resolver
    # singleton (same pattern as ``lp_handler`` and ``lending_handler``).
    # On a resolver miss the original (address) value is preserved so the
    # confidence ``unavailable_reason`` still shows the on-chain address
    # (Empty != zero / no fabricated symbol substitution).
    #
    # VIB-4487: ``token_in`` / ``token_out`` are now the canonical resolved
    # identity (computed once above), so the price lookup reuses them
    # directly instead of resolving a second time. Pre-VIB-4487 the symbol
    # was resolved HERE only for the price key while the FIFO key + identity
    # hash kept the raw ``.upper()`` address — the divergence VIB-4487 fixes.
    token_in_key = token_in
    token_out_key = token_out
    # Capture price-presence as separate booleans BEFORE the USD conversion,
    # so the confidence helper can distinguish "no price in
    # price_inputs_json" from "price was present but USD was forced to None
    # because amounts were unmeasured" (Empty != zero propagation).
    has_price_in = bool(token_in_key) and token_in_key in price_oracle
    has_price_out = bool(token_out_key) and token_out_key in price_oracle
    amount_in_usd = _token_usd(token_in_key, amount_in, price_oracle) if amount_in is not None else None
    amount_out_usd = _token_usd(token_out_key, amount_out, price_oracle) if amount_out is not None else None

    # ── Position key for FIFO lot store ─────────────────────────────────────
    # Swap lots are keyed per-chain per-wallet (not per-protocol) so that a USDC
    # balance accumulated on Arbitrum across different DEXes is tracked as one pool.
    chain_norm = chain.lower().strip()
    wallet_norm = wallet_address.lower().strip()
    swap_position_key = f"swap:{chain_norm}:{wallet_norm}" if chain_norm and wallet_norm else ""

    # FIFO matching + lot recording require measured amounts (Decimal | None
    # contract); skip both legs when either amount is unmeasured to avoid
    # consuming/recording fake-zero lots.
    realized_pnl_usd: Decimal | None = None
    realized_pnl_usd_matched: Decimal | None = None
    unmatched_amount_in: Decimal | None = None
    unmatched_proceeds_usd: Decimal | None = None
    cost_basis_recorded = False
    if basis_store is not None and swap_position_key and not amounts_unmeasured:
        outcome = _record_basis_lots(
            basis_store=basis_store,
            deployment_id=deployment_id,
            cycle_id=cycle_id,
            swap_position_key=swap_position_key,
            token_in=token_in,
            token_out=token_out,
            amount_in=amount_in,
            amount_out=amount_out,
            amount_in_usd=amount_in_usd,
            amount_out_usd=amount_out_usd,
            timestamp=timestamp,
            tx_hash=tx_hash,
            ledger_entry_id=ledger_entry_id,
        )
        realized_pnl_usd = outcome.realized_pnl_usd
        realized_pnl_usd_matched = outcome.realized_pnl_usd_matched
        unmatched_amount_in = outcome.unmatched_amount_in
        unmatched_proceeds_usd = outcome.unmatched_proceeds_usd
        cost_basis_recorded = outcome.cost_basis_recorded

    # ── Confidence ───────────────────────────────────────────────────────────
    confidence, unavailable_reason = _determine_confidence(
        has_price_in=has_price_in,
        has_price_out=has_price_out,
        token_in=token_in,
        token_out=token_out,
        amounts_unmeasured=amounts_unmeasured,
    )

    # ── Event identity ───────────────────────────────────────────────────────
    _id_seed = tx_hash or ledger_entry_id
    _id_suffix = f"{token_in}_{token_out}"
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, "SWAP", _id_seed, _id_suffix),
        deployment_id=deployment_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        timestamp=timestamp,
        chain=chain,
        protocol=protocol,
        wallet_address=wallet_address,
        tx_hash=tx_hash,
        ledger_entry_id=ledger_entry_id,
    )

    return SwapAccountingEvent(
        identity=identity,
        event_type=SwapEventType.SWAP,
        protocol=protocol,
        token_in=token_in,
        token_out=token_out,
        amount_in=amount_in,
        amount_out=amount_out,
        amount_in_usd=amount_in_usd,
        amount_out_usd=amount_out_usd,
        effective_price=effective_price,
        slippage_bps=slippage_bps,
        realized_pnl_usd=realized_pnl_usd,
        cost_basis_recorded=cost_basis_recorded,
        gas_usd=gas_usd,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
        swap_position_key=swap_position_key,
        # VIB-4905 (F1): partial-match contract — populated even when
        # ``realized_pnl_usd`` above is None due to ``_unmatched > 0``.
        realized_pnl_usd_matched=realized_pnl_usd_matched,
        unmatched_amount_in=unmatched_amount_in,
        unmatched_proceeds_usd=unmatched_proceeds_usd,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────


def _parse_decimal(value: Any) -> Decimal | None:
    """Safely parse value to Decimal.  Returns None on failure or non-finite result."""
    if value is None:
        return None
    try:
        parsed = Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None
    return parsed if parsed.is_finite() else None


def _token_usd(symbol: str, amount: Decimal | None, oracle: dict[str, Decimal]) -> Decimal | None:
    """Compute USD value for a token amount using the price oracle.

    Returns None when the price is missing or the amount is None. The
    ``oracle`` dict is the flat ``{SYMBOL_UPPER: Decimal}`` mapping
    produced by :func:`parse_price_inputs` (VIB-3885) — symbol lookup is
    therefore upper-case-only.

    Callers should pass a value already mapped to a symbol via
    :func:`_resolve_price_lookup_key` (VIB-4304). Passing a raw address
    here is a no-op price miss, not an error — the lookup just returns
    ``None`` and the confidence helper reports the gap.
    """
    if not symbol or amount is None:
        return None
    price = oracle.get(symbol.upper())
    if price is None:
        return None
    try:
        return price * amount
    except (ArithmeticError, TypeError):
        return None


def _resolve_price_lookup_key(value: str, chain: str) -> str:
    """Map a ledger ``token_in``/``token_out`` value to a canonical token key.

    Originally added by VIB-4304 to fix the price-oracle lookup; VIB-4487
    promotes this to the canonical token identity used by ALL three swap
    consumers — the ``price_inputs_json`` lookup, the FIFO basis-store key,
    and the ``SwapAccountingEvent`` identity hash — so they never diverge.

    **Single source of truth (VIB-4487 audit Fold A).** The canonicalization
    rule lives in exactly one place:
    :func:`almanak.connectors._strategy_base.base.resolve_swap_token_symbol`,
    which the SWAP receipt parsers also call when stamping
    ``swap_amounts.token_in`` / ``token_out``. This handler delegates to it so
    the value that keys the FIFO basis lot here is **byte-identical** to the
    value the parser wrote into the ledger row — any divergence (e.g. an
    EVM-address-on-miss case mismatch) would silently orphan cross-connector
    lots. Do NOT re-implement the rule here.

    The shared helper accepts ``str | None`` and returns ``str | None``; the
    ledger column is always a (possibly empty) string here, so the result is
    likewise a string. An empty input returns ``""`` unchanged (the caller
    emits its own missing-token diagnostic).

    Behaviour (inherited from the shared helper):

    - Empty → returned unchanged.
    - Symbol-shaped value → upper-cased (``price_oracle`` keys are uppercase
      per ``parse_price_inputs`` / VIB-3885).
    - Address-shaped value → ``ResolvedToken.symbol`` uppercased; on a
      resolver miss (unknown token, no chain, exception) the original address
      lowercased for EVM / preserved for Solana, so the ``has_price_*`` check
      reports "missing prices: <address>" rather than a phantom symbol
      (Empty != zero).
    """
    from almanak.connectors._strategy_base.base import resolve_swap_token_symbol

    # The ledger token column is always a string (never None) at this call
    # site; ``resolve_swap_token_symbol`` returns the same string type back.
    return resolve_swap_token_symbol(value, chain) or ""


# ──────────────────────────────────────────────────────────────────────────────
# Registry adapter (VIB-4163, T3)
# ──────────────────────────────────────────────────────────────────────────────

from almanak.framework.accounting.category_handlers import HandlerContext, register
from almanak.framework.primitives.types import AccountingCategory


@register(AccountingCategory.SWAP)
def _dispatch_swap(ctx: HandlerContext) -> SwapAccountingEvent | None:
    return handle_swap(ctx.outbox_row, ctx.ledger_row, ctx.basis_store)
