"""Swap category handler for AccountingProcessor (VIB-3473).

Reads all inputs from the ledger row (price_inputs_json, token_in/out, amounts,
effective_price, slippage_bps) — no live chain calls.

FIFO cost basis:
  - token_in:  FIFO-match against previously recorded acquisition lots to compute
               realized_pnl_usd = amount_in_usd - cost_basis_consumed.
  - token_out: record a new acquisition lot so future disposals can match against it.

Pendle PT swaps are routed to handle_pendle_pt() by the classifier before this
handler is called.  A belt-and-suspenders guard still returns None if a Pendle
swap reaches this handler to prevent double-counting.
"""

from __future__ import annotations

import logging
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
       != zero, blueprints/27-accounting.md).
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
) -> tuple[Decimal | None, bool]:
    """Consume token_in lots (FIFO) and record token_out acquisition.

    Returns ``(realized_pnl_usd, cost_basis_recorded)``. Skips silently
    when ``amount_in`` / ``amount_out`` is ``None`` or the position key
    is empty — caller filters most of these cases up front, this is
    belt-and-braces.
    """
    realized_pnl_usd: Decimal | None = None
    cost_basis_recorded = False

    # 1. Consume token_in lots to compute realized PnL.
    if amount_in is not None and amount_in > 0 and token_in:
        cost_basis_consumed, _unmatched = basis_store.match_swap_disposal(
            deployment_id=deployment_id,
            position_key=swap_position_key,
            token=token_in,
            amount=amount_in,
        )
        if cost_basis_consumed is not None and amount_in_usd is not None and _unmatched == Decimal("0"):
            realized_pnl_usd = amount_in_usd - cost_basis_consumed

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

    return realized_pnl_usd, cost_basis_recorded


def handle_swap(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
    basis_store: FIFOBasisStore | None = None,
) -> SwapAccountingEvent | None:
    """Build a SwapAccountingEvent from an outbox + ledger row pair.

    All inputs are read from the ledger row fields — no live chain calls.
    Returns None for Pendle PT swaps (handled by handle_pendle_pt).

    The outbox_row provides: wallet_address, position_key.
    The ledger_row provides: all other fields.

    FIFO lot management:
      - token_in:  match_swap_disposal → realized_pnl_usd (None if no prior lot)
      - token_out: record_swap_acquisition → cost_basis_recorded = True

    Called from AccountingProcessor._dispatch after category routing.
    """
    protocol = (ledger_row.get("protocol") or "").lower()

    # Belt-and-suspenders Pendle guard — classifier should have routed these to
    # handle_pendle_pt already.  If one arrives here, return None to avoid
    # double-counting (PT buy events carry the cost basis for Pendle, not swap lots).
    if "pendle" in protocol:
        logger.debug("handle_swap: skipping Pendle swap (protocol=%s) — owned by pendle_pt handler", protocol)
        return None

    # ── Identity fields ──────────────────────────────────────────────────────
    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    strategy_id = ledger_row.get("strategy_id") or outbox_row.get("strategy_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    chain = ledger_row.get("chain") or ""
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""

    timestamp = _parse_timestamp(ledger_row.get("timestamp"))

    # ── Token / amount fields ────────────────────────────────────────────────
    token_in = (ledger_row.get("token_in") or "").upper()
    token_out = (ledger_row.get("token_out") or "").upper()

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
    # zero-amount swap. Per blueprints/27-accounting.md "Empty != zero" —
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
    # Capture price-presence as separate booleans BEFORE the USD conversion,
    # so the confidence helper can distinguish "no price in
    # price_inputs_json" from "price was present but USD was forced to None
    # because amounts were unmeasured" (Empty != zero propagation).
    has_price_in = bool(token_in) and token_in in price_oracle
    has_price_out = bool(token_out) and token_out in price_oracle
    amount_in_usd = _token_usd(token_in, amount_in, price_oracle) if amount_in is not None else None
    amount_out_usd = _token_usd(token_out, amount_out, price_oracle) if amount_out is not None else None

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
    cost_basis_recorded = False
    if basis_store is not None and swap_position_key and not amounts_unmeasured:
        realized_pnl_usd, cost_basis_recorded = _record_basis_lots(
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
        strategy_id=strategy_id,
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
