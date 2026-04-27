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

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

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

    # ── Timestamp ────────────────────────────────────────────────────────────
    raw_ts = ledger_row.get("timestamp")
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        timestamp = datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        timestamp = datetime.now(UTC)

    # ── Token / amount fields ────────────────────────────────────────────────
    token_in = (ledger_row.get("token_in") or "").upper()
    token_out = (ledger_row.get("token_out") or "").upper()

    amount_in = _parse_decimal(ledger_row.get("amount_in")) or Decimal("0")
    amount_out = _parse_decimal(ledger_row.get("amount_out")) or Decimal("0")

    # Effective price from ledger row; recompute from amounts if missing / empty.
    raw_ep = ledger_row.get("effective_price")
    effective_price: Decimal
    if raw_ep and raw_ep != "":
        effective_price = _parse_decimal(raw_ep) or Decimal("0")
    else:
        effective_price = amount_out / amount_in if amount_in > 0 else Decimal("0")

    slippage_bps_raw = ledger_row.get("slippage_bps")
    slippage_bps: int | None = None
    if slippage_bps_raw is not None:
        try:
            slippage_bps = int(slippage_bps_raw)
        except (TypeError, ValueError):
            pass

    # ── Gas ──────────────────────────────────────────────────────────────────
    gas_usd: Decimal | None = None
    gas_usd_raw = ledger_row.get("gas_usd")
    if gas_usd_raw is not None and gas_usd_raw != "":
        gas_usd = _parse_decimal(gas_usd_raw)

    # ── USD pricing from price_inputs_json ──────────────────────────────────
    price_oracle = _parse_price_oracle(ledger_row.get("price_inputs_json") or "")
    amount_in_usd = _token_usd(token_in, amount_in, price_oracle)
    amount_out_usd = _token_usd(token_out, amount_out, price_oracle)

    # ── Position key for FIFO lot store ─────────────────────────────────────
    # Swap lots are keyed per-chain per-wallet (not per-protocol) so that a USDC
    # balance accumulated on Arbitrum across different DEXes is tracked as one pool.
    chain_norm = chain.lower().strip()
    wallet_norm = wallet_address.lower().strip()
    swap_position_key = f"swap:{chain_norm}:{wallet_norm}" if chain_norm and wallet_norm else ""

    # ── FIFO lot matching ────────────────────────────────────────────────────
    realized_pnl_usd: Decimal | None = None
    cost_basis_recorded = False

    if basis_store is not None and swap_position_key:
        # 1. Consume token_in lots to compute realized PnL.
        if amount_in > 0 and token_in:
            cost_basis_consumed, _unmatched = basis_store.match_swap_disposal(
                deployment_id=deployment_id,
                position_key=swap_position_key,
                token=token_in,
                amount=amount_in,
            )
            if cost_basis_consumed is not None and amount_in_usd is not None and _unmatched == Decimal("0"):
                realized_pnl_usd = amount_in_usd - cost_basis_consumed

        # 2. Record acquisition lot for token_out (only when a positive amount was acquired).
        if token_out and amount_out > 0:
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

    # ── Confidence ───────────────────────────────────────────────────────────
    if amount_in_usd is not None and amount_out_usd is not None:
        confidence = AccountingConfidence.HIGH
        unavailable_reason = ""
    else:
        confidence = AccountingConfidence.ESTIMATED
        missing: list[str] = []
        if amount_in_usd is None:
            missing.append(f"{token_in or 'token_in'} price")
        if amount_out_usd is None:
            missing.append(f"{token_out or 'token_out'} price")
        unavailable_reason = f"missing prices in price_inputs_json: {', '.join(missing)}"

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


def _parse_price_oracle(price_inputs_json: str) -> dict[str, Any]:
    """Parse price_inputs_json → {symbol: price_str} dict.  Returns {} on failure."""
    if not price_inputs_json:
        return {}
    try:
        d = json.loads(price_inputs_json)
        return d if isinstance(d, dict) else {}
    except (json.JSONDecodeError, TypeError):
        return {}


def _token_usd(symbol: str, amount: Decimal | None, oracle: dict[str, Any]) -> Decimal | None:
    """Compute USD value for a token amount using the price oracle.

    Returns None when the price is missing or the amount is None.
    Looks up symbol case-insensitively (tries upper then lower).
    """
    if not symbol or amount is None:
        return None
    price_raw = oracle.get(symbol.upper()) or oracle.get(symbol.lower())
    if price_raw is None:
        return None
    try:
        return Decimal(str(price_raw)) * amount
    except Exception:  # noqa: BLE001
        return None
