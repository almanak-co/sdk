"""Pendle LP and Pendle PT category handlers for AccountingProcessor.

Ports logic from pendle_accounting.py and pendle_pt_accounting.py but reads
all inputs from ledger row fields — no live chain calls.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.accounting.basis import FIFOBasisStore

from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    PendleAccountingEvent,
    PendleEventType,
)
from almanak.framework.accounting.pendle_pt_accounting import (
    _parse_pt_maturity,
    compute_implied_apr_bps,
)

logger = logging.getLogger(__name__)

_SCALE_18 = Decimal(10**18)


# ──────────────────────────────────────────────────────────────────────────────
# Pendle LP handler
# ──────────────────────────────────────────────────────────────────────────────


def handle_pendle_lp(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
) -> PendleAccountingEvent | None:
    """Build a PendleAccountingEvent(PENDLE_LP_OPEN|PENDLE_LP_CLOSE) from ledger row.

    SY and PT amounts are read from deserialized extracted_data_json.
    Position key is from the outbox_row (pre-computed by runner).
    """
    from almanak.framework.observability.ledger import deserialize_extracted_data

    intent_type_str = (ledger_row.get("intent_type") or "").upper()
    if intent_type_str not in ("LP_OPEN", "LP_CLOSE"):
        return None

    protocol = (ledger_row.get("protocol") or "").lower()
    if "pendle" not in protocol:
        return None

    event_type = PendleEventType.PENDLE_LP_OPEN if intent_type_str == "LP_OPEN" else PendleEventType.PENDLE_LP_CLOSE

    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    strategy_id = ledger_row.get("strategy_id") or outbox_row.get("strategy_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    chain = ledger_row.get("chain") or ""
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""
    position_key = outbox_row.get("position_key") or ""

    raw_ts = ledger_row.get("timestamp")
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        timestamp = datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        timestamp = datetime.now(UTC)

    extracted = deserialize_extracted_data(ledger_row.get("extracted_data_json") or "")

    sy_amount_raw: int | None = None
    pt_amount_raw: int | None = None
    market_address = ""

    if intent_type_str == "LP_OPEN":
        lp_open = extracted.get("lp_open_data")
        if lp_open is not None:
            sy_amount_raw = _get_field(lp_open, "amount0")
            pt_amount_raw = _get_field(lp_open, "amount1")
        # Derive market_address from position_key or outbox
        market_address = outbox_row.get("market_id") or _market_from_position_key(position_key) or ""
    else:
        lp_close = extracted.get("lp_close_data")
        if lp_close is not None:
            sy_amount_raw = _get_field(lp_close, "amount0_collected")
            pt_amount_raw = _get_field(lp_close, "amount1_collected")
        market_address = outbox_row.get("market_id") or _market_from_position_key(position_key) or ""

    sy_amount = Decimal(str(sy_amount_raw)) / _SCALE_18 if sy_amount_raw is not None else None
    pt_amount = Decimal(str(pt_amount_raw)) / _SCALE_18 if pt_amount_raw is not None else None

    if not position_key and market_address and wallet_address:
        position_key = f"pendle_lp:{chain.lower()}:{wallet_address.lower()}:{market_address.lower()}"

    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, event_type.value, _id_seed, position_key),
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

    return PendleAccountingEvent(
        identity=identity,
        event_type=event_type,
        position_key=position_key,
        market_id=market_address,
        pt_token="",
        maturity_timestamp=None,
        pt_amount=pt_amount,
        sy_amount=sy_amount,
        pt_price=None,
        implied_apr_bps=None,
        days_to_maturity=None,
        realized_yield_usd=None,
        confidence=AccountingConfidence.ESTIMATED,
        unavailable_reason="SY/PT scaled by assumed 18-decimal precision; pt_token and USD price absent",
    )


# ──────────────────────────────────────────────────────────────────────────────
# Pendle PT handler
# ──────────────────────────────────────────────────────────────────────────────


def handle_pendle_pt(  # noqa: C901
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
    basis_store: FIFOBasisStore | None = None,
) -> PendleAccountingEvent | None:
    """Build a PendleAccountingEvent(PT_BUY) from a ledger row.

    Reads swap_amounts from deserialized extracted_data_json.
    token_out is from the ledger row's token_out column (PT symbol).

    When basis_store is provided, records a FIFO PT lot so PT_REDEEM can
    match the original cost basis.

    Ordering note: the lot is recorded inside _dispatch() BEFORE drain_one
    calls writer.write().  This is safe because FIFOBasisStore is in-memory
    and is reconstructed from accounting_events on restart — a crash before
    the event is persisted loses both the event and the in-memory lot, which
    leaves the store consistent with accounting_events on the next startup.
    """
    from almanak.framework.observability.ledger import deserialize_extracted_data

    intent_type_str = (ledger_row.get("intent_type") or "").upper()
    if intent_type_str != "SWAP":
        return None

    protocol = (ledger_row.get("protocol") or "").lower()
    if "pendle" not in protocol:
        return None

    pt_token_sym = ledger_row.get("token_out") or ""
    if not pt_token_sym.upper().startswith("PT-"):
        return None

    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    strategy_id = ledger_row.get("strategy_id") or outbox_row.get("strategy_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    chain = ledger_row.get("chain") or ""
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""
    position_key = outbox_row.get("position_key") or ""

    raw_ts = ledger_row.get("timestamp")
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        now = datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        now = datetime.now(UTC)

    extracted = deserialize_extracted_data(ledger_row.get("extracted_data_json") or "")

    # ── Amounts from swap_amounts ────────────────────────────────────────────
    sy_amount: Decimal | None = None
    pt_amount: Decimal | None = None
    swap_amounts = extracted.get("swap_amounts")
    if swap_amounts is not None:
        raw_in = _get_field(swap_amounts, "amount_in")
        raw_out = _get_field(swap_amounts, "amount_out")
        if raw_in is not None:
            try:
                sy_amount = Decimal(str(raw_in))
            except InvalidOperation:
                pass
        if raw_out is not None:
            try:
                pt_amount = Decimal(str(raw_out))
            except InvalidOperation:
                pass

    # ── PT price, maturity, implied APR ─────────────────────────────────────
    pt_price: Decimal | None = None
    if sy_amount and pt_amount and pt_amount > 0:
        try:
            pt_price = sy_amount / pt_amount
        except (InvalidOperation, ZeroDivisionError):
            pass

    maturity_ts = _parse_pt_maturity(pt_token_sym)
    days_to_maturity: int | None = None
    if maturity_ts is not None:
        days_to_maturity = (maturity_ts.date() - now.date()).days

    implied_apr_bps: int | None = None
    if pt_price is not None and days_to_maturity is not None:
        implied_apr_bps = compute_implied_apr_bps(pt_price, days_to_maturity)

    has_core_fields = pt_price is not None and pt_amount is not None
    has_apr = implied_apr_bps is not None
    if has_core_fields and has_apr:
        confidence = AccountingConfidence.HIGH
        unavailable_reason = ""
    elif has_core_fields and maturity_ts is not None:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "PT matured — days_to_maturity <= 0, implied APR not applicable"
    elif has_core_fields:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "maturity not parsed from PT symbol (implied APR unavailable)"
    else:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "PT buy amounts unavailable from receipt"

    market_address = outbox_row.get("market_id") or ""
    if not position_key and market_address and wallet_address:
        position_key = f"pendle_pt:{chain.lower()}:{wallet_address.lower()}:{market_address}"

    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, "PT_BUY", _id_seed, position_key),
        deployment_id=deployment_id,
        strategy_id=strategy_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        timestamp=now,
        chain=chain,
        protocol=protocol,
        wallet_address=wallet_address,
        tx_hash=tx_hash,
        ledger_entry_id=ledger_entry_id,
    )

    event = PendleAccountingEvent(
        identity=identity,
        event_type=PendleEventType.PT_BUY,
        position_key=position_key,
        market_id=market_address,
        pt_token=pt_token_sym,
        maturity_timestamp=maturity_ts,
        pt_amount=pt_amount,
        sy_amount=sy_amount,
        pt_price=pt_price,
        implied_apr_bps=implied_apr_bps,
        days_to_maturity=days_to_maturity,
        realized_yield_usd=None,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )

    # Record FIFO PT lot so PT_REDEEM can compute realized yield on the same run.
    if basis_store is not None and pt_amount is not None and sy_amount is not None and pt_amount > 0:
        pt_token_key = pt_token_sym or "PT"
        basis_store.record_pt_buy(
            deployment_id=deployment_id,
            position_key=position_key,
            pt_token=pt_token_key,
            pt_amount=pt_amount,
            sy_cost=sy_amount,
            timestamp=now,
            lot_id=identity.id,
            source_ledger_entry_id=ledger_entry_id,
        )

    return event


# ──────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────────────


def _get_field(obj: Any, field: str) -> Any:
    """Get a field from a dataclass or dict, returning None if absent."""
    if obj is None:
        return None
    if hasattr(obj, field):
        return getattr(obj, field)
    if isinstance(obj, dict):
        return obj.get(field)
    return None


def _market_from_position_key(position_key: str) -> str:
    """Extract market address from a pendle_lp position_key (last segment)."""
    if not position_key:
        return ""
    parts = position_key.split(":")
    return parts[-1] if len(parts) >= 4 else ""


# ──────────────────────────────────────────────────────────────────────────────
# Registry adapters (VIB-4163, T3)
# ──────────────────────────────────────────────────────────────────────────────

from almanak.framework.accounting.category_handlers import HandlerContext, register
from almanak.framework.primitives.types import AccountingCategory


@register(AccountingCategory.PENDLE_LP)
def _dispatch_pendle_lp(ctx: HandlerContext) -> PendleAccountingEvent | None:
    return handle_pendle_lp(ctx.outbox_row, ctx.ledger_row)


@register(AccountingCategory.PENDLE_PT)
def _dispatch_pendle_pt(ctx: HandlerContext) -> PendleAccountingEvent | None:
    return handle_pendle_pt(ctx.outbox_row, ctx.ledger_row, ctx.basis_store)
