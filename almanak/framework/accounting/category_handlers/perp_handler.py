"""Perp category handler for AccountingProcessor (VIB-3471).

Ports logic from perp_accounting.py to work from ledger_row / outbox_row dicts
rather than live intent / result objects.  No live chain calls.

Fields sourced from ledger row; confidence is ESTIMATED because realized PnL
on close requires perp receipt parser data not yet wired in.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, PerpEventType
from almanak.framework.accounting.perp_accounting import PerpAccountingEvent

logger = logging.getLogger(__name__)

_PERP_TYPES = frozenset({"PERP_OPEN", "PERP_CLOSE", "PERP_INCREASE", "PERP_DECREASE", "PERP_LIQUIDATE"})

_INTENT_TO_EVENT_TYPE: dict[str, PerpEventType] = {
    "PERP_OPEN": PerpEventType.PERP_OPEN,
    "PERP_CLOSE": PerpEventType.PERP_CLOSE,
    "PERP_INCREASE": PerpEventType.PERP_INCREASE,
    "PERP_DECREASE": PerpEventType.PERP_DECREASE,
    "PERP_LIQUIDATE": PerpEventType.PERP_LIQUIDATE,
}


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
        return d if d.is_finite() else None
    except Exception:  # noqa: BLE001
        return None


def _market_from_position_key(position_key: str) -> str:
    """Extract the market key (last ':' segment) from a position key.

    e.g. "perp:gmx_v2:arbitrum:0xwallet:eth/usd" → "eth/usd"
    """
    if not position_key:
        return ""
    return position_key.rsplit(":", 1)[-1]


def handle_perp(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
) -> PerpAccountingEvent | None:
    """Build a PerpAccountingEvent from an outbox + ledger row pair.

    Returns None for non-perp intent types.

    All inputs come from the dicts — no live chain calls.
    Confidence is ESTIMATED because realized PnL requires a perp receipt parser
    that is not yet wired.
    """
    from almanak.framework.observability.ledger import deserialize_extracted_data

    intent_type_str = (ledger_row.get("intent_type") or "").upper()
    if intent_type_str not in _PERP_TYPES:
        return None

    event_type = _INTENT_TO_EVENT_TYPE.get(intent_type_str)
    if event_type is None:
        return None

    # ── Identity fields ──────────────────────────────────────────────────────
    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    strategy_id = ledger_row.get("strategy_id") or outbox_row.get("strategy_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    chain = ledger_row.get("chain") or ""
    protocol = (ledger_row.get("protocol") or "").lower()
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""
    position_key = outbox_row.get("position_key") or ""

    # ── Timestamp ────────────────────────────────────────────────────────────
    raw_ts = ledger_row.get("timestamp")
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        timestamp = datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        timestamp = datetime.now(UTC)

    # ── Market key ───────────────────────────────────────────────────────────
    # market_id from outbox is the canonical source (set at execution time by runner).
    # Fall back to last segment of position_key.
    market = outbox_row.get("market_id") or _market_from_position_key(position_key) or ""

    # ── Token / amount ───────────────────────────────────────────────────────
    collateral_token = (ledger_row.get("token_in") or "").upper()
    collateral_amount = _safe_decimal(ledger_row.get("amount_in") or None)

    # ── Extracted data (PerpData) ─────────────────────────────────────────────
    extracted = deserialize_extracted_data(ledger_row.get("extracted_data_json") or "")
    perp_data = extracted.get("perp_data")

    size_usd: Decimal | None = None
    is_long: bool | None = None
    leverage: Decimal | None = None
    entry_price: Decimal | None = None
    realized_pnl_usd: Decimal | None = None
    funding_paid_usd: Decimal | None = None

    if perp_data is not None:
        # PerpData may expose size_delta, leverage, entry_price, realized_pnl, funding_fee_usd
        size_delta_raw = getattr(perp_data, "size_delta", None)
        if size_delta_raw is not None:
            size_usd = _safe_decimal(size_delta_raw)
        leverage = _safe_decimal(getattr(perp_data, "leverage", None))
        entry_price = _safe_decimal(getattr(perp_data, "entry_price", None))
        realized_pnl_raw = getattr(perp_data, "realized_pnl", None)
        if realized_pnl_raw is not None:
            realized_pnl_usd = _safe_decimal(realized_pnl_raw)
        funding_fee_raw = getattr(perp_data, "funding_fee_usd", None)
        if funding_fee_raw is not None:
            funding_paid_usd = _safe_decimal(funding_fee_raw)

    # ── Identity / ID ────────────────────────────────────────────────────────
    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, intent_type_str, _id_seed, position_key),
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

    return PerpAccountingEvent(
        identity=identity,
        event_type=event_type,
        position_key=position_key,
        market=market,
        collateral_token=collateral_token,
        size_usd=size_usd,
        collateral_amount=collateral_amount,
        is_long=is_long,
        leverage=leverage,
        entry_price=entry_price,
        realized_pnl_usd=realized_pnl_usd,
        funding_paid_usd=funding_paid_usd,
        confidence=AccountingConfidence.ESTIMATED,
        unavailable_reason="entry_price and realized_pnl require perp receipt parser (pending)",
    )
