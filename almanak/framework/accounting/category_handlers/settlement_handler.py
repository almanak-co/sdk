"""Settlement category handler for AccountingProcessor (VIB-5666).

Builds a :class:`~almanak.framework.accounting.settlement_accounting.SettlementAccountingEvent`
from a ``ledger_row`` / ``outbox_row`` pair for a vault SETTLE_DEPOSIT /
SETTLE_REDEEM tx. No live chain calls — every input is read off the two dicts,
exactly like every other category handler. The settlement outputs (assets /
shares deltas, post-settle NAV, fee-shares, USD) are pre-computed in HUMAN units
by the runner-owned settlement-commit pipeline
(:func:`almanak.framework.runner.settlement_commit.commit_settlement_intent`) and
stamped onto ``extracted_data_json`` under the ``"settlement"`` key, so this
handler stays a pure dict→event transform.

Capital-event discipline: the event carries only assets/shares magnitudes and
NAV — never ``principal_delta_usd`` / ``realized_pnl_usd`` — so no PnL fold reads
a depositor deposit as profit or a redemption as loss.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, SettlementEventType
from almanak.framework.accounting.settlement_accounting import SettlementAccountingEvent

logger = logging.getLogger(__name__)

_SETTLEMENT_TYPES = frozenset({"SETTLE_DEPOSIT", "SETTLE_REDEEM"})

_INTENT_TO_EVENT_TYPE: dict[str, SettlementEventType] = {
    "SETTLE_DEPOSIT": SettlementEventType.SETTLE_DEPOSIT,
    "SETTLE_REDEEM": SettlementEventType.SETTLE_REDEEM,
}


def _safe_decimal(value: Any) -> Decimal | None:
    """Parse a stamped money value to Decimal, honouring Empty ≠ Zero.

    ``None`` (unmeasured) and ``""`` (parser-absent) both map to ``None`` — never
    to ``Decimal("0")``. A malformed value also maps to ``None`` (unmeasured)
    rather than fabricating a measured zero.
    """
    if value is None or value == "":
        return None
    try:
        d = Decimal(str(value))
        return d if d.is_finite() else None
    except Exception:  # noqa: BLE001
        return None


def _settlement_from_extracted(ledger_row: dict[str, Any]) -> dict[str, Any]:
    """Read the ``settlement`` sub-object stamped on ``extracted_data_json``.

    Returns ``{}`` when absent / unparseable so the handler degrades to an
    all-unmeasured event (Empty ≠ Zero) rather than crashing the drain.
    """
    raw = ledger_row.get("extracted_data_json")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    settlement = parsed.get("settlement")
    return settlement if isinstance(settlement, dict) else {}


def handle_settlement(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
) -> SettlementAccountingEvent | None:
    """Build a SettlementAccountingEvent from an outbox + ledger row pair.

    Returns None for non-settlement intent types (defensive — the dispatcher
    only routes ``AccountingCategory.SETTLEMENT`` here, but a mis-stamped ledger
    row must degrade to "no event", never crash).
    """
    intent_type_str = (ledger_row.get("intent_type") or "").upper()
    if intent_type_str not in _SETTLEMENT_TYPES:
        return None
    event_type = _INTENT_TO_EVENT_TYPE.get(intent_type_str)
    if event_type is None:
        return None

    # ── Identity fields (canonical ledger/outbox row shape) ──────────────────
    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    chain = ledger_row.get("chain") or ""
    protocol = (ledger_row.get("protocol") or "").lower()
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""
    position_key = outbox_row.get("position_key") or ""
    # vault address: outbox market_id is canonical (stamped at commit time);
    # fall back to the last ':' segment of the settlement position key.
    vault_address = outbox_row.get("market_id") or (position_key.rsplit(":", 1)[-1] if position_key else "") or ""
    asset_token = (ledger_row.get("token_in") or "").upper()

    # ── Timestamp ────────────────────────────────────────────────────────────
    raw_ts = ledger_row.get("timestamp")
    try:
        ts_str = raw_ts.replace("Z", "+00:00") if isinstance(raw_ts, str) else None
        timestamp = datetime.fromisoformat(ts_str) if ts_str else datetime.now(UTC)
    except (ValueError, AttributeError):
        timestamp = datetime.now(UTC)

    # ── Settlement outputs (HUMAN units, pre-computed by the commit pipeline) ─
    settlement = _settlement_from_extracted(ledger_row)
    assets_delta = _safe_decimal(settlement.get("assets"))
    shares_delta = _safe_decimal(settlement.get("shares"))
    new_total_assets = _safe_decimal(settlement.get("new_total_assets"))
    fee_shares = _safe_decimal(settlement.get("fee_shares"))
    assets_usd = _safe_decimal(settlement.get("assets_usd"))
    epoch_raw = settlement.get("epoch_id")
    try:
        epoch_id = int(epoch_raw) if epoch_raw is not None else None
    except (TypeError, ValueError):
        epoch_id = None

    # ── Confidence (Empty ≠ Zero audit trail) ────────────────────────────────
    # HIGH only when the two receipt-measured deltas are both present; otherwise
    # ESTIMATED with a reason (mirrors the SWAP handler's discipline and the
    # _Versioned confidence-exclusivity invariant: HIGH ⇔ no unavailable_reason).
    missing: list[str] = []
    if assets_delta is None:
        missing.append("assets_delta")
    if shares_delta is None:
        missing.append("shares_delta")
    if missing:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = f"settlement receipt did not yield {', '.join(missing)}"
    else:
        confidence = AccountingConfidence.HIGH
        unavailable_reason = ""

    _id_seed = tx_hash or ledger_entry_id or position_key
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, intent_type_str, _id_seed, position_key),
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

    return SettlementAccountingEvent(
        identity=identity,
        event_type=event_type,
        position_key=position_key,
        vault_address=vault_address,
        asset_token=asset_token,
        assets_delta=assets_delta,
        shares_delta=shares_delta,
        new_total_assets=new_total_assets,
        fee_shares=fee_shares,
        assets_usd=assets_usd,
        epoch_id=epoch_id,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Registry adapter (VIB-4163 pattern)
# ──────────────────────────────────────────────────────────────────────────────

from almanak.framework.accounting.category_handlers import HandlerContext, register
from almanak.framework.primitives.types import AccountingCategory


@register(AccountingCategory.SETTLEMENT)
def _dispatch_settlement(ctx: HandlerContext) -> SettlementAccountingEvent | None:
    return handle_settlement(ctx.outbox_row, ctx.ledger_row)
