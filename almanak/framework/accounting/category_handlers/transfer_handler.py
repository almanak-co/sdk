"""Transfer category handler stub (VIB-4163, T3).

T3 of the Primitives Refactor introduces ``AccountingCategory.TRANSFER`` to the
dispatch table and ships the handler stub. T4 (VIB-4164) wires the
classifier so BRIDGE intents reclassify from ``SWAP`` (its current home) to
``TRANSFER``; until T4 ships, this handler is registered but is never reached
by production traffic.

The handler emits a :class:`TransferAccountingEvent` whose
``settlement_status`` is one of ``pending`` / ``settled`` / ``failed``. The
event lands in the existing ``accounting_events.payload_json`` column — no
DDL change.

Stub semantics:

* ``settlement_status`` defaults to ``PENDING`` for the source-leg row of a
  BRIDGE-shaped transfer (T4 will refine this when destination-side observation
  is wired).
* ``confidence`` is ``ESTIMATED`` because the cross-chain settlement gap means
  we do not have ground truth on the destination side at write time.
* Token / amount / chain fields are read straight from the ledger row; missing
  fields propagate as ``None`` per the "Empty != zero" rule.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.models import (
    AccountingConfidence,
    AccountingIdentity,
    TransferAccountingEvent,
    TransferEventType,
    TransferSettlementStatus,
)

logger = logging.getLogger(__name__)


def _safe_decimal(value: Any) -> Decimal | None:
    """Best-effort Decimal conversion, returning ``None`` for empty / unparsable input."""
    if value is None or value == "":
        return None
    try:
        d = Decimal(str(value))
    except Exception:  # noqa: BLE001
        return None
    return d if d.is_finite() else None


def _parse_timestamp(raw: Any) -> datetime:
    """Parse the ledger timestamp, falling back to ``datetime.now(UTC)`` only if parsing fails.

    A malformed timestamp is observable: a WARNING is emitted before the fallback
    so audit / debugging surfaces can detect "ledger row had a bad timestamp"
    vs the expected absent-timestamp case (raw is None / empty — no warning).
    """
    if isinstance(raw, str) and raw:
        try:
            return datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            logger.warning(
                "handle_transfer: malformed ledger timestamp %r; falling back to datetime.now(UTC)",
                raw,
            )
    return datetime.now(UTC)


def handle_transfer(
    outbox_row: dict[str, Any],
    ledger_row: dict[str, Any],
) -> TransferAccountingEvent:
    """Build a :class:`TransferAccountingEvent` from an outbox + ledger row pair.

    All inputs are read from the ledger row — no live chain calls. **Always
    returns** a :class:`TransferAccountingEvent`; when the ledger row did not
    record a usable asset / amount, the event is emitted with
    ``confidence=UNAVAILABLE`` and a populated ``unavailable_reason`` so an
    auditor can distinguish "never measured" from "measured zero" per the
    "Empty != zero" rule (blueprints/27-accounting.md).
    """

    # ── Identity fields ──────────────────────────────────────────────────────
    deployment_id = ledger_row.get("deployment_id") or outbox_row.get("deployment_id") or ""
    strategy_id = ledger_row.get("strategy_id") or outbox_row.get("strategy_id") or ""
    cycle_id = ledger_row.get("cycle_id") or outbox_row.get("cycle_id") or ""
    execution_mode = ledger_row.get("execution_mode") or ""
    chain = ledger_row.get("chain") or ""
    protocol = ledger_row.get("protocol") or ""
    tx_hash = ledger_row.get("tx_hash") or ""
    ledger_entry_id = ledger_row.get("id") or ""
    wallet_address = outbox_row.get("wallet_address") or ""
    position_key = outbox_row.get("position_key") or ""

    # ── Transfer subject ─────────────────────────────────────────────────────
    asset = (ledger_row.get("token_in") or "").upper()
    amount = _safe_decimal(ledger_row.get("amount_in"))
    # Cross-chain shape: BRIDGE intents land destination_chain in the ledger row.
    # When the field is absent (single-chain transfer), default to the source chain.
    source_chain = chain
    destination_chain = ledger_row.get("destination_chain") or chain
    gas_usd = _safe_decimal(ledger_row.get("gas_usd"))

    # ── Settlement status ────────────────────────────────────────────────────
    # Source-leg writes are PENDING until T4 wires destination-observation.
    # A future ``settlement_status`` override on the ledger row (e.g. backfill
    # from a destination listener) is honored when present.
    raw_status = ledger_row.get("settlement_status")
    if isinstance(raw_status, str) and raw_status:
        try:
            settlement_status = TransferSettlementStatus(raw_status)
        except ValueError:
            logger.warning(
                "handle_transfer: invalid settlement_status %r in ledger row %s; defaulting to PENDING",
                raw_status,
                ledger_entry_id,
            )
            settlement_status = TransferSettlementStatus.PENDING
    else:
        settlement_status = TransferSettlementStatus.PENDING

    # ── USD valuation ────────────────────────────────────────────────────────
    # Stub: we don't price transfers in T3. T4 / Accounting rework will plumb
    # price_inputs_json through the same _price_helpers pipeline used by SWAP.
    # ``None`` (not Decimal(0)) preserves "Empty != zero".
    amount_usd: Decimal | None = None

    # ── Confidence ───────────────────────────────────────────────────────────
    if not asset or amount is None:
        confidence = AccountingConfidence.UNAVAILABLE
        unavailable_reason = "transfer asset or amount missing in ledger row"
    elif settlement_status == TransferSettlementStatus.PENDING:
        confidence = AccountingConfidence.ESTIMATED
        unavailable_reason = "transfer pending destination-side settlement"
    else:
        confidence = AccountingConfidence.HIGH
        unavailable_reason = ""

    # ── Event identity ───────────────────────────────────────────────────────
    timestamp = _parse_timestamp(ledger_row.get("timestamp"))
    _id_seed = tx_hash or ledger_entry_id
    _id_suffix = f"{asset}_{source_chain}_{destination_chain}"
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, "TRANSFER", _id_seed, _id_suffix),
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

    return TransferAccountingEvent(
        identity=identity,
        event_type=TransferEventType.TRANSFER,
        position_key=position_key,
        asset=asset,
        amount=amount,
        amount_usd=amount_usd,
        source_chain=source_chain,
        destination_chain=destination_chain,
        settlement_status=settlement_status,
        gas_usd=gas_usd,
        confidence=confidence,
        unavailable_reason=unavailable_reason,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Registry adapter (VIB-4163, T3)
# ──────────────────────────────────────────────────────────────────────────────

from almanak.framework.accounting.category_handlers import HandlerContext, register
from almanak.framework.primitives.types import AccountingCategory


@register(AccountingCategory.TRANSFER)
def _dispatch_transfer(ctx: HandlerContext) -> TransferAccountingEvent:
    return handle_transfer(ctx.outbox_row, ctx.ledger_row)
