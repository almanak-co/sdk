"""Perp accounting event builder for GMX V2, Gains Network / bb_perps (VIB-3516).

Records PERP_OPEN and PERP_CLOSE events to accounting_events.
Fields sourced from PerpOpenIntent / PerpCloseIntent; confidence is ESTIMATED
because realized PnL on close requires on-chain state that is not yet extracted
from receipt data.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import uuid4

from almanak.framework.accounting.ids import make_accounting_event_id
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, PerpEventType

logger = logging.getLogger(__name__)

_PERP_OPEN_CLOSE_TYPES = frozenset({"PERP_OPEN", "PERP_CLOSE"})


class PerpAccountingEvent:
    """Duck-typed perp accounting event consumed by AccountingWriter and both backends."""

    schema_version: int = 1
    # VIB-4166 (T6) — see ``almanak.framework.accounting.payload_schemas`` module
    # docstring for the bump policy. Class attribute so the augment chokepoint
    # has a sane fallback when writers don't override it; the chokepoint
    # overwrites with the canonical per-primitive value at write time.
    primitive_version: int = 1

    def __init__(
        self,
        identity: AccountingIdentity,
        event_type: PerpEventType,
        position_key: str,
        market: str,
        collateral_token: str,
        size_usd: Decimal | None,
        collateral_amount: Decimal | None,
        is_long: bool | None,
        leverage: Decimal | None,
        entry_price: Decimal | None,
        realized_pnl_usd: Decimal | None,
        funding_paid_usd: Decimal | None,
        confidence: AccountingConfidence,
        unavailable_reason: str = "",
    ) -> None:
        self.identity = identity
        self.event_type = event_type.value
        self.position_key = position_key
        self.market = market
        self.collateral_token = collateral_token
        self.size_usd = size_usd
        self.collateral_amount = collateral_amount
        self.is_long = is_long
        self.leverage = leverage
        self.entry_price = entry_price
        self.realized_pnl_usd = realized_pnl_usd
        self.funding_paid_usd = funding_paid_usd
        self.confidence = confidence
        self.unavailable_reason = unavailable_reason

    def to_payload_json(self) -> str:
        def _enc(v: Any) -> Any:
            if isinstance(v, Decimal):
                return str(v)
            return v

        return json.dumps(
            {
                "event_type": self.event_type,
                "position_key": self.position_key,
                "market": self.market,
                "collateral_token": self.collateral_token,
                "size_usd": _enc(self.size_usd),
                "collateral_amount": _enc(self.collateral_amount),
                "is_long": self.is_long,
                "leverage": _enc(self.leverage),
                "entry_price": _enc(self.entry_price),
                "realized_pnl_usd": _enc(self.realized_pnl_usd),
                "funding_paid_usd": _enc(self.funding_paid_usd),
                "confidence": str(self.confidence),
                "unavailable_reason": self.unavailable_reason,
                "schema_version": self.schema_version,
                "primitive_version": self.primitive_version,
            }
        )


def _intent_type_str(intent: Any) -> str:
    it = getattr(intent, "intent_type", None)
    if it is None:
        return ""
    return it.value if hasattr(it, "value") else str(it)


def _safe_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        d = Decimal(str(value))
        return d if d.is_finite() else None
    except Exception:  # noqa: BLE001
        return None


def build_perp_accounting_event(
    *,
    intent: Any,
    result: Any,
    deployment_id: str,
    strategy_id: str,
    cycle_id: str,
    execution_mode: str,
    chain: str,
    wallet_address: str,
    ledger_entry_id: str | None = None,
) -> PerpAccountingEvent | None:
    """Build a PerpAccountingEvent for a completed PERP_OPEN or PERP_CLOSE intent.

    Returns None for non-perp intents.

    Fields come from the intent (market, collateral_token, size_usd, is_long, leverage).
    Realized PnL on close is not yet extractable from receipt data; it is left as None
    (confidence ESTIMATED) until a perp receipt parser adds structured close data.
    """
    intent_type_str = _intent_type_str(intent)
    if intent_type_str not in _PERP_OPEN_CLOSE_TYPES:
        return None

    event_type = PerpEventType.PERP_OPEN if intent_type_str == "PERP_OPEN" else PerpEventType.PERP_CLOSE
    protocol = (getattr(intent, "protocol", "") or "").lower()
    now = datetime.now(UTC)

    tx_hash = getattr(result, "tx_hash", None) or ""
    if not tx_hash:
        for tr in getattr(result, "transaction_results", None) or []:
            h = getattr(tr, "tx_hash", None)
            if h:
                tx_hash = h
                break

    market = str(getattr(intent, "market", None) or "")
    collateral_token = str(getattr(intent, "collateral_token", None) or "")
    size_usd = _safe_decimal(getattr(intent, "size_usd", None))
    collateral_amount = _safe_decimal(getattr(intent, "collateral_amount", None))
    is_long = getattr(intent, "is_long", None)
    leverage = _safe_decimal(getattr(intent, "leverage", None))

    # Normalize market to a stable key component (strip 0x prefix for readability)
    market_key = market.lower().replace(" ", "_")

    position_key = f"perp:{protocol}:{chain.lower()}:{wallet_address.lower()}:{market_key}"

    _id_seed = tx_hash or ledger_entry_id or str(uuid4())
    identity = AccountingIdentity(
        id=make_accounting_event_id(deployment_id, cycle_id, event_type.value, _id_seed, position_key),
        deployment_id=deployment_id,
        strategy_id=strategy_id,
        cycle_id=cycle_id,
        execution_mode=execution_mode,
        timestamp=now,
        chain=chain,
        protocol=protocol,
        wallet_address=wallet_address,
        tx_hash=tx_hash,
        ledger_entry_id=ledger_entry_id or "",
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
        entry_price=None,
        realized_pnl_usd=None,
        funding_paid_usd=None,
        confidence=AccountingConfidence.ESTIMATED,
        unavailable_reason="entry_price and realized_pnl require perp receipt parser (pending)",
    )
