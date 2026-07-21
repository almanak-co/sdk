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
from almanak.framework.accounting.measured import encode_money_payload
from almanak.framework.accounting.models import AccountingConfidence, AccountingIdentity, PerpEventType

logger = logging.getLogger(__name__)

_PERP_OPEN_CLOSE_TYPES = frozenset({"PERP_OPEN", "PERP_CLOSE"})

# VIB-5717: the perp receipt parser that would measure entry/exit price and
# realized PnL is not yet wired, so every perp event is ESTIMATED with this
# reason. VIB-5941: when ``size`` itself is unmeasured (a full close declares no
# size — see ``PerpCloseIntent``), the reason must NAME the missing size too, so
# the audit trail explains the ``size=None`` the ``PerpCloseEventPayload``
# validator requires a reason for.
_BASE_UNAVAILABLE_REASON = "entry_price and realized_pnl require perp receipt parser (pending)"
_SIZE_UNAVAILABLE_CLAUSE = "; position size requires perp receipt parser (pending)"


def perp_unavailable_reason(size_usd: Decimal | None) -> str:
    """The ESTIMATED ``unavailable_reason`` for a perp event, size-aware (VIB-5941).

    Names the missing size when ``size_usd`` is unmeasured (a full close declares
    no size) so the ``PerpCloseEventPayload`` ``size=None`` state is self-explained
    rather than "covered" by a reason that only mentions entry/realized PnL.
    """
    if size_usd is None:
        return _BASE_UNAVAILABLE_REASON + _SIZE_UNAVAILABLE_CLAUSE
    return _BASE_UNAVAILABLE_REASON


class PerpAccountingEvent:
    """Duck-typed perp accounting event consumed by AccountingWriter and both backends."""

    schema_version: int = 1
    # VIB-4166 (T6) — see ``almanak.framework.accounting.payload_schemas`` module
    # docstring for the bump policy. Class attribute so the augment chokepoint
    # has a sane fallback when writers don't override it; the chokepoint
    # overwrites with the canonical per-primitive value
    # (``PRIMITIVE_VERSIONS[Primitive.PERP]``) at write time.
    # VIB-5941 (v1→v2): kept in lock-step with ``PRIMITIVE_VERSIONS[Primitive.PERP]``
    # so the unaugmented ``to_payload_json`` fallback (tests / debug) matches the
    # production augment stamp — the perp payload field-set changed (size_usd→size,
    # intent-known is_long, nullable close-size + unavailable_reason invariant).
    primitive_version: int = 2

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
        # VIB-5724 — venue-observed truth vs the intent's request. Optional +
        # default None so this is an additive, back-compatible payload extension
        # (Empty ≠ Zero: an unmeasured venue read stays None, never defaulted).
        venue_leverage: Decimal | None = None,
        venue_margin_mode: str | None = None,
        requested_leverage: Decimal | None = None,
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
        # VIB-5724 — the LEVERAGE the venue actually applied ("cross"/"isolated"
        # margin mode too), observed on-venue. ``leverage`` (above) already
        # carries the venue value for CoreWriter perps; these expose it
        # explicitly plus the margin mode, and ``requested_leverage`` keeps the
        # intent's request as metadata for divergence forensics — never as venue
        # truth.
        self.venue_leverage = venue_leverage
        self.venue_margin_mode = venue_margin_mode
        self.requested_leverage = requested_leverage

    def to_payload_json(self) -> str:
        def _enc(v: Any) -> Any:
            if isinstance(v, Decimal):
                # VIB-5213 (US-007): money crosses the serialization seam as a
                # MeasuredMoney. Byte-identical to ``str(v)`` for finite Decimals.
                return encode_money_payload(v)
            return v

        return json.dumps(
            {
                "event_type": self.event_type,
                "position_key": self.position_key,
                "market": self.market,
                "collateral_token": self.collateral_token,
                # VIB-5941: the frozen ``PerpOpenEventPayload`` /
                # ``PerpCloseEventPayload`` schema names this field ``size`` (a
                # required Decimal). It is the position's USD notional — the
                # intent's ``size_usd`` — carried under the canonical schema key;
                # emitting it as ``size_usd`` left ``size`` absent and FAILed
                # Pydantic validation, silently blocking G6 / G13 / P3 / P5.
                "size": _enc(self.size_usd),
                "collateral_amount": _enc(self.collateral_amount),
                "is_long": self.is_long,
                "leverage": _enc(self.leverage),
                "entry_price": _enc(self.entry_price),
                "realized_pnl_usd": _enc(self.realized_pnl_usd),
                "funding_paid_usd": _enc(self.funding_paid_usd),
                # VIB-5724 — venue-observed leverage / margin mode + the intent's
                # requested leverage (metadata). Emitted only when measured so
                # unaugmented readers see null rather than a fabricated value.
                "venue_leverage": _enc(self.venue_leverage),
                "venue_margin_mode": self.venue_margin_mode,
                "requested_leverage": _enc(self.requested_leverage),
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
        unavailable_reason=perp_unavailable_reason(size_usd),
    )
