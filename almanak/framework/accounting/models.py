"""Typed accounting event models.

Design rules:
- None = unmeasured / unavailable. Decimal("0") = measured zero. Never conflate.
- Every event carries AccountingIdentity for full traceability.
- schema_version on every model so formula upgrades don't corrupt old records.
- All monetary fields use Decimal or string — never float.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any


class AccountingConfidence(StrEnum):
    HIGH = "HIGH"
    ESTIMATED = "ESTIMATED"
    STALE = "STALE"
    UNAVAILABLE = "UNAVAILABLE"


class LendingEventType(StrEnum):
    SUPPLY = "SUPPLY"
    BORROW = "BORROW"
    REPAY = "REPAY"
    DELEVERAGE = "DELEVERAGE"
    WITHDRAW = "WITHDRAW"
    CLOSE = "CLOSE"
    LIQUIDATION_RISK_UPDATE = "LIQUIDATION_RISK_UPDATE"


class PendleEventType(StrEnum):
    PT_BUY = "PT_BUY"
    PT_SELL = "PT_SELL"
    PT_REDEEM = "PT_REDEEM"
    LP_OPEN = "LP_OPEN"
    LP_CLOSE = "LP_CLOSE"


@dataclass
class AccountingIdentity:
    id: str
    deployment_id: str
    strategy_id: str
    cycle_id: str
    execution_mode: str
    timestamp: datetime
    chain: str
    protocol: str
    wallet_address: str
    tx_hash: str
    ledger_entry_id: str

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["timestamp"] = self.timestamp.isoformat()
        return d


@dataclass
class LendingAccountingEvent:
    identity: AccountingIdentity
    event_type: LendingEventType
    position_key: str
    market_id: str
    asset: str

    collateral_value_before_usd: Decimal | None
    collateral_value_after_usd: Decimal | None
    debt_value_before_usd: Decimal | None
    debt_value_after_usd: Decimal | None
    net_equity_before_usd: Decimal | None
    net_equity_after_usd: Decimal | None

    health_factor_before: Decimal | None
    health_factor_after: Decimal | None
    liquidation_threshold: Decimal | None
    lltv: Decimal | None

    supply_apr_bps: int | None
    borrow_apr_bps: int | None

    principal_delta_usd: Decimal | None
    interest_delta_usd: Decimal | None
    gas_usd: Decimal | None

    confidence: AccountingConfidence
    unavailable_reason: str = ""
    schema_version: int = 1

    def to_payload_json(self) -> str:
        def _enc(v: Any) -> Any:
            if isinstance(v, Decimal):
                return str(v)
            if isinstance(v, AccountingConfidence | LendingEventType):
                return v.value
            return v

        d = {
            "event_type": self.event_type.value,
            "position_key": self.position_key,
            "market_id": self.market_id,
            "asset": self.asset,
            "collateral_value_before_usd": _enc(self.collateral_value_before_usd),
            "collateral_value_after_usd": _enc(self.collateral_value_after_usd),
            "debt_value_before_usd": _enc(self.debt_value_before_usd),
            "debt_value_after_usd": _enc(self.debt_value_after_usd),
            "net_equity_before_usd": _enc(self.net_equity_before_usd),
            "net_equity_after_usd": _enc(self.net_equity_after_usd),
            "health_factor_before": _enc(self.health_factor_before),
            "health_factor_after": _enc(self.health_factor_after),
            "liquidation_threshold": _enc(self.liquidation_threshold),
            "lltv": _enc(self.lltv),
            "supply_apr_bps": self.supply_apr_bps,
            "borrow_apr_bps": self.borrow_apr_bps,
            "principal_delta_usd": _enc(self.principal_delta_usd),
            "interest_delta_usd": _enc(self.interest_delta_usd),
            "gas_usd": _enc(self.gas_usd),
            "confidence": self.confidence.value,
            "unavailable_reason": self.unavailable_reason,
            "schema_version": self.schema_version,
        }
        return json.dumps(d)

    @classmethod
    def from_payload_json(cls, identity: AccountingIdentity, payload: str) -> LendingAccountingEvent:
        d = json.loads(payload)

        def _dec(v: Any) -> Decimal | None:
            return Decimal(v) if v is not None else None

        return cls(
            identity=identity,
            event_type=LendingEventType(d["event_type"]),
            position_key=d["position_key"],
            market_id=d["market_id"],
            asset=d["asset"],
            collateral_value_before_usd=_dec(d.get("collateral_value_before_usd")),
            collateral_value_after_usd=_dec(d.get("collateral_value_after_usd")),
            debt_value_before_usd=_dec(d.get("debt_value_before_usd")),
            debt_value_after_usd=_dec(d.get("debt_value_after_usd")),
            net_equity_before_usd=_dec(d.get("net_equity_before_usd")),
            net_equity_after_usd=_dec(d.get("net_equity_after_usd")),
            health_factor_before=_dec(d.get("health_factor_before")),
            health_factor_after=_dec(d.get("health_factor_after")),
            liquidation_threshold=_dec(d.get("liquidation_threshold")),
            lltv=_dec(d.get("lltv")),
            supply_apr_bps=d.get("supply_apr_bps"),
            borrow_apr_bps=d.get("borrow_apr_bps"),
            principal_delta_usd=_dec(d.get("principal_delta_usd")),
            interest_delta_usd=_dec(d.get("interest_delta_usd")),
            gas_usd=_dec(d.get("gas_usd")),
            confidence=AccountingConfidence(d["confidence"]),
            unavailable_reason=d.get("unavailable_reason", ""),
            schema_version=d.get("schema_version", 1),
        )


@dataclass
class PendleAccountingEvent:
    identity: AccountingIdentity
    event_type: PendleEventType
    position_key: str
    market_id: str
    pt_token: str
    maturity_timestamp: datetime | None

    pt_amount: Decimal | None
    sy_amount: Decimal | None
    pt_price: Decimal | None
    implied_apr_bps: int | None
    days_to_maturity: int | None
    realized_yield_usd: Decimal | None
    basis_lot_id: str | None = None

    confidence: AccountingConfidence = AccountingConfidence.HIGH
    unavailable_reason: str = ""
    schema_version: int = 1

    def to_payload_json(self) -> str:
        def _enc(v: Any) -> Any:
            if isinstance(v, Decimal):
                return str(v)
            if isinstance(v, datetime):
                return v.isoformat()
            if isinstance(v, AccountingConfidence | PendleEventType):
                return v.value
            return v

        d = {
            "event_type": self.event_type.value,
            "position_key": self.position_key,
            "market_id": self.market_id,
            "pt_token": self.pt_token,
            "maturity_timestamp": _enc(self.maturity_timestamp),
            "pt_amount": _enc(self.pt_amount),
            "sy_amount": _enc(self.sy_amount),
            "pt_price": _enc(self.pt_price),
            "implied_apr_bps": self.implied_apr_bps,
            "days_to_maturity": self.days_to_maturity,
            "realized_yield_usd": _enc(self.realized_yield_usd),
            "basis_lot_id": self.basis_lot_id,
            "confidence": self.confidence.value,
            "unavailable_reason": self.unavailable_reason,
            "schema_version": self.schema_version,
        }
        return json.dumps(d)

    @classmethod
    def from_payload_json(cls, identity: AccountingIdentity, payload: str) -> PendleAccountingEvent:
        d = json.loads(payload)

        def _dec(v: Any) -> Decimal | None:
            return Decimal(v) if v is not None else None

        def _dt(v: Any) -> datetime | None:
            return datetime.fromisoformat(v) if v is not None else None

        return cls(
            identity=identity,
            event_type=PendleEventType(d["event_type"]),
            position_key=d["position_key"],
            market_id=d["market_id"],
            pt_token=d["pt_token"],
            maturity_timestamp=_dt(d.get("maturity_timestamp")),
            pt_amount=_dec(d.get("pt_amount")),
            sy_amount=_dec(d.get("sy_amount")),
            pt_price=_dec(d.get("pt_price")),
            implied_apr_bps=d.get("implied_apr_bps"),
            days_to_maturity=d.get("days_to_maturity"),
            realized_yield_usd=_dec(d.get("realized_yield_usd")),
            basis_lot_id=d.get("basis_lot_id"),
            confidence=AccountingConfidence(d.get("confidence", AccountingConfidence.HIGH)),
            unavailable_reason=d.get("unavailable_reason", ""),
            schema_version=d.get("schema_version", 1),
        )


@dataclass
class BorrowLot:
    """A single borrow lot for FIFO interest matching."""

    lot_id: str
    deployment_id: str
    position_key: str
    borrow_timestamp: datetime
    principal_amount: Decimal
    principal_usd: Decimal
    token: str
    chain: str
    protocol: str
    market_id: str
    remaining_principal: Decimal = field(init=False)

    def __post_init__(self) -> None:
        self.remaining_principal = self.principal_amount
