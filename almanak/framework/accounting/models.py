"""Typed accounting event models.

Design rules:
- None = unmeasured / unavailable. Decimal("0") = measured zero. Never conflate.
- Every event carries AccountingIdentity for full traceability.
- schema_version on every model so formula upgrades don't corrupt old records.
- primitive_version on every model so per-primitive contract changes (set of
  fields, lifecycle states, financial invariants) can be tracked independently
  from the lot-matching algorithm version. See
  ``almanak.framework.accounting.payload_schemas`` module docstring for the
  bump policy: bump ONLY on a primitive-semantics change (NOT classifier
  tweaks, parser fixes, or new event_type strings within an existing
  primitive). Stamping defaults to 1 now (VIB-4166, T6 of VIB-4160) — pre-T6
  rows on disk lack the field; the read rail tolerates absence via
  ``_Versioned.extra='ignore'``, and the augment chokepoint stamps the
  canonical per-primitive value at write time.
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
    PENDLE_LP_OPEN = "PENDLE_LP_OPEN"
    PENDLE_LP_CLOSE = "PENDLE_LP_CLOSE"


class LPEventType(StrEnum):
    LP_OPEN = "LP_OPEN"
    LP_CLOSE = "LP_CLOSE"
    LP_COLLECT_FEES = "LP_COLLECT_FEES"
    LP_SNAPSHOT = "LP_SNAPSHOT"
    LP_REBALANCE = "LP_REBALANCE"


class PerpEventType(StrEnum):
    PERP_OPEN = "PERP_OPEN"
    PERP_CLOSE = "PERP_CLOSE"
    PERP_INCREASE = "PERP_INCREASE"
    PERP_DECREASE = "PERP_DECREASE"
    PERP_LIQUIDATE = "PERP_LIQUIDATE"


class VaultEventType(StrEnum):
    VAULT_DEPOSIT = "VAULT_DEPOSIT"
    VAULT_WITHDRAW = "VAULT_WITHDRAW"
    VAULT_HARVEST = "VAULT_HARVEST"
    VAULT_SNAPSHOT = "VAULT_SNAPSHOT"


class SwapEventType(StrEnum):
    SWAP = "SWAP"


class PredictionEventType(StrEnum):
    """Lifecycle events for prediction-market positions (VIB-3707).

    PREDICTION_OPEN  — first BUY for a (market_id, outcome) — opens the position.
    PREDICTION_INCREASE — subsequent BUY on an existing position (averaging up).
    PREDICTION_REDUCE — partial SELL (position size > 0 after).
    PREDICTION_CLOSE — full SELL that zeros the position.
    PREDICTION_REDEEM — on-chain CTF redemption after market resolution; always closes.
    """

    PREDICTION_OPEN = "PREDICTION_OPEN"
    PREDICTION_INCREASE = "PREDICTION_INCREASE"
    PREDICTION_REDUCE = "PREDICTION_REDUCE"
    PREDICTION_CLOSE = "PREDICTION_CLOSE"
    PREDICTION_REDEEM = "PREDICTION_REDEEM"


class TransferEventType(StrEnum):
    """Lifecycle event for value transfer / bridge primitives.

    Introduced by T3 of the Primitives Refactor (VIB-4163) as a registered
    but unreached handler stub. T4 (VIB-4164) wired the BRIDGE classification
    path: ``classify("BRIDGE") == AccountingCategory.TRANSFER`` and the
    payload-only ``TAXONOMY['TRANSFER']`` row resolves the BRIDGE primitive
    for the writer's augment chokepoint, so this event appears in production
    data for every BRIDGE intent.
    """

    TRANSFER = "TRANSFER"


class TransferSettlementStatus(StrEnum):
    """Settlement state of a TRANSFER accounting event (VIB-4163).

    BRIDGE intents in particular settle asynchronously across two chains,
    so a single TRANSFER event may be written PENDING (source-side leg
    landed; destination-side not observed yet), advanced to SETTLED when
    the destination-side credit is observed, or marked FAILED when the
    bridge guarantees the value will not arrive.
    """

    PENDING = "pending"
    SETTLED = "settled"
    FAILED = "failed"


# Union of all valid accounting event type strings — used by the gateway
# whitelist (state_service.py) and the AccountingProcessor classifier.
#
# VIB-4164 (T4): ``TransferEventType`` is now included. The three changes —
# (a) BRIDGE → AccountingCategory.TRANSFER in primitives/taxonomy.py,
# (b) payload-only ``"TRANSFER"`` row in the same TAXONOMY table,
# (c) ``TransferEventType`` in this whitelist —
# landed atomically in PR for VIB-4164 so the gateway accepts
# ``event_type='TRANSFER'`` only after the writer's augment chokepoint can
# resolve `record_for("TRANSFER").primitive` and stamp the correct
# `matching_policy_version`. Reverting any one of the three legs without the
# others fails ``test_atomic_bridge_transfer_alignment``.
ALL_ACCOUNTING_EVENT_TYPES: frozenset[str] = frozenset(
    e.value
    for cls in (
        LendingEventType,
        PendleEventType,
        LPEventType,
        PerpEventType,
        VaultEventType,
        SwapEventType,
        PredictionEventType,
        TransferEventType,
    )
    for e in cls
)


@dataclass
class AccountingIdentity:
    id: str
    deployment_id: str
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


# Supported payload schema versions for accounting event models.
# When bumping schema_version on a model, add the new integer here.
# Records stored with an unknown version are rejected at read time to prevent
# silent payload drift from corrupting downstream consumers.
SUPPORTED_SCHEMA_VERSIONS: frozenset[int] = frozenset({1})


def _validate_schema_version(data: dict[str, Any], model_name: str) -> int:
    """Return the schema_version from *data*, defaulting to 1 for legacy records.

    Raises ``ValueError`` if the version is not a plain ``int`` (rejects bool/float
    edge cases: Python equality means ``True in {1}`` and ``1.0 in {1}`` are both
    ``True``, so we must type-check first) or is not in ``SUPPORTED_SCHEMA_VERSIONS``.
    """
    version = data.get("schema_version", 1)
    if type(version) is not int:
        raise ValueError(
            f"Invalid schema_version type {type(version).__name__!r} in {model_name} payload"
            f"; expected int, got {version!r}"
        )
    if version not in SUPPORTED_SCHEMA_VERSIONS:
        supported = sorted(SUPPORTED_SCHEMA_VERSIONS)
        raise ValueError(f"Unsupported schema_version {version!r} in {model_name} payload — supported: {supported}")
    return version


def _validate_primitive_version(data: dict[str, Any], model_name: str) -> int:
    """Return primitive_version from *data*, defaulting to 1 for legacy records.

    Mirrors :func:`_validate_schema_version`'s strict type discipline (VIB-4166):
    rejects bool subclass (``True == 1`` in Python), floats, and stringified
    ints. Production exposure on the read rail is bounded because
    :func:`writer.augment_accounting_payload` is authoritative at write time
    and overwrites any tampered value — but if a future PR introduces a read
    path that bypasses the augment, strict validation here surfaces the
    drift loudly instead of silently coercing.
    """
    version = data.get("primitive_version", 1)
    if type(version) is not int:
        raise ValueError(
            f"Invalid primitive_version type {type(version).__name__!r} in {model_name} payload"
            f"; expected int, got {version!r}"
        )
    if version < 1:
        raise ValueError(f"Invalid primitive_version {version!r} in {model_name} payload; expected int >= 1")
    return version


def _dec(v: Any) -> Decimal | None:
    """Convert a string/number value to Decimal, or return None."""
    return Decimal(v) if v is not None else None


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

    # Raw token amount (human-decimal) — required for FIFO basis reconstruction on restart.
    # None when the amount could not be derived from the execution result.
    amount_token: Decimal | None = None

    confidence: AccountingConfidence = AccountingConfidence.HIGH
    unavailable_reason: str = ""
    schema_version: int = 1
    # VIB-4166 (T6) — see module docstring for bump policy.
    primitive_version: int = 1

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
            "amount_token": _enc(self.amount_token),
            "confidence": self.confidence.value,
            # VIB-3938 — see LPAccountingEvent.to_payload_json for rationale.
            "unavailable_reason": self.unavailable_reason or None,
            "schema_version": self.schema_version,
            "primitive_version": self.primitive_version,
        }
        return json.dumps(d)

    @classmethod
    def from_payload_json(cls, identity: AccountingIdentity, payload: str) -> LendingAccountingEvent:
        d = json.loads(payload)
        schema_version = _validate_schema_version(d, "LendingAccountingEvent")
        primitive_version = _validate_primitive_version(d, "LendingAccountingEvent")
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
            amount_token=_dec(d.get("amount_token")),
            confidence=AccountingConfidence(d.get("confidence", AccountingConfidence.HIGH.value)),
            unavailable_reason=d.get("unavailable_reason") or "",
            schema_version=schema_version,
            primitive_version=primitive_version,
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

    # VIB-3488: SY price in USD, additive optional field (stays schema_version=1).
    # None = price unavailable (preserve None vs Decimal("0") discipline).
    # sy_value_usd = sy_amount * sy_price; pt_value_usd = pt_amount * pt_price.
    sy_price: Decimal | None = None

    confidence: AccountingConfidence = AccountingConfidence.HIGH
    unavailable_reason: str = ""
    schema_version: int = 1
    # VIB-4166 (T6) — see module docstring for bump policy.
    primitive_version: int = 1

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
            "sy_price": _enc(self.sy_price),
            "implied_apr_bps": self.implied_apr_bps,
            "days_to_maturity": self.days_to_maturity,
            "realized_yield_usd": _enc(self.realized_yield_usd),
            "basis_lot_id": self.basis_lot_id,
            "confidence": self.confidence.value,
            # VIB-3938 — see LPAccountingEvent.to_payload_json for rationale.
            "unavailable_reason": self.unavailable_reason or None,
            "schema_version": self.schema_version,
            "primitive_version": self.primitive_version,
        }
        return json.dumps(d)

    @classmethod
    def from_payload_json(cls, identity: AccountingIdentity, payload: str) -> PendleAccountingEvent:
        d = json.loads(payload)
        schema_version = _validate_schema_version(d, "PendleAccountingEvent")
        primitive_version = _validate_primitive_version(d, "PendleAccountingEvent")

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
            sy_price=_dec(d.get("sy_price")),
            implied_apr_bps=d.get("implied_apr_bps"),
            days_to_maturity=d.get("days_to_maturity"),
            realized_yield_usd=_dec(d.get("realized_yield_usd")),
            basis_lot_id=d.get("basis_lot_id"),
            confidence=AccountingConfidence(d.get("confidence", AccountingConfidence.HIGH)),
            unavailable_reason=d.get("unavailable_reason") or "",
            schema_version=schema_version,
            primitive_version=primitive_version,
        )


@dataclass
class SwapAccountingEvent:
    """Accounting event emitted after a successful SWAP intent execution (VIB-3473).

    Tracks token flow (token_in → token_out), USD amounts from price_inputs_json,
    FIFO realized PnL for token_in, and acquisition lot recording for token_out.

    Design rules:
    - None = unmeasured / unavailable. Decimal("0") = measured zero. Never conflate.
    - realized_pnl_usd is None when no prior acquisition lot exists for token_in
      (e.g. first swap, or lot history predates the accounting system).
    - cost_basis_recorded = True when the token_out acquisition lot was stored
      in FIFOBasisStore (always True when basis_store is provided).
    - swap_position_key is stored in the payload to enable FIFO lot reconstruction
      on restart (reconstruct_from_events reads it back).
    """

    identity: AccountingIdentity
    event_type: SwapEventType  # always SwapEventType.SWAP
    protocol: str  # "enso" | "uniswap_v3" | "jupiter" | etc.
    token_in: str
    token_out: str
    # ``None`` when the receipt parser could not resolve token decimals (or the
    # ledger row's amount field is empty / unparsable). ``Decimal(0)`` is a
    # measured zero. Per blueprints/27-accounting.md "Empty != zero" — never
    # conflate.
    amount_in: Decimal | None
    amount_out: Decimal | None
    amount_in_usd: Decimal | None
    amount_out_usd: Decimal | None
    # ``None`` when amounts are unmeasured (token decimals could not be resolved
    # by the receipt parser — see ``observability/ledger.py`` and
    # ``connectors/pancakeswap_v3/receipt_parser.py``). ``Decimal(0)`` is a
    # measured zero (e.g. ``amount_in == 0`` and the swap legitimately had a
    # zero numerator). Per blueprints/27-accounting.md "Empty != zero" — never
    # conflate.
    effective_price: Decimal | None
    slippage_bps: int | None
    realized_pnl_usd: Decimal | None  # amount_in_usd - cost_basis_consumed; None if no prior lot
    cost_basis_recorded: bool  # True if acquisition lot was recorded for token_out
    gas_usd: Decimal | None
    confidence: AccountingConfidence
    unavailable_reason: str
    # Position key used for FIFO lot lookup (swap:<chain>:<wallet>).
    # Stored in payload for reconstruct_from_events reconstruction.
    swap_position_key: str = ""
    schema_version: int = 1
    # VIB-4166 (T6) — see module docstring for bump policy.
    primitive_version: int = 1

    def to_payload_json(self) -> str:
        def _enc(v: Any) -> Any:
            if isinstance(v, Decimal):
                return str(v)
            if isinstance(v, AccountingConfidence | SwapEventType):
                return v.value
            return v

        d = {
            "event_type": self.event_type.value,
            "protocol": self.protocol,
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": _enc(self.amount_in),
            "amount_out": _enc(self.amount_out),
            "amount_in_usd": _enc(self.amount_in_usd),
            "amount_out_usd": _enc(self.amount_out_usd),
            "effective_price": _enc(self.effective_price),
            "slippage_bps": self.slippage_bps,
            "realized_pnl_usd": _enc(self.realized_pnl_usd),
            "cost_basis_recorded": self.cost_basis_recorded,
            "gas_usd": _enc(self.gas_usd),
            "confidence": self.confidence.value,
            # VIB-3938 — see LPAccountingEvent.to_payload_json for rationale.
            "unavailable_reason": self.unavailable_reason or None,
            "swap_position_key": self.swap_position_key,
            "schema_version": self.schema_version,
            "primitive_version": self.primitive_version,
        }
        return json.dumps(d)

    @classmethod
    def from_payload_json(cls, identity: AccountingIdentity, payload: str) -> SwapAccountingEvent:
        d = json.loads(payload)
        schema_version = _validate_schema_version(d, "SwapAccountingEvent")
        primitive_version = _validate_primitive_version(d, "SwapAccountingEvent")
        return cls(
            identity=identity,
            event_type=SwapEventType(d["event_type"]),
            protocol=d.get("protocol", ""),
            token_in=d.get("token_in", ""),
            token_out=d.get("token_out", ""),
            amount_in=Decimal(d["amount_in"]) if d.get("amount_in") is not None else None,
            amount_out=Decimal(d["amount_out"]) if d.get("amount_out") is not None else None,
            amount_in_usd=_dec(d.get("amount_in_usd")),
            amount_out_usd=_dec(d.get("amount_out_usd")),
            effective_price=Decimal(d["effective_price"]) if d.get("effective_price") is not None else None,
            slippage_bps=d.get("slippage_bps"),
            realized_pnl_usd=_dec(d.get("realized_pnl_usd")),
            cost_basis_recorded=bool(d.get("cost_basis_recorded", False)),
            gas_usd=_dec(d.get("gas_usd")),
            confidence=AccountingConfidence(d.get("confidence", AccountingConfidence.HIGH.value)),
            unavailable_reason=d.get("unavailable_reason") or "",
            swap_position_key=d.get("swap_position_key", ""),
            schema_version=schema_version,
            primitive_version=primitive_version,
        )


@dataclass
class PredictionAccountingEvent:
    """Accounting event emitted after a prediction-market intent (VIB-3707).

    Captures cost-basis (BUY) and realized PnL (SELL/REDEEM) for Polymarket
    PREDICTION_BUY / PREDICTION_SELL / PREDICTION_REDEEM intents.

    Position state is keyed by (market_id, outcome) — one weighted-average
    aggregate per (market_id, outcome) pair. Averaging-up via repeat BUYs
    rolls into the same aggregate (event_type=PREDICTION_INCREASE);
    partial SELLs reduce the aggregate (PREDICTION_REDUCE); a SELL that
    zeros the position emits PREDICTION_CLOSE; on-chain redemption emits
    PREDICTION_REDEEM and always zeros the position.

    Design rules (mirrors SwapAccountingEvent):
    - None = unmeasured / unavailable. Decimal("0") = measured zero.
    - realized_pnl_usd is None when no prior basis row existed for the
      position (e.g. the strategy was deployed with an existing on-chain
      position the framework never recorded a BUY for). Callers must NOT
      treat None as zero — it signals "PnL unknown for this disposal."
    - position_size_after / position_basis_after capture the post-trade
      aggregate so reconstruction-from-events can restore in-memory state.
    """

    identity: AccountingIdentity
    event_type: PredictionEventType
    position_key: str  # prediction:<protocol>:<chain>:<wallet>:<market_id>:<outcome>
    market_id: str
    outcome: str  # "YES" / "NO"
    intent_type: str  # PREDICTION_BUY / PREDICTION_SELL / PREDICTION_REDEEM
    # Per-trade fields. shares_delta and usd_delta are the absolute trade
    # sizes (always >= 0); the sign is implied by event_type. realized_pnl_usd
    # is populated only on SELL/REDEEM (None on BUY).
    shares_delta: Decimal
    usd_delta: Decimal
    realized_pnl_usd: Decimal | None
    # Post-trade aggregate snapshot (used by reconstruct_from_events).
    position_size_after: Decimal
    position_basis_after: Decimal
    gas_usd: Decimal | None = None
    confidence: AccountingConfidence = AccountingConfidence.HIGH
    unavailable_reason: str = ""
    schema_version: int = 1
    # VIB-4166 (T6) — see module docstring for bump policy.
    primitive_version: int = 1

    def to_payload_json(self) -> str:
        def _enc(v: Any) -> Any:
            if isinstance(v, Decimal):
                return str(v)
            if isinstance(v, AccountingConfidence | PredictionEventType):
                return v.value
            return v

        d = {
            "event_type": self.event_type.value,
            "position_key": self.position_key,
            "market_id": self.market_id,
            "outcome": self.outcome,
            "intent_type": self.intent_type,
            "shares_delta": _enc(self.shares_delta),
            "usd_delta": _enc(self.usd_delta),
            "realized_pnl_usd": _enc(self.realized_pnl_usd),
            "position_size_after": _enc(self.position_size_after),
            "position_basis_after": _enc(self.position_basis_after),
            "gas_usd": _enc(self.gas_usd),
            "confidence": self.confidence.value,
            # VIB-3938 — see LPAccountingEvent.to_payload_json for rationale.
            "unavailable_reason": self.unavailable_reason or None,
            "schema_version": self.schema_version,
            "primitive_version": self.primitive_version,
        }
        return json.dumps(d)

    @classmethod
    def from_payload_json(cls, identity: AccountingIdentity, payload: str) -> PredictionAccountingEvent:
        d = json.loads(payload)
        schema_version = _validate_schema_version(d, "PredictionAccountingEvent")
        primitive_version = _validate_primitive_version(d, "PredictionAccountingEvent")
        return cls(
            identity=identity,
            event_type=PredictionEventType(d["event_type"]),
            position_key=d.get("position_key", ""),
            market_id=d.get("market_id", ""),
            outcome=d.get("outcome", ""),
            intent_type=d.get("intent_type", ""),
            shares_delta=Decimal(d["shares_delta"]) if d.get("shares_delta") is not None else Decimal("0"),
            usd_delta=Decimal(d["usd_delta"]) if d.get("usd_delta") is not None else Decimal("0"),
            realized_pnl_usd=_dec(d.get("realized_pnl_usd")),
            position_size_after=(
                Decimal(d["position_size_after"]) if d.get("position_size_after") is not None else Decimal("0")
            ),
            position_basis_after=(
                Decimal(d["position_basis_after"]) if d.get("position_basis_after") is not None else Decimal("0")
            ),
            gas_usd=_dec(d.get("gas_usd")),
            confidence=AccountingConfidence(d.get("confidence", AccountingConfidence.HIGH.value)),
            unavailable_reason=d.get("unavailable_reason") or "",
            schema_version=schema_version,
            primitive_version=primitive_version,
        )


@dataclass
class TransferAccountingEvent:
    """Payload-only typed event for TRANSFER / BRIDGE intents (VIB-4163).

    Stub introduced by T3 of the Primitives Refactor. Settles in the existing
    ``accounting_events.payload_json`` column — no DDL change.

    ``settlement_status`` is the load-bearing field: it tracks whether the
    transfer has landed on the destination side. BRIDGE intents in particular
    settle asynchronously across two chains, so a single ``TransferAccountingEvent``
    may be written ``PENDING`` (source-leg landed, destination not yet observed)
    and advanced later. T4 (VIB-4164) wires the classifier so BRIDGE intents
    actually reach this handler; T3 ships only the typed shape.
    """

    identity: AccountingIdentity
    event_type: TransferEventType  # always TRANSFER

    # Transfer subject — what was moved.
    asset: str
    amount: Decimal | None
    amount_usd: Decimal | None

    # Cross-chain support (BRIDGE-shaped). When the transfer is single-chain
    # (e.g. an internal value move) source_chain == destination_chain.
    source_chain: str
    destination_chain: str

    # Settlement contract — see TransferSettlementStatus docstring.
    settlement_status: TransferSettlementStatus

    # Position-tracking key. Both state backends read ``getattr(event,
    # "position_key", "")`` to populate ``accounting_events.position_key``;
    # an empty string here makes per-position queries silently miss transfer
    # rows (dashboard rollups, basis-store rehydration). Set explicitly by
    # ``transfer_handler`` from ``outbox_row['position_key']``. Default ``""``
    # is intentionally typed as str (not None) to match the column contract.
    position_key: str = ""

    gas_usd: Decimal | None = None
    confidence: AccountingConfidence = AccountingConfidence.HIGH
    unavailable_reason: str = ""
    schema_version: int = 1
    # VIB-4166 (T6) — see module docstring for bump policy.
    primitive_version: int = 1

    def __post_init__(self) -> None:
        """Coerce enum-typed fields so raw strings raise at construction.

        Without this, ``TransferAccountingEvent(..., settlement_status="garbage")``
        silently stores the string and only crashes at serialization time
        (``self.settlement_status.value`` → AttributeError). Coercing via the
        enum constructor surfaces invalid values immediately. CodeRabbit Major
        finding on PR #2194.
        """
        self.event_type = TransferEventType(self.event_type)
        self.settlement_status = TransferSettlementStatus(self.settlement_status)
        self.confidence = AccountingConfidence(self.confidence)

    def to_payload_json(self) -> str:
        def _enc(v: Any) -> Any:
            if isinstance(v, Decimal):
                return str(v)
            if isinstance(v, AccountingConfidence | TransferEventType | TransferSettlementStatus):
                return v.value
            return v

        d = {
            "event_type": self.event_type.value,
            "position_key": self.position_key,
            "asset": self.asset,
            "amount": _enc(self.amount),
            "amount_usd": _enc(self.amount_usd),
            "source_chain": self.source_chain,
            "destination_chain": self.destination_chain,
            "settlement_status": self.settlement_status.value,
            "gas_usd": _enc(self.gas_usd),
            "confidence": self.confidence.value,
            # VIB-3938 — see LPAccountingEvent.to_payload_json for rationale.
            "unavailable_reason": self.unavailable_reason or None,
            "schema_version": self.schema_version,
            "primitive_version": self.primitive_version,
        }
        return json.dumps(d)

    @classmethod
    def from_payload_json(cls, identity: AccountingIdentity, payload: str) -> TransferAccountingEvent:
        d = json.loads(payload)
        schema_version = _validate_schema_version(d, "TransferAccountingEvent")
        primitive_version = _validate_primitive_version(d, "TransferAccountingEvent")
        return cls(
            identity=identity,
            event_type=TransferEventType(d["event_type"]),
            position_key=d.get("position_key", ""),
            asset=d.get("asset", ""),
            amount=_dec(d.get("amount")),
            amount_usd=_dec(d.get("amount_usd")),
            source_chain=d.get("source_chain", ""),
            destination_chain=d.get("destination_chain", ""),
            settlement_status=TransferSettlementStatus(d["settlement_status"]),
            gas_usd=_dec(d.get("gas_usd")),
            confidence=AccountingConfidence(d.get("confidence", AccountingConfidence.HIGH.value)),
            unavailable_reason=d.get("unavailable_reason") or "",
            schema_version=schema_version,
            primitive_version=primitive_version,
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
