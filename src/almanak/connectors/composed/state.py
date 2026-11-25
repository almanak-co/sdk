"""A single checkpoint retains typed protocol work and shared fund cash."""

import json
from functools import cache, reduce
from operator import or_
from typing import Literal

from pydantic import Field, TypeAdapter, ValidationError, field_validator, model_validator

from almanak.configuration.composed import Parameters
from almanak.connectors.aave_v3 import state as lending
from almanak.connectors.aave_v3.models import Intent as LendingIntent
from almanak.connectors.aave_v3.observations import Ownership as LendingOwnership
from almanak.connectors.aave_v4 import models as aave_v4_models
from almanak.connectors.aave_v4 import state as aave_v4
from almanak.connectors.across import settlement_types as across_settlement
from almanak.connectors.across import state as across
from almanak.connectors.across.models import Intent as AcrossIntent
from almanak.connectors.across.observations import Ownership as AcrossOwnership
from almanak.connectors.aerodrome import state as aerodrome
from almanak.connectors.aerodrome.models import Intent as AerodromeIntent
from almanak.connectors.aerodrome.positions import Ownership as AerodromeOwnership
from almanak.connectors.aerodrome_slipstream import state as slipstream
from almanak.connectors.aerodrome_slipstream.models import Intent as SlipstreamIntent
from almanak.connectors.aerodrome_slipstream.positions import Ownership as SlipstreamOwnership
from almanak.connectors.composed.authority import Capabilities
from almanak.connectors.composed.inventory import validate
from almanak.connectors.composed.models import Config, OwnedParameters
from almanak.connectors.compound_v3 import state as compound
from almanak.connectors.compound_v3.models import Intent as CompoundIntent
from almanak.connectors.compound_v3.observations import Ownership as CompoundOwnership
from almanak.connectors.curve import state as curve
from almanak.connectors.curve.models import Intent as CurveIntent
from almanak.connectors.curve.positions import Ownership as CurveOwnership
from almanak.connectors.decision_observations import Observation
from almanak.connectors.gmx_v2 import state as gmx
from almanak.connectors.gmx_v2.intents import Intent as GmxIntent
from almanak.connectors.morpho_blue import state as morpho
from almanak.connectors.morpho_blue.models import Intent as MorphoIntent
from almanak.connectors.morpho_blue.observations import Ownership as MorphoOwnership
from almanak.connectors.pancakeswap_v3 import state as pancake
from almanak.connectors.pancakeswap_v3.models import Intent as PancakeIntent
from almanak.connectors.pancakeswap_v3.positions import Ownership as PancakeOwnership
from almanak.connectors.repayment import Coverage
from almanak.connectors.sparklend import state as spark
from almanak.connectors.sparklend.models import Intent as SparkIntent
from almanak.connectors.sparklend.observations import Ownership as SparkOwnership
from almanak.connectors.target_routing import target_bindings, target_connector
from almanak.connectors.uniswap_v3 import state as exchange
from almanak.connectors.uniswap_v3.models import Intent as ExchangeIntent
from almanak.connectors.uniswap_v3.positions import Ownership as ExchangeOwnership
from almanak.connectors.uniswap_v4 import state as v4
from almanak.connectors.uniswap_v4.models import Intent as V4Intent
from almanak.effects import Balance, WalletAsset
from almanak.gateway.permission_models import PermissionManifest
from almanak.models import Model, UInt
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import DecisionContext, Memory, OutcomeContext, StrategyDecision
from almanak.transfers import Transfer, validate_transfers
from almanak.triggers import DecisionSchedule
from almanak.work import ChainWork, validate_work

Ownership = (
    LendingOwnership
    | aave_v4_models.Ownership
    | SparkOwnership
    | v4.Ownership
    | ExchangeOwnership
    | PancakeOwnership
    | AerodromeOwnership
    | SlipstreamOwnership
    | MorphoOwnership
    | CompoundOwnership
    | CurveOwnership
    | gmx.OwnershipType
    | AcrossOwnership
)
Intent = (
    V4Intent
    | aave_v4_models.Intent
    | LendingIntent
    | SparkIntent
    | ExchangeIntent
    | PancakeIntent
    | AerodromeIntent
    | SlipstreamIntent
    | MorphoIntent
    | CompoundIntent
    | CurveIntent
    | GmxIntent
    | AcrossIntent
)
DecisionRecord = (
    v4.DecisionRecord
    | aave_v4_models.DecisionRecord
    | lending.DecisionRecord
    | spark.DecisionRecord
    | exchange.DecisionRecord
    | pancake.DecisionRecord
    | aerodrome.DecisionRecord
    | slipstream.DecisionRecord
    | morpho.DecisionRecord
    | compound.DecisionRecord
    | curve.DecisionRecord
    | gmx.DecisionRecord
    | across.DecisionRecord
)
TransactionPlan = (
    v4.TransactionPlan
    | aave_v4.TransactionPlan
    | lending.TransactionPlan
    | spark.TransactionPlan
    | exchange.TransactionPlan
    | pancake.TransactionPlan
    | aerodrome.TransactionPlan
    | slipstream.TransactionPlan
    | morpho.TransactionPlan
    | compound.TransactionPlan
    | curve.TransactionPlan
    | gmx.TransactionPlan
    | across.TransactionPlan
)
Pending = (
    v4.Pending
    | aave_v4.Pending
    | lending.Pending
    | spark.Pending
    | exchange.Pending
    | pancake.Pending
    | aerodrome.Pending
    | slipstream.Pending
    | morpho.Pending
    | compound.Pending
    | curve.Pending
    | gmx.Pending
    | across.Pending
)
Outcome = (
    v4.Outcome
    | aave_v4.Outcome
    | lending.Outcome
    | spark.Outcome
    | exchange.Outcome
    | pancake.Outcome
    | aerodrome.Outcome
    | slipstream.Outcome
    | morpho.Outcome
    | compound.Outcome
    | curve.Outcome
    | gmx.Outcome
    | across.Outcome
    | across_settlement.ArrivalOutcome
    | across_settlement.RefundOutcome
)
DecisionInput = DecisionContext[Config | OwnedParameters | Parameters, Ownership, Observation]
Decision = StrategyDecision[Intent]
OutcomeInput = OutcomeContext[
    Config | OwnedParameters | Parameters,
    v4.ActionResult
    | aave_v4_models.ActionResult
    | lending.ActionResult
    | spark.ActionResult
    | exchange.ActionResult
    | pancake.ActionResult
    | aerodrome.ActionResult
    | slipstream.ActionResult
    | morpho.ActionResult
    | compound.ActionResult
    | curve.ActionResult
    | gmx.ActionResult
    | gmx.LiquidationResult
    | across.ActionResult
    | across_settlement.MeasuredArrival
    | across_settlement.RefundResult,
]
Work = ChainWork[DecisionRecord, TransactionPlan, Pending]

# The records each connector retains, keyed by the connector of the allocation they act on.
RECORDS = {
    "uniswap_v4": (v4.DecisionRecord, v4.TransactionPlan, v4.Pending, v4.Outcome),
    "aave_v4": (
        aave_v4_models.DecisionRecord,
        aave_v4.TransactionPlan,
        aave_v4.Pending,
        aave_v4.Outcome,
    ),
    "aave_v3": (lending.DecisionRecord, lending.TransactionPlan, lending.Pending, lending.Outcome),
    "sparklend": (spark.DecisionRecord, spark.TransactionPlan, spark.Pending, spark.Outcome),
    "uniswap_v3": (
        exchange.DecisionRecord,
        exchange.TransactionPlan,
        exchange.Pending,
        exchange.Outcome,
    ),
    "pancakeswap_v3": (
        pancake.DecisionRecord,
        pancake.TransactionPlan,
        pancake.Pending,
        pancake.Outcome,
    ),
    "aerodrome": (
        aerodrome.DecisionRecord,
        aerodrome.TransactionPlan,
        aerodrome.Pending,
        aerodrome.Outcome,
    ),
    "aerodrome_slipstream": (
        slipstream.DecisionRecord,
        slipstream.TransactionPlan,
        slipstream.Pending,
        slipstream.Outcome,
    ),
    "morpho_blue": (morpho.DecisionRecord, morpho.TransactionPlan, morpho.Pending, morpho.Outcome),
    "compound_v3": (
        compound.DecisionRecord,
        compound.TransactionPlan,
        compound.Pending,
        compound.Outcome,
    ),
    "curve": (curve.DecisionRecord, curve.TransactionPlan, curve.Pending, curve.Outcome),
    "gmx_v2": (gmx.DecisionRecord, gmx.TransactionPlan, gmx.Pending, gmx.Outcome),
    "across": (across.DecisionRecord, across.TransactionPlan, across.Pending, across.Outcome),
}
RECORD_FIELDS = {"decision": 0, "plan": 1, "pending": 2, "released": 2, "outcomes": 3}
# Fields that retain a sequence of records rather than one.
RECORD_SEQUENCES = ("released", "outcomes")
UNIONS = (DecisionRecord, TransactionPlan, Pending, Outcome)


@cache
def record_adapter(record_type):
    return TypeAdapter(record_type)


def record_target(record, position):
    # Decision records hold the intent; plans, pendings and outcomes retain their decision.
    decision = record if position == 0 else record.get("decision")
    try:
        target = decision["intent"]["target"]
    except (KeyError, TypeError):
        return None
    return target if isinstance(target, dict) else None


def configured_connector(config, record, position):
    target = record_target(record, position) if isinstance(record, dict) else None
    allocation = None if target is None else target.get("allocation")
    if not isinstance(allocation, str):
        return None
    try:
        return str(target_connector(config.target(allocation)))
    except (KeyError, ValueError):
        return None


@cache
def target_adapter():
    return TypeAdapter(reduce(or_, (cls for cls, _ in target_bindings())))


def targeted_connector(record, position):
    """The connector of a record's own target, for records that precede the configuration.

    Chain work is declared before the configuration, so its records cannot resolve their
    allocation. Every record retains its strict target, which only one connector accepts.
    """
    target = record_target(record, position) if isinstance(record, dict) else None
    if target is None:
        return None
    try:
        return str(target_connector(target_adapter().validate_json(json.dumps(target))))
    except (ValidationError, ValueError):
        return None


def decode_record(position, record, connector):
    """Validate one JSON record as its connector's type before the full union.

    Connector targets are distinct strict types, so the record of the connector its
    target belongs to is the only union member that can accept it. Validating it first
    avoids a smart-union attempt against every connector. Any record it cannot route or
    accept takes the full union, whose result and errors are unchanged.
    """
    body = json.dumps(record)
    if connector in RECORDS:
        try:
            return record_adapter(RECORDS[connector][position]).validate_json(body)
        except ValidationError:
            pass
    return record_adapter(UNIONS[position]).validate_json(body)


def decode_work(item):
    if not isinstance(item, dict):
        return record_adapter(Work).validate_json(json.dumps(item))
    records = {f: item[f] for f in ("decision", "plan", "pending") if item.get(f) is not None}
    released = item.get("released")
    routed = released if isinstance(released, list) and released else []
    omitted = {*records, "released"} if routed else set(records)
    # Omitting the records keeps the fields set exactly as the full decode would.
    shell = record_adapter(Work).validate_json(
        json.dumps({f: value for f, value in item.items() if f not in omitted})
    )
    update = {
        f: decode_record(RECORD_FIELDS[f], record, targeted_connector(record, RECORD_FIELDS[f]))
        for f, record in records.items()
    }
    if routed:
        position = RECORD_FIELDS["released"]
        update["released"] = tuple(
            decode_record(position, record, targeted_connector(record, position))
            for record in routed
        )
    return shell.model_copy(update=update)


class Checkpoint(Model):
    schema_version: Literal[1] = 1
    schedule: DecisionSchedule
    work: tuple[Work, ...] = ()
    scheduler_cursor: UInt = 0
    deployment_id: str
    config: Config
    strategy: StrategyDefinition
    capabilities: Capabilities
    permissions: PermissionManifest[Capabilities] | None
    mode: Literal["running", "teardown", "closed"]
    initialized: bool
    memory: Memory = Memory()
    delivered: UInt = 0
    decision: DecisionRecord | None = None
    owned: tuple[Ownership, ...]
    inventory: tuple[Balance, ...]
    requests: tuple[gmx.Request, ...] = ()
    # Committed pages of each component's own scans: GMX liquidations and Across
    # settlement. A page's kind names the connector that reads and audits it.
    external_scans: tuple[gmx.Scan | across_settlement.Scan, ...] = ()
    # Value in flight between two of the deployment's chains: owned, and in no wallet's
    # inventory until its arrival or its refund is measured. Omitted while there is none.
    transfers: tuple[Transfer, ...] = Field(default=(), exclude_if=lambda value: not value)
    # One chain's view only, never retained: this chain's wallet assets that a bridge
    # deposit another chain has planned or signed, and not yet recorded, delivers into.
    inbound: tuple[WalletAsset, ...] = Field(default=(), exclude_if=lambda value: not value)
    baseline: tuple[Observation, ...] = ()
    pending: Pending | None = None
    # Signed attempts released past their protocol deadline, kept as reconciliation evidence
    # until an attempt at their submitter nonce is reconciled. Omitted when empty.
    released: tuple[Pending, ...] = Field(default=(), exclude_if=lambda value: not value)
    plan: TransactionPlan | None = None
    outcomes: tuple[Outcome, ...] = ()
    closure: tuple[Observation, ...] = ()
    integrity_error: str | None = None
    # Loan asset sent to the fund that full repays absorbed into the inventory, per
    # wallet key; omitted while none was.
    top_ups: tuple[Balance, ...] = Field(default=(), exclude_if=lambda value: not value)
    # Repayment coverage measured at the latest decision while debt is open.
    repayment: tuple[Coverage, ...] = Field(default=(), exclude_if=lambda value: not value)

    @field_validator("work", mode="wrap")
    @classmethod
    def route_work(cls, value, handler, info):
        if info.mode != "json" or not isinstance(value, list):
            return handler(value)
        # JSON arrays are typed tuples; the decoded work is revalidated by instance.
        return handler(tuple(decode_work(item) for item in value))

    @field_validator(*RECORD_FIELDS, mode="wrap")
    @classmethod
    def route_records(cls, value, handler, info):
        config = info.data.get("config")
        if info.mode != "json" or config is None or value is None:
            return handler(value)
        position = RECORD_FIELDS[info.field_name]
        if info.field_name not in RECORD_SEQUENCES:
            return handler(
                decode_record(position, value, configured_connector(config, value, position))
            )
        if not isinstance(value, list):
            return handler(value)
        # JSON arrays are typed tuples; the decoded records are revalidated by instance.
        return handler(
            tuple(
                decode_record(position, record, configured_connector(config, record, position))
                for record in value
            )
        )

    @model_validator(mode="after")
    def scope(self):
        self.schedule.validate_trigger(self.config.decision_trigger)
        validate_work(self)
        validate(self)
        gmx.validate_request_history(self)
        gmx.validate_active_decisions(self)
        validate_transfers(self.transfers)
        if self.work and self.inbound:
            raise ValueError("inbound bridge work is one chain's view and is never retained")
        if any(
            self.config.target(page.target.allocation) != page.target
            for page in self.external_scans
        ):
            raise ValueError("external scan history differs from its configured target")
        if tuple(o.target for o in self.owned) != tuple(
            self.config.target(a.name) for a in self.config.positions
        ):
            raise ValueError("ownership must retain each configured target exactly once")
        if self.delivered > len(self.outcomes):
            raise ValueError("outcome delivery cannot precede retained execution evidence")
        return self
