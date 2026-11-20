"""Aerodrome share and fee evidence uses the shared durable runtime lifecycle."""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.connectors.aerodrome.models import Capabilities, Config, Intent, Parameters
from almanak.connectors.aerodrome.observations import Snapshot
from almanak.connectors.aerodrome.operations import Approval, ProtocolOperation
from almanak.connectors.aerodrome.positions import Ownership
from almanak.connectors.decision_observations import Observation
from almanak.connectors.dex_residual import counter_settled
from almanak.dex_intents import CloseLiquidity, ExitExchange, RevokeExchange
from almanak.gateway.accounts import ContractCall, HexData, PreparedCall
from almanak.gateway.evm_fees import OpFeeEvidence, validate_fee_evidence
from almanak.gateway.permission_models import PermissionManifest
from almanak.models import AssetAmount, Block, Hash32, Model, UInt
from almanak.operator_deployment import validate_exclusive_origin
from almanak.state import FeeQuote
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import (
    DecisionContext,
    Memory,
    OutcomeContext,
    SimulationEvidence,
    StrategyDecision,
)
from almanak.transaction_family import TransactionFamily
from almanak.triggers import DecisionSchedule, TriggerEvent
from almanak.work import ChainWork, ObservationScope, validate_work

DecisionInput = DecisionContext[Config | Parameters, Ownership, Snapshot]
Decision = StrategyDecision[Intent]


class DecisionRecord(Model):
    trigger: TriggerEvent
    identity: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    intent: Intent
    observations: tuple[Observation, ...]
    required_observations: tuple[ObservationScope, ...] = ()
    memory: Memory


class ActionResult(Model):
    intent: Intent
    success: bool
    complete: bool
    delta0: int
    delta1: int
    shares_delta: int
    block: Block
    tx_hash: Hash32 | None = None
    simulation: SimulationEvidence | None = None
    accounting_issues: tuple[str, ...] = ()

    @property
    def status(self) -> Literal["completed", "intermediate", "failed"]:
        if not self.success:
            return "failed"
        return "completed" if self.complete else "intermediate"

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @model_validator(mode="after")
    def evidence(self):
        if (self.tx_hash is None) == (self.simulation is None):
            raise ValueError("action result requires exactly one on-chain or simulated identity")
        if self.block.chain != self.intent.target.fund.chain:
            raise ValueError("action result must retain the intent's execution chain")
        return self


OutcomeInput = OutcomeContext[Config | Parameters, ActionResult]


class TransactionPlan(Model):
    attempt_id: str
    decision: DecisionRecord
    operation: ProtocolOperation
    before: Snapshot
    call: ContractCall
    execution: PreparedCall
    nonce: UInt
    gas_estimate: UInt
    gas: UInt
    gas_price: UInt
    fee_quote: FeeQuote | None = None


class Pending(TransactionPlan):
    family: TransactionFamily | None = None
    raw_transaction: HexData
    tx_hash: Hash32
    # True from signing until the gateway durably records, before the first send, that the
    # bytes may leave. Absent, as on attempts signed before this field, means maybe sent.
    unsent: bool = Field(default=False, exclude_if=lambda value: not value)


class Outcome(Model):
    family: TransactionFamily | None = None
    transaction_plan: TransactionPlan | None = None
    revert_reason: str | None = Field(default=None, exclude_if=lambda v: v is None)
    decision: DecisionRecord
    operation: ProtocolOperation
    tx_hash: Hash32
    nonce: UInt
    before: Snapshot
    after: Snapshot
    success: bool
    delta0: int
    delta1: int
    shares_delta: int
    fees0: UInt | None
    fees1: UInt | None
    gas_paid: AssetAmount
    receipt: dict
    op_fee_evidence: OpFeeEvidence | None = None
    call: ContractCall
    execution: PreparedCall
    accounting_issues: tuple[str, ...] = ()

    @model_validator(mode="after")
    def scope(self):
        validate_fee_evidence(
            self.op_fee_evidence, self.tx_hash, self.after.block, self.receipt, self.gas_paid
        )
        if any(
            target != self.operation.target
            for target in (self.before.target, self.after.target, self.decision.intent.target)
        ):
            raise ValueError("Aerodrome outcome must retain its exact economic target")
        self.validate_payer()
        return self

    def validate_payer(self):
        if self.before.native_balance is None:
            raise ValueError("Aerodrome outcome must retain its native payer scope")
        if (self.gas_paid.asset, self.before.payer) != (
            self.before.native_balance.asset,
            self.after.payer,
        ):
            raise ValueError("Aerodrome outcome must retain its native payer scope")

    def complete(self):
        if not self.success:
            return False
        if isinstance(self.decision.intent, RevokeExchange):
            return not self.after.needs_cleanup
        if isinstance(self.decision.intent, ExitExchange):
            return exit_complete(self.after)
        if isinstance(self.decision.intent, CloseLiquidity):
            return self.after.owned.position is None
        return not isinstance(self.operation, Approval)

    def strategy_result(self):
        return ActionResult(
            intent=self.decision.intent,
            success=self.success,
            complete=self.complete(),
            delta0=self.delta0,
            delta1=self.delta1,
            shares_delta=self.shares_delta,
            block=self.after.block,
            tx_hash=self.tx_hash,
            accounting_issues=self.accounting_issues,
        )


Work = ChainWork[DecisionRecord, TransactionPlan, Pending]


class Checkpoint(Model):
    schema_version: Literal[1] = 1
    schedule: DecisionSchedule
    work: tuple[Work, ...] = ()
    scheduler_cursor: UInt = 0
    deployment_id: str
    config: Config
    strategy: StrategyDefinition | None = None
    # Set once at creation for a deployment that takes operator requests.
    operator: bool = Field(default=False, exclude_if=lambda v: not v)
    capabilities: Capabilities
    permissions: PermissionManifest[Capabilities] | None
    mode: Literal["running", "teardown", "closed"]
    initialized: bool
    memory: Memory = Memory()
    delivered: UInt = 0
    decision: DecisionRecord | None = None
    owned: tuple[Ownership, ...]
    baseline: tuple[Snapshot, ...] = ()
    pending: Pending | None = None
    # Signed attempts released past their protocol deadline, kept as reconciliation evidence
    # until an attempt at their submitter nonce is reconciled. Omitted when empty.
    released: tuple[Pending, ...] = Field(default=(), exclude_if=lambda value: not value)
    plan: TransactionPlan | None = None
    outcomes: tuple[Outcome, ...] = ()
    closure: tuple[Snapshot, ...] = ()
    integrity_error: str | None = None

    @model_validator(mode="after")
    def scope(self):
        self.schedule.validate_trigger(self.config.decision_trigger)
        validate_work(self)
        validate_exclusive_origin(self)
        if tuple(value.target for value in self.owned) != tuple(
            self.config.target(a.name) for a in self.config.positions
        ):
            raise ValueError("ownership must retain each configured Aerodrome target exactly once")
        if self.delivered > len(self.outcomes):
            raise ValueError("outcome delivery cannot precede retained execution evidence")
        return self


def exit_complete(after):
    """No position, cleanup or counter inventory beyond the residual measured here."""
    return (
        after.owned.position is None
        and not after.needs_cleanup
        and counter_settled(after.owned, after.exit_residual)
    )
