"""Comet execution identities and signed ownership effects in durable runtime state."""

from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.connectors.compound_v3.authority import Capabilities
from almanak.connectors.compound_v3.models import Config, Intent, Parameters
from almanak.connectors.compound_v3.observations import (
    Ownership,
    Snapshot,
)
from almanak.connectors.compound_v3.observations import (
    owned as observed_ownership,
)
from almanak.connectors.compound_v3.operations import (
    ApproveOperation,
    CompoundOperation,
    RevokeOperation,
)
from almanak.connectors.decision_observations import Observation
from almanak.connectors.repayment import Coverage
from almanak.gateway.accounts import ContractCall, HexData, PreparedCall
from almanak.gateway.evm_fees import OpFeeEvidence, validate_fee_evidence
from almanak.gateway.permission_models import PermissionManifest
from almanak.lending_intents import RepayIntent, WithdrawIntent
from almanak.models import AssetAmount, Block, Hash32, Model, UInt
from almanak.operator_deployment import ConsumedNonce, validate_exclusive_origin
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
    assets: AssetAmount
    principal_delta: int
    collateral_delta: int
    block: Block
    tx_hash: Hash32 | None = None
    simulation: SimulationEvidence | None = None

    @property
    def status(self) -> Literal["completed", "intermediate", "failed"]:
        if not self.success:
            return "failed"
        return "completed" if self.complete else "intermediate"

    @property
    def effective_at(self):
        from datetime import UTC, datetime

        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @model_validator(mode="after")
    def evidence(self):
        if (self.tx_hash is None) == (self.simulation is None):
            raise ValueError("action result requires exactly one on-chain or simulated identity")
        if (
            self.assets.asset != self.intent.target.asset
            or self.block.chain != self.intent.target.fund.chain
        ):
            raise ValueError("action result must retain the intent's asset and execution scope")
        return self


OutcomeInput = OutcomeContext[Config | Parameters, ActionResult]


class TransactionPlan(Model):
    attempt_id: str
    decision: DecisionRecord
    operation: CompoundOperation
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


class Outcome(Model):
    family: TransactionFamily | None = None
    transaction_plan: TransactionPlan | None = None
    revert_reason: str | None = Field(default=None, exclude_if=lambda v: v is None)
    decision: DecisionRecord
    operation: CompoundOperation
    tx_hash: Hash32
    nonce: UInt
    before: Snapshot
    after: Snapshot
    success: bool
    assets: UInt
    principal_delta: int
    collateral_delta: int
    gas_paid: AssetAmount
    receipt: dict
    op_fee_evidence: OpFeeEvidence | None = Field(default=None, exclude_if=lambda v: v is None)
    call: ContractCall
    execution: PreparedCall
    accounting_issues: tuple[str, ...] = ()

    @model_validator(mode="after")
    def scope(self):
        validate_fee_evidence(
            self.op_fee_evidence, self.tx_hash, self.after.block, self.receipt, self.gas_paid
        )
        target = self.operation.target
        if any(
            t != target
            for t in (self.before.target, self.after.target, self.decision.intent.target)
        ):
            raise ValueError("outcome must retain its exact economic target")
        if (
            self.gas_paid.asset != self.before.native_balance.asset
            or self.after.payer != self.before.payer
        ):
            raise ValueError("outcome must retain its gas asset and payer")
        return self

    def strategy_result(self):
        complete = self.success and not isinstance(self.operation, ApproveOperation)
        if isinstance(self.decision.intent, RepayIntent):
            complete = complete and not (self.after.account.principal != 0 or self.after.allowance)
        elif isinstance(self.decision.intent, WithdrawIntent):
            complete = complete and not (
                not observed_ownership(self.after).empty or self.after.allowance
            )
        elif not isinstance(self.operation, RevokeOperation):
            complete = complete and not self.after.allowance
        return ActionResult(
            intent=self.decision.intent,
            success=self.success,
            complete=complete,
            assets=AssetAmount(asset=self.operation.target.asset, amount=self.assets),
            principal_delta=self.principal_delta,
            collateral_delta=self.collateral_delta,
            block=self.after.block,
            tx_hash=self.tx_hash,
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
    # Signed steps an operator deployment dropped because a final transaction took their
    # nonce, each with the gateway's proof. Omitted when empty.
    superseded: tuple[ConsumedNonce, ...] = Field(default=(), exclude_if=lambda v: not v)
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
    plan: TransactionPlan | None = None
    outcomes: tuple[Outcome, ...] = ()
    closure: tuple[Snapshot, ...] = ()
    integrity_error: str | None = None
    # Repayment coverage measured at the latest decision while debt is open.
    repayment: tuple[Coverage, ...] = Field(default=(), exclude_if=lambda value: not value)

    @model_validator(mode="after")
    def scope(self):
        self.schedule.validate_trigger(self.config.decision_trigger)
        validate_work(self)
        validate_exclusive_origin(self)
        if tuple(o.target for o in self.owned) != tuple(
            self.config.target(a.name) for a in self.config.positions
        ):
            raise ValueError("ownership must identify each configured reserve exactly once")
        if self.delivered > len(self.outcomes):
            raise ValueError("outcome delivery cannot precede retained execution evidence")
        return self
