"""Across execution identities and the transfers a deployment holds in flight."""

from datetime import UTC, datetime
from typing import Literal

from pydantic import Field, model_validator

from almanak.connectors.across.authority import Capabilities
from almanak.connectors.across.models import Config, Intent, Parameters
from almanak.connectors.across.observations import Ownership, Snapshot
from almanak.connectors.across.operations import (
    AcrossOperation,
    DepositOperation,
    RevokeOperation,
)
from almanak.gateway.accounts import ContractCall, HexData, PreparedCall
from almanak.gateway.evm_fees import OpFeeEvidence, validate_fee_evidence
from almanak.gateway.permission_models import PermissionManifest
from almanak.models import AssetAmount, Block, Hash32, Model, UInt
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
from almanak.transfers import Transfer, validate_transfers
from almanak.triggers import DecisionSchedule, TriggerEvent
from almanak.work import ChainWork, ObservationScope, validate_work

DecisionInput = DecisionContext[Config | Parameters, Ownership, Snapshot]
Decision = StrategyDecision[Intent]


class DecisionRecord(Model):
    trigger: TriggerEvent
    identity: str = Field(pattern=r"^[0-9a-f]{32}$")
    intent: Intent
    observations: tuple[Snapshot, ...]
    required_observations: tuple[ObservationScope, ...] = ()
    memory: Memory


class ActionResult(Model):
    intent: Intent
    success: bool
    complete: bool
    # What left the source fund. The amount received is unknown until the transfer arrives.
    sent: AssetAmount
    transfer: Transfer | None = None
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
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @model_validator(mode="after")
    def evidence(self):
        if (self.tx_hash is None) == (self.simulation is None):
            raise ValueError("action result requires exactly one on-chain or simulated identity")
        if (
            self.sent.asset != self.intent.target.asset
            or self.block.chain != self.intent.target.fund.chain
        ):
            raise ValueError("action result must retain the intent's asset and execution scope")
        return self


OutcomeInput = OutcomeContext[Config | Parameters, ActionResult]


class TransactionPlan(Model):
    attempt_id: str
    decision: DecisionRecord
    operation: AcrossOperation
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
    operation: AcrossOperation
    tx_hash: Hash32
    nonce: UInt
    before: Snapshot
    after: Snapshot
    success: bool
    # Base units of the sent asset that left the fund in this transaction.
    assets: UInt
    # The claim a mined deposit opened; absent for an approval, a revocation or a revert.
    transfer: Transfer | None = Field(default=None, exclude_if=lambda v: v is None)
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
        deposited = self.success and isinstance(self.operation, DepositOperation)
        if deposited != (self.transfer is not None) or deposited != bool(self.assets):
            raise ValueError("exactly a mined deposit moves the asset and opens a transfer")
        return self

    def strategy_result(self):
        settled = isinstance(self.operation, (DepositOperation, RevokeOperation))
        return ActionResult(
            intent=self.decision.intent,
            success=self.success,
            complete=self.success and settled and not self.after.allowance,
            sent=AssetAmount(asset=self.operation.target.asset, amount=self.assets),
            transfer=self.transfer,
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
    strategy: StrategyDefinition
    capabilities: Capabilities
    permissions: PermissionManifest[Capabilities] | None
    mode: Literal["running", "teardown", "closed"]
    initialized: bool
    memory: Memory = Memory()
    delivered: UInt = 0
    decision: DecisionRecord | None = None
    owned: tuple[Ownership, ...]
    # Value in flight between two of the deployment's chains: owned state, not pending work.
    transfers: tuple[Transfer, ...] = ()
    baseline: tuple[Snapshot, ...] = ()
    pending: Pending | None = None
    # Signed deposits released past their quote deadline, kept as reconciliation evidence
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
        validate_transfers(self.transfers)
        if tuple(o.target for o in self.owned) != tuple(
            self.config.target(a.name) for a in self.config.positions
        ):
            raise ValueError("ownership must identify each configured route exactly once")
        if self.delivered > len(self.outcomes):
            raise ValueError("outcome delivery cannot precede retained execution evidence")
        return self
