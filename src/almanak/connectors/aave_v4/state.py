"""One atomic checkpoint containing memory, ownership and execution evidence."""

from typing import Literal

from pydantic import Field, model_validator

from almanak.connectors.aave_v4.models import (
    AaveCapabilities,
    ActionResult,
    BorrowProjection,
    DecisionRecord,
    LendingConfig,
    LendingSnapshot,
    Memory,
    Ownership,
)
from almanak.connectors.aave_v4.operations import (
    AaveOperation,
    ApproveOperation,
    BorrowOperation,
)
from almanak.connectors.aave_v4.permission_adapter import PermissionManifest
from almanak.connectors.repayment import Coverage
from almanak.gateway.accounts import ContractCall, HexData, PreparedCall
from almanak.gateway.evm_fees import OpFeeEvidence, validate_fee_evidence
from almanak.lending_intents import BorrowIntent, RepayIntent, WithdrawIntent
from almanak.models import AssetAmount, Hash32, Model, UInt
from almanak.operator_deployment import ConsumedNonce, validate_origin
from almanak.state import FeeQuote
from almanak.strategy import StrategyDefinition
from almanak.transaction_family import TransactionFamily
from almanak.triggers import DecisionSchedule
from almanak.work import ChainWork, validate_work


class TransactionPlan(Model):
    attempt_id: str | None = None
    decision: DecisionRecord
    operation: AaveOperation
    before: LendingSnapshot
    call: ContractCall
    execution: PreparedCall | None = None
    borrow_projection: BorrowProjection | None = None
    nonce: UInt
    gas_estimate: UInt
    gas: UInt
    gas_price: UInt
    # The quote's observed block makes admission independent of a moving gas market.
    fee_quote: FeeQuote | None = Field(default=None, exclude_if=lambda v: v is None)


class Pending(TransactionPlan):
    family: TransactionFamily | None = Field(default=None, exclude_if=lambda v: v is None)
    raw_transaction: HexData
    tx_hash: Hash32


class Outcome(Model):
    family: TransactionFamily | None = Field(default=None, exclude_if=lambda v: v is None)
    transaction_plan: TransactionPlan | None = Field(default=None, exclude_if=lambda v: v is None)
    revert_reason: str | None = Field(default=None, exclude_if=lambda v: v is None)
    decision: DecisionRecord
    operation: AaveOperation
    tx_hash: Hash32
    nonce: UInt
    before: LendingSnapshot
    after: LendingSnapshot
    success: bool
    assets: UInt
    shares: UInt
    gas_paid: AssetAmount
    receipt: dict
    op_fee_evidence: OpFeeEvidence | None = Field(default=None, exclude_if=lambda v: v is None)
    accounting_issues: tuple[str, ...] = Field(default=(), exclude_if=lambda v: not v)
    call: ContractCall | None = None
    execution: PreparedCall | None = None
    borrow_projection: BorrowProjection | None = None

    @model_validator(mode="after")
    def economic_scope(self):
        validate_fee_evidence(
            self.op_fee_evidence, self.tx_hash, self.after.block, self.receipt, self.gas_paid
        )
        if (
            self.before.target != self.operation.target
            or self.after.target != self.operation.target
            or self.decision.intent.target != self.operation.target
            or self.gas_paid.asset != self.before.native_balance.asset
            or self.gas_paid.asset != self.after.native_balance.asset
        ):
            raise ValueError("outcome quantities must retain their account and asset scope")
        return self

    def strategy_result(self) -> ActionResult:
        complete = self.success and not isinstance(self.operation, ApproveOperation)
        if isinstance(self.decision.intent, BorrowIntent):
            complete = self.success and isinstance(self.operation, BorrowOperation)
        if isinstance(self.decision.intent, RepayIntent):
            complete = self.success and not (
                self.after.position.total_debt
                or self.after.drawn_shares
                or self.after.premium_shares
                or self.after.premium_offset_ray
                or self.after.allowance
                or any(c.enabled for c in self.after.collateral)
            )
        if isinstance(self.decision.intent, WithdrawIntent):
            complete = complete and not (
                self.after.position.supplied_shares or self.after.allowance
            )
        return ActionResult(
            intent=self.decision.intent,
            success=self.success,
            complete=complete,
            assets=AssetAmount(asset=self.operation.target.reserve.asset, amount=self.assets),
            shares=self.shares,
            block=self.after.position.reserve.block,
            tx_hash=self.tx_hash,
        )


Work = ChainWork[DecisionRecord, TransactionPlan, Pending]


class Checkpoint(Model):
    schedule: DecisionSchedule
    work: tuple[Work, ...] = Field(default=(), exclude_if=lambda v: not v)
    scheduler_cursor: UInt = Field(default=0, exclude_if=lambda v: not v)
    schema_version: Literal[5] = 5
    deployment_id: str
    config: LendingConfig
    strategy: StrategyDefinition | None = None
    # Set once at creation for a deployment that takes operator requests.
    operator: bool = Field(default=False, exclude_if=lambda v: not v)
    # Signed steps an operator deployment dropped because a final transaction took their
    # nonce, each with the gateway's proof. Omitted when empty.
    superseded: tuple[ConsumedNonce, ...] = Field(default=(), exclude_if=lambda v: not v)
    capabilities: AaveCapabilities | None = None
    permissions: PermissionManifest | None = None
    mode: Literal["running", "teardown", "closed"]
    initialized: bool
    memory: Memory = Memory()
    delivered: UInt = 0
    decision: DecisionRecord | None = None
    owned: tuple[Ownership, ...]
    baseline: tuple[LendingSnapshot, ...]
    pending: Pending | None
    plan: TransactionPlan | None = None
    outcomes: tuple[Outcome, ...]
    closure: tuple[LendingSnapshot, ...]
    integrity_error: str | None = Field(default=None, exclude_if=lambda v: v is None)
    # Repayment coverage measured at the latest decision while debt is open.
    repayment: tuple[Coverage, ...] = Field(default=(), exclude_if=lambda value: not value)

    def shares(self, target):
        return next(item.shares for item in self.owned if item.target == target)

    @model_validator(mode="after")
    def scope(self):
        self.schedule.validate_trigger(self.config.decision_trigger)
        validate_work(self)
        validate_origin(self)
        expected = tuple(self.config.target(a.name) for a in self.config.positions)
        if tuple(item.target for item in self.owned) != expected:
            raise ValueError("ownership must identify each configured supply position exactly once")
        if self.delivered > len(self.outcomes):
            raise ValueError("outcome delivery cannot precede retained execution evidence")
        return self
