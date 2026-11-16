"""Attributed exchange inventory and exact NFT ownership, separate from author memory."""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.caip import AssetId, AssetType
from almanak.connectors.decision_observations import Observation
from almanak.connectors.uniswap_v4.models import (
    Capabilities,
    Config,
    Intent,
    Parameters,
    Range,
    Target,
    validate_position_range,
)
from almanak.connectors.uniswap_v4.observation_models import Allowance as Allowance
from almanak.connectors.uniswap_v4.observation_models import (
    LiquidityPosition as LiquidityPosition,
)
from almanak.connectors.uniswap_v4.observation_models import Ownership as Ownership
from almanak.connectors.uniswap_v4.observation_models import (
    SimulatedLiquidity as SimulatedLiquidity,
)
from almanak.connectors.uniswap_v4.observation_models import Snapshot as Snapshot
from almanak.gateway.accounts import ContractCall, HexData, PreparedCall
from almanak.gateway.evm_fees import OpFeeEvidence, validate_fee_evidence
from almanak.gateway.permission_models import PermissionManifest
from almanak.models import Address, AssetAmount, Block, Hash32, Model, UInt
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
    identity: Hash32 | Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    intent: Intent
    observations: tuple[Observation, ...]
    required_observations: tuple[ObservationScope, ...] = Field(
        default=(), exclude_if=lambda v: not v
    )
    memory: Memory

    @model_validator(mode="after")
    def observed_target(self):
        if sum(o.target == self.intent.target for o in self.observations) != 1:
            raise ValueError("decision requires one observation of its exact allocation and chain")
        return self


class ActionResult(Model):
    @property
    def status(self) -> Literal["completed", "intermediate", "failed"]:
        if not self.success:
            return "failed"
        return "completed" if self.complete else "intermediate"

    intent: Intent
    success: bool
    complete: bool
    block: Block
    tx_hash: Hash32 | None = None
    simulation: SimulationEvidence | None = None

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @model_validator(mode="after")
    def execution_evidence(self):
        if (self.tx_hash is None) == (self.simulation is None):
            raise ValueError("action result requires exactly one live or simulated identity")
        return self


OutcomeInput = OutcomeContext[Config | Parameters, ActionResult]


class Operation(Model):
    target: Target

    @property
    def account(self):
        return self.target.account

    @property
    def allocation(self):
        return self.target.allocation


class Approval(Operation):
    kind: Literal["approve_erc20", "approve_permit2", "revoke_erc20", "revoke_permit2"]
    asset: AssetType
    spender: Address
    amount: UInt
    expiration: UInt | None = None


class Swap(Operation):
    kind: Literal["swap"] = "swap"
    amount: AssetAmount
    minimum: AssetAmount
    deadline: UInt


class LiquidityOperation(Operation):
    # A banded target's concrete range travels with the operation so calldata, receipts
    # and reconciliation compare against the same ticks; fixed targets leave it absent.
    range: Range | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def scope(self):
        validate_position_range(self.target, self.range)
        return self


class Mint(LiquidityOperation):
    kind: Literal["mint"] = "mint"
    liquidity: Annotated[UInt, Field(gt=0, lt=2**128)]
    maximum0: AssetAmount
    maximum1: AssetAmount
    deadline: UInt


class Collect(LiquidityOperation):
    kind: Literal["collect"] = "collect"
    nft: AssetId
    deadline: UInt


class Burn(LiquidityOperation):
    kind: Literal["burn"] = "burn"
    nft: AssetId
    liquidity: Annotated[UInt, Field(gt=0, lt=2**128)]
    minimum0: AssetAmount
    minimum1: AssetAmount
    deadline: UInt


ProtocolOperation = Annotated[Approval | Swap | Mint | Collect | Burn, Field(discriminator="kind")]


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
    family: TransactionFamily | None = Field(default=None, exclude_if=lambda v: v is None)
    raw_transaction: HexData
    tx_hash: Hash32
    # True from signing until the gateway durably records, before the first send, that the
    # bytes may leave. Absent, as on attempts signed before this field, means maybe sent.
    unsent: bool = Field(default=False, exclude_if=lambda value: not value)


class Outcome(Model):
    family: TransactionFamily | None = Field(default=None, exclude_if=lambda v: v is None)
    transaction_plan: TransactionPlan | None = Field(default=None, exclude_if=lambda v: v is None)
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
    fees0: UInt | None
    fees1: UInt | None
    gas_paid: AssetAmount
    receipt: dict
    op_fee_evidence: OpFeeEvidence | None = Field(default=None, exclude_if=lambda v: v is None)
    call: ContractCall
    execution: PreparedCall
    accounting_issues: tuple[str, ...] = ()

    @model_validator(mode="after")
    def fee_scope(self):
        validate_fee_evidence(
            self.op_fee_evidence, self.tx_hash, self.after.block, self.receipt, self.gas_paid
        )
        return self

    def strategy_result(self):
        complete = self.success and not isinstance(self.operation, Approval)
        if self.decision.intent.kind == "revoke_exchange":
            complete = self.success and not self.after.needs_cleanup
        if self.decision.intent.kind == "exit_exchange":
            from almanak.connectors.uniswap_v4.exit import cleared

            complete = self.success and cleared(self.after)
        return ActionResult(
            intent=self.decision.intent,
            success=self.success,
            complete=complete,
            block=self.after.block,
            tx_hash=self.tx_hash,
        )


Work = ChainWork[DecisionRecord, TransactionPlan, Pending]


class Checkpoint(Model):
    schedule: DecisionSchedule
    work: tuple[Work, ...] = Field(default=(), exclude_if=lambda v: not v)
    scheduler_cursor: UInt = Field(default=0, exclude_if=lambda v: not v)
    schema_version: Literal[2] = 2
    deployment_id: str
    config: Config
    strategy: StrategyDefinition
    capabilities: Capabilities
    permissions: PermissionManifest[Capabilities] | None = None
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
        if any(isinstance(o.position, SimulatedLiquidity) for o in self.owned):
            raise ValueError("live execution cannot adopt a simulated liquidity position")
        if tuple(o.target for o in self.owned) != tuple(
            self.config.target(a.name) for a in self.config.positions
        ):
            raise ValueError("ownership must cover every configured target exactly once")
        if self.delivered > len(self.outcomes):
            raise ValueError("outcome delivery cannot precede retained evidence")
        return self
