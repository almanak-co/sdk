"""Accepted GMX requests outlive the submitter transaction and retain external keeper evidence."""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.connectors.decision_observations import Observation
from almanak.connectors.gmx_v2.exchange_domain import ExchangeTarget
from almanak.connectors.gmx_v2.exchange_observations import ExchangeOwnership, ExchangeSnapshot
from almanak.connectors.gmx_v2.intents import Intent, RevokePosition
from almanak.connectors.gmx_v2.liquidation_types import (
    LiquidationOutcome,
    LiquidationResult,
    Scan,
)
from almanak.connectors.gmx_v2.models import Capabilities, Config, Parameters
from almanak.connectors.gmx_v2.observations import Snapshot
from almanak.connectors.gmx_v2.operations import ProtocolOperation
from almanak.connectors.gmx_v2.positions import Ownership
from almanak.connectors.gmx_v2.requests import Request, RequestStatus
from almanak.dex_intents import LiquidityBudget, LiquidityMinimum, RevokeExchange, SwapExactInput
from almanak.effects import AssetDelta
from almanak.gateway.accounts import ContractCall, HexData, PreparedCall
from almanak.gateway.evm_fees import OpFeeEvidence, validate_fee_evidence
from almanak.gateway.permission_models import PermissionManifest
from almanak.models import AssetAmount, Block, Hash32, Model, UInt
from almanak.state import FeeQuote
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import DecisionContext, Memory, OutcomeContext, StrategyDecision
from almanak.transaction_family import TransactionFamily
from almanak.triggers import DecisionSchedule, TriggerEvent
from almanak.work import ChainWork, ObservationScope, validate_work

SnapshotType = Snapshot | ExchangeSnapshot
OwnershipType = Ownership | ExchangeOwnership
DecisionInput = DecisionContext[Config | Parameters, OwnershipType, SnapshotType]
Decision = StrategyDecision[Intent]


class DecisionRecord(Model):
    trigger: TriggerEvent
    identity: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    intent: Intent
    observations: tuple[SnapshotType | Observation, ...]
    required_observations: tuple[ObservationScope, ...] = ()
    memory: Memory


class ActionResult(Model):
    intent: Intent
    success: bool
    complete: bool
    request_status: RequestStatus | None
    assets: tuple[AssetDelta, ...]
    block: Block
    tx_hash: Hash32
    accounting_issues: tuple[str, ...] = ()

    @property
    def effective_at(self):
        return datetime.fromtimestamp(self.block.timestamp, UTC)

    @property
    def status(self) -> Literal["completed", "intermediate", "failed"]:
        if not self.success:
            return "failed"
        return "completed" if self.complete else "intermediate"


OutcomeInput = OutcomeContext[Config | Parameters, ActionResult | LiquidationResult]


class TransactionPlan(Model):
    attempt_id: str
    decision: DecisionRecord
    operation: ProtocolOperation
    before: SnapshotType
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


class EconomicOutcome(Model):
    decision: DecisionRecord
    operation: ProtocolOperation
    before: SnapshotType
    after: SnapshotType
    tx_hash: Hash32
    receipt: dict
    success: bool
    request: Request | None
    assets: tuple[AssetDelta, ...]
    accounting_issues: tuple[str, ...] = ()

    @model_validator(mode="after")
    def scope(self):
        if any(
            target != self.operation.target
            for target in (self.before.target, self.after.target, self.decision.intent.target)
        ):
            raise ValueError("GMX outcome must retain its exact economic target")
        if self.request is not None and (
            self.request.fund,
            self.request.allocation,
            self.request.decision_identity,
        ) != (self.operation.target.fund, self.operation.allocation, self.decision.identity):
            raise ValueError("GMX outcome request differs from its admitted decision and fund")
        if self.request is not None and isinstance(self.operation.target, ExchangeTarget):
            intent = self.decision.intent
            if (
                not isinstance(intent, (SwapExactInput, LiquidityBudget, LiquidityMinimum))
                or self.request.admission_deadline != intent.deadline
            ):
                raise ValueError("GMX exchange outcome must retain its original admission deadline")
        validate_outcome_receipt(self)
        if any(
            (delta.key.account, delta.key.chain) != (self.operation.account, self.after.block.chain)
            for delta in self.assets
        ):
            raise ValueError("GMX economic effects must retain their fund account and chain")
        return self

    def strategy_result(self):
        status = self.request.status if self.request is not None else None
        complete = self.request is not None and not self.request.unresolved
        if isinstance(self.decision.intent, (RevokePosition, RevokeExchange)):
            complete = not self.after.needs_cleanup
        return ActionResult(
            intent=self.decision.intent,
            success=self.success and status != "cancelled",
            complete=complete,
            request_status=status,
            assets=self.assets,
            block=self.after.block,
            tx_hash=self.tx_hash,
            accounting_issues=self.accounting_issues,
        )


def validate_outcome_receipt(outcome):
    expected = {
        "transactionHash": outcome.tx_hash,
        "blockHash": outcome.after.block.hash,
        "blockNumber": hex(outcome.after.block.number),
        "status": "0x1" if outcome.success else "0x0",
    }
    if any(outcome.receipt.get(key) != value for key, value in expected.items()):
        raise ValueError("GMX outcome must retain its exact receipt identity and success")


class RequestOutcome(EconomicOutcome):
    kind: Literal["gmx_request_outcome"] = "gmx_request_outcome"
    request: Request

    @model_validator(mode="after")
    def keeper_evidence(self):
        event = self.request.events[-1]
        if event.status == "accepted" or (
            event.identity.transaction_hash,
            event.identity.block,
            event.receipt,
        ) != (self.tx_hash, self.after.block, self.receipt):
            raise ValueError("GMX keeper outcome requires its exact request progress evidence")
        return self


class TransactionOutcome(EconomicOutcome):
    kind: Literal["gmx_transaction_outcome"] = "gmx_transaction_outcome"
    family: TransactionFamily | None = None
    transaction_plan: TransactionPlan | None = None
    revert_reason: str | None = Field(default=None, exclude_if=lambda v: v is None)
    nonce: UInt
    gas_paid: AssetAmount
    op_fee_evidence: OpFeeEvidence | None = None
    call: ContractCall
    execution: PreparedCall

    @model_validator(mode="after")
    def fees(self):
        validate_fee_evidence(
            self.op_fee_evidence, self.tx_hash, self.after.block, self.receipt, self.gas_paid
        )
        if (self.before.payer, self.before.native_balance.asset) != (
            self.after.payer,
            self.gas_paid.asset,
        ):
            raise ValueError("GMX transaction outcome must retain its submitter and gas units")
        return self


Outcome = Annotated[
    TransactionOutcome | RequestOutcome | LiquidationOutcome, Field(discriminator="kind")
]
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
    owned: tuple[OwnershipType, ...]
    requests: tuple[Request, ...] = ()
    external_scans: tuple[Scan, ...] = ()
    baseline: tuple[SnapshotType, ...] = ()
    pending: Pending | None = None
    plan: TransactionPlan | None = None
    outcomes: tuple[Outcome, ...] = ()
    closure: tuple[SnapshotType, ...] = ()
    integrity_error: str | None = None

    @model_validator(mode="after")
    def scope(self):
        self.schedule.validate_trigger(self.config.decision_trigger)
        validate_work(self)
        if tuple(o.target for o in self.owned) != tuple(
            self.config.target(a.name) for a in self.config.positions
        ):
            raise ValueError("GMX ownership must retain each configured target exactly once")
        if self.delivered > len(self.outcomes):
            raise ValueError("GMX outcome delivery cannot precede retained execution evidence")
        validate_request_history(self)
        validate_active_decisions(self)
        return self


def validate_request_history(state):
    identities = [(r.fund.chain, r.key) for r in state.requests]
    if len(set(identities)) != len(identities):
        raise ValueError("GMX request identity must occur exactly once in retained history")
    for request in state.requests:
        target = state.config.target(request.allocation)
        if (request.fund, request.market) != (target.fund, target.market.share_asset):
            raise ValueError("GMX request history differs from its configured target")


def validate_active_decisions(state):
    decisions = {work.chain: work.decision for work in state.work}
    active_chains = set()
    for request in state.requests:
        if not request.unresolved:
            continue
        decision = decisions.get(request.fund.chain) if state.work else state.decision
        validate_active_decision(state, request, decision)
        if request.fund.chain in active_chains:
            raise ValueError("GMX permits one admitted unresolved request per execution chain")
        active_chains.add(request.fund.chain)


def validate_active_decision(state, request, decision):
    if decision is None or (decision.identity, decision.intent.target) != (
        request.decision_identity,
        state.config.target(request.allocation),
    ):
        raise ValueError("GMX unresolved request requires its original admitted decision")
