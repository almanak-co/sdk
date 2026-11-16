"""One journal row holds the runtime's fields and the Safe's HyperCore documents together.

The checkpoint keeps the custody, trading and book documents of blueprint 30 (sections
4.5 and 5.5) beside the runtime's own fields (section 13.1), so one journal transaction
commits a decision, its attempt and their effects under the coordinator's lease and the
gateway's admission of every update. A plan is either a custody transaction, signed and
submitted as any EVM transaction, or a venue action, one agent-signed `/exchange` action
that has no transaction hash, gas or EVM nonce.

The documents change only at outcomes. A plan carries what it will add (the attempt the
trading rules planned, the agent approval's fresh address and label); an outcome carries
the evidence that settles it, and `lifecycle.documents` applies that evidence to the
checkpoint's own documents, so the gateway re-derives every change it admits.
"""

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import Field, model_validator

from almanak.connectors.decision_observations import Observation
from almanak.connectors.hyperliquid.agents import Approval
from almanak.connectors.hyperliquid.authority import Capabilities
from almanak.connectors.hyperliquid.custody import Custody
from almanak.connectors.hyperliquid.custody_requests import CustodyRequest
from almanak.connectors.hyperliquid.effects import Book, Effect, Pages
from almanak.connectors.hyperliquid.models import Config, Intent, Parameters, Target
from almanak.connectors.hyperliquid.observations import Snapshot
from almanak.connectors.hyperliquid.operations import CustodyOperation, VenueAction
from almanak.connectors.hyperliquid.positions import Ownership, projected
from almanak.connectors.hyperliquid.trading import Attempt, AttemptId, PerpTarget, Trading
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import AssetDelta
from almanak.external_effects import ExternalEffect, ExternalIdentity, ExternalScan
from almanak.gateway.accounts import ContractCall, HexData, PreparedCall
from almanak.gateway.evm_fees import OpFeeEvidence, validate_fee_evidence
from almanak.gateway.hypercore_exchange import Answer
from almanak.gateway.hypercore_shapes import Millis
from almanak.gateway.hypercore_signing import SignedAction
from almanak.gateway.permission_models import PermissionManifest
from almanak.models import AssetAmount, Hash32, Model, UInt
from almanak.state import FeeQuote
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import DecisionContext, Memory, OutcomeContext, StrategyDecision
from almanak.transaction_family import TransactionFamily
from almanak.triggers import DecisionSchedule, TriggerEvent, UtcInstant
from almanak.work import ChainWork, ObservationScope, validate_work

DecisionInput = DecisionContext[Config | Parameters, Ownership, Snapshot]
Decision = StrategyDecision[Intent]


class DecisionRecord(Model):
    trigger: TriggerEvent
    identity: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    intent: Intent
    observations: tuple[Snapshot | Observation, ...]
    required_observations: tuple[ObservationScope, ...] = ()
    memory: Memory


class ActionResult(Model):
    intent: Intent
    success: bool
    complete: bool
    assets: tuple[AssetDelta, ...]
    effective_at: UtcInstant
    # The custody transaction; a venue action has no transaction hash.
    tx_hash: Hash32 | None = Field(default=None, exclude_if=lambda v: v is None)
    # The venue attempt; a custody transaction has none.
    attempt: AttemptId | None = Field(default=None, exclude_if=lambda v: v is None)
    # Whether the step took effect on HyperCore: an `Ioc` order that matched nothing, or
    # an agent approval HyperCore dropped, resolved cleanly without applying. Absent for
    # a custody transaction, whose effect is its request's own outcome.
    applied: bool | None = Field(default=None, exclude_if=lambda v: v is None)
    accounting_issues: tuple[str, ...] = ()

    @property
    def status(self) -> Literal["completed", "intermediate", "failed"]:
        if not self.success:
            return "failed"
        return "completed" if self.complete else "intermediate"


class EffectsResult(Model):
    """HyperCore effects no intent of the strategy's caused: funding, outside fills, the
    venue's liquidation of the Safe's position."""

    kind: Literal["external_hypercore_effects"] = "external_hypercore_effects"
    identity: ExternalIdentity
    target: Target
    effects: tuple[Effect, ...]
    effective_at: UtcInstant
    accounting_issues: tuple[str, ...] = ()


OutcomeInput = OutcomeContext[Config | Parameters, ActionResult | EffectsResult]


class TransactionPlan(Model):
    """A custody operation: one HyperEVM call the operator makes as the Safe through Roles."""

    kind: Literal["hyperliquid_transaction"] = "hyperliquid_transaction"
    attempt_id: str
    decision: DecisionRecord
    operation: CustodyOperation
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


class VenuePlan(Model):
    """A venue action: the retained attempt and the observation its admission rests on.

    Its agent submission scope, nonce and `expiresAfter` are the attempt's own request.
    """

    kind: Literal["hyperliquid_venue_action"] = "hyperliquid_venue_action"
    attempt_id: str
    decision: DecisionRecord
    operation: VenueAction
    before: Snapshot

    @model_validator(mode="after")
    def scope(self):
        if self.before.target != self.operation.target:
            raise ValueError("a venue plan retains its exact observed target")
        return self


class VenuePending(VenuePlan):
    """The signed body, recorded before anything is posted (section 5.2), and what settled it.

    Only the gateway changes a pending attempt: it clears `unsent` before the post and
    records the venue's answer, or the attempt's own evidence once it resolves it, in the
    same locked commit as the send or the reads (`gateway/hypercore_gateway.py`). Its
    outcome is then re-derived from this record alone, so the gateway can verify it.
    """

    signed: SignedAction
    # True until the gateway durably records, before its post, that the body may leave.
    unsent: bool = True
    # The venue's answer to the post, absent while none has been received.
    answer: Answer | None = None
    # The attempt as its answer or its own evidence resolved it, and when that was received
    # on the deployment clock.
    resolved: Attempt | None = None
    resolved_at: Millis | None = None

    @model_validator(mode="after")
    def signed_scope(self):
        attempt = self.operation.attempt
        if (self.signed.request, self.signed.agent) != (attempt.request, attempt.agent):
            raise ValueError("the signed venue action is not the retained attempt")
        if (self.answer is not None or self.resolved is not None) and self.unsent:
            raise ValueError("an answered venue action was sent")
        if (self.resolved is None) != (self.resolved_at is None):
            raise ValueError("a resolved venue action retains when it was resolved")
        if self.resolved is not None and not settles(attempt, self.resolved, self.signed):
            raise ValueError("a venue action resolves only its own signed attempt")
        return self


def settles(planned: Attempt, resolved: Attempt, signed: SignedAction) -> bool:
    """Whether `resolved` is the planned attempt, signed as recorded and resolved."""
    same = ("id", "purpose", "coin", "agent", "request", "book", "retires", "cancels")
    return (
        resolved.resolved
        and resolved.signed == signed
        and all(getattr(planned, name) == getattr(resolved, name) for name in same)
    )


Plan = Annotated[TransactionPlan | VenuePlan, Field(discriminator="kind")]
PendingWork = Annotated[Pending | VenuePending, Field(discriminator="kind")]


class EconomicOutcome(Model):
    decision: DecisionRecord
    before: Snapshot
    after: Snapshot
    success: bool
    # Whether the intent's last step is done; an intermediate step keeps its decision.
    complete: bool
    assets: tuple[AssetDelta, ...]
    accounting_issues: tuple[str, ...] = ()

    def result(self, effective_at, **identity) -> ActionResult:
        return ActionResult(
            intent=self.decision.intent,
            success=self.success,
            complete=self.complete,
            assets=self.assets,
            effective_at=effective_at,
            accounting_issues=self.accounting_issues,
            **identity,
        )


def validate_scope(outcome):
    operation = outcome.operation
    if any(
        target != operation.target
        for target in (outcome.before.target, outcome.after.target, outcome.decision.intent.target)
    ):
        raise ValueError("Hyperliquid outcome must retain its exact economic target")
    if outcome.complete and not outcome.success:
        raise ValueError("a failed Hyperliquid step cannot complete its intent")
    if any(
        (delta.key.account, delta.key.chain) != (operation.account, outcome.after.block.chain)
        for delta in outcome.assets
    ):
        raise ValueError("Hyperliquid economic effects must retain their fund and chain")


def validate_outcome_receipt(outcome):
    expected = {
        "transactionHash": outcome.tx_hash,
        "blockHash": outcome.after.block.hash,
        "blockNumber": hex(outcome.after.block.number),
        "status": "0x1" if outcome.success else "0x0",
    }
    if any(outcome.receipt.get(key) != value for key, value in expected.items()):
        raise ValueError("Hyperliquid outcome must retain its exact receipt identity and success")


class TransactionOutcome(EconomicOutcome):
    """A mined custody transaction. Its receipt shows only that HyperCore was asked: the
    request it queued (or the agent approval it submitted) is retained from this receipt
    into the checkpoint's custody document (`lifecycle.documents`), and resolved later by
    its own effect."""

    kind: Literal["hyperliquid_transaction_outcome"] = "hyperliquid_transaction_outcome"
    operation: CustodyOperation
    tx_hash: Hash32
    receipt: dict
    family: TransactionFamily | None = None
    transaction_plan: TransactionPlan | None = None
    revert_reason: str | None = Field(default=None, exclude_if=lambda v: v is None)
    nonce: UInt
    gas_paid: AssetAmount
    op_fee_evidence: OpFeeEvidence | None = None
    call: ContractCall
    execution: PreparedCall

    @model_validator(mode="after")
    def evidence(self):
        validate_scope(self)
        validate_outcome_receipt(self)
        validate_fee_evidence(
            self.op_fee_evidence, self.tx_hash, self.after.block, self.receipt, self.gas_paid
        )
        if (self.before.payer, self.before.native_balance.asset) != (
            self.after.payer,
            self.gas_paid.asset,
        ):
            raise ValueError("Hyperliquid transaction outcome must retain its submitter and gas")
        return self

    def strategy_result(self):
        return self.result(self.after.effective_at, tx_hash=self.tx_hash)


class VenueOutcome(EconomicOutcome):
    """A venue action resolved by its answer or by its own evidence (section 5.2).

    A venue action changes no HyperEVM fact, so `after` is the plan's own snapshot. An
    attempt that did not apply is a clean outcome when the venue left nothing to do, such
    as an `Ioc` order that matched nothing or an order released as non-execution under
    A4; `success` is false only for a genuine refusal, which stops new exposure.
    """

    kind: Literal["hyperliquid_venue_outcome"] = "hyperliquid_venue_outcome"
    operation: VenueAction
    # The planned attempt, resolved; the checkpoint's trading document retains it.
    attempt: Attempt
    # When the answer or the evidence that resolved it was received.
    resolved_at: UtcInstant

    @model_validator(mode="after")
    def evidence(self):
        validate_scope(self)
        planned = self.operation.attempt
        if self.attempt.id != planned.id or not self.attempt.resolved:
            raise ValueError("a venue outcome retains its own attempt, resolved")
        if self.attempt.signed is None or not settles(planned, self.attempt, self.attempt.signed):
            raise ValueError("a venue outcome resolves its own signed attempt")
        if self.attempt.status == "applied" and not self.success:
            raise ValueError("an applied venue action is a successful outcome")
        if self.before != self.after or self.assets:
            raise ValueError("a venue action changes no HyperEVM fact")
        return self

    @property
    def applied(self) -> bool:
        return self.attempt.status == "applied"

    def strategy_result(self):
        return self.result(self.resolved_at, attempt=self.attempt.id, applied=self.applied)


class CustodyOutcome(EconomicOutcome):
    """A custody request, or an agent approval, resolved by its own HyperCore effect.

    It carries only what resolved, with its evidence, and `lifecycle.documents` takes it
    into the checkpoint's custody document, so an outcome never repeats settled history.
    """

    kind: Literal["hyperliquid_custody_outcome"] = "hyperliquid_custody_outcome"
    operation: CustodyOperation
    # The request its effect resolved, or the agent approval HyperCore applied or dropped.
    request: CustodyRequest | None = Field(default=None, exclude_if=lambda v: v is None)
    approval: Approval | None = Field(default=None, exclude_if=lambda v: v is None)

    @model_validator(mode="after")
    def evidence(self):
        validate_scope(self)
        if (self.request is None) == (self.approval is None):
            raise ValueError("a custody outcome resolves one request or one approval")
        if self.request is not None and self.request.status != "resolved":
            raise ValueError("a custody outcome names a request it resolved")
        if self.approval is not None and self.approval.status not in ("active", "dropped"):
            raise ValueError("a custody outcome names an approval it resolved")
        return self

    @property
    def applied(self) -> bool:
        """An approval HyperCore dropped resolved without applying; a request always applied."""
        return self.approval is None or self.approval.status != "dropped"

    def strategy_result(self):
        return self.result(self.after.effective_at, applied=self.applied)


def scope(target) -> ObservationScope:
    return ObservationScope(
        chain=target.fund.chain, account=target.account, allocation=target.allocation
    )


class EffectsOutcome(ExternalEffect):
    """One reconciled range of the Safe's fills, funding and ledger entries (section 6).

    It carries its pages and the effects they applied for the first time; the book they
    leave is re-derived from the checkpoint's own book (`lifecycle.documents`), so the
    outcome never repeats the whole book.
    """

    kind: Literal["hyperliquid_effects_outcome"] = "hyperliquid_effects_outcome"
    target: Target
    pages: Pages
    effects: Annotated[tuple[Effect, ...], Field(min_length=1)]
    accounting_issues: tuple[str, ...] = ()

    @model_validator(mode="after")
    def evidence(self):
        if (self.identity.connector, self.identity.scope) != (
            ConnectorId("hyperliquid"),
            scope(self.target),
        ):
            raise ValueError("HyperCore effects differ from their exact fund and chain")
        if self.pages.account.request.info.user != self.target.fund.address:  # type: ignore[union-attr]
            raise ValueError("HyperCore effects belong to the target's Safe")
        if len({e.key for e in self.effects}) != len(self.effects):
            raise ValueError("HyperCore effects are applied once each")
        return self

    def strategy_result(self):
        return EffectsResult(
            identity=self.identity,
            target=self.target,
            effects=self.effects,
            effective_at=self.effective_at,
            accounting_issues=self.accounting_issues,
        )


class Scan(ExternalScan):
    """One committed external-effects page: the HyperCore range it reconciled."""

    kind: Literal["hyperliquid_external_scan"] = "hyperliquid_external_scan"
    target: Target
    outcome: EffectsOutcome | None = None

    @model_validator(mode="after")
    def scoped(self):
        if (self.connector, self.scope) != (ConnectorId("hyperliquid"), scope(self.target)):
            raise ValueError("a HyperCore scan page keeps its exact fund and chain")
        return self


def effective(millis: int) -> datetime:
    return datetime.fromtimestamp(millis / 1000, UTC)


Outcome = Annotated[
    TransactionOutcome | VenueOutcome | CustodyOutcome | EffectsOutcome,
    Field(discriminator="kind"),
]
Work = ChainWork[DecisionRecord, Plan, PendingWork]


def perp_target(allocation) -> PerpTarget:
    """The trading document's target for an allocation: its resolved market, pinned, with
    the author's leverage, margin and slippage. The `meta` answer it was read from stays
    with the configuration's resolved evidence."""
    market = allocation.market
    return PerpTarget(
        coin=market.coin,
        asset=market.asset,
        size_decimals=market.size_decimals,
        max_leverage=market.max_leverage,
        leverage=allocation.leverage,
        slippage_bps=allocation.slippage_bps,
        margin="cross",
    )


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
    baseline: tuple[Snapshot, ...] = ()
    pending: PendingWork | None = None
    plan: Plan | None = None
    # Signed attempts released past their protocol deadline, kept as reconciliation evidence
    # until an attempt in their submission scope is reconciled.
    released: tuple[PendingWork, ...] = Field(default=(), exclude_if=lambda value: not value)
    outcomes: tuple[Outcome, ...] = ()
    external_scans: tuple[Scan, ...] = ()
    closure: tuple[Snapshot, ...] = ()
    integrity_error: str | None = None
    # The Safe's HyperCore documents: custody requests and agents, trading attempts and
    # the reconciled perp account. They change only through verified evidence.
    custody: Custody
    trading: Trading
    book: Book

    @model_validator(mode="after")
    def scope(self):
        self.schedule.validate_trigger(self.config.decision_trigger)
        validate_work(self)
        if tuple(o.target for o in self.owned) != tuple(
            self.config.target(a.name) for a in self.config.positions
        ):
            raise ValueError(
                "Hyperliquid ownership must retain each configured target exactly once"
            )
        if self.delivered > len(self.outcomes):
            raise ValueError("Hyperliquid delivery cannot precede retained execution evidence")
        validate_documents(self)
        return self


def validate_documents(state):
    (owned,) = state.owned
    safe = owned.target.fund.address
    if {state.custody.safe, state.trading.safe, state.book.safe} != {safe}:
        raise ValueError("Hyperliquid documents belong to the configured Safe")
    if state.trading.deployment != state.deployment_id:
        raise ValueError("Hyperliquid trading document belongs to this deployment")
    expected = tuple(perp_target(a) for a in state.config.allocations)
    if tuple(t.model_copy(update={"evidence": None}) for t in state.trading.targets) != expected:
        raise ValueError("Hyperliquid trading targets differ from the configured markets")
    figures = projected(state.custody, state.trading, state.book, owned.target.market.coin)
    if any(getattr(owned, name) != value for name, value in figures.items()):
        raise ValueError("Hyperliquid ownership differs from the figures its documents project")
