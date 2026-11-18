"""Test-only economic connector and ledger. No EVM addresses, nonces or gas fields."""

import secrets
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Literal
from uuid import uuid4

from pydantic import model_validator

from almanak.caip import AssetType, ChainId
from almanak.chains import ChainReference, CheckpointReference, TransactionReference
from almanak.gateway.client import GatewayClient
from almanak.gateway.ethereum import DataError
from almanak.gateway.routing import ChainRouter
from almanak.gateway.service import GatewayService, http_server
from almanak.models import Account, AssetAmount, Chain, Model, UInt
from almanak.runtime import Runtime
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import DecisionContext, Memory, OutcomeContext, StrategyDecision
from almanak.triggers import DecisionSchedule, IntervalTrigger, TriggerEvent
from almanak.work import ChainWork, ObservationScope, initial_work, validate_work


class Profile(Model):
    fund: Account
    kind: Literal["test_only"] = "test_only"

    @property
    def submitter(self):
        return self.fund


class Binding(Model):
    name: str
    profile: Profile


class Accounts(Model):
    bindings: tuple[Binding, ...]

    def binding(self, name):
        return next(b for b in self.bindings if b.name == name)


class Target(Model):
    allocation: str
    account: str
    fund: Account


class Allocation(Model):
    name: str
    account: str
    chain: Chain


class Config(Model):
    connector: Literal["test_only"] = "test_only"
    accounts: Accounts
    allocations: tuple[Allocation, ...]
    production: None = None
    decision_trigger: IntervalTrigger = IntervalTrigger(kind="interval", seconds=5)

    @property
    def positions(self):
        return self.allocations

    def target(self, name):
        item = next(a for a in self.allocations if a.name == name)
        return Target(
            allocation=name,
            account=item.account,
            fund=self.accounts.binding(item.account).profile.fund,
        )


class Owned(Model):
    target: Target
    units: UInt


class Snapshot(Model):
    target: Target
    block: CheckpointReference
    units: UInt

    @property
    def allocation(self):
        return self.target.allocation


class Intent(Model):
    kind: Literal["open", "close"]
    target: Target

    @property
    def allocation(self):
        return self.target.allocation


class DecisionRecord(Model):
    identity: str
    trigger: TriggerEvent
    intent: Intent
    observations: tuple[Snapshot, ...]
    memory: Memory
    required_observations: tuple[ObservationScope, ...] = ()


class Plan(Model):
    attempt_id: str
    decision: DecisionRecord
    operation: Intent
    before: Snapshot
    identity: TransactionReference


class Pending(Plan):
    signed_message: str


class Result(Model):
    intent: Intent
    success: bool = True
    complete: bool = True
    identity: TransactionReference


class Outcome(Model):
    decision: DecisionRecord
    operation: Intent
    identity: TransactionReference
    before: Snapshot
    after: Snapshot
    success: bool = True
    accounting_issues: tuple[str, ...] = ()
    cost: AssetAmount

    def strategy_result(self):
        return Result(intent=self.operation, identity=self.identity, success=self.success)


Work = ChainWork[DecisionRecord, Plan, Pending]


class State(Model):
    schema_version: Literal[1] = 1
    deployment_id: str
    config: Config
    strategy: StrategyDefinition
    capabilities: Model
    permissions: None = None
    mode: Literal["running", "teardown", "closed"] = "running"
    initialized: bool = False
    work: tuple[Work, ...]
    scheduler_cursor: UInt = 0
    schedule: DecisionSchedule = DecisionSchedule()
    memory: Memory = Memory()
    delivered: UInt = 0
    decision: DecisionRecord | None = None
    plan: Plan | None = None
    pending: Pending | None = None
    owned: tuple[Owned, ...]
    baseline: tuple[Snapshot, ...] = ()
    closure: tuple[Snapshot, ...] = ()
    outcomes: tuple[Outcome, ...] = ()
    integrity_error: str | None = None

    @model_validator(mode="after")
    def scope(self):
        self.schedule.validate_trigger(self.config.decision_trigger)
        validate_work(self)
        return self


def initial(config, strategy, capabilities, permissions):
    return State(
        deployment_id=uuid4().hex,
        config=config,
        strategy=strategy,
        capabilities=capabilities,
        work=initial_work(config, Work),
        owned=tuple(Owned(target=config.target(a.name), units=0) for a in config.allocations),
    )


def validate_owned(state, observed):
    assert tuple((o.target, o.units) for o in state.owned) == tuple(
        (o.target, o.units) for o in observed
    )


def validate_closure(state, observed):
    validate_owned(state, observed)
    if any(o.units for o in observed):
        raise ValueError("open test-ledger obligation")


def commit_owned(owned, outcome):
    return tuple(
        Owned(target=o.target, units=outcome.after.units)
        if o.target == outcome.operation.target
        else o
        for o in owned
    )


def connector():
    capability = SimpleNamespace(
        validate=lambda *_: None, admit=lambda *_: None, evaluate=lambda *_: Model()
    )
    codec = SimpleNamespace(
        intent=Intent,
        checkpoint=State,
        schema_version=1,
        capability=capability,
        snapshot=Snapshot,
        plan=Plan,
        pending=Pending,
        outcome=Outcome,
        decision_record=DecisionRecord,
        decision_input=DecisionContext[Config, Owned, Snapshot],
        outcome_input=OutcomeContext[Config, Result],
    )
    lifecycle = SimpleNamespace(
        initial=initial,
        validate_initial=validate_owned,
        validate_owned=validate_owned,
        validate_closure=validate_closure,
        commit_owned=commit_owned,
        ownership=ownership,
        increases_exposure=lambda intent: intent.kind == "open",
    )
    return SimpleNamespace(
        codec=codec,
        lifecycle=lifecycle,
        permissions=SimpleNamespace(manifest_type=Model),
        requests=None,
        external_effects=None,
    )


def ownership(state, outcome=None):
    return {"owned": state.owned if outcome is None else commit_owned(state.owned, outcome)}


@dataclass
class Ledger:
    target: Target
    available: bool = True
    invalidated: bool = False
    height: int = 0
    units: int = 0
    broadcasts: list = field(default_factory=list)
    receipts: dict = field(default_factory=dict)
    snapshots: dict = field(default_factory=dict)

    def observation(self):
        block = CheckpointReference(
            chain=self.target.fund.chain,
            reference=ChainReference(f"Checkpoint-{self.height}"),
            timestamp=self.height,
        )
        result = Snapshot(target=self.target, block=block, units=self.units)
        self.snapshots[block.reference] = result
        return result


class Adapter:
    def __init__(self, ledger):
        self.ledger = ledger

    def guard(self):
        if not self.ledger.available:
            raise DataError("transport", "test ledger unavailable")

    def close(self):
        pass

    def checkpoint(self):
        self.guard()
        return self.ledger.observation().block

    def transaction_reference(self, record):
        return record.identity

    def unsigned_plan(self, pending):
        return Plan.model_validate(pending.model_dump(exclude={"signed_message"}))

    def authorize_submission(self, state):
        self.guard()

    def advance_family(self, state, action, identity, index):
        raise DataError("replacement_unsupported", "test ledger does not support replacement")

    def submit(self, state):
        return state, self.broadcast(state.pending, state.permissions)

    def execution_cost(self, pending, receipt):
        return AssetAmount(
            asset=AssetType(f"{self.ledger.target.fund.chain.chain_id}/token:Credits"), amount=1
        )

    def audit_history(self, state):
        self.guard()
        if self.ledger.invalidated:
            raise DataError("reorganization", "test ledger invalidated its committed history")

    def observe(self, state=None):
        self.guard()
        return (self.ledger.observation(),)

    def snapshot(self, name, block, reference=None):
        self.guard()
        if name != self.ledger.target.allocation or block.chain != self.ledger.target.fund.chain:
            raise ValueError("wrong ledger observation route")
        return self.ledger.snapshots[block.reference]

    def verify_snapshot(self, observation):
        return self.snapshot(observation.allocation, observation.block)

    def prepare(self, state, intent, before, attempt_id):
        self.guard()
        if intent.target != self.ledger.target:
            raise ValueError("wrong ledger execution route")
        return Plan(
            attempt_id=attempt_id,
            decision=state.decision,
            operation=intent,
            before=before,
            identity=TransactionReference(
                chain=intent.target.fund.chain,
                reference=ChainReference(f"Transfer-{self.ledger.height + 1}"),
            ),
        )

    def sign(self, state):
        self.guard()
        return Pending(
            **state.plan.model_dump(), signed_message="authenticated:" + state.plan.attempt_id
        )

    def verify_signed_admission(self, state, pending):
        return self.sign(state) == pending

    def prepare_submission(self, state):
        self.guard()

    def broadcast(self, pending, permissions=None):
        self.guard()
        if pending.identity in self.ledger.receipts:
            return False
        self.ledger.units = 1 if pending.operation.kind == "open" else 0
        self.ledger.height += 1
        self.ledger.broadcasts.append(pending.identity)
        self.ledger.receipts[pending.identity] = Outcome(
            decision=pending.decision,
            operation=pending.operation,
            identity=pending.identity,
            before=pending.before,
            after=self.ledger.observation(),
            cost=self.execution_cost(pending, None),
        )
        return True

    def receipt(self, pending):
        self.guard()
        return (
            {"identity": str(pending.identity.reference)}
            if pending.identity in self.ledger.receipts
            else None
        )

    def reconcile(self, pending, receipt):
        self.guard()
        return self.ledger.receipts[pending.identity]


def configuration():
    chains = tuple(
        Chain(chain_id=ChainId(f"testing:{name}"), environment="isolated")
        for name in ("Alpha", "Beta")
    )
    accounts = Accounts(
        bindings=tuple(
            Binding(
                name=str(c.chain_id), profile=Profile(fund=Account(chain=c, address="SameAccount"))
            )
            for c in chains
        )
    )
    return Config(
        accounts=accounts,
        allocations=tuple(
            Allocation(name=b.name, account=b.name, chain=b.profile.fund.chain)
            for b in accounts.bindings
        ),
    )


def decide(_strategy, context):
    observed = context.observations[0]
    if context.teardown:
        intent = Intent(kind="close", target=observed.target) if observed.units else None
    else:
        intent = None if observed.units else Intent(kind="open", target=observed.target)
    return StrategyDecision[Intent](memory=context.memory, intent=intent)


def outcome_memory(request):
    result = request.context.result
    memory = request.context.memory
    return Memory(
        values={**memory.values, result.intent.target.allocation: result.intent.kind}
    ).model_dump_json()


class Environment:
    def __init__(self, path, monkeypatch):
        self.now = datetime(2026, 9, 6, tzinfo=UTC)
        self.config = configuration()
        self.path = path
        self.connector = connector()
        self.ledgers = [Ledger(self.config.target(a.name)) for a in self.config.allocations]
        self.token = path / "auth"
        self.token.write_text(secrets.token_hex(32))
        self.token.chmod(0o600)
        for module in (
            "runtime",
            "request_runtime",
            "gateway.service",
            "gateway.client",
            "connectors.composition",
        ):
            monkeypatch.setattr(f"almanak.{module}.connector_for", lambda _: self.connector)
        for module in ("connectors.composition", "gateway.client"):
            monkeypatch.setattr(f"almanak.{module}.checkpoint_from_json", State.model_validate_json)
        monkeypatch.setattr(
            "almanak.gateway.client.configuration_from_json", Config.model_validate_json
        )
        monkeypatch.setattr("almanak.runtime.strategy_decision", decide)
        monkeypatch.setattr("almanak.runtime.strategy_worker", outcome_memory)
        self.start(create=True)

    def start(self, *, create=False, fault=None):
        router = ChainRouter(
            self.config,
            {
                ledger.target.fund.chain: (lambda ledger=ledger: Adapter(ledger))
                for ledger in self.ledgers
            },
        )
        self.service = GatewayService(
            self.config, self.path / "state.sqlite", router, lease_seconds=30
        )
        if create:
            source = self.path / "strategy.py"
            source.write_text("def decide(context): pass\n")
            self.service.journal.create(self.config, StrategyDefinition.read(source))
        self.server = http_server(self.service, self.token, 0)
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()
        client = GatewayClient(f"http://127.0.0.1:{self.server.server_port}", self.token)
        client.acquire()
        self.runtime = object.__new__(Runtime)
        self.runtime.gateway = self.runtime.journal = client
        self.runtime.connector = self.connector
        self.runtime.closed = False
        self.runtime.clock = lambda: self.now
        self.runtime.fault = fault or (lambda *_: None)

    def stop(self):
        self.runtime.close()
        self.server.shutdown()
        self.thread.join(timeout=5)
        self.server.server_close()
        self.service.backend.close()

    def until(self, predicate, limit=100):
        for _ in range(limit):
            state = self.runtime.journal.load()
            if predicate(state):
                return state
            self.now += timedelta(seconds=5)
            self.runtime.tick()
        raise AssertionError("test runtime did not reach the expected state")
