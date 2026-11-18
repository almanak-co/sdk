"""Component recovery against captured receipts, without signing or chain access."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest

from almanak.connectors.aave_v4.reader import plan_call, plan_operation
from almanak.connectors.aave_v4.state import Pending, TransactionPlan
from almanak.connectors.composition import connector_for
from almanak.gateway.ethereum import DataError
from almanak.gateway.fork_execution import ForkGateway
from almanak.runtime import Runtime
from almanak.state import Journal


def pending_for(config, outcome):
    allocation = config.allocation(outcome.operation.allocation)
    fund = config.accounts.binding(outcome.operation.account).profile.fund
    return Pending(
        attempt_id=uuid4().hex,
        decision=outcome.decision,
        operation=outcome.operation,
        before=outcome.before,
        call=plan_call(allocation, fund, outcome.operation),
        nonce=outcome.nonce,
        gas_estimate=int(outcome.receipt["gasUsed"], 16),
        gas=int(outcome.receipt["gasUsed"], 16),
        gas_price=int(outcome.receipt["effectiveGasPrice"], 16),
        raw_transaction="0x00",  # Synthetic bytes never reach signing or submission.
        tx_hash=outcome.tx_hash,
    )


def receipt_gateway(config, after):
    gateway = object.__new__(ForkGateway)
    gateway.config = config
    gateway.guard = lambda: None
    gateway.reader = SimpleNamespace(pin_block=lambda number: after.position.reserve.block)
    gateway.snapshot = lambda account, block, reference=None: after
    return gateway


class CapturedGateway:
    def __init__(self, journal, recorded):
        self.journal = journal
        self.recorded = recorded
        self.confirmed = {}
        self.broadcasts = []

    def observe(self):
        if self.confirmed:
            return (list(self.confirmed.values())[-1].after,)
        return self.recorded.baseline

    def verify_snapshot(self, observation):
        candidates = (
            *self.recorded.baseline,
            *(o.before for o in self.recorded.outcomes),
            *(o.after for o in self.recorded.outcomes),
        )
        return next(
            s for s in candidates if s.target == observation.target and s.block == observation.block
        )

    def outcome(self, kind):
        return next(item for item in self.recorded.outcomes if item.operation.kind == kind)

    def prepare(self, state, intent):
        before = next(item for item in self.observe() if item.target == intent.target)
        operation = plan_operation(intent, before)
        pending = pending_for(state.config, self.outcome(operation.kind))
        pending = pending.model_copy(update={"decision": state.decision})
        return TransactionPlan.model_validate(
            pending.model_dump(exclude={"raw_transaction", "tx_hash"})
        )

    def sign(self):
        plan = self.journal.load().plan
        outcome = self.outcome(plan.operation.kind)
        return Pending(**plan.model_dump(), raw_transaction="0x00", tx_hash=outcome.tx_hash)

    def receipt(self, pending):
        outcome = self.confirmed.get(pending.tx_hash)
        return None if outcome is None else outcome.receipt

    def broadcast(self, pending):
        if pending.tx_hash in self.confirmed:
            return False
        self.broadcasts.append(pending.tx_hash)
        self.confirmed[pending.tx_hash] = self.outcome(pending.operation.kind)
        return True

    def reconcile(self, pending, receipt):
        outcome = self.confirmed[pending.tx_hash]
        return receipt_gateway(self.recorded.config, outcome.after).reconcile(pending, receipt)


class Interrupted(Exception):
    pass


def coordinator(journal, gateway, fault=None, clock=None):
    # Construct only the orchestration component; HTTP authentication is tested separately.
    runtime = object.__new__(Runtime)
    runtime.connector = connector_for(journal.load().config)
    runtime.journal = journal
    runtime.gateway = gateway
    runtime.closed = False
    runtime.clock = clock or (lambda: datetime(2026, 9, 5, tzinfo=UTC))
    runtime.fault = fault or (lambda *args: None)
    return runtime


def crash_at(expected):
    def fault(stage, attempt):
        if stage == expected and attempt.operation.kind == "supply":
            raise Interrupted(stage)

    return fault


def tick_until(runtime, predicate):
    for _ in range(60):
        runtime.tick()
        state = runtime.journal.load()
        if predicate(state):
            return state
    raise AssertionError("component lifecycle did not reach the expected state")


@pytest.mark.parametrize(
    "stage",
    ["reserved", "signed", "prepared", "broadcast", "receipt", "commit", "outcome", "transition"],
)
def test_crash_recovery_commits_one_supply_and_withdraws(
    tmp_path, recorded, reference_strategy, stage
):
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(recorded.config, reference_strategy)
    gateway = CapturedGateway(journal, recorded)
    interrupted = coordinator(journal, gateway, crash_at(stage))
    with pytest.raises(Interrupted, match=stage):
        tick_until(interrupted, lambda state: False)
    resumed = coordinator(Journal(journal.path), gateway)
    holding = tick_until(
        resumed,
        lambda state: bool(state.memory.values.get("supplied_at")) and state.pending is None,
    )
    assert holding.owned[0].shares == gateway.outcome("supply").shares
    journal.teardown()
    closed = tick_until(resumed, lambda state: state.mode == "closed")
    assert [item.model_dump(exclude={"decision"}) for item in closed.outcomes] == [
        item.model_dump(exclude={"decision"}) for item in recorded.outcomes
    ]
    assert gateway.broadcasts == [item.tx_hash for item in recorded.outcomes]
    assert closed.owned[0].shares == 0


@pytest.mark.parametrize("stage", ["reserved", "signed", "broadcast"])
def test_teardown_cancels_only_unsigned_new_risk(tmp_path, recorded, reference_strategy, stage):
    journal = Journal(tmp_path / "state.sqlite")
    journal.create(recorded.config, reference_strategy)
    gateway = CapturedGateway(journal, recorded)
    runtime = coordinator(journal, gateway, crash_at(stage))
    with pytest.raises(Interrupted):
        tick_until(runtime, lambda state: False)
    journal.teardown()
    runtime = coordinator(journal, gateway)
    if stage == "broadcast":
        closed = tick_until(runtime, lambda state: state.mode == "closed")
        assert [item.model_dump(exclude={"decision"}) for item in closed.outcomes] == [
            item.model_dump(exclude={"decision"}) for item in recorded.outcomes
        ]
    else:
        runtime.tick()
        assert journal.load().plan is None
        assert gateway.broadcasts == [gateway.outcome("approve").tx_hash]


@pytest.mark.parametrize("kind", ["approve", "supply", "withdraw"])
def test_receipt_reconciliation_matches_captured_economics(recorded, kind):
    outcome = next(item for item in recorded.outcomes if item.operation.kind == kind)
    pending = pending_for(recorded.config, outcome)
    gateway = receipt_gateway(recorded.config, outcome.after)
    assert gateway.reconcile(pending, outcome.receipt) == outcome.model_copy(
        update={"call": pending.call}
    )


@pytest.mark.parametrize("field", ["allowance", "shares", "debt"])
def test_receipt_reconciliation_refuses_unexplained_balance_changes(recorded, field):
    outcome = next(item for item in recorded.outcomes if item.operation.kind == "supply")
    after = changed_balance(outcome.after, field)
    gateway = receipt_gateway(recorded.config, after)
    with pytest.raises(DataError, match="reconcile|owned shares"):
        gateway.reconcile(pending_for(recorded.config, outcome), outcome.receipt)


@pytest.mark.parametrize("field", ["wallet_balance", "native_balance"])
def test_external_movements_preserve_protocol_effects_and_expose_accounting(recorded, field):
    outcome = next(item for item in recorded.outcomes if item.operation.kind == "supply")
    after = changed_balance(outcome.after, field)
    gateway = receipt_gateway(recorded.config, after)
    actual = gateway.reconcile(pending_for(recorded.config, outcome), outcome.receipt)
    assert (actual.success, actual.assets, actual.shares) == (True, outcome.assets, outcome.shares)
    assert len(actual.accounting_issues) == 1


def changed_balance(after, field):
    if field in ("wallet_balance", "allowance"):
        return after.model_copy(update={field: getattr(after, field) + 1})
    if field == "native_balance":
        native = after.native_balance.model_copy(update={"amount": after.native_balance.amount + 1})
        return after.model_copy(update={"native_balance": native})
    key = "supplied_shares" if field == "shares" else "total_debt"
    changes = {key: getattr(after.position, key) + 1}
    if field == "debt":
        changes["drawn_debt"] = after.position.drawn_debt + 1
    position = after.position.model_copy(update=changes)
    return after.model_copy(update={"position": position})


def test_reverted_receipt_records_only_gas(recorded):
    outcome = next(item for item in recorded.outcomes if item.operation.kind == "supply")
    native = outcome.before.native_balance.model_copy(
        update={"amount": outcome.before.native_balance.amount - outcome.gas_paid.amount}
    )
    after = outcome.before.model_copy(update={"native_balance": native})
    reserve = after.position.reserve.model_copy(
        update={"block": outcome.after.position.reserve.block}
    )
    after = after.model_copy(
        update={"position": after.position.model_copy(update={"reserve": reserve})}
    )
    receipt = {**outcome.receipt, "status": "0x0", "logs": []}
    result = receipt_gateway(recorded.config, after).reconcile(
        pending_for(recorded.config, outcome), receipt
    )
    assert (result.success, result.assets, result.shares) == (False, 0, 0)
    assert result.gas_paid == outcome.gas_paid


@pytest.fixture
def admission(tmp_path, recorded):
    from almanak.gateway.service import GatewayService

    outcome = next(item for item in recorded.outcomes if item.operation.kind == "supply")
    pending = pending_for(recorded.config, outcome)
    plan = TransactionPlan.model_validate(
        pending.model_dump(exclude={"raw_transaction", "tx_hash"})
    )
    before = recorded.model_copy(
        update={"mode": "running", "outcomes": (), "delivered": 0, "decision": plan.decision}
    )
    backend = SimpleNamespace(
        unsigned_plan=lambda pending: TransactionPlan.model_validate(
            pending.model_dump(exclude={"raw_transaction", "tx_hash"})
        ),
        transaction_reference=lambda item: (item.operation.target.fund.chain, item.tx_hash),
        snapshot=lambda *args: plan.before,
        prepare=lambda *args: plan,
        verify_signed_admission=lambda state, signed: signed == pending,
        receipt=lambda *args: outcome.receipt,
        reconcile=lambda *args: outcome,
    )
    service = GatewayService(
        recorded.config, tmp_path / "admission.sqlite", backend, lease_seconds=2
    )
    return SimpleNamespace(
        service=service, before=before, plan=plan, pending=pending, outcome=outcome
    )


def test_gateway_accepts_reserved_signed_and_resolved_transitions(admission):
    reserved = admission.before.model_copy(update={"plan": admission.plan})
    admitted = reserved.model_copy(update={"plan": None, "pending": admission.pending})
    from almanak.runtime import commit_outcome

    resolved = commit_outcome(admitted, admission.pending, admission.outcome)
    admission.service.validate_update(admission.before, reserved)
    admission.service.validate_update(reserved, admitted)
    admission.service.validate_update(admitted, resolved)


@pytest.mark.parametrize("change", ["observation", "preparation"])
def test_gateway_refuses_plan_that_differs_from_verification(admission, change):
    if change == "observation":
        admission.service.backend.snapshot = lambda *args: admission.outcome.after
    else:
        altered = admission.plan.model_copy(update={"nonce": admission.plan.nonce + 1})
        admission.service.backend.prepare = lambda *args: altered
    after = admission.before.model_copy(update={"plan": admission.plan})
    part = "snapshot" if change == "observation" else "nonce"
    with pytest.raises(ValueError, match=f"verified gateway preparation: .*{part}"):
        admission.service.validate_update(admission.before, after)


@pytest.mark.parametrize("change", ["unreserved", "nonce", "signature"])
def test_gateway_refuses_signed_attempt_without_exact_authorized_plan(admission, change):
    before = admission.before.model_copy(update={"plan": admission.plan})
    pending = admission.pending
    if change == "unreserved":
        before = admission.before
    elif change == "nonce":
        pending = pending.model_copy(update={"nonce": pending.nonce + 1})
    else:
        pending = pending.model_copy(update={"raw_transaction": "0x01"})
    after = before.model_copy(update={"plan": None, "pending": pending})
    with pytest.raises(ValueError, match="signed transaction differs"):
        admission.service.validate_update(before, after)


@pytest.mark.parametrize("outcomes", [(), ("wrong_hash",)])
def test_gateway_cannot_drop_pending_without_its_outcome(admission, outcomes):
    before = admission.before.model_copy(update={"pending": admission.pending})
    rows = tuple(
        admission.outcome.model_copy(update={"tx_hash": "0x" + "00" * 32}) for _ in outcomes
    )
    after = before.model_copy(update={"pending": None, "outcomes": rows})
    with pytest.raises(
        ValueError,
        match="recorded outcome|independently reconciled chain evidence|cannot release signed work",
    ):
        admission.service.validate_update(before, after)


@pytest.mark.parametrize("change", ["outcomes", "delivered", "mode"])
def test_gateway_cannot_erase_completed_evidence(recorded, admission, change):
    replacement = {"outcomes": (), "delivered": 0, "mode": "running"}[change]
    after = recorded.model_copy(update={change: replacement})
    with pytest.raises(ValueError, match="erase execution evidence or reopen"):
        admission.service.validate_update(recorded, after)
