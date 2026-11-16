"""Shared durable orchestration with independent non-EVM ledgers and HTTP routing."""

import pytest

from almanak.gateway.ethereum import DataError
from almanak.work import project, work_for
from tests.non_evm_support import Environment


@pytest.fixture
def environment(tmp_path, monkeypatch):
    monkeypatch.setattr("pathlib.Path.home", lambda: tmp_path)
    env = Environment(tmp_path, monkeypatch)
    try:
        yield env
    finally:
        env.stop()


def entered(state):
    return len(state.outcomes) == state.delivered == 2 and all(o.units == 1 for o in state.owned)


def test_independent_entry_recovery_and_teardown_without_evm_fields(environment):
    env = environment
    failed, healthy = env.ledgers
    failed.available = False
    state = env.until(lambda s: s.delivered == 1)
    assert healthy.units == 1 and failed.units == 0
    assert work_for(state, failed.target.fund.chain).fault.kind == "transport"
    assert work_for(state, healthy.target.fund.chain).initialized
    failed.available = True
    state = env.until(entered)
    assert all(work.fault is None for work in state.work)
    assert len({o.identity.reference for o in state.outcomes}) == 1
    assert len({o.identity for o in state.outcomes}) == 2
    assert state.config.accounts.bindings[0].profile.fund.address == "SameAccount"
    env.stop()
    env.start()
    failed.available = False
    env.runtime.journal.teardown()
    state = env.until(lambda s: work_for(s, healthy.target.fund.chain).closed)
    assert state.mode == "teardown" and healthy.units == 0 and failed.units == 1
    failed.available = True
    closed = env.until(lambda s: s.mode == "closed")
    assert all(work.closed for work in closed.work)
    assert all(len(ledger.broadcasts) == 2 for ledger in env.ledgers)
    assert all(o.cost.asset.reference == "Credits" for o in closed.outcomes)
    assert all(
        "nonce" not in o.model_dump() and "gas_paid" not in o.model_dump() for o in closed.outcomes
    )


class Interrupted(Exception):
    pass


@pytest.mark.parametrize(
    "stage",
    ["reserved", "signed", "prepared", "broadcast", "receipt", "commit", "outcome", "transition"],
)
@pytest.mark.parametrize("failed_index", [0, 1])
def test_retained_attempts_survive_each_interruption_and_gateway_replacement(
    environment, stage, failed_index
):
    env = environment
    failed = env.ledgers[failed_index]
    tripped = False

    def interrupt(actual, record):
        nonlocal tripped
        if actual != stage:
            return
        target = record.operation.target
        if target == failed.target and not tripped:
            tripped = True
            raise Interrupted

    env.runtime.fault = interrupt
    with pytest.raises(Interrupted):
        env.until(entered)
    before = env.runtime.journal.load()
    local = project(before, failed.target.fund.chain)
    identity = (local.pending or local.plan).identity if local.pending or local.plan else None
    env.stop()
    failed.available = False
    env.start()
    other = env.ledgers[1 - failed_index]
    env.until(
        lambda s: (
            any(o.target == other.target and o.units == 1 for o in s.owned)
            and s.delivered == len(s.outcomes)
        )
    )
    failed.available = True
    state = env.until(entered)
    assert all(len(ledger.broadcasts) == 1 for ledger in env.ledgers)
    if identity is not None:
        assert sum(o.identity == identity for o in state.outcomes) == 1
    assert len(state.memory.values) == 2


def test_invalidated_history_halts_only_that_chain(environment):
    env = environment
    env.until(entered)
    failed, healthy = env.ledgers
    failed.invalidated = True
    env.runtime.journal.teardown()
    state = env.until(lambda s: work_for(s, healthy.target.fund.chain).closed)
    assert work_for(state, failed.target.fund.chain).fault.invalidated
    assert state.integrity_error is None and state.mode == "teardown"
    failed.invalidated = False
    for _ in range(4):
        env.runtime.tick()
    assert failed.units == 1 and len(failed.broadcasts) == 1


def test_chain_route_and_cross_chain_mutation_are_refused(environment):
    env = environment
    env.until(entered)
    first, second = (ledger.target.fund.chain for ledger in env.ledgers)
    with pytest.raises(DataError, match="refused"):
        env.runtime.gateway.scoped("observe")
    state = env.runtime.journal.load()
    other = work_for(state, second).model_copy(update={"initialized": False})
    changed = state.model_copy(update={"work": (state.work[0], other)})
    with pytest.raises(DataError, match="refused"):
        env.runtime.gateway.scoped("update", chain=first, before=state, after=changed)
    assert env.runtime.journal.load() == state


def test_shared_storage_and_strategy_failures_stop_progress(environment, monkeypatch):
    env = environment

    def unavailable():
        raise DataError("gateway_unavailable", "journal unavailable")

    monkeypatch.setattr(env.runtime.journal, "load", unavailable)
    with pytest.raises(DataError, match="journal unavailable"):
        env.runtime.tick()
    assert all(not ledger.broadcasts for ledger in env.ledgers)


@pytest.mark.parametrize("kind", ["execution_failed", "accounting"])
def test_retained_local_failures_are_visible_and_allow_teardown(environment, kind):
    env = environment
    state = env.until(entered)
    outcome = state.outcomes[0].model_copy(
        update={
            "success": kind != "execution_failed",
            "accounting_issues": ("unexplained movement",) if kind == "accounting" else (),
        }
    )
    failed = outcome.operation.target.fund.chain
    # Inject retained chain evidence at the fixture journal boundary. Public
    # clients cannot fabricate outcomes through the independently fenced service.
    env.service.journal.update(
        lambda current: current.model_copy(update={"outcomes": (outcome, *current.outcomes[1:])})
    )
    state = env.until(lambda s: work_for(s, failed).fault is not None)
    assert work_for(state, failed).fault.kind == kind
    assert state.integrity_error is None
    env.runtime.journal.teardown()
    closed = env.until(lambda s: s.mode == "closed")
    assert all(w.closed and w.fault is None for w in closed.work)


def test_one_interval_limits_evaluations_across_chain_routes(environment):
    from datetime import timedelta

    env = environment
    state = env.until(entered)
    schedule = state.schedule
    env.now = schedule.next_due_at - timedelta(microseconds=1)
    for _ in range(4):
        env.runtime.tick()
    assert env.runtime.journal.load().schedule == schedule
    env.now = schedule.next_due_at
    for _ in range(4):
        env.runtime.tick()
    advanced = env.runtime.journal.load().schedule
    assert advanced.last_evaluation.started_at == env.now
    assert advanced.next_due_at == env.now + timedelta(seconds=5)
    env.stop()
    env.start()
    assert env.runtime.journal.load().schedule == advanced


def test_an_idle_multichain_coordinator_waits_once_every_chain_is_idle(environment):
    from datetime import timedelta

    from almanak.runtime import IDLE_POLL_SECONDS, next_delay

    env = environment
    state = env.until(entered)
    due = state.schedule.next_due_at
    env.now = due - timedelta(seconds=60)
    # Each tick advances one chain; the coordinator idles only after the whole cycle.
    env.runtime.tick()
    assert env.runtime.idle is None and next_delay(env.runtime, 1, 0) == 1
    env.runtime.tick()
    committed, at = env.runtime.idle
    assert at == due and committed == env.runtime.journal.load()
    assert next_delay(env.runtime, 1, 0) == IDLE_POLL_SECONDS
    env.runtime.tick()
    assert env.runtime.idle is None
    # A chain with work ends the cycle, so the next idle wait needs a whole idle cycle again.
    env.now = due
    env.runtime.tick()
    assert env.runtime.idle is None and env.runtime.idle_chains == {}
    env.now = due + timedelta(seconds=1)
    env.runtime.tick()
    assert env.runtime.idle is None
    env.runtime.tick()
    assert env.runtime.idle[1] == env.runtime.journal.load().schedule.next_due_at
