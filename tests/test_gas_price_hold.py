"""A fee quote above a per-transaction ceiling holds the coordinator; it never stops it.

QA saw a deployment stop on a `gas_price` refusal and stay stopped after the fee fell
to 0.03 gwei. The ceiling is a market condition to wait out, so the coordinator keeps
its decision, quotes again on later ticks and signs once the quote is within it.
"""

import json
from types import SimpleNamespace

import pytest

from almanak import runtime as runtime_module
from almanak.gateway.ethereum import DataError
from almanak.gateway.production_fees import validate_fee_limits
from almanak.runtime import (
    REPLAN_ATTEMPTS,
    TRANSIENT_HOLD_HORIZON_SECONDS,
    CeilingHold,
    ReplansExhausted,
)
from tests.test_expired_attempt_release import MemoryJournal
from tests.test_production import production_attempt as production_attempt
from tests.test_production import production_recorded as production_recorded
from tests.test_refusal_reasons import policy
from tests.test_runtime_recovery import coordinator
from tests.test_simulation_reverts import planned

ABOVE = (
    "market gas quote exceeds the configured price ceiling: quoted 5000000000 wei, "
    "max_gas_price_wei is 30000000"
)


class Stop(Exception):
    pass


@pytest.fixture
def slept(monkeypatch):
    """Sleeps advance the monotonic clock the hold measures instead of waiting."""
    sleeps = []
    monkeypatch.setattr(runtime_module.time, "sleep", sleeps.append)
    monkeypatch.setattr(runtime_module.time, "monotonic", lambda: sum(sleeps))
    return sleeps


def deciding(production_attempt):
    state, pending, _ = production_attempt
    return state.model_copy(update={"pending": None, "plan": None}), pending


def logged(capsys, prefix):
    lines = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    return [e for e in lines if e["event"].startswith(prefix)]


@pytest.mark.parametrize("kind", ["gas_price", "gas_fee_quote"])
def test_a_quote_above_the_ceiling_holds_past_the_read_horizon_then_signs(
    production_attempt, slept, capsys, kind
):
    state, pending = deciding(production_attempt)
    journal = MemoryJournal(state)
    # Long enough that a transient read hold would have stopped a running deployment.
    held = 40
    prepared = []

    def prepare(current, intent):
        prepared.append(current)
        if len(prepared) <= held:
            raise DataError(kind, ABOVE)
        return planned(current, pending).plan

    signed = []
    gateway = SimpleNamespace(
        prepare=prepare, sign=lambda: signed.append(None) or pending, close=lambda: None
    )
    runtime = coordinator(journal, gateway)

    def interrupt():
        if journal.load().pending is not None:
            raise Stop

    with pytest.raises(Stop):
        runtime.run(interrupt=interrupt)
    # Every held tick prepared the same retained decision again; nothing was signed
    # until the quote was within the ceiling.
    assert len(prepared) == held + 1
    assert all(current.decision == state.decision for current in prepared)
    assert all(current.plan is None for current in prepared)
    assert len(signed) == 1 and journal.load().pending == pending
    events = logged(capsys, "gas_ceiling_")
    *holds, recovered = events
    assert len(holds) == held
    assert {(e["status"], e["kind"], e["reason"], e["mode"]) for e in holds} == {
        ("held", kind, ABOVE, "running")
    }
    assert {e["stops_after_seconds"] for e in holds} == {None}
    assert [e["retry_in_seconds"] for e in holds[:5]] == [5, 10, 20, 40, 60]
    assert holds[-1]["elapsed_seconds"] > TRANSIENT_HOLD_HORIZON_SECONDS
    assert recovered == {
        "event": "gas_ceiling_recovered",
        "attempts": held,
        "elapsed_seconds": holds[-1]["elapsed_seconds"] + holds[-1]["retry_in_seconds"],
    }
    assert not any(e["event"].endswith("_escalated") for e in events)


def test_a_tick_refused_on_the_ceiling_commits_nothing(production_attempt):
    # One tick still raises, so an operator request (`ax`) is refused at once; only
    # the coordinator's run loop holds.
    state, _ = deciding(production_attempt)
    journal = MemoryJournal(state)

    def prepare(current, intent):
        raise DataError("gas_price", ABOVE)

    runtime = coordinator(journal, SimpleNamespace(prepare=prepare))
    with pytest.raises(CeilingHold, match="price ceiling") as held:
        runtime.tick()
    assert held.value.kind == "gas_price"
    assert journal.load() == state and runtime.replanning().attempts == {}


@pytest.mark.parametrize("kind", ["gas_limit", "gas_budget"])
def test_other_gas_policy_refusals_still_stop(production_attempt, slept, kind):
    state, _ = deciding(production_attempt)
    journal = MemoryJournal(state)

    def prepare(current, intent):
        raise DataError(kind, "exceeds the configured ceiling")

    runtime = coordinator(journal, SimpleNamespace(prepare=prepare, close=lambda: None))
    with pytest.raises(DataError) as stopped:
        runtime.run()
    assert stopped.value.kind == kind
    assert slept == [] and runtime.closed and journal.load() == state


def test_a_post_signing_ceiling_refusal_still_stops(production_attempt, slept):
    # Signing re-checks the fees preparation pinned; it never holds without a fresh quote.
    state, pending, _ = production_attempt
    reserved = planned(state, pending)
    journal = MemoryJournal(reserved)

    def sign():
        raise DataError("gas_price", ABOVE)

    runtime = coordinator(journal, SimpleNamespace(sign=sign, close=lambda: None))
    with pytest.raises(DataError, match="price ceiling") as stopped:
        runtime.run()
    assert not isinstance(stopped.value, CeilingHold)
    assert slept == [] and journal.load() == reserved


def test_ceiling_holds_between_market_refusals_never_exhaust_the_replans(production_attempt):
    # During a long hold the market can move under the retained decision. Each refusal
    # re-plans, and a held preparation proves the market checks passed again, so the
    # refusals are not consecutive and never stop the coordinator.
    state, _ = deciding(production_attempt)
    journal = MemoryJournal(state)
    answers = []

    def prepare(current, intent):
        raise answers.pop(0)

    runtime = coordinator(journal, SimpleNamespace(prepare=prepare))
    for _ in range(3 * REPLAN_ATTEMPTS):
        journal.state = state
        answers.append(DataError("price_movement", "pool price moved"))
        assert runtime.tick() is False and journal.load().decision is None
        assert runtime.replanning().attempts == {None: 1}
        journal.state = state
        answers.append(DataError("gas_price", ABOVE))
        with pytest.raises(CeilingHold):
            runtime.tick()
        assert runtime.replanning().attempts == {}


def test_a_dropped_and_redecided_decision_keeps_one_hold(production_attempt, slept, capsys):
    # An expired plan drops the decision mid-hold; the strategy decides again and the
    # hold continues. No recovery is logged until a plan is admitted.
    state, pending = deciding(production_attempt)
    journal = MemoryJournal(state)
    answers = [
        DataError("gas_price", ABOVE),
        DataError("gas_price", ABOVE),
        DataError("stale_plan", "execution observation expired"),
        DataError("gas_price", ABOVE),
    ]

    def prepare(current, intent):
        if answers:
            raise answers.pop(0)
        return planned(current, pending).plan

    gateway = SimpleNamespace(prepare=prepare, sign=lambda: pending, close=lambda: None)
    runtime = coordinator(journal, gateway)

    def interrupt():
        if journal.load().pending is not None:
            raise Stop
        if journal.load().decision is None:
            journal.state = state  # the strategy decides the same action again

    with pytest.raises(Stop):
        runtime.run(interrupt=interrupt)
    events = logged(capsys, "gas_ceiling_")
    assert [(e["event"], e.get("attempt", e.get("attempts"))) for e in events] == [
        ("gas_ceiling_hold", 1),
        ("gas_ceiling_hold", 2),
        ("gas_ceiling_hold", 3),
        ("gas_ceiling_recovered", 3),
    ]


def test_teardown_drops_a_held_new_risk_decision_and_ends_the_hold(
    production_attempt, slept, capsys
):
    state, _ = deciding(production_attempt)
    assert coordinator(MemoryJournal(state), None).connector.lifecycle.increases_exposure(
        state.decision.intent
    )
    journal = MemoryJournal(state)
    prepared = []

    def prepare(current, intent):
        prepared.append(None)
        raise DataError("gas_price", ABOVE)

    runtime = coordinator(journal, SimpleNamespace(prepare=prepare, close=lambda: None))

    def interrupt():
        if journal.load().decision is None:
            raise Stop
        if len(prepared) == 2:
            journal.state = journal.state.model_copy(update={"mode": "teardown"})

    with pytest.raises(Stop):
        runtime.run(interrupt=interrupt)
    assert len(prepared) == 2
    events = logged(capsys, "gas_ceiling_")
    assert [e["event"] for e in events] == ["gas_ceiling_hold"] * 2 + ["gas_ceiling_dropped"]


def test_a_teardown_that_keeps_requesting_an_entry_logs_each_refusal(
    production_attempt, slept, capsys
):
    state, _ = deciding(production_attempt)
    entry = state.model_copy(update={"mode": "teardown"})
    intent = entry.decision.intent
    journal = MemoryJournal(entry)
    prepared = []
    gateway = SimpleNamespace(prepare=lambda *a: prepared.append(a), close=lambda: None)
    runtime = coordinator(journal, gateway)
    assert runtime.connector.lifecycle.increases_exposure(intent)
    refusals = []

    def interrupt():
        if journal.load().decision is None:
            refusals.append(None)
            if len(refusals) == 3:
                raise Stop
            journal.state = entry  # the strategy proposes the same entry again

    with pytest.raises(Stop):
        runtime.run(interrupt=interrupt)
    assert prepared == []
    events = logged(capsys, "exposure_refused_in_teardown")
    assert [(e["intent"], e["allocation"]) for e in events] == [
        (intent.kind, intent.target.allocation)
    ] * 3


def test_signing_refusals_between_ceiling_holds_still_exhaust_the_replans(production_attempt):
    # A refusal at signing ran checks preparation does not repeat, so a held preparation
    # keeps a count that includes one, and alternating them stops at the limit.
    state, pending, _ = production_attempt
    deciding_state = state.model_copy(update={"pending": None, "plan": None})
    journal = MemoryJournal(deciding_state)

    def sign():
        raise DataError("price_movement", "pool price moved")

    def prepare(current, intent):
        raise DataError("gas_price", ABOVE)

    runtime = coordinator(journal, SimpleNamespace(sign=sign, prepare=prepare))
    for attempt in range(1, REPLAN_ATTEMPTS):
        journal.state = planned(state, pending)
        assert runtime.tick() is False and journal.load().decision is None
        journal.state = deciding_state
        with pytest.raises(CeilingHold):
            runtime.tick()
        assert runtime.replanning().attempts == {None: attempt}
    journal.state = planned(state, pending)
    with pytest.raises(ReplansExhausted):
        runtime.tick()


def test_an_op_fee_quote_above_the_transaction_ceiling_holds(production_attempt):
    # The gateway's own refusal, with L1 data and operator fees, reaches the run loop
    # as a hold, while the L2 price is within its ceiling.
    state, _ = deciding(production_attempt)
    journal = MemoryJournal(state)
    fee = policy().max_transaction_fee.amount

    def prepare(current, intent):
        validate_fee_limits(policy(), 400_000, 10**9, fee - 400_000 * 10**9 + 1)

    runtime = coordinator(journal, SimpleNamespace(prepare=prepare))
    with pytest.raises(CeilingHold, match="L1 data and operator fees") as held:
        runtime.tick()
    assert held.value.kind == "gas_fee_quote" and journal.load() == state


@pytest.mark.parametrize(
    ("fixed", "kind"),
    [(10**15, "gas_fee_quote"), (10**15 + 1, "gas_budget")],
)
def test_fixed_operator_fees_above_the_ceiling_stop_instead_of_holding(monkeypatch, fixed, kind):
    from almanak.gateway import execution, op_fees

    limits = policy(base_fee_multiplier_bps=12_500)
    backend = SimpleNamespace(config=None, reader=None)
    monkeypatch.setattr(op_fees, "fixed_exposure", lambda reader, block, gas, bps: fixed)
    monkeypatch.setattr(
        execution, "execution_gas", lambda config: SimpleNamespace(policy=lambda a: limits)
    )

    def validate(state, operation, balance, gas, price, exposure):
        validate_fee_limits(limits, gas, price, exposure)

    monkeypatch.setattr(execution, "validate_budget", validate)
    operation = SimpleNamespace(account="main")
    before = SimpleNamespace(native_balance=None, block=None)
    fees = {"gas": 400_000, "gas_price": 10**9}
    with pytest.raises(DataError) as refused:
        execution.validate_quoted_budget(backend, None, operation, before, fees, 10**15)
    assert refused.value.kind == kind
    if kind == "gas_budget":
        assert str(refused.value) == (
            "fixed operator fees exceed the transaction fee ceiling: 400000 gas costs at "
            f"least {fixed} in OP Stack operator fees, max_transaction_fee is {10**15} "
            f"{limits.asset} base units"
        )


def test_the_operator_bound_never_exceeds_the_exposure_bound():
    from almanak.gateway.op_fees import FjordParameters, operator_bound

    parameters = FjordParameters.model_construct(
        operator_fee_scalar=7, operator_fee_constant=11, revision="jovian"
    )
    # 400000 gas x 7 x 100 = 280000000, margined by 12500 bps and rounded up, plus 11.
    assert operator_bound(parameters, 400_000, 12_500) == 350_000_011
    assert operator_bound(parameters, 1, 10_001) == (700 * 10_001 + 9999) // 10000 + 11
