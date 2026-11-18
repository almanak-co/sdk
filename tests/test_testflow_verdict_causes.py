"""The test verdict names why a lifecycle held, failed or ran out of time."""

import gzip
import json
import time
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from pydantic import BaseModel

from almanak import decision_facts, testflow
from almanak.caip import AssetType
from almanak.cli import error_document
from almanak.connectors import pool_lending_lifecycle
from almanak.connectors.compound_v3 import lifecycle as compound_lifecycle
from almanak.connectors.morpho_blue import lifecycle as morpho_lifecycle
from almanak.connectors.repayment import coverage
from almanak.gateway.ethereum import DataError
from almanak.gateway.service import data_error_reply, differing
from almanak.supervisor import Supervisor
from almanak.worker_session import WorkerSession, WorkerSessionTimeout, worker_timed_out
from tests.test_testflow_lifecycle import FIXTURES, Process, sequence, started, workspace

USDC = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    monkeypatch.setattr(testflow, "POLL_SECONDS", 0)


class Entry(BaseModel):
    entry_quote_error: str | None = None
    risk: dict | None = None

    @property
    def allocation(self):
        return "lp"


def test_observation_errors_name_the_allocation_and_field():
    quiet = SimpleNamespace(allocation="swap")
    blocked = Entry(
        entry_quote_error="entry quote unavailable: the pool holds no liquidity",
        risk={"oracle_error": "x" * 900, "positions": [{"risk_error": ""}]},
    )
    rows = decision_facts.observation_errors((quiet, blocked, Entry()))
    assert rows == [
        {
            "allocation": "lp",
            "field": "entry_quote_error",
            "message": "entry quote unavailable: the pool holds no liquidity",
        },
        {"allocation": "lp", "field": "risk.oracle_error", "message": "x" * 500},
    ]
    assert "observation_errors" in decision_facts.VERDICT_FIELDS


def test_a_held_row_carries_its_observation_errors(tmp_path):
    (tmp_path / "logs").mkdir()
    errors = [{"allocation": "lp", "field": "entry_quote_error", "message": "empty pool"}]
    line = {"event": "decision_evaluated", "time": "t1", "observation_errors": errors}
    (tmp_path / "logs" / "coordinator.log").write_text(json.dumps(line) + "\n")
    row = testflow.with_facts(tmp_path, {"time": "t1", "result": "hold"})
    assert row["observation_errors"] == errors


@pytest.mark.parametrize(
    ("passed", "entered", "status"),
    [(True, True, "entered"), (True, False, "not_entered"), (False, True, "failed")],
)
def test_status_tells_an_entered_lifecycle_from_one_that_only_held(passed, entered, status):
    verdict = {**testflow.empty_verdict(), "passed": passed, "entered": entered}
    assert testflow.status(verdict) == status


def test_the_real_lifecycle_lists_teardown_actions_apart_from_decisions(monkeypatch):
    from tests.lending_captures import current_capture

    raw = current_capture(
        json.loads(gzip.decompress((FIXTURES / "aave-v3-safe-lifecycle.json.gz").read_bytes()))
    )
    rows = testflow.transactions(raw)
    assert [row["trigger"] for row in rows] == ["decision"] * 5 + ["teardown"] * 7
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: raw)
    verdict = testflow.empty_verdict()
    testflow.record_closure(Path("."), verdict)
    assert verdict["executed"] == ["supply", "collateral", "borrow"]
    assert verdict["teardown_executed"] == ["repay_amount", "withdraw_amount", "repay", "withdraw"]
    assert verdict["closed"] is True and verdict["repayment"] == []
    assert testflow.executed(raw) == (verdict["executed"], verdict["after_entry"])


def test_closure_reports_the_latest_repayment_coverage(monkeypatch):
    short = coverage("loan", USDC, None, 281_215_217, 281_000_000)
    state = SimpleNamespace(
        mode="teardown",
        outcomes=(),
        repayment=(short,),
        integrity_error=None,
        pending=None,
        plan=None,
        delivered=0,
        work=(),
    )
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: {})
    monkeypatch.setattr(testflow, "decoded", lambda wire: state)
    verdict = testflow.empty_verdict()
    testflow.record_closure(Path("."), verdict)
    [row] = verdict["repayment"]
    assert (row["allocation"], row["debt"], row["available"], row["shortfall"]) == (
        "loan",
        281_215_217,
        281_000_000,
        215_217,
    )


def remaining_refusal():
    return DataError(
        "remaining_obligations",
        "Pool teardown retains supply, debt, collateral or authority: loan debt_scaled 5",
        details={"remaining": [{"allocation": "loan", "asset": str(USDC), "debt_scaled": "5"}]},
    )


def test_a_teardown_refusal_carries_its_remaining_obligations(tmp_path, monkeypatch):
    refusal = remaining_refusal()
    assert data_error_reply(refusal)["details"] == refusal.details
    root = workspace(tmp_path, {"coordinator": {"pid": 1}})
    (root / "logs" / "coordinator.log").write_text(json.dumps(error_document(refusal)) + "\n")
    monkeypatch.setattr("almanak.supervisor_process.running", lambda record: False)
    monkeypatch.setattr("almanak.workspace.request", lambda root, action: {})
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: {"mode": "teardown"})
    monkeypatch.setattr(testflow, "record_closure", lambda root, verdict: None)
    verdict = testflow.empty_verdict()
    testflow.teardown(root, verdict)
    [entry] = verdict["failures"]
    assert (entry["stage"], entry["kind"], entry["class"]) == (
        "teardown",
        "remaining_obligations",
        "strategy",
    )
    assert entry["details"] == refusal.details
    assert "details" not in error_document(DataError("rpc", "x"))


def test_pool_closure_names_each_remaining_obligation(monkeypatch):
    monkeypatch.setattr(pool_lending_lifecycle, "validate_owned", lambda state, observed: None)

    def observed(allocation, allowance=0, configuration=0, supplied=0, debt=0):
        position = SimpleNamespace(
            user_configuration=configuration, supplied_assets=supplied, debt_assets=debt
        )
        target = SimpleNamespace(allocation=allocation, asset=USDC)
        return SimpleNamespace(
            allocation=allocation, target=target, allowance=allowance, position=position
        )

    def owned(supply=0, debt=0, collateral=False):
        return SimpleNamespace(
            supply_scaled=supply, debt_scaled=debt, collateral_enabled=collateral
        )

    clear = SimpleNamespace(owned=(owned(), owned()))
    pool_lending_lifecycle.validate_closure(clear, (observed("a"), observed("b")))
    state = SimpleNamespace(owned=(owned(), owned(debt=90, collateral=True)))
    with pytest.raises(DataError) as refused:
        pool_lending_lifecycle.validate_closure(
            state, (observed("lp", allowance=1), observed("loan", configuration=3, debt=100))
        )
    assert refused.value.kind == "remaining_obligations"
    assert refused.value.details == {
        "remaining": [
            {"allocation": "lp", "asset": str(USDC), "allowance": "1"},
            {
                "allocation": "loan",
                "asset": str(USDC),
                "debt_scaled": "90",
                "collateral_enabled": True,
                "user_configuration": "3",
                "debt": "100",
            },
        ]
    }
    assert str(refused.value).endswith(
        ": lp allowance 1; loan debt_scaled 90, collateral_enabled True, "
        "user_configuration 3, debt 100"
    )


def test_comet_closure_names_each_remaining_obligation(monkeypatch):
    monkeypatch.setattr(compound_lifecycle, "validate_owned", lambda state, observed: None)

    def observed(allocation, base, allowance=0, supplied=0, borrowed=0):
        target = SimpleNamespace(
            allocation=allocation, asset=USDC, reserve=SimpleNamespace(is_base=base)
        )
        account = SimpleNamespace(supplied_base=supplied, borrowed_base=borrowed)
        return SimpleNamespace(
            allocation=allocation, target=target, allowance=allowance, account=account
        )

    def owned(principal=0, collateral=0):
        return SimpleNamespace(principal=principal, collateral=collateral)

    compound_lifecycle.validate_closure(
        SimpleNamespace(owned=(owned(), owned())), (observed("a", True), observed("b", False))
    )
    state = SimpleNamespace(owned=(owned(principal=-500), owned(collateral=9)))
    with pytest.raises(DataError) as refused:
        compound_lifecycle.validate_closure(
            state, (observed("loan", True, borrowed=510), observed("collateral", False, 1))
        )
    assert refused.value.details["remaining"] == [
        {"allocation": "loan", "asset": str(USDC), "principal": "-500", "debt": "510"},
        {"allocation": "collateral", "asset": str(USDC), "collateral": "9", "allowance": "1"},
    ]


def test_morpho_closure_names_each_remaining_obligation(monkeypatch):
    monkeypatch.setattr(morpho_lifecycle, "validate_owned", lambda state, observed: None)
    monkeypatch.setattr(morpho_lifecycle, "borrowed_assets", lambda account, at: 1_001)

    def observed(allocation, allowance=0):
        target = SimpleNamespace(allocation=allocation, asset=USDC)
        return SimpleNamespace(
            allocation=allocation,
            target=target,
            allowance=allowance,
            account=None,
            block=SimpleNamespace(timestamp=0),
        )

    def owned(supply=0, borrow=0, collateral=0):
        return SimpleNamespace(supply_shares=supply, borrow_shares=borrow, collateral=collateral)

    morpho_lifecycle.validate_closure(
        SimpleNamespace(owned=(owned(), owned())), (observed("a"), observed("b"))
    )
    state = SimpleNamespace(owned=(owned(borrow=1_000), owned(collateral=5)))
    with pytest.raises(DataError) as refused:
        morpho_lifecycle.validate_closure(state, (observed("loan"), observed("collateral", 2)))
    assert refused.value.details["remaining"] == [
        {"allocation": "loan", "asset": str(USDC), "borrow_shares": "1000", "debt": "1001"},
        {"allocation": "collateral", "asset": str(USDC), "collateral": "5", "allowance": "2"},
    ]


def test_wall_time_stops_are_transient_infra_failures(monkeypatch):
    monkeypatch.setattr(testflow, "DECISION_SECONDS", 0)
    monkeypatch.setattr(testflow, "TEARDOWN_SECONDS", 0)
    verdict = testflow.empty_verdict()
    with pytest.raises(testflow.Stopped) as stopped:
        testflow.next_decision(Path("."), None)
    stopped.value.record(verdict, "decision")
    with pytest.raises(testflow.Stopped) as stopped:
        testflow.await_closure(Path("."), None)
    stopped.value.record(verdict, "teardown")
    assert [(f["kind"], f["class"], f["transient"]) for f in verdict["failures"]] == [
        ("decision_timeout", "infra", True),
        ("teardown_timeout", "infra", True),
    ]


def coordinator_logging(tmp_path, monkeypatch, *events):
    """A workspace whose running coordinator logged `events` after it started."""
    root = workspace(tmp_path, {"coordinator": {"pid": 1}})
    state = json.loads((root / "workspace.json").read_text())
    state["coordinator_log_offset"] = 0
    (root / "workspace.json").write_text(json.dumps(state))
    lines = [json.dumps(event) + "\n" for event in events]
    (root / "logs" / "coordinator.log").write_text("".join(lines))
    monkeypatch.setattr("almanak.supervisor_process.running", lambda record: True)
    return root


def worker_timeout(attempt, elapsed):
    return {
        "event": "strategy_worker_timeout",
        "status": "degraded",
        "kind": "wall",
        "attempt": attempt,
        "elapsed_seconds": elapsed,
    }


READ_HOLD = {
    "event": "transient_read_hold",
    "status": "held",
    "kind": "unavailable",
    "reason": "eth_getBlockByNumber: block unavailable",
    "attempt": 1,
    "elapsed_seconds": 0,
}


def test_a_coordinator_stalled_on_worker_timeouts_stops_the_wait_with_their_kind(
    tmp_path, monkeypatch
):
    elapsed = (0, 25, 55, 95, 155, 235, 315)
    holds = [worker_timeout(attempt, s) for attempt, s in enumerate(elapsed, 1)]
    root = coordinator_logging(tmp_path, monkeypatch, *holds)
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: None)
    started_at = time.monotonic()
    with pytest.raises(testflow.Stopped) as stopped:
        testflow.next_decision(root, None)
    assert time.monotonic() - started_at < testflow.DECISION_SECONDS / 10
    verdict = testflow.empty_verdict()
    stopped.value.record(verdict, "decision")
    [entry] = verdict["failures"]
    assert (entry["kind"], entry["class"], entry["transient"]) == (
        "strategy_worker_timeout",
        "infra",
        True,
    )
    assert entry["message"] == (
        "coordinator holding on strategy_worker_timeout for 315 seconds "
        "(7 consecutive attempts): strategy worker wall timeout"
    )
    assert entry["details"] == {"kind": "wall", "attempt": 7, "elapsed_seconds": 315}


def test_a_newer_read_hold_does_not_mask_a_worker_timeout_past_the_threshold(tmp_path, monkeypatch):
    root = coordinator_logging(tmp_path, monkeypatch, worker_timeout(7, 315), READ_HOLD)
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: None)
    started_at = time.monotonic()
    with pytest.raises(testflow.Stopped, match="strategy_worker_timeout for 315 seconds"):
        testflow.next_decision(root, None)
    assert time.monotonic() - started_at < testflow.DECISION_SECONDS / 10


def test_a_brief_transient_hold_does_not_stop_a_decision_that_settles(tmp_path, monkeypatch):
    root = coordinator_logging(tmp_path, monkeypatch, READ_HOLD)
    settled = {"schedule": {"last_evaluation": {"started_at": "t1"}}}
    monkeypatch.setattr("almanak.reports.checkpoint", sequence([None, None, settled]))
    assert testflow.next_decision(root, None) is settled


def test_a_decision_timeout_is_named_by_the_hold_the_coordinator_is_still_on(tmp_path, monkeypatch):
    monkeypatch.setattr(testflow, "DECISION_SECONDS", 0)
    root = coordinator_logging(tmp_path, monkeypatch, worker_timeout(1, 0), READ_HOLD)
    with pytest.raises(testflow.Stopped) as stopped:
        testflow.next_decision(root, None)
    assert (stopped.value.kind, stopped.value.transient) == ("transient_read_hold", True)
    assert str(stopped.value).endswith("eth_getBlockByNumber: block unavailable")
    # A recovered read leaves the worker timeouts before it as the cause.
    recovered = {"event": "transient_read_recovered", "attempts": 1}
    root = coordinator_logging(tmp_path, monkeypatch, worker_timeout(1, 0), READ_HOLD, recovered)
    with pytest.raises(testflow.Stopped, match="strategy_worker_timeout"):
        testflow.next_decision(root, None)


def test_a_decision_timeout_without_a_hold_stays_transient(tmp_path, monkeypatch):
    monkeypatch.setattr(testflow, "DECISION_SECONDS", 0)
    recovered = {"event": "strategy_worker_recovered", "attempts": 1}
    root = coordinator_logging(tmp_path, monkeypatch, worker_timeout(1, 0), recovered)
    with pytest.raises(testflow.Stopped) as stopped:
        testflow.next_decision(root, None)
    assert (stopped.value.kind, stopped.value.transient) == ("decision_timeout", True)


@pytest.mark.parametrize(
    ("row", "bound"),
    [
        (
            {"time": "t0", "result": "intent", "settled": False},
            testflow.NO_PROGRESS_TEARDOWN_SECONDS,
        ),
        ({"time": "t0", "result": "hold"}, None),
    ],
)
def test_teardown_is_bounded_only_after_no_settled_decision(tmp_path, monkeypatch, row, bound):
    bounds = []
    monkeypatch.setattr(testflow, "start", started(tmp_path))
    monkeypatch.setattr(testflow, "admitted", lambda *a: True)
    monkeypatch.setattr(testflow, "Keepers", lambda root, env: Mock(rows=[], failures=[]))
    monkeypatch.setattr(
        testflow, "drive", lambda r, a, verdict, k: verdict["decisions"].append(row)
    )
    monkeypatch.setattr(testflow, "teardown", lambda *a: bounds.append(a[-1]))
    monkeypatch.setattr(testflow, "cleanup", lambda *a: None)
    testflow.command(Namespace(output=tmp_path / "run", format="json", advance=60, env_file=None))
    assert bounds == [bound]


def test_a_bounded_teardown_records_why_it_waited_less(tmp_path, monkeypatch):
    root = coordinator_logging(tmp_path, monkeypatch)
    monkeypatch.setattr("almanak.workspace.request", lambda root, action: {})
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: None)
    verdict = testflow.empty_verdict()
    testflow.teardown(root, verdict, bound=0)
    [entry] = verdict["failures"]
    assert entry["message"] == (
        "not closed within 0 seconds, the wait bounded because no decision settled"
    )
    assert (entry["kind"], entry["class"], entry["transient"]) == (
        "teardown_timeout",
        "infra",
        True,
    )


def test_teardown_stops_on_a_stall_hold_logged_after_it_started(tmp_path, monkeypatch):
    root = coordinator_logging(tmp_path, monkeypatch)
    since = testflow.coordinator_log_end(root)
    with (root / "logs" / "coordinator.log").open("a") as log:
        log.write(json.dumps(worker_timeout(9, testflow.STALL_HOLD_SECONDS)) + "\n")
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: {"mode": "teardown"})
    with pytest.raises(testflow.Stopped) as stopped:
        testflow.await_closure(root, None, since=since)
    assert stopped.value.kind == "strategy_worker_timeout"


def test_an_interrupted_run_is_an_infra_failure(tmp_path, monkeypatch):
    monkeypatch.setattr(testflow, "start", started(tmp_path))
    monkeypatch.setattr(testflow, "admitted", lambda *a: True)
    monkeypatch.setattr(testflow, "Keepers", lambda root, env: Mock(rows=[], failures=[]))

    def interrupted(*_a):
        raise KeyboardInterrupt

    monkeypatch.setattr(testflow, "drive", interrupted)
    monkeypatch.setattr(testflow, "cleanup", lambda *a: None)
    args = Namespace(output=tmp_path / "run", format="json", env_file=None)
    assert testflow.command(args) == 1
    verdict = json.loads((tmp_path / "run" / "test-verdict.json").read_text())
    [entry] = verdict["failures"]
    assert (entry["kind"], entry["class"], entry["transient"]) == ("interrupt", "infra", False)
    assert verdict["status"] == "failed" and verdict["failure_class"] == "infra"


def timed_out_worker():
    try:
        raise WorkerSessionTimeout("sandboxed preload worker timed out")
    except WorkerSessionTimeout as slow:
        try:
            raise RuntimeError("sandbox stage failed") from slow
        except RuntimeError as wrapped:
            return wrapped


def test_a_preload_worker_timeout_is_typed_through_its_wrappers(monkeypatch):
    monkeypatch.setattr("almanak.worker_session.select.select", lambda *a: ([], [], []))
    session = WorkerSession()
    session.started = True
    session.process = SimpleNamespace(stdout=object())
    monkeypatch.setattr("almanak.worker_session.kill_worker", lambda process: None)
    monkeypatch.setattr(WorkerSession, "startup_errors", lambda self: "bwrap slow")
    monkeypatch.setattr(WorkerSession, "__exit__", lambda self, *a: None)
    with pytest.raises(WorkerSessionTimeout, match="timed out: bwrap slow") as raised:
        session.__enter__()
    assert isinstance(raised.value, RuntimeError) and raised.value.kind == "worker_timeout"
    assert worker_timed_out(timed_out_worker())
    assert not worker_timed_out(RuntimeError("other")) and not worker_timed_out(None)


@pytest.mark.parametrize(
    ("raised", "cause"), [(timed_out_worker(), "worker_timeout"), (RuntimeError("x"), None)]
)
def test_the_supervisor_names_a_worker_timeout_and_still_marks_it_retryable(raised, cause):
    persisted = []
    host = SimpleNamespace(
        password="x",
        setup_active=True,
        state={"stage": "provisioning"},
        persist=lambda **changes: persisted.append(changes),
    )

    def work():
        raise raised

    Supervisor.setup_worker(host, work)
    [changes] = persisted
    assert (changes["failure"], changes["cause"]) == ("start", cause)


def test_a_worker_timeout_refusal_is_transient_infra(tmp_path, monkeypatch):
    failed = {"stage": "failed", "failure": "start", "cause": "worker_timeout", "diagnostic": "d"}
    monkeypatch.setattr(testflow, "supervisor_status", lambda root: failed)
    verdict = testflow.empty_verdict()
    assert testflow.admitted(Path("."), Process(), verdict) is False
    refusal = verdict["admission"]["refusal"]
    assert (refusal["error"], refusal["kind"], refusal["class"], refusal["transient"]) == (
        "start",
        "worker_timeout",
        "infra",
        True,
    )

    def refuse(args, **options):
        raise timed_out_worker()

    monkeypatch.setattr("almanak.workspace.prepare", refuse)
    verdict = testflow.empty_verdict()
    args = Namespace(output=tmp_path / "run", param=None, production_parameters=False)
    testflow.start(args, tmp_path / "run", verdict)
    refusal = verdict["admission"]["refusal"]
    assert (refusal["kind"], refusal["class"], refusal["transient"]) == (
        "worker_timeout",
        "infra",
        True,
    )


def request(decision, *statuses):
    events = [{"status": "accepted"}, *({"status": status} for status in statuses)]
    return {"key": "0x01", "decision_identity": decision, "events": events}


@pytest.mark.parametrize(
    ("wire", "planning"),
    [
        (None, False),
        ({"decision": {"id": 1}, "plan": None, "pending": None}, True),
        ({"decision": {"id": 1}, "plan": {"id": 2}}, False),
        ({"decision": {"identity": "d1"}, "requests": [request("d1")]}, False),
        ({"decision": {"identity": "d1"}, "requests": [request("d1", "frozen")]}, False),
        ({"decision": {"identity": "d2"}, "requests": [request("d1", "executed")]}, True),
        ({"decision": {"identity": "d2"}, "requests": [request("d2", "cancelled")]}, True),
        ({"decision": {"identity": "d2"}, "requests": [request("d1", "accepted")]}, True),
        ({"decision": None, "work": [{"decision": {"id": 1}}]}, True),
        ({"decision": None, "work": [{"decision": {"id": 1}, "pending": {"id": 3}}]}, False),
    ],
)
def test_planning_is_a_decision_without_plan_attempt_or_request(wire, planning):
    assert testflow.planning(wire) is planning


def test_one_chain_awaiting_its_request_never_frees_another_chains_planning():
    awaiting = {"chain": "a", "decision": {"identity": "d1"}}
    deciding = {"chain": "b", "decision": {"identity": "d2"}}
    requests = [request("d1", "accepted")]
    assert testflow.planning({"work": [awaiting], "requests": requests}) is False
    assert testflow.planning({"work": [awaiting, deciding], "requests": requests}) is True


def test_a_completed_gmx_request_does_not_let_teardown_advance_time_mid_plan(monkeypatch):
    advanced = []
    history = [request("d1", "executed"), request("d2", "cancelled")]
    deciding = {"mode": "teardown", "decision": {"identity": "d3"}, "requests": history}
    states = [deciding] * 4 + [{"mode": "closed"}]
    monkeypatch.setattr(testflow, "STALL_SECONDS", 0)
    monkeypatch.setattr("almanak.reports.checkpoint", sequence(states))
    monkeypatch.setattr(testflow, "require_coordinator", lambda root, since=None: None)
    monkeypatch.setattr(testflow, "advance", lambda root, step: advanced.append(step))
    testflow.await_closure(Path("."), 300)
    assert advanced == []


def test_concurrent_chains_advance_only_once_no_chain_is_planning(monkeypatch):
    advanced = []
    requests = [request("d1", "accepted")]
    awaiting = {"chain": "a", "decision": {"identity": "d1"}}
    both = {"mode": "teardown", "work": [awaiting, {"chain": "b", "decision": {"identity": "d2"}}]}
    planned = {"chain": "b", "decision": {"identity": "d2"}, "pending": {"id": 3}}
    only = {"mode": "teardown", "work": [awaiting, planned]}
    states = [{**both, "requests": requests}] * 3 + [{**only, "requests": requests}] * 2
    monkeypatch.setattr(testflow, "STALL_SECONDS", 0)
    monkeypatch.setattr("almanak.reports.checkpoint", sequence([*states, {"mode": "closed"}]))
    monkeypatch.setattr(testflow, "require_coordinator", lambda root, since=None: None)
    monkeypatch.setattr(testflow, "advance", lambda root, step: advanced.append(step))
    testflow.await_closure(Path("."), 300)
    assert advanced == [300]


def test_teardown_never_advances_time_while_a_decision_is_planned(monkeypatch):
    advanced = []
    deciding = {"mode": "teardown", "decision": {"id": 1}}
    quiet = {"mode": "teardown"}
    states = [deciding, deciding, deciding, quiet, quiet, {"mode": "closed"}]
    monkeypatch.setattr(testflow, "STALL_SECONDS", 0)
    monkeypatch.setattr("almanak.reports.checkpoint", sequence(states))
    monkeypatch.setattr(testflow, "require_coordinator", lambda root, since=None: None)
    monkeypatch.setattr(testflow, "advance", lambda root, step: advanced.append(step))
    testflow.await_closure(Path("."), 300)
    assert advanced == [300]


def test_a_mismatched_plan_names_the_differing_parts_only():
    class Plan(BaseModel):
        gas_price: int
        nonce: int

    assert differing(Plan(gas_price=2, nonce=1), Plan(gas_price=3, nonce=1)) == ["gas_price"]
    assert differing(SimpleNamespace(), Plan(gas_price=3, nonce=1)) == ["gas_price", "nonce"]
    assert differing(Plan(gas_price=3, nonce=1), SimpleNamespace()) == ["plan"]


def test_a_teardown_that_keeps_requesting_an_entry_swap_ends_as_teardown_refused(
    tmp_path, monkeypatch
):
    root = workspace(tmp_path)
    log = root / "logs" / "coordinator.log"
    refusal = {"event": "exposure_refused_in_teardown", "intent": "swap", "allocation": "trade"}
    log.write_text(json.dumps({"event": "decision_evaluated"}) + "\n")
    since = testflow.coordinator_log_end(root)
    polls = []

    def checkpoint(_root):
        polls.append(None)
        with log.open("a") as stream:
            stream.write(json.dumps(refusal) + "\n")
        return {"mode": "teardown", "outcomes": [{}]}

    monkeypatch.setattr("almanak.reports.checkpoint", checkpoint)
    monkeypatch.setattr(testflow, "require_coordinator", lambda root, since=None: None)
    with pytest.raises(testflow.Stopped) as stopped:
        testflow.await_closure(root, None, since=since)
    assert len(polls) == testflow.TEARDOWN_REFUSALS
    verdict = testflow.empty_verdict()
    stopped.value.record(verdict, "teardown")
    [entry] = verdict["failures"]
    assert (entry["kind"], entry["class"], entry["transient"]) == (
        "teardown_refused",
        "strategy",
        False,
    )
    assert entry["details"] == {"intent": "swap", "allocation": "trade", "refusals": 3}
    assert "swap" in entry["message"] and "trade" in entry["message"]


def test_a_teardown_outcome_between_refusals_restarts_the_count(tmp_path, monkeypatch):
    root = workspace(tmp_path)
    log = root / "logs" / "coordinator.log"
    refusal = {"event": "exposure_refused_in_teardown", "intent": "swap", "allocation": "trade"}
    outcomes = iter([1, 1, 2, 2, 2])

    def checkpoint(_root):
        with log.open("a") as stream:
            stream.write(json.dumps(refusal) + "\n")
        count = next(outcomes, None)
        if count is None:
            return {"mode": "closed"}
        return {"mode": "teardown", "outcomes": [{}] * count}

    monkeypatch.setattr("almanak.reports.checkpoint", checkpoint)
    monkeypatch.setattr(testflow, "require_coordinator", lambda root, since=None: None)
    testflow.await_closure(root, None, since=0)
