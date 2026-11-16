"""`almanak test` sequencing over fake supervision seams; no process or provider."""

import gzip
import json
import sqlite3
import subprocess
from argparse import Namespace
from pathlib import Path
from unittest.mock import Mock

import pytest

from almanak import testflow
from almanak.checks import CheckFailure

ROOT = Path(__file__).parents[1]
FIXTURES = ROOT / "tests" / "fixtures"


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    monkeypatch.setattr(testflow, "POLL_SECONDS", 0)


class Process:
    def __init__(self, code=None):
        self.code = code
        self.terminated = False

    def poll(self):
        return self.code

    def wait(self, timeout=None):
        if self.code is None and not self.terminated:
            raise subprocess.TimeoutExpired("supervisor", timeout)
        return 0

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.terminated = True


def sequence(values):
    items = iter(values)
    last = []

    def next_value(*_args, **_kwargs):
        try:
            last[:] = [next(items)]
        except StopIteration:
            pass
        return last[0]

    return next_value


def workspace(tmp_path, processes=None):
    root = tmp_path / "run"
    root.mkdir(exist_ok=True)
    (root / "logs").mkdir(exist_ok=True)
    state = {"processes": processes or {}, "stage": "created"}
    (root / "workspace.json").write_text(json.dumps(state))
    return root


def started(tmp_path):
    """A `start` that creates the workspace, as `prepare` does."""
    return lambda *a: (workspace(tmp_path), Process())[1]


def test_command_reports_a_closed_lifecycle_and_retains_the_verdict(tmp_path, monkeypatch):
    root = tmp_path / "run"
    calls = []
    monkeypatch.setattr(testflow, "start", started(tmp_path))
    monkeypatch.setattr(testflow, "drive", lambda *a: calls.append("drive"))

    def closed(_root, verdict, step, keepers, bound):
        calls.append(("teardown", step, bound))
        verdict["closed"] = verdict["evidence_clean"] = True

    monkeypatch.setattr(testflow, "teardown", closed)
    monkeypatch.setattr(testflow, "cleanup", lambda *a: calls.append("cleanup"))
    monkeypatch.setattr(
        testflow, "Keepers", lambda root, env: Mock(rows=[{"request": "0x01"}], failures=[])
    )
    args = Namespace(output=root, format="json", advance=60, env_file=None)

    def admit(_root, _process, verdict):
        verdict["admission"]["ok"] = True
        return True

    monkeypatch.setattr(testflow, "admitted", admit)
    assert testflow.command(args) == 0
    # The fake drive settled no decision, so teardown's wait is bounded.
    assert calls == ["drive", ("teardown", 60, testflow.NO_PROGRESS_TEARDOWN_SECONDS), "cleanup"]
    verdict = json.loads((root / "test-verdict.json").read_text())
    assert verdict["passed"] is True and verdict["keepers"] == [{"request": "0x01"}]


@pytest.mark.parametrize(
    "change",
    [
        {"closed": False},
        {"evidence_clean": False},
        {"failures": [{"stage": "decision", "message": "x"}]},
        {"admission": {"ok": False, "refusal": None}},
    ],
)
def test_a_verdict_passes_only_admitted_closed_clean_and_without_failures(change):
    verdict = {**testflow.empty_verdict(), "closed": True, "evidence_clean": True}
    verdict["admission"] = {"ok": True, "refusal": None}
    assert testflow.passed(verdict)
    assert not testflow.passed({**verdict, **change})


@pytest.mark.parametrize("error", [KeyboardInterrupt, RuntimeError])
def test_command_cleans_up_after_an_interrupt_or_failure(tmp_path, monkeypatch, error):
    root = tmp_path / "run"
    cleaned = []
    monkeypatch.setattr(testflow, "start", started(tmp_path))
    monkeypatch.setattr(testflow, "admitted", lambda *a: True)

    def fail(*_):
        raise error("stop")

    monkeypatch.setattr(testflow, "drive", fail)
    monkeypatch.setattr(testflow, "cleanup", lambda *a: cleaned.append(a[1]))
    assert testflow.command(Namespace(output=root, format="json")) == 1
    verdict = json.loads((root / "test-verdict.json").read_text())
    assert verdict["failures"][0]["stage"] in ("interrupt", "internal")
    assert len(cleaned) == 1


def interrupted_teardown():
    """A Spark loop's journal when the harness interrupted its teardown (a rig run)."""
    return json.loads(
        gzip.decompress((FIXTURES / "spark-loop-interrupted-teardown.json.gz").read_bytes())
    )


def test_an_interrupted_teardown_reports_what_teardown_executed(tmp_path, monkeypatch):
    root = tmp_path / "run"
    wire = interrupted_teardown()
    calls = []
    monkeypatch.setattr(testflow, "start", started(tmp_path))
    monkeypatch.setattr(testflow, "admitted", lambda *a: True)
    monkeypatch.setattr(
        testflow, "Keepers", lambda root, env: Mock(rows=[], failures=[], step=lambda w, s: 0)
    )

    def drive(_root, _args, verdict, _keepers):
        decision = {**wire, "outcomes": [o for o in wire["outcomes"] if not by_teardown(o)]}
        verdict["transactions"] = testflow.transactions(decision)

    def interrupt(*_args):
        raise KeyboardInterrupt

    def cleanup(_root, _process, verdict):
        calls.append(dict(verdict))

    def logged(*_args):
        (root / "logs").mkdir(parents=True, exist_ok=True)
        (root / "logs" / "coordinator.log").write_text(
            json.dumps(
                {
                    "event": "decision_evaluated",
                    "time": "2026-10-19T10:00:00Z",
                    "trigger": "teardown",
                    "result": "intent",
                    "intent": "withdraw_amount",
                }
            )
            + "\n"
        )
        raise KeyboardInterrupt

    monkeypatch.setattr(testflow, "drive", drive)
    monkeypatch.setattr("almanak.workspace.request", lambda root, action: {})
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: wire)
    monkeypatch.setattr(testflow, "await_closure", logged)
    monkeypatch.setattr(testflow, "cleanup", cleanup)
    assert testflow.command(Namespace(output=root, format="json", advance=60, env_file=None)) == 1
    verdict = json.loads((root / "test-verdict.json").read_text())
    assert [f["kind"] for f in verdict["failures"]] == ["interrupt"]
    [decision] = verdict["teardown_decisions"]
    assert (decision["result"], decision["intents"]) == ("intent", ["withdraw_amount"])
    assert verdict["teardown_executed"] == ["withdraw_amount", "swap_exact_input", "repay_amount"]
    torn = [row for row in verdict["transactions"] if row["trigger"] == "teardown"]
    assert len(torn) == 6 and all(row["status"] == "success" for row in torn)
    assert verdict["closed"] is False and verdict["evidence_clean"] is False
    assert verdict["entered"] is True
    [flight] = verdict["in_flight"]
    assert (flight["trigger"], flight["intent"]) == ("teardown", "swap_exact_input")
    assert flight["transaction"] == wire["pending"]["tx_hash"]
    # Recorded before cleanup, whose shutdown reads `closed`.
    assert calls[0]["teardown_executed"] == verdict["teardown_executed"]


def by_teardown(outcome) -> bool:
    return outcome["decision"]["trigger"]["kind"] == "teardown"


def test_an_unreadable_journal_at_interrupt_still_cleans_up(tmp_path, monkeypatch):
    root = tmp_path / "run"
    cleaned = []
    monkeypatch.setattr(testflow, "start", started(tmp_path))
    monkeypatch.setattr(testflow, "admitted", lambda *a: True)

    def interrupt(*_args):
        raise KeyboardInterrupt

    monkeypatch.setattr(testflow, "Keepers", lambda root, env: Mock(rows=[], failures=[]))
    monkeypatch.setattr(testflow, "drive", interrupt)
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: {"mode": "running"})
    monkeypatch.setattr(testflow, "cleanup", lambda *a: cleaned.append(a[1]))
    assert testflow.command(Namespace(output=root, format="json", env_file=None)) == 1
    verdict = json.loads((root / "test-verdict.json").read_text())
    assert [f["stage"] for f in verdict["failures"]] == ["interrupt", "interrupt"]
    assert verdict["failure_class"] == "infra" and len(cleaned) == 1


def test_in_flight_lists_each_retained_decision():
    assert testflow.in_flight(None) == []
    assert testflow.in_flight({"decision": None, "work": []}) == []
    unit = {"decision": {"trigger": {"kind": "interval"}, "intent": {"kind": "supply"}}}
    assert testflow.in_flight({"decision": None, "work": [unit]}) == [
        {"trigger": "interval", "intent": "supply", "allocation": None, "transaction": None}
    ]


def test_an_existing_output_keeps_its_earlier_verdict(tmp_path, monkeypatch):
    root = workspace(tmp_path)
    (root / "test-verdict.json").write_text('{"closed": true}')
    monkeypatch.setattr(testflow, "start", lambda *a: None)
    monkeypatch.setattr(testflow, "cleanup", lambda *a: None)
    assert testflow.command(Namespace(output=root, format="json")) == 1
    assert json.loads((root / "test-verdict.json").read_text()) == {"closed": True}


def test_refused_inputs_are_the_admission_refusal(tmp_path, monkeypatch):
    def refuse(args, **options):
        raise ValueError("bad config")

    monkeypatch.setattr("almanak.workspace.prepare", refuse)
    verdict = testflow.empty_verdict()
    args = Namespace(output=tmp_path / "run", param=None, production_parameters=False)
    assert testflow.start(args, tmp_path / "run", verdict) is None
    assert verdict["admission"]["refusal"] == {
        "error": "invalid_input",
        "message": "bad config",
        "kind": "invalid_input",
        "class": "strategy",
        "transient": False,
    }


def test_start_launches_the_prepared_workspace(tmp_path, monkeypatch):
    monkeypatch.setattr("almanak.workspace.prepare", lambda args, **options: None)
    monkeypatch.setattr("almanak.workspace.launch", lambda root: "process")
    verdict = testflow.empty_verdict()
    args = Namespace(param=None, production_parameters=False)
    assert testflow.start(args, tmp_path, verdict) == "process"
    assert verdict["workspace"] == str(tmp_path)


@pytest.mark.parametrize(
    ("states", "code", "ok", "stage"),
    [
        ([{"stage": "provisioning", "provisioning": True}, {"stage": "running"}], None, True, None),
        ([{"stage": "failed", "diagnostic": "funding"}], None, False, "failed"),
        ([{"stage": "created"}], 1, False, "supervisor"),
    ],
)
def test_admission_waits_for_the_running_coordinator(monkeypatch, states, code, ok, stage):
    monkeypatch.setattr(testflow, "supervisor_status", sequence(states))
    verdict = testflow.empty_verdict()
    assert testflow.admitted(Path("."), Process(code), verdict) is ok
    assert verdict["admission"]["ok"] is ok
    if stage:
        assert verdict["admission"]["refusal"]["stage"] == stage


def test_admission_times_out(monkeypatch):
    monkeypatch.setattr(testflow, "ADMISSION_SECONDS", 0)
    verdict = testflow.empty_verdict()
    assert testflow.admitted(Path("."), Process(), verdict) is False
    assert verdict["admission"]["refusal"]["stage"] == "timeout"


def test_supervisor_status_falls_back_to_the_workspace_file(tmp_path, monkeypatch):
    root = workspace(tmp_path)

    def unavailable(*_a, **_k):
        raise ValueError("supervisor is not ready")

    monkeypatch.setattr("almanak.workspace.request", unavailable)
    assert testflow.supervisor_status(root)["stage"] == "created"


def wire(started_at, **fields):
    return {"schedule": {"last_evaluation": {"started_at": started_at}}, **fields}


def test_drive_advances_between_decisions_and_stops_after_entry(monkeypatch):
    advanced = []
    decisions = sequence([wire("t1", outcomes=[]), wire("t2", outcomes=[])])
    monkeypatch.setattr(testflow, "next_decision", lambda root, previous, keepers=None: decisions())
    monkeypatch.setattr(testflow, "advance", lambda root, seconds: advanced.append(seconds))
    monkeypatch.setattr(testflow, "decision_interval", lambda root: 3600)
    monkeypatch.setattr(
        testflow, "entered", lambda w: w["schedule"]["last_evaluation"]["started_at"] == "t2"
    )
    monkeypatch.setattr(testflow, "executed", lambda w: ([], []))
    verdict = testflow.empty_verdict()
    testflow.drive(Path("."), Namespace(decisions=5, advance=None), verdict)
    assert advanced == [3600]
    assert [d["time"] for d in verdict["decisions"]] == ["t1", "t2"]
    assert verdict["entered"] is True


def test_through_exit_keeps_deciding_after_entry_until_the_cap(monkeypatch):
    advanced = []
    times = ["t1", "t2", "t3", "t4"]
    decisions = sequence([wire(t, outcomes=[]) for t in times])
    monkeypatch.setattr(testflow, "next_decision", lambda root, previous, keepers=None: decisions())
    monkeypatch.setattr(testflow, "advance", lambda root, seconds: advanced.append(seconds))
    monkeypatch.setattr(testflow, "decision_interval", lambda root: 60)
    monkeypatch.setattr(testflow, "entered", lambda w: True)
    swap, mint, close = "swap_exact_input", "open_liquidity", "close_liquidity"
    reports = iter(
        [
            ([swap], []),
            ([swap, mint], [mint]),
            ([swap, mint, close], [mint, close]),
            ([swap, mint, close], [mint, close]),
        ]
    )
    monkeypatch.setattr(testflow, "executed", lambda w: next(reports))
    verdict = testflow.empty_verdict()
    testflow.drive(
        Path("."), Namespace(decisions=4, advance=90, through_exit=True, idle=5), verdict
    )
    assert advanced == [90, 90, 90]
    assert [d["time"] for d in verdict["decisions"]] == times
    assert verdict["executed"] == [swap, mint, close]
    assert verdict["after_entry"] == [mint, close] and verdict["entered"] is True


def intent_wire(time, kind):
    decision = {"trigger": {"started_at": time}, "intent": {"kind": kind}}
    return wire(time, outcomes=[{"decision": decision}])


@pytest.mark.parametrize(
    ("kinds", "idle", "seen"),
    [
        # Bought, waited out its hold time, sold, then idled: three holds end it.
        (["buy", None, None, "sell", None, None, None, None], 3, 7),
        # Two holds while waiting is not idleness.
        (["buy", None, None, "sell", None, None], 3, 6),
        (["buy", None, None, None, None], 2, 3),
    ],
)
def test_through_exit_stops_once_the_entered_strategy_idles(monkeypatch, kinds, idle, seen):
    times = [f"t{i}" for i in range(len(kinds))]
    wires = [
        intent_wire(t, k) if k else wire(t, outcomes=[]) for t, k in zip(times, kinds, strict=True)
    ]
    decisions = sequence(wires)
    monkeypatch.setattr(testflow, "next_decision", lambda root, previous, keepers=None: decisions())
    monkeypatch.setattr(testflow, "advance", lambda root, seconds: None)
    monkeypatch.setattr(testflow, "decision_interval", lambda root: 60)
    monkeypatch.setattr(testflow, "entered", lambda w: True)
    monkeypatch.setattr(testflow, "executed", lambda w: ([], []))
    verdict = testflow.empty_verdict()
    args = Namespace(decisions=len(kinds), advance=60, through_exit=True, idle=idle)
    testflow.drive(Path("."), args, verdict)
    assert len(verdict["decisions"]) == seen


def test_holds_before_the_entry_never_end_the_lifecycle(monkeypatch):
    decisions = sequence([wire(f"t{i}", outcomes=[]) for i in range(5)])
    monkeypatch.setattr(testflow, "next_decision", lambda root, previous, keepers=None: decisions())
    monkeypatch.setattr(testflow, "advance", lambda root, seconds: None)
    monkeypatch.setattr(testflow, "decision_interval", lambda root: 60)
    monkeypatch.setattr(testflow, "entered", lambda w: False)
    monkeypatch.setattr(testflow, "executed", lambda w: ([], []))
    verdict = testflow.empty_verdict()
    args = Namespace(decisions=5, advance=60, through_exit=True, idle=2)
    testflow.drive(Path("."), args, verdict)
    assert len(verdict["decisions"]) == 5


def test_drive_records_why_the_lifecycle_stopped(monkeypatch):
    def stopped(root, previous, keepers=None):
        raise testflow.Stopped("coordinator stopped: gas_limit")

    monkeypatch.setattr(testflow, "next_decision", stopped)
    verdict = testflow.empty_verdict()
    testflow.drive(Path("."), Namespace(decisions=2, advance=60), verdict)
    assert verdict["failures"] == [
        {
            "stage": "decision",
            "message": "coordinator stopped: gas_limit",
            "kind": "stopped",
            "class": "strategy",
            "transient": False,
        }
    ]


def test_an_unsettled_decision_is_reported_with_its_transactions(monkeypatch):
    def stopped(root, previous, keepers=None):
        raise testflow.Stopped("no settled decision within 600 seconds")

    in_flight = {"trigger": {"started_at": "t1"}, "intent": {"kind": "open_position"}}
    monkeypatch.setattr(testflow, "next_decision", stopped)
    monkeypatch.setattr(
        "almanak.reports.checkpoint", lambda root: wire("t1", outcomes=[], decision=in_flight)
    )
    monkeypatch.setattr(testflow, "entered", lambda w: True)
    monkeypatch.setattr(testflow, "executed", lambda w: (["open_position"], []))
    verdict = testflow.empty_verdict()
    testflow.drive(Path("."), Namespace(decisions=2, advance=60), verdict)
    assert verdict["executed"] == ["open_position"]
    assert verdict["decisions"] == [
        {"time": "t1", "result": "intent", "intents": ["open_position"], "settled": False}
    ]
    assert verdict["entered"] is True
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: None)
    verdict = testflow.empty_verdict()
    testflow.drive(Path("."), Namespace(decisions=2, advance=60), verdict)
    assert verdict["decisions"] == []


def test_next_decision_waits_for_a_settled_new_evaluation(monkeypatch):
    states = [
        None,
        wire("t0", pending=None, plan=None, decision=None),
        wire("t1", pending={"x": 1}),
        wire("t1", pending=None, plan=None, decision=None),
    ]
    monkeypatch.setattr("almanak.reports.checkpoint", sequence(states))
    monkeypatch.setattr(testflow, "require_coordinator", lambda root, since=None: None)
    assert (
        testflow.next_decision(Path("."), "t0")["schedule"]["last_evaluation"]["started_at"] == "t1"
    )


def test_next_decision_times_out(monkeypatch):
    monkeypatch.setattr(testflow, "DECISION_SECONDS", 0)
    with pytest.raises(testflow.Stopped, match="no settled decision"):
        testflow.next_decision(Path("."), None)


def test_decision_interval_reads_the_frozen_trigger(monkeypatch):
    monkeypatch.setattr(
        "almanak.reports.checkpoint", lambda root: {"config": {"decision_trigger": {"seconds": 60}}}
    )
    assert testflow.decision_interval(Path(".")) == 60
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: None)
    with pytest.raises(testflow.Stopped):
        testflow.decision_interval(Path("."))


def test_advance_moves_each_fork_and_the_coordinator_clock(tmp_path, monkeypatch):
    root = workspace(tmp_path, {"fork-eip155:1": {"endpoint": "http://fork"}, "gateway": {}})
    (root / "clock-offset").write_text("0")
    calls = []

    class Rpc:
        def __init__(self, endpoint):
            self.endpoint = endpoint

        def __call__(self, method, params=None):
            calls.append((method, params))
            return {"timestamp": hex(1000)}

    monkeypatch.setattr("almanak.gateway.fork_fixture.Rpc", Rpc)
    testflow.advance(root, 3600)
    testflow.advance(root, 60)
    assert ("evm_setNextBlockTimestamp", [4600]) in calls
    assert (root / "clock-offset").read_text() == "3660"


def test_stopped_coordinator_names_its_last_refusal(tmp_path, monkeypatch):
    root = workspace(tmp_path, {"coordinator": {"pid": 1}})
    (root / "logs" / "coordinator.log").write_text(
        '{"event": "ready"}\n{"error": "gas_limit", "message": "estimated gas"}\nInterrupted\n'
    )
    monkeypatch.setattr("almanak.supervisor_process.running", lambda record: False)
    with pytest.raises(testflow.Stopped, match='"error": "gas_limit"'):
        testflow.require_coordinator(root)
    (root / "logs" / "coordinator.log").write_text("no refusal\n")
    assert testflow.last_error(root) == "inspect logs/coordinator.log"
    (root / "logs" / "coordinator.log").unlink()
    assert testflow.last_error(root) == "inspect logs/coordinator.log"


def test_a_coordinator_holding_on_a_fee_ceiling_stops_the_lifecycle_at_once(tmp_path, monkeypatch):
    root = workspace(tmp_path, {"coordinator": {"pid": 1}})
    log = root / "logs" / "coordinator.log"
    # An earlier run's hold is not this coordinator's.
    log.write_text(json.dumps({"event": "gas_ceiling_hold", "kind": "gas_price"}) + "\n")
    state = json.loads((root / "workspace.json").read_text())
    state["coordinator_log_offset"] = log.stat().st_size
    (root / "workspace.json").write_text(json.dumps(state))
    monkeypatch.setattr("almanak.supervisor_process.running", lambda record: True)
    testflow.require_coordinator(root)
    reason = "market gas quote exceeds the configured price ceiling: quoted 9 wei, max 1"
    hold = {"event": "gas_ceiling_hold", "status": "held", "kind": "gas_price", "reason": reason}
    with log.open("a") as stream:
        stream.write(json.dumps(hold) + "\n")
    with pytest.raises(testflow.Stopped) as stopped:
        testflow.require_coordinator(root)
    assert stopped.value.kind == "gas_price" and not stopped.value.transient
    assert str(stopped.value) == f"coordinator holding on gas_price: {reason}"
    # A held coordinator that has since recovered keeps running.
    with log.open("a") as stream:
        stream.write(json.dumps({"event": "gas_ceiling_recovered", "attempts": 1}) + "\n")
    testflow.require_coordinator(root)


def test_entry_is_a_completed_intent_that_increases_exposure(monkeypatch):
    class Lifecycle:
        @staticmethod
        def increases_exposure(intent):
            return intent.kind == "buy"

    def outcome(intent, *, success=True, complete=True):
        result = Namespace(success=success, complete=complete)
        decision = None if intent is None else Namespace(intent=Namespace(kind=intent))
        return Namespace(success=success, decision=decision, strategy_result=lambda: result)

    state = Namespace(
        config=None,
        outcomes=[
            outcome("buy", complete=False),
            outcome("buy", success=False),
            outcome("sell"),
            outcome(None),
        ],
    )
    monkeypatch.setattr("almanak.state.decode_checkpoint", lambda body: state)
    monkeypatch.setattr(
        "almanak.connectors.composition.connector_for",
        lambda config: Namespace(lifecycle=Lifecycle),
    )
    assert testflow.entered({}) is False
    assert testflow.executed({}) == (["sell"], [])
    state.outcomes.append(outcome("buy"))
    assert testflow.entered({}) is True
    state.outcomes.append(outcome("sell"))
    assert testflow.executed({}) == (["sell", "buy"], ["sell"])


def keeper_cancelled_entry(monkeypatch):
    """A GMX increase whose order its keeper cancelled, then teardown's exit swap.

    The order's creation leaves its request unresolved; the keeper's outcome mines, so
    its transaction succeeded, but GMX's result marks the cancelled request unsuccessful.
    """

    class Lifecycle:
        @staticmethod
        def increases_exposure(intent):
            return intent.kind == "increase_position"

    def outcome(kind, trigger="interval", *, success=True, complete=True):
        result = Namespace(success=success, complete=complete)
        decision = Namespace(intent=Namespace(kind=kind), trigger=Namespace(kind=trigger))
        return Namespace(success=True, decision=decision, strategy_result=lambda: result)

    state = Namespace(
        config=None,
        outcomes=[
            outcome("increase_position", complete=False),
            outcome("increase_position", success=False),
            outcome("exit_exchange", "teardown"),
        ],
    )
    monkeypatch.setattr("almanak.state.decode_checkpoint", lambda body: state)
    monkeypatch.setattr(
        "almanak.connectors.composition.connector_for",
        lambda config: Namespace(lifecycle=Lifecycle),
    )
    return state


def test_a_keeper_cancelled_increase_is_not_an_entry(monkeypatch):
    state = keeper_cancelled_entry(monkeypatch)
    assert testflow.entered({}) is False
    assert testflow.executions(state) == ([], [], ["exit_exchange"])


def test_a_passed_lifecycle_whose_only_entry_was_cancelled_is_not_entered(monkeypatch):
    keeper_cancelled_entry(monkeypatch)
    times = ["t1", "t2", "t3"]
    decisions = sequence([wire(t, outcomes=[]) for t in times])
    monkeypatch.setattr(testflow, "next_decision", lambda root, previous, keepers=None: decisions())
    monkeypatch.setattr(testflow, "advance", lambda root, seconds: None)
    monkeypatch.setattr(testflow, "decision_interval", lambda root: 60)
    verdict = testflow.empty_verdict()
    args = Namespace(decisions=3, advance=60, through_exit=True, idle=1)
    testflow.drive(Path("."), args, verdict)
    # No entry, so the idle stop never armed and every decision ran.
    assert [d["time"] for d in verdict["decisions"]] == times
    assert verdict["entered"] is False
    assert verdict["executed"] == [] and verdict["after_entry"] == []
    verdict.update(admission={"ok": True, "refusal": None}, closed=True, evidence_clean=True)
    verdict["passed"] = testflow.passed(verdict)
    assert testflow.status(verdict) == "not_entered"


def journal_state(mode, **fields):
    values = dict(integrity_error=None, pending=None, plan=None, outcomes=(), delivered=0, work=())
    return Namespace(mode=mode, **{**values, **fields})


def test_teardown_waits_until_closed_and_records_clean_evidence(monkeypatch):
    monkeypatch.setattr("almanak.workspace.request", lambda root, action: {})
    monkeypatch.setattr(
        "almanak.reports.checkpoint", sequence([{"mode": "teardown"}, {"mode": "closed"}])
    )
    monkeypatch.setattr(testflow, "require_coordinator", lambda root, since=None: None)
    monkeypatch.setattr(testflow, "decoded", lambda wire: journal_state(wire["mode"]))
    verdict = testflow.empty_verdict()
    testflow.teardown(Path("."), verdict)
    assert (verdict["closed"], verdict["evidence_clean"], verdict["failures"]) == (
        True,
        True,
        [],
    )


@pytest.mark.parametrize(
    "fields",
    [
        {"integrity_error": "invalidated"},
        {"pending": "attempt"},
        {"plan": "reserved"},
        {"outcomes": (Namespace(accounting_issues=()),)},
        {"outcomes": (Namespace(accounting_issues=("x",)),), "delivered": 1},
        {"work": (Namespace(fault=Namespace(invalidated=True), pending=None, plan=None),)},
        {"work": (Namespace(fault=None, pending="attempt", plan=None),)},
    ],
)
def test_unresolved_journal_evidence_is_not_clean(fields):
    assert testflow.evidence_clean(journal_state("closed"))
    assert not testflow.evidence_clean(journal_state("closed", **fields))


def test_teardown_reports_a_refused_or_stopped_closure(monkeypatch):
    def refused(root, action):
        raise ValueError("provisioning is active")

    monkeypatch.setattr("almanak.workspace.request", refused)
    verdict = testflow.empty_verdict()
    testflow.teardown(Path("."), verdict)
    assert verdict["failures"][0]["stage"] == "teardown"

    def stopped(root, since=None):
        raise testflow.Stopped("coordinator stopped: x")

    monkeypatch.setattr("almanak.workspace.request", lambda root, action: {})
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: {"mode": "teardown"})
    monkeypatch.setattr(testflow, "require_coordinator", stopped)
    monkeypatch.setattr(testflow, "decoded", lambda wire: journal_state(wire["mode"]))
    verdict = testflow.empty_verdict()
    testflow.teardown(Path("."), verdict)
    assert verdict["closed"] is False
    assert verdict["failures"] == [
        {
            "stage": "teardown",
            "message": "coordinator stopped: x",
            "kind": "stopped",
            "class": "strategy",
            "transient": False,
        }
    ]


def test_a_stalled_teardown_advances_fork_time(monkeypatch):
    advanced = []
    states = [{"mode": "teardown"}, {"mode": "teardown"}, {"mode": "closed"}]
    monkeypatch.setattr(testflow, "STALL_SECONDS", 0)
    monkeypatch.setattr("almanak.workspace.request", lambda root, action: {})
    monkeypatch.setattr("almanak.reports.checkpoint", sequence(states))
    monkeypatch.setattr(testflow, "require_coordinator", lambda root, since=None: None)
    monkeypatch.setattr(testflow, "advance", lambda root, step: advanced.append(step))
    monkeypatch.setattr(testflow, "decoded", lambda wire: journal_state(wire["mode"]))
    verdict = testflow.empty_verdict()
    testflow.teardown(Path("."), verdict, 300)
    assert advanced == [300]
    assert verdict["closed"] is True
    assert testflow.progress(None) == ""


def test_interval_is_optional_for_teardown(monkeypatch):
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: None)
    assert testflow.interval_or_none(Path(".")) is None


def test_teardown_times_out(monkeypatch):
    monkeypatch.setattr(testflow, "TEARDOWN_SECONDS", 0)
    monkeypatch.setattr("almanak.workspace.request", lambda root, action: {})
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: None)
    verdict = testflow.empty_verdict()
    testflow.teardown(Path("."), verdict)
    assert "not closed" in verdict["failures"][0]["message"]


def test_cleanup_stops_services_supervisor_and_claims(tmp_path, monkeypatch):
    root = workspace(tmp_path, {"fork-eip155:1": {"pid": 1}, "coordinator": {"pid": 2}})
    stopped, released, controls = [], [], []
    monkeypatch.setattr(testflow, "supervisor_status", lambda root: {"provisioning": False})

    def control(root, action, force=False):
        controls.append(force)
        if not force:
            raise ValueError("remaining obligations")

    monkeypatch.setattr("almanak.workspace.request", control)
    monkeypatch.setattr("almanak.supervisor_process.running", lambda record: True)

    def stop(record, interrupt=False):
        stopped.append((record["pid"], interrupt))
        if record["pid"] == 2:
            raise RuntimeError("did not acknowledge")

    monkeypatch.setattr("almanak.supervisor_process.stop", stop)
    monkeypatch.setattr("almanak.state.release_claims", lambda database: released.append(database))
    verdict = testflow.empty_verdict()
    verdict["closed"] = True
    process = Process()
    testflow.cleanup(root, process, verdict)
    assert controls == [False, True]
    assert sorted(stopped) == [(1, False), (2, True)]
    assert released == [root / "state.sqlite"]
    assert process.terminated
    assert verdict["failures"][0]["message"] == "coordinator: did not acknowledge"


def test_cleanup_without_a_supervisor_is_a_no_op(tmp_path):
    verdict = testflow.empty_verdict()
    testflow.cleanup(tmp_path, None, verdict)
    assert verdict["failures"] == []


def test_failed_shutdown_is_reported(tmp_path, monkeypatch):
    root = workspace(tmp_path)

    def control(root, action, force=False):
        raise ValueError("unavailable")

    monkeypatch.setattr(testflow, "settle_setup", lambda root, process: None)
    monkeypatch.setattr("almanak.workspace.request", control)
    monkeypatch.setattr("almanak.state.release_claims", lambda database: 0)
    verdict = testflow.empty_verdict()
    testflow.cleanup(root, Process(0), verdict)
    assert verdict["failures"][0]["stage"] == "shutdown"


def test_settle_setup_waits_while_provisioning(monkeypatch):
    statuses = sequence([{"provisioning": True}, {"provisioning": False}])
    monkeypatch.setattr(testflow, "supervisor_status", statuses)
    testflow.settle_setup(Path("."), Process())


def journal(path: Path, body: dict) -> None:
    path.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path / "state.sqlite") as db:
        db.execute("CREATE TABLE checkpoint (id INTEGER PRIMARY KEY, body TEXT)")
        db.execute("INSERT INTO checkpoint VALUES (1, ?)", (json.dumps(body),))


def receipt_outcome(kind: str, gas: int, success=True):
    chain = {"chain_id": "eip155:8453", "environment": "anvil:1"}
    return {
        "success": success,
        "operation": {
            "kind": kind,
            "target": {"fund": {"chain": chain}, "pool": {"kind": "uniswap_v3"}},
        },
        "execution": {"binding": {"profile": {"kind": "safe_roles"}}},
        "receipt": {
            "gasUsed": hex(gas),
            "transactionHash": "0x" + "33" * 32,
            "blockNumber": hex(7),
            "blockTimestamp": 99,
        },
    }


def test_gas_reference_harvest_keeps_the_largest_receipt(tmp_path):
    from scripts import gas_references

    journal(
        tmp_path / "a",
        {"outcomes": [receipt_outcome("swap", 400000), receipt_outcome("swap", 1, False)]},
    )
    journal(tmp_path / "b", {"outcomes": [receipt_outcome("swap", 390000), {"success": True}]})
    output = tmp_path / "references.json"
    assert gas_references.main(["--run", f"a={tmp_path / 'a'}", "--output", str(output)]) == 0
    assert gas_references.main(["--run", f"b={tmp_path / 'b'}", "--output", str(output)]) == 0
    [row] = json.loads(output.read_text())["references"]
    assert (row["gas_used"], row["source"]["run"], row["source"]["block_timestamp"]) == (
        400000,
        "a",
        99,
    )
    assert gas_references.target_connector("x") is None
    assert gas_references.quantity("0x10") == 16
    assert gas_references.quantity(None) is None
    with pytest.raises(Exception, match="LABEL=RUN_DIRECTORY"):
        gas_references.labelled("missing")
    output.write_text(json.dumps({"schema": "other"}))
    with pytest.raises(ValueError, match="almanak-gas-references-v1"):
        gas_references.read_references(output)


def test_an_unanswered_estimate_keeps_the_receipt(tmp_path, monkeypatch):
    from scripts import gas_references

    journal(tmp_path / "a", {"outcomes": [receipt_outcome("swap", 400000)]})
    monkeypatch.setattr(gas_references, "fork_endpoints", lambda run: {"eip155:8453": "http://x"})

    def unanswered(*_):
        raise RuntimeError("fork RPC failed")

    monkeypatch.setattr(gas_references, "parent_estimate", unanswered)
    [row] = gas_references.journal_rows("a", tmp_path / "a", estimate=True)
    assert row["gas_used"] == 400000 and "gas_estimate" not in row


def test_fork_config_writes_the_operator_file(tmp_path, monkeypatch):
    from almanak import fork_operator

    funding = {"accounts": [], "payers": []}
    monkeypatch.setattr(
        fork_operator,
        "pinned_anchor",
        lambda chain, env, block, lag, bindings: {
            "chain": str(chain),
            "height": 1,
            "reference": "0x" + "a" * 64,
        },
    )
    monkeypatch.setattr(fork_operator, "draft_funding", lambda args, draft: funding)
    monkeypatch.setattr(fork_operator, "validate", lambda document, operator: None)
    monkeypatch.setattr(fork_operator, "funded_assets", lambda funding, account: [])
    directory = ROOT / "strategies" / "uniswap_v4_avalanche_trading"
    args = Namespace(directory=directory, config=None, strategy=None, env_file=None, block=None)
    args.output, args.force, args.format = tmp_path / "op.yaml", False, "json"
    assert fork_operator.command(args) == 0
    assert "schema_version: operator-gateway-config-v1" in args.output.read_text()
    with pytest.raises(ValueError, match="pass --force"):
        fork_operator.command(args)
    args.force = True
    assert fork_operator.command(args) == 0


def test_fork_config_refuses_one_block_for_several_chains(monkeypatch):
    from almanak import fork_operator

    directory = ROOT / "strategies" / "aave_v4_multichain_borrowing"
    args = Namespace(
        directory=directory, config=None, strategy=None, env_file=None, block=7, output=None
    )
    with pytest.raises(ValueError, match="pins one anchor"):
        fork_operator.command(args)


def test_draft_funding_reads_the_funding_block_even_from_a_refused_check(monkeypatch):
    from almanak import fork_operator

    args = Namespace(directory=Path("."), config=None, strategy=None, env_file=Path(".env"))
    monkeypatch.setattr("almanak.checks.check", lambda namespace: {"funding": {"x": 1}})
    assert fork_operator.draft_funding(args, {}) == {"x": 1}

    def refused(namespace):
        failure = CheckFailure(ValueError("fixture"), [{"stage": "fixture"}], "op.yaml")
        failure.detail["funding"] = {"y": 2}
        raise failure

    monkeypatch.setattr("almanak.checks.check", refused)
    assert fork_operator.draft_funding(args, {}) == {"y": 2}
    monkeypatch.setattr("almanak.checks.check", lambda namespace: {})
    with pytest.raises(RuntimeError, match="no funding block"):
        fork_operator.draft_funding(args, {})


def test_a_step_shorter_than_the_interval_reaches_the_next_due_decision(tmp_path):
    from datetime import UTC, datetime, timedelta

    root = workspace(tmp_path)
    (root / "clock-offset").write_text("600")
    now = datetime.now(UTC) + timedelta(seconds=600)
    due = wire("t1")
    due["schedule"]["next_due_at"] = (now + timedelta(hours=1)).isoformat()
    assert 3590 <= testflow.step(root, due, 60) <= 3600
    assert testflow.step(root, due, 7200) == 7200
    assert testflow.step(root, wire("t1"), 60) == 60
    assert testflow.step(root, None, 60) == 60


def test_a_real_closed_checkpoint_runs_through_the_lifecycle_projections():
    from tests.lending_captures import current_capture

    # The capture predates scope-only authority; read it in today's interface.
    raw = current_capture(
        json.loads(gzip.decompress((FIXTURES / "aave-v3-safe-lifecycle.json.gz").read_bytes()))
    )
    first = raw["outcomes"][0]["decision"]["trigger"]["started_at"]
    decision = testflow.classify(raw, first)
    assert decision["result"] == "intent" and decision["intents"]
    rows = testflow.transactions(raw)
    assert len(rows) == len(raw["outcomes"])
    assert all(row["status"] == "success" and row["via_roles"] for row in rows)
    assert all(row["gas_used"] > 0 for row in rows)
    assert testflow.settled(raw)
    assert testflow.entered(raw)
    state = testflow.decoded(raw)
    assert state.mode == "closed" and testflow.evidence_clean(state)
    undelivered = testflow.decoded({**raw, "delivered": raw["delivered"] - 1})
    assert not testflow.evidence_clean(undelivered)


def test_test_command_parses_the_contract_flags():
    from almanak.cli_arguments import build_parser

    args = build_parser().parse_args(
        [
            "test",
            "run",
            "strategy",
            "--gateway-config",
            "op.yaml",
            "--env-file",
            ".env",
            "--output",
            "run",
            "--decisions",
            "4",
            "--advance",
            "60",
            "--json",
        ]
    )
    assert (args.product, args.decisions, args.advance, args.format) == ("test", 4, 60, "json")
    assert (args.fork, args.production) == (True, False)
    config = build_parser().parse_args(
        ["test", "config", "strategy", "--env-file", ".env", "--block", "7", "--output", "o"]
    )
    assert (config.product, config.block, config.force) == ("test-config", 7, False)


def test_cli_dispatches_test_run_and_config(monkeypatch):
    from almanak.cli import dispatch

    monkeypatch.setattr("almanak.testflow.command", lambda args: 7)
    monkeypatch.setattr("almanak.fork_operator.command", lambda args: 8)
    assert dispatch(Namespace(product="test")) == 7
    assert dispatch(Namespace(product="test-config")) == 8


@pytest.mark.parametrize(
    ("line", "kind", "infra", "transient"),
    [
        ('{"error": "rpc", "message": "JSON-RPC -32603"}', "rpc", True, False),
        (
            '{"error": "unavailable", "message": "block", "transient": true}',
            "unavailable",
            True,
            True,
        ),
        (
            '{"error": "gas_limit", "message": "estimated gas", "transient": true}',
            "gas_limit",
            False,
            False,
        ),
        ('{"error": "rpc", "message": "eth_call reverted", "reverted": true}', "rpc", False, False),
        ("no refusal", "coordinator_stopped", False, False),
    ],
)
def test_a_stopped_coordinator_carries_its_refusal_kind(
    tmp_path, monkeypatch, line, kind, infra, transient
):
    root = workspace(tmp_path, {"coordinator": {"pid": 1}})
    (root / "logs" / "coordinator.log").write_text(line + "\n")
    monkeypatch.setattr("almanak.supervisor_process.running", lambda record: False)
    with pytest.raises(testflow.Stopped) as stopped:
        testflow.require_coordinator(root)
    verdict = testflow.empty_verdict()
    stop = stopped.value
    testflow.failure(verdict, "decision", str(stop), stop.kind, stop.transient, stop.reverted)
    entry = verdict["failures"][0]
    assert (entry["kind"], entry["class"], entry["transient"]) == (
        kind,
        "infra" if infra else "strategy",
        transient,
    )


def test_failure_class_is_the_refusal_else_the_first_failure():
    verdict = testflow.empty_verdict()
    assert testflow.failure_class(verdict) is None
    testflow.failure(verdict, "decision", "x", "transport", True)
    testflow.failure(verdict, "teardown", "y")
    assert testflow.failure_class(verdict) == "infra"
    testflow.refused(verdict, "failed", "contract has no bytecode", "unsupported_on_fork")
    refusal = verdict["admission"]["refusal"]
    assert (refusal["error"], refusal["kind"], refusal["class"], refusal["transient"]) == (
        "unsupported_on_fork",
        "unsupported_on_fork",
        "infra",
        False,
    )
    testflow.refused(verdict, "failed", "funding", "start")
    assert testflow.failure_class(verdict) == "strategy"


def test_an_admission_timeout_is_an_infra_failure(monkeypatch):
    monkeypatch.setattr(testflow, "ADMISSION_SECONDS", 0)
    verdict = testflow.empty_verdict()
    testflow.admitted(Path("."), Process(), verdict)
    refusal = verdict["admission"]["refusal"]
    assert (refusal["kind"], refusal["class"], refusal["transient"]) == (
        "admission_timeout",
        "infra",
        False,
    )


def test_a_transient_input_refusal_keeps_its_mark(tmp_path, monkeypatch):
    from almanak.gateway.ethereum import DataError

    def refuse(args, **options):
        error = DataError("transport", "provider unreachable")
        error.transient = True
        raise error

    monkeypatch.setattr("almanak.workspace.prepare", refuse)
    verdict = testflow.empty_verdict()
    args = Namespace(output=tmp_path / "run", param=None, production_parameters=False)
    testflow.start(args, tmp_path / "run", verdict)
    refusal = verdict["admission"]["refusal"]
    assert (refusal["kind"], refusal["class"], refusal["transient"]) == ("transport", "infra", True)


def test_a_keeper_failure_carries_its_data_error_kind():
    from almanak.gateway.ethereum import DataError

    class Keepers:
        failures: list[str] = []

        def settle(self, wire):
            raise DataError("rpc", "settle: JSON-RPC -32000")

    with pytest.raises(testflow.Stopped) as stopped:
        testflow.settle_requests(Keepers(), {})
    assert (stopped.value.kind, stopped.value.transient) == ("rpc", False)


def test_a_revert_keeps_its_mark_through_the_gateway_and_the_error_document():
    from almanak.cli import error_document
    from almanak.gateway.ethereum import DataError, Reverted
    from almanak.gateway.service import data_error_reply

    revert = Reverted("eth_call: reverted", b"", code=3)
    assert data_error_reply(revert)["reverted"] is True
    assert error_document(revert)["reverted"] is True
    assert "reverted" not in error_document(DataError("rpc", "eth_call: -32603"))


def test_a_transient_read_is_classified_transient_from_the_coordinator_log(tmp_path, monkeypatch):
    """A read the gateway marked transient stays transient through the logged refusal."""
    from almanak.cli import error_document
    from almanak.gateway.ethereum import DataError

    held = DataError("unavailable", "eth_getBlockByNumber: block unavailable", transient=True)
    root = workspace(tmp_path, {"coordinator": {"pid": 1}})
    (root / "logs" / "coordinator.log").write_text(json.dumps(error_document(held)) + "\n")
    monkeypatch.setattr("almanak.supervisor_process.running", lambda record: False)
    with pytest.raises(testflow.Stopped) as stopped:
        testflow.require_coordinator(root)
    verdict = testflow.empty_verdict()
    stop = stopped.value
    testflow.failure(verdict, "decision", str(stop), stop.kind, stop.transient, stop.reverted)
    entry = verdict["failures"][0]
    assert (entry["kind"], entry["class"], entry["transient"]) == ("unavailable", "infra", True)
    assert testflow.failure_class(verdict) == "infra"


def test_teardown_ignores_a_fee_ceiling_hold_logged_before_it(tmp_path, monkeypatch):
    root = workspace(tmp_path, {"coordinator": {"pid": 1}})
    log = root / "logs" / "coordinator.log"
    hold = {"event": "gas_ceiling_hold", "kind": "gas_price", "reason": "quoted 9 wei"}
    log.write_text(json.dumps(hold) + "\n")
    state = json.loads((root / "workspace.json").read_text())
    state["coordinator_log_offset"] = 0
    (root / "workspace.json").write_text(json.dumps(state))
    monkeypatch.setattr("almanak.supervisor_process.running", lambda record: True)
    since = log.stat().st_size
    with pytest.raises(testflow.Stopped):
        testflow.require_coordinator(root)
    testflow.require_coordinator(root, since)
    with log.open("a") as stream:
        stream.write(json.dumps(hold) + "\n")
    with pytest.raises(testflow.Stopped, match="holding on gas_price"):
        testflow.require_coordinator(root, since)


def test_the_teardown_log_offset_needs_no_workspace(tmp_path):
    assert testflow.coordinator_log_end(None) is None
    root = workspace(tmp_path)
    assert testflow.coordinator_log_end(root) == 0
    (root / "logs" / "coordinator.log").write_text("line\n")
    assert testflow.coordinator_log_end(str(root)) == 5


def test_a_fresh_run_releases_the_claims_a_killed_run_left_on_its_output(
    tmp_path, monkeypatch, recorded, reference_strategy
):
    """A worker reuses one output path; a run killed before cleanup must not block the next."""
    import shutil

    from almanak.state import Journal, Lease, claims_root

    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    root = tmp_path / "testflow" / "lifecycle"
    root.mkdir(parents=True)
    killed = Journal(root / "state.sqlite")
    killed.create(recorded.config, reference_strategy)
    Lease(killed, killed.load()).close()
    elsewhere = Journal(tmp_path / "other" / "state.sqlite")
    elsewhere.path.parent.mkdir()
    elsewhere.create(recorded.config, reference_strategy)
    other = claims_root() / "other.claim"
    other.write_text(json.dumps({"deployment_id": "x", "database": str(elsewhere.path)}))
    # The kill skipped cleanup; the worker removes the output before the next run.
    shutil.rmtree(root)
    root.mkdir()
    fresh = Journal(root / "state.sqlite")
    fresh.create(recorded.config, reference_strategy)
    with pytest.raises(RuntimeError, match="already claimed"):
        Lease(fresh, fresh.load())
    shutil.rmtree(root)

    monkeypatch.setattr("almanak.workspace.prepare", lambda args, **options: root.mkdir())
    monkeypatch.setattr("almanak.workspace.launch", lambda root: "process")
    verdict = testflow.empty_verdict()
    args = Namespace(output=root, param=None, production_parameters=False)
    assert testflow.start(args, root, verdict) == "process"
    fresh = Journal(root / "state.sqlite")
    fresh.create(recorded.config, reference_strategy)
    Lease(fresh, fresh.load()).close()
    assert other.exists()


def test_claims_under_an_existing_output_are_never_released(tmp_path, monkeypatch):
    from almanak.state import claims_root

    monkeypatch.setattr(Path, "home", lambda: tmp_path / "home")
    root = tmp_path / "lifecycle"
    root.mkdir()
    claims_root().mkdir(parents=True)
    claim = claims_root() / "live.claim"
    claim.write_text(json.dumps({"deployment_id": "x", "database": str(root / "state.sqlite")}))
    assert testflow.release_orphaned_claims(root) == 0
    assert claim.exists()
    root.rmdir()
    assert testflow.release_orphaned_claims(root) == 1
    assert not claim.exists()
