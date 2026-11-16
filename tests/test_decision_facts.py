"""Each evaluated decision logs the facts the runtime held; the test verdict carries them."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

from almanak import decision_facts, testflow
from almanak.backtest.types import UnixTime
from almanak.gateway.ethereum import DataError
from almanak.price_series import PricePoint, PriceSeries
from almanak.triggers import IntervalEvent
from almanak.work import ChainAvailability, ChainFault
from tests.pool_price_chain import CHAIN
from tests.test_strategy_prices import HOUR, PriceGateway, binding, price_runtime

NOW = datetime.fromtimestamp(10 * HOUR, UTC)


def series(count, window=4):
    points = tuple(
        PricePoint(source_time=UnixTime(10 * HOUR - (count - 1 - i) * HOUR), value=Decimal(3000))
        for i in range(count)
    )
    return PriceSeries(
        binding=binding(window=window), cutoff=UnixTime(10 * HOUR), points=points, evidence=()
    )


def context(prices=(), availability=()):
    return SimpleNamespace(
        trigger=IntervalEvent(started_at=NOW, due_at=NOW),
        observations=(SimpleNamespace(allocation="swap"), SimpleNamespace(allocation="lp")),
        prices=prices,
        availability=availability,
    )


def test_a_hold_states_the_series_it_saw_and_the_chains_not_ready():
    faulted = ChainAvailability(
        chain=CHAIN, status="unavailable", fault=ChainFault(kind="rpc", message="HTTP 429")
    )
    ready = ChainAvailability(chain=CHAIN, status="ready")
    hold = SimpleNamespace(intent=None)
    facts = decision_facts.facts(context((series(2),), (ready, faulted)), hold)
    assert (
        facts["time"]
        == IntervalEvent(started_at=NOW, due_at=NOW).model_dump(mode="json")["started_at"]
    )
    assert facts["evaluated"] is True and facts["result"] == "hold" and "intent" not in facts
    assert facts["observed"] == ["lp", "swap"]
    [row] = facts["price_series"]
    assert row["complete"] is False and (row["points"], row["window"]) == (2, 4)
    assert row["gaps"] == 0 and row["age_seconds"] == 0 and row["timeframe"] == "1h"
    assert facts["availability"] == [faulted.model_dump(mode="json")]


def test_a_complete_series_and_an_intent_are_stated():
    intent = SimpleNamespace(intent=SimpleNamespace(kind="swap_exact_input"))
    facts = decision_facts.facts(context((series(4),)), intent)
    assert facts["result"] == "intent" and facts["intent"] == "swap_exact_input"
    assert facts["price_series"][0]["complete"] is True and facts["availability"] == []


def series_hold(monkeypatch, opted_in):
    gateway = PriceGateway(DataError("http", "eth_getLogs: HTTP 429"))
    runtime, state, decided, held = price_runtime(monkeypatch, gateway)
    if opted_in:
        runtime.decision_facts = True
    state.schedule = SimpleNamespace(
        event=lambda now, teardown: IntervalEvent(started_at=now, due_at=now)
    )
    monkeypatch.setattr(
        runtime.connector.codec, "decision_input", lambda **fields: context(availability=())
    )
    assert runtime.apply_decision(state, ()) is False and held == ["kept"]
    return runtime


def emitted(capsys):
    lines = [json.loads(line) for line in capsys.readouterr().err.splitlines()]
    return [line for line in lines if line.get("event") == "decision_evaluated"]


def test_a_series_hold_states_the_strategy_did_not_run(monkeypatch, capsys):
    series_hold(monkeypatch, opted_in=True)
    [facts] = emitted(capsys)
    assert facts["evaluated"] is False and facts["result"] == "hold"
    assert facts["price_error"] == {"kind": "http", "message": "eth_getLogs: HTTP 429"}


def test_a_coordinator_not_opted_in_never_logs_facts(monkeypatch, capsys):
    series_hold(monkeypatch, opted_in=False)
    assert emitted(capsys) == []


def test_facts_are_logged_only_after_the_decision_is_recorded(monkeypatch):
    from almanak import runtime as runtime_module

    order = []
    # A teardown decision reads no series, so the strategy's answer is recorded directly.
    runtime, state, decided, held = price_runtime(monkeypatch, PriceGateway(), mode="teardown")
    runtime.decision_facts = True
    monkeypatch.setattr(
        runtime_module, "strategy_decision", lambda strategy, ctx: SimpleNamespace(intent=None)
    )
    monkeypatch.setattr(runtime, "record_decision", lambda *a: order.append("recorded") or False)
    monkeypatch.setattr(decision_facts, "log", lambda *a, **k: order.append("logged"))
    runtime.apply_decision(state, ())
    assert order == ["recorded", "logged"]


def test_only_a_test_workspace_starts_its_coordinator_logging_facts(tmp_path, monkeypatch):
    from almanak import supervisor as supervisor_module
    from almanak.cli_arguments import build_parser

    spawned = []
    monkeypatch.setattr(supervisor_module, "spawn", lambda root, name, args: spawned.append(args))
    supervisor = object.__new__(supervisor_module.Supervisor)
    supervisor.root = tmp_path
    supervisor.persist = lambda **fields: None
    for state in ({}, {"clock_offset_file": "clock-offset", "decision_facts": True}):
        supervisor.state = {"gateway_url": "http://gateway", "processes": {}, **state}
        supervisor.spawn_coordinator(create=False)
    production, test = spawned
    assert "--decision-facts" not in production and "--decision-facts" in test
    parsed = build_parser().parse_args(test)
    assert parsed.decision_facts is True
    assert build_parser().parse_args(production).decision_facts is False


def test_unreadable_facts_never_stop_the_decision(capsys):
    decision_facts.log(SimpleNamespace(trigger=None))
    assert capsys.readouterr().err == ""


def test_a_failing_log_stream_never_stops_the_decision(monkeypatch):
    class Closed:
        def write(self, text):
            raise ValueError("I/O operation on closed file")

        def flush(self):
            raise OSError("broken pipe")

    monkeypatch.setattr(decision_facts.sys, "stderr", Closed())
    decision_facts.log(context(), SimpleNamespace(intent=None))


def test_a_scoped_tick_names_its_chain():
    facts = decision_facts.facts(context(), SimpleNamespace(intent=None), chain=CHAIN)
    assert facts["chain"] == CHAIN.model_dump(mode="json")
    assert decision_facts.facts(context(), SimpleNamespace(intent=None))["chain"] is None


def test_the_verdict_row_carries_the_facts_logged_for_its_time(tmp_path):
    root = tmp_path
    (root / "logs").mkdir()
    early = {"event": "decision_evaluated", "time": "t0", "evaluated": False}
    late = {
        "event": "decision_evaluated",
        "time": "t1",
        "result": "hold",
        "evaluated": True,
        "observed": ["swap"],
        "price_series": [{"asset": "a", "complete": False, "points": 1, "window": 120}],
        "availability": [],
    }
    lines = [json.dumps(early), "not json", '{"error": "x", "message": "y"}', json.dumps(late)]
    (root / "logs" / "coordinator.log").write_text("\n".join(lines) + "\n")
    row = testflow.with_facts(root, {"time": "t1", "result": "hold"})
    assert row == {
        "time": "t1",
        "result": "hold",
        "evaluated": True,
        "observed": ["swap"],
        "price_series": late["price_series"],
        "availability": [],
    }
    assert testflow.with_facts(root, {"time": "t2", "result": "hold"}) == {
        "time": "t2",
        "result": "hold",
    }
    assert testflow.with_facts(Path(tmp_path / "missing"), {"time": "t1"}) == {"time": "t1"}


def test_two_chains_logging_one_time_keep_their_own_facts(tmp_path):
    (tmp_path / "logs").mkdir()
    base = {"chain_id": "eip155:8453", "environment": "anvil"}
    arbitrum = {"chain_id": "eip155:42161", "environment": "anvil"}
    lines = [
        {
            "event": "decision_evaluated",
            "time": "t1",
            "chain": base,
            "evaluated": True,
            "observed": ["base_swap"],
        },
        {
            "event": "decision_evaluated",
            "time": "t1",
            "chain": arbitrum,
            "evaluated": False,
            "observed": ["arb_swap"],
        },
    ]
    (tmp_path / "logs" / "coordinator.log").write_text(
        "\n".join(json.dumps(line) for line in lines) + "\n"
    )
    assert len(decision_facts.logged(tmp_path / "logs" / "coordinator.log")["t1"]) == 2
    intent = {
        "trigger": {"started_at": "t1"},
        "intent": {"kind": "swap", "target": {"fund": {"chain": arbitrum}}},
    }
    wire = {"outcomes": [], "work": [{"decision": intent}]}
    row = testflow.with_facts(tmp_path, {"time": "t1", "result": "intent"}, wire)
    assert row["chain"] == arbitrum and row["observed"] == ["arb_swap"]
    assert row["evaluated"] is False
    # Facts logged on another chain than the intent's never attach, even when alone.
    only_base = tmp_path / "base"
    (only_base / "logs").mkdir(parents=True)
    (only_base / "logs" / "coordinator.log").write_text(json.dumps(lines[0]) + "\n")
    lone = testflow.with_facts(only_base, {"time": "t1", "result": "intent"}, wire)
    assert lone == {"time": "t1", "result": "intent"}
    # A hold names no chain, so neither chain's facts are attached to it.
    hold = testflow.with_facts(tmp_path, {"time": "t1", "result": "hold"}, {"outcomes": []})
    assert hold == {"time": "t1", "result": "hold"}


def test_two_lines_for_one_chain_and_time_are_dropped_as_ambiguous(tmp_path):
    (tmp_path / "logs").mkdir()
    line = {"event": "decision_evaluated", "time": "t1", "evaluated": True}
    (tmp_path / "logs" / "coordinator.log").write_text(
        json.dumps(line) + "\n" + json.dumps({**line, "evaluated": False}) + "\n"
    )
    assert decision_facts.logged(tmp_path / "logs" / "coordinator.log") == {"t1": []}
    assert testflow.with_facts(tmp_path, {"time": "t1"}) == {"time": "t1"}


def test_a_hold_and_an_intent_carry_the_reason_the_strategy_stated():
    hold = SimpleNamespace(intent=None, reason="Holding: no executable debt-buyback quote")
    facts = decision_facts.facts(context(), hold)
    assert facts["result"] == "hold" and facts["reason"] == hold.reason
    intent = SimpleNamespace(intent=SimpleNamespace(kind="borrow"), reason="round 2 of 3")
    facts = decision_facts.facts(context(), intent)
    assert (facts["intent"], facts["reason"]) == ("borrow", "round 2 of 3")


def test_an_unstated_reason_is_null_and_a_long_one_is_cut():
    assert decision_facts.facts(context(), SimpleNamespace(intent=None))["reason"] is None
    assert decision_facts.facts(context(), None, DataError("http", "429"))["reason"] is None
    long = SimpleNamespace(intent=None, reason="x" * 1000)
    reason = decision_facts.facts(context(), long)["reason"]
    assert reason == "x" * decision_facts.REASON_CHARACTERS


def test_a_typed_decision_keeps_its_reason_off_the_wire_unless_stated():
    from almanak.connectors.compound_v3.state import Decision
    from almanak.strategy_api import Memory

    plain = Decision(memory=Memory())
    assert "reason" not in plain.model_dump_json() and plain.reason is None
    wire = Decision(memory=Memory(), reason="held").model_dump_json()
    stated = Decision.model_validate_json(wire)
    assert decision_facts.facts(context(), stated)["reason"] == "held"
