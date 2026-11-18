"""Teardown decisions, the full teardown sequence and final holdings in the test verdict."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak import testflow, testflow_evidence
from almanak.cli import error_document
from almanak.decision_facts import teardown_evaluations
from tests.test_testflow_lifecycle import FIXTURES, workspace
from tests.test_testflow_verdict_causes import remaining_refusal

FUND = "0xb845feb18ced83e8d376d2ba773fc8cdc19e3a9b"
WETH = "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
USDC = "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
NATIVE = "eip155:1/slip44:60"


@pytest.fixture(autouse=True)
def no_waiting(monkeypatch):
    monkeypatch.setattr(testflow, "POLL_SECONDS", 0)


def lifecycle():
    from tests.lending_captures import current_capture

    return current_capture(
        json.loads(gzip.decompress((FIXTURES / "aave-v3-safe-lifecycle.json.gz").read_bytes()))
    )


def evaluated(time, trigger, result, intent=None, **facts):
    line = {"event": "decision_evaluated", "time": time, "trigger": trigger, "result": result}
    if intent:
        line["intent"] = intent
    return json.dumps({**line, "chain": None, "evaluated": True, "observed": ["loan"], **facts})


def test_a_new_verdict_carries_the_closure_evidence_fields():
    verdict = testflow.empty_verdict()
    assert verdict["teardown_sequence"] == verdict["teardown_decisions"] == []
    assert verdict["holdings"] == []


def test_teardown_evaluations_keep_every_teardown_row_in_order(tmp_path):
    log = tmp_path / "coordinator.log"
    log.write_text(
        "\n".join(
            [
                evaluated("t1", "interval", "intent", "supply"),
                "not json decision_evaluated",
                evaluated("t2", "teardown", "intent", "swap_exact_input"),
                evaluated("t2", "teardown", "intent", "swap_exact_input"),
                evaluated("t3", "teardown", "hold", observation_errors=[]),
            ]
        )
        + "\n"
    )
    rows = teardown_evaluations(log)
    assert [(r["time"], r["result"], r.get("intents")) for r in rows] == [
        ("t2", "intent", ["swap_exact_input"]),
        ("t2", "intent", ["swap_exact_input"]),
        ("t3", "hold", None),
    ]
    assert rows[-1]["observed"] == ["loan"] and "chain" not in rows[-1]
    assert teardown_evaluations(tmp_path / "missing.log") == []


def test_a_teardown_hold_that_left_debt_open_is_visible_with_its_refusal(tmp_path, monkeypatch):
    refusal = remaining_refusal()
    root = workspace(tmp_path, {"coordinator": {"pid": 1}})
    (root / "logs" / "coordinator.log").write_text(
        evaluated("t1", "interval", "intent", "borrow")
        + "\n"
        + evaluated("t2", "teardown", "hold")
        + "\n"
        + json.dumps(error_document(refusal))
        + "\n"
    )
    monkeypatch.setattr("almanak.supervisor_process.running", lambda record: False)
    monkeypatch.setattr("almanak.workspace.request", lambda root, action: {})
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: {"mode": "teardown"})
    monkeypatch.setattr(testflow, "record_closure", lambda root, verdict: None)
    verdict = testflow.empty_verdict()
    testflow.teardown(root, verdict)
    [row] = verdict["teardown_decisions"]
    assert (row["time"], row["result"]) == ("t2", "hold")
    assert (row["closure"], row["refusal"]) == ("not_closed", "remaining_obligations")


def test_a_closing_hold_is_marked_closed(tmp_path):
    (tmp_path / "logs").mkdir()
    (tmp_path / "logs" / "coordinator.log").write_text(
        evaluated("t1", "teardown", "intent", "repay") + "\n" + evaluated("t2", "teardown", "hold")
    )
    verdict = {**testflow.empty_verdict(), "closed": True}
    rows = testflow_evidence.teardown_decisions(tmp_path, verdict)
    assert [r["result"] for r in rows] == ["intent", "hold"]
    assert "closure" not in rows[0] and rows[1]["closure"] == "closed"


def test_the_sequence_keeps_repeated_teardown_intents_that_executed_lists_once():
    state = testflow.decoded(lifecycle())
    first = [o for o in state.outcomes if testflow.by_teardown(o) and testflow.completed(o)][0]
    repeated = SimpleNamespace(outcomes=(*state.outcomes, first), config=state.config)
    assert testflow.executions(repeated)[2] == [
        "repay_amount",
        "withdraw_amount",
        "repay",
        "withdraw",
    ]
    assert testflow_evidence.teardown_sequence(repeated) == [
        {"intent": "repay_amount", "allocation": "loan"},
        {"intent": "withdraw_amount", "allocation": "collateral"},
        {"intent": "repay", "allocation": "loan"},
        {"intent": "withdraw", "allocation": "collateral"},
        {"intent": "repay_amount", "allocation": "loan"},
    ]


def test_closure_records_each_funds_final_holdings(tmp_path, monkeypatch):
    raw = lifecycle()
    (tmp_path / "workspace.json").write_text(
        json.dumps({"processes": {"fork-eip155:1": {"endpoint": "http://fork"}}})
    )
    balances = {WETH: 10**16 + 5, USDC: 0}
    calls = []

    def rpc(self, method, params=(), *, timeout=30):
        calls.append((self.endpoint, method))
        if method == "eth_getBalance":
            assert params == [FUND, "latest"]
            return hex(0)
        call, block = params
        assert block == "latest" and call["data"].endswith(FUND[2:])
        asset = next(a for a in balances if a.endswith(call["to"]))
        return hex(balances[asset])

    monkeypatch.setattr("almanak.gateway.fork_fixture.Rpc.__call__", rpc)
    monkeypatch.setattr("almanak.reports.checkpoint", lambda root: raw)
    verdict = testflow.empty_verdict()
    testflow.record_closure(tmp_path, verdict)
    [fund] = verdict["holdings"]
    assert (fund["account"], fund["chain"]) == ("fund", "eip155:1")
    assert fund["balances"] == [
        {"asset": USDC, "amount": "0", "symbol": "USDC", "quantity": "0"},
        {
            "asset": WETH,
            "amount": str(10**16 + 5),
            "symbol": "WETH",
            "quantity": "0.010000000000000005",
        },
        {"asset": NATIVE, "amount": "0", "quantity": "0"},
    ]
    assert {endpoint for endpoint, _ in calls} == {"http://fork"}
    assert verdict["teardown_sequence"][-1] == {"intent": "withdraw", "allocation": "collateral"}


def test_holdings_name_a_failed_read_and_need_a_workspace(tmp_path, monkeypatch):
    state = testflow.decoded(lifecycle())
    assert testflow_evidence.holdings(Path(tmp_path), state) == []
    (tmp_path / "workspace.json").write_text(json.dumps({"processes": {}}))
    [fund] = testflow_evidence.holdings(tmp_path, state)
    assert fund["error"] == "the run has no fork node for this fund" and not fund["balances"]

    def down(self, method, params=(), *, timeout=30):
        raise RuntimeError("fork RPC failed: eth_call: gone")

    (tmp_path / "workspace.json").write_text(
        json.dumps({"processes": {"fork-eip155:1": {"endpoint": "http://fork"}}})
    )
    monkeypatch.setattr("almanak.gateway.fork_fixture.Rpc.__call__", down)
    [fund] = testflow_evidence.holdings(tmp_path, state)
    assert fund["error"] == "balance read failed: fork RPC failed: eth_call: gone"
