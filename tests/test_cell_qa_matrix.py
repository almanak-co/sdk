"""The hosted cell QA plan selects a cell's complete case set and refuses unsafe widening."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.ci import cell_qa_matrix
from scripts.ci.cell_qa_matrix import (
    bench_seconds,
    collect_reversal_nodes,
    job_minutes,
    main,
    node_reference,
    plan,
)
from tests.qa.catalog import Case, catalog

CELL = {"connector": "uniswap_v3", "chain": "eip155:56"}
COMPOSED = "bnb_aave_borrow_and_trade_v3"
REVERSAL = (
    "tests/fork/test_aave_composition_reversal.py::"
    f"test_router_failure_preserves_lending_obligations[{COMPOSED}]"
)


@pytest.fixture(scope="module")
def cases():
    return catalog()


def selection(**changes):
    values = dict(
        connector="",
        chain="",
        cases="",
        lanes="fork,backtest",
        reverse_fault=True,
        nodes="",
        commit="abc",
    )
    return SimpleNamespace(**{**values, **changes})


def collected():
    return [(REVERSAL, COMPOSED), ("tests/fork/test_other.py::test_x[unrelated]", "unrelated")]


def test_cell_plan_holds_every_case_of_the_cell_and_its_reverse_fault(cases):
    value = plan(selection(**CELL), cases, collected)
    entries = value["entries"]
    ids = [entry["id"] for entry in entries if entry["kind"] == "case"]
    expected = [
        case.id
        for case in cases
        if any(
            str(o.connector) == "uniswap_v3" and str(o.chain) == "eip155:56"
            for o in case.obligations()
        )
    ]
    assert ids == expected and len(ids) == 31
    assert [e["id"] for e in entries if e["kind"] == "node"] == [REVERSAL]
    assert value["reverse_fault"] == {"requested": True, "nodes": [REVERSAL]}
    assert value["cell"] == CELL and value["commit"] == "abc" and value["excluded"] == []
    assert COMPOSED in value["references"]
    assert [e["index"] for e in entries] == list(range(len(entries)))
    assert entries[0]["artifact"] == "cell-qa-entry-000"
    fork = next(e for e in entries if e["lane"] == "fork")
    assert (fork["bench_timeout"], fork["timeout_minutes"]) == (3600, 90)
    backtest = next(e for e in entries if e["lane"] == "backtest")
    assert (backtest["bench_timeout"], backtest["timeout_minutes"]) == (9000, 180)


def test_unselected_and_local_lanes_stay_visible_as_not_run(cases):
    value = plan(
        selection(connector="uniswap_v4", chain="eip155:56", lanes="fork"), cases, collected
    )
    excluded = {e["id"]: e["reason"] for e in value["excluded"]}
    assert excluded["model-actions/uniswap_v4/eip155:56"] == "lane not selected"
    assert all(e["lane"] == "fork" for e in value["entries"] if e["kind"] == "case")
    blocked = Case(id="blocked/x", lane="blocked")
    hosted, excluded = cell_qa_matrix.partition([blocked], ("fork",))
    assert not hosted and "candidate" in excluded[0]["reason"]


def test_explicit_cases_run_without_a_cell_or_reverse_fault(cases):
    chosen = "fork/pinned/uniswap_v3_bnb_lp/eoa/configured"
    value = plan(selection(cases=f"{chosen},\n{chosen}"), cases, collected)
    assert [e["id"] for e in value["entries"]] == [chosen]
    assert value["cell"] is None and value["reverse_fault"] == {"requested": False, "nodes": []}
    extra = "tests/fork/test_uniswap_lp_cleanup.py::test_cleanup[eoa-uniswap_v3_bnb_lp]"
    value = plan(selection(cases=chosen, nodes=extra), cases, collected)
    assert value["entries"][-1] == {
        "id": extra,
        "kind": "node",
        "lane": "faults",
        "reference": "",
        "bench_timeout": 0,
        "timeout_minutes": 150,
        "index": 1,
        "artifact": "cell-qa-entry-001",
    }


def test_discovered_reversal_is_not_duplicated_by_an_explicit_node(cases):
    value = plan(selection(**CELL, nodes=REVERSAL), cases, collected)
    assert [e["id"] for e in value["entries"]].count(REVERSAL) == 1
    value = plan(selection(**CELL, reverse_fault=False), cases, collected)
    assert value["reverse_fault"] == {"requested": False, "nodes": []}
    assert all(e["kind"] == "case" for e in value["entries"])


@pytest.mark.parametrize(
    "changes,message",
    [
        ({}, "either a cell"),
        ({**CELL, "cases": "x"}, "either a cell"),
        ({"connector": "uniswap_v3"}, "both a connector"),
        ({"connector": "uniswap_v3", "chain": "56"}, "invalid cell identity"),
        ({"connector": "uniswap_v3", "chain": "eip155:999999"}, "no QA case proves"),
        ({"cases": "missing/case"}, "unknown case IDs"),
        ({**CELL, "lanes": "fork,offline"}, "cannot pass on hosted runners"),
        ({**CELL, "lanes": "fork,faults"}, "cannot pass on hosted runners"),
        ({**CELL, "lanes": "forks"}, "unknown lanes"),
        ({**CELL, "nodes": "tests/fork/test_x.py::test_y; rm -rf /"}, "node IDs must name"),
        ({**CELL, "nodes": "src/almanak/cli.py::main"}, "node IDs must name"),
        ({**CELL, "lanes": "pytest", "reverse_fault": False}, "leaves no entry"),
    ],
)
def test_plan_refuses_ambiguous_unknown_or_unsafe_selections(cases, changes, message):
    with pytest.raises(SystemExit, match=message):
        plan(selection(**changes), cases, collected)


def test_matrix_limit_is_refused(cases, monkeypatch):
    monkeypatch.setattr(cell_qa_matrix, "MAX_ENTRIES", 3)
    with pytest.raises(SystemExit, match="matrix limit"):
        plan(selection(**CELL), cases, collected)


def test_long_native_preparation_keeps_its_budget_inside_the_hosted_limit(cases):
    native = next(
        c for c in cases if c.id == "backtest/native/arbitrum_aave_curve_borrow_and_trade/economic"
    )
    assert bench_seconds(native) == 7200 + 7200
    assert job_minutes(bench_seconds(native)) == 270
    detailed = next(c for c in cases if c.model_mode == "historical_path")
    assert bench_seconds(detailed) == 7200
    with pytest.raises(SystemExit, match="hosted job limit"):
        job_minutes(6 * 3600)


def test_reversal_discovery_reads_parameters_and_single_reference_modules():
    assert node_reference(REVERSAL) == COMPOSED
    module = (
        "tests/fork/test_base_v3_composition_reversal.py::"
        "test_base_borrow_and_trade_router_failure_preserves_lending_obligations"
    )
    assert node_reference(module) == "base_borrow_and_trade"
    discovered = dict(collect_reversal_nodes())
    assert discovered[REVERSAL] == COMPOSED
    assert discovered[module] == "base_borrow_and_trade"


def test_main_writes_the_plan_prints_the_matrix_and_warns_without_reversal(
    tmp_path, capsys, monkeypatch
):
    monkeypatch.setattr(cell_qa_matrix, "collect_reversal_nodes", lambda: [])
    output = tmp_path / "plan" / "plan.json"
    arguments = ["plan", "--connector", "uniswap_v3", "--chain", "eip155:56"]
    assert main([*arguments, "--commit", "abc", "--output", str(output)]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == json.loads(output.read_text())["entries"]
    assert "no reverse-fault node" in captured.err


def test_freeze_resolves_only_the_planned_cases(tmp_path, monkeypatch):
    chosen = "fork/fresh/uniswap_v3_bnb_lp/eoa/configured"
    path = tmp_path / "plan.json"
    path.write_text(
        json.dumps({"entries": [{"id": chosen, "kind": "case"}, {"id": "n", "kind": "node"}]})
    )
    calls = []
    monkeypatch.setattr(
        cell_qa_matrix,
        "freeze_inputs",
        lambda cases, env, directory: calls.append(([c.id for c in cases], env, directory)),
    )
    arguments = ["freeze", "--plan", str(path), "--env-file", "env", "--directory", str(tmp_path)]
    assert main(arguments) == 0
    assert calls == [([chosen], Path("env"), tmp_path)]


def test_repeated_explicit_nodes_are_planned_once():
    node = "tests/fork/test_a.py::test_b"
    assert cell_qa_matrix.validated_nodes(f"{node},{node}") == [node]


def test_bench_resolves_optional_input_paths(tmp_path, monkeypatch):
    from scripts import qa_bench

    monkeypatch.chdir(tmp_path)
    args = SimpleNamespace(output=Path("out"), env_file=None, frozen_inputs=Path("frozen.json"))
    qa_bench.resolve_paths(args)
    assert args.output == tmp_path / "out" and args.env_file is None
    assert args.frozen_inputs == tmp_path / "frozen.json"


def test_bench_plan_initializes_output_and_metadata_offline(tmp_path):
    from scripts import qa_bench

    args = SimpleNamespace(
        output=tmp_path / "plan",
        env_file=None,
        frozen_inputs=None,
        plan=True,
        case=[],
        lane=["backtest"],
    )
    cases, selected, metadata, rows = qa_bench.initialize(args)
    assert selected and all(case.lane == "backtest" for case in selected)
    assert metadata["execution"] == "plan" and metadata["input_time"] == metadata["started_at"]
    assert metadata["selected_cases"] == [case.id for case in selected]
    assert len(rows) == len(cases) and (args.output / "public").is_dir()
