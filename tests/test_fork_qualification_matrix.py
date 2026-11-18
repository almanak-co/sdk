"""The hosted fork qualification matrix names each reference and whether it rehearses."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from almanak.configuration.yaml import ConfigurationError
from scripts.ci import fork_qualification_matrix
from scripts.ci.check_strategies import CONVERTED
from scripts.ci.fork_qualification_matrix import (
    main,
    matrix,
    node_ids,
    production_cells,
    production_references,
    production_selection,
    reference_scope,
    summary,
    support_rows,
)
from tests.strategy_fork import rehearsal

ROOT = Path(__file__).parents[1]
LIFECYCLE = "tests/strategy_fork/test_strategies.py::test_strategy_lifecycle"
REHEARSAL = (
    "tests/strategy_fork/test_production_rehearsals.py"
    "::test_production_reference_path_on_identified_fork"
)


def test_matrix_marks_the_rehearsal_only_for_production_capable_references(monkeypatch):
    monkeypatch.setattr(rehearsal, "declares_production", lambda name: name.startswith("aave"))
    monkeypatch.setattr(
        "scripts.ci.fork_qualification_matrix.declares_production", rehearsal.declares_production
    )
    entries = matrix(["uniswap_v4_lp", "aave_v4_weth_supply", "aave_v4_weth_supply"], True, True)
    assert entries == [
        {
            "reference": "aave_v4_weth_supply",
            "rehearsal": True,
            "qualify": False,
            "nodes": [
                f"{LIFECYCLE}[configured-aave_v4_weth_supply]",
                f"{LIFECYCLE}[distinct-aave_v4_weth_supply]",
                f"{REHEARSAL}[aave_v4_weth_supply]",
            ],
        },
        {
            "reference": "uniswap_v4_lp",
            "rehearsal": False,
            "qualify": False,
            "nodes": [
                f"{LIFECYCLE}[configured-uniswap_v4_lp]",
                f"{LIFECYCLE}[distinct-uniswap_v4_lp]",
            ],
        },
    ]
    assert all(not entry["rehearsal"] for entry in matrix(["aave_v4_weth_supply"], False, True))


def test_matrix_marks_unrehearsed_references_for_the_qualification_override(monkeypatch):
    monkeypatch.setattr(
        "scripts.ci.fork_qualification_matrix.UNREHEARSED", frozenset({"uniswap_v4_lp"})
    )
    (entry,) = matrix(["uniswap_v4_lp"], False, True)
    assert entry["qualify"] is True and entry["rehearsal"] is False
    (entry,) = matrix(["aave_v4_weth_supply"], False, True)
    assert entry["qualify"] is False


def test_matrix_lists_no_nodes_when_nothing_applies(monkeypatch):
    monkeypatch.setattr("scripts.ci.fork_qualification_matrix.declares_production", lambda n: False)
    (entry,) = matrix(["uniswap_v4_lp"], True, False)
    assert entry["rehearsal"] is False and entry["nodes"] == []


def test_matrix_refuses_names_that_are_not_converted_references():
    with pytest.raises(SystemExit, match="not converted references"):
        matrix(["aave_v4_weth_supply", "../etc"], True, True)
    with pytest.raises(SystemExit, match="no references selected"):
        main([" , "])


def test_matrix_refuses_references_that_do_not_load_on_this_revision(monkeypatch):
    def refuse(path):
        raise ConfigurationError("connector does not support eip155:4663")

    monkeypatch.setattr("scripts.ci.fork_qualification_matrix.read_inputs", refuse)
    with pytest.raises(SystemExit, match="does not load on this revision: connector"):
        matrix(["uniswap_v4_lp"], True, True)


def test_main_prints_json_and_notes_lifecycle_only_references(capsys, monkeypatch):
    monkeypatch.setattr("scripts.ci.fork_qualification_matrix.declares_production", lambda n: False)
    main(["--lifecycle", "--rehearsal", "--", "uniswap_v4_lp"])
    captured = capsys.readouterr()
    assert json.loads(captured.out) == [
        {
            "reference": "uniswap_v4_lp",
            "rehearsal": False,
            "qualify": False,
            "nodes": [
                f"{LIFECYCLE}[configured-uniswap_v4_lp]",
                f"{LIFECYCLE}[distinct-uniswap_v4_lp]",
            ],
        }
    ]
    assert "::notice::uniswap_v4_lp declares no production EOA support" in captured.err
    assert "only the fork lifecycle runs for it" in captured.err


def test_main_notes_that_nothing_is_selected_without_the_lifecycle(capsys, monkeypatch):
    monkeypatch.setattr("scripts.ci.fork_qualification_matrix.declares_production", lambda n: False)
    main(["--rehearsal", "--", "uniswap_v4_lp"])
    captured = capsys.readouterr()
    assert json.loads(captured.out)[0]["nodes"] == []
    assert "the lifecycle is off, so no case is selected and its job fails" in captured.err
    assert "only the fork lifecycle runs" not in captured.err


def test_node_ids_collect_exactly_the_selected_cases():
    # A `-k "base_borrow_and_trade]"` expression also selected this namesake's cases.
    assert "ethereum_base_borrow_and_trade" in CONVERTED
    rehearsed = rehearsal.production_references()[0]
    nodes = node_ids("base_borrow_and_trade", True, False) + node_ids(rehearsed, False, True)
    environment = {key: value for key, value in os.environ.items() if key != rehearsal.QUALIFY_ENV}
    collected = subprocess.run(
        [sys.executable, "-m", "pytest", "--collect-only", "-o", "addopts=", "-q"]
        + ["-p", "no:cacheprovider", *nodes],
        cwd=ROOT,
        env=environment,
        capture_output=True,
        text=True,
        check=True,
    )
    assert [line for line in collected.stdout.splitlines() if "::" in line] == nodes


def test_the_workflow_runs_the_matrix_node_ids_and_nothing_else():
    workflow = yaml.load(
        (ROOT / ".github/workflows/fork_qualification.yml").read_text(), Loader=yaml.BaseLoader
    )
    (select,) = (s for s in workflow["jobs"]["select"]["steps"] if s.get("id") == "matrix")
    assert select["env"]["LIFECYCLE"] == "${{ inputs.lifecycle }}"
    assert "flags+=(--lifecycle)" in select["run"]
    (run,) = (
        s
        for s in workflow["jobs"]["qualify"]["steps"]
        if s.get("name") == "Run the fork qualification"
    )
    assert run["env"]["NODES"] == "${{ join(matrix.nodes, ' ') }}"
    assert 'pytest "${nodes[@]}"' in run["run"]
    # Substring selection such as `-k "$REFERENCE]"` also runs namesakes with longer names.
    assert " -k " not in run["run"]


ETHEREUM, BASE, ARBITRUM = "eip155:1", "eip155:8453", "eip155:42161"
AVALANCHE = "eip155:43114"


def test_production_cells_are_the_declared_production_rows():
    rows = {
        "aave_v3": [
            {"chain": ETHEREUM, "modes": ["fork", "production", "backtest"]},
            {"chain": BASE, "modes": ["fork"]},
        ],
        "curve": [{"chain": BASE, "modes": ["fork", "production"]}],
    }
    assert production_cells(rows) == {("aave_v3", ETHEREUM), ("curve", BASE)}
    assert production_cells({}) == set()
    # HyperCore is verified against a synthesized model, never Fork Qualification evidence.
    model = {"hyperliquid": [{"chain": "eip155:999", "modes": ["fork", "production"]}]}
    assert production_cells(model) == set()


def test_production_selection_names_every_reference_on_each_cell():
    cells = {("aave_v3", ETHEREUM), ("curve", BASE)}
    scopes = {
        "aave_lending": {("aave_v3", ETHEREUM)},
        "composed": {("aave_v3", ETHEREUM), ("curve", BASE), ("uniswap_v4", BASE)},
        "fork_only": {("uniswap_v4", BASE)},
        "candidate": set(),
    }
    assert production_selection(cells, scopes) == {
        ("aave_v3", ETHEREUM): ["aave_lending", "composed"],
        ("curve", BASE): ["composed"],
    }


def test_production_selection_includes_keeper_settled_references():
    cells = {("aave_v3", ARBITRUM), ("gmx_v2", ARBITRUM)}
    scopes = {
        "aave_lending": {("aave_v3", ARBITRUM)},
        "aave_and_gmx": {("aave_v3", ARBITRUM), ("gmx_v2", ARBITRUM)},
        "gmx_swap": {("gmx_v2", ARBITRUM)},
    }
    assert production_selection(cells, scopes) == {
        ("aave_v3", ARBITRUM): ["aave_and_gmx", "aave_lending"],
        ("gmx_v2", ARBITRUM): ["aave_and_gmx", "gmx_swap"],
    }


def test_production_selection_refuses_an_empty_or_partial_selection():
    with pytest.raises(SystemExit, match="no connector declares production support"):
        production_selection(set(), {"aave_lending": {("aave_v3", ETHEREUM)}})
    with pytest.raises(
        SystemExit, match=r"no converted reference exercises: \['curve on eip155:8453'\]"
    ):
        production_selection(
            {("aave_v3", ETHEREUM), ("curve", BASE)},
            {"aave_lending": {("aave_v3", ETHEREUM)}},
        )


def write_config(directory, connector):
    directory.mkdir()
    config = directory / "config.yaml"
    config.write_text(yaml.safe_dump({"markets": {"main": {"connector": connector}}}))
    return config


def test_reference_scope_is_empty_only_for_an_unsupported_chain_candidate(tmp_path, monkeypatch):
    rows = {"compound_v3": [{"chain": ETHEREUM, "modes": ["fork", "production"]}]}

    def refuse(message):
        def read(path):
            raise ConfigurationError(message)

        return read

    candidate = write_config(tmp_path / "compound_polygon", "compound_v3")
    monkeypatch.setattr(
        fork_qualification_matrix,
        "read_inputs",
        refuse("connector does not support eip155:137"),
    )
    assert reference_scope(candidate, rows) == set()
    # A refusal for a chain the connector does declare is a failure, not a candidate.
    monkeypatch.setattr(
        fork_qualification_matrix, "read_inputs", refuse("connector does not support eip155:1")
    )
    with pytest.raises(SystemExit, match="compound_polygon does not load on this revision"):
        reference_scope(candidate, rows)
    monkeypatch.setattr(fork_qualification_matrix, "read_inputs", refuse("bad market field"))
    with pytest.raises(SystemExit, match="does not load on this revision: bad market field"):
        reference_scope(candidate, rows)


def test_reference_scope_lists_each_market_cell():
    scope = reference_scope(ROOT / "strategies/ethereum_base_borrow_and_trade/config.yaml", {})
    assert ("aave_v3", ETHEREUM) in scope and ("uniswap_v3", BASE) in scope


def test_the_revision_selection_covers_every_declared_production_cell():
    rows = support_rows()
    cells = production_cells(rows)
    covering = production_references()
    assert cells and set(covering) == cells
    selected = {name for names in covering.values() for name in names}
    assert selected <= CONVERTED
    assert "aave_v4_weth_supply" in selected
    # GMX on Arbitrum qualifies on hosted runners from the pinned signed-report copies.
    assert "gmx_v2_arbitrum_position" in covering[("gmx_v2", ARBITRUM)]
    # A composed reference joins through any of its production cells: GMX on Avalanche
    # is fork-only, so this one runs its lifecycle under the Aave V3 Avalanche cell.
    assert "avalanche_aave_borrow_and_trade_gmx" in covering[("aave_v3", AVALANCHE)]
    assert ("gmx_v2", AVALANCHE) not in covering
    # Declared for production, but its own fork rehearsal stands in (blueprint 30, D7).
    assert ("hyperliquid", "eip155:999") not in covering


def test_summary_lists_each_cell_with_its_references():
    text = summary(
        {
            ("aave_v3", ETHEREUM): ["aave_lending", "composed"],
            ("gmx_v2", ARBITRUM): ["gmx_swap"],
        }
    )
    assert "2 connector/chain cells declare production support" in text
    assert "3 references exercise them" in text
    assert "| `aave_v3` | `eip155:1` | `aave_lending`, `composed` |" in text
    assert "| `gmx_v2` | `eip155:42161` | `gmx_swap` |" in text


def test_main_selects_the_production_cells_and_writes_the_summary(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr(
        fork_qualification_matrix,
        "production_references",
        lambda: {("uniswap_v4", ETHEREUM): ["uniswap_v4_lp", "uniswap_v4_trading"]},
    )
    monkeypatch.setattr(fork_qualification_matrix, "declares_production", lambda n: True)
    output = tmp_path / "summary.md"
    main(["--lifecycle", "--rehearsal", "--production-cells", "--summary", str(output)])
    captured = capsys.readouterr()
    entries = json.loads(captured.out)
    assert [entry["reference"] for entry in entries] == ["uniswap_v4_lp", "uniswap_v4_trading"]
    assert all(entry["rehearsal"] and len(entry["nodes"]) == 3 for entry in entries)
    assert "not qualified here" not in captured.err
    assert "| `uniswap_v4` | `eip155:1` |" in output.read_text()
    with pytest.raises(SystemExit, match="not both"):
        main(["--production-cells", "--", "uniswap_v4_lp"])
    with pytest.raises(SystemExit, match="no references selected"):
        main(["--lifecycle"])


def test_the_weekly_schedule_derives_its_selection_and_reports_failures():
    workflow = yaml.load(
        (ROOT / ".github/workflows/fork_qualification.yml").read_text(), Loader=yaml.BaseLoader
    )
    triggers = workflow["on"]
    (schedule,) = triggers["schedule"]
    minute, hour, day, month, weekday = schedule["cron"].split()
    assert (day, month) == ("*", "*") and weekday.isdigit() and minute.isdigit()
    # Manual dispatch keeps its named references and its flags.
    inputs = triggers["workflow_dispatch"]["inputs"]
    assert set(inputs) == {"references", "lifecycle", "rehearsal"}
    assert inputs["references"]["required"] == "true"
    assert workflow["permissions"] == {"contents": "read"}
    (select,) = (s for s in workflow["jobs"]["select"]["steps"] if s.get("id") == "matrix")
    assert select["env"]["EVENT"] == "${{ github.event_name }}"
    assert '[ "$EVENT" = "schedule" ]' in select["run"]
    assert "--lifecycle --rehearsal --production-cells" in select["run"]
    assert '-- "$REFERENCES"' in select["run"]
    qualify = workflow["jobs"]["qualify"]
    assert qualify["timeout-minutes"] == "150"
    assert qualify["strategy"]["fail-fast"] == "false"
    assert qualify["strategy"]["max-parallel"].isdigit()
    # GMX keeper drivers read the committed evidence copies, verified before any case runs.
    assert qualify["permissions"] == {"contents": "read"}
    runs = [step.get("run", "") for step in qualify["steps"]]
    fetch = runs.index("make fixtures")
    assert fetch < next(i for i, run in enumerate(runs) if "uv run --locked pytest" in run)
    report = workflow["jobs"]["report"]
    assert report["needs"] == ["select", "qualify"]
    assert report["if"] == "always() && github.event_name == 'schedule'"
    assert report["permissions"] == {"actions": "read", "contents": "read", "issues": "write"}
    (step,) = report["steps"]
    assert step["env"]["GH_TOKEN"] == "${{ github.token }}"
    assert "secrets." not in str(report)
    for command in ("gh issue create", "gh issue comment", "gh issue close", "exit 1"):
        assert command in step["run"]
