"""Contracts for the Demo Tests lane.

The lane's whole claim is "this ran exactly as shipped". These tests exist to
keep that claim from decaying into "this ran", which is the failure the retired
nightly Anvil lane shipped for months: it rewrote every demo's ``config.json``
before running it, so its green never described the artifact a user receives.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture(scope="module")
def demo_module() -> ModuleType:
    path = REPO_ROOT / "qa_lab" / "qa_demo.py"
    spec = importlib.util.spec_from_file_location("qa_demo_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _demo_tree(root: Path, name: str, *, config: dict | None = None) -> Path:
    directory = root / name
    directory.mkdir(parents=True)
    (directory / "strategy.py").write_text("# demo\n", encoding="utf-8")
    (directory / "config.json").write_text(json.dumps(config or {"chain": "arbitrum"}), encoding="utf-8")
    return directory


# ---------------------------------------------------------------------------
# Source digest — the binding the whole lane rests on
# ---------------------------------------------------------------------------


def test_digest_changes_when_config_changes(demo_module: ModuleType, tmp_path: Path) -> None:
    """A config rewrite must be detectable — that is the retired lane's bug."""
    directory = _demo_tree(tmp_path, "demo_a")
    before = demo_module.demo_source_digest(directory)
    (directory / "config.json").write_text(json.dumps({"chain": "arbitrum", "force_action": "open"}), encoding="utf-8")
    assert demo_module.demo_source_digest(directory) != before


def test_digest_changes_when_strategy_changes(demo_module: ModuleType, tmp_path: Path) -> None:
    directory = _demo_tree(tmp_path, "demo_b")
    before = demo_module.demo_source_digest(directory)
    (directory / "strategy.py").write_text("# demo v2\n", encoding="utf-8")
    assert demo_module.demo_source_digest(directory) != before


def test_digest_distinguishes_absent_from_empty(demo_module: ModuleType, tmp_path: Path) -> None:
    """A deleted config must not hash the same as an empty one.

    Skipping a missing member would let an "as shipped" claim survive the
    deletion of the very file that defines the demo's runtime.
    """
    absent = tmp_path / "absent"
    absent.mkdir()
    (absent / "strategy.py").write_text("# demo\n", encoding="utf-8")
    empty = tmp_path / "empty"
    empty.mkdir()
    (empty / "strategy.py").write_text("# demo\n", encoding="utf-8")
    (empty / "config.json").write_text("", encoding="utf-8")
    assert demo_module.demo_source_digest(absent) != demo_module.demo_source_digest(empty)


def test_digest_ignores_unrelated_files(demo_module: ModuleType, tmp_path: Path) -> None:
    """A README edit must not invalidate an otherwise valid seal."""
    directory = _demo_tree(tmp_path, "demo_c")
    before = demo_module.demo_source_digest(directory)
    (directory / "README.md").write_text("docs\n", encoding="utf-8")
    assert demo_module.demo_source_digest(directory) == before


# ---------------------------------------------------------------------------
# Admission — a mutated run can never be graded green
# ---------------------------------------------------------------------------


def _cell(**overrides: object) -> dict:
    cell = {
        "cell_id": "demo.uniswap_lp.anvil.eoa",
        "demo": "uniswap_lp",
        "network": "anvil",
        "exec_path": "eoa",
        "chain": "arbitrum",
        "source_digest": "a" * 64,
        "is_canary": False,
    }
    cell.update(overrides)
    return cell


def _result(**overrides: object) -> dict:
    result = {
        "demo": "uniswap_lp",
        "network": "anvil",
        "exec_path": "eoa",
        "verdict": "PASS",
        "source_digest": "a" * 64,
    }
    result.update(overrides)
    return result


def test_matching_digest_and_pass_is_graded_pass(demo_module: ModuleType) -> None:
    admitted = demo_module._admit_demo_claim(_result(), _cell())
    assert admitted["status"] == "PASS"
    assert admitted["as_is"] is True


def test_declared_pass_on_mutated_source_is_void(demo_module: ModuleType) -> None:
    """The load-bearing test: a producer saying PASS cannot override the digest."""
    admitted = demo_module._admit_demo_claim(_result(source_digest="b" * 64), _cell())
    assert admitted["status"] == "VOID"
    assert admitted["as_is"] is False
    assert admitted["declared_verdict"] == "PASS"


def test_missing_digest_is_void_not_pass(demo_module: ModuleType) -> None:
    """An absent digest is unmeasured, and unmeasured is never green."""
    admitted = demo_module._admit_demo_claim(_result(source_digest=""), _cell())
    assert admitted["status"] == "VOID"


@pytest.mark.parametrize("axis,value", [("demo", "other_demo"), ("network", "mainnet"), ("exec_path", "safe")])
def test_axis_mismatch_is_void(demo_module: ModuleType, axis: str, value: str) -> None:
    """A bundle filed into the wrong cell is a mis-file, never evidence."""
    admitted = demo_module._admit_demo_claim(_result(**{axis: value}), _cell())
    assert admitted["status"] == "VOID"
    assert axis in admitted["reason"]


@pytest.mark.parametrize("verdict", ["", "SKIP", "UNKNOWN", "XFAIL"])
def test_unofficial_verdict_is_unverified_not_pass_or_fail(demo_module: ModuleType, verdict: str) -> None:
    admitted = demo_module._admit_demo_claim(_result(verdict=verdict), _cell())
    assert admitted["status"] == "UNVERIFIED"


def test_declared_fail_is_graded_fail(demo_module: ModuleType) -> None:
    admitted = demo_module._admit_demo_claim(_result(verdict="FAIL"), _cell())
    assert admitted["status"] == "FAIL"


def test_canary_is_exempt_from_the_digest_binding(demo_module: ModuleType) -> None:
    """The control row has no shipped source, so it must still be gradeable."""
    cell = _cell(
        cell_id="demo.harness_control.canary.anvil.eoa", demo="harness_control", source_digest="canary", is_canary=True
    )
    result = _result(demo="harness_control", verdict="FAIL", source_digest="whatever")
    assert demo_module._admit_demo_claim(result, cell)["status"] == "FAIL"


# ---------------------------------------------------------------------------
# Catalog
# ---------------------------------------------------------------------------


def test_catalog_enumerates_every_axis_and_excuses_nothing(demo_module: ModuleType) -> None:
    """19 demos x 2 networks x 2 exec paths, with no cell dropped at build time."""
    catalog = demo_module.build_demo_catalog()
    product = [cell for cell in catalog["cells"] if not cell["is_canary"]]
    demos = {cell["demo"] for cell in product}
    assert len(product) == len(demos) * len(demo_module.DEMO_NETWORKS) * len(demo_module.DEMO_EXEC_PATHS)
    for demo in demos:
        axes = {(cell["network"], cell["exec_path"]) for cell in product if cell["demo"] == demo}
        assert axes == {
            (network, exec_path) for network in demo_module.DEMO_NETWORKS for exec_path in demo_module.DEMO_EXEC_PATHS
        }


def test_catalog_carries_a_canary(demo_module: ModuleType) -> None:
    catalog = demo_module.build_demo_catalog()
    canaries = [cell for cell in catalog["cells"] if cell["is_canary"]]
    assert len(canaries) == 1
    assert canaries[0]["pipeline_status"] == "ready"


def test_catalog_marks_unwired_axes_as_gaps_not_results(demo_module: ModuleType) -> None:
    """An axis with no runner must be distinguishable from one that passed."""
    catalog = demo_module.build_demo_catalog()
    for cell in catalog["cells"]:
        wired = (cell["network"], cell["exec_path"]) in demo_module.DEMO_WIRED_AXES
        assert cell["pipeline_status"] == ("ready" if wired else "gap")


def test_catalog_is_derived_not_hand_listed(demo_module: ModuleType) -> None:
    """The inventory must track the production registry, not a local copy."""
    from almanak.framework.demos import DemoCatalog

    catalog = demo_module.build_demo_catalog()
    assert catalog["source"] == "almanak.framework.demos.DemoCatalog.discover()"
    assert {row["name"] for row in catalog["demos"]} == {spec.name for spec in DemoCatalog.discover().specs}


def test_catalog_digests_match_the_files_on_disk(demo_module: ModuleType) -> None:
    catalog = demo_module.build_demo_catalog()
    for row in catalog["demos"]:
        directory = REPO_ROOT / str(row["directory"])
        assert row["source_digest"] == demo_module.demo_source_digest(directory)


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def test_unwired_cell_plan_is_unroutable_with_a_reason(demo_module: ModuleType) -> None:
    catalog = demo_module.build_demo_catalog()
    unwired = next(cell for cell in catalog["cells"] if cell["pipeline_status"] == "gap")
    plan = demo_module.demo_cell_plan(cell_id=unwired["cell_id"], catalog=catalog)
    assert plan["routable"] is False
    assert plan["command"] == ""
    assert plan["reason"]


def test_wired_cell_plan_names_a_real_command(demo_module: ModuleType) -> None:
    catalog = demo_module.build_demo_catalog()
    wired = next(cell for cell in catalog["cells"] if cell["pipeline_status"] == "ready" and not cell["is_canary"])
    plan = demo_module.demo_cell_plan(cell_id=wired["cell_id"], catalog=catalog)
    assert plan["routable"] is True
    assert plan["command"].startswith("uv run python qa_lab/qa_demo.py run ")
    assert f"--demo {wired['demo']}" in plan["command"]


def test_runner_refuses_an_unwired_axis(demo_module: ModuleType, tmp_path: Path) -> None:
    """Never silently substitute a different axis for the one requested."""
    catalog = demo_module.build_demo_catalog()
    demo = catalog["demos"][0]["name"]
    with pytest.raises(ValueError, match="No runner is wired"):
        demo_module.run_demo_cell(demo=demo, network="mainnet", exec_path="safe", output=tmp_path, catalog=catalog)


# ---------------------------------------------------------------------------
# Bundle validation
# ---------------------------------------------------------------------------


def test_bundle_without_transcript_is_rejected(demo_module: ModuleType, tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "result.json").write_text(json.dumps(_result()), encoding="utf-8")
    with pytest.raises(ValueError, match="missing required member: run.log"):
        demo_module._validate_bundle(bundle)


def test_bundle_carrying_a_secret_name_is_rejected(demo_module: ModuleType, tmp_path: Path) -> None:
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    (bundle / "result.json").write_text(json.dumps(_result()), encoding="utf-8")
    (bundle / "run.log").write_text("log\n", encoding="utf-8")
    (bundle / "wallets.env").write_text("KEY=1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="secret-bearing"):
        demo_module._validate_bundle(bundle)


# ---------------------------------------------------------------------------
# Sealing — the only store-mutating path in the lane
# ---------------------------------------------------------------------------

_PROVENANCE = {
    "commit": "a" * 40,
    "dirty": False,
    "sdk_version": "0.0.0-test",
    "source": "sealed-source-run",
}


def _bundle_dir(root: Path, result: dict) -> Path:
    bundle = root / "bundle"
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "run.log").write_text("demo ran\n", encoding="utf-8")
    (bundle / "result.json").write_text(json.dumps(result), encoding="utf-8")
    return bundle


def test_seal_writes_the_index_row_and_refuses_a_second_seal(demo_module: ModuleType, tmp_path: Path) -> None:
    store = tmp_path / "store"
    (store / "index").mkdir(parents=True)
    catalog = {"cells": [_cell()]}
    bundle = _bundle_dir(tmp_path, _result())

    target = demo_module.seal_demo_bundle(
        bundle=bundle,
        store=store,
        catalog=catalog,
        cell_id="demo.uniswap_lp.anvil.eoa",
        sdk_provenance=dict(_PROVENANCE),
        run_id="run-one",
    )

    assert (target / "manifest.json").is_file()
    latest = json.loads((store / "index" / "demo_latest.json").read_text())
    row = latest["demo.uniswap_lp.anvil.eoa"]
    assert row["status"] == "PASS"
    assert row["run_id"] == "run-one"
    runs = (store / "index" / "demo_runs.jsonl").read_text().strip().splitlines()
    assert len(runs) == 1

    with pytest.raises(FileExistsError, match="already sealed"):
        demo_module.seal_demo_bundle(
            bundle=bundle,
            store=store,
            catalog=catalog,
            cell_id="demo.uniswap_lp.anvil.eoa",
            sdk_provenance=dict(_PROVENANCE),
            run_id="run-one",
        )


def test_seal_grades_a_mutated_demo_void_not_pass(demo_module: ModuleType, tmp_path: Path) -> None:
    """A run whose on-disk source differs from the catalog digest is VOID even if it declared PASS."""
    store = tmp_path / "store"
    (store / "index").mkdir(parents=True)
    catalog = {"cells": [_cell()]}
    bundle = _bundle_dir(tmp_path, _result(source_digest="b" * 64))

    demo_module.seal_demo_bundle(
        bundle=bundle,
        store=store,
        catalog=catalog,
        cell_id="demo.uniswap_lp.anvil.eoa",
        sdk_provenance=dict(_PROVENANCE),
        run_id="run-void",
    )

    latest = json.loads((store / "index" / "demo_latest.json").read_text())
    assert latest["demo.uniswap_lp.anvil.eoa"]["status"] == "VOID"
