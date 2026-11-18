"""Direct author-intent qualification is separate from earlier nested-operation captures."""

import json

import pytest

from almanak.backtest.engine import Journal
from almanak.connectors.composition import (
    checkpoint_from_json,
    connector_for,
    connector_from_wire,
)
from tests.qa.catalog import ROOT, catalog
from tests.qa.intent_coverage import assert_economic_direct, assert_native_direct
from tests.qa.lp_refactor import assert_reference_source
from tests.qa.retained import current_json, current_value
from tests.qa.test_uniswap_v3 import artifact
from tests.qa.worker import assert_backtest, assert_fork_actions

BASE = ROOT / "docs/internal/reports/evidence/2026-09-08-rollout"
ANCHORS = BASE / "direct-lp-anchors"
ADDITIONAL = BASE / "direct-uniswap-remaining-prepared"
CURVE = BASE / "direct-curve-remaining-prepared"
NATIVE = json.loads((ANCHORS / "manifest.json").read_text())["cases"]
PREPARED = (
    *(
        (CURVE, row["case"], "curve_" + row["case"].replace("-", "_", 1))
        for row in json.loads((CURVE / "manifest.json").read_text())["cases"]
    ),
    (ANCHORS, "prepared/curve-optimism/optimism-stable_lp", "curve_optimism_stable_lp"),
    (ANCHORS, "prepared/uniswap-optimism/optimism-lp", "uniswap_v3_optimism_lp"),
    *(
        (ADDITIONAL, chain + "-lp", "uniswap_v3_" + chain + "_lp")
        for chain in ("ethereum", "base", "arbitrum")
    ),
)


@pytest.mark.parametrize("row", NATIVE, ids=lambda row: row["case"])
def test_captured_native_lp_references_complete_direct_close_and_exit(tmp_path, row):
    case = next(c for c in catalog() if c.id == row["case"])
    prefix = "native/" + row["directory"]
    files = json.loads((ANCHORS / "manifest.json").read_text())["files"]
    paths = [
        r["path"]
        for r in files
        if r["path"].startswith(prefix + "/") and r["path"].endswith("/result.json.gz")
    ]
    assert len(paths) == 1
    record = json.loads(artifact(ANCHORS, paths[0]))
    state = checkpoint_from_json(
        current_json(artifact(ANCHORS, paths[0].replace("/result.json.gz", "/checkpoint.json.gz")))
    )
    assert row["returncode"] == 0 and record["status"] == "passed"
    assert_reference_source(state.strategy, case.reference)
    assert state.mode == "closed" and state.delivered == len(state.outcomes)
    assert {b.profile.kind for b in state.config.accounts.bindings} == {case.profile}
    connector_for(state.config).lifecycle.validate_closure(state, state.closure)
    config = tmp_path / "config.yaml"
    config.write_bytes(artifact(ANCHORS, prefix + "/config.yaml.gz"))
    assert_fork_actions(case, state.model_dump(mode="json"), config)
    assert_native_direct(case, state)


@pytest.mark.parametrize("package,prefix,reference", PREPARED, ids=lambda value: str(value))
def test_captured_prepared_lp_direct_intents_and_both_real_recoveries(
    tmp_path, package, prefix, reference
):
    case = next(c for c in catalog() if c.id == f"backtest/pinned/{reference}/economic")
    for name in ("manifest.json", "report.json", "state.sqlite"):
        (tmp_path / name).write_bytes(artifact(package, prefix + "/run/" + name + ".gz"))
    assert_backtest(case, tmp_path)
    raw = json.loads((tmp_path / "manifest.json").read_text())
    binding = connector_from_wire(raw["config"]).backtest(raw["settings"]["market_model"])
    manifest = binding.manifest_type.model_validate_json(json.dumps(current_value(raw)))
    assert_reference_source(manifest.strategy, reference)
    expected = Journal(tmp_path / "state.sqlite", type(manifest.initial)).load()
    assert_economic_direct(case, expected, manifest.config)
    result = isolated_result(package, prefix)
    assert result["closed"] and result["reproduced"] and result["retained_resume_equal"]
    assert result["provider_requests"] == 0 and result["network_probe_errno"] == 101
    assert {r["stage"] for r in result["recovery"]} == {"pending_commit", "effects_commit"}
    for recovery in result["recovery"]:
        assert recovery["killed_returncode"] == -9 and recovery["exact_state_equal"]
        path = tmp_path / (recovery["stage"] + ".sqlite")
        path.write_bytes(artifact(package, prefix + "/" + path.name + ".gz"))
        assert Journal(path, type(manifest.initial)).load() == expected


def isolated_result(package, prefix):
    if package == ANCHORS:
        name = prefix.rsplit("/", 1)[0] + "/isolated-results.json.gz"
        rows = json.loads(artifact(package, name))
    else:
        rows = json.loads((package / "manifest.json").read_text())["cases"]
    return next(row for row in rows if row["case"] == prefix.rsplit("/", 1)[-1])
