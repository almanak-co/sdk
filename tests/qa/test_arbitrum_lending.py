"""Arbitrum declarations require retained native, economic and process evidence."""

import gzip
import hashlib
import json
from dataclasses import replace

import pytest

from almanak.backtest.engine import Journal
from almanak.connectors.composition import (
    checkpoint_from_json,
    connector_for,
    connector_from_wire,
)
from almanak.connectors.support import EconomicAction
from tests.lending_captures import current_json, current_manifest, current_record
from tests.qa.catalog import ROOT, catalog, validate
from tests.qa.lending_refactor import archived_reference
from tests.qa.worker import assert_backtest, assert_fork_actions

PACKAGE = ROOT / "docs/internal/reports/evidence/2026-09-08-lending-rollout/arbitrum-lending"
FILES = json.loads((PACKAGE / "manifest.json").read_text())["files"]
REFERENCES = (
    "compound_v3_arbitrum_lending",
    "morpho_blue_arbitrum_lending",
    "arbitrum_compound_borrow_and_trade",
    "arbitrum_morpho_borrow_and_trade",
)
CASES = tuple(c for c in catalog() if c.reference in REFERENCES)
NATIVE = tuple(c for c in CASES if c.lane == "fork" and (c.connector or c.input_policy == "pinned"))
PREPARED = tuple(
    c for c in CASES if c.lane == "backtest" and (c.connector or c.input_policy == "pinned")
)
RECOVERY = tuple(
    n for n in FILES if n.startswith("promotion-recovery/") and n.endswith("/result.json.gz")
)


def artifact(name):
    raw = (PACKAGE / name).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == FILES[name]
    return gzip.decompress(raw) if name.endswith(".gz") else raw


def case_prefix(case):
    prefix = "cases/" + case.id.replace("/", "-")
    roots = {"/".join(n.split("/")[:2]) for n in FILES if n.startswith(prefix)}
    assert len(roots) == 1
    return roots.pop() + "/"


def native_prefix(case):
    if case.connector:
        return case_prefix(case)
    return f"composition/{case.reference}-{case.profile}-{case.operator_mode}-"


def single_file(prefix, suffix):
    names = [n for n in FILES if n.startswith(prefix) and n.endswith(suffix)]
    assert len(names) == 1
    return names[0]


def author_config(case, directory):
    raw = (
        artifact(case_prefix(case) + "config.yaml.gz")
        if case.connector
        else (ROOT / "strategies" / case.reference / "config.yaml").read_bytes()
    )
    path = directory / "config.yaml"
    path.write_bytes(raw)
    return path


@pytest.mark.parametrize("case", NATIVE, ids=lambda case: case.id)
def test_native_arbitrum_outcomes_cover_scoped_actions(tmp_path, case):
    name = single_file(native_prefix(case), "/checkpoint.json.gz")
    state = checkpoint_from_json(current_json(artifact(name).decode()))
    result = json.loads(artifact(name.replace("/checkpoint.json.gz", "/result.json.gz")))
    assert result["status"] == "passed"
    assert state.strategy == archived_reference(case.reference)
    assert {b.profile.kind for b in state.config.accounts.bindings} == {case.profile}
    assert (state.mode, state.pending, state.plan, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes)
    assert all(outcome.success and not outcome.accounting_issues for outcome in state.outcomes)
    connector_for(state.config).lifecycle.validate_closure(state, state.closure)
    config = author_config(case, tmp_path)
    assert_fork_actions(case, state.model_dump(mode="json"), config)


def prepared_prefix(case):
    if case.connector:
        return case_prefix(case) + "run/"
    kind = "compound" if "compound" in case.reference else "morpho"
    return f"cases/composed-prepared-{kind}/run/"


@pytest.mark.parametrize("case", PREPARED, ids=lambda case: case.id)
def test_prepared_arbitrum_actions_and_whole_reproduction(tmp_path, case):
    prefix = prepared_prefix(case)
    for name in ("manifest.json", "report.json", "state.sqlite"):
        (tmp_path / name).write_bytes(artifact(prefix + name + ".gz"))
    current_manifest(tmp_path)
    assert_backtest(case, tmp_path)
    assert json.loads(artifact(prefix + "reproduction.json.gz"))["reproduced"]
    raw = json.loads(artifact(prefix + "manifest.json.gz"))
    binding = connector_from_wire(raw["config"]).backtest(raw["settings"]["market_model"])
    manifest = binding.manifest_type.model_validate_json(json.dumps(current_record(raw)))
    assert manifest.strategy == archived_reference(case.reference)
    expected = Journal(tmp_path / "state.sqlite", type(manifest.initial)).load()
    reproduced = single_file(prefix + "reproduce-", ".sqlite.gz")
    replay = tmp_path / "reproduced.sqlite"
    replay.write_bytes(artifact(reproduced))
    assert Journal(replay, type(manifest.initial)).load() == expected
    if case.connector:
        with pytest.raises(AssertionError, match="missing economic actions"):
            assert_backtest(replace(case, actions=(EconomicAction.REFRESH,)), tmp_path)


@pytest.mark.parametrize("name", RECOVERY)
def test_actual_process_interruptions_preserve_one_borrow(name):
    record = json.loads(artifact(name))
    assert record["status"] == "passed"
    before = checkpoint_from_json(json.dumps(current_record(record["reserved"])))
    unknown = checkpoint_from_json(json.dumps(current_record(record["lost_response"])))
    committed = checkpoint_from_json(json.dumps(current_record(record["committed"])))
    final = checkpoint_from_json(
        current_json(artifact(name.replace("/result.json.gz", "/checkpoint.json.gz")).decode())
    )
    assert before.pending is None and before.plan.operation.kind == "borrow"
    assert unknown.pending.attempt_id == before.plan.attempt_id
    assert record["broadcast_transaction"]["hash"] == unknown.pending.tx_hash
    assert committed.pending is None and committed.delivered < len(committed.outcomes)
    assert committed.outcomes[-1].tx_hash == unknown.pending.tx_hash
    assert (final.mode, final.pending, final.plan, final.decision) == ("closed", None, None, None)
    assert final.delivered == len(final.outcomes)
    assert sum(outcome.tx_hash == unknown.pending.tx_hash for outcome in final.outcomes) == 1
    connector_for(final.config).lifecycle.validate_closure(final, final.closure)


def test_all_thirty_required_lanes_remain_mapped():
    assert (len(NATIVE), len(PREPARED), len(RECOVERY)) == (18, 8, 4)
    cases = catalog()
    validate(cases)
    for reference in REFERENCES[:2]:
        with pytest.raises(ValueError, match="QA coverage gaps"):
            validate([case for case in cases if case.reference != reference])
