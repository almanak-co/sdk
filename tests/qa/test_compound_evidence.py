"""Compound catalog claims are checked against the sealed native and prepared runs."""

import gzip
import hashlib
import json
from dataclasses import replace

import pytest

from almanak.connectors.composition import checkpoint_from_json, connector_for
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.support import EconomicAction
from almanak.strategy import StrategyDefinition
from tests.legacy_references import current_imports_if_legacy
from tests.lending_captures import current_manifest
from tests.qa.catalog import ROOT, catalog, validate
from tests.qa.worker import assert_backtest, assert_fork_actions
from tests.scope_only_sources import reviewed_json

PACKAGE = ROOT / "docs/internal/reports/evidence/2026-09-08-lending-rollout/compound-anchors"
CASES = tuple(
    c
    for c in catalog()
    if c.connector == ConnectorId("compound_v3")
    and c.reference in ("compound_v3_ethereum_lending", "compound_v3_base_lending")
)


def artifact(name):
    manifest = json.loads((PACKAGE / "manifest.json").read_text())
    raw = (PACKAGE / name).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == manifest["files"][name]
    return gzip.decompress(raw) if name.endswith(".gz") else raw


@pytest.mark.parametrize("case", [c for c in CASES if c.lane == "fork"], ids=lambda c: c.id)
def test_native_catalog_uses_completed_direct_intents_and_closed_accounts(tmp_path, case):
    prefix = "cases/" + case.id.replace("/", "-") + "/"
    files = json.loads((PACKAGE / "manifest.json").read_text())["files"]
    names = [n for n in files if n.startswith(prefix) and n.endswith("/checkpoint.json.gz")]
    assert len(names) == 1
    state = checkpoint_from_json(reviewed_json(artifact(names[0]).decode()))
    assert json.loads(artifact(prefix + "result.json.gz"))["status"] == "passed"
    assert current_imports_if_legacy(state.strategy) == StrategyDefinition.read(
        ROOT / "strategies" / case.reference / "strategy.py"
    )
    assert {b.profile.kind for b in state.config.accounts.bindings} == {case.profile}
    assert (state.mode, state.pending, state.plan, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes) == 14
    assert all(o.success and not o.accounting_issues for o in state.outcomes)
    completed = [o for o in state.outcomes if o.strategy_result().status == "completed"]
    assert len(completed) == len({o.decision.identity for o in completed}) == 9
    assert {o.decision.intent.kind for o in completed} == {
        "supply",
        "withdraw",
        "withdraw_amount",
        "borrow",
        "repay",
        "repay_amount",
    }
    assert all(
        o.decision.intent.target == state.config.target(o.decision.intent.target.allocation)
        for o in completed
    )
    connector_for(state.config).lifecycle.validate_closure(state, state.closure)
    config = tmp_path / "config.yaml"
    config.write_bytes(artifact(prefix + "config.yaml.gz"))
    assert_fork_actions(case, state.model_dump(mode="json"), config)


@pytest.mark.parametrize("case", [c for c in CASES if c.lane == "backtest"], ids=lambda c: c.id)
def test_prepared_catalog_checks_retained_state_and_rejects_missing_actions(tmp_path, case):
    prefix = "cases/" + case.id.replace("/", "-") + "/"
    assert json.loads(artifact(prefix + "result.json.gz"))["status"] == "passed"
    for name in ("manifest.json", "report.json", "state.sqlite"):
        (tmp_path / name).write_bytes(artifact(prefix + "run/" + name + ".gz"))
    current_manifest(tmp_path)
    assert_backtest(case, tmp_path)
    assert json.loads(artifact(prefix + "run/reproduction.json.gz"))["reproduced"]
    with pytest.raises(AssertionError, match="missing economic actions"):
        assert_backtest(replace(case, actions=(EconomicAction.REFRESH,)), tmp_path)


def test_catalog_cannot_drop_either_compound_chain():
    cases = catalog()
    validate(cases)
    assert len(CASES) == 18
    for reference in ("compound_v3_ethereum_lending", "compound_v3_base_lending"):
        with pytest.raises(ValueError, match="QA coverage gaps"):
            validate([c for c in cases if c.reference != reference])
