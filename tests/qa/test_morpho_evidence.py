"""Morpho claims require sealed direct intent evidence for every connector scope."""

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
from almanak.strategy import StrategyDefinition
from tests.legacy_references import historical_strategy_current_imports, with_reviewed_hold_guard
from tests.lending_captures import current_manifest
from tests.qa.catalog import ROOT, catalog
from tests.qa.intent_coverage import assert_economic_direct, assert_native_direct
from tests.qa.worker import assert_backtest, assert_fork_actions
from tests.scope_only_sources import reviewed_json, reviewed_record

PACKAGE = ROOT / "docs/internal/reports/evidence/2026-09-08-lending-rollout/morpho-anchors"
FILES = json.loads((PACKAGE / "manifest.json").read_text())["files"]
CASES = tuple(
    c
    for c in catalog()
    if c.reference
    in (
        "morpho_blue_ethereum_lending",
        "morpho_blue_base_lending",
        "base_morpho_borrow_and_trade",
        "ethereum_morpho_curve_borrow_and_trade",
    )
)
NATIVE = tuple(c for c in CASES if c.lane == "fork" and (c.connector or c.input_policy == "pinned"))
PREPARED = tuple(
    c for c in CASES if c.lane == "backtest" and (c.connector or c.input_policy == "pinned")
)


def artifact(name):
    raw = (PACKAGE / name).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == FILES[name]
    return gzip.decompress(raw) if name.endswith(".gz") else raw


def native_prefix(case):
    if case.connector:
        return "cases/" + case.id.replace("/", "-") + "/"
    return f"morpho-composition/{case.reference}-{case.profile}-{case.operator_mode}-"


@pytest.mark.parametrize("case", NATIVE, ids=lambda c: c.id)
def test_native_morpho_retains_closed_direct_scoped_actions(tmp_path, case):
    prefix = native_prefix(case)
    names = [n for n in FILES if n.startswith(prefix) and n.endswith("/checkpoint.json.gz")]
    assert len(names) == 1
    state = checkpoint_from_json(reviewed_json(artifact(names[0]).decode()))
    result = json.loads(artifact(names[0].replace("/checkpoint.json.gz", "/result.json.gz")))
    assert result["status"] == "passed"
    # The sealed artifact predates the package rename. This comparison permits only
    # the exact import rewrite; current-source lifecycle qualification remains separate.
    assert with_reviewed_hold_guard(
        historical_strategy_current_imports(state.strategy)
    ) == StrategyDefinition.read(ROOT / "strategies" / case.reference / "strategy.py")
    assert {b.profile.kind for b in state.config.accounts.bindings} == {case.profile}
    assert (state.mode, state.pending, state.plan, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes)
    assert all(o.success and not o.accounting_issues for o in state.outcomes)
    connector_for(state.config).lifecycle.validate_closure(state, state.closure)
    config = tmp_path / "config.yaml"
    config.write_bytes(
        artifact(prefix + "config.yaml.gz")
        if case.connector
        else (ROOT / "strategies" / case.reference / "config.yaml").read_bytes()
    )
    assert_fork_actions(case, state.model_dump(mode="json"), config)
    for check in case.checks():
        assert_native_direct(check, state)
    lending = next(c for c in case.checks() if str(c.connector) == "morpho_blue")
    with pytest.raises(AssertionError, match="missing direct completed author intents"):
        assert_native_direct(replace(lending, actions=(EconomicAction.SWAP,)), state)


def prepared_prefix(case):
    if case.connector:
        return "cases/" + case.id.replace("/", "-") + "/run/"
    chain = "base-v3" if case.reference.startswith("base_") else "ethereum-curve"
    return f"morpho-composition-prepared/{chain}/run/"


@pytest.mark.parametrize("case", PREPARED, ids=lambda c: c.id)
def test_prepared_morpho_proves_each_model_and_direct_intent(tmp_path, case):
    prefix = prepared_prefix(case)
    for name in ("manifest.json", "report.json", "state.sqlite"):
        (tmp_path / name).write_bytes(artifact(prefix + name + ".gz"))
    current_manifest(tmp_path)
    assert_backtest(case, tmp_path)
    raw = json.loads(artifact(prefix + "manifest.json.gz"))
    binding = connector_from_wire(raw["config"]).backtest(raw["settings"]["market_model"])
    manifest = binding.manifest_type.model_validate_json(json.dumps(reviewed_record(raw)))
    # The sealed artifact predates the package rename. This comparison permits only
    # the exact import rewrite; current-source lifecycle qualification remains separate.
    assert with_reviewed_hold_guard(
        historical_strategy_current_imports(manifest.strategy)
    ) == StrategyDefinition.read(ROOT / "strategies" / case.reference / "strategy.py")
    state = Journal(tmp_path / "state.sqlite", type(manifest.initial)).load()
    for check in case.checks():
        assert_economic_direct(check, state, manifest.config)
    assert json.loads(artifact(prefix + "reproduction.json.gz"))["reproduced"]
    if not case.connector:
        wrong = replace(case.scopes[0], model=case.scopes[1].model)
        with pytest.raises(AssertionError, match="component model differs"):
            assert_backtest(replace(case, scopes=(wrong, case.scopes[1])), tmp_path)


@pytest.mark.parametrize("case", [c for c in NATIVE if c.profile == "eoa"], ids=lambda c: c.id)
def test_eoa_code_qualification_matches_every_fund_and_exact_origin(case):
    prefix = native_prefix(case)
    names = [n for n in FILES if n.startswith(prefix) and n.endswith("/checkpoint.json.gz")]
    assert len(names) == 1
    raw = artifact(names[0])
    state = checkpoint_from_json(reviewed_json(raw.decode()))
    if case.connector:
        audit = json.loads(artifact("eoa-code-audit.json"))
        rows = [r for r in audit["rows"] if r["case"] == case.id.replace("/", "-")]
    else:
        audits = [
            n for n in FILES if n.startswith(prefix) and n.endswith("/eoa-code-audit.json.gz")
        ]
        assert len(audits) == 1
        audit = json.loads(artifact(audits[0]))
        assert audit["checkpoint_sha256"] == hashlib.sha256(raw).hexdigest()
        rows = audit["rows"]
    bindings = state.config.accounts.bindings
    assert len(rows) == len(bindings)
    assert {r["account"] for r in rows} == {str(b.name) for b in bindings}
    for binding in bindings:
        row = next(r for r in rows if r["account"] == str(binding.name))
        fund = binding.profile.fund
        assert row["address"] == str(fund.address)
        origin = next(b.origin for b in state.config.fork_bindings if b.chain == fund.chain)
        assert row["origin"] == origin.model_dump(mode="json")
        assert row["plain_eoa"] and row["result"] == "0x"
        assert row["request"] == {
            "method": "eth_getCode",
            "params": [
                str(fund.address),
                {"blockHash": str(origin.hash), "requireCanonical": True},
            ],
        }
