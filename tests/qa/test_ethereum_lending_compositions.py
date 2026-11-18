"""Retained native and prepared Ethereum lending compositions preserve economic order."""

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from almanak.connectors.composed.state import Checkpoint
from tests.scope_only_sources import reviewed_record

ROOT = Path(__file__).parents[2]
EVIDENCE = (
    ROOT / "docs/internal/reports/evidence/2026-09-09-lending-rollout/ethereum-lending-compositions"
)


def record(path):
    return json.loads(gzip.decompress((EVIDENCE / (path + ".gz")).read_bytes()))


def summary():
    return record("audit/summary.json")


def test_sealed_manifest_covers_all_anchor_profiles():
    manifest = json.loads((EVIDENCE / "manifest.json").read_text())
    for path, digest in manifest["files"].items():
        assert hashlib.sha256((EVIDENCE / path).read_bytes()).hexdigest() == digest
    rows = summary()["composed_native"]
    for connector in ("spark", "compound"):
        assert {(r["profile"], r["operator"]) for r in rows if r["connector"] == connector} == {
            ("eoa", "configured"),
            ("safe_roles", "configured"),
            ("safe_roles", "distinct"),
        }
    assert len(summary()["composed_prepared"]) == 2


@pytest.mark.parametrize("index", range(6))
def test_native_anchor_closes_in_order_and_retains_fund_authority(index):
    row = summary()["composed_native"][index]
    state = Checkpoint.model_validate_json(
        json.dumps(reviewed_record(record(row["stage"] + "/" + row["path"] + "/checkpoint.json")))
    )
    assert state.mode == "closed" and state.pending is None and state.plan is None
    assert state.delivered == len(state.outcomes) and all(o.success for o in state.outcomes)
    completed = [o.decision.intent.kind for o in state.outcomes if o.strategy_result().complete]
    expected = ["supply", "borrow", "swap_exact_input", "exit_exchange", "repay", "withdraw"]
    if row["connector"] == "compound":
        expected = ["supply", *expected, "withdraw"]
        assert all(p.empty for p in state.owned[:-1])
    else:
        assert all(p.supply_scaled == p.debt_scaled == 0 for p in state.owned[:-1])
        assert not any(p.collateral_enabled for p in state.owned[:-1])
    assert completed == expected
    assert not state.memory.values["traded"]
    assert all(o.allowance == 0 for o in state.closure[:-1])
    assert not state.closure[-1].needs_cleanup
    assert all(b.profile.kind == row["profile"] for b in state.config.accounts.bindings)
    if row["operator"] == "distinct":
        assert len({b.profile.submitter for b in state.config.accounts.bindings}) == len(
            state.config.accounts.bindings
        )


@pytest.mark.parametrize("index", range(2))
def test_prepared_anchor_retains_isolated_reproduction(index):
    row = summary()["composed_prepared"][index]
    path = row["stage"] + "/" + row["path"]
    report = record(path + "/run/report.json")
    assert report["closed"] and report["completed_intents"] == row["completed_intents"]
    assert report["provider_requests_during_execution"] == 0
    assert record(path + "/run/reproduction.json")["reproduced"]
    connector = "sparklend" if row["connector"] == "spark" else "compound_v3"
    assert set(report["components"]) == {connector, "curve"}
    for phase in ("cached-prepare", "execute", "resume", "reproduce"):
        result = record(path + "/" + phase + ".json")
        assert result["status"] == "passed" and result["network"] == "isolated"
