"""Require the retained complete matrices for the three additional lending pairs."""

import gzip
import hashlib
import json
from pathlib import Path

import pytest

from almanak.connectors.composed.state import Checkpoint as Composed
from almanak.connectors.compound_v3.state import Checkpoint as Compound
from almanak.connectors.morpho_blue.state import Checkpoint as Morpho
from tests.scope_only_sources import reviewed_json
from tests.strategy_fork.lending_trade_assertions import assert_closed

ROOT = Path(__file__).parents[2]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-09-lending-rollout/op-polygon-lending"
REFERENCES = (
    "compound_v3_optimism_lending",
    "morpho_blue_optimism_lending",
    "morpho_blue_polygon_lending",
)
COMPOSITIONS = (
    "optimism_compound_curve_borrow_and_trade",
    "optimism_morpho_curve_borrow_and_trade",
    "polygon_morpho_curve_borrow_and_trade",
)


def load(path):
    return json.loads(gzip.decompress(path.read_bytes()))


def retained_checkpoint(directory, model):
    sources = tuple(directory.rglob("checkpoint.json.gz"))
    assert len(sources) == 1
    return model.model_validate_json(reviewed_json(gzip.decompress(sources[0].read_bytes())))


def test_evidence_hashes_and_complete_matrix():
    manifest = json.loads((EVIDENCE / "manifest.json").read_text())
    for name, expected in manifest["files"].items():
        assert hashlib.sha256((EVIDENCE / name).read_bytes()).hexdigest() == expected
    summary = load(EVIDENCE / "current/promotion-summary.json.gz")
    assert summary["counts"] == {
        "native": 18,
        "prepared": 9,
        "recovery": 6,
        "composed_native": 9,
        "composed_prepared": 3,
    }


@pytest.mark.parametrize("reference", REFERENCES)
@pytest.mark.parametrize("policy", ("pinned", "fresh"))
@pytest.mark.parametrize(
    "profile,operator",
    (("eoa", "configured"), ("safe_roles", "configured"), ("safe_roles", "distinct")),
)
def test_native_reference_closes_scoped_ownership(reference, policy, profile, operator):
    directory = EVIDENCE / "current/proofs" / f"fork-{policy}-{reference}-{profile}-{operator}"
    assert load(directory / "result.json.gz")["status"] == "passed"
    state = retained_checkpoint(
        directory / "evidence", Compound if reference.startswith("compound") else Morpho
    )
    assert (state.mode, state.pending, state.plan) == ("closed", None, None)
    assert all(item.empty for item in state.owned)
    assert all(item.allowance == 0 for item in state.closure)
    assert state.delivered == len(state.outcomes)
    assert {binding.profile.kind for binding in state.config.accounts.bindings} == {profile}
    intents = [
        outcome.decision.intent.kind
        for outcome in state.outcomes
        if outcome.strategy_result().status == "completed"
    ]
    assert len(intents) == 9
    assert {"supply", "withdraw", "withdraw_amount", "borrow", "repay", "repay_amount"} <= set(
        intents
    )


@pytest.mark.parametrize("reference", REFERENCES)
@pytest.mark.parametrize("policy", ("pinned", "fresh", "native"))
def test_current_prepared_run_keeps_cold_source_and_offline_reproduction(reference, policy):
    name = f"backtest-{policy}-{reference}-economic"
    current = EVIDENCE / "current/proofs" / ("current-" + name)
    original = EVIDENCE / "original/proofs" / name
    assert load(current / "result.json.gz")["status"] == "passed"
    assert load(original / "cold-prepare.json.gz")["status"] == "passed"
    report = load(current / "run/report.json.gz")
    assert (
        report["closed"],
        report["completed_intents"],
        report["provider_requests_during_execution"],
    ) == (True, 9, 0)
    assert load(current / "run/reproduction.json.gz")["reproduced"]
    for phase in ("execute", "resume", "reproduce"):
        assert load(current / (phase + ".json.gz"))["network"] == "isolated"


@pytest.mark.parametrize("reference", COMPOSITIONS)
def test_composed_native_and_prepared_round_trips(reference):
    connector = "compound" if "compound" in reference else "morpho"
    cases = tuple((EVIDENCE / f"current/{connector}-composition").glob(reference + "-*"))
    assert len(cases) == 3
    for case in cases:
        assert_closed(None, retained_checkpoint(case, Composed))
    prepared = EVIDENCE / "current/composed-prepared-v2" / reference
    assert load(prepared / "result.json.gz")["status"] == "passed"
    assert load(prepared / "run/reproduction.json.gz")["reproduced"]
    assertions = load(prepared / "run/qa-assertions.json.gz")
    assert set(assertions["actions"]) == {
        "supply",
        "borrow",
        "swap_exact_input",
        "exit_exchange",
        "repay",
        "withdraw",
    }


@pytest.mark.parametrize("reference", REFERENCES)
def test_real_process_recovery_preserves_one_borrow(reference):
    summary = load(EVIDENCE / "current/promotion-summary.json.gz")
    rows = [row for row in summary["recovery"] if row["reference"] == reference]
    assert {row["profile"] for row in rows} == {"eoa", "safe_roles"}
    connector = "compound" if reference.startswith("compound") else "morpho"
    network = "polygon" if "polygon" in reference else "optimism"
    for row in rows:
        directory = EVIDENCE / f"current/{connector}-promotion-recovery"
        sources = tuple(directory.glob(network + "-" + row["profile"] + "-*/result.json.gz"))
        assert len(sources) == 1
        record = load(sources[0])
        assert (
            record["reserved"]["plan"]["attempt_id"]
            == record["lost_response"]["pending"]["attempt_id"]
        )
        assert (
            record["broadcast_transaction"]["hash"]
            == record["committed"]["outcomes"][-1]["tx_hash"]
        )
        assert record["status"] == "passed"
