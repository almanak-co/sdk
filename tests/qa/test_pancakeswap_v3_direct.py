"""Current LP references directly close before exiting residual counter inventory."""

import gzip
import hashlib
import json

import pytest

from almanak.backtest.engine import Journal
from almanak.connectors.pancakeswap_v3.economic_binding import Manifest
from almanak.connectors.pancakeswap_v3.state import Checkpoint
from tests.exchange_scope import without_exchange_budgets
from tests.qa.catalog import ROOT, catalog
from tests.qa.intent_coverage import assert_economic_direct, assert_native_direct
from tests.qa.lp_refactor import assert_reference_source
from tests.strategy_fork.v3_pool_assertions import assert_closed

BASE = ROOT / "docs/internal/reports/evidence/2026-09-08-rollout/pancakeswap-v3-direct-close"
GUARD = BASE.with_name("pancakeswap-v3-direct-scope-recheck")
NETWORKS = ("bnb", "ethereum", "base", "arbitrum")


def artifact(name, directory=BASE):
    manifest = json.loads((directory / "manifest.json").read_text())
    row = next(row for row in manifest["files"] if row["path"] == name)
    raw = (directory / name).read_bytes()
    assert len(raw) == row["bytes"] and hashlib.sha256(raw).hexdigest() == row["sha256"]
    return gzip.decompress(raw)


def reference(network):
    return f"pancakeswap_v3_{network}_lp"


def native(network, profile):
    return Checkpoint.model_validate_json(
        json.dumps(
            without_exchange_budgets(
                json.loads(
                    artifact(f"native/{reference(network)}-{profile}/evidence/checkpoint.json.gz")
                )
            )
        )
    )


@pytest.mark.parametrize("network", NETWORKS)
@pytest.mark.parametrize("profile", ("eoa", "safe_roles"))
def test_current_reference_direct_intents_reconcile_closed_native_protocol_effects(
    network, profile
):
    name = reference(network)
    state = native(network, profile)
    assert_reference_source(state.strategy, name)
    case = next(
        case
        for case in catalog()
        if (case.reference, case.lane, case.profile, case.input_policy, case.operator_mode)
        == (name, "fork", profile, "pinned", "configured")
    )
    assert_native_direct(case, state)
    assert_closed(None, state)
    assert state.delivered == len(state.outcomes)
    direct = [
        result.intent.kind
        for outcome in state.outcomes[: state.delivered]
        if (result := outcome.strategy_result()).status == "completed"
    ]
    assert direct[-2:] == ["close_liquidity", "exit_exchange"]
    report = json.loads(artifact(f"native/{name}-{profile}/public-report.log.gz"))
    assert report["mode"] == "closed" and report["transaction_accounting_complete"]


def test_each_direct_eoa_fund_matches_an_independent_empty_code_origin_read():
    rows = json.loads(artifact("native/origin-fund-qualification.json.gz"))
    assert {row["direct_reference"] for row in rows} == {reference(n) for n in NETWORKS}
    for row in rows:
        state = Checkpoint.model_validate_json(
            json.dumps(
                without_exchange_budgets(
                    json.loads(
                        artifact(
                            f"native/{row['direct_reference']}-eoa/evidence/checkpoint.json.gz"
                        )
                    )
                )
            )
        )
        assert row["address"] == state.config.accounts.bindings[0].profile.fund.address
        assert row["result"] == "0x" and row["plain_eoa"]
        result = json.loads(
            artifact(f"native/{row['direct_reference']}-eoa/evidence/result.json.gz")
        )
        assert row["origin"] in result["origins"]


@pytest.mark.parametrize("network", NETWORKS)
def test_prepared_captured_reference_has_direct_close_and_public_offline_phases(tmp_path, network):
    name = reference(network)
    manifest = Manifest.model_validate_json(
        json.dumps(
            without_exchange_budgets(json.loads(artifact(f"prepared/{name}/run/manifest.json.gz")))
        )
    )
    assert_reference_source(manifest.strategy, name)
    path = tmp_path / "state.sqlite"
    path.write_bytes(artifact(f"prepared/{name}/run/state.sqlite.gz"))
    state = Journal(path, type(manifest.initial)).load()
    case = next(
        case
        for case in catalog()
        if (case.reference, case.lane, case.input_policy) == (name, "backtest", "pinned")
    )
    assert_economic_direct(case, state, manifest.config)
    assert [item.transition.result.intent.kind for item in state.completed][-2:] == [
        "close_liquidity",
        "exit_exchange",
    ]
    for phase in ("execute", "reproduce", "resume", "public-report", "public-html"):
        record = json.loads(artifact(f"prepared/{name}/{phase}.json.gz"))
        assert (record["status"], record["exit_code"], record["network"]) == (
            "passed",
            0,
            "isolated",
        )
    report = json.loads(artifact(f"prepared/{name}/run/report.json.gz"))
    assert report["closed"] and report["provider_requests_during_execution"] == 0


def test_all_new_reference_states_survive_both_real_os_isolated_commit_crashes():
    records = json.loads(artifact("prepared/isolated-results.json.gz"))
    assert {row["case"] for row in records} == {reference(n) for n in NETWORKS}
    for row in records:
        assert row["network_probe_errno"] == 101 and row["provider_requests"] == 0
        assert row["reproduced"] and row["retained_resume_equal"]
        assert {r["stage"] for r in row["recovery"]} == {"pending_commit", "effects_commit"}
        assert all(r["killed_returncode"] == -9 and r["exact_state_equal"] for r in row["recovery"])


def test_stronger_connector_scope_recheck_qualifies_every_sealed_direct_proof():
    result = json.loads(artifact("result.json.gz", GUARD))
    assert (
        result["evidence_manifest_sha256"]
        == hashlib.sha256((BASE / "manifest.json").read_bytes()).hexdigest()
    )
    assert len(result["cases"]) == 12
    assert all(
        row["passed"] and row["trusted_connector"] == "pancakeswap_v3" for row in result["cases"]
    )
