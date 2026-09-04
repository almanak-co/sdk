"""Contracts for fail-closed chain/protocol readiness seals and Lab UX."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import ModuleType

import pytest

TEST_COMMIT = "a" * 40
TEST_SDK = {
    "commit": TEST_COMMIT,
    "branch": "test",
    "dirty": False,
    "sdk_version": "0.0-test",
    "source": "executing-worktree",
}


@pytest.fixture(scope="module")
def readiness_module() -> ModuleType:
    path = Path(__file__).resolve().parents[3] / "qa_lab" / "qa_readiness.py"
    spec = importlib.util.spec_from_file_location("qa_readiness_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_readiness_aggregate_is_fail_closed(readiness_module: ModuleType) -> None:
    contract = list(readiness_module.CHAIN_GATES)
    green = [
        {
            "gate_id": gate["id"],
            "state": "PASS",
            "summary": "ok",
            "evidence": [],
            "evidence_basis": "DERIVED",
        }
        for gate in contract
    ]
    assert readiness_module.readiness_verdict(green, contract) == "OFFICIAL"

    declared = [dict(row) for row in green]
    declared[0]["evidence_basis"] = "DECLARED"
    assert readiness_module.readiness_verdict(declared, contract) == "NOT_READY"

    for weak_state in ("REFERENCE", "NEVER", "BLOCKED", "FAIL"):
        weak = [dict(row) for row in green]
        weak[0]["state"] = weak_state
        assert readiness_module.readiness_verdict(weak, contract) != "OFFICIAL"


def test_empty_readiness_board_is_honest_and_navigable(readiness_module: ModuleType, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    page = readiness_module.render_readiness_lab(store=store, lab_css="")
    rendered = page.read_text(encoding="utf-8")

    assert json.loads((store / "index" / "chain_readiness_latest.json").read_text()) == {}
    assert json.loads((store / "index" / "protocol_readiness_latest.json").read_text()) == {}
    assert ">Chains</button>" in rendered
    assert ">Protocols</button>" in rendered
    assert "Robinhood" in rendered
    assert "GMX V2" in rendered
    assert 'id="subject-picker"' in rendered
    assert "Open full Support Report" in rendered
    assert "every required gate must be DERIVED exact PASS" in rendered
    assert "Unpinned PASS" in rendered
    assert "LEGACY / UNPINNED" in rendered
    assert "Order re-check" in rendered
    assert "RECHECK_ROUTES" in rendered
    assert 'class="btn active" href="readiness.html">Checklists</a>' in rendered
    assert '<div class="logo">AQA</div>' in rendered


def test_robinhood_census_seals_but_never_promotes_reference(readiness_module: ModuleType, tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    store = tmp_path / "qa"
    readiness_module.bootstrap_readiness(store)
    catalog = readiness_module.build_readiness_catalog(output=store / "catalog" / "readiness_checklists.json")
    bundle = tmp_path / "bundle"
    readiness_module.run_readiness_census(
        output=bundle,
        kind="chain",
        subject="robinhood",
        store=store,
        repo_root=repo_root,
        sdk_provenance=TEST_SDK,
    )
    target = readiness_module.seal_readiness_bundle(bundle=bundle, store=store, catalog=catalog)
    manifest = json.loads((target / "manifest.json").read_text())
    latest = json.loads((store / "index" / "chain_readiness_latest.json").read_text())["robinhood"]

    assert manifest["verdict"] == "NOT_READY"
    assert manifest["attribution_mode"] == "mixed-repo-census"
    assert all(row["evidence_basis"] in {"DERIVED", "DECLARED"} for row in manifest["gates"])
    assert latest["verdict"] == "NOT_READY"
    assert next(row for row in manifest["gates"] if row["gate_id"] == "C10")["state"] == "FAIL"
    assert (target / "report.html").is_file()
    assert all(artifact["sha256"] for artifact in manifest["artifacts"])
    with pytest.raises(FileExistsError):
        readiness_module.seal_readiness_bundle(bundle=bundle, store=store, catalog=catalog)


def test_seal_rejects_missing_gate(readiness_module: ModuleType, tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    store = tmp_path / "qa"
    readiness_module.bootstrap_readiness(store)
    catalog = readiness_module.build_readiness_catalog()
    bundle = tmp_path / "bundle"
    readiness_module.run_readiness_census(
        output=bundle,
        kind="chain",
        subject="robinhood",
        store=store,
        repo_root=repo_root,
        sdk_provenance=TEST_SDK,
    )
    manifest = json.loads((bundle / "manifest.json").read_text())
    manifest["gates"].pop()
    (bundle / "manifest.json").write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="every checklist gate"):
        readiness_module.seal_readiness_bundle(bundle=bundle, store=store, catalog=catalog)


def test_seal_rejects_duplicate_gate(readiness_module: ModuleType, tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[3]
    store = tmp_path / "qa"
    readiness_module.bootstrap_readiness(store)
    catalog = readiness_module.build_readiness_catalog()
    bundle = tmp_path / "bundle"
    readiness_module.run_readiness_census(
        output=bundle,
        kind="chain",
        subject="robinhood",
        store=store,
        repo_root=repo_root,
        sdk_provenance=TEST_SDK,
    )
    manifest = json.loads((bundle / "manifest.json").read_text())
    manifest["gates"].append(dict(manifest["gates"][0]))
    (bundle / "manifest.json").write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="every checklist gate exactly once"):
        readiness_module.seal_readiness_bundle(bundle=bundle, store=store, catalog=catalog)


def test_gmx_readiness_requires_exact_all_chain_resource_closure(
    readiness_module: ModuleType,
    tmp_path: Path,
) -> None:
    store = tmp_path / "qa"
    (store / "index").mkdir(parents=True)
    latest = {
        "protocol.gmx_v2.market_resolution.arbitrum.offline": {
            "cell_id": "protocol.gmx_v2.market_resolution.arbitrum.offline",
            "verdict": "PASS",
        },
        "protocol.gmx_v2.resource_catalogue_reconciliation.arbitrum.mainnet": {
            "cell_id": "protocol.gmx_v2.resource_catalogue_reconciliation.arbitrum.mainnet",
            "capability": "resource_catalogue_reconciliation",
            "verdict": "PASS",
            "checks": {"denominator": 130},
            "report_path": "protocols/arbitrum/report.html",
        },
        "protocol.gmx_v2.consumer_closure.arbitrum.mainnet": {
            "cell_id": "protocol.gmx_v2.consumer_closure.arbitrum.mainnet",
            "capability": "consumer_closure",
            "verdict": "FAIL",
            "checks": {"denominator": 130},
            "report_path": "protocols/arbitrum/report.html",
        },
    }
    (store / "index" / "protocol_latest.json").write_text(json.dumps(latest))

    gates = {row["gate_id"]: row for row in readiness_module._gmx_results(store)}

    assert gates["P3"]["state"] == "NEVER"
    assert "1/2 required chain closures" in gates["P3"]["summary"]
    assert gates["P4"]["state"] == "FAIL"
    assert "still missing avalanche" in gates["P4"]["summary"]


def _junit(tests: int = 4, failures: int = 0, errors: int = 0) -> str:
    cases = "".join(f'<testcase classname="t" name="t{index}"/>' for index in range(tests))
    return (
        f'<?xml version="1.0" encoding="utf-8"?>'
        f'<testsuite name="readiness" tests="{tests}" failures="{failures}" errors="{errors}">{cases}</testsuite>'
    )


def _official_chain_bundle(readiness_module: ModuleType, bundle: Path, *, run_id: str) -> dict:
    """A chain census whose every mandatory gate is PASS on a parseable report."""
    bundle.mkdir(parents=True, exist_ok=True)
    gates = []
    for gate in readiness_module.CHAIN_GATES:
        name = f"{gate['id'].lower()}-unit.junit.xml"
        (bundle / name).write_text(_junit(), encoding="utf-8")
        gates.append(
            {
                "gate_id": gate["id"],
                "state": "PASS",
                "summary": "focused suite passed",
                "evidence": [name],
                "evidence_basis": "DERIVED",
            }
        )
    manifest = {
        "schema_version": 1,
        "evidence_kind": "almanak.readiness.census",
        "run_id": run_id,
        "kind": "chain",
        "subject": "robinhood",
        "git_sha": TEST_COMMIT,
        "sdk": TEST_SDK,
        "completed_at": "2026-08-19T12:00:00+00:00",
        "attribution_mode": "mixed-repo-census",
        "certificate_rule": "OFFICIAL requires every mandatory gate to be DERIVED and exact PASS",
        "verdict": "OFFICIAL",
        "claim_boundary": "synthetic fixture",
        "gates": gates,
    }
    (bundle / "manifest.json").write_text(json.dumps(manifest))
    (bundle / "observations.json").write_text(json.dumps({"subject": "robinhood", "kind": "chain", "gates": gates}))
    return manifest


def test_a_declared_derived_basis_is_demoted_when_nothing_backs_it(
    readiness_module: ModuleType, tmp_path: Path
) -> None:
    """Reproduces scored failure F1's mechanism at the gate level.

    ``evidence_basis`` decides whether a gate can promote a subject to OFFICIAL,
    and it arrived as an author-supplied string. C10 declares DERIVED while
    citing no attached artifact at all -- it was derived from the repository at
    census time, which the seal cannot check and the reader cannot audit.
    """
    repo_root = Path(__file__).resolve().parents[3]
    store = tmp_path / "qa"
    readiness_module.bootstrap_readiness(store)
    catalog = readiness_module.build_readiness_catalog(output=store / "catalog" / "readiness_checklists.json")
    bundle = tmp_path / "bundle"
    readiness_module.run_readiness_census(
        output=bundle,
        kind="chain",
        subject="robinhood",
        store=store,
        repo_root=repo_root,
        sdk_provenance=TEST_SDK,
    )
    declared = json.loads((bundle / "manifest.json").read_text())
    assert next(row for row in declared["gates"] if row["gate_id"] == "C10")["evidence_basis"] == "DERIVED"

    target = readiness_module.seal_readiness_bundle(bundle=bundle, store=store, catalog=catalog)

    sealed = {row["gate_id"]: row for row in json.loads((target / "manifest.json").read_text())["gates"]}
    assert sealed["C10"]["declared_evidence_basis"] == "DERIVED"
    assert sealed["C10"]["evidence_basis"] == "DECLARED"
    assert sealed["C10"]["derivation"]["reason"] == "gate_cites_no_machine_readable_evidence"
    assert sealed["C10"]["authorities"] == []
    # The gate keeps its state and its prose; it just cannot carry a certificate.
    assert sealed["C10"]["state"] == "FAIL"


def test_a_hand_typed_readiness_manifest_cannot_mint_an_official_certificate(
    readiness_module: ModuleType, tmp_path: Path
) -> None:
    """Scored failure F1: a readiness certificate produced from one file."""
    from qa_lab.admission_contract import AdmissionRefused

    store = tmp_path / "qa"
    readiness_module.bootstrap_readiness(store)
    catalog = readiness_module.build_readiness_catalog(output=store / "catalog" / "readiness_checklists.json")
    bundle = tmp_path / "forged"
    manifest = _official_chain_bundle(readiness_module, bundle, run_id="20260819-forged")
    # Strip the proof: the manifest still asserts thirteen DERIVED exact passes.
    for path in bundle.glob("*.junit.xml"):
        path.unlink()

    with pytest.raises(AdmissionRefused, match="declared PASS but the sealer derived FAIL"):
        readiness_module.seal_readiness_bundle(bundle=bundle, store=store, catalog=catalog)

    assert manifest["verdict"] == "OFFICIAL"
    assert not list((store / "readiness").rglob("manifest.json"))
    assert not (store / "index" / "experiment_runs.jsonl").exists()


def test_an_official_certificate_must_survive_its_own_negative_control(
    readiness_module: ModuleType, tmp_path: Path
) -> None:
    """The liveness control is only exercised on greens, so a green must exist.

    Both live readiness subjects sit at NOT_READY, where the admission contract
    records liveness as NOT_APPLICABLE. Without this fixture the negative control
    would never run on this surface and could rot green untested.
    """
    store = tmp_path / "qa"
    readiness_module.bootstrap_readiness(store)
    catalog = readiness_module.build_readiness_catalog(output=store / "catalog" / "readiness_checklists.json")
    bundle = tmp_path / "official"
    _official_chain_bundle(readiness_module, bundle, run_id="20260819-official")

    target = readiness_module.seal_readiness_bundle(bundle=bundle, store=store, catalog=catalog)

    sealed = json.loads((target / "manifest.json").read_text())
    assert sealed["verdict"] == "OFFICIAL"
    control = sealed["derived_claim"]["admission_control"]
    assert control["declared_status"] == "PASS"
    assert control["derived_status"] == "PASS"
    assert control["liveness"]["status"] == "PASS"
    # Every mandatory gate's report was corrupted in turn, and each corruption
    # moved the certificate off OFFICIAL.
    mutated = {row["source"] for row in control["liveness"]["mutations"]}
    assert mutated == {f"{gate['id'].lower()}-unit.junit.xml" for gate in readiness_module.CHAIN_GATES}
    assert all(row["mutant_status"] != "PASS" for row in control["liveness"]["mutations"])
    ledger = [json.loads(line) for line in (store / "index" / "experiment_runs.jsonl").read_text().splitlines() if line]
    assert ledger[-1]["admission"]["status"] == "OFFICIAL"
    assert ledger[-1]["cell_verdicts"]["readiness.chain.robinhood.C1"] == "PASS"


def test_a_gate_contradicted_by_its_own_report_is_not_derived(readiness_module: ModuleType, tmp_path: Path) -> None:
    """A PASS whose attached suite records a failure loses the basis, not the record."""
    from qa_lab.admission_contract import AdmissionRefused

    store = tmp_path / "qa"
    readiness_module.bootstrap_readiness(store)
    catalog = readiness_module.build_readiness_catalog(output=store / "catalog" / "readiness_checklists.json")
    bundle = tmp_path / "contradicted"
    _official_chain_bundle(readiness_module, bundle, run_id="20260819-contradicted")
    (bundle / "c3-unit.junit.xml").write_text(_junit(tests=4, failures=1), encoding="utf-8")

    with pytest.raises(AdmissionRefused, match="declared PASS but the sealer derived FAIL"):
        readiness_module.seal_readiness_bundle(bundle=bundle, store=store, catalog=catalog)

    # A zero-test report cannot mean "everything passed" either.
    claim = readiness_module.derive_readiness_claim(bundle)
    assert claim["verdict"] == "NOT_READY"
    assert "declared_derived_not_supported:C3" in claim["reason_codes"]
    (bundle / "c3-unit.junit.xml").write_text(_junit(tests=0), encoding="utf-8")
    empty = {row["gate_id"]: row for row in readiness_module.derive_readiness_claim(bundle)["gates"]}
    assert empty["C3"]["derivation"]["note"] == "junit_reports_zero_tests"
    assert empty["C3"]["derivation"]["reason"] == "declared_state_contradicts_cited_evidence"


def test_the_readiness_board_shows_the_basis_the_seal_could_support(
    readiness_module: ModuleType, tmp_path: Path
) -> None:
    """The census claimed DERIVED on C10; the rail has to show that it did not hold."""
    repo_root = Path(__file__).resolve().parents[3]
    store = tmp_path / "qa"
    readiness_module.bootstrap_readiness(store)
    catalog = readiness_module.build_readiness_catalog(output=store / "catalog" / "readiness_checklists.json")
    bundle = tmp_path / "bundle"
    readiness_module.run_readiness_census(
        output=bundle,
        kind="chain",
        subject="robinhood",
        store=store,
        repo_root=repo_root,
        sdk_provenance=TEST_SDK,
    )
    readiness_module.seal_readiness_bundle(bundle=bundle, store=store, catalog=catalog)

    page = readiness_module.render_readiness_lab(store=store, lab_css=".admission{}").read_text(encoding="utf-8")

    assert "function gateAdmission(result)" in page
    assert "census claimed DERIVED" in page
    assert '"declared_evidence_basis"' in page
    assert '"gate_cites_no_machine_readable_evidence"' in page
