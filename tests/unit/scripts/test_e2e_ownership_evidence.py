"""Ownership attacks use recorded opening bytes from a captured fork run."""

import pytest

from qa_lab import qa_coverage
from qa_lab.e2e_card import REPO, canonical, digest, load_json
from qa_lab.e2e_ownership import OwnershipError
from qa_lab.e2e_ownership_evidence import validate_ownership_evidence
from tests.unit.scripts import test_e2e_bundle as bundle_tests
from tests.unit.scripts.test_quant_admission import _decision

checkout = bundle_tests.checkout
lane = bundle_tests.lane


def _record_open(lane):
    context, _, _ = lane
    token = context.store.reserve_launch(context.lease, "subject")
    context.store.claim_launch(context.lease, role="subject", token=token)
    raw = (REPO / "tests/fixtures/accounting/harness/lp-dual-generations/positions-open.json").read_bytes()
    context.store.publish_phase(context.lease, "open", raw)
    return raw


def test_cleanup_successor_exports_original_phase_generation(lane):
    context, _, _ = lane
    raw = _record_open(lane)
    original = context.lease
    context.store.clock = lambda: 10**10
    context.lease = context.store.acquire("successor", cleanup=True, seconds=300)
    bundle_tests.assemble(lane)
    bundle = context.root / "bundle"
    value = load_json(bundle / "ownership.json")
    assert value["generation"] == original.generation + 1
    assert value["phases"] == [{"phase": "open", "sha256": digest(raw), "generation": original.generation}]
    assert "token" not in value["launches"][0]
    assert validate_ownership_evidence(bundle, load_json(bundle / "preparation/card.json")) == [
        "ownership.json",
        "positions-open.json",
    ]
    with pytest.raises(OwnershipError, match="stale"):
        context.store.export_cleanup_evidence(original)


@pytest.mark.parametrize("mutation", ["orphan", "changed", "missing"])
def test_assembly_refuses_unrecorded_or_changed_phase(lane, mutation):
    context, _, _ = lane
    raw = _record_open(lane) if mutation != "orphan" else b"{}\n"
    path = context.root / "positions-open.json"
    if mutation == "missing":
        path.unlink()
    else:
        path.write_bytes(raw + b" ")
    with pytest.raises(OwnershipError, match="ownership record|changed|missing"):
        bundle_tests.assemble(lane)
    assert not (context.root / "bundle/assembly.json").exists()


@pytest.mark.parametrize("mutation", ["file", "record", "card", "future", "unclaimed", "missing"])
def test_refreshed_audit_cannot_replace_owned_phase(lane, mutation):
    context, _, _ = lane
    _record_open(lane)
    bundle_tests.assemble(lane)
    bundle = context.root / "bundle"
    path = bundle / "ownership.json"
    value = load_json(path)
    if mutation == "file":
        (bundle / "positions-open.json").write_bytes(b"{}\n")
    elif mutation == "record":
        value["phases"] = []
    elif mutation == "card":
        value["card_sha256"] = "0" * 64
    elif mutation == "future":
        value["phases"][0]["generation"] += 1
    elif mutation == "unclaimed":
        value["launches"][0]["claimed"] = 0
    path.write_bytes(canonical(value))
    if mutation == "missing":
        path.unlink()
    provenance = load_json(bundle / "git.json")
    provenance.update(dirty=False, sdk_version="0.0-test", branch="test")
    (bundle / "git.json").write_bytes(canonical(provenance))
    _decision(bundle)
    store = context.root / "sealed-store"
    with pytest.raises(ValueError, match="ownership|Ownership|owned regular"):
        qa_coverage.seal_bundle(
            bundle=bundle,
            store=store,
            catalog_path=REPO / "qa_lab/docs/catalog/v1/cells.yaml",
            cell_id="lp.uniswap_v3.arbitrum.complex.anvil.eoa",
            network="anvil",
            exec_path="eoa",
            lane="daily",
            run_id="mutated-ownership",
        )
    assert not list((store / "quant").rglob("manifest.json"))


def test_running_owner_cannot_export_cleanup_authority(lane):
    context, _, _ = lane
    with pytest.raises(OwnershipError, match="cleanup ownership"):
        context.store.export_cleanup_evidence(context.lease)


def test_takeover_during_assembly_prevents_completed_manifest(lane, monkeypatch):
    context, _, _ = lane
    _record_open(lane)

    def takeover(value):
        context.store.clock = lambda: 10**10
        context.store.acquire("successor", cleanup=True, seconds=300)
        return value

    monkeypatch.setattr(bundle_tests.e2e_bundle, "redact_diagnostic", takeover)
    with pytest.raises(OwnershipError, match="stale"):
        bundle_tests.assemble(lane)
    assert not (context.root / "bundle/assembly.json").exists()
