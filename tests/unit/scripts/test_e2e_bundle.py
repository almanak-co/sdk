import shutil
import sqlite3
from types import SimpleNamespace

import pytest

from qa_lab import e2e_bundle
from qa_lab.e2e_card import REPO, STRATEGY, TEMPLATE, canonical, digest, load_json, prepare
from tests.unit.scripts import test_e2e_card as card_tests

checkout = card_tests.checkout


@pytest.fixture
def lane(checkout, tmp_path):
    root = tmp_path / "lane"
    prepare(checkout, REPO / TEMPLATE, root / "preparation", "assembly-test-run")
    subject = root / "subject"
    shutil.copytree(checkout / STRATEGY, subject)
    shutil.copyfile(root / "preparation/config.json", subject / "config.json")
    cleanup = root / "cleanup-attempt"
    cleanup.mkdir()
    (cleanup / "result.json").write_bytes(canonical({"scope": "subject_lp_teardown", "status": "FAIL"}))
    quantities = cleanup / "quantities"
    quantities.mkdir()
    with sqlite3.connect(quantities / "almanak_state.db") as db:
        db.execute("CREATE TABLE retained (value INTEGER)")
        db.execute("INSERT INTO retained VALUES (7)")
    (root / "run.log").write_text("2026-09-07T00:00:00Z endpoint https://example.test/secret-path?token=example\n")
    context = SimpleNamespace(root=root, require_owned=lambda path: path)
    from qa_lab.e2e_ownership import OwnershipStore

    context.store = OwnershipStore(root / "ownership.sqlite")
    context.store.initialize(
        run_id="assembly-test-run", card_hash=digest((root / "preparation/card.json").read_bytes())
    )
    context.lease = context.store.acquire("assembler", seconds=300)
    return context, cleanup, checkout


def assemble(lane):
    context, cleanup, repo = lane
    context.store.request_cleanup(context.lease)
    return e2e_bundle.assemble_bundle(
        context, context.store, context.lease, cleanup=cleanup, output=context.root / "bundle", repo=repo
    )


def test_partial_attempt_exports_exact_contract_and_snapshot_without_claiming_admission(lane):
    context, cleanup, _ = lane
    original = (context.root / "run.log").read_bytes()
    result = assemble(lane)
    bundle = context.root / "bundle"
    assert result["status"] == "INCOMPLETE"
    assert result["e2e_admission"] == "UNMEASURED"
    assert "positions-managed.json" in result["missing_evidence"]
    assert "hold-observations/" in result["missing_evidence"]
    assert (bundle / "lifecycle-contract.json").read_bytes() == (
        context.root / "preparation/lifecycle-contract.json"
    ).read_bytes()
    assert (bundle / "almanak_state.db").read_bytes() == (cleanup / "quantities/almanak_state.db").read_bytes()
    assert (context.root / "run.log").read_bytes() == original
    assert "secret-path" not in (bundle / "run.log").read_text()
    assert not any((bundle / name).exists() for name in ("finding.json", "audit-decision.json", "audit.md"))
    for item in result["artifacts"]:
        assert digest((bundle / item["path"]).read_bytes()) == item["sha256"]
        assert digest((context.root / item["source"]).read_bytes()) == item["source_sha256"]
    with pytest.raises(FileExistsError):
        assemble(lane)
    assert load_json(bundle / "assembly.json") == result


def test_local_subject_log_is_exported_with_source_binding_and_redaction(lane):
    context, _, _ = lane
    source = context.root / "subject-process.log"
    (context.root / "run.log").rename(source)
    original = source.read_bytes()
    result = assemble(lane)
    record = next(item for item in result["artifacts"] if item["path"] == "run.log")
    assert record["source"] == "subject-process.log"
    assert record["source_sha256"] == digest(original)
    assert source.read_bytes() == original
    assert "run.log" not in result["missing_evidence"]
    assert "secret-path" not in (context.root / "bundle/run.log").read_text()


def test_ambiguous_subject_logs_cannot_be_selected_by_precedence(lane):
    context, _, _ = lane
    (context.root / "subject-process.log").write_text("different run transcript\n")
    with pytest.raises(ValueError, match="Ambiguous subject log"):
        assemble(lane)
    assert not (context.root / "bundle/assembly.json").exists()


@pytest.mark.parametrize("mutation", ["strategy", "parent_symlink", "wal"])
def test_unsafe_sources_cannot_produce_a_completed_assembly(lane, mutation):
    context, cleanup, _ = lane
    if mutation == "strategy":
        (context.root / "subject/strategy.py").write_text("changed after launch")
    elif mutation == "parent_symlink":
        (context.root / "price-observations").symlink_to(context.root / "preparation", target_is_directory=True)
    else:
        (cleanup / "quantities/almanak_state.db-wal").write_bytes(b"uncheckpointed")
    with pytest.raises(ValueError):
        assemble(lane)
    assert not (context.root / "bundle/assembly.json").exists()


def test_source_mutation_during_copy_leaves_no_completed_assembly(lane, monkeypatch):
    context, _, _ = lane

    def changed(value):
        (context.root / "run.log").write_text("changed while exporting")
        return value

    monkeypatch.setattr(e2e_bundle, "redact_diagnostic", changed)
    with pytest.raises(ValueError, match="changed during bundle assembly"):
        assemble(lane)
    assert not (context.root / "bundle/assembly.json").exists()


def test_actor_evidence_retains_physical_closure_separately_from_failed_quantities(lane):
    context, cleanup, _ = lane
    actor = context.root / "actor"
    actor.mkdir()
    (actor / "config.json").write_bytes(canonical({"amount_usdc_raw": 100}))
    (actor / "quote.json").write_bytes(canonical({"status": "QUOTED"}))
    (actor / "provisioning.json").write_bytes(canonical({"nonce": 7017}))
    (actor / "binding.json").write_bytes(
        canonical(
            {
                "bindings": {
                    f"{name}_sha256": digest((actor / f"{name}.json").read_bytes())
                    for name in ("config", "quote", "provisioning")
                }
            }
        )
    )
    (actor / "strategy.py").write_text("# retained actor source\n")
    (actor / "__init__.py").write_text("")
    evidence = cleanup / "stimulus"
    chain = evidence / "quantities/chain"
    chain.mkdir(parents=True)
    (evidence / "closure-result.json").write_bytes(canonical({"status": "PASS"}))
    (evidence / "result.json").write_bytes(canonical({"status": "FAIL", "position_cleanup_status": "PASS"}))
    (evidence / "quantities/result.json").write_bytes(canonical({"status": "FAIL"}))
    (chain / "submission-census.json").write_bytes(canonical({"status": "FAIL", "unattributed_tx_hashes": ["0x01"]}))
    shutil.copyfile(cleanup / "quantities/almanak_state.db", evidence / "quantities/almanak_state.db")

    result = assemble(lane)
    bundle = context.root / "bundle"
    assert load_json(bundle / "stimulus-closure.json")["status"] == "PASS"
    assert load_json(bundle / "stimulus-cleanup.json")["status"] == "FAIL"
    retained = {item["path"]: item for item in result["artifacts"]}
    for path in (
        "stimulus/config.json",
        "stimulus/binding.json",
        "stimulus/quote.json",
        "stimulus/provisioning.json",
        "stimulus/strategy.py",
        "stimulus/__init__.py",
        "stimulus-quantities/result.json",
        "stimulus-quantities/almanak_state.db",
        "stimulus-quantities/chain/submission-census.json",
    ):
        item = retained[path]
        assert (bundle / path).read_bytes() == (context.root / item["source"]).read_bytes()
        assert digest((bundle / path).read_bytes()) == item["sha256"]
    assert result["e2e_admission"] == "UNMEASURED"


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_actor_snapshot_sidecars_prevent_completed_assembly(lane, suffix):
    context, cleanup, _ = lane
    quantities = cleanup / "stimulus/quantities"
    quantities.mkdir(parents=True)
    shutil.copyfile(cleanup / "quantities/almanak_state.db", quantities / "almanak_state.db")
    (quantities / ("almanak_state.db" + suffix)).write_bytes(b"uncheckpointed")
    with pytest.raises(ValueError, match="closed database snapshot"):
        assemble(lane)
    assert not (context.root / "bundle/assembly.json").exists()


@pytest.mark.parametrize("name", ["config", "quote", "provisioning"])
def test_actor_bound_input_edit_prevents_completed_export(lane, name):
    context, _, _ = lane
    actor = context.root / "actor"
    actor.mkdir()
    bindings = {}
    for kind in ("config", "quote", "provisioning"):
        raw = canonical({"kind": kind})
        (actor / f"{kind}.json").write_bytes(raw)
        bindings[f"{kind}_sha256"] = digest(raw)
    (actor / "binding.json").write_bytes(canonical({"bindings": bindings}))
    (actor / f"{name}.json").write_bytes(canonical({"edited": True}))
    with pytest.raises(ValueError, match="differs from its recorded binding"):
        assemble(lane)
    assert not (context.root / "bundle/assembly.json").exists()


@pytest.mark.parametrize("value", [None, "", 1, "not-a-digest"])
def test_missing_actor_binding_hash_cannot_disable_export_verification(lane, value):
    context, _, _ = lane
    actor = context.root / "actor"
    actor.mkdir()
    bindings = {f"{name}_sha256": "a" * 64 for name in ("config", "quote", "provisioning")}
    bindings["quote_sha256"] = value
    (actor / "binding.json").write_bytes(canonical({"bindings": bindings}))
    with pytest.raises(ValueError, match="measured SHA-256"):
        assemble(lane)
    assert not (context.root / "bundle/assembly.json").exists()


def test_admission_accepts_frozen_contract_without_execution_checkout(lane):
    from qa_lab.quant_admission import validate_audit_decision
    from tests.unit.scripts.test_quant_admission import _decision

    context, _, repo = lane
    assemble(lane)
    bundle = context.root / "bundle"
    _decision(bundle)
    shutil.rmtree(repo)
    contract = load_json(bundle / "lifecycle-contract.json")
    result = validate_audit_decision(
        bundle, contract=contract, contract_sha256=digest((bundle / "lifecycle-contract.json").read_bytes())
    )
    assert result["status"] == "OFFICIAL"


@pytest.mark.parametrize("mutation", ["root_contract", "prepared_contract", "missing_preparation", "missing_card"])
def test_refreshed_audit_cannot_drop_frozen_obligations(lane, mutation):
    from qa_lab.quant_admission import validate_audit_decision
    from tests.unit.scripts.test_quant_admission import _decision

    context, _, _ = lane
    assemble(lane)
    bundle = context.root / "bundle"
    if mutation in {"root_contract", "prepared_contract"}:
        path = bundle / "lifecycle-contract.json"
        contract = load_json(path)
        contract.pop("residual_policy")
        path.write_bytes(canonical(contract))
        if mutation == "prepared_contract":
            shutil.copyfile(path, bundle / "preparation/lifecycle-contract.json")
    elif mutation == "missing_preparation":
        shutil.rmtree(bundle / "preparation")
    else:
        (bundle / "preparation/card.json").unlink()
    provenance = load_json(bundle / "git.json")
    provenance.update(dirty=False, sdk_version="0.0-test", branch="test")
    (bundle / "git.json").write_bytes(canonical(provenance))
    _decision(bundle)
    contract = load_json(bundle / "lifecycle-contract.json")
    with pytest.raises(ValueError, match="frozen preparation|Frozen preparation|owned preparation|owned regular"):
        validate_audit_decision(
            bundle, contract=contract, contract_sha256=digest((bundle / "lifecycle-contract.json").read_bytes())
        )

    from qa_lab import qa_coverage

    store = context.root / "sealed-store"
    with pytest.raises(ValueError, match="frozen preparation|Frozen preparation|owned preparation|owned regular"):
        qa_coverage.seal_bundle(
            bundle=bundle,
            store=store,
            catalog_path=REPO / "qa_lab/docs/catalog/v1/cells.yaml",
            cell_id="lp.uniswap_v3.arbitrum.complex.anvil.eoa",
            network="anvil",
            exec_path="eoa",
            lane="daily",
            run_id="mutated-preparation",
        )
    assert not list((store / "quant").rglob("manifest.json"))


def test_local_startup_and_shutdown_proofs_keep_their_source_hashes(lane):
    context, _, _ = lane
    startup = context.root / "startup-observations"
    startup.mkdir()
    (startup / "result.json").write_bytes(canonical({"status": "OBSERVED", "execution_status": "UNMEASURED"}))
    for name in ("fork-custody-enabled.json", "fork-release.json", "fork-release-observed.json", "fork-shutdown.json"):
        (context.root / name).write_bytes(canonical({"scope": "test-export", "artifact": name}))
    result = assemble(lane)
    inventory = {item["path"]: item for item in result["artifacts"]}
    assert inventory["startup/result.json"]["source"] == "startup-observations/result.json"
    assert "startup/" not in result["missing_evidence"]
    for name in ("startup/result.json", "fork-shutdown.json", "fork-release-observed.json"):
        item = inventory[name]
        assert digest((context.root / item["source"]).read_bytes()) == item["source_sha256"]
        assert digest((context.root / "bundle" / name).read_bytes()) == item["sha256"]
    assert result["status"] == "INCOMPLETE"
    assert result["e2e_admission"] == "UNMEASURED"


def test_ambiguous_startup_sources_cannot_be_selected_by_precedence(lane):
    context, _, _ = lane
    (context.root / "startup").mkdir()
    (context.root / "startup-observations").mkdir()
    with pytest.raises(ValueError, match="Ambiguous startup"):
        assemble(lane)
    assert not (context.root / "bundle/assembly.json").exists()
