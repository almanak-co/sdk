"""The QA ledger correction must be a reproducible migration, not a hand-cleanup.

Before this module existed, the Aug 8-11 producer-attested quant seals and the
three Aug 17 poisoned seals had been invalidated by a hand-run loop on one
laptop.  The only surviving evidence was that laptop's store, so nobody could
review WHICH records were corrected, re-derive WHY, or reproduce the correction
on a second store.  These tests bind the committed manifest to the behaviour
the migration must have: projection rebuild excludes the named records, the
run bytes survive as a forensic record, and re-applying is a no-op.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_DIR = REPO_ROOT / "qa_lab"
MANIFEST_PATH = REPO_ROOT / "qa_lab/docs/catalog/v1/experiment-invalidations.json"
COMMIT = "a" * 40
SDK = {
    "commit": COMMIT,
    "branch": "test/invalidate",
    "dirty": False,
    "sdk_version": "0.0-test",
    "source": "executing-worktree",
}
# The quant surface now refuses an unadmitted seal (85eccd216). The legacy
# poisoned records predate that gate, so a fixture re-creating one must supply
# an admission; the migration matches on (surface, run_id), never on admission.
OFFICIAL_ADMISSION = {
    "status": "OFFICIAL",
    "evidence_set_sha256": "e" * 64,
    "audit_decision_sha256": "f" * 64,
}
# The three run_ids the Phase A plan of record names as poisoned.
PLAN_NAMED_POISON = {
    "20260817-0218-aave-supply-base",
    "20260817-0218-looping-arb",
    "20260817-0347-looping-arb-rerun",
}


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, SCRIPT_DIR / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def invalidate():
    return _load("qa_invalidate_test", "qa_invalidate.py")


@pytest.fixture
def history(invalidate):
    return invalidate.load_history_module()


@pytest.fixture
def manifest(invalidate):
    return invalidate.load_manifest(MANIFEST_PATH)


def _seal(history, store: Path, *, surface: str, run_id: str, verdict: str, hour: int) -> dict:
    run_dir = store / "runs" / "2026" / "08" / "08" / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "result.txt").write_text(f"{verdict}\n", encoding="utf-8")
    (run_dir / "manifest.json").write_text(json.dumps({"run_id": run_id, "sdk": SDK}) + "\n", encoding="utf-8")
    stamp = f"2026-08-08T{hour:02d}:00:00Z"
    return history.append_experiment(
        store=store,
        surface=surface,
        run_id=run_id,
        run_dir=run_dir,
        manifest_path=run_dir / "manifest.json",
        sdk=SDK,
        cell_verdicts={"lending.aave_v3.base.simple.mainnet.eoa": verdict},
        started_at=f"2026-08-08T{hour - 1:02d}:00:00Z",
        completed_at=stamp,
        sealed_at=stamp,
        admission=OFFICIAL_ADMISSION,
    )


def test_manifest_names_every_poisoned_record_with_a_reviewable_finding(manifest) -> None:
    run_ids = {entry["run_id"] for entry in manifest["entries"]}
    assert PLAN_NAMED_POISON <= run_ids
    poison = [entry for entry in manifest["entries"] if entry["run_id"] in PLAN_NAMED_POISON]
    assert {entry["group"] for entry in poison} == {"aug17-poisoned-seals"}

    # The Aug 8-11 producer-attested seals: the whole coverage6 / coverage6r /
    # short6r sweep, all on the quant surface.
    legacy = [entry for entry in manifest["entries"] if entry["group"] == "pre-admission-quant-seals"]
    assert len(legacy) == 18
    assert {entry["surface"] for entry in manifest["entries"]} == {"quant"}
    assert {entry["run_id"][:8] for entry in legacy} == {"20260808", "20260809", "20260811"}

    # Every entry carries a reason a reviewer can argue with, not just an id.
    for entry in manifest["entries"]:
        assert entry["reason_codes"]
        assert entry["operator"]
        assert len(entry["finding"]) > 40


def test_rebuild_excludes_invalidated_records_and_keeps_the_forensic_bytes(invalidate, history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    poisoned = _seal(history, store, surface="quant", run_id="20260817-0218-looping-arb", verdict="FAIL", hour=1)
    clean = _seal(history, store, surface="quant", run_id="20260819-0100-clean", verdict="PASS", hour=3)

    before = json.loads((store / "index" / history.HISTORY_PROJECTION_NAME).read_text())
    assert before["active_experiment_count"] == 2
    assert {row["run_id"] for row in before["cells"]["lending.aave_v3.base.simple.mainnet.eoa"]["runs"]} == {
        poisoned["run_id"],
        clean["run_id"],
    }

    manifest = invalidate.load_manifest(MANIFEST_PATH)
    summary = invalidate.apply_invalidations(
        store=store,
        manifest=manifest,
        sdk=SDK,
        sealed_at="2026-08-19T00:00:00Z",
        dry_run=False,
    )

    assert summary["counts"]["applied"] == 1
    # The other 20 manifest entries simply are not in this store.
    assert summary["counts"]["absent"] == 20
    assert summary["counts"]["pending"] == 0

    after = json.loads((store / "index" / history.HISTORY_PROJECTION_NAME).read_text())
    runs = after["cells"]["lending.aave_v3.base.simple.mainnet.eoa"]["runs"]
    assert [row["run_id"] for row in runs] == [clean["run_id"]]
    assert after["active_experiment_count"] == 1
    assert after["invalidation_count"] == 1

    # Append-only: the poisoned experiment record and its bytes are still there.
    assert history.verify_history(store)["status"] == "PASS"
    assert (store / poisoned["store_path"] / "result.txt").read_text() == "FAIL\n"
    ledger = (store / "index" / history.HISTORY_LEDGER_NAME).read_text().splitlines()
    assert json.loads(ledger[0])["record_sha256"] == poisoned["record_sha256"]


def test_negative_control_stripping_the_invalidation_makes_the_record_reappear(
    invalidate, history, tmp_path: Path
) -> None:
    """Prove the exclusion assertion can actually fail.

    ``cell_latest`` in the live store is literally ``{}``, so "no invalidated
    id appears in the projection" passes on an empty projection and proves
    nothing. This drives the discriminator from the other side: rebuild the
    SAME fixture with the invalidation line removed from the ledger and assert
    the record comes back. If ``rebuild_projections`` ever stopped honouring
    invalidations, the two halves would agree and this test would fail.
    """
    store = tmp_path / "qa"
    poisoned = _seal(history, store, surface="quant", run_id="20260817-0218-looping-arb", verdict="FAIL", hour=1)
    cell = "lending.aave_v3.base.simple.mainnet.eoa"
    ledger_path = store / "index" / history.HISTORY_LEDGER_NAME
    manifest = invalidate.load_manifest(MANIFEST_PATH)

    invalidate.apply_invalidations(
        store=store, manifest=manifest, sdk=SDK, sealed_at="2026-08-19T00:00:00Z", dry_run=False
    )

    # Excluded: the cell has no runs left at all, so the projection drops it.
    excluded = json.loads((store / "index" / history.HISTORY_PROJECTION_NAME).read_text())
    assert cell not in excluded["cells"]
    assert excluded["active_experiment_count"] == 0

    # Negative control: same store, invalidation line removed, rebuild.
    lines = ledger_path.read_text(encoding="utf-8").splitlines()
    assert json.loads(lines[-1])["record_kind"] == "invalidation"
    ledger_path.write_text("\n".join(lines[:-1]) + "\n", encoding="utf-8")

    restored = history.rebuild_projections(store)

    assert cell in restored["cells"]
    assert [row["run_id"] for row in restored["cells"][cell]["runs"]] == [poisoned["run_id"]]
    assert restored["active_experiment_count"] == 1


def test_seq_13_accounting_record_is_explicitly_dispositioned_not_silently_skipped(manifest) -> None:
    """The one Aug 8-11 record the correction did not invalidate.

    It sits in the window but on the accounting surface, so it needs
    a stated disposition with evidence rather than an unexplained absence.
    """
    run_id = "20260810-1419z-accounting-aave_v3-complex-arbitrum-8a67bdc"
    assert run_id not in {entry["run_id"] for entry in manifest["entries"]}

    excluded = {row["run_id"]: row for row in manifest["considered_and_excluded"]}
    assert run_id in excluded, "a record inside the correction window must be dispositioned, not skipped"
    row = excluded[run_id]
    assert row["disposition"] == "not-poison"
    assert row["surface"] == "accounting"
    # The disposition has to be argued, not asserted.
    assert len(row["evidence"]) >= 3
    assert row["residual_risk"]


def test_a_record_cannot_be_both_invalidated_and_excluded(invalidate, manifest, tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    entry = manifest["entries"][0]
    contradictory = {
        **manifest,
        "considered_and_excluded": [
            {
                "surface": entry["surface"],
                "run_id": entry["run_id"],
                "disposition": "not-poison",
                "evidence": ["contradicts the entry list"],
            }
        ],
    }
    path.write_text(json.dumps(contradictory), encoding="utf-8")

    with pytest.raises(ValueError, match="both invalidates and excludes"):
        invalidate.load_manifest(path)


def test_an_exclusion_without_evidence_is_rejected(invalidate, manifest, tmp_path: Path) -> None:
    """An unevidenced exclusion is the silent skip wearing a label."""
    path = tmp_path / "manifest.json"
    unevidenced = {
        **manifest,
        "considered_and_excluded": [
            {"surface": "quant", "run_id": "some-run", "disposition": "not-poison", "evidence": []}
        ],
    }
    path.write_text(json.dumps(unevidenced), encoding="utf-8")

    with pytest.raises(ValueError, match="requires evidence"):
        invalidate.load_manifest(path)


def test_applying_twice_is_a_no_op_so_a_hand_corrected_store_converges(invalidate, history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    _seal(history, store, surface="quant", run_id="20260817-0218-looping-arb", verdict="FAIL", hour=1)
    manifest = invalidate.load_manifest(MANIFEST_PATH)

    invalidate.apply_invalidations(
        store=store, manifest=manifest, sdk=SDK, sealed_at="2026-08-19T00:00:00Z", dry_run=False
    )
    ledger_after_first = (store / "index" / history.HISTORY_LEDGER_NAME).read_bytes()

    second = invalidate.apply_invalidations(
        store=store, manifest=manifest, sdk=SDK, sealed_at="2026-08-19T01:00:00Z", dry_run=False
    )

    assert second["counts"]["applied"] == 0
    assert second["counts"]["already_invalidated"] == 1
    assert (store / "index" / history.HISTORY_LEDGER_NAME).read_bytes() == ledger_after_first


def test_dry_run_is_the_default_and_writes_nothing(invalidate, history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    _seal(history, store, surface="quant", run_id="20260817-0218-looping-arb", verdict="FAIL", hour=1)
    manifest = invalidate.load_manifest(MANIFEST_PATH)
    before = (store / "index" / history.HISTORY_LEDGER_NAME).read_bytes()

    summary = invalidate.apply_invalidations(store=store, manifest=manifest, sdk=SDK)

    assert summary["dry_run"] is True
    assert summary["counts"]["pending"] == 1
    assert summary["counts"]["applied"] == 0
    assert (store / "index" / history.HISTORY_LEDGER_NAME).read_bytes() == before


def test_require_all_fails_closed_when_the_store_is_missing_a_named_record(invalidate, history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    _seal(history, store, surface="quant", run_id="20260817-0218-looping-arb", verdict="FAIL", hour=1)
    manifest = invalidate.load_manifest(MANIFEST_PATH)

    with pytest.raises(ValueError, match="absent from this store"):
        invalidate.apply_invalidations(store=store, manifest=manifest, sdk=SDK, require_all=True)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ({"schema_version": 2}, "schema_version=1"),
        ({"evidence_kind": "something-else"}, "evidence_kind="),
        ({"entries": []}, "non-empty entry list"),
        ({"groups": {}}, "non-empty group map"),
    ],
)
def test_manifest_validation_fails_closed(invalidate, manifest, tmp_path: Path, mutation, message) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({**manifest, **mutation}), encoding="utf-8")

    with pytest.raises(ValueError, match=message):
        invalidate.load_manifest(path)


def test_manifest_rejects_a_duplicated_or_unreasoned_entry(invalidate, manifest, tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    duplicated = {**manifest, "entries": [manifest["entries"][0], dict(manifest["entries"][0])]}
    path.write_text(json.dumps(duplicated), encoding="utf-8")
    with pytest.raises(ValueError, match="more than once"):
        invalidate.load_manifest(path)

    unreasoned = {**manifest, "entries": [{**manifest["entries"][0], "reason_codes": []}]}
    path.write_text(json.dumps(unreasoned), encoding="utf-8")
    with pytest.raises(ValueError, match="requires reason_codes"):
        invalidate.load_manifest(path)
