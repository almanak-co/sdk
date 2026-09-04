"""Scientific-history contracts shared by every QA evidence surface."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[3] / "qa_lab" / "qa_history.py"
COMMIT_A = "a" * 40
COMMIT_B = "b" * 40
COMMIT_C = "c" * 40
OFFICIAL_ADMISSION = {
    "status": "OFFICIAL",
    "evidence_set_sha256": "e" * 64,
    "audit_decision_sha256": "f" * 64,
}


@pytest.fixture
def history():
    spec = importlib.util.spec_from_file_location("qa_history_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _sdk(commit: str) -> dict:
    return {
        "commit": commit,
        "branch": "test/history",
        "dirty": False,
        "sdk_version": "9.9.9-test",
        "source": "executing-worktree",
    }


def _experiment(store: Path, *, run_id: str, payload: str, sdk: dict | None = None) -> Path:
    target = store / "runs" / "2026" / "08" / "08" / run_id
    target.mkdir(parents=True)
    (target / "result.txt").write_text(payload, encoding="utf-8")
    (target / "manifest.json").write_text(
        json.dumps({"run_id": run_id, "sdk": sdk or _sdk(COMMIT_A)}) + "\n", encoding="utf-8"
    )
    return target


def _append(history, store: Path, *, run_id: str, verdict: str, commit: str, hour: int) -> dict:
    sdk = _sdk(commit)
    target = _experiment(store, run_id=run_id, payload=f"{verdict}\n", sdk=sdk)
    timestamp = f"2026-08-08T{hour:02d}:00:00Z"
    return history.append_experiment(
        store=store,
        surface="quant",
        run_id=run_id,
        run_dir=target,
        manifest_path=target / "manifest.json",
        sdk=sdk,
        cell_verdicts={"lp.uniswap_v3.arbitrum.simple.mainnet.eoa": verdict},
        started_at=f"2026-08-08T{hour - 1:02d}:00:00Z",
        completed_at=timestamp,
        sealed_at=timestamp,
        catalog_sha256="d" * 64,
        admission=OFFICIAL_ADMISSION,
    )


def test_fail_pass_fail_history_is_append_only_and_reproducible(history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    first = _append(history, store, run_id="night-1", verdict="FAIL", commit=COMMIT_A, hour=1)
    second = _append(history, store, run_id="night-2", verdict="PASS", commit=COMMIT_B, hour=3)
    third = _append(history, store, run_id="night-3", verdict="FAIL", commit=COMMIT_C, hour=5)

    assert first["previous_record_sha256"] is None
    assert second["previous_record_sha256"] == first["record_sha256"]
    assert third["previous_record_sha256"] == second["record_sha256"]

    projection = json.loads((store / "index" / "experiment_history.json").read_text())
    cell = projection["cells"]["lp.uniswap_v3.arbitrum.simple.mainnet.eoa"]
    assert [row["verdict"] for row in cell["runs"]] == ["FAIL", "PASS", "FAIL"]
    assert [row["sdk"]["commit"] for row in cell["runs"]] == [COMMIT_A, COMMIT_B, COMMIT_C]
    assert cell["regressions"] == 1
    assert cell["transitions"] == 2
    assert cell["pass_streak"] == 0
    assert cell["last_pass"]["run_id"] == "night-2"
    assert history.verify_history(store) == {
        "status": "PASS",
        "reproducibility": "VERIFIED",
        "records": 3,
        "cells": 1,
        "artifacts": 6,
        "ledger_sha256": projection["ledger_sha256"],
        "warnings": [],
    }


def test_rejects_unknown_or_dirty_sdk_provenance(history) -> None:
    with pytest.raises(ValueError, match="40-character"):
        history.validate_sdk_provenance(
            {"commit": "unknown", "dirty": False, "sdk_version": "1.0", "source": "executing-worktree"}
        )
    with pytest.raises(ValueError, match="dirty=false"):
        history.validate_sdk_provenance(
            {"commit": COMMIT_A, "dirty": True, "sdk_version": "1.0", "source": "executing-worktree"}
        )
    with pytest.raises(ValueError, match="sdk_version"):
        history.provenance_from_bundle_git({"commit": COMMIT_A, "dirty": False})


def test_artifact_mutation_is_detected(history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    _append(history, store, run_id="night-1", verdict="PASS", commit=COMMIT_A, hour=1)
    artifact = store / "runs" / "2026" / "08" / "08" / "night-1" / "result.txt"
    artifact.write_text("rewritten\n", encoding="utf-8")

    with pytest.raises(ValueError, match="artifact set changed"):
        history.verify_history(store)


def test_artifact_addition_is_detected(history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    _append(history, store, run_id="night-1", verdict="PASS", commit=COMMIT_A, hour=1)
    run = store / "runs" / "2026" / "08" / "08" / "night-1"
    (run / "post-seal.txt").write_text("not part of the experiment\n", encoding="utf-8")

    with pytest.raises(ValueError, match="artifact set changed"):
        history.verify_history(store)


def test_future_seals_exclude_sqlite_and_process_sidecars(history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    target = _experiment(store, run_id="night-1", payload="PASS\n")
    for name in ("db.sqlite-shm", "db.sqlite-wal", "gateway.lock", "runner.pid", ".DS_Store"):
        (target / name).write_text("volatile\n", encoding="utf-8")
    cache = target / "source" / "__pycache__"
    cache.mkdir(parents=True)
    (cache / "strategy.cpython-312.pyc").write_bytes(b"derived cache")

    record = history.append_experiment(
        store=store,
        surface="quant",
        run_id="night-1",
        run_dir=target,
        manifest_path=target / "manifest.json",
        sdk=_sdk(COMMIT_A),
        cell_verdicts={"cell": "PASS"},
        completed_at="2026-08-08T01:00:00Z",
        sealed_at="2026-08-08T01:00:00Z",
        admission=OFFICIAL_ADMISSION,
    )

    assert {Path(row["relpath"]).name for row in record["artifacts"]} == {"manifest.json", "result.txt"}
    assert history.verify_history(store)["reproducibility"] == "VERIFIED"


def test_missing_or_changed_legacy_sidecars_are_explicit_warning(history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    record = _append(history, store, run_id="night-1", verdict="PASS", commit=COMMIT_A, hour=1)
    run = store / record["store_path"]
    shm = run / "db.sqlite-shm"
    wal = run / "db.sqlite-wal"
    shm.write_bytes(b"legacy shared memory")
    wal.write_bytes(b"")
    legacy_artifacts = sorted(
        [
            *record["artifacts"],
            {
                "relpath": shm.relative_to(store).as_posix(),
                "bytes": shm.stat().st_size,
                "sha256": history._sha256_file(shm),
            },
            {
                "relpath": wal.relative_to(store).as_posix(),
                "bytes": wal.stat().st_size,
                "sha256": history._sha256_file(wal),
            },
        ],
        key=lambda row: row["relpath"],
    )
    record["artifacts"] = legacy_artifacts
    record["artifact_set_sha256"] = history._sha256_bytes(history._canonical_bytes(legacy_artifacts))
    record["record_sha256"] = history._record_digest(record)
    ledger = store / "index" / "experiment_runs.jsonl"
    ledger.write_text(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n", encoding="utf-8")

    shm.unlink()
    wal.write_bytes(b"changed after seal")
    result = history.verify_history(store)

    assert result["status"] == "PASS_WITH_WARNINGS"
    assert result["reproducibility"] == "LEGACY_VOLATILE_WARNING"
    assert result["artifacts"] == 2
    assert result["warnings"] == [
        {
            "code": history.LEGACY_VOLATILE_WARNING,
            "surface": "quant",
            "run_id": "night-1",
            "artifacts": [shm.relative_to(store).as_posix(), wal.relative_to(store).as_posix()],
            "message": (
                "Legacy seal recorded mutable SQLite/process sidecars; stable artifacts verified, "
                "but volatile sidecars are excluded from reproducibility claims."
            ),
        }
    ]


def test_missing_stable_artifact_in_legacy_record_is_rejected(history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    record = _append(history, store, run_id="night-1", verdict="PASS", commit=COMMIT_A, hour=1)
    run = store / record["store_path"]
    (run / "result.txt").unlink()

    with pytest.raises(ValueError, match="artifact set changed"):
        history.verify_history(store)


def test_ledger_reordering_or_rewrite_is_detected(history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    _append(history, store, run_id="night-1", verdict="PASS", commit=COMMIT_A, hour=1)
    _append(history, store, run_id="night-2", verdict="FAIL", commit=COMMIT_B, hour=3)
    ledger = store / "index" / "experiment_runs.jsonl"
    lines = ledger.read_text(encoding="utf-8").splitlines()
    ledger.write_text("\n".join(reversed(lines)) + "\n", encoding="utf-8")

    with pytest.raises(ValueError, match="sequence break"):
        history.read_history(store)


def test_duplicate_surface_run_is_rejected(history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    target = _experiment(store, run_id="night-1", payload="PASS\n")
    kwargs = {
        "store": store,
        "surface": "quant",
        "run_id": "night-1",
        "run_dir": target,
        "manifest_path": target / "manifest.json",
        "sdk": _sdk(COMMIT_A),
        "cell_verdicts": {"cell": "PASS"},
        "completed_at": "2026-08-08T01:00:00Z",
        "sealed_at": "2026-08-08T01:00:00Z",
        "admission": OFFICIAL_ADMISSION,
    }
    history.append_experiment(**kwargs)

    with pytest.raises(FileExistsError, match="already contains"):
        history.append_experiment(**kwargs)


def test_rejects_manifest_identity_or_non_utc_order(history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    target = _experiment(store, run_id="night-1", payload="PASS\n", sdk=_sdk(COMMIT_B))
    kwargs = {
        "store": store,
        "surface": "quant",
        "run_id": "night-1",
        "run_dir": target,
        "manifest_path": target / "manifest.json",
        "sdk": _sdk(COMMIT_A),
        "cell_verdicts": {"cell": "PASS"},
        "completed_at": "2026-08-08T01:00:00Z",
        "sealed_at": "2026-08-08T01:00:00Z",
        "admission": OFFICIAL_ADMISSION,
    }
    with pytest.raises(ValueError, match="does not match its sealed manifest"):
        history.append_experiment(**kwargs)

    kwargs["sdk"] = _sdk(COMMIT_B)
    kwargs["completed_at"] = "2026-08-08T02:00:00Z"
    with pytest.raises(ValueError, match="after sealed_at"):
        history.append_experiment(**kwargs)


def test_invalidation_is_append_only_and_excluded_from_projections(history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    passed = _append(history, store, run_id="night-pass", verdict="PASS", commit=COMMIT_A, hour=1)
    failed = _append(history, store, run_id="night-fail", verdict="FAIL", commit=COMMIT_B, hour=3)

    invalidation = history.append_invalidation(
        store=store,
        invalidates_record_sha256=failed["record_sha256"],
        reason_codes=["audit_overturned", "producer_classified"],
        operator="qa-framework-owner",
        sdk=_sdk(COMMIT_C),
        sealed_at="2026-08-08T04:00:00Z",
    )

    records = history.read_history(store)
    assert [record.get("record_kind", "experiment") for record in records] == [
        "experiment",
        "experiment",
        "invalidation",
    ]
    assert invalidation["previous_record_sha256"] == failed["record_sha256"]
    projection = json.loads((store / "index" / history.HISTORY_PROJECTION_NAME).read_text())
    cell = projection["cells"]["lp.uniswap_v3.arbitrum.simple.mainnet.eoa"]
    assert [row["run_id"] for row in cell["runs"]] == ["night-pass"]
    assert cell["last_pass"]["record_sha256"] == passed["record_sha256"]
    assert projection["record_count"] == 3
    assert projection["active_experiment_count"] == 1
    assert projection["invalidation_count"] == 1
    assert history.verify_history(store)["status"] == "PASS"


def test_invalidation_cannot_target_unknown_or_already_invalidated_record(history, tmp_path: Path) -> None:
    store = tmp_path / "qa"
    record = _append(history, store, run_id="night-1", verdict="PASS", commit=COMMIT_A, hour=1)
    kwargs = {
        "store": store,
        "invalidates_record_sha256": record["record_sha256"],
        "reason_codes": ["audit_overturned"],
        "operator": "qa-framework-owner",
        "sdk": _sdk(COMMIT_B),
        "sealed_at": "2026-08-08T02:00:00Z",
    }
    history.append_invalidation(**kwargs)

    with pytest.raises(FileExistsError, match="already invalidated"):
        history.append_invalidation(**kwargs)
    with pytest.raises(ValueError, match="not an existing experiment"):
        history.append_invalidation(**{**kwargs, "invalidates_record_sha256": "0" * 64})


def _append_unadmitted(history, store: Path, *, surface: str, run_id: str, verdict: str, hour: int) -> dict:
    """Seal a record with no OFFICIAL admission, as every non-quant surface does."""
    sdk = _sdk(COMMIT_A)
    target = _experiment(store, run_id=run_id, payload=f"{verdict}\n", sdk=sdk)
    timestamp = f"2026-08-08T{hour:02d}:00:00Z"
    return history.append_experiment(
        store=store,
        surface=surface,
        run_id=run_id,
        run_dir=target,
        manifest_path=target / "manifest.json",
        sdk=sdk,
        cell_verdicts={"lp.uniswap_v3.arbitrum.simple.mainnet.eoa": verdict},
        started_at=f"2026-08-08T{hour - 1:02d}:00:00Z",
        completed_at=timestamp,
        sealed_at=timestamp,
        admission=None,
    )


def test_unadmitted_unverified_and_partial_runs_are_forensic_and_never_graded(history, tmp_path: Path) -> None:
    """A grade implies an admission. Neither UNVERIFIED nor PARTIAL ever had one.

    Before this contract, an unadmitted PARTIAL row sat on the History page
    under the banner "Scientific record", broke the PASS streak, and was
    counted as a regression -- three grades applied to an observation that was
    never admitted as official history.
    """
    store = tmp_path / "qa"
    _append_unadmitted(history, store, surface="intent", run_id="obs-1", verdict="PASS", hour=1)
    _append_unadmitted(history, store, surface="intent", run_id="obs-2", verdict="PARTIAL", hour=3)
    _append_unadmitted(history, store, surface="intent", run_id="obs-3", verdict="UNVERIFIED", hour=5)

    projection = json.loads((store / "index" / history.HISTORY_PROJECTION_NAME).read_text())
    cell = projection["cells"]["lp.uniswap_v3.arbitrum.simple.mainnet.eoa"]

    # Every row is still listed: the ledger is the forensic record.
    assert [row["run_id"] for row in cell["runs"]] == ["obs-1", "obs-2", "obs-3"]
    assert [row["record_class"] for row in cell["runs"]] == [
        history.SCIENTIFIC_CLASS,
        history.FORENSIC_CLASS,
        history.FORENSIC_CLASS,
    ]
    # ...and none of them is graded.
    assert cell["graded_run_count"] == 1
    assert cell["forensic_run_count"] == 2
    assert cell["regressions"] == 0
    assert cell["transitions"] == 0
    assert cell["pass_streak"] == 1
    assert cell["last_pass"]["run_id"] == "obs-1"
    assert projection["forensic_run_count"] == 2
    assert projection["graded_run_count"] == 1


def test_official_verdicts_and_admitted_records_stay_scientific(history, tmp_path: Path) -> None:
    """The filter is narrow on purpose: only unadmitted non-official verdicts move."""
    store = tmp_path / "qa"
    # Unadmitted but PASS/FAIL: the surface's own seal contract constrains it.
    fail_row = _append_unadmitted(history, store, surface="intent", run_id="graded-1", verdict="FAIL", hour=1)
    # Admitted OFFICIAL: scientific whatever the verdict shape.
    admitted = history.append_experiment(
        store=store,
        surface="quant",
        run_id="graded-2",
        run_dir=_experiment(store, run_id="graded-2", payload="PASS\n"),
        manifest_path=store / "runs" / "2026" / "08" / "08" / "graded-2" / "manifest.json",
        sdk=_sdk(COMMIT_A),
        cell_verdicts={"lp.uniswap_v3.arbitrum.simple.mainnet.eoa": "PASS"},
        completed_at="2026-08-08T04:00:00Z",
        sealed_at="2026-08-08T04:00:00Z",
        admission=OFFICIAL_ADMISSION,
    )

    assert history.run_record_class(fail_row, "FAIL") == history.SCIENTIFIC_CLASS
    assert history.run_record_class(admitted, "PASS") == history.SCIENTIFIC_CLASS
    assert history.is_admitted_official(admitted) is True
    assert history.is_admitted_official(fail_row) is False

    projection = json.loads((store / "index" / history.HISTORY_PROJECTION_NAME).read_text())
    cell = projection["cells"]["lp.uniswap_v3.arbitrum.simple.mainnet.eoa"]
    assert cell["forensic_run_count"] == 0
    assert cell["graded_run_count"] == 2
    assert cell["transitions"] == 1


def test_projection_failure_after_fsync_keeps_the_ledger_line_durable(
    history, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VIB-6707: the append commits at the fsync, not at the projection rebuild.

    A projection failure must be reported as a distinct post-commit error so no
    caller can read it as "the ledger append did not happen" and then quarantine
    the run directory the durable record points at.
    """
    store = tmp_path / "qa"
    _append(history, store, run_id="night-1", verdict="PASS", commit=COMMIT_A, hour=1)
    ledger = store / "index" / history.HISTORY_LEDGER_NAME
    before = ledger.read_bytes()

    real_rebuild = history.rebuild_projections

    def explode(target_store, records=None):
        raise OSError("injected-projection-failure")

    monkeypatch.setattr(history, "rebuild_projections", explode)
    failure: Exception | None = None
    try:
        _append(history, store, run_id="night-2", verdict="FAIL", commit=COMMIT_B, hour=3)
    except Exception as exc:  # noqa: BLE001 - durability is asserted before the type
        failure = exc
    assert failure is not None

    # The record IS committed: the ledger grew by one line and reads back.
    lines = ledger.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    assert ledger.read_bytes().startswith(before)
    assert json.loads(lines[1])["run_id"] == "night-2"
    assert history.read_history(store)[-1]["run_id"] == "night-2"

    # The failure is typed and carries the committed record, so no caller can
    # read it as "the append did not happen".
    assert isinstance(failure, history.ProjectionRebuildError)
    assert failure.record["run_id"] == "night-2"

    # Rebuilding is the idempotent retry the caller owns.
    monkeypatch.setattr(history, "rebuild_projections", real_rebuild)
    projection = history.rebuild_projections(store)
    assert projection["active_experiment_count"] == 2
    assert history.verify_history(store)["status"] == "PASS"

    # Negative control: a pre-fsync failure is still an ordinary failed append
    # that leaves the ledger byte-identical, so the assertions above discriminate.
    grown = ledger.read_bytes()
    with pytest.raises(ValueError, match="Official quant cell verdicts require a validated admission"):
        history.append_experiment(
            store=store,
            surface="quant",
            run_id="night-3",
            run_dir=_experiment(store, run_id="night-3", payload="PASS\n"),
            manifest_path=store / "runs" / "2026" / "08" / "08" / "night-3" / "manifest.json",
            sdk=_sdk(COMMIT_C),
            cell_verdicts={"cell": "PASS"},
            completed_at="2026-08-08T06:00:00Z",
            sealed_at="2026-08-08T06:00:00Z",
            admission=None,
        )
    assert ledger.read_bytes() == grown


def test_invalidation_projection_failure_also_keeps_its_ledger_line(
    history, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """VIB-6707: append_invalidation carries the identical post-commit contract."""
    store = tmp_path / "qa"
    record = _append(history, store, run_id="night-1", verdict="FAIL", commit=COMMIT_A, hour=1)
    ledger = store / "index" / history.HISTORY_LEDGER_NAME

    def explode(target_store, records=None):
        raise OSError("injected-projection-failure")

    monkeypatch.setattr(history, "rebuild_projections", explode)
    failure: Exception | None = None
    try:
        history.append_invalidation(
            store=store,
            invalidates_record_sha256=record["record_sha256"],
            reason_codes=["audit_overturned"],
            operator="qa-framework-owner",
            sdk=_sdk(COMMIT_B),
            sealed_at="2026-08-08T04:00:00Z",
        )
    except Exception as exc:  # noqa: BLE001 - durability is asserted before the type
        failure = exc
    assert failure is not None

    assert len(ledger.read_text(encoding="utf-8").splitlines()) == 2
    records = history.read_history(store)
    assert records[-1]["record_kind"] == "invalidation"
    assert records[-1]["invalidates_record_sha256"] == record["record_sha256"]
    assert isinstance(failure, history.ProjectionRebuildError)


def test_an_admission_governed_surface_cannot_grade_a_cell_without_an_admission(history, tmp_path: Path) -> None:
    """The gate lives in the ledger, not only in each sealer.

    ``qa_ax.seal_ax_bundle`` derives its own verdict, so in normal use it always
    supplies an admission. This asserts the rule is enforced where the record is
    actually written: a future caller -- or a repair script -- cannot append a
    graded ax PASS by simply not passing one.
    """
    store = tmp_path / "qa"
    with pytest.raises(ValueError, match="Official ax cell verdicts require a validated admission"):
        _append_unadmitted(history, store, surface="ax", run_id="ax-1", verdict="PASS", hour=1)
    with pytest.raises(ValueError, match="Official ax cell verdicts require a validated admission"):
        _append_unadmitted(history, store, surface="ax", run_id="ax-2", verdict="FAIL", hour=2)

    # Nothing reached the chain, and an ungraded ax record still seals.
    assert not (store / "index" / "experiment_runs.jsonl").exists()
    record = _append_unadmitted(history, store, surface="ax", run_id="ax-3", verdict="UNVERIFIED", hour=3)
    assert record["admission"] is None

    # An unmigrated surface is unaffected: this gate does not silently widen.
    assert _append_unadmitted(history, store, surface="intent", run_id="in-1", verdict="PASS", hour=4)
