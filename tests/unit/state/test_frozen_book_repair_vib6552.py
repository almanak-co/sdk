"""VIB-6552 — offline frozen-book ``position_reference`` repair.

The VIB-6346 join repair fires only on a registry write
(:meth:`SQLiteStore.save_ledger_and_registry_atomic`), and the registry
UPSERT's monotone status guard makes ``closed`` terminal — so a position that
settled before that fix shipped never receives another registry write and its
Phase-1 ``source="legacy"`` accounting rows are never revisited. No previously
shipped SDK surface could trigger the repair on an existing book
(``ax positions reconcile`` needs a live chain; the only ``--db`` CLIs never
reach this path).

:meth:`SQLiteStore.repair_frozen_position_references` is the operator-invoked
half, exposed as ``almanak strat repair-position-references``. These tests
score it against the committed frozen mainnet capture
``tests/fixtures/accounting/perp/vib6346_gmx_arb_mainnet_prefix_unrepaired.sqlite``
(bundle ``20260804-2310-gmxrt-vib6522-5513``, AUDIT_CONFIRMED), which still
reproduces the defect as committed — the negative control below pins that.

The fault-injection shapes mirror the independent UAT card
``docs/internal/uat-cards/VIB-6346.md`` D3.F1/F3/F4/F5, whose ``MIGRATION_CMD``
slot is this command's acceptance instrument: on those injected DBs the repair
must complete (exit 0 at the CLI), never mask the injected defect, and never
crash.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
from pathlib import Path
from typing import Any

from click.testing import CliRunner

from almanak.framework.accounting.accountant_test import run_against_sqlite
from almanak.framework.cli.repair_position_references import repair_position_references_cmd
from almanak.framework.state.backends.sqlite import (
    SQLiteConfig,
    SQLiteStore,
    _claim_backup_path,
    _frozen_row_integrity_anomaly,
    _wal_sidecar_nonempty,
)

_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "fixtures"
    / "accounting"
    / "perp"
    / "vib6346_gmx_arb_mainnet_prefix_unrepaired.sqlite"
)
_PHID = "0x4aa383f59c6dd6ceacf3180ffe3b7574d852af5ce0d329e6bb2b1fc1d1f3a715"
_DEPLOYMENT = "deployment:926e7ed624d4"
_SYNTHETIC_ID = "0x6346deadbeef6346deadbeef6346deadbeef6346deadbeef6346deadbeef6346"
_SYNTHETIC_TX = "0x6346cafebabe6346cafebabe6346cafebabe6346cafebabe6346cafebabe6346"


def _copy_fixture(tmp_path: Path) -> Path:
    """Copy the frozen bundle so nothing can mutate the committed fixture (VIB-6548)."""
    assert _FIXTURE.is_file(), f"VIB-6346 fixture missing: {_FIXTURE}"
    target = tmp_path / "book.sqlite"
    shutil.copyfile(_FIXTURE, target)
    return target


def _repair(db: Path, **kwargs: Any) -> Any:
    return SQLiteStore(SQLiteConfig(db_path=str(db))).repair_frozen_position_references(**kwargs)


def _cell22(db: Path) -> Any:
    report = run_against_sqlite(db, primitive="perp", strict_lifecycle=False)
    cells = [c for c in report.cells if c.cell_id == "L5_22"]
    assert len(cells) == 1
    return cells[0]


def _event_references(db: Path) -> dict[str, dict[str, Any] | None]:
    """payload_json's position_reference per OPEN/CLOSE event id."""
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        return {
            r["id"]: json.loads(r["payload_json"]).get("position_reference")
            for r in conn.execute(
                "SELECT id, payload_json FROM accounting_events WHERE event_type IN ('PERP_OPEN', 'PERP_CLOSE')"
            )
        }
    finally:
        conn.close()


def _all_rows(db: Path) -> list[dict[str, Any]]:
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        return [dict(r) for r in conn.execute("SELECT * FROM accounting_events ORDER BY id")]
    finally:
        conn.close()


# =============================================================================
# The committed fixture: negative control, then the flip
# =============================================================================


def test_negative_control_fixture_still_fails_l5_22_unrepaired(tmp_path: Path) -> None:
    """Liveness guard: if this passes, every positive test below is vacuous."""
    db = _copy_fixture(tmp_path)
    cell = _cell22(db)
    assert cell.status == "FAIL", f"fixture no longer reproduces the defect: {cell.diagnostic}"
    assert "inverse orphan" in cell.diagnostic
    assert _PHID in cell.diagnostic


def test_repair_flips_l5_22_fail_to_pass_and_moves_no_other_cell(tmp_path: Path) -> None:
    """The ticket's acceptance criterion, verbatim.

    On a copy of the real pre-fix mainnet capture the operator command moves
    L5_22 FAIL → PASS with no other cell moving.
    """
    db = _copy_fixture(tmp_path)
    before = {c.cell_id: c.status for c in run_against_sqlite(db, primitive="perp", strict_lifecycle=False).cells}

    result = _repair(db)
    assert result.written is True
    assert result.repaired_events == 2  # PERP_OPEN + PERP_CLOSE
    assert result.skipped_rows == 0
    assert result.anomalies == []

    after = {c.cell_id: c.status for c in run_against_sqlite(db, primitive="perp", strict_lifecycle=False).cells}
    moved = {k: (before[k], after[k]) for k in before if before[k] != after.get(k)}
    assert moved == {"L5_22": ("FAIL", "PASS")}, f"only L5_22 may move: {moved}"
    cell = _cell22(db)
    assert "zero orphans on either side" in cell.diagnostic


def test_repair_touches_no_payload_key_other_than_position_reference(tmp_path: Path) -> None:
    """The single-writer contract: ``restamp_position_reference`` mutates one key.

    Every other payload key, and every non-payload column, must be
    byte-identical — a version restamp here would be the quiet audit-trail
    mutation the version-stamp design exists to prevent.
    """
    db = _copy_fixture(tmp_path)
    before = {r["id"]: r for r in _all_rows(db)}
    _repair(db)
    for row in _all_rows(db):
        prev = before[row["id"]]
        for column, value in row.items():
            if column in ("payload_json", "position_reference"):
                continue
            assert value == prev[column], f"non-payload column {column} changed on {row['id']}"
        old_payload = json.loads(prev["payload_json"])
        new_payload = json.loads(row["payload_json"])
        for key in set(old_payload) | set(new_payload):
            if key == "position_reference":
                continue
            assert new_payload.get(key) == old_payload.get(key), f"payload key {key} changed on {row['id']}"
        # The denormalized column moves in lock-step with the payload key.
        column_ref = json.loads(row["position_reference"]) if row["position_reference"] else None
        assert column_ref == new_payload.get("position_reference"), row["id"]


def test_repaired_events_carry_the_registry_identity(tmp_path: Path) -> None:
    db = _copy_fixture(tmp_path)
    _repair(db)
    for event_id, reference in _event_references(db).items():
        assert reference is not None, event_id
        assert reference["source"] == "registry", event_id
        assert reference["physical_identity_hash"] == _PHID, event_id


# =============================================================================
# Dry-run, idempotency, backup
# =============================================================================


def test_dry_run_reports_real_counts_and_writes_nothing(tmp_path: Path) -> None:
    db = _copy_fixture(tmp_path)
    original = _FIXTURE.read_bytes()

    result = _repair(db, dry_run=True)
    assert result.dry_run is True
    assert result.repaired_events == 2
    assert result.written is False
    assert result.backup_path is None
    assert db.read_bytes() == original, "dry-run must leave the DB byte-identical"
    assert list(tmp_path.glob("*.bak-*")) == []
    assert _cell22(db).status == "FAIL", "dry-run must not repair"


def test_repair_is_idempotent_second_run_writes_nothing(tmp_path: Path) -> None:
    db = _copy_fixture(tmp_path)
    first = _repair(db)
    assert first.repaired_events == 2 and first.written is True

    second = _repair(db)
    assert second.repaired_events == 0
    assert second.written is False
    assert second.backup_path is None
    assert _cell22(db).status == "PASS"


def test_backup_is_created_before_write_and_preserves_the_pre_repair_state(tmp_path: Path) -> None:
    db = _copy_fixture(tmp_path)
    result = _repair(db)
    assert result.backup_path is not None
    backup = Path(result.backup_path)
    assert backup.is_file()
    for reference in _event_references(backup).values():
        assert reference is not None and reference["source"] == "legacy", "the backup must hold the PRE-repair book"


# =============================================================================
# Fault-injection shapes (mirror the UAT card's D3.F1/F3/F4/F5)
# =============================================================================


def test_wrong_identity_registry_row_repairs_nothing_and_does_not_crash(tmp_path: Path) -> None:
    """Card D3.F1: every joinable identifier replaced with values found nowhere else.

    The anchors match no accounting event, so there is nothing the repair can
    prove — it must complete cleanly with zero repairs, leaving the injected
    inverse orphan for L5_22 to report.
    """
    db = _copy_fixture(tmp_path)
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE position_registry SET physical_identity_hash=?, closed_tx=?, opened_tx=?, "
        "semantic_grouping_key='synthetic:vib6346:unmatched' WHERE status='closed'",
        (_SYNTHETIC_ID, _SYNTHETIC_TX, _SYNTHETIC_TX),
    )
    conn.commit()
    conn.close()

    result = _repair(db)
    assert result.repaired_events == 0
    assert result.written is False
    cell = _cell22(db)
    assert cell.status == "FAIL"
    assert "inverse orphan" in cell.diagnostic


def test_malformed_position_reference_column_is_skipped_loudly_never_overwritten(tmp_path: Path) -> None:
    """Card D3.F3: corrupt identity payload must stay loud.

    A frozen book where the denormalized column and the canonical payload
    disagree — or where either is unparseable — was touched by something other
    than the writers in ``sqlite.py``. Overwriting it would both destroy the
    forensic evidence and let the repair launder corruption into a green cell.
    The repair must skip the anchored registry row (reported, exit 0), leaving
    the malformed bytes for L5_22 to report.

    Negative control for the integrity precondition: delete the
    ``_frozen_row_integrity_anomaly`` gate and this test fails with the cell
    at PASS and the malformed column healed.
    """
    db = _copy_fixture(tmp_path)
    conn = sqlite3.connect(db)
    conn.execute("UPDATE accounting_events SET position_reference='{not json' WHERE event_type LIKE '%CLOSE%'")
    conn.commit()
    conn.close()

    result = _repair(db)
    assert result.repaired_events == 0
    assert result.written is False
    assert result.skipped_rows == 1
    assert result.skips[0].reason == "integrity_anomaly"
    assert result.skips[0].physical_identity_hash == _PHID
    assert len(result.anomalies) == 1

    conn = sqlite3.connect(db)
    stored = conn.execute(
        "SELECT position_reference FROM accounting_events WHERE event_type LIKE '%CLOSE%'"
    ).fetchone()[0]
    conn.close()
    assert stored == "{not json", "the corrupt bytes must survive as evidence"
    cell = _cell22(db)
    assert cell.status == "FAIL"
    assert "malformed" in cell.diagnostic


def test_column_payload_drift_is_an_integrity_anomaly(tmp_path: Path) -> None:
    """Valid-JSON drift between the column and the payload key is equally disqualifying."""
    db = _copy_fixture(tmp_path)
    conn = sqlite3.connect(db)
    conn.execute(
        "UPDATE accounting_events SET position_reference=? WHERE event_type LIKE '%CLOSE%'",
        (json.dumps({"source": "registry", "physical_identity_hash": _SYNTHETIC_ID}),),
    )
    conn.commit()
    conn.close()

    result = _repair(db)
    assert result.repaired_events == 0
    assert result.skipped_rows == 1
    assert "disagrees" in result.anomalies[0]


def test_two_identities_one_closed_repairs_the_real_row_only(tmp_path: Path) -> None:
    """Card D3.F4 — the ticket's own named risk: two identities, one closed.

    The real registry row's anchors join to the real events; the synthetic
    row's anchors join to nothing. The repair must close the real join, leave
    the synthetic row an inverse orphan, and complete cleanly — never stamp
    the synthetic identity anywhere.
    """
    db = _copy_fixture(tmp_path)
    conn = sqlite3.connect(db)
    conn.executescript(
        f"""
        CREATE TEMP TABLE t AS SELECT * FROM position_registry WHERE status='closed';
        UPDATE t SET physical_identity_hash='{_SYNTHETIC_ID}', closed_tx='{_SYNTHETIC_TX}',
                     opened_tx='{_SYNTHETIC_TX}', semantic_grouping_key='synthetic:vib6346:second';
        INSERT INTO position_registry SELECT * FROM t;
        """
    )
    conn.commit()
    conn.close()

    result = _repair(db)
    assert result.registry_rows == 2
    assert result.repaired_events == 2
    assert result.skipped_rows == 0
    for reference in _event_references(db).values():
        assert reference is not None
        assert reference["physical_identity_hash"] == _PHID, "the synthetic identity must never be stamped"
    cell = _cell22(db)
    assert cell.status == "FAIL"
    assert "inverse orphan" in cell.diagnostic


def test_registry_table_absent_is_a_clean_no_op(tmp_path: Path) -> None:
    """Card D3.F5: a legacy book has nothing to join against — 0 repairs, no crash."""
    db = _copy_fixture(tmp_path)
    conn = sqlite3.connect(db)
    conn.execute("DROP TABLE position_registry")
    conn.commit()
    conn.close()

    result = _repair(db)
    assert result.registry_absent is True
    assert result.repaired_events == 0
    assert result.written is False


# =============================================================================
# Scoping and environmental errors
# =============================================================================


def test_deployment_id_filter_scopes_the_repair(tmp_path: Path) -> None:
    db = _copy_fixture(tmp_path)
    other = _repair(db, deployment_id="deployment:000000000000")
    assert other.registry_rows == 0
    assert other.repaired_events == 0
    assert _cell22(db).status == "FAIL", "a non-matching filter must repair nothing"

    scoped = _repair(db, deployment_id=_DEPLOYMENT)
    assert scoped.registry_rows == 1
    assert scoped.repaired_events == 2
    assert _cell22(db).status == "PASS"


def test_missing_db_raises_and_maps_to_cli_exit_1(tmp_path: Path) -> None:
    missing = tmp_path / "nope.sqlite"
    runner = CliRunner()
    outcome = runner.invoke(repair_position_references_cmd, ["--db", str(missing)])
    assert outcome.exit_code == 1
    assert "State DB not found" in outcome.output


def test_non_strategy_db_raises_and_maps_to_cli_exit_1(tmp_path: Path) -> None:
    stray = tmp_path / "stray.sqlite"
    conn = sqlite3.connect(stray)
    conn.execute("CREATE TABLE unrelated (x INTEGER)")
    conn.commit()
    conn.close()
    runner = CliRunner()
    outcome = runner.invoke(repair_position_references_cmd, ["--db", str(stray)])
    assert outcome.exit_code == 1
    assert "accounting_events" in outcome.output


def test_cli_end_to_end_exit_0_and_reports_the_repair(tmp_path: Path) -> None:
    """The operator surface: same engine, exit 0, human-readable summary."""
    db = _copy_fixture(tmp_path)
    runner = CliRunner()

    dry = runner.invoke(repair_position_references_cmd, ["--db", str(db), "--dry-run"])
    assert dry.exit_code == 0, dry.output
    assert "DRY RUN" in dry.output
    assert _cell22(db).status == "FAIL"

    real = runner.invoke(repair_position_references_cmd, ["--db", str(db)])
    assert real.exit_code == 0, real.output
    assert "events re-pointed: 2" in real.output
    assert "Committed." in real.output
    assert _cell22(db).status == "PASS"

    again = runner.invoke(repair_position_references_cmd, ["--db", str(db)])
    assert again.exit_code == 0, again.output
    assert "events re-pointed: 0" in again.output


# =============================================================================
# The integrity classifier itself
# =============================================================================


def _row(payload_json: str | None, position_reference: str | None) -> sqlite3.Row:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT ? AS payload_json, ? AS position_reference", (payload_json, position_reference)
    ).fetchone()
    conn.close()
    return row


def test_integrity_classifier_accepts_healthy_and_column_less_rows() -> None:
    reference = {"source": "legacy", "physical_identity_hash": None}
    payload = json.dumps({"event_type": "PERP_CLOSE", "position_reference": reference})
    assert _frozen_row_integrity_anomaly(_row(payload, json.dumps(reference))) is None
    # Pre-column books and non-OPEN/CLOSE rows legitimately carry no column.
    assert _frozen_row_integrity_anomaly(_row(payload, None)) is None
    assert _frozen_row_integrity_anomaly(_row(payload, "")) is None
    assert _frozen_row_integrity_anomaly(_row(json.dumps({"event_type": "PERP_SETTLEMENT"}), None)) is None


def test_integrity_classifier_flags_each_corruption_shape() -> None:
    reference = {"source": "legacy"}
    payload = json.dumps({"event_type": "PERP_CLOSE", "position_reference": reference})
    assert _frozen_row_integrity_anomaly(_row("{not json", None)) == "payload_json is not valid JSON"
    assert _frozen_row_integrity_anomaly(_row("[1, 2]", None)) == "payload_json is not a JSON object"
    assert _frozen_row_integrity_anomaly(_row(payload, "{not json")) == "position_reference column is not valid JSON"
    assert "disagrees" in _frozen_row_integrity_anomaly(_row(payload, json.dumps({"source": "registry"})))


# =============================================================================
# PR #3615 review fixes — each of these fails on the pre-review code
# =============================================================================


def test_column_present_but_payload_key_absent_is_an_anomaly(tmp_path: Path) -> None:
    """Review bug 1: the classifier accepted a populated column over a payload
    that lacks the position_reference key — drift the writers cannot produce,
    and the repair would have overwritten the canonical payload."""
    reference = {"source": "legacy"}
    payload_without_key = json.dumps({"event_type": "PERP_CLOSE"})
    verdict = _frozen_row_integrity_anomaly(_row(payload_without_key, json.dumps(reference)))
    assert verdict is not None and "lacks" in verdict

    # End-to-end: strip the payload key on the fixture's CLOSE row, keep the
    # column — the anchored registry row must be skipped, nothing overwritten.
    db = _copy_fixture(tmp_path)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    close_row = conn.execute("SELECT id, payload_json FROM accounting_events WHERE event_type='PERP_CLOSE'").fetchone()
    stripped = json.loads(close_row["payload_json"])
    stripped.pop("position_reference")
    conn.execute(
        "UPDATE accounting_events SET payload_json=? WHERE id=?",
        (json.dumps(stripped, sort_keys=True), close_row["id"]),
    )
    conn.commit()
    conn.close()

    result = _repair(db)
    assert result.repaired_events == 0
    assert result.skipped_rows == 1
    assert result.skips[0].reason == "integrity_anomaly"
    conn = sqlite3.connect(db)
    after = conn.execute("SELECT payload_json FROM accounting_events WHERE id=?", (close_row["id"],)).fetchone()[0]
    conn.close()
    assert json.loads(after) == stripped, "the canonical payload must not be rewritten"


def test_repair_error_in_committed_pass_rolls_back_partial_row_writes(tmp_path: Path, monkeypatch: Any) -> None:
    """Review bug 2: a repair dying between a row's OPEN and CLOSE UPDATEs
    committed the half-stamped row while the log claimed 'events stay legacy'.

    The counting pass is allowed to succeed (calls 1–2); the COMMITTED pass
    fails on the row's second anchor (call 4). Without the per-row SAVEPOINT
    this leaves a committed half-joined book; with it, the book is untouched.
    """
    import almanak.framework.accounting.writer as writer_mod

    db = _copy_fixture(tmp_path)
    real = writer_mod.restamp_position_reference
    calls = {"n": 0}

    def _flaky(*args: Any, **kwargs: Any):
        calls["n"] += 1
        if calls["n"] == 4:
            raise RuntimeError("simulated mid-row failure in the committed pass")
        return real(*args, **kwargs)

    monkeypatch.setattr(writer_mod, "restamp_position_reference", _flaky)

    result = _repair(db)
    assert calls["n"] == 4
    assert result.written is False
    assert result.repaired_events == 0
    assert result.skipped_rows == 1
    assert result.skips[0].reason == "repair_error"
    for reference in _event_references(db).values():
        assert reference is not None and reference["source"] == "legacy", (
            "a failed row must leave NO partial stamp in the committed book"
        )
    # The recovery point taken before the committed pass is kept.
    assert result.backup_path is not None and Path(result.backup_path).is_file()


def test_poisoned_txs_are_scoped_per_deployment_and_chain(tmp_path: Path) -> None:
    """Review P1: a bare-tx poison key let one deployment's corrupt row block a
    healthy same-hash book on another deployment/chain."""
    db = _copy_fixture(tmp_path)
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    other = "deployment:bbbbbbbbbbbb"
    conn.execute(
        """
        INSERT INTO accounting_events
        SELECT 'b-' || id, ?, cycle_id, execution_mode, timestamp, 'avalanche', protocol,
               wallet_address, event_type, position_key, ledger_entry_id, tx_hash,
               confidence, payload_json, schema_version, position_reference
        FROM accounting_events WHERE event_type IN ('PERP_OPEN','PERP_CLOSE')
        """,
        (other,),
    )
    conn.execute(
        """
        INSERT INTO position_registry
        SELECT ?, 'avalanche', primitive, accounting_category, physical_identity_hash,
               semantic_grouping_key, grouping_policy_version, handle, status, payload,
               opened_at_block, opened_tx, closed_at_block, closed_tx,
               last_reconciled_at_block, matching_policy_version
        FROM position_registry WHERE status='closed'
        """,
        (other,),
    )
    # Corrupt ONLY the original deployment's CLOSE column.
    conn.execute(
        "UPDATE accounting_events SET position_reference='{not json' WHERE event_type='PERP_CLOSE' AND deployment_id=?",
        (_DEPLOYMENT,),
    )
    conn.commit()
    conn.close()

    result = _repair(db)
    assert result.registry_rows == 2
    assert result.skipped_rows == 1, "only the corrupt deployment's row may be skipped"
    assert result.skips[0].deployment_id == _DEPLOYMENT
    assert result.repaired_events == 2, "the healthy same-hash deployment must still repair"
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    for r in conn.execute(
        "SELECT payload_json FROM accounting_events WHERE deployment_id=? AND event_type IN ('PERP_OPEN','PERP_CLOSE')",
        (other,),
    ):
        assert json.loads(r["payload_json"])["position_reference"]["physical_identity_hash"] == _PHID
    conn.close()


def test_backup_path_claim_never_overwrites_a_same_second_backup(tmp_path: Path) -> None:
    """Review P1: one-second timestamp resolution let a second scoped repair
    silently truncate the first run's recovery point."""
    db = str(tmp_path / "book.sqlite")
    first = _claim_backup_path(db, "20260805T140000Z")
    second = _claim_backup_path(db, "20260805T140000Z")
    assert first != second
    assert Path(first).exists() and Path(second).exists()
    assert first == f"{db}.bak-20260805T140000Z"
    assert second == f"{db}.bak-20260805T140000Z-2"


def test_running_writer_lock_refuses_both_modes(tmp_path: Path) -> None:
    """Review P1: the repair previously proceeded while another process held the
    gateway flock. Both modes must refuse — a dry-run's counts are a promise
    about a later write."""
    from almanak.framework.local_paths import (
        LocalDbLockError,
        acquire_local_db_lock,
        release_local_db_lock,
    )

    db = _copy_fixture(tmp_path)
    fd = acquire_local_db_lock(db)
    try:
        for kwargs in ({}, {"dry_run": True}):
            try:
                _repair(db, **kwargs)
                raise AssertionError(f"repair must refuse while the DB lock is held ({kwargs})")
            except LocalDbLockError:
                pass
    finally:
        release_local_db_lock(fd)

    # Lock released → the same repair proceeds.
    assert _repair(db).repaired_events == 2


def test_wal_sidecar_heuristic(tmp_path: Path) -> None:
    db = tmp_path / "x.sqlite"
    db.write_bytes(b"")
    assert _wal_sidecar_nonempty(db) is False
    (tmp_path / "x.sqlite-wal").write_bytes(b"")
    assert _wal_sidecar_nonempty(db) is False
    (tmp_path / "x.sqlite-wal").write_bytes(b"frames")
    assert _wal_sidecar_nonempty(db) is True


# =============================================================================
# CodeRabbit round — preconditions and UX guards
# =============================================================================


def test_repair_refuses_an_initialized_store_connection(tmp_path: Path) -> None:
    """The flock is per-process: it cannot fence out this process's own store
    connection, which could write between the two passes and invalidate the
    integrity pre-scan. The repair demands an offline (uninitialized) store."""
    import asyncio

    db = _copy_fixture(tmp_path)
    store = SQLiteStore(SQLiteConfig(db_path=str(db)))
    asyncio.run(store.initialize())
    try:
        try:
            store.repair_frozen_position_references()
            raise AssertionError("an initialized store must be refused")
        except ValueError as exc:
            assert "OFFLINE" in str(exc)
    finally:
        conn = store._conn
        if conn is not None:
            conn.close()


def test_zero_match_deployment_filter_warns_loudly(tmp_path: Path) -> None:
    """A strategy name passed to -s scopes the repair to nothing; that must
    read as a warning, never as a clean no-op."""
    db = _copy_fixture(tmp_path)
    runner = CliRunner()
    outcome = runner.invoke(repair_position_references_cmd, ["--db", str(db), "-s", "my_strategy_name"])
    assert outcome.exit_code == 0
    assert "WARNING: no position_registry rows match" in outcome.output
    assert _cell22(db).status == "FAIL", "nothing may be repaired under a non-matching filter"


def test_failed_backup_removes_the_claimed_placeholder(tmp_path: Path, monkeypatch: Any) -> None:
    """A backup that dies (e.g. disk full) must not leave a 0-byte .bak file
    that reads as a valid recovery point."""
    db = _copy_fixture(tmp_path)

    real_connect = sqlite3.connect

    def _connect(path: Any, *a: Any, **k: Any) -> Any:
        if ".bak-" in str(path):
            raise sqlite3.OperationalError("database or disk is full")
        return real_connect(path, *a, **k)

    monkeypatch.setattr("almanak.framework.state.backends.sqlite.sqlite3.connect", _connect)
    try:
        _repair(db)
        raise AssertionError("backup failure must propagate")
    except sqlite3.OperationalError:
        pass
    assert list(tmp_path.glob("*.bak-*")) == [], "the claimed placeholder must be removed"
