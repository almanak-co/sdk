import sqlite3

import pytest

from qa_lab.e2e_card import digest
from qa_lab.e2e_final_database import compare_final_database


@pytest.fixture
def snapshots(tmp_path):
    (tmp_path / "subject-final").mkdir()
    for name, values in (("almanak_state.db", [1, 2]), ("subject-final/almanak_state.db", [2, 1])):
        with sqlite3.connect(tmp_path / name) as db:
            db.execute("CREATE TABLE ledger (amount INTEGER, token TEXT)")
            db.executemany("INSERT INTO ledger VALUES (?, 'WETH')", [(value,) for value in values])
            db.execute("CREATE TABLE snapshots (value BLOB)")
            db.execute("INSERT INTO snapshots VALUES (?)", (b"\x00\xff",))
    return tmp_path


def replay(root):
    return compare_final_database(
        root, {"status": "PASS", "database_sha256": digest((root / "subject-final/almanak_state.db").read_bytes())}
    )


def test_final_database_matches_all_logical_rows_despite_storage_order(snapshots):
    result = replay(snapshots)
    assert result["status"] == "PASS"
    assert result["tables_compared"] == 2
    assert result["canonical_database_sha256"] != result["final_database_sha256"]


@pytest.mark.parametrize(
    "statement",
    [
        "UPDATE ledger SET amount=10 WHERE amount=1",
        "DELETE FROM ledger WHERE amount=2",
        "INSERT INTO ledger VALUES (3,'WETH')",
        "UPDATE snapshots SET value=NULL",
        "CREATE TABLE late_state (id INTEGER)",
        "ALTER TABLE ledger ADD COLUMN late INTEGER",
    ],
)
def test_any_late_persisted_difference_invalidates_early_snapshot(snapshots, statement):
    with sqlite3.connect(snapshots / "subject-final/almanak_state.db") as db:
        db.execute(statement)
    assert replay(snapshots)["status"] == "FAIL"


def test_final_database_hash_must_match_exit_proof(snapshots):
    result = compare_final_database(snapshots, {"status": "PASS", "database_sha256": "0" * 64})
    assert result["status"] == "FAIL"


@pytest.mark.parametrize("name", ["almanak_state.db", "subject-final/almanak_state.db"])
def test_unresolved_journal_cannot_be_ignored(snapshots, name):
    (snapshots / (name + "-wal")).touch()
    assert replay(snapshots)["status"] == "FAIL"


def test_database_equality_does_not_substitute_for_process_exit(snapshots):
    assert compare_final_database(snapshots, {"status": "UNMEASURED"})["status"] == "UNMEASURED"
