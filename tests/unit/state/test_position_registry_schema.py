"""Schema-level tests for the position_registry table (VIB-4190 / T05).

Verifies that:
- The table is created with all 16 columns from PRD §Registry Data Shape.
- Column types and NOT NULL constraints match the ratified shape verbatim.
- The two partial unique indexes exist with correct column tuples and
  WHERE clauses (AND-conjoined for ix_registry_auto_mode, not OR).
- The schema bootstrap is idempotent on rerun.

This is a schema-only PR (T05). Writers come in T11 (VIB-4197). The
schema_contract entry is intentionally deferred — see blueprints/28
§5.1 for rationale.
"""

from __future__ import annotations

import sqlite3

from almanak.framework.state.backends.sqlite import SCHEMA_SQL


_EXPECTED_COLUMNS: dict[str, tuple[str, bool]] = {
    # column_name: (declared_type, not_null)
    "deployment_id": ("TEXT", True),
    "chain": ("TEXT", True),
    "primitive": ("TEXT", True),
    "accounting_category": ("TEXT", True),
    "physical_identity_hash": ("TEXT", True),
    "semantic_grouping_key": ("TEXT", True),
    "grouping_policy_version": ("TEXT", True),
    "handle": ("TEXT", False),
    "status": ("TEXT", True),
    # PRD declares `payload JSON NOT NULL`; SQLite has no native JSON type so
    # the realization is TEXT (matches sibling JSON-bearing columns elsewhere
    # in sqlite.py). See SCHEMA_SQL block for rationale.
    "payload": ("TEXT", True),
    "opened_at_block": ("INTEGER", False),
    "opened_tx": ("TEXT", False),
    "closed_at_block": ("INTEGER", False),
    "closed_tx": ("TEXT", False),
    "last_reconciled_at_block": ("INTEGER", False),
    "matching_policy_version": ("INTEGER", True),
}


def _bootstrap() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.executescript(SCHEMA_SQL)
    return conn


def test_table_exists() -> None:
    conn = _bootstrap()
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='position_registry'"
    ).fetchone()
    assert row is not None, "position_registry table not created by SCHEMA_SQL"


def test_columns_match_prd_shape() -> None:
    conn = _bootstrap()
    info = conn.execute("PRAGMA table_info(position_registry)").fetchall()
    # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
    by_name = {row[1]: row for row in info}

    assert set(by_name) == set(_EXPECTED_COLUMNS), (
        f"column set mismatch. expected={sorted(_EXPECTED_COLUMNS)}, got={sorted(by_name)}"
    )

    for name, (want_type, want_not_null) in _EXPECTED_COLUMNS.items():
        row = by_name[name]
        got_type = row[2].upper()
        got_not_null = bool(row[3])
        assert got_type == want_type, (
            f"column {name!r}: type {got_type!r} != expected {want_type!r}"
        )
        assert got_not_null == want_not_null, (
            f"column {name!r}: not_null={got_not_null} != expected {want_not_null}"
        )


def test_primary_key_shape() -> None:
    conn = _bootstrap()
    info = conn.execute("PRAGMA table_info(position_registry)").fetchall()
    pk_columns = [row[1] for row in sorted(info, key=lambda r: r[5]) if row[5] > 0]
    assert pk_columns == ["deployment_id", "chain", "primitive", "physical_identity_hash"], (
        f"PRIMARY KEY shape wrong: got {pk_columns}"
    )


def test_handle_partial_unique_index() -> None:
    conn = _bootstrap()
    rows = conn.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='index' AND tbl_name='position_registry' AND name='ix_registry_handle'"
    ).fetchall()
    assert rows, "ix_registry_handle missing"
    sql = rows[0][1].lower()
    assert "unique" in sql, f"ix_registry_handle is not UNIQUE: {sql}"
    # Column tuple
    assert "(deployment_id, accounting_category, handle)" in sql.replace(" ,", ","), (
        f"ix_registry_handle column tuple wrong: {sql}"
    )
    assert "where handle is not null" in sql, (
        f"ix_registry_handle WHERE clause wrong: {sql}"
    )


def test_auto_mode_partial_unique_index() -> None:
    conn = _bootstrap()
    rows = conn.execute(
        "SELECT name, sql FROM sqlite_master "
        "WHERE type='index' AND tbl_name='position_registry' AND name='ix_registry_auto_mode'"
    ).fetchall()
    assert rows, "ix_registry_auto_mode missing"
    sql = rows[0][1].lower().replace("\n", " ")
    assert "unique" in sql
    assert "(deployment_id, chain, accounting_category, semantic_grouping_key)" in sql.replace(
        " ,", ","
    ), f"ix_registry_auto_mode column tuple wrong: {sql}"
    where_clause = sql.split("where", 1)[1] if "where" in sql else ""
    assert "status = 'open'" in where_clause, f"missing status='open' guard: {where_clause}"
    assert "handle is null" in where_clause, f"missing handle IS NULL guard: {where_clause}"
    assert " and " in where_clause, (
        f"ix_registry_auto_mode WHERE must conjoin with AND, not OR: {where_clause}"
    )
    assert " or " not in where_clause, (
        f"ix_registry_auto_mode WHERE uses OR (forbidden): {where_clause}"
    )


def test_handle_index_actually_enforces_uniqueness() -> None:
    """Behavioral verification: insert two rows with the same handle -> SECOND fails."""
    conn = _bootstrap()
    base_row = (
        "dep1",
        "arbitrum",
        "lp",
        "lp",
        "phys_a",
        "sem_a",
        "univ3_lp@v1",
        "leg_long",
        "open",
        "{}",
        100,
        "0xabc",
        None,
        None,
        None,
        3,
    )
    conn.execute(
        "INSERT INTO position_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        base_row,
    )
    # Different physical_identity_hash, same handle in same accounting_category -> conflict
    duplicate = list(base_row)
    duplicate[4] = "phys_b"  # physical_identity_hash differs (so PK doesn't trip first)
    duplicate[5] = "sem_b"  # semantic_grouping_key differs
    try:
        conn.execute(
            "INSERT INTO position_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            tuple(duplicate),
        )
        raise AssertionError("expected handle uniqueness violation")
    except sqlite3.IntegrityError:
        pass

    # Different deployment with same handle is allowed
    other_dep = list(base_row)
    other_dep[0] = "dep2"
    other_dep[4] = "phys_c"
    conn.execute(
        "INSERT INTO position_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        tuple(other_dep),
    )


def test_auto_mode_index_rejects_duplicate_unhandled_open() -> None:
    """Behavioral: two open rows with same semantic_grouping_key, no handle -> conflict."""
    conn = _bootstrap()
    row_a = (
        "dep1",
        "arbitrum",
        "lp",
        "lp",
        "phys_a",
        "pool_xyz",
        "univ3_lp@v1",
        None,  # handle = NULL -> auto-mode
        "open",
        "{}",
        100,
        "0xa",
        None,
        None,
        None,
        3,
    )
    conn.execute(
        "INSERT INTO position_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        row_a,
    )
    row_b = list(row_a)
    row_b[4] = "phys_b"  # different physical id (so PK doesn't trip)
    try:
        conn.execute(
            "INSERT INTO position_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            tuple(row_b),
        )
        raise AssertionError("expected auto-mode collision")
    except sqlite3.IntegrityError:
        pass

    # Closing the first row releases the slot -> opening a new one is allowed
    conn.execute(
        "UPDATE position_registry SET status='closed' WHERE physical_identity_hash='phys_a'"
    )
    conn.execute(
        "INSERT INTO position_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        tuple(row_b),
    )

    # Supplying a handle on a third row bypasses the guard
    row_c = list(row_a)
    row_c[4] = "phys_c"
    row_c[7] = "leg_a"  # handle non-null
    conn.execute(
        "INSERT INTO position_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        tuple(row_c),
    )


def test_status_check_constraint_rejects_invalid_values() -> None:
    """Behavioral: CHECK pins status to {open, closed, reorg_invalidated}.

    A case-variant typo (`OPEN`, `Open`, `INVALIDATED`, …) would silently bypass
    the partial unique index `ix_registry_auto_mode` (which guards rows where
    `status = 'open'`) and admit duplicate semantic groups. The CHECK constraint
    closes that hole at the storage boundary.
    """
    conn = _bootstrap()
    base = (
        "dep1",
        "arbitrum",
        "lp",
        "lp",
        "phys_a",
        "sem_a",
        "univ3_lp@v1",
        None,
        "open",  # canonical — accepted
        "{}",
        100,
        "0xa",
        None,
        None,
        None,
        3,
    )
    conn.execute(
        "INSERT INTO position_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        base,
    )

    for bad_status in ("OPEN", "Open", "Closed", "REORG_INVALIDATED", "active", ""):
        bad = list(base)
        bad[4] = f"phys_{bad_status or 'empty'}"  # unique physical_identity_hash
        bad[5] = f"sem_{bad_status or 'empty'}"  # unique semantic_grouping_key
        bad[8] = bad_status
        try:
            conn.execute(
                "INSERT INTO position_registry VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                tuple(bad),
            )
            raise AssertionError(
                f"expected status={bad_status!r} to be rejected by CHECK constraint"
            )
        except sqlite3.IntegrityError:
            pass


def test_migration_idempotent() -> None:
    """Running SCHEMA_SQL twice produces identical schema (no errors)."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(SCHEMA_SQL)
    snapshot_before = conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE tbl_name='position_registry' ORDER BY name"
    ).fetchall()

    # Run again — every CREATE uses IF NOT EXISTS, so this must be a no-op.
    conn.executescript(SCHEMA_SQL)
    snapshot_after = conn.execute(
        "SELECT type, name, tbl_name, sql FROM sqlite_master "
        "WHERE tbl_name='position_registry' ORDER BY name"
    ).fetchall()

    assert snapshot_before == snapshot_after, (
        "rerunning SCHEMA_SQL changed the position_registry schema"
    )
