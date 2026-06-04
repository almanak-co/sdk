"""Behavioral tests for the VIB-4931 Pendle ``position_registry`` category backfill.

``_backfill_pendle_registry_category`` runs at boot (``_run_migrations``) and relabels
any legacy ``pendle_lp`` / ``pendle_pt`` registry rows to the generic ``lp`` / ``swap``
categories the de-leak left behind, so an open-before/close-after round-trip still matches
the ``(deployment_id, accounting_category, …)`` uniqueness index.

These tests pin the four behaviours the migration must guarantee — relabel, no-op when the
table/rows are absent, idempotent re-run — plus the collision-safety contract: because both
registry unique indexes are scoped by ``accounting_category``, a legacy Pendle row that
shares an identity with an existing generic ``lp`` / ``swap`` row must NOT raise
``IntegrityError`` and strand the strategy at boot (codex review on #2600). The fixtures use
the real ``SCHEMA_SQL`` (table + the actual ``ix_registry_handle`` / ``ix_registry_auto_mode``
unique indexes) so the collision path is exercised against the production constraints, not a
hand-rolled approximation.
"""

from __future__ import annotations

import logging
import sqlite3

from almanak.framework.state.backends.sqlite import (
    SCHEMA_SQL,
    _backfill_pendle_registry_category,
)

_INSERT_COLS = (
    "deployment_id, chain, primitive, accounting_category, physical_identity_hash, "
    "semantic_grouping_key, grouping_policy_version, handle, status, payload, "
    "matching_policy_version"
)


def _conn_with_schema() -> sqlite3.Connection:
    """In-memory connection with the real position_registry table + unique indexes."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(SCHEMA_SQL)
    return conn


def _insert_row(
    conn: sqlite3.Connection,
    *,
    accounting_category: str,
    physical_identity_hash: str,
    semantic_grouping_key: str,
    handle: str | None = None,
    deployment_id: str = "dep-1",
    chain: str = "arbitrum",
    primitive: str = "lp",
    status: str = "open",
) -> None:
    conn.execute(
        f"INSERT INTO position_registry ({_INSERT_COLS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            deployment_id,
            chain,
            primitive,
            accounting_category,
            physical_identity_hash,
            semantic_grouping_key,
            "grouping-v1",
            handle,
            status,
            "{}",
            1,
        ),
    )
    conn.commit()


def _categories(conn: sqlite3.Connection) -> dict[str, int]:
    rows = conn.execute(
        "SELECT accounting_category, COUNT(*) FROM position_registry GROUP BY accounting_category"
    ).fetchall()
    return dict(rows)


def test_relabels_pendle_lp_to_lp_and_pendle_pt_to_swap() -> None:
    conn = _conn_with_schema()
    _insert_row(conn, accounting_category="pendle_lp", physical_identity_hash="H_LP", semantic_grouping_key="g-lp")
    _insert_row(
        conn,
        accounting_category="pendle_pt",
        physical_identity_hash="H_PT",
        semantic_grouping_key="g-pt",
        primitive="swap",
    )

    _backfill_pendle_registry_category(conn)

    cats = _categories(conn)
    assert cats == {"lp": 1, "swap": 1}
    # No legacy categories remain.
    assert "pendle_lp" not in cats and "pendle_pt" not in cats


def test_noop_when_table_absent() -> None:
    # Fresh connection with NO schema — the table-existence guard must short-circuit.
    conn = sqlite3.connect(":memory:")
    _backfill_pendle_registry_category(conn)  # must not raise
    assert (
        conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='position_registry'").fetchone()
        is None
    )


def test_noop_when_no_legacy_rows() -> None:
    conn = _conn_with_schema()
    _insert_row(conn, accounting_category="lp", physical_identity_hash="H_LP", semantic_grouping_key="g-lp")
    _insert_row(
        conn,
        accounting_category="perp",
        physical_identity_hash="H_PERP",
        semantic_grouping_key="g-perp",
        primitive="perp",
    )

    _backfill_pendle_registry_category(conn)

    assert _categories(conn) == {"lp": 1, "perp": 1}


def test_idempotent_rerun() -> None:
    conn = _conn_with_schema()
    _insert_row(conn, accounting_category="pendle_lp", physical_identity_hash="H_LP", semantic_grouping_key="g-lp")

    _backfill_pendle_registry_category(conn)
    after_first = _categories(conn)
    # Second run finds no legacy rows and is a no-op.
    _backfill_pendle_registry_category(conn)
    after_second = _categories(conn)

    assert after_first == {"lp": 1}
    assert after_second == after_first


def test_collision_via_handle_is_skipped_not_stranding_boot(caplog) -> None:
    """A pendle_lp row sharing (deployment, handle) with an existing lp row collides on
    ix_registry_handle after relabel. OR IGNORE must skip it (no IntegrityError, boot not
    stranded), preserve the existing lp row, leave the pendle row unchanged, and log ERROR.
    """
    conn = _conn_with_schema()
    # Existing generic LP position holding handle "leg_a".
    _insert_row(
        conn,
        accounting_category="lp",
        physical_identity_hash="H_UNI",
        semantic_grouping_key="g-uni",
        handle="leg_a",
    )
    # Legacy Pendle LP row sharing the same (deployment_id, handle); distinct PK via pih.
    # Allowed pre-migration because the unique index is scoped by accounting_category.
    _insert_row(
        conn,
        accounting_category="pendle_lp",
        physical_identity_hash="H_PENDLE",
        semantic_grouping_key="g-pendle",
        handle="leg_a",
    )

    with caplog.at_level(logging.ERROR):
        _backfill_pendle_registry_category(conn)  # must NOT raise

    cats = _categories(conn)
    # The existing lp row is preserved; the colliding pendle row is left unchanged.
    assert cats == {"lp": 1, "pendle_lp": 1}
    # Both rows still present (no data loss) — 2 rows total.
    assert conn.execute("SELECT COUNT(*) FROM position_registry").fetchone()[0] == 2
    assert any("could not be relabeled" in r.message for r in caplog.records)


def test_collision_via_auto_mode_is_skipped_not_stranding_boot(caplog) -> None:
    """Same guarantee on the handle-less auto-mode index ix_registry_auto_mode
    (deployment_id, chain, accounting_category, semantic_grouping_key) WHERE status='open'.
    """
    conn = _conn_with_schema()
    _insert_row(
        conn,
        accounting_category="lp",
        physical_identity_hash="H_UNI",
        semantic_grouping_key="shared-group",
        handle=None,
    )
    _insert_row(
        conn,
        accounting_category="pendle_lp",
        physical_identity_hash="H_PENDLE",
        semantic_grouping_key="shared-group",
        handle=None,
    )

    with caplog.at_level(logging.ERROR):
        _backfill_pendle_registry_category(conn)  # must NOT raise

    cats = _categories(conn)
    assert cats == {"lp": 1, "pendle_lp": 1}
    assert any("could not be relabeled" in r.message for r in caplog.records)
