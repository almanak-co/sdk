"""Unit tests for the --fresh flag table-clearing logic in run_helpers.py."""

import sqlite3
import tempfile
from pathlib import Path

from almanak.framework.cli.run_helpers import _FRESH_STRATEGY_ID_TABLES, _fresh_clear_state


def _create_db(path: Path) -> None:
    """Create a minimal state DB with all tables the --fresh flag should clear."""
    with sqlite3.connect(str(path)) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS strategy_state (
                strategy_id TEXT, data TEXT
            );
            CREATE TABLE IF NOT EXISTS teardown_requests (
                strategy_id TEXT, data TEXT
            );
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                strategy_id TEXT, data TEXT
            );
            CREATE TABLE IF NOT EXISTS portfolio_metrics (
                strategy_id TEXT PRIMARY KEY, data TEXT
            );
            CREATE TABLE IF NOT EXISTS transaction_ledger (
                id TEXT PRIMARY KEY, strategy_id TEXT, data TEXT
            );
            CREATE TABLE IF NOT EXISTS accounting_events (
                id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                deployment_id TEXT NOT NULL,
                data TEXT
            );
            CREATE TABLE IF NOT EXISTS accounting_outbox (
                id TEXT PRIMARY KEY,
                strategy_id TEXT NOT NULL,
                deployment_id TEXT NOT NULL,
                data TEXT
            );
            CREATE TABLE IF NOT EXISTS position_events (
                id TEXT PRIMARY KEY,
                deployment_id TEXT NOT NULL,
                data TEXT
            );
        """)


def _seed_rows(conn: sqlite3.Connection, strategy_id: str, deployment_id: str) -> None:
    """Insert one row per table for the given strategy (IDs scoped by deployment_id)."""
    conn.execute("INSERT INTO strategy_state VALUES (?, 'x')", (strategy_id,))
    conn.execute("INSERT INTO teardown_requests VALUES (?, 'x')", (strategy_id,))
    conn.execute("INSERT INTO portfolio_snapshots VALUES (?, 'x')", (strategy_id,))
    conn.execute("INSERT INTO portfolio_metrics VALUES (?, 'x')", (strategy_id,))
    conn.execute(f"INSERT INTO transaction_ledger VALUES ('{deployment_id}-t', ?, 'x')", (strategy_id,))  # noqa: S608
    conn.execute(
        f"INSERT INTO accounting_events VALUES ('{deployment_id}-ae', ?, ?, 'x')",  # noqa: S608
        (strategy_id, deployment_id),
    )
    conn.execute(
        f"INSERT INTO accounting_outbox VALUES ('{deployment_id}-ao', ?, ?, 'x')",  # noqa: S608
        (strategy_id, deployment_id),
    )
    conn.execute(f"INSERT INTO position_events VALUES ('{deployment_id}-pe', ?, 'x')", (deployment_id,))  # noqa: S608


def _count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608


class TestFreshStrategyIdTableList:
    """Verify the module-level constant is kept in sync."""

    def test_contains_expected_tables(self) -> None:
        assert "strategy_state" in _FRESH_STRATEGY_ID_TABLES
        assert "portfolio_snapshots" in _FRESH_STRATEGY_ID_TABLES
        assert "accounting_events" in _FRESH_STRATEGY_ID_TABLES
        # position_events is deployment_id-keyed and handled separately
        assert "position_events" not in _FRESH_STRATEGY_ID_TABLES


class TestFreshFlagAnvil:
    """On Anvil all rows across all strategies must be deleted."""

    def test_all_tables_cleared(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)

        _create_db(db)
        with sqlite3.connect(str(db)) as conn:
            _seed_rows(conn, "strat-a", "dep-a")
            _seed_rows(conn, "strat-b", "dep-b")
            # Give strat-b a second row in position_events
            conn.execute("INSERT INTO position_events VALUES ('dep-b-pe2', 'dep-b', 'y')")

        _fresh_clear_state(sqlite3.connect(str(db)), "strat-a", is_anvil=True)

        with sqlite3.connect(str(db)) as conn:
            all_tables = [
                *_FRESH_STRATEGY_ID_TABLES,
                "position_events",
            ]
            for table in all_tables:
                assert _count(conn, table) == 0, f"Expected {table} to be empty on Anvil"


class TestFreshFlagMainnet:
    """On mainnet only the target strategy's rows are deleted."""

    def test_target_strategy_rows_cleared(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)

        _create_db(db)
        with sqlite3.connect(str(db)) as conn:
            _seed_rows(conn, "target", "dep-target")
            _seed_rows(conn, "other", "dep-other")

        _fresh_clear_state(sqlite3.connect(str(db)), "target", is_anvil=False)

        with sqlite3.connect(str(db)) as conn:
            for table in _FRESH_STRATEGY_ID_TABLES:
                rows = conn.execute(
                    f"SELECT strategy_id FROM {table}"  # noqa: S608
                ).fetchall()
                assert len(rows) == 1, f"Expected exactly 1 row in {table} after mainnet --fresh, got {len(rows)}"
                assert rows[0][0] == "other", f"Surviving row in {table} should be 'other', got {rows[0][0]!r}"
            # position_events for target deployment should be gone
            n_pe = conn.execute("SELECT COUNT(*) FROM position_events WHERE deployment_id = 'dep-target'").fetchone()[0]
            assert n_pe == 0, "position_events for target deployment should be deleted"
            # position_events for other deployment must be preserved
            n_pe_other = conn.execute(
                "SELECT COUNT(*) FROM position_events WHERE deployment_id = 'dep-other'"
            ).fetchone()[0]
            assert n_pe_other == 1, "position_events for other deployment must be preserved"

    def test_outbox_only_deployment_id_clears_position_events(self) -> None:
        """position_events are cleared even when the deployment_id only appears in
        accounting_outbox (i.e. the outbox hasn't been drained into accounting_events yet).
        """
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)

        _create_db(db)
        with sqlite3.connect(str(db)) as conn:
            # Seed target strategy with a deployment_id that ONLY appears in accounting_outbox.
            conn.execute("INSERT INTO strategy_state VALUES ('target', 'x')")
            conn.execute("INSERT INTO accounting_outbox VALUES ('ao-target', 'target', 'dep-outbox', 'x')")
            conn.execute("INSERT INTO position_events VALUES ('pe-outbox', 'dep-outbox', 'x')")
            # Seed an unrelated strategy that must be preserved.
            conn.execute("INSERT INTO strategy_state VALUES ('other', 'x')")
            conn.execute("INSERT INTO accounting_outbox VALUES ('ao-other', 'other', 'dep-other', 'x')")
            conn.execute("INSERT INTO position_events VALUES ('pe-other', 'dep-other', 'x')")

        _fresh_clear_state(sqlite3.connect(str(db)), "target", is_anvil=False)

        with sqlite3.connect(str(db)) as conn:
            # Target's position_events should be gone even though dep-outbox never
            # made it into accounting_events.
            n_target = conn.execute(
                "SELECT COUNT(*) FROM position_events WHERE deployment_id = 'dep-outbox'"
            ).fetchone()[0]
            assert n_target == 0, "position_events for outbox-only dep should be deleted"
            # Other strategy's events must be untouched.
            n_other = conn.execute("SELECT COUNT(*) FROM position_events WHERE deployment_id = 'dep-other'").fetchone()[
                0
            ]
            assert n_other == 1, "position_events for other strategy must be preserved"

    def test_missing_tables_do_not_raise(self) -> None:
        """Older DBs without all tables should not error."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)
        # Only create strategy_state — no other tables
        with sqlite3.connect(str(db)) as conn:
            conn.execute("CREATE TABLE strategy_state (strategy_id TEXT, data TEXT)")
            conn.execute("INSERT INTO strategy_state VALUES ('s1', 'x')")
        _fresh_clear_state(sqlite3.connect(str(db)), "s1", is_anvil=False)
        with sqlite3.connect(str(db)) as conn:
            assert _count(conn, "strategy_state") == 0
