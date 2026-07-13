"""Unit tests for the --fresh flag table-clearing logic in run_helpers.py."""

import sqlite3
import tempfile
from pathlib import Path

from almanak.framework.cli._run_setup import (
    _FRESH_DECISION_STATE_TABLES,
    _FRESH_ONCHAIN_RECORD_TABLES,
)
from almanak.framework.cli.run_helpers import (
    _FRESH_DEPLOYMENT_ID_TABLES,
    _fresh_clear_state,
)


def _create_db(path: Path) -> None:
    """Create a minimal state DB with all tables the --fresh flag should clear."""
    with sqlite3.connect(str(path)) as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS strategy_state (
                deployment_id TEXT, data TEXT
            );
            CREATE TABLE IF NOT EXISTS teardown_requests (
                deployment_id TEXT, data TEXT
            );
            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                deployment_id TEXT, data TEXT
            );
            CREATE TABLE IF NOT EXISTS portfolio_metrics (
                deployment_id TEXT PRIMARY KEY, data TEXT
            );
            CREATE TABLE IF NOT EXISTS transaction_ledger (
                id TEXT PRIMARY KEY, deployment_id TEXT, data TEXT
            );
            CREATE TABLE IF NOT EXISTS accounting_events (
                id TEXT PRIMARY KEY,
                deployment_id TEXT NOT NULL,
                data TEXT
            );
            CREATE TABLE IF NOT EXISTS accounting_outbox (
                id TEXT PRIMARY KEY,
                deployment_id TEXT NOT NULL,
                data TEXT
            );
            CREATE TABLE IF NOT EXISTS position_events (
                id TEXT PRIMARY KEY,
                deployment_id TEXT NOT NULL,
                data TEXT
            );
            CREATE TABLE IF NOT EXISTS position_state_snapshots (
                id TEXT PRIMARY KEY,
                deployment_id TEXT NOT NULL,
                data TEXT
            );
            CREATE TABLE IF NOT EXISTS clob_orders (
                id TEXT PRIMARY KEY,
                deployment_id TEXT NOT NULL,
                data TEXT
            );
            CREATE TABLE IF NOT EXISTS position_registry (
                id TEXT PRIMARY KEY,
                deployment_id TEXT NOT NULL,
                data TEXT
            );
            CREATE TABLE IF NOT EXISTS migration_state (
                id TEXT PRIMARY KEY,
                deployment_id TEXT NOT NULL,
                data TEXT
            );
        """)


def _seed_rows(conn: sqlite3.Connection, deployment_id: str) -> None:
    """Insert one row per table for the given strategy (IDs scoped by deployment_id)."""
    conn.execute("INSERT INTO strategy_state VALUES (?, 'x')", (deployment_id,))
    conn.execute("INSERT INTO teardown_requests VALUES (?, 'x')", (deployment_id,))
    conn.execute("INSERT INTO portfolio_snapshots VALUES (?, 'x')", (deployment_id,))
    conn.execute("INSERT INTO portfolio_metrics VALUES (?, 'x')", (deployment_id,))
    conn.execute(f"INSERT INTO transaction_ledger VALUES ('{deployment_id}-t', ?, 'x')", (deployment_id,))  # noqa: S608
    conn.execute(
        f"INSERT INTO accounting_events VALUES ('{deployment_id}-ae', ?, 'x')",  # noqa: S608
        (deployment_id,),
    )
    conn.execute(
        f"INSERT INTO accounting_outbox VALUES ('{deployment_id}-ao', ?, 'x')",  # noqa: S608
        (deployment_id,),
    )
    conn.execute(f"INSERT INTO position_events VALUES ('{deployment_id}-pe', ?, 'x')", (deployment_id,))  # noqa: S608
    conn.execute(
        f"INSERT INTO position_state_snapshots VALUES ('{deployment_id}-pss', ?, 'x')",  # noqa: S608
        (deployment_id,),
    )
    conn.execute(f"INSERT INTO clob_orders VALUES ('{deployment_id}-clob', ?, 'x')", (deployment_id,))  # noqa: S608
    conn.execute(f"INSERT INTO position_registry VALUES ('{deployment_id}-reg', ?, 'x')", (deployment_id,))  # noqa: S608
    conn.execute(f"INSERT INTO migration_state VALUES ('{deployment_id}-mig', ?, 'x')", (deployment_id,))  # noqa: S608


def _count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]  # noqa: S608


class TestFreshTableLists:
    """Verify the module-level table-list constants are kept in sync."""

    def test_deployment_scoped_tables_are_deployment_id_keyed(self) -> None:
        assert "strategy_state" in _FRESH_DEPLOYMENT_ID_TABLES
        assert "portfolio_snapshots" in _FRESH_DEPLOYMENT_ID_TABLES
        assert "accounting_events" in _FRESH_DEPLOYMENT_ID_TABLES
        assert "position_events" in _FRESH_DEPLOYMENT_ID_TABLES

    def test_full_set_is_decision_plus_onchain(self) -> None:
        # The Anvil wipe-everything set is the union of the two categories.
        assert set(_FRESH_DEPLOYMENT_ID_TABLES) == set(_FRESH_DECISION_STATE_TABLES) | set(
            _FRESH_ONCHAIN_RECORD_TABLES
        )
        # The two categories must be disjoint — a table is either decision
        # state (wiped on --fresh) or the immutable on-chain record (preserved
        # on real networks), never both.
        assert not (set(_FRESH_DECISION_STATE_TABLES) & set(_FRESH_ONCHAIN_RECORD_TABLES))

    def test_books_tables_are_onchain_record(self) -> None:
        # VIB-5784: the immutable "books" must live in the preserve-on-mainnet
        # category so a --fresh relaunch cannot erase real executed trades. This
        # is an INDEPENDENT contract from the constant itself — it pins every
        # immutable table by name so accidentally moving one (e.g.
        # position_state_snapshots or position_registry) into decision state
        # fails here, not silently in production.
        for table in (
            "transaction_ledger",
            "accounting_events",
            "accounting_outbox",
            "position_events",
            "position_state_snapshots",
            "position_registry",
        ):
            assert table in _FRESH_ONCHAIN_RECORD_TABLES
            assert table not in _FRESH_DECISION_STATE_TABLES


class TestFreshFlagAnvil:
    """On Anvil all rows across all strategies must be deleted."""

    def test_all_tables_cleared(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)

        _create_db(db)
        with sqlite3.connect(str(db)) as conn:
            _seed_rows(conn, "dep-a")
            _seed_rows(conn, "dep-b")
            # Give strat-b a second row in position_events
            conn.execute("INSERT INTO position_events VALUES ('dep-b-pe2', 'dep-b', 'y')")

        _fresh_clear_state(sqlite3.connect(str(db)), "dep-a", is_anvil=True)

        with sqlite3.connect(str(db)) as conn:
            all_tables = [
                *_FRESH_DEPLOYMENT_ID_TABLES,
                "position_events",
            ]
            for table in all_tables:
                assert _count(conn, table) == 0, f"Expected {table} to be empty on Anvil"


class TestFreshFlagMainnet:
    """On a real network --fresh resets only the target strategy's DECISION
    state; the immutable on-chain accounting/ledger record is preserved so a
    relaunch cannot erase trades that already landed on-chain (VIB-5784)."""

    def test_target_decision_state_cleared_onchain_record_preserved(self) -> None:
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)

        _create_db(db)
        with sqlite3.connect(str(db)) as conn:
            _seed_rows(conn, "dep-target")
            _seed_rows(conn, "dep-other")

        _fresh_clear_state(sqlite3.connect(str(db)), "dep-target", is_anvil=False)

        with sqlite3.connect(str(db)) as conn:
            # Decision-state tables: only the target's row is removed.
            for table in _FRESH_DECISION_STATE_TABLES:
                rows = conn.execute(
                    f"SELECT deployment_id FROM {table}"  # noqa: S608
                ).fetchall()
                assert len(rows) == 1, f"Expected exactly 1 row in {table} after mainnet --fresh, got {len(rows)}"
                assert rows[0][0] == "dep-other", f"Surviving row in {table} should be 'dep-other', got {rows[0][0]!r}"
            # On-chain record tables: BOTH deployments' rows survive — --fresh
            # must never delete the immutable record of executed activity.
            for table in _FRESH_ONCHAIN_RECORD_TABLES:
                ids = {
                    row[0]
                    for row in conn.execute(f"SELECT deployment_id FROM {table}").fetchall()  # noqa: S608
                }
                assert ids == {"dep-target", "dep-other"}, (
                    f"{table} must preserve BOTH deployments on mainnet --fresh, got {ids!r}"
                )

    def test_onchain_record_preserved_even_when_only_in_outbox(self) -> None:
        """The immutable record is preserved even when the deployment_id only
        appears in accounting_outbox (outbox not yet drained into accounting_events)."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)

        _create_db(db)
        with sqlite3.connect(str(db)) as conn:
            # Target strategy: decision state + an undrained outbox event + a position event.
            conn.execute("INSERT INTO strategy_state VALUES ('dep-outbox', 'x')")
            conn.execute("INSERT INTO accounting_outbox VALUES ('ao-target', 'dep-outbox', 'x')")
            conn.execute("INSERT INTO position_events VALUES ('pe-outbox', 'dep-outbox', 'x')")
            # Unrelated strategy that must also be preserved.
            conn.execute("INSERT INTO strategy_state VALUES ('dep-other', 'x')")
            conn.execute("INSERT INTO accounting_outbox VALUES ('ao-other', 'dep-other', 'x')")
            conn.execute("INSERT INTO position_events VALUES ('pe-other', 'dep-other', 'x')")

        _fresh_clear_state(sqlite3.connect(str(db)), "dep-outbox", is_anvil=False)

        with sqlite3.connect(str(db)) as conn:
            # Target's decision state is reset...
            assert (
                conn.execute("SELECT COUNT(*) FROM strategy_state WHERE deployment_id = 'dep-outbox'").fetchone()[0] == 0
            )
            # ...but its undrained outbox event AND position event survive.
            assert (
                conn.execute("SELECT COUNT(*) FROM accounting_outbox WHERE deployment_id = 'dep-outbox'").fetchone()[0]
                == 1
            ), "undrained accounting_outbox must survive mainnet --fresh (it still needs draining)"
            assert (
                conn.execute("SELECT COUNT(*) FROM position_events WHERE deployment_id = 'dep-outbox'").fetchone()[0]
                == 1
            ), "position_events for target must survive mainnet --fresh"
            # Other strategy untouched.
            assert (
                conn.execute("SELECT COUNT(*) FROM position_events WHERE deployment_id = 'dep-other'").fetchone()[0] == 1
            )

    def test_vib_5784_relaunch_does_not_drop_prior_executed_trade(self) -> None:
        """Regression for VIB-5784.

        Reproduces the confirmed mechanism: launch 1 executes + persists a
        balancing SWAP (its transaction_ledger + accounting_events rows), the
        process is restarted, and launch 2 boots with --fresh on the SAME
        deterministic deployment_id. The --fresh clear must NOT delete the
        already-executed SWAP's books; it may only reset decision state.
        """
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)

        _create_db(db)
        deployment_id = "deployment:5f50490f814c"
        with sqlite3.connect(str(db)) as conn:
            # Launch 1 aftermath: strategy decision state + the executed SWAP's books.
            conn.execute("INSERT INTO strategy_state VALUES (?, 'stale-decision')", (deployment_id,))
            conn.execute(
                "INSERT INTO transaction_ledger VALUES ('swap-ledger-1', ?, 'SWAP 0x3d3189')",
                (deployment_id,),
            )
            conn.execute(
                "INSERT INTO accounting_events VALUES ('swap-acc-1', ?, 'SWAP')",
                (deployment_id,),
            )

        # Launch 2 boots with --fresh (real network).
        _fresh_clear_state(sqlite3.connect(str(db)), deployment_id, is_anvil=False)

        with sqlite3.connect(str(db)) as conn:
            # Decision state was reset...
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM strategy_state WHERE deployment_id = ?", (deployment_id,)
                ).fetchone()[0]
                == 0
            ), "--fresh should reset strategy decision state"
            # ...but the executed SWAP's books MUST survive (this is the bug).
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM transaction_ledger WHERE deployment_id = ?", (deployment_id,)
                ).fetchone()[0]
                == 1
            ), "VIB-5784: transaction_ledger row for an executed SWAP must survive a --fresh relaunch"
            assert (
                conn.execute(
                    "SELECT COUNT(*) FROM accounting_events WHERE deployment_id = ?", (deployment_id,)
                ).fetchone()[0]
                == 1
            ), "VIB-5784: accounting_events row for an executed SWAP must survive a --fresh relaunch"

    def test_anvil_relaunch_still_wipes_everything(self) -> None:
        """On Anvil the fork reset invalidates the on-chain record, so --fresh
        still wipes the ledger/accounting rows too (VIB-2573 preserved)."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)

        _create_db(db)
        deployment_id = "deployment:5f50490f814c"
        with sqlite3.connect(str(db)) as conn:
            conn.execute("INSERT INTO strategy_state VALUES (?, 'x')", (deployment_id,))
            conn.execute("INSERT INTO transaction_ledger VALUES ('t', ?, 'SWAP')", (deployment_id,))
            conn.execute("INSERT INTO accounting_events VALUES ('a', ?, 'SWAP')", (deployment_id,))

        _fresh_clear_state(sqlite3.connect(str(db)), deployment_id, is_anvil=True)

        with sqlite3.connect(str(db)) as conn:
            for table in ("strategy_state", "transaction_ledger", "accounting_events"):
                assert _count(conn, table) == 0, f"Anvil --fresh must wipe {table}"

    def test_missing_tables_do_not_raise(self) -> None:
        """Older DBs without all tables should not error."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            db = Path(f.name)
        # Only create strategy_state — no other tables
        with sqlite3.connect(str(db)) as conn:
            conn.execute("CREATE TABLE strategy_state (deployment_id TEXT, data TEXT)")
            conn.execute("INSERT INTO strategy_state VALUES ('s1', 'x')")
        _fresh_clear_state(sqlite3.connect(str(db)), "s1", is_anvil=False)
        with sqlite3.connect(str(db)) as conn:
            assert _count(conn, "strategy_state") == 0
