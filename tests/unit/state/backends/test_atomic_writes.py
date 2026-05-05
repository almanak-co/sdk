"""Crash-recovery tests for atomic state writes (VIB-3156).

Verifies the durability invariant documented in
``almanak.framework.state.state_manager``:

    A successful ``save_state()`` call guarantees durability or raises.
    State rows never exist on disk with a version bump but an invalid
    checksum.

These tests simulate process crashes at multiple points during a
SQLite state write and assert that on recovery the store always reads
back a self-consistent prior state -- never a torn row where version
has advanced but the stored checksum no longer matches state_data.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import sqlite3
import tempfile

import pytest
import pytest_asyncio

from almanak.framework.state.backends.sqlite import (
    SQLiteConfig,
    SQLiteStore,
)
from almanak.framework.state.state_manager import (
    SQLiteConfigLight,
    StateData,
    StateManager,
    StateManagerConfig,
    WarmBackendType,
)

pytestmark = pytest.mark.asyncio


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def temp_db_path():
    fd, path = tempfile.mkstemp(suffix=".db", prefix="atomic_")
    os.close(fd)
    yield path
    for ext in ("", "-wal", "-shm", "-journal"):
        try:
            os.unlink(path + ext)
        except FileNotFoundError:
            pass


@pytest_asyncio.fixture
async def store(temp_db_path):
    s = SQLiteStore(SQLiteConfig(db_path=temp_db_path, wal_mode=True))
    await s.initialize()
    yield s
    await s.close()


# =============================================================================
# Helpers
# =============================================================================


def _read_row_raw(db_path: str, strategy_id: str) -> dict | None:
    """Read the raw state row via a fresh sqlite3 connection.

    Bypasses SQLiteStore so we can observe exactly what is on disk, e.g.
    after simulating a crash before the owning store had a chance to
    commit.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT strategy_id, version, state_data, checksum FROM strategy_state WHERE strategy_id = ?",
            (strategy_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _recompute_checksum(state_data_json: str) -> str:
    """Recompute the checksum the way SQLiteStore.save writes it.

    NOTE: SQLiteStore.save serializes with ``sort_keys=True`` and
    hashes the resulting JSON. Keep in sync with that function.
    """
    parsed = json.loads(state_data_json)
    canonical = json.dumps(parsed, sort_keys=True, default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


# =============================================================================
# Happy path: atomic write succeeds, checksum matches
# =============================================================================


class TestAtomicWriteHappyPath:
    async def test_save_then_reload_checksum_matches(self, store, temp_db_path):
        """After a successful save, the on-disk row is self-consistent."""
        state = StateData(
            strategy_id="atomic-happy",
            version=1,
            state={"nonce": 7, "last_tx": "0xabc"},
        )
        await store.save(state)

        row = _read_row_raw(temp_db_path, "atomic-happy")
        assert row is not None
        assert row["version"] == 1
        assert row["checksum"] == _recompute_checksum(row["state_data"])

    async def test_cas_update_keeps_checksum_consistent(self, store, temp_db_path):
        """CAS update bumps version and rewrites checksum atomically."""
        state = StateData(strategy_id="atomic-cas", version=1, state={"n": 1})
        await store.save(state)

        state.state["n"] = 2
        await store.save(state, expected_version=1)

        row = _read_row_raw(temp_db_path, "atomic-cas")
        assert row is not None
        assert row["version"] == 2
        assert row["checksum"] == _recompute_checksum(row["state_data"])
        assert json.loads(row["state_data"]) == {"n": 2}


# =============================================================================
# Crash simulation: abort transaction before COMMIT
# =============================================================================


class TestCrashBeforeCommit:
    """Simulate a crash between the UPDATE statement and COMMIT.

    This is the classic torn-write risk: the caller has written new
    bytes into the transaction but a crash prevents the commit. SQLite
    transactions guarantee the uncommitted bytes are never visible to
    a later reader, even after reopening the database. We assert that
    property here.
    """

    async def test_crash_before_commit_preserves_prior_version(self, store, temp_db_path):
        """A fresh read after crash returns the prior durable row."""
        # First, a good save to establish baseline state.
        v1 = StateData(strategy_id="atomic-crash", version=1, state={"nonce": 1})
        await store.save(v1)

        # Now perform a raw UPDATE inside a transaction, simulate the
        # process dying before COMMIT by rolling back, then reopen the
        # store and check recovery. We intentionally use a separate
        # connection to avoid interfering with the store's own state.
        conn = sqlite3.connect(temp_db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            # Write would-be-v2 bytes with a checksum that matches the
            # new body. The key is we never COMMIT.
            new_state_json = json.dumps({"nonce": 2}, sort_keys=True, default=str)
            new_checksum = hashlib.sha256(new_state_json.encode()).hexdigest()
            conn.execute(
                """
                UPDATE strategy_state
                SET version = version + 1,
                    state_data = ?,
                    checksum = ?
                WHERE strategy_id = ?
                """,
                (new_state_json, new_checksum, "atomic-crash"),
            )
            # Simulate crash: close without committing. SQLite will
            # discard the uncommitted changes on the next open.
            conn.execute("ROLLBACK")
        finally:
            conn.close()

        # Reopen via the store (same code path a restarted strategy takes).
        await store.close()
        recovered = SQLiteStore(SQLiteConfig(db_path=temp_db_path, wal_mode=True))
        await recovered.initialize()
        try:
            loaded = await recovered.get("atomic-crash")
            assert loaded is not None
            # Version did NOT advance -- the torn write never landed.
            assert loaded.version == 1
            assert loaded.state == {"nonce": 1}
            # And the checksum the store wrote is self-consistent.
            assert loaded.verify_checksum()
        finally:
            await recovered.close()

    async def test_repeated_crashes_never_produce_torn_state(self, temp_db_path):
        """Drive multiple save attempts, simulating crash at random points.

        On recovery, whichever version landed last must have a checksum
        matching its state_data. A torn state (version bumped, checksum
        stale) would violate the VIB-3156 invariant.
        """
        import random

        rng = random.Random(0)

        # Baseline
        s0 = SQLiteStore(SQLiteConfig(db_path=temp_db_path, wal_mode=True))
        await s0.initialize()
        await s0.save(StateData(strategy_id="rep", version=1, state={"n": 0}))
        await s0.close()

        last_successful_state = {"n": 0}
        last_successful_version = 1

        # Drive 20 save attempts. For each, randomly pick "commit" or
        # "rollback before commit" to simulate a crash.
        for i in range(1, 21):
            new_state = {"n": i}
            if rng.random() < 0.5:
                # Successful save path: use the real store API.
                store = SQLiteStore(SQLiteConfig(db_path=temp_db_path, wal_mode=True))
                await store.initialize()
                try:
                    await store.save(
                        StateData(
                            strategy_id="rep",
                            version=last_successful_version,
                            state=new_state,
                        ),
                        expected_version=last_successful_version,
                    )
                    last_successful_state = new_state
                    last_successful_version += 1
                finally:
                    await store.close()
            else:
                # Crash path: start a write, roll back before commit.
                conn = sqlite3.connect(temp_db_path)
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    sj = json.dumps(new_state, sort_keys=True, default=str)
                    cs = hashlib.sha256(sj.encode()).hexdigest()
                    conn.execute(
                        "UPDATE strategy_state SET version = version + 1, "
                        "state_data = ?, checksum = ? WHERE strategy_id = ?",
                        (sj, cs, "rep"),
                    )
                    conn.execute("ROLLBACK")
                finally:
                    conn.close()

            # After every iteration, recovery must always see a
            # self-consistent row matching the last successful write.
            recovered = SQLiteStore(SQLiteConfig(db_path=temp_db_path, wal_mode=True))
            await recovered.initialize()
            try:
                loaded = await recovered.get("rep")
                assert loaded is not None
                assert loaded.verify_checksum(), f"iter {i}: torn state detected (version={loaded.version})"
                assert loaded.state == last_successful_state
                assert loaded.version == last_successful_version
            finally:
                await recovered.close()


# =============================================================================
# Checksum-mismatch pre-commit guard
# =============================================================================


class TestChecksumInvariantOnDisk:
    """Every on-disk row is self-consistent after save completes.

    The VIB-3156 invariant: state_data and checksum land in the same
    atomic transaction; a fresh reader always sees a row whose stored
    checksum matches a re-hash of its stored state_data.
    """

    async def test_reloaded_state_always_verifies(self, temp_db_path):
        config = StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path=temp_db_path, wal_mode=True),
            load_state_on_startup=False,
        )
        mgr = StateManager(config)
        await mgr.initialize()
        try:
            # Drive several saves, each with CAS. Each reload must
            # self-verify.
            state = StateData(strategy_id="inv", version=1, state={"n": 0})
            saved = await mgr.save_state(state)
            for i in range(1, 6):
                saved.state = {"n": i}
                saved = await mgr.save_state(saved, expected_version=saved.version)

            mgr.invalidate_hot_cache("inv")
            loaded = await mgr.load_state("inv")
            assert loaded.verify_checksum()
            # Raw row is also self-consistent.
            row = _read_row_raw(temp_db_path, "inv")
            assert row is not None
            assert row["checksum"] == _recompute_checksum(row["state_data"])
        finally:
            await mgr.close()

    async def test_sqlite_backend_refuses_torn_serialization(self, store):
        """Direct SQLiteStore.save guards against inconsistent inputs.

        If a caller were to supply a StateData where the pre-hashed
        state_data would not match the stored checksum (normally
        impossible because the backend computes both from the same
        bytes), the backend re-validates before commit.
        """
        # This is a defense-in-depth check; SQLiteStore.save always
        # computes checksum from state_json internally, so the
        # re-validation uses the same source and must pass. If it ever
        # diverged (e.g., non-deterministic JSON ordering regression)
        # SQLiteBackendError would fire.
        s = StateData(strategy_id="dd", version=1, state={"a": [1, 2, 3]})
        await store.save(s)
        loaded = await store.get("dd")
        assert loaded is not None
        assert loaded.verify_checksum()


# =============================================================================
# Concurrent-writer serialization (regression guard)
# =============================================================================


class TestConcurrentWritersAreSerialized:
    """The BEGIN IMMEDIATE + _db_lock combo ensures CAS-correctness.

    Two concurrent CAS saves at the same expected_version must produce
    exactly one success and one StateConflictError.
    """

    async def test_concurrent_cas_one_wins(self, store):
        from almanak.framework.state.state_manager import StateConflictError

        await store.save(StateData(strategy_id="conc", version=1, state={"n": 0}))

        async def writer(value: int) -> bool:
            try:
                await store.save(
                    StateData(strategy_id="conc", version=1, state={"n": value}),
                    expected_version=1,
                )
                return True
            except StateConflictError:
                return False

        results = await asyncio.gather(writer(1), writer(2))
        # Exactly one must succeed.
        assert sum(results) == 1


# =============================================================================
# VIB-3181: portfolio_snapshots and clob_orders atomic-write extension
# =============================================================================


def _make_snapshot(strategy_id: str, iteration: int, total_usd: str = "1000"):
    """Build a minimal PortfolioSnapshot for atomic-write tests."""
    from datetime import UTC, datetime
    from decimal import Decimal

    from almanak.framework.portfolio.models import (
        PortfolioSnapshot,
        TokenBalance,
        ValueConfidence,
    )

    return PortfolioSnapshot(
        timestamp=datetime.now(UTC),
        strategy_id=strategy_id,
        total_value_usd=Decimal(total_usd),
        available_cash_usd=Decimal("500"),
        value_confidence=ValueConfidence.HIGH,
        deployed_capital_usd=Decimal("500"),
        wallet_total_value_usd=Decimal(total_usd),
        wallet_balances=[
            TokenBalance(
                symbol="USDC",
                balance=Decimal("500"),
                value_usd=Decimal("500"),
                address="0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
                price_usd=Decimal("1"),
            ),
        ],
        token_prices={"avalanche:0xusdc": {"price_usd": "1.0", "symbol": "USDC", "decimals": 6}},
        chain="avalanche",
        iteration_number=iteration,
    )


def _make_clob_order(order_id: str, status_value: str = "live", filled: str = "0"):
    """Build a minimal ClobOrderState for atomic-write tests."""
    from datetime import UTC, datetime
    from decimal import Decimal

    from almanak.framework.execution.clob_handler import ClobOrderState, ClobOrderStatus

    return ClobOrderState(
        order_id=order_id,
        market_id="market-x",
        token_id="token-yes",
        side="BUY",
        status=ClobOrderStatus(status_value),
        price=Decimal("0.55"),
        size=Decimal("100"),
        filled_size=Decimal(filled),
        order_type="GTC",
        intent_id="intent-1",
        submitted_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
    )


class TestPortfolioSnapshotAtomicWrites:
    """save_portfolio_snapshot wraps the write in BEGIN IMMEDIATE / COMMIT.

    The legacy implementation called raw ``_conn.execute`` + ``_conn.commit``
    outside ``_db_lock``, which left the write unserialized against
    concurrent ``save()`` / ``save_snapshot_and_metrics()`` callers and
    skipped the explicit transaction boundary other state writers rely on.
    """

    async def test_happy_path_round_trip(self, store, temp_db_path):
        """A successful save lands a self-consistent row on disk."""
        snap = _make_snapshot("snap-happy", iteration=1, total_usd="2500.50")
        row_id = await store.save_portfolio_snapshot(snap)
        assert row_id > 0

        conn = sqlite3.connect(temp_db_path)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT total_value_usd, iteration_number, wallet_balances_json "
                "FROM portfolio_snapshots WHERE strategy_id = ?",
                ("snap-happy",),
            ).fetchone()
        finally:
            conn.close()
        assert row is not None
        assert row["total_value_usd"] == "2500.50"
        assert row["iteration_number"] == 1
        # wallet_balances_json must be a parseable JSON list
        parsed = json.loads(row["wallet_balances_json"])
        assert isinstance(parsed, list) and len(parsed) == 1

    async def test_crash_before_commit_preserves_prior_row(self, store, temp_db_path):
        """A separate connection that ROLLBACKs leaves no torn row."""
        baseline = _make_snapshot("snap-crash", iteration=1, total_usd="100")
        await store.save_portfolio_snapshot(baseline)

        conn = sqlite3.connect(temp_db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "INSERT INTO portfolio_snapshots ("
                "strategy_id, timestamp, iteration_number, total_value_usd, "
                "available_cash_usd, deployed_capital_usd, wallet_total_value_usd, "
                "value_confidence, positions_json, token_prices_json, "
                "wallet_balances_json, chain, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "snap-crash",
                    "2030-01-01T00:00:00+00:00",
                    99,
                    "9999",
                    "0",
                    "0",
                    "9999",
                    "HIGH",
                    "[]",
                    "{}",
                    "[]",
                    "avalanche",
                    "2030-01-01T00:00:00+00:00",
                ),
            )
            # Simulate crash before COMMIT.
            conn.execute("ROLLBACK")
        finally:
            conn.close()

        # Reopen and confirm the torn write never landed: row count for
        # iteration 99 (the would-be torn row) is 0; baseline iteration
        # is intact.
        await store.close()
        recovered = SQLiteStore(SQLiteConfig(db_path=temp_db_path, wal_mode=True))
        await recovered.initialize()
        try:
            conn = sqlite3.connect(temp_db_path)
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    "SELECT iteration_number, total_value_usd FROM portfolio_snapshots "
                    "WHERE strategy_id = ? ORDER BY iteration_number",
                    ("snap-crash",),
                ).fetchall()
            finally:
                conn.close()
            assert [r["iteration_number"] for r in rows] == [1]
            assert rows[0]["total_value_usd"] == "100"
        finally:
            await recovered.close()

    async def test_phase4_identity_fields_preserved_on_conflict(self, store, temp_db_path):
        """save_portfolio_snapshot must NOT clobber phase-4 identity fields.

        save_snapshot_and_metrics writes (deployment_id, cycle_id,
        execution_mode) for the same (strategy_id, timestamp). A
        subsequent save_portfolio_snapshot for the same key must
        preserve those fields rather than reset them to '' default
        (legacy INSERT OR REPLACE behavior — VIB-3181 follow-up,
        CodeRabbit review on PR #2006).
        """
        # Seed a row carrying phase-4 metadata directly via SQL so the
        # test does not depend on save_snapshot_and_metrics internals.
        from datetime import UTC, datetime

        ts = datetime.now(UTC)
        ts_iso = ts.isoformat()
        conn = sqlite3.connect(temp_db_path)
        try:
            conn.execute(
                "INSERT INTO portfolio_snapshots ("
                "strategy_id, deployment_id, cycle_id, execution_mode, "
                "timestamp, iteration_number, total_value_usd, "
                "available_cash_usd, deployed_capital_usd, wallet_total_value_usd, "
                "value_confidence, positions_json, token_prices_json, "
                "wallet_balances_json, chain, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "snap-phase4",
                    "deploy-abc",  # deployment_id (must be preserved)
                    "cycle-42",  # cycle_id (must be preserved)
                    "live",  # execution_mode (must be preserved)
                    ts_iso,
                    1,
                    "1000",
                    "500",
                    "500",
                    "1000",
                    "HIGH",
                    "[]",
                    "{}",
                    "[]",
                    "avalanche",
                    ts_iso,
                ),
            )
            conn.commit()
        finally:
            conn.close()

        # Now write via save_portfolio_snapshot with the SAME
        # (strategy_id, timestamp) — UPSERT path triggered.
        snap = _make_snapshot("snap-phase4", iteration=2, total_usd="2000")
        snap.timestamp = ts
        row_id = await store.save_portfolio_snapshot(snap)
        assert row_id > 0

        # Read back and assert phase-4 fields survived.
        conn = sqlite3.connect(temp_db_path)
        conn.row_factory = sqlite3.Row
        try:
            row = conn.execute(
                "SELECT deployment_id, cycle_id, execution_mode, "
                "iteration_number, total_value_usd "
                "FROM portfolio_snapshots WHERE strategy_id = ?",
                ("snap-phase4",),
            ).fetchone()
        finally:
            conn.close()
        assert row is not None
        # New snapshot fields applied:
        assert row["iteration_number"] == 2
        assert row["total_value_usd"] == "2000"
        # Phase-4 identity preserved (the regression guard):
        assert row["deployment_id"] == "deploy-abc"
        assert row["cycle_id"] == "cycle-42"
        assert row["execution_mode"] == "live"

    async def test_concurrent_writers_serialized(self, store, temp_db_path):
        """Concurrent saves with distinct timestamps both land cleanly.

        Without ``BEGIN IMMEDIATE`` under ``_db_lock`` two concurrent
        writers could race on the underlying connection; with the
        atomic-write pattern, each transaction commits in turn.
        """
        snaps = [_make_snapshot("snap-conc", iteration=i, total_usd=f"{1000 + i}") for i in range(8)]
        # Force distinct timestamps so INSERT OR REPLACE doesn't collapse rows.
        for i, s in enumerate(snaps):
            s.timestamp = s.timestamp.replace(microsecond=i)

        results = await asyncio.gather(*[store.save_portfolio_snapshot(s) for s in snaps])
        assert all(r > 0 for r in results)
        assert len(set(results)) == len(snaps)  # all distinct row IDs

        conn = sqlite3.connect(temp_db_path)
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM portfolio_snapshots WHERE strategy_id = ?",
                ("snap-conc",),
            ).fetchone()[0]
        finally:
            conn.close()
        assert count == len(snaps)


class TestClobOrderAtomicWrites:
    """save_clob_order wraps the SELECT-then-INSERT/UPDATE in one transaction.

    The legacy implementation issued a SELECT to choose between INSERT
    and UPDATE, then ran the second statement and committed on a raw
    autocommit connection.  Two writers racing on the same ``order_id``
    could both observe "missing" and both attempt INSERT, producing a
    UNIQUE constraint failure or a duplicate write window.  Wrapping in
    ``BEGIN IMMEDIATE`` under ``_db_lock`` collapses that race: writer
    A finishes before writer B starts its SELECT.
    """

    async def test_insert_then_update_round_trip(self, store, temp_db_path):
        """First save inserts; second save with same order_id updates."""
        from decimal import Decimal

        order = _make_clob_order("order-1", status_value="live")
        ok = await store.save_clob_order(order)
        assert ok is True

        # Update path: same order_id, new status + filled_size
        order.status = type(order.status)("matched")
        order.filled_size = Decimal("100")
        order.average_fill_price = Decimal("0.55")
        ok2 = await store.save_clob_order(order)
        assert ok2 is True

        conn = sqlite3.connect(temp_db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT status, filled_size FROM clob_orders WHERE order_id = ?",
                ("order-1",),
            ).fetchall()
        finally:
            conn.close()
        assert len(rows) == 1  # No duplicate inserts
        assert rows[0]["status"] == "matched"
        assert rows[0]["filled_size"] == "100"

    async def test_concurrent_inserts_no_duplicate(self, store, temp_db_path):
        """Two concurrent saves of the same NEW order_id must not duplicate.

        With the SELECT-then-INSERT race in the legacy code, both writers
        could see "missing" and both INSERT, producing either a UNIQUE
        violation (if a constraint exists) or two rows with the same
        ``order_id``.  Under BEGIN IMMEDIATE the second writer waits for
        the first to commit and then takes the UPDATE branch.
        """
        order_a = _make_clob_order("order-race", status_value="live", filled="10")
        order_b = _make_clob_order("order-race", status_value="live", filled="20")

        results = await asyncio.gather(
            store.save_clob_order(order_a),
            store.save_clob_order(order_b),
            return_exceptions=True,
        )
        # Both calls must succeed (no UNIQUE violation, no exception).
        for r in results:
            assert r is True, f"unexpected result {r!r}"

        conn = sqlite3.connect(temp_db_path)
        conn.row_factory = sqlite3.Row
        try:
            rows = conn.execute(
                "SELECT order_id, filled_size FROM clob_orders WHERE order_id = ?",
                ("order-race",),
            ).fetchall()
        finally:
            conn.close()
        assert len(rows) == 1, f"expected single row after race; got {len(rows)}"
        # Whichever writer committed last wins; we only assert no duplicate
        # rows landed.
        assert rows[0]["filled_size"] in ("10", "20")

    async def test_crash_before_commit_preserves_prior_row(self, store, temp_db_path):
        """A separate-connection ROLLBACK never advances the row."""
        from decimal import Decimal

        baseline = _make_clob_order("order-crash", status_value="live", filled="0")
        await store.save_clob_order(baseline)

        # Open a second sqlite3 connection, BEGIN IMMEDIATE, write a torn
        # update, then ROLLBACK.  Recovery must show the original "live" /
        # filled=0 row.
        conn = sqlite3.connect(temp_db_path)
        try:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "UPDATE clob_orders SET status = ?, filled_size = ?, updated_at = ? WHERE order_id = ?",
                ("matched", "9999", "2030-01-01T00:00:00+00:00", "order-crash"),
            )
            conn.execute("ROLLBACK")
        finally:
            conn.close()

        await store.close()
        recovered = SQLiteStore(SQLiteConfig(db_path=temp_db_path, wal_mode=True))
        await recovered.initialize()
        try:
            loaded = await recovered.get_clob_order("order-crash")
            assert loaded is not None
            assert loaded.status.value == "live"
            assert loaded.filled_size == Decimal("0")
        finally:
            await recovered.close()
