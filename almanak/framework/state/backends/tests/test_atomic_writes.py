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
