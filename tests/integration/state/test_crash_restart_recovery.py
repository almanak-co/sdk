"""Crash-restart recovery integration tests for the local SQLite stack.

Contract under test (VIB-3467 / VIB-3156 / plan 011):
  - Exactly-once outbox replay: crash before drain -> restart -> drain_pending
    replays all pending rows exactly once, with no duplicate accounting_events.
  - Re-drain idempotency: drain_pending on fully-processed rows returns 0;
    drain_one on a processed row returns True with no new events.
  - Mid-drain crash recovery (c-i): outbox row stuck at 'processing' (crash
    between mark-processing and event write) is retried exactly once on restart.
  - Mid-drain crash recovery (c-ii): outbox row at 'processing' with event
    already written (crash between event write and mark-processed) is recovered
    via the idempotency guard - no duplicate event.
  - State CAS / version survival: iteration state written before crash is
    readable post-restart with correct payload, version, and checksum; stale
    expected_version raises StateConflictError.

Scope: SQLiteStore + AccountingProcessor only. No gateway, no Anvil, no network.

Crash simulation — why `del` without `close()`:
  A real OS-level crash never runs graceful shutdown.  Routing crash simulation
  through `close()` would mask flush-on-close regressions — exactly the class
  this test is designed to guard against.  On-disk durability is guaranteed by
  per-write committed transactions with PRAGMA synchronous=FULL (VIB-3156), not
  by `close()`, so the committed prefix is intact the moment `del` fires.
  CPython's reference-counting semantics make the abandonment deterministic in
  this CPython-only repo; each test uses an isolated `tmp_path` database, so
  there is no shared state between tests and SQLite tolerates the second
  connection opened by the restarted instance.
  Short pointer at each site: "Crash: see module docstring — deliberate, no close()."

See: plans/011-crash-recovery-integration-test.md
"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime

import pytest

from almanak.framework.accounting.basis import FIFOBasisStore
from almanak.framework.accounting.processor import AccountingProcessor, write_outbox_entry
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import (
    StateConflictError,
    StateData,
    StateManager,
    StateManagerConfig,
)

# Single deployment identity used throughout this test module.
DEP = "deployment:crashtest0123"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ledger_entry(
    ledger_id: str,
    token_in: str = "WETH",
    token_out: str = "USDC",
) -> LedgerEntry:
    """Build a deterministic SWAP LedgerEntry.

    Symbol-shaped token_in/token_out values like 'WETH'/'USDC' short-circuit
    in resolve_swap_token_symbol (non-address input is returned unchanged) so
    no resolver or network call is made. execution_mode='paper' keeps the
    mode-aware augment path from taking the live-raise branch.
    """
    return LedgerEntry(
        id=ledger_id,
        cycle_id="cycle-1",
        deployment_id=DEP,
        execution_mode="paper",
        timestamp=datetime(2026, 6, 11, 12, 0, 0, tzinfo=UTC),
        intent_type="SWAP",
        token_in=token_in,
        amount_in="1.5",
        token_out=token_out,
        amount_out="3000",
        tx_hash="0x" + uuid.uuid4().hex,
        chain="arbitrum",
        protocol="uniswap_v3",
        success=True,
    )


async def _write_row(store: SQLiteStore, ledger_id: str) -> str:
    """Write a ledger + outbox row pair in production success-path order.

    Blueprint 02/27 'enrich -> ledger -> outbox+fire': ledger first, then
    outbox. Returns the outbox row id (asserted truthy).
    """
    entry = _make_ledger_entry(ledger_id)
    await store.save_ledger_entry(entry)
    outbox_id = await write_outbox_entry(
        store,
        deployment_id=DEP,
        cycle_id="cycle-1",
        ledger_entry_id=ledger_id,
        intent_type="SWAP",
        wallet_address="0xwallet",
    )
    assert outbox_id, f"write_outbox_entry returned falsy for ledger_id={ledger_id}"
    return outbox_id


def _db_counts(db_path: str, ledger_id: str) -> tuple[int, str | None]:
    """Return (accounting_events count, outbox status) for a given ledger_id.

    Uses a raw sqlite3 connection so assertions are independent of the store
    under test - reading through the same object would hide bugs in the reader.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        events_cur = conn.execute(
            "SELECT COUNT(*) FROM accounting_events WHERE ledger_entry_id = ?",
            (ledger_id,),
        )
        events_count: int = events_cur.fetchone()[0]

        status_cur = conn.execute(
            "SELECT status FROM accounting_outbox WHERE ledger_entry_id = ?",
            (ledger_id,),
        )
        row = status_cur.fetchone()
        status: str | None = row["status"] if row else None
    finally:
        conn.close()
    return events_count, status


def _total_accounting_events(db_path: str) -> int:
    """Return total row count across all accounting_events."""
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT COUNT(*) FROM accounting_events")
        return cur.fetchone()[0]
    finally:
        conn.close()


def _store(db_path: str) -> SQLiteStore:
    return SQLiteStore(SQLiteConfig(db_path=db_path))


def _processor(store: SQLiteStore) -> AccountingProcessor:
    """Build an AccountingProcessor backed by a SQLiteStore.

    AccountingProcessor resolves every persistence call via hasattr duck-typing
    (processor.py:521-548) and SQLiteStore implements all six methods natively,
    so passing SQLiteStore directly as state_manager is valid.
    """
    return AccountingProcessor(store, FIFOBasisStore(), deployment_id=DEP)


# ---------------------------------------------------------------------------
# Scenario (a) + (b): crash before drain -> exactly-once replay; re-drain -> zero
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_crash_before_drain_then_restart_replays_exactly_once(tmp_path):
    """Scenario (a): crash before drain -> restart -> drain_pending replays exactly once.

    Instance A writes two ledger+outbox pairs with DISTINCT token pairs
    (WETH->USDC and DAI->ARB) to keep FIFO lots from interacting, then
    crashes without draining. Instance B (fresh store, same file) drains.
    """
    db = str(tmp_path / "crash.db")
    lid1 = "ledger-crash-a-1"
    lid2 = "ledger-crash-a-2"

    # --- Instance A (will crash) ---
    store_a = _store(db)
    await store_a.initialize()
    await _write_row(store_a, lid1)
    # lid2 uses a disjoint token pair (no asset shared with lid1) so FIFO lots cannot interact.
    # lid1 pair: WETH -> USDC. lid2 pair: DAI -> ARB. Zero symbol overlap.
    entry2 = _make_ledger_entry(lid2, token_in="DAI", token_out="ARB")
    await store_a.save_ledger_entry(entry2)
    await write_outbox_entry(
        store_a,
        deployment_id=DEP,
        cycle_id="cycle-1",
        ledger_entry_id=lid2,
        intent_type="SWAP",
        wallet_address="0xwallet",
    )
    # Crash: deliberate instance abandonment without close() — see module docstring.
    del store_a

    # --- Instance B (restart) ---
    store_b = _store(db)
    await store_b.initialize()
    proc_b = _processor(store_b)
    drained = await proc_b.drain_pending()

    # Scenario (a): exactly-once drain.
    assert drained == 2, f"expected 2 drained, got {drained}"
    for lid in (lid1, lid2):
        events_count, status = _db_counts(db, lid)
        assert events_count == 1, f"expected 1 accounting event for {lid}, got {events_count}"
        assert status == "processed", f"expected outbox status 'processed' for {lid}, got {status!r}"
    assert _total_accounting_events(db) == 2, "total accounting_events must be exactly 2"

    # Scenario (b): re-drain returns 0 and does not re-emit.
    drained_again = await proc_b.drain_pending()
    assert drained_again == 0, f"re-drain should return 0, got {drained_again}"

    # drain_one on a processed row returns True via the early-return
    # at processor.py:173-174; no new events emitted.
    result = await proc_b.drain_one(lid1)
    assert result is True, f"drain_one on processed row should return True, got {result}"
    assert _total_accounting_events(db) == 2, "drain_one on processed row must not re-emit"

    await store_b.close()


# ---------------------------------------------------------------------------
# Scenario (c-i): crash after 'processing' mark, before event write
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_crash_mid_drain_before_event_write_recovers_on_restart(tmp_path):
    """Scenario (c-i): 'processing' stuck row is retried exactly once on restart.

    Manually set outbox row to 'processing' to reproduce the exact durable
    state a crash between drain_one commit #1 (mark 'processing') and
    commit #2 (event write) leaves on disk:
      - outbox row: status='processing', attempts=0
      - NO accounting_events row for this ledger_entry_id

    This pins the get_outbox_pending docstring contract verbatim:
    "'processing' rows are included so that entries that were in-flight
    when the runner crashed are retried on restart rather than being
    permanently orphaned." (sqlite.py:4151+)
    """
    db = str(tmp_path / "midcrash_ci.db")
    lid3 = "ledger-crash-ci-3"

    # --- Instance A (will crash) ---
    store_a = _store(db)
    await store_a.initialize()
    outbox_id3 = await _write_row(store_a, lid3)

    # Simulate durable state of: crash between commit #1 and commit #2.
    # Manually set status to 'processing' (exactly what drain_one writes
    # at commit #1 before attempting the event write at commit #2).
    await store_a.update_outbox_entry(outbox_id3, "processing")

    # Verify pre-crash state: outbox 'processing', no accounting event yet.
    pre_events, pre_status = _db_counts(db, lid3)
    assert pre_status == "processing", f"setup error: expected 'processing', got {pre_status!r}"
    assert pre_events == 0, f"setup error: expected 0 events, got {pre_events}"

    # Crash: deliberate instance abandonment without close() — see module docstring.
    del store_a

    # --- Instance B (restart) ---
    store_b = _store(db)
    await store_b.initialize()
    proc_b = _processor(store_b)
    drained = await proc_b.drain_pending()

    assert drained == 1, f"expected 1 drained, got {drained}"
    events_count, status = _db_counts(db, lid3)
    assert events_count == 1, f"expected 1 accounting event for {lid3}, got {events_count}"
    assert status == "processed", f"expected outbox status 'processed' for {lid3}, got {status!r}"

    await store_b.close()


# ---------------------------------------------------------------------------
# Scenario (c-ii): crash after event write, before 'processed' mark
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_crash_mid_drain_after_event_write_is_idempotent_on_restart(tmp_path):
    """Scenario (c-ii): event already written; restart marks processed without re-emitting.

    Equivalence argument: drain_one's commits #2 (event) and #3 (mark processed)
    are separate committed transactions. A crash between them leaves on disk
    exactly {accounting_events row present, outbox 'processing', attempts=0,
    error=''} which is byte-for-byte the state this reconstruction produces
    (only updated_at differs, and nothing reads it for idempotency purposes).

    On restart, drain_one hits the idempotency guard at processor.py:186:
    _has_accounting_event_for_ledger returns True -> marks processed WITHOUT
    re-dispatching the handler -> no duplicate accounting event.
    """
    db = str(tmp_path / "midcrash_cii.db")
    lid4 = "ledger-crash-cii-4"

    # --- Instance A (will crash) ---
    store_a = _store(db)
    await store_a.initialize()
    outbox_id4 = await _write_row(store_a, lid4)
    proc_a = _processor(store_a)

    # Run drain_one to completion so the event is written and outbox is 'processed'.
    ok = await proc_a.drain_one(lid4)
    assert ok, "initial drain_one should succeed"
    events_after_drain, status_after_drain = _db_counts(db, lid4)
    assert events_after_drain == 1, "should have exactly 1 event after first drain"
    assert status_after_drain == "processed"

    # Now reconstruct the pre-crash state: rewind outbox to 'processing'
    # to simulate the disk snapshot at crash-between-commit-#2-and-commit-#3
    # (accounting_events row present, but outbox not yet marked 'processed').
    await store_a.update_outbox_entry(outbox_id4, "processing")

    # Verify reconstruction: event present, outbox back to 'processing'.
    events_reconstructed, status_reconstructed = _db_counts(db, lid4)
    assert events_reconstructed == 1, "reconstruction: event row must still exist"
    assert status_reconstructed == "processing", "reconstruction: outbox must be 'processing'"

    # Crash: deliberate instance abandonment without close() — see module docstring.
    del store_a, proc_a

    # --- Instance B (restart) ---
    store_b = _store(db)
    await store_b.initialize()
    proc_b = _processor(store_b)
    drained = await proc_b.drain_pending()

    # drain_pending returns 1: drain_one found the 'processing' row,
    # hit the idempotency guard (event row exists), marked processed, returned True.
    assert drained == 1, f"expected 1 drained via idempotency guard, got {drained}"

    events_count, status = _db_counts(db, lid4)
    # No duplicate: still exactly 1 event row (the guard prevented re-dispatch).
    assert events_count == 1, (
        f"idempotency guard must prevent duplicate emission; "
        f"expected 1 accounting event, got {events_count}"
    )
    assert status == "processed", f"expected outbox status 'processed', got {status!r}"
    # Total events across the DB is also exactly 1.
    assert _total_accounting_events(db) == 1, "total accounting_events must remain 1"

    await store_b.close()


# ---------------------------------------------------------------------------
# Scenario (d): iteration-state CAS/version survives restart
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_state_cas_version_survives_restart(tmp_path):
    """Scenario (d): version + payload + checksum survive crash; stale CAS raises.

    Instance A writes state twice (UPSERT then CAS bump), then crashes.
    Instance B loads the state and verifies the payload, version, and
    checksum are intact (VIB-3156 'never a torn state' invariant).
    The stale-version guard asserts a pre-crash expected_version cannot
    clobber the recovered state.
    """
    db = str(tmp_path / "state_cas.db")

    # --- Instance A (will crash) ---
    store_a = _store(db)
    sm_a = StateManager(StateManagerConfig(), warm_backend=store_a)
    await sm_a.initialize()

    # Initial UPSERT insert (version=1).
    await sm_a.save_state(StateData(deployment_id=DEP, version=1, state={"iteration": 1}))

    # CAS bump: read, mutate, save with expected_version.
    loaded = await sm_a.load_state(DEP)
    loaded.state["iteration"] = 2
    saved = await sm_a.save_state(loaded, expected_version=loaded.version)
    version_after = saved.version  # expected to be 2

    # Crash: deliberate instance abandonment without close() — see module docstring.
    del sm_a, store_a

    # --- Instance B (restart) ---
    store_b = _store(db)
    sm_b = StateManager(StateManagerConfig(), warm_backend=store_b)
    await sm_b.initialize()

    final = await sm_b.load_state(DEP)

    assert final.state == {"iteration": 2}, (
        f"post-restart state payload mismatch: got {final.state!r}"
    )
    assert final.version == version_after, (
        f"post-restart version mismatch: expected {version_after}, got {final.version}"
    )
    assert final.verify_checksum() is True, (
        "post-restart checksum verification failed (VIB-3156 torn-state invariant)"
    )

    # Stale-CAS guard: a writer holding a pre-crash version (1) cannot clobber
    # the recovered state (version 2).
    with pytest.raises(StateConflictError):
        await sm_b.save_state(
            StateData(deployment_id=DEP, version=final.version, state={"iteration": 99}),
            expected_version=1,
        )

    # Clean up instance B properly (B did not crash).
    await sm_b.close()
