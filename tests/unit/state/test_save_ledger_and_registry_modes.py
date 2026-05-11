"""Unit tests for ``save_ledger_and_registry_atomic`` mode parameter (T24 / VIB-4210).

Per the UAT card §D1.S4 + §D2.M5 + §D3.F4 + §D3.F5: the atomic primitive
gains a ``mode`` parameter (default 'commit'; new value 'registry_reconciliation').
This file pins:

- mode='commit' (default): bit-identical to T11/T19 — writes ledger + registry + handle.
- mode='registry_reconciliation': SKIPS the ledger write; writes registry +
  handle ONLY, atomically inside the same transaction.
- Invalid mode value raises ValueError BEFORE opening the transaction.
- RegistryAutoCollisionError classification still fires under the new mode.
- DB-error rollback leaves all four persistence surfaces UNCHANGED.

These tests are unit-level: in-memory SQLite, no gateway boot, no network.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import UTC, datetime

import pytest
import pytest_asyncio

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.registry_errors import RegistryAutoCollisionError


@pytest_asyncio.fixture
async def store():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "test.db")
        s = SQLiteStore(SQLiteConfig(db_path=db_path, wal_mode=False))
        await s.initialize()
        yield s, db_path
        await s.close()


def _ledger(id_: str = "ledger-1", tx: str = "0xtxhash") -> LedgerEntry:
    return LedgerEntry(
        id=id_,
        cycle_id="cycle-1",
        strategy_id="TestStrat:abc",
        deployment_id="TestStrat:abc",
        execution_mode="live",
        timestamp=datetime(2026, 5, 12, 12, 0, 0, tzinfo=UTC),
        intent_type="LP_OPEN",
        token_in="USDC",
        amount_in="100",
        token_out="WETH",
        amount_out="0.04",
        effective_price="2500",
        slippage_bps=10.0,
        gas_used=200000,
        gas_usd="0.50",
        tx_hash=tx,
        chain="arbitrum",
        protocol="uniswap_v3",
        success=True,
        error="",
    )


def _registry(pih: str = "hash_a", handle: str | None = None, sgk: str = "arbitrum:pool_a") -> RegistryRow:
    return RegistryRow(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash=pih,
        semantic_grouping_key=sgk,
        grouping_policy_version="univ3_lp@v1",
        status="open",
        payload={"source": "reconciliation_discovery", "token_id": 12345},
        matching_policy_version=1,
        handle=handle,
        opened_at_block=1000,
        opened_tx="0xopen",
        last_reconciled_at_block=1234567,
    )


@pytest.mark.asyncio
async def test_default_mode_writes_ledger(store):
    """Default mode='commit' writes both ledger AND registry rows.

    Backward-compat invariant — this test pins T11/T19 behaviour against
    accidental regression from the new mode parameter.
    """
    s, db_path = store
    await s.save_ledger_and_registry_atomic(_ledger(), _registry(), None)  # mode default

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM transaction_ledger").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM position_registry").fetchone()[0] == 1
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_explicit_commit_mode_equals_default(store):
    """mode='commit' (explicit) === mode= (default). Wire-compat invariant."""
    s, db_path = store
    await s.save_ledger_and_registry_atomic(_ledger(), _registry(), None, "commit")

    conn = sqlite3.connect(db_path)
    try:
        assert conn.execute("SELECT COUNT(*) FROM transaction_ledger").fetchone()[0] == 1
        assert conn.execute("SELECT COUNT(*) FROM position_registry").fetchone()[0] == 1
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_registry_reconciliation_mode_skips_ledger(store):
    """mode='registry_reconciliation' writes registry+handle ONLY — NO ledger.

    The headline invariant of T24 (VIB-4210). Closing GH #2131 depends on this
    write path landing a phantom-missing row WITHOUT polluting the immutable
    intent history.
    """
    s, db_path = store
    await s.save_ledger_and_registry_atomic(
        _ledger(), _registry(), None, "registry_reconciliation"
    )

    conn = sqlite3.connect(db_path)
    try:
        n_ledger = conn.execute("SELECT COUNT(*) FROM transaction_ledger").fetchone()[0]
        n_reg = conn.execute("SELECT COUNT(*) FROM position_registry").fetchone()[0]
    finally:
        conn.close()

    assert n_ledger == 0, "ledger MUST NOT be written under mode=registry_reconciliation"
    assert n_reg == 1, "registry row MUST be written under mode=registry_reconciliation"


@pytest.mark.asyncio
async def test_registry_reconciliation_mode_writes_payload(store):
    """The registry row written under registry_reconciliation carries the full payload."""
    s, db_path = store
    await s.save_ledger_and_registry_atomic(
        _ledger(), _registry(pih="hash_reconciled"), None, "registry_reconciliation"
    )

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute(
            "SELECT payload, status, physical_identity_hash, last_reconciled_at_block "
            "FROM position_registry WHERE physical_identity_hash = ?",
            ("hash_reconciled",),
        ).fetchone()
    finally:
        conn.close()

    import json

    assert row is not None
    assert row["status"] == "open"
    assert row["last_reconciled_at_block"] == 1234567
    payload = json.loads(row["payload"])
    assert payload.get("source") == "reconciliation_discovery"


@pytest.mark.asyncio
async def test_registry_reconciliation_with_handle_writes_handle_column(store):
    """Handle backfill UPDATE still runs under registry_reconciliation mode."""
    s, db_path = store
    await s.save_ledger_and_registry_atomic(
        _ledger(), _registry(pih="hash_with_handle", handle="leg_a"), None, "registry_reconciliation"
    )

    conn = sqlite3.connect(db_path)
    try:
        row = conn.execute(
            "SELECT handle FROM position_registry WHERE physical_identity_hash = ?",
            ("hash_with_handle",),
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row[0] == "leg_a"


@pytest.mark.asyncio
async def test_invalid_mode_raises_value_error(store):
    """mode='BOGUS' / mode=None / etc. raises ValueError BEFORE opening tx.

    Silent fallthrough to a default branch is the anti-pattern this fast-fail
    guard prevents. UAT card §D3.F6 silent-error guard.
    """
    s, _ = store
    with pytest.raises(ValueError, match="invalid mode"):
        await s.save_ledger_and_registry_atomic(_ledger(), _registry(), None, "BOGUS")
    # Also reject None (proto3 default '' must be normalized upstream).
    with pytest.raises(ValueError, match="invalid mode"):
        await s.save_ledger_and_registry_atomic(_ledger(), _registry(), None, "")


@pytest.mark.asyncio
async def test_registry_reconciliation_atomicity_under_failure(store):
    """If the registry UPSERT fails, NEITHER ledger NOR registry row commits.

    UAT card §D3.F4 (mid-flight termination invariant): registry_reconciliation
    is atomic just like the legacy three-write contract. Forcing a failure by
    passing an invalid status value (CHECK violation) leaves the DB unchanged.
    """
    s, db_path = store
    # Force a CHECK violation: status='garbage' is not in the CHECK list.
    bad_registry = RegistryRow(
        deployment_id="TestStrat:abc",
        chain="arbitrum",
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash="hash_bad",
        semantic_grouping_key="arbitrum:bad",
        grouping_policy_version="univ3_lp@v1",
        status="garbage",  # type: ignore[arg-type]
        payload={"source": "reconciliation_discovery"},
        matching_policy_version=1,
    )
    with pytest.raises(Exception):  # noqa: PT011 — IntegrityError subclass
        await s.save_ledger_and_registry_atomic(
            _ledger(), bad_registry, None, "registry_reconciliation"
        )

    conn = sqlite3.connect(db_path)
    try:
        n_ledger = conn.execute("SELECT COUNT(*) FROM transaction_ledger").fetchone()[0]
        n_reg = conn.execute("SELECT COUNT(*) FROM position_registry").fetchone()[0]
    finally:
        conn.close()
    assert n_ledger == 0, "ledger MUST be empty after rollback"
    assert n_reg == 0, "registry MUST be empty after rollback"


@pytest.mark.asyncio
async def test_registry_reconciliation_raises_on_db_error(store):
    """DB-layer errors propagate as the existing exception class (UAT §D3.F5).

    Reconciliation mode does NOT inherit the live/paper/dry_run leniency rule
    because there is no execution_mode context for a control-plane RPC —
    every error raises. The classifier still distinguishes RegistryAutoCollision
    from generic persistence errors per VIB-4200.
    """
    s, _ = store

    # First commit normally — establishes a handle-less open row in an
    # auto-mode group.
    await s.save_ledger_and_registry_atomic(
        _ledger(id_="first"),
        _registry(pih="hash_first", sgk="arbitrum:same_pool"),
        None,
        "commit",
    )

    # Second commit under registry_reconciliation with a DIFFERENT pih
    # but the SAME (deployment_id, chain, accounting_category, semantic_grouping_key)
    # and handle=None — triggers ix_registry_auto_mode collision.
    second_registry = _registry(pih="hash_second", sgk="arbitrum:same_pool")
    with pytest.raises(RegistryAutoCollisionError) as exc_info:
        await s.save_ledger_and_registry_atomic(
            _ledger(id_="second"),
            second_registry,
            None,
            "registry_reconciliation",
        )
    # The collision classifier must fire under registry_reconciliation too —
    # NOT just on the legacy 'commit' path. Validates UAT §D3.F9.
    assert "same_pool" in exc_info.value.semantic_grouping_key
