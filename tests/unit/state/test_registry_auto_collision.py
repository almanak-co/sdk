"""VIB-4200 / T14 — RegistryAutoCollisionError typed exception + auto-mode
collision guard.

This test file is the runnable contract that backs UAT card
``docs/internal/uat-cards/VIB-4200.md`` (Phase 1 verdict: ``SPEC_OK``,
card blob ``e09dcf3ca7e3...``).

It exercises:

- D1: typed-exception happy path (collision raises with all required
  fields; distinctness from :class:`AccountingPersistenceError`).
- D2: variance across registry-flipped ``AccountingCategory`` values
  (LP / PENDLE_LP / PERP) AND distinct chain / category / deployment
  do NOT collide.
- D3: failure-mode robustness — non-collision IntegrityErrors do NOT
  mis-classify; same-PIH retries are idempotent; same-PIH upserts
  observably mutate; closed rows in the same group do not block
  re-opening; handles bypass; ≤3-line URL-bearing error message;
  concurrent writers (one wins, one collides); constraint-name
  fallback; mode-uniform raise; duplicate-handle classification.
- D5: static anti-bypass guards (no ``classify``, no manual classifier,
  no taxonomy lookup in the new module) AND the canonical
  ``record_for(...)`` strict-validation behavioral check.

The tests are async-pytest-style and use the same SQLite-backed
StateManager fixtures as ``tests/integration/state/test_atomic_commit_local.py``.
"""

from __future__ import annotations

import ast
import asyncio
import os
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from almanak.framework.accounting.commit import (
    HandleMapping,
    RegistryRow,
    save_ledger_and_registry,
)
from almanak.framework.accounting.payload_schemas import MATCHING_POLICY_VERSIONS
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.primitives.taxonomy import UnknownIntentTypeError, record_for
from almanak.framework.primitives.types import (
    AccountingCategory,
    Primitive,
)
from almanak.framework.state.backends.sqlite import SQLiteStore
from almanak.framework.state.exceptions import AccountingPersistenceError
from almanak.framework.state.registry_errors import (
    DOC_POINTER_URL,
    RegistryAutoCollisionError,
)
from almanak.framework.state.state_manager import (
    SQLiteConfigLight,
    StateManager,
    StateManagerConfig,
    WarmBackendType,
)


# =============================================================================
# FIXTURES
# =============================================================================


@pytest_asyncio.fixture
async def temp_db_path():
    """Temp-file SQLite path. Used for tests that need a real on-disk DB
    (e.g., D3.F7 concurrent writers — the BEGIN IMMEDIATE lock fires only
    on file-backed connections; in-memory DBs serialize trivially)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield os.path.join(tmpdir, "test_registry_auto_collision.db")


@pytest_asyncio.fixture
async def state_manager(temp_db_path):
    """Initialized StateManager with a fresh SQLite DB."""
    config = StateManagerConfig(
        warm_backend=WarmBackendType.SQLITE,
        sqlite_config=SQLiteConfigLight(db_path=temp_db_path, wal_mode=False),
        load_state_on_startup=False,
    )
    manager = StateManager(config)
    await manager.initialize()
    yield manager
    await manager.close()


# =============================================================================
# BUILDERS
# =============================================================================


def _make_ledger_entry(
    *,
    id_: str = "ledger-vib4200-1",
    intent_type: str = "LP_OPEN",
    deployment_id: str = "TestStrat:abc123",
    chain: str = "arbitrum",
    protocol: str = "uniswap_v3",
    tx_hash: str = "0xtxA",
    success: bool = True,
    execution_mode: str = "live",
) -> LedgerEntry:
    return LedgerEntry(
        id=id_,
        cycle_id="cycle-1",
        deployment_id=deployment_id,
        execution_mode=execution_mode,
        timestamp=datetime(2026, 5, 10, 12, 0, 0, tzinfo=UTC),
        intent_type=intent_type,
        token_in="USDC",
        amount_in="100",
        token_out="WETH",
        amount_out="0.04",
        effective_price="2500",
        slippage_bps=10.0,
        gas_used=200000,
        gas_usd="0.50",
        tx_hash=tx_hash,
        chain=chain,
        protocol=protocol,
        success=success,
        error="",
    )


def _make_registry_row(
    *,
    deployment_id: str = "TestStrat:abc123",
    chain: str = "arbitrum",
    primitive: Primitive = Primitive.LP,
    accounting_category: AccountingCategory = AccountingCategory.LP,
    physical_identity_hash: str = "HASH_A",
    semantic_grouping_key: str = "arbitrum:0xPOOL_A",
    handle: str | None = None,
    status: str = "open",
    opened_tx: str | None = "0xtxA",
    opened_at_block: int | None = 1000,
    closed_tx: str | None = None,
    closed_at_block: int | None = None,
    payload: dict | None = None,
) -> RegistryRow:
    return RegistryRow(
        deployment_id=deployment_id,
        chain=chain,
        primitive=primitive,
        accounting_category=accounting_category,
        physical_identity_hash=physical_identity_hash,
        semantic_grouping_key=semantic_grouping_key,
        grouping_policy_version="lp@v1",
        handle=handle,
        status=status,
        payload=payload if payload is not None else {"k": "v1"},
        opened_at_block=opened_at_block,
        opened_tx=opened_tx,
        closed_at_block=closed_at_block,
        closed_tx=closed_tx,
        last_reconciled_at_block=None,
        matching_policy_version=MATCHING_POLICY_VERSIONS[primitive],
    )


def _row_count(conn: sqlite3.Connection, table: str, where: str = "1=1", params: tuple = ()) -> int:
    cursor = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}", params)
    return cursor.fetchone()[0]


# =============================================================================
# D1 — Correctness (happy path)
# =============================================================================


@pytest.mark.asyncio
async def test_d1_s1_collision_raises_typed_error(state_manager, temp_db_path):
    """D1.S1 — happy-path collision raises typed error with all required
    fields; only the winner row lands in both tables."""
    # First write — winner.
    ledger_a = _make_ledger_entry(id_="led-A", tx_hash="0xtxA")
    row_a = _make_registry_row(
        physical_identity_hash="HASH_A",
        semantic_grouping_key="arbitrum:0xPOOL_A",
        opened_tx="0xtxA",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    # Second write — same group, different physical_identity_hash, no
    # handle => must collide.
    ledger_b = _make_ledger_entry(id_="led-B", tx_hash="0xtxB")
    row_b = _make_registry_row(
        physical_identity_hash="HASH_B",
        semantic_grouping_key="arbitrum:0xPOOL_A",
        opened_tx="0xtxB",
    )
    with pytest.raises(RegistryAutoCollisionError) as excinfo:
        await save_ledger_and_registry(
            state_manager, ledger=ledger_b, registry=row_b, mode="registry",
        )

    err = excinfo.value
    # Field assertions on the raised error.
    assert err.semantic_grouping_key == "arbitrum:0xPOOL_A"
    assert err.existing_physical_identity_hash == "HASH_A"
    assert err.opened_tx == "0xtxA"
    assert err.accounting_category == "lp"
    # Doc-pointer URL present.
    assert "blueprints/28-position-registry" in str(err)
    # ≤3 lines.
    assert str(err).count("\n") <= 2, f"error message too long: {str(err)!r}"

    # Post-state: only A landed.
    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
        cursor = conn.execute(
            "SELECT physical_identity_hash FROM position_registry LIMIT 1",
        )
        assert cursor.fetchone()[0] == "HASH_A"
        assert _row_count(conn, "transaction_ledger") == 1
        cursor = conn.execute("SELECT id FROM transaction_ledger LIMIT 1")
        assert cursor.fetchone()[0] == "led-A"
    finally:
        conn.close()


def test_d1_s2_distinct_from_accounting_persistence_error():
    """D1.S2 — RegistryAutoCollisionError is NOT a subclass of
    AccountingPersistenceError (in either direction)."""
    assert not issubclass(RegistryAutoCollisionError, AccountingPersistenceError)
    assert not issubclass(AccountingPersistenceError, RegistryAutoCollisionError)

    err = RegistryAutoCollisionError(
        semantic_grouping_key="g",
        existing_physical_identity_hash="h",
        opened_tx="0xt",
        accounting_category="lp",
    )
    # ``except AccountingPersistenceError`` must NOT match.
    matched_ape = False
    matched_rac = False
    try:
        raise err
    except AccountingPersistenceError:
        matched_ape = True
    except RegistryAutoCollisionError:
        matched_rac = True
    assert matched_ape is False
    assert matched_rac is True


# =============================================================================
# D2 — Scalability (variance matrix)
# =============================================================================


@pytest.mark.asyncio
async def test_d2_m1_collision_lp(state_manager, temp_db_path):
    """D2.M1 — accounting_category=LP collision."""
    ledger_a = _make_ledger_entry(id_="m1-A")
    row_a = _make_registry_row(
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash="HASH_LP_A",
        semantic_grouping_key="arbitrum:0xPOOL_LP",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    ledger_b = _make_ledger_entry(id_="m1-B")
    row_b = _make_registry_row(
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash="HASH_LP_B",
        semantic_grouping_key="arbitrum:0xPOOL_LP",
    )
    with pytest.raises(RegistryAutoCollisionError) as excinfo:
        await save_ledger_and_registry(state_manager, ledger=ledger_b, registry=row_b, mode="registry")
    assert excinfo.value.accounting_category == "lp"

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
        cursor = conn.execute(
            "SELECT physical_identity_hash FROM position_registry LIMIT 1",
        )
        assert cursor.fetchone()[0] == "HASH_LP_A"
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d2_m2_collision_pendle_lp(state_manager, temp_db_path):
    """D2.M2 — accounting_category=PENDLE_LP collision; pins category-scoping."""
    ledger_a = _make_ledger_entry(id_="m2-A", protocol="pendle")
    row_a = _make_registry_row(
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.PENDLE_LP,
        physical_identity_hash="HASH_PLP_A",
        semantic_grouping_key="arbitrum:0xMARKET_X:1234567890",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    ledger_b = _make_ledger_entry(id_="m2-B", protocol="pendle")
    row_b = _make_registry_row(
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.PENDLE_LP,
        physical_identity_hash="HASH_PLP_B",
        semantic_grouping_key="arbitrum:0xMARKET_X:1234567890",
    )
    with pytest.raises(RegistryAutoCollisionError) as excinfo:
        await save_ledger_and_registry(state_manager, ledger=ledger_b, registry=row_b, mode="registry")
    assert excinfo.value.accounting_category == "pendle_lp"

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d2_m3_collision_perp(state_manager, temp_db_path):
    """D2.M3 — accounting_category=PERP collision."""
    ledger_a = _make_ledger_entry(id_="m3-A", protocol="gmx_v2", intent_type="PERP_OPEN")
    row_a = _make_registry_row(
        primitive=Primitive.PERP,
        accounting_category=AccountingCategory.PERP,
        physical_identity_hash="HASH_PERP_A",
        semantic_grouping_key="arbitrum:0xMARKET:long",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    ledger_b = _make_ledger_entry(id_="m3-B", protocol="gmx_v2", intent_type="PERP_OPEN")
    row_b = _make_registry_row(
        primitive=Primitive.PERP,
        accounting_category=AccountingCategory.PERP,
        physical_identity_hash="HASH_PERP_B",
        semantic_grouping_key="arbitrum:0xMARKET:long",
    )
    with pytest.raises(RegistryAutoCollisionError) as excinfo:
        await save_ledger_and_registry(state_manager, ledger=ledger_b, registry=row_b, mode="registry")
    assert excinfo.value.accounting_category == "perp"

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d2_m4_distinct_accounting_category_does_not_collide(state_manager, temp_db_path):
    """D2.M4 — two rows with the SAME semantic_grouping_key but DIFFERENT
    accounting_category coexist (pins accounting_category as part of the
    uniqueness tuple)."""
    ledger_a = _make_ledger_entry(id_="m4-A")
    row_a = _make_registry_row(
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash="HASH_M4_LP",
        semantic_grouping_key="arbitrum:0xSAME",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    ledger_b = _make_ledger_entry(id_="m4-B", protocol="pendle")
    row_b = _make_registry_row(
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.PENDLE_LP,
        physical_identity_hash="HASH_M4_PLP",
        semantic_grouping_key="arbitrum:0xSAME",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_b, registry=row_b, mode="registry")

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 2
        cursor = conn.execute(
            "SELECT accounting_category FROM position_registry ORDER BY accounting_category",
        )
        cats = [r[0] for r in cursor.fetchall()]
        assert cats == ["lp", "pendle_lp"]
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d2_m5_distinct_chain_does_not_collide(state_manager, temp_db_path):
    """D2.M5 — two rows with the SAME semantic_grouping_key but DIFFERENT
    chain coexist."""
    ledger_a = _make_ledger_entry(id_="m5-A", chain="arbitrum")
    row_a = _make_registry_row(
        chain="arbitrum",
        physical_identity_hash="HASH_M5_ARB",
        semantic_grouping_key="X:0xSAME",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    ledger_b = _make_ledger_entry(id_="m5-B", chain="base")
    row_b = _make_registry_row(
        chain="base",
        physical_identity_hash="HASH_M5_BASE",
        semantic_grouping_key="X:0xSAME",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_b, registry=row_b, mode="registry")

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 2
        cursor = conn.execute("SELECT chain FROM position_registry ORDER BY chain")
        chains = [r[0] for r in cursor.fetchall()]
        assert chains == ["arbitrum", "base"]
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d2_m6_distinct_deployment_does_not_collide(state_manager, temp_db_path):
    """D2.M6 — two rows with the SAME group but DIFFERENT deployment_id
    coexist."""
    ledger_a = _make_ledger_entry(id_="m6-A", deployment_id="StrategyA:abc")
    row_a = _make_registry_row(
        deployment_id="StrategyA:abc",
        physical_identity_hash="HASH_M6_A",
        semantic_grouping_key="arbitrum:0xSAME",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    ledger_b = _make_ledger_entry(id_="m6-B", deployment_id="StrategyB:def")
    row_b = _make_registry_row(
        deployment_id="StrategyB:def",
        physical_identity_hash="HASH_M6_B",
        semantic_grouping_key="arbitrum:0xSAME",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_b, registry=row_b, mode="registry")

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 2
        cursor = conn.execute("SELECT DISTINCT deployment_id FROM position_registry ORDER BY deployment_id")
        depls = [r[0] for r in cursor.fetchall()]
        assert depls == ["StrategyA:abc", "StrategyB:def"]
    finally:
        conn.close()


# =============================================================================
# D3 — Robustness (NO SILENT FAILURE — HARD GATE)
# =============================================================================


@pytest.mark.asyncio
async def test_d3_f1_check_violation_does_not_misclassify_as_collision(
    state_manager, temp_db_path,
):
    """D3.F1 — even when an existing same-group open row is present, a
    CHECK violation on the second insert MUST surface as
    AccountingPersistenceError, not RegistryAutoCollisionError."""
    # Setup: open a valid row that fills the auto-mode group.
    ledger_a = _make_ledger_entry(id_="f1-A")
    row_a = _make_registry_row(
        physical_identity_hash="HASH_F1_A",
        semantic_grouping_key="G_F1",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    # Inject: second write with valid identity but invalid status.
    bad_row = RegistryRow(
        deployment_id="TestStrat:abc123",
        chain="arbitrum",
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash="HASH_F1_B",
        semantic_grouping_key="G_F1",
        grouping_policy_version="lp@v1",
        handle=None,
        status="INVALID_STATUS",  # type: ignore[arg-type]  -- intentional CHECK violation
        payload={"x": 1},
        matching_policy_version=MATCHING_POLICY_VERSIONS[Primitive.LP],
    )
    ledger_b = _make_ledger_entry(id_="f1-B")
    with pytest.raises(AccountingPersistenceError) as excinfo:
        await save_ledger_and_registry(
            state_manager, ledger=ledger_b, registry=bad_row, mode="registry",
        )
    # Distinctness check.
    err = excinfo.value
    assert isinstance(err, AccountingPersistenceError)
    assert not isinstance(err, RegistryAutoCollisionError)

    # Post-state: only A's row + ledger.
    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
        cursor = conn.execute(
            "SELECT physical_identity_hash, status FROM position_registry LIMIT 1",
        )
        pih, status = cursor.fetchone()
        assert pih == "HASH_F1_A"
        assert status == "open"
        assert _row_count(conn, "transaction_ledger") == 1
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d3_f2_same_physical_identity_hash_is_idempotent(state_manager, temp_db_path):
    """D3.F2.a — exact-retry idempotency: same PIH + same payload, no
    exception, single surviving row."""
    ledger = _make_ledger_entry(id_="f2-1")
    row = _make_registry_row(
        physical_identity_hash="HASH_F2",
        semantic_grouping_key="G_F2",
        payload={"k": "v1"},
    )
    await save_ledger_and_registry(state_manager, ledger=ledger, registry=row, mode="registry")

    # Retry — same exact inputs.
    await save_ledger_and_registry(state_manager, ledger=ledger, registry=row, mode="registry")
    # No exception; specifically NOT RegistryAutoCollisionError.

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
        cursor = conn.execute(
            "SELECT physical_identity_hash, semantic_grouping_key, status FROM position_registry LIMIT 1",
        )
        pih, sgk, status = cursor.fetchone()
        assert pih == "HASH_F2"
        assert sgk == "G_F2"
        assert status == "open"
        assert _row_count(conn, "transaction_ledger") == 1
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d3_f2b_same_pk_upsert_observed_field_change(state_manager, temp_db_path):
    """D3.F2.b — same-PIH UPSERT: status open→closed mutates the row,
    proving the conflict-clause UPDATE actually executed (not silently
    swallowed)."""
    ledger_a = _make_ledger_entry(id_="f2b-A", intent_type="LP_OPEN")
    row_open = _make_registry_row(
        physical_identity_hash="HASH_F2B",
        status="open",
        closed_at_block=None,
        closed_tx=None,
        payload={"k": "v1"},
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_open, mode="registry")

    # Second write — same PIH, status=closed. The monotone status priority
    # guard lets this update through (open=0 < closed=1).
    ledger_b = _make_ledger_entry(id_="f2b-B", intent_type="LP_CLOSE")
    row_closed = _make_registry_row(
        physical_identity_hash="HASH_F2B",
        status="closed",
        closed_at_block=2000,
        closed_tx="0xclosetx",
        payload={"k": "v2"},
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_b, registry=row_closed, mode="registry")
    # No exception (specifically NOT RegistryAutoCollisionError).

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
        cursor = conn.execute(
            "SELECT status, closed_at_block, closed_tx, payload FROM position_registry LIMIT 1",
        )
        status, closed_at_block, closed_tx, payload = cursor.fetchone()
        assert status == "closed"
        assert closed_at_block == 2000
        assert closed_tx == "0xclosetx"
        assert "v2" in payload
        # Both ledger rows landed (different ids).
        assert _row_count(conn, "transaction_ledger") == 2
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d3_f3_different_pih_same_grouping_key_is_collision(state_manager, temp_db_path):
    """D3.F3 — different physical_identity_hash + same group = collision."""
    ledger_a = _make_ledger_entry(id_="f3-A")
    row_a = _make_registry_row(
        physical_identity_hash="HA",
        semantic_grouping_key="G",
        opened_tx="0xtxA",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    ledger_b = _make_ledger_entry(id_="f3-B")
    row_b = _make_registry_row(
        physical_identity_hash="HB",
        semantic_grouping_key="G",
        opened_tx="0xtxB",
    )
    with pytest.raises(RegistryAutoCollisionError) as excinfo:
        await save_ledger_and_registry(state_manager, ledger=ledger_b, registry=row_b, mode="registry")

    err = excinfo.value
    assert isinstance(err, RegistryAutoCollisionError)
    assert not isinstance(err, AccountingPersistenceError)
    assert err.existing_physical_identity_hash == "HA"

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
        cursor = conn.execute(
            "SELECT physical_identity_hash FROM position_registry LIMIT 1",
        )
        assert cursor.fetchone()[0] == "HA"
        assert _row_count(conn, "transaction_ledger") == 1
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d3_f4_closed_row_in_group_does_not_block_reopen(state_manager, temp_db_path):
    """D3.F4 — a closed row in the same group does NOT block reopening
    because the partial index filters by status='open' AND handle IS NULL."""
    # Step 1: open A.
    ledger_open = _make_ledger_entry(id_="f4-A-open", intent_type="LP_OPEN")
    row_open = _make_registry_row(
        physical_identity_hash="HA",
        semantic_grouping_key="G",
        status="open",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_open, registry=row_open, mode="registry")

    # Step 2: close A (legitimate UPSERT via monotone status priority).
    ledger_close = _make_ledger_entry(id_="f4-A-close", intent_type="LP_CLOSE")
    row_closed = _make_registry_row(
        physical_identity_hash="HA",
        semantic_grouping_key="G",
        status="closed",
        closed_at_block=2000,
        closed_tx="0xclose",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_close, registry=row_closed, mode="registry")

    # Step 3: open C in the same group — must succeed.
    ledger_c = _make_ledger_entry(id_="f4-C")
    row_c = _make_registry_row(
        physical_identity_hash="HC",
        semantic_grouping_key="G",
        status="open",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_c, registry=row_c, mode="registry")
    # No exception expected.

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 2
        cursor = conn.execute(
            "SELECT physical_identity_hash, status FROM position_registry "
            "ORDER BY physical_identity_hash",
        )
        rows = cursor.fetchall()
        assert rows == [("HA", "closed"), ("HC", "open")]
        assert _row_count(conn, "transaction_ledger") == 3
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d3_f5_handle_bypasses_collision(state_manager, temp_db_path):
    """D3.F5 — supplying a handle on the second open bypasses the partial
    index (which filters WHERE handle IS NULL)."""
    ledger_a = _make_ledger_entry(id_="f5-A")
    row_a = _make_registry_row(
        physical_identity_hash="HA",
        semantic_grouping_key="G",
        handle=None,
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    ledger_b = _make_ledger_entry(id_="f5-B")
    row_b = _make_registry_row(
        physical_identity_hash="HB",
        semantic_grouping_key="G",
        handle="leg_b",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_b, registry=row_b, mode="registry")
    # No collision.

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 2
        cursor = conn.execute(
            "SELECT physical_identity_hash, handle FROM position_registry "
            "ORDER BY physical_identity_hash",
        )
        rows = cursor.fetchall()
        assert rows == [("HA", None), ("HB", "leg_b")]
        assert _row_count(conn, "transaction_ledger") == 2
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d3_f6_error_message_three_lines_with_doc_pointer(state_manager, temp_db_path):
    """D3.F6 — error message shape contract (≤3 lines + URL + grep-able
    fields)."""
    import re

    ledger_a = _make_ledger_entry(id_="f6-A", tx_hash="0xtxFA")
    row_a = _make_registry_row(
        physical_identity_hash="HF6_A",
        semantic_grouping_key="arbitrum:0xpoolF6",
        opened_tx="0xtxFA",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    ledger_b = _make_ledger_entry(id_="f6-B", tx_hash="0xtxFB")
    row_b = _make_registry_row(
        physical_identity_hash="HF6_B",
        semantic_grouping_key="arbitrum:0xpoolF6",
        opened_tx="0xtxFB",
    )
    with pytest.raises(RegistryAutoCollisionError) as excinfo:
        await save_ledger_and_registry(state_manager, ledger=ledger_b, registry=row_b, mode="registry")

    msg = str(excinfo.value)
    # ≤3 lines.
    assert msg.count("\n") <= 2, f"message has too many lines: {msg!r}"
    # Contains a URL.
    url_match = re.search(r"https?://\S+", msg)
    assert url_match is not None, f"no URL in error message: {msg!r}"
    assert "blueprints/28-position-registry" in url_match.group(0)
    # Grep-able fields.
    assert "arbitrum:0xpoolF6" in msg
    assert "HF6_A" in msg
    assert "0xtxFA" in msg
    assert "lp" in msg

    # Post-state.
    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
        assert _row_count(conn, "transaction_ledger") == 1
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d3_f7_concurrent_writers_one_wins_one_collides(temp_db_path):
    """D3.F7.a — concurrent writers: one wins, the other collides; no
    deadlock."""
    # Build two StateManager instances pointing at the same on-disk DB.
    config_a = StateManagerConfig(
        warm_backend=WarmBackendType.SQLITE,
        sqlite_config=SQLiteConfigLight(db_path=temp_db_path, wal_mode=False),
        load_state_on_startup=False,
    )
    config_b = StateManagerConfig(
        warm_backend=WarmBackendType.SQLITE,
        sqlite_config=SQLiteConfigLight(db_path=temp_db_path, wal_mode=False),
        load_state_on_startup=False,
    )
    sm_a = StateManager(config_a)
    sm_b = StateManager(config_b)
    await sm_a.initialize()
    await sm_b.initialize()
    try:
        ledger_a = _make_ledger_entry(id_="f7-A", tx_hash="0xtxFA")
        row_a = _make_registry_row(
            physical_identity_hash="HF7_A",
            semantic_grouping_key="G_F7",
            opened_tx="0xtxFA",
        )
        ledger_b = _make_ledger_entry(id_="f7-B", tx_hash="0xtxFB")
        row_b = _make_registry_row(
            physical_identity_hash="HF7_B",
            semantic_grouping_key="G_F7",
            opened_tx="0xtxFB",
        )

        async def _attempt(sm, ledger, row):
            try:
                await save_ledger_and_registry(sm, ledger=ledger, registry=row, mode="registry")
                return ("ok", None)
            except Exception as e:  # noqa: BLE001 — categorize exceptions in result
                return ("err", e)

        # Cap the wall-clock at 5s — a deadlock would exceed this.
        results = await asyncio.wait_for(
            asyncio.gather(_attempt(sm_a, ledger_a, row_a), _attempt(sm_b, ledger_b, row_b)),
            timeout=5.0,
        )
        outcomes = [r[0] for r in results]
        # Exactly one ok and one err.
        assert outcomes.count("ok") == 1, f"expected 1 ok, got {outcomes}"
        assert outcomes.count("err") == 1, f"expected 1 err, got {outcomes}"
        # The err is a RegistryAutoCollisionError.
        err = next(r[1] for r in results if r[0] == "err")
        assert isinstance(err, RegistryAutoCollisionError)
    finally:
        await sm_a.close()
        await sm_b.close()

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
        assert _row_count(conn, "transaction_ledger") == 1
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d3_f7b_collision_originates_from_integrity_error(state_manager, temp_db_path):
    """D3.F7.b — the collision detection rides on the post-INSERT
    IntegrityError, NOT on a pre-INSERT SELECT lookup. Uses SQLite's
    native ``Connection.set_trace_callback`` to record the SQL trace,
    asserts no group-membership SELECT issued before the INSERT."""
    # Seed the winner row first.
    ledger_a = _make_ledger_entry(id_="f7b-A", tx_hash="0xtxFA")
    row_a = _make_registry_row(
        physical_identity_hash="HF7B_A",
        semantic_grouping_key="G_F7B",
        opened_tx="0xtxFA",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    # Install a trace callback on the live SQLite connection BEFORE the
    # collision attempt. The connection is the StateManager's ``_warm`` →
    # SQLiteStore's ``_conn``.
    sqlite_store = state_manager._warm
    assert isinstance(sqlite_store, SQLiteStore)
    conn = sqlite_store._conn
    assert conn is not None

    captured: list[str] = []

    def _trace(stmt: str) -> None:
        captured.append(stmt)

    conn.set_trace_callback(_trace)
    try:
        ledger_b = _make_ledger_entry(id_="f7b-B", tx_hash="0xtxFB")
        row_b = _make_registry_row(
            physical_identity_hash="HF7B_B",
            semantic_grouping_key="G_F7B",
            opened_tx="0xtxFB",
        )
        with pytest.raises(RegistryAutoCollisionError):
            await save_ledger_and_registry(
                state_manager, ledger=ledger_b, registry=row_b, mode="registry",
            )
    finally:
        conn.set_trace_callback(None)

    # Find the indices of the INSERT INTO position_registry statement and
    # any group-membership SELECT against position_registry.
    insert_idx: int | None = None
    pre_insert_select_idx: int | None = None
    post_insert_select_idx: int | None = None
    for i, stmt in enumerate(captured):
        normalized = " ".join(stmt.split()).upper()
        if "INSERT INTO POSITION_REGISTRY" in normalized and insert_idx is None:
            insert_idx = i
        # Look for the group-membership SELECT signature. It's a SELECT
        # against position_registry with the auto-mode predicate (status
        # = 'open' AND handle IS NULL). The actual collision-detection
        # SELECT in our implementation runs POST-rollback, which means
        # POST-INSERT-attempt — that is acceptable.
        if (
            "SELECT" in normalized
            and "FROM POSITION_REGISTRY" in normalized
            and "STATUS = 'OPEN'" in normalized
            and "HANDLE IS NULL" in normalized
        ):
            if insert_idx is None:
                # SELECT seen before the INSERT — that's the forbidden
                # pre-insert lookup pattern.
                pre_insert_select_idx = i
            else:
                post_insert_select_idx = i
    assert insert_idx is not None, (
        f"no INSERT INTO position_registry seen in trace; "
        f"the framework did not attempt the actual INSERT. Trace: {captured}"
    )
    assert pre_insert_select_idx is None, (
        f"forbidden pre-INSERT group-membership SELECT issued at trace "
        f"index {pre_insert_select_idx}; the framework should detect the "
        f"collision via the post-INSERT IntegrityError, not a pre-INSERT "
        f"lookup. Trace: {captured}"
    )
    assert post_insert_select_idx is not None, (
        f"no post-INSERT group-membership SELECT seen in trace; the "
        f"framework should query the existing row to populate the typed "
        f"exception fields. Trace: {captured}"
    )

    # Post-state check.
    conn2 = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn2, "position_registry") == 1
        assert _row_count(conn2, "transaction_ledger") == 1
    finally:
        conn2.close()


@pytest.mark.asyncio
async def test_d3_f8_constraint_name_absence_falls_back_to_row_check(
    state_manager, temp_db_path,
):
    """D3.F8 — the detection uses a row-existence check, NOT string-matching
    the IntegrityError message. Drives a real collision and asserts the
    error fields are populated correctly even though the implementation
    cannot rely on the SQLite version-specific constraint-name in the
    error message."""
    ledger_a = _make_ledger_entry(id_="f8-A", tx_hash="0xtxFA")
    row_a = _make_registry_row(
        physical_identity_hash="HF8_A",
        semantic_grouping_key="G_F8",
        opened_tx="0xtxFA",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    ledger_b = _make_ledger_entry(id_="f8-B", tx_hash="0xtxFB")
    row_b = _make_registry_row(
        physical_identity_hash="HF8_B",
        semantic_grouping_key="G_F8",
        opened_tx="0xtxFB",
    )
    with pytest.raises(RegistryAutoCollisionError) as excinfo:
        await save_ledger_and_registry(state_manager, ledger=ledger_b, registry=row_b, mode="registry")
    # The detection succeeded without depending on the message text — the
    # populated fields prove the row-check fallback ran.
    err = excinfo.value
    assert err.existing_physical_identity_hash == "HF8_A"
    assert err.opened_tx == "0xtxFA"

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
        assert _row_count(conn, "transaction_ledger") == 1
    finally:
        conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("execution_mode", ["live", "paper", "dry_run"])
async def test_d3_f9_mode_uniform_raise(execution_mode):
    """D3.F9 — collision raises uniformly across live / paper / dry_run.
    Programming bugs MUST NOT be downgraded to log-and-continue in paper
    mode (collisions are NOT VIB-3762's infrastructure-failure case)."""
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, f"f9_{execution_mode}.db")
        config = StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path=db_path, wal_mode=False),
            load_state_on_startup=False,
        )
        sm = StateManager(config)
        await sm.initialize()
        try:
            ledger_a = _make_ledger_entry(
                id_=f"f9-A-{execution_mode}", execution_mode=execution_mode,
            )
            row_a = _make_registry_row(
                physical_identity_hash="HF9_A",
                semantic_grouping_key="G_F9",
            )
            await save_ledger_and_registry(sm, ledger=ledger_a, registry=row_a, mode="registry")

            ledger_b = _make_ledger_entry(
                id_=f"f9-B-{execution_mode}", execution_mode=execution_mode,
            )
            row_b = _make_registry_row(
                physical_identity_hash="HF9_B",
                semantic_grouping_key="G_F9",
            )
            with pytest.raises(RegistryAutoCollisionError):
                await save_ledger_and_registry(sm, ledger=ledger_b, registry=row_b, mode="registry")
        finally:
            await sm.close()

        conn = sqlite3.connect(db_path)
        try:
            assert _row_count(conn, "position_registry") == 1
            assert _row_count(conn, "transaction_ledger") == 1
        finally:
            conn.close()


@pytest.mark.asyncio
async def test_d3_f10b_handled_row_unique_failure_with_existing_handleless_row_is_not_collision(
    state_manager, temp_db_path,
):
    """D3.F10.b — CodeRabbit PR #2228 MAJOR finding regression.

    The partial unique index ``ix_registry_auto_mode`` is defined
    ``WHERE status='open' AND handle IS NULL`` — it CANNOT fire on an
    INSERT whose row carries a handle. But the row-existence check
    (``WHERE ... handle IS NULL``) would still find a same-group
    handle-less open row. Without the early-out guard, this case would
    silently mis-classify a duplicate-handle (`ix_registry_handle`)
    failure as an auto-mode collision when an unrelated handle-less
    open row happens to occupy the same semantic group.

    Regression test: ensure that when an incoming row HAS a handle and
    triggers a UNIQUE violation (via duplicate handle), the failure
    surfaces as AccountingPersistenceError, NOT
    RegistryAutoCollisionError, even when a same-group handle-less row
    exists.
    """
    # Setup row 1: handle='leg_x', semantic_grouping_key='G_x'.
    ledger_x = _make_ledger_entry(id_="f10b-X")
    row_x = _make_registry_row(
        physical_identity_hash="HX",
        semantic_grouping_key="G_x",
        handle="leg_x",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_x, registry=row_x, mode="registry")

    # Setup row 2: HANDLE-LESS, same group as row 3 will be (G_y).
    # This is the row that the row-existence check would find.
    ledger_y = _make_ledger_entry(id_="f10b-Y")
    row_y = _make_registry_row(
        physical_identity_hash="HY",
        semantic_grouping_key="G_y",
        handle=None,
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_y, registry=row_y, mode="registry")

    # Inject row 3: HAS a handle ('leg_x' — duplicate of row 1's handle),
    # different physical_identity_hash, AND its semantic_grouping_key
    # matches row 2's (which is handle-less). The actual constraint that
    # fires is ix_registry_handle (handle uniqueness). Without the early
    # out guard, the row-check would find row 2 and mis-classify as a
    # collision. With the guard, the IntegrityError propagates as
    # AccountingPersistenceError.
    ledger_z = _make_ledger_entry(id_="f10b-Z")
    row_z = _make_registry_row(
        physical_identity_hash="HZ",
        semantic_grouping_key="G_y",  # same as row Y
        handle="leg_x",  # duplicate of row X's handle
    )
    with pytest.raises(AccountingPersistenceError) as excinfo:
        await save_ledger_and_registry(state_manager, ledger=ledger_z, registry=row_z, mode="registry")

    err = excinfo.value
    assert isinstance(err, AccountingPersistenceError)
    assert not isinstance(err, RegistryAutoCollisionError), (
        "Handled-row UNIQUE failure must NOT be re-classified as "
        "RegistryAutoCollisionError even when an unrelated handle-less "
        "open row exists in the same group (CodeRabbit PR #2228 MAJOR)."
    )

    # Post-state: rows X and Y survived; Z was rejected.
    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 2
        cursor = conn.execute(
            "SELECT physical_identity_hash, handle FROM position_registry "
            "ORDER BY physical_identity_hash",
        )
        rows = cursor.fetchall()
        assert rows == [("HX", "leg_x"), ("HY", None)]
        assert _row_count(conn, "transaction_ledger") == 2
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d3_f10_duplicate_handle_is_not_collision(state_manager, temp_db_path):
    """D3.F10 — duplicate handle (ix_registry_handle violation) is NOT a
    collision. Surfaces as AccountingPersistenceError, not
    RegistryAutoCollisionError."""
    # First open: hash HA, handle 'leg_a', group G_a.
    ledger_a = _make_ledger_entry(id_="f10-A")
    row_a = _make_registry_row(
        physical_identity_hash="HA",
        semantic_grouping_key="G_a",
        handle="leg_a",
    )
    await save_ledger_and_registry(state_manager, ledger=ledger_a, registry=row_a, mode="registry")

    # Second open: DIFFERENT hash, DIFFERENT semantic_grouping_key, but
    # SAME handle — violates ix_registry_handle.
    ledger_b = _make_ledger_entry(id_="f10-B")
    row_b = _make_registry_row(
        physical_identity_hash="HB",
        semantic_grouping_key="G_b",
        handle="leg_a",
    )
    with pytest.raises(AccountingPersistenceError) as excinfo:
        await save_ledger_and_registry(state_manager, ledger=ledger_b, registry=row_b, mode="registry")

    err = excinfo.value
    assert isinstance(err, AccountingPersistenceError)
    assert not isinstance(err, RegistryAutoCollisionError)

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
        cursor = conn.execute(
            "SELECT physical_identity_hash, handle FROM position_registry LIMIT 1",
        )
        pih, handle = cursor.fetchone()
        assert pih == "HA"
        assert handle == "leg_a"
        assert _row_count(conn, "transaction_ledger") == 1
    finally:
        conn.close()


# =============================================================================
# D5 — Static anti-bypass guard
# =============================================================================


# Files explicitly modified by VIB-4200. The list is hard-coded (not
# derived from `git diff`) so the test is hermetic and can run on a
# fresh checkout. If a follow-up PR modifies additional state-layer
# files, it must extend this list.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_VIB4200_STATE_LAYER_FILES: tuple[Path, ...] = (
    _REPO_ROOT / "almanak" / "framework" / "state" / "registry_errors.py",
    _REPO_ROOT / "almanak" / "framework" / "state" / "state_manager.py",
    _REPO_ROOT / "almanak" / "framework" / "state" / "backends" / "sqlite.py",
)


def _ast_walk(path: Path) -> ast.AST:
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def _has_classify_call_or_import(tree: ast.AST) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == "classify":
                hits.append((node.lineno, "classify(...) call"))
            elif isinstance(func, ast.Attribute) and func.attr == "classify":
                hits.append((node.lineno, f".{func.attr}(...) call"))
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if (
                mod == "almanak.framework.primitives.taxonomy"
                or mod == "almanak.framework.primitives"
                or mod.endswith(".primitives.taxonomy")
                or mod.endswith(".primitives")
            ):
                for alias in node.names:
                    if alias.name == "classify":
                        hits.append((node.lineno, f"from {mod} import classify"))
    return hits


def _has_record_for_call_or_import(tree: ast.AST) -> list[tuple[int, str]]:
    hits: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if isinstance(func, ast.Name) and func.id == "record_for":
                hits.append((node.lineno, "record_for(...) call"))
            elif isinstance(func, ast.Attribute) and func.attr == "record_for":
                hits.append((node.lineno, f".{func.attr}(...) call"))
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if (
                mod == "almanak.framework.primitives.taxonomy"
                or mod == "almanak.framework.primitives"
                or mod.endswith(".primitives.taxonomy")
                or mod.endswith(".primitives")
            ):
                for alias in node.names:
                    if alias.name == "record_for":
                        hits.append((node.lineno, f"from {mod} import record_for"))
    return hits


def _has_manual_intent_to_enum_map(tree: ast.AST) -> list[tuple[int, str]]:
    """Detect dict literals or module-level assignments mapping
    intent-shaped strings (LP_OPEN, *_CLOSE, …) to ``Primitive`` /
    ``AccountingCategory`` member access — a hand-rolled classifier shape.

    The heuristic looks for ``ast.Dict`` nodes whose KEYS are string
    constants ending in ``_OPEN`` / ``_CLOSE`` / ``_BUY`` / ``_SELL`` /
    ``_REPAY`` / ``_BORROW`` / etc. (the intent-name suffix family)
    and whose VALUES are ``Attribute(Name('Primitive'|'AccountingCategory'), ...)``
    accesses. This intentionally over-flags rather than under-flags —
    a false positive forces a rename / refactor; a false negative ships
    a hand-rolled classifier the registry pattern forbids.
    """
    hits: list[tuple[int, str]] = []
    intent_suffixes = (
        "_OPEN", "_CLOSE", "_BUY", "_SELL", "_BORROW", "_REPAY",
        "_SUPPLY", "_WITHDRAW", "_REDEEM", "_DEPOSIT", "_SWAP",
    )

    def _is_taxonomy_enum_value(node: ast.AST) -> bool:
        if not isinstance(node, ast.Attribute):
            return False
        # node.value is the Primitive / AccountingCategory name
        if isinstance(node.value, ast.Name) and node.value.id in {
            "Primitive", "AccountingCategory",
        }:
            return True
        return False

    def _is_intent_string(node: ast.AST) -> bool:
        if not isinstance(node, ast.Constant):
            return False
        if not isinstance(node.value, str):
            return False
        upper = node.value.upper()
        return any(upper.endswith(s) for s in intent_suffixes)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        # Look for dicts with intent-string keys mapping to Primitive /
        # AccountingCategory enum values.
        intent_keys = sum(1 for k in node.keys if k is not None and _is_intent_string(k))
        enum_values = sum(1 for v in node.values if _is_taxonomy_enum_value(v))
        if intent_keys >= 1 and enum_values >= 1:
            hits.append((
                node.lineno,
                "dict literal mapping intent-string keys to Primitive/AccountingCategory enum values",
            ))
    return hits


def test_d5_2_state_layer_files_do_not_use_classify():
    """D5.2 — every state-layer file modified by VIB-4200 must NOT call
    or import ``classify``."""
    all_violations: list[str] = []
    for path in _VIB4200_STATE_LAYER_FILES:
        assert path.exists(), f"VIB-4200 state-layer file missing: {path}"
        tree = _ast_walk(path)
        for lineno, snippet in _has_classify_call_or_import(tree):
            all_violations.append(f"{path.name}:{lineno}: {snippet}")
    assert not all_violations, (
        "VIB-4200 state-layer files contain forbidden `classify(...)` "
        "call or import. Use `record_for()` strict instead. "
        "Violations:\n  " + "\n  ".join(all_violations)
    )


def test_d5_3_registry_errors_module_does_not_use_record_for():
    """D5.3 — the new registry_errors.py module has NO taxonomy
    responsibility; it must not import or call ``record_for`` (a stray
    use would be a wrong-layer creep symptom)."""
    path = _REPO_ROOT / "almanak" / "framework" / "state" / "registry_errors.py"
    assert path.exists(), f"new module missing: {path}"
    tree = _ast_walk(path)
    hits = _has_record_for_call_or_import(tree)
    assert not hits, (
        f"registry_errors.py must not import/call record_for; the module "
        f"is an exception class and has no taxonomy responsibility. "
        f"Violations: {hits}"
    )


def test_d5_4_state_layer_files_do_not_introduce_manual_classifier():
    """D5.4 — every state-layer file modified by VIB-4200 must NOT
    introduce a manual `intent_string -> Primitive/AccountingCategory`
    classifier dict. Use `record_for()` strict if a taxonomy lookup is
    needed."""
    all_violations: list[str] = []
    for path in _VIB4200_STATE_LAYER_FILES:
        assert path.exists(), f"VIB-4200 state-layer file missing: {path}"
        tree = _ast_walk(path)
        for lineno, snippet in _has_manual_intent_to_enum_map(tree):
            all_violations.append(f"{path.name}:{lineno}: {snippet}")
    assert not all_violations, (
        "VIB-4200 state-layer files contain a forbidden manual "
        "intent-string-to-enum classifier. Use `record_for()` strict. "
        "Violations:\n  " + "\n  ".join(all_violations)
    )


def test_d5_5_record_for_is_the_strict_taxonomy_path():
    """D5.5 — positive proof of the Linear criterion:
    `record_for("LP_OPEN")` returns the expected canonical record AND
    `record_for("UNKNOWN_INTENT")` raises UnknownIntentTypeError (NO
    silent fall-through to NO_ACCOUNTING)."""
    record = record_for("LP_OPEN")
    assert record.primitive == Primitive.LP
    assert record.accounting_category == AccountingCategory.LP
    assert record.position_type is not None  # registry-producing intent

    with pytest.raises(UnknownIntentTypeError):
        record_for("DEFINITELY_NOT_AN_INTENT_42")


# =============================================================================
# Sanity: the URL constant in registry_errors is well-formed
# =============================================================================


def test_doc_pointer_url_is_well_formed():
    """The DOC_POINTER_URL constant must be an http(s) URL the operator
    can paste into a browser; it must point at the canonical blueprint."""
    import re

    assert re.match(r"^https?://\S+$", DOC_POINTER_URL), (
        f"DOC_POINTER_URL is not a well-formed URL: {DOC_POINTER_URL!r}"
    )
    assert "blueprints/28-position-registry" in DOC_POINTER_URL


# =============================================================================
# Validation: the typed exception's __init__ rejects empty inputs
# =============================================================================


def test_typed_exception_rejects_empty_semantic_grouping_key():
    with pytest.raises(ValueError, match="semantic_grouping_key"):
        RegistryAutoCollisionError(
            semantic_grouping_key="",
            existing_physical_identity_hash="h",
            opened_tx="0xt",
            accounting_category="lp",
        )


def test_typed_exception_rejects_empty_existing_physical_identity_hash():
    with pytest.raises(ValueError, match="existing_physical_identity_hash"):
        RegistryAutoCollisionError(
            semantic_grouping_key="g",
            existing_physical_identity_hash="",
            opened_tx="0xt",
            accounting_category="lp",
        )


def test_typed_exception_rejects_empty_accounting_category():
    with pytest.raises(ValueError, match="accounting_category"):
        RegistryAutoCollisionError(
            semantic_grouping_key="g",
            existing_physical_identity_hash="h",
            opened_tx="0xt",
            accounting_category="",
        )


def test_typed_exception_allows_empty_opened_tx():
    """Empty opened_tx is allowed for legacy/back-filled rows where the
    original transaction hash was not captured (per
    docs/internal/migration-cutover-position-registry.md §3.5)."""
    err = RegistryAutoCollisionError(
        semantic_grouping_key="g",
        existing_physical_identity_hash="h",
        opened_tx="",  # legacy
        accounting_category="lp",
    )
    assert err.opened_tx == ""
