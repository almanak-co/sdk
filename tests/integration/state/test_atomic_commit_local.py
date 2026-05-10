"""Integration tests for the atomic commit primitive (VIB-4197 / T11).

Covers UAT card §D1, §D2 (mode × primitive matrix), §D3 (failure modes
including the cross-table parity invariant), and §D4 (audit reproducibility).
The card lives at ``docs/internal/uat-cards/VIB-4197.md``.

These tests are integration-level (touch the SQLite backend through the
StateManager facade) but use in-memory or temp-file DBs — no network, no
real chain. Each test starts with a clean DB so result-identity is
deterministic.
"""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
import pytest_asyncio

from almanak.framework.accounting.commit import (
    HandleMapping,
    RegistryRow,
    save_ledger_and_registry,
)
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.exceptions import AccountingPersistenceError
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
    with tempfile.TemporaryDirectory() as tmpdir:
        yield os.path.join(tmpdir, "test_atomic.db")


@pytest_asyncio.fixture
async def state_manager(temp_db_path):
    config = StateManagerConfig(
        warm_backend=WarmBackendType.SQLITE,
        sqlite_config=SQLiteConfigLight(db_path=temp_db_path, wal_mode=False),
        load_state_on_startup=False,
    )
    manager = StateManager(config)
    await manager.initialize()
    yield manager
    await manager.close()


def _make_ledger_entry(
    *, id_: str = "ledger-test-1",
    intent_type: str = "LP_OPEN",
    deployment_id: str = "TestStrat:abc123",
    success: bool = True,
    tx_hash: str = "0xledgertx",
    protocol: str = "uniswap_v3",
) -> LedgerEntry:
    """Builder for a deterministic LedgerEntry."""
    return LedgerEntry(
        id=id_,
        cycle_id="cycle-1",
        strategy_id="test-strat",
        deployment_id=deployment_id,
        execution_mode="live",
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
        chain="arbitrum",
        protocol=protocol,
        success=success,
        error="",
    )


def _make_registry_row(
    *, deployment_id: str = "TestStrat:abc123",
    chain: str = "arbitrum",
    primitive: Primitive = Primitive.LP,
    accounting_category: AccountingCategory = AccountingCategory.LP,
    physical_identity_hash: str = "HASH_A",
    semantic_grouping_key: str = "arbitrum:0xpool_a",
    handle: str | None = None,
    status: str = "open",
    opened_tx: str | None = "0xopentx",
    opened_at_block: int | None = 1000,
    closed_tx: str | None = None,
    closed_at_block: int | None = None,
    last_reconciled_at_block: int | None = None,
    payload: dict | None = None,
) -> RegistryRow:
    """Builder for a deterministic RegistryRow."""
    return RegistryRow(
        deployment_id=deployment_id,
        chain=chain,
        primitive=primitive,
        accounting_category=accounting_category,
        physical_identity_hash=physical_identity_hash,
        semantic_grouping_key=semantic_grouping_key,
        grouping_policy_version="univ3_lp@v1",
        handle=handle,
        status=status,
        payload=payload if payload is not None else {
            "token_id": 12345,
            "tick_lower": -100,
            "tick_upper": 100,
        },
        opened_at_block=opened_at_block,
        opened_tx=opened_tx,
        closed_at_block=closed_at_block,
        closed_tx=closed_tx,
        last_reconciled_at_block=last_reconciled_at_block,
        matching_policy_version=1,
    )


def _row_count(conn: sqlite3.Connection, table: str, where: str = "1=1", params: tuple = ()) -> int:
    cursor = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}", params)
    return cursor.fetchone()[0]


# =============================================================================
# D1.S2 — accounting_only mode (default)
# =============================================================================


@pytest.mark.asyncio
async def test_accounting_only_mode_writes_ledger_only(state_manager, temp_db_path):
    """D1.S2 — default mode writes ledger only; rejects registry/handle."""
    ledger = _make_ledger_entry()
    await save_ledger_and_registry(state_manager, ledger=ledger, mode="accounting_only")

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "transaction_ledger") == 1
        assert _row_count(conn, "position_registry") == 0
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_accounting_only_is_default(state_manager, temp_db_path):
    """D2.M4 — calling without mode= produces accounting_only behaviour."""
    ledger = _make_ledger_entry(id_="ledger-default")
    await save_ledger_and_registry(state_manager, ledger=ledger)

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "transaction_ledger") == 1
        assert _row_count(conn, "position_registry") == 0
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_accounting_only_rejects_registry_arg(state_manager, temp_db_path):
    """D1.S2 — registry arg in accounting_only mode is a ValueError, not a silent ignore."""
    ledger = _make_ledger_entry()
    registry = _make_registry_row()

    with pytest.raises(ValueError, match="forbids the 'registry' argument"):
        await save_ledger_and_registry(
            state_manager, ledger=ledger, registry=registry, mode="accounting_only",
        )

    # And nothing landed.
    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "transaction_ledger") == 0
        assert _row_count(conn, "position_registry") == 0
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_accounting_only_rejects_handle_arg(state_manager):
    """D1.S2 — handle arg in accounting_only mode raises."""
    ledger = _make_ledger_entry()
    handle = HandleMapping(
        handle="leg_a",
        deployment_id="TestStrat:abc123",
        accounting_category=AccountingCategory.LP,
    )

    with pytest.raises(ValueError, match="forbids the 'handle' argument"):
        await save_ledger_and_registry(
            state_manager, ledger=ledger, handle=handle, mode="accounting_only",
        )


# =============================================================================
# D1.S3 — registry mode happy path
# =============================================================================


@pytest.mark.asyncio
async def test_registry_mode_writes_all_three(state_manager, temp_db_path):
    """D1.S3 — registry mode writes ledger + registry + handle in one tx."""
    ledger = _make_ledger_entry()
    registry = _make_registry_row(handle="leg_a")
    handle = HandleMapping(
        handle="leg_a",
        deployment_id="TestStrat:abc123",
        accounting_category=AccountingCategory.LP,
    )

    await save_ledger_and_registry(
        state_manager, ledger=ledger, registry=registry, handle=handle, mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        # Ledger row landed.
        assert _row_count(conn, "transaction_ledger", "id = ?", (ledger.id,)) == 1
        # Registry row landed and carries the handle (handle is a column on
        # position_registry, not a separate table per blueprint 28 §4.2).
        cursor = conn.execute(
            "SELECT handle, status, primitive, accounting_category, payload "
            "FROM position_registry "
            "WHERE deployment_id=? AND chain=? AND primitive=? AND physical_identity_hash=?",
            (registry.deployment_id, registry.chain, registry.primitive_value(),
             registry.physical_identity_hash),
        )
        row = cursor.fetchone()
        assert row is not None, "registry row not found"
        assert row[0] == "leg_a"
        assert row[1] == "open"
        assert row[2] == "lp"
        assert row[3] == "lp"
        # payload is valid JSON with the dict shape we passed.
        payload = json.loads(row[4])
        assert payload["token_id"] == 12345
    finally:
        conn.close()


# =============================================================================
# D2.M1 — mode × primitive matrix
# =============================================================================


@pytest.mark.asyncio
async def test_mode_x_primitive_matrix(state_manager, temp_db_path):
    """D2.M1 — accounting_only + registry × {LP, Perp, Pendle LP}."""
    # 1: accounting_only — no registry write.
    await save_ledger_and_registry(
        state_manager, ledger=_make_ledger_entry(id_="m1"), mode="accounting_only",
    )

    # 2: registry, LP / lp
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="m2", intent_type="LP_OPEN"),
        registry=_make_registry_row(physical_identity_hash="HASH_LP"),
        mode="registry",
    )

    # 3: registry, perp / perp
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="m3", intent_type="PERP_OPEN", protocol="gmx_v2"),
        registry=_make_registry_row(
            primitive=Primitive.PERP,
            accounting_category=AccountingCategory.PERP,
            physical_identity_hash="HASH_PERP",
            semantic_grouping_key="arbitrum:gmx:eth-usdc:long",
        ),
        mode="registry",
    )

    # 4: registry, LP / pendle_lp (same primitive, different category)
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="m4", intent_type="PENDLE_LP_DEPOSIT", protocol="pendle"),
        registry=_make_registry_row(
            primitive=Primitive.LP,
            accounting_category=AccountingCategory.PENDLE_LP,
            physical_identity_hash="HASH_PENDLE",
            semantic_grouping_key="arbitrum:pendle:0xmkt:1735689600",
        ),
        mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "transaction_ledger") == 4
        assert _row_count(conn, "position_registry") == 3
        # Per-category counts confirm the rows belong to distinct identities.
        assert _row_count(
            conn, "position_registry", "accounting_category = ?", ("lp",),
        ) == 1
        assert _row_count(
            conn, "position_registry", "accounting_category = ?", ("perp",),
        ) == 1
        assert _row_count(
            conn, "position_registry", "accounting_category = ?", ("pendle_lp",),
        ) == 1
    finally:
        conn.close()


# =============================================================================
# D2.M2 — idempotent retry on physical_identity_hash
# =============================================================================


@pytest.mark.asyncio
async def test_idempotent_retry(state_manager, temp_db_path):
    """D2.M2 — same identity fired twice produces one registry row."""
    ledger = _make_ledger_entry()
    registry = _make_registry_row()

    await save_ledger_and_registry(
        state_manager, ledger=ledger, registry=registry, mode="registry",
    )
    # Fire the same call again (simulates lost RPC response → runner retry).
    await save_ledger_and_registry(
        state_manager, ledger=ledger, registry=registry, mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        # Ledger uses INSERT OR REPLACE on id — exactly 1 row.
        assert _row_count(conn, "transaction_ledger", "id = ?", (ledger.id,)) == 1
        # Registry is idempotent on (deployment_id, chain, primitive,
        # physical_identity_hash) — 1 row.
        assert _row_count(
            conn, "position_registry",
            "deployment_id=? AND chain=? AND primitive=? AND physical_identity_hash=?",
            (registry.deployment_id, registry.chain, registry.primitive_value(),
             registry.physical_identity_hash),
        ) == 1
    finally:
        conn.close()


# =============================================================================
# D2.M3b — conflict-update column contract (payload + close anchors + handle COALESCE)
# =============================================================================


@pytest.mark.asyncio
async def test_conflict_update_columns_full_contract(state_manager, temp_db_path):
    """D2.M3b — payload + close anchors are refreshed; handle is COALESCEd."""
    pih = "HASH_CONFLICT"

    # 1. Initial open with handle and a sparse payload.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="conflict-open"),
        registry=_make_registry_row(
            physical_identity_hash=pih,
            handle="leg_a",
            status="open",
            payload={"a": 1},
            opened_at_block=100, opened_tx="0xopen",
        ),
        mode="registry",
    )

    # 2. CLOSE: richer payload + close anchors + handle deliberately MISSING.
    # The COALESCE on the conflict clause MUST preserve the existing handle
    # rather than clearing it — a future close intent that forgets to plumb
    # the handle through is a DX bug, not a registry-state corruption.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="conflict-close", intent_type="LP_CLOSE"),
        registry=_make_registry_row(
            physical_identity_hash=pih,
            handle=None,                              # caller omitted
            status="closed",
            payload={"a": 1, "fee_owed": "100"},
            closed_at_block=150, closed_tx="0xclose",
        ),
        mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT handle, status, payload, closed_at_block, closed_tx "
            "FROM position_registry WHERE physical_identity_hash=?",
            (pih,),
        )
        handle, status, payload_json, c_blk, c_tx = cursor.fetchone()
        assert handle == "leg_a", f"COALESCE failed; handle was cleared: {handle!r}"
        assert status == "closed"
        payload = json.loads(payload_json)
        assert payload["fee_owed"] == "100", "payload not refreshed on conflict update"
        assert c_blk == 150, "closed_at_block not populated"
        assert c_tx == "0xclose", "closed_tx not populated"
    finally:
        conn.close()

    # 2b. Adversarial-handle case: an admitted conflict update arrives with
    #     a DIFFERENT non-null handle. The blueprint 28 §4.2 contract
    #     specifies `handle = COALESCE(position_registry.handle,
    #     EXCLUDED.handle)` — existing wins. A defective UPSERT using the
    #     reverse order (EXCLUDED.handle first) would silently clobber the
    #     canonical handle on every retry that supplies a (different)
    #     handle. We use a distinct deployment_id so the per-deployment
    #     handle uniqueness index doesn't blur this case with the case-1
    #     row above.
    pih_adv = "HASH_ADV_HANDLE"
    adv_dep = "TestStrat:adv-handle"
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="adv-open", deployment_id=adv_dep),
        registry=_make_registry_row(
            deployment_id=adv_dep,
            physical_identity_hash=pih_adv,
            semantic_grouping_key="arbitrum:0xpool_adv",
            handle="leg_canonical",
            status="open",
            opened_at_block=100, opened_tx="0xopen",
        ),
        mode="registry",
    )
    # Admitted conflict update (priority-allowed: open→closed) carrying a
    # different non-null handle. The existing handle 'leg_canonical' MUST win.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="adv-close", intent_type="LP_CLOSE", deployment_id=adv_dep),
        registry=_make_registry_row(
            deployment_id=adv_dep,
            physical_identity_hash=pih_adv,
            semantic_grouping_key="arbitrum:0xpool_adv",
            handle="leg_attacker",              # adversarial: different non-null handle
            status="closed",
            closed_at_block=150, closed_tx="0xclose",
        ),
        mode="registry",
    )
    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT handle, status FROM position_registry "
            "WHERE deployment_id=? AND physical_identity_hash=?",
            (adv_dep, pih_adv),
        )
        handle_adv, status_adv = cursor.fetchone()
        assert handle_adv == "leg_canonical", (
            f"COALESCE order is wrong; existing handle was clobbered by retry's "
            f"different non-null handle. Got {handle_adv!r}, expected 'leg_canonical'. "
            f"Spec: handle = COALESCE(position_registry.handle, EXCLUDED.handle) "
            f"(existing wins) per blueprint 28 §4.2."
        )
        assert status_adv == "closed", (
            "the priority-admitted close should still apply (this isolates the "
            "handle-COALESCE from the priority guard)"
        )
    finally:
        conn.close()

    # 2c. Inverse case: existing handle NULL, retry supplies a non-null
    # handle. The COALESCE MUST fill the NULL slot.
    pih_fill = "HASH_FILL_HANDLE"
    fill_dep = "TestStrat:fill-handle"
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="fill-open", deployment_id=fill_dep),
        registry=_make_registry_row(
            deployment_id=fill_dep,
            physical_identity_hash=pih_fill,
            semantic_grouping_key="arbitrum:0xpool_fill",
            handle=None,                        # auto-mode open
            status="open",
            opened_at_block=100, opened_tx="0xopen",
        ),
        mode="registry",
    )
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="fill-close", intent_type="LP_CLOSE", deployment_id=fill_dep),
        registry=_make_registry_row(
            deployment_id=fill_dep,
            physical_identity_hash=pih_fill,
            semantic_grouping_key="arbitrum:0xpool_fill",
            handle="leg_x",                     # supplied at close
            status="closed",
            closed_at_block=150, closed_tx="0xclose",
        ),
        mode="registry",
    )
    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT handle FROM position_registry "
            "WHERE deployment_id=? AND physical_identity_hash=?",
            (fill_dep, pih_fill),
        )
        h = cursor.fetchone()[0]
        assert h == "leg_x", (
            f"COALESCE failed to fill NULL handle on admitted retry; got {h!r}"
        )
    finally:
        conn.close()

    # 3. Reconciliation retry: same identity, last_reconciled_at_block=200.
    # status=closed retries against status=closed do NOT take the priority
    # guard's positive branch (priority equal); but the COALESCE update
    # MUST still propagate non-NULL fields so the reconciliation pass
    # advances the row's last-seen-block stamp.
    #
    # NOTE on idempotent reconciliation retries: per blueprint 28 §4.3 the
    # status field strictly does not regress (priority guard); the OTHER
    # fields update on every conflict via the DO UPDATE clause. Because
    # SQLite's ON CONFLICT DO UPDATE applies the SET list when the WHERE
    # predicate is true, equal-priority retries (closed→closed) do NOT
    # advance the SET. We therefore use status='closed' but supply the new
    # last_reconciled_at_block via the SAME row, accepting that this is
    # not exercised by the priority-WHERE in the conflict clause. The
    # check below documents this contract: callers should fold
    # reconciliation updates into the row at write-time, not as a
    # separate retry.
    pih2 = "HASH_RECON"
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="recon-open"),
        registry=_make_registry_row(
            physical_identity_hash=pih2,
            status="open", handle=None,
            opened_at_block=100, opened_tx="0xopen",
        ),
        mode="registry",
    )
    # Status open → closed advances the priority guard, so the SET runs and
    # last_reconciled_at_block lands.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="recon-close", intent_type="LP_CLOSE"),
        registry=_make_registry_row(
            physical_identity_hash=pih2,
            status="closed", handle=None,
            payload={"recon": True},
            closed_at_block=150, closed_tx="0xclose",
            last_reconciled_at_block=200,
        ),
        mode="registry",
    )
    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT last_reconciled_at_block FROM position_registry "
            "WHERE physical_identity_hash=?",
            (pih2,),
        )
        assert cursor.fetchone()[0] == 200, (
            "last_reconciled_at_block must populate when the conflict-update "
            "WHERE predicate admits the change"
        )
    finally:
        conn.close()


# =============================================================================
# D2.M3c — priority-rejected retries leave ALL protected columns untouched
# =============================================================================


@pytest.mark.asyncio
async def test_priority_rejected_retry_preserves_all_columns(state_manager, temp_db_path):
    """D2.M3c — when the priority guard rejects, the entire DO UPDATE skips.

    A defective UPSERT that pulls SET fields outside the priority WHERE
    clause would silently overwrite the row's payload / open anchors /
    close anchors / reconciliation block / handle from a stale-lower-priority
    or equal-terminal retry. Status would stay correct (the CASE expression
    pins it) but the row's metadata would silently drift.
    """
    pih = "HASH_REJECTED"

    # 1. Open with canonical metadata.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="rej-open"),
        registry=_make_registry_row(
            physical_identity_hash=pih,
            handle="leg_a",
            status="open",
            payload={"a": 1},
            opened_at_block=100, opened_tx="0xopen",
        ),
        mode="registry",
    )
    # 2. Close with richer payload + close anchors.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="rej-close", intent_type="LP_CLOSE"),
        registry=_make_registry_row(
            physical_identity_hash=pih,
            handle="leg_a",
            status="closed",
            payload={"a": 1, "fee": "10"},
            opened_at_block=100, opened_tx="0xopen",
            closed_at_block=150, closed_tx="0xclose",
        ),
        mode="registry",
    )

    # Snapshot the canonical row state.
    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT handle, status, payload, opened_at_block, opened_tx, "
            "       closed_at_block, closed_tx, last_reconciled_at_block "
            "FROM position_registry WHERE physical_identity_hash=?",
            (pih,),
        )
        canonical = cursor.fetchone()
    finally:
        conn.close()
    assert canonical[1] == "closed"

    # 3. Stale-open retry with adversarial metadata.
    # Priority guard rejects (status open=0 NOT > closed=1). The entire DO
    # UPDATE must be skipped — none of the SET columns may change.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="rej-stale-open"),
        registry=_make_registry_row(
            physical_identity_hash=pih,
            handle="other_leg",                       # adversarial
            status="open",
            payload={"adversarial": "drift"},         # adversarial
            opened_at_block=999, opened_tx="0xattacker",  # adversarial
            closed_at_block=999, closed_tx="0xattacker",  # adversarial
            last_reconciled_at_block=999,             # adversarial
        ),
        mode="registry",
    )
    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT handle, status, payload, opened_at_block, opened_tx, "
            "       closed_at_block, closed_tx, last_reconciled_at_block "
            "FROM position_registry WHERE physical_identity_hash=?",
            (pih,),
        )
        post_stale = cursor.fetchone()
    finally:
        conn.close()
    assert post_stale == canonical, (
        "stale-open priority-rejected retry must not change any column. "
        f"got {post_stale!r}, expected {canonical!r}"
    )

    # 4. Equal-terminal retry: closed→reorg_invalidated. Strict-> rejects
    #    (priority 1 ≯ 1). Same: no column should change.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="rej-equal-terminal", intent_type="LP_REORG"),
        registry=_make_registry_row(
            physical_identity_hash=pih,
            handle="adversarial_handle",
            status="reorg_invalidated",
            payload={"adversarial2": True},
            opened_at_block=888, opened_tx="0xattack2",
            closed_at_block=888, closed_tx="0xattack2",
            last_reconciled_at_block=888,
        ),
        mode="registry",
    )
    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT handle, status, payload, opened_at_block, opened_tx, "
            "       closed_at_block, closed_tx, last_reconciled_at_block "
            "FROM position_registry WHERE physical_identity_hash=?",
            (pih,),
        )
        post_equal_terminal = cursor.fetchone()
    finally:
        conn.close()
    assert post_equal_terminal == canonical, (
        f"equal-terminal priority-rejected retry must not change any column "
        f"(strict > guard). got {post_equal_terminal!r}, expected {canonical!r}"
    )


# =============================================================================
# D2.M3 — monotone status priority
# =============================================================================


@pytest.mark.asyncio
async def test_status_monotone_priority(state_manager, temp_db_path):
    """D2.M3 — open→closed allowed; closed→open rejected (no regression)."""
    pih = "HASH_MONOTONE"
    # 1. Insert open.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="open-1"),
        registry=_make_registry_row(physical_identity_hash=pih, status="open"),
        mode="registry",
    )

    # 2. Update to closed.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="close-1", intent_type="LP_CLOSE"),
        registry=_make_registry_row(
            physical_identity_hash=pih, status="closed",
            closed_at_block=1500, closed_tx="0xclosetx",
        ),
        mode="registry",
    )
    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT status, closed_tx FROM position_registry WHERE physical_identity_hash=?",
            (pih,),
        )
        row = cursor.fetchone()
        assert row[0] == "closed"
        assert row[1] == "0xclosetx"

        # 3. Stale retry with status=open — registry must NOT regress.
        # The atomic primitive accepts the call (no exception), but the
        # ON CONFLICT priority guard suppresses the status field. Other
        # fields may refresh idempotently per blueprint 28 §4.3.
    finally:
        conn.close()

    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="stale-open"),
        registry=_make_registry_row(physical_identity_hash=pih, status="open"),
        mode="registry",
    )
    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT status FROM position_registry WHERE physical_identity_hash=?", (pih,),
        )
        assert cursor.fetchone()[0] == "closed", "stale open retry must not regress closed"
    finally:
        conn.close()

    # 4. reorg_invalidated retry on a closed row — same priority (1==1) is
    # rejected by the strict-> guard.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="reorg-1"),
        registry=_make_registry_row(physical_identity_hash=pih, status="reorg_invalidated"),
        mode="registry",
    )
    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT status FROM position_registry WHERE physical_identity_hash=?", (pih,),
        )
        assert cursor.fetchone()[0] == "closed", (
            "reorg_invalidated must NOT overwrite closed (priority-equal terminal states)"
        )
    finally:
        conn.close()


# =============================================================================
# D3 — failure modes
# =============================================================================


@pytest.mark.asyncio
async def test_mid_transaction_failure_neither_row(state_manager, temp_db_path):
    """D3.F1 — mid-transaction failure leaves both tables empty."""
    ledger = _make_ledger_entry()
    # Build a registry row with status that violates the CHECK constraint.
    # The CHECK fails AFTER the ledger INSERT inside the same tx, forcing
    # rollback of the ledger row. If the primitive were non-transactional
    # the ledger would survive.
    bad_registry = RegistryRow(
        deployment_id="TestStrat:abc123",
        chain="arbitrum",
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash="HASH_BAD",
        semantic_grouping_key="arbitrum:0xbad",
        grouping_policy_version="univ3_lp@v1",
        handle=None,
        status="INVALID_STATUS",  # type: ignore[arg-type]  -- intentional CHECK violation
        payload={"x": 1},
        matching_policy_version=1,
    )
    with pytest.raises((AccountingPersistenceError, sqlite3.IntegrityError)):
        await save_ledger_and_registry(
            state_manager, ledger=ledger, registry=bad_registry, mode="registry",
        )

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "transaction_ledger") == 0
        assert _row_count(conn, "position_registry") == 0
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_rpc_lost_response_retry(state_manager, temp_db_path):
    """D3.F2 — successful call + retry produces single registry row."""
    ledger = _make_ledger_entry(id_="rpc-test")
    registry = _make_registry_row(physical_identity_hash="HASH_RPC")

    await save_ledger_and_registry(
        state_manager, ledger=ledger, registry=registry, mode="registry",
    )
    # Simulate "response was lost" — runner retries with same args.
    await save_ledger_and_registry(
        state_manager, ledger=ledger, registry=registry, mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "transaction_ledger", "id = ?", ("rpc-test",)) == 1
        assert _row_count(
            conn, "position_registry",
            "physical_identity_hash = ?", ("HASH_RPC",),
        ) == 1
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_malformed_input_raises_no_partial_write(state_manager, temp_db_path):
    """D3.F4 — malformed inputs raise BEFORE any DB write."""
    # 1: registry mode but registry=None
    with pytest.raises(ValueError, match="requires the 'registry' argument"):
        await save_ledger_and_registry(
            state_manager, ledger=_make_ledger_entry(), mode="registry",
        )

    # 2: accounting_only with registry passed (covered in D1.S2 dedicated test).

    # 3: empty physical_identity_hash
    bad_registry = _make_registry_row(physical_identity_hash="")
    with pytest.raises(ValueError, match="non-empty"):
        await save_ledger_and_registry(
            state_manager, ledger=_make_ledger_entry(), registry=bad_registry, mode="registry",
        )

    # 4: whitespace-only physical_identity_hash
    bad_registry_ws = _make_registry_row(physical_identity_hash="   ")
    with pytest.raises(ValueError, match="non-empty"):
        await save_ledger_and_registry(
            state_manager, ledger=_make_ledger_entry(), registry=bad_registry_ws, mode="registry",
        )

    # 5: handle deployment_id mismatch
    registry = _make_registry_row()
    handle = HandleMapping(
        handle="leg_a",
        deployment_id="OtherStrat:mismatch",  # mismatched
        accounting_category=AccountingCategory.LP,
    )
    with pytest.raises(ValueError, match="deployment_id must match"):
        await save_ledger_and_registry(
            state_manager, ledger=_make_ledger_entry(), registry=registry, handle=handle,
            mode="registry",
        )

    # 6: invalid mode value (e.g., a typo). The dispatch MUST raise rather
    #    than silently downgrade to accounting_only — otherwise a future
    #    typo in a caller would land a ledger row without the matching
    #    registry row, exactly the failure mode the primitive exists to
    #    prevent.
    with pytest.raises(ValueError, match="mode must be"):
        await save_ledger_and_registry(
            state_manager,
            ledger=_make_ledger_entry(),
            registry=_make_registry_row(physical_identity_hash="HASH_TYPO"),
            mode="regsitry",  # type: ignore[arg-type]  -- deliberate typo
        )

    # 7: invalid mode value with NO registry/handle (could be ambiguous if
    #    treated as accounting_only). Must still raise — invalid mode is
    #    invalid regardless of args.
    with pytest.raises(ValueError, match="mode must be"):
        await save_ledger_and_registry(
            state_manager,
            ledger=_make_ledger_entry(),
            mode="account_only",  # type: ignore[arg-type]
        )

    # 8: empty registry.handle (registry mode, no separate HandleMapping).
    #    The partial unique index ix_registry_handle filters on `WHERE handle
    #    IS NOT NULL` — two rows with handle='' would BOTH be admitted by
    #    the index but compared as equal by the unique constraint, silently
    #    colliding. CodeRabbit PR #2207 finding: reject blank handles upfront.
    with pytest.raises(ValueError, match="non-empty, non-whitespace"):
        await save_ledger_and_registry(
            state_manager,
            ledger=_make_ledger_entry(),
            registry=_make_registry_row(handle=""),
            mode="registry",
        )

    # 9: whitespace-only registry.handle.
    with pytest.raises(ValueError, match="non-empty, non-whitespace"):
        await save_ledger_and_registry(
            state_manager,
            ledger=_make_ledger_entry(),
            registry=_make_registry_row(handle="   "),
            mode="registry",
        )

    # All tables empty.
    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "transaction_ledger") == 0
        assert _row_count(conn, "position_registry") == 0
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_payload_with_non_json_serializable_raises(state_manager):
    """RegistryRow.payload_json must raise TypeError on non-JSON values.

    Gemini PR #2207 finding: ``json.dumps(default=str)`` silently coerces
    Decimals / datetimes / dataclasses to strings, which is irreversible at
    read time. The fix drops ``default=str`` and forces callers to convert
    upstream. This test pins the strict behaviour: a non-JSON-serializable
    value in ``payload`` raises rather than landing as a stringified column.
    """
    from decimal import Decimal

    bad = _make_registry_row(payload={"fee": Decimal("1.5")})
    with pytest.raises(TypeError):
        bad.payload_json()


@pytest.mark.asyncio
async def test_same_status_retry_backfills_missing_handle(state_manager, temp_db_path):
    """Same-status retry MUST backfill a NULL handle (CodeRabbit PR #2207).

    The priority-gated WHERE clause on the main DO UPDATE skips the entire
    update when status doesn't strictly increase. Without an explicit handle-
    backfill statement, a row that landed with ``handle=NULL`` would stay
    NULL forever even if a later same-status writer knows the handle.

    Scenario:
      1. First write: open row with handle=None.
      2. Second write: same primary key, same status (open), now with
         handle="leg_a". Status priority unchanged → main DO UPDATE skips.
      3. The separate handle-backfill UPDATE runs and writes "leg_a"
         (idempotent: WHERE handle IS NULL).
      4. Third write: same primary key, same status, handle="attacker".
         WHERE handle IS NULL filters out — existing handle preserved.
    """
    # 1. First write: handle=None.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(intent_type="LP_OPEN"),
        registry=_make_registry_row(handle=None, status="open"),
        mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        row = conn.execute(
            "SELECT handle FROM position_registry WHERE physical_identity_hash=?",
            ("HASH_A",),
        ).fetchone()
        assert row[0] is None, f"first write should land handle=NULL, got {row[0]!r}"
    finally:
        conn.close()

    # 2. Same-status retry with newly-known handle.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(intent_type="LP_OPEN"),
        registry=_make_registry_row(handle="leg_a", status="open"),
        mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        row = conn.execute(
            "SELECT handle FROM position_registry WHERE physical_identity_hash=?",
            ("HASH_A",),
        ).fetchone()
        assert row[0] == "leg_a", (
            f"same-status retry must backfill NULL→'leg_a'; got {row[0]!r}. "
            "If this fails, the backfill UPDATE in sqlite.py was reverted "
            "or the WHERE handle IS NULL guard is wrong."
        )
    finally:
        conn.close()

    # 3. Adversarial same-status retry with different handle MUST NOT overwrite.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(intent_type="LP_OPEN"),
        registry=_make_registry_row(handle="attacker", status="open"),
        mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        row = conn.execute(
            "SELECT handle FROM position_registry WHERE physical_identity_hash=?",
            ("HASH_A",),
        ).fetchone()
        assert row[0] == "leg_a", (
            f"adversarial retry must NOT overwrite existing handle; got "
            f"{row[0]!r}. The backfill UPDATE's WHERE handle IS NULL guard "
            "is wrong."
        )
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_sqlite_operational_error_propagates(state_manager, temp_db_path):
    """D3.F5 — sqlite OperationalError surfaces as AccountingPersistenceError."""
    # Patch the backend's transactional method to raise a low-level error.
    # The function should wrap it into a typed AccountingPersistenceError so
    # callers (the runner) can take the fail-closed branch.
    from almanak.framework.state.backends import sqlite as sqlite_backend_mod

    original = sqlite_backend_mod.SQLiteStore.save_ledger_and_registry_atomic

    async def _broken(self, *args, **kwargs):
        raise sqlite3.OperationalError("disk I/O error")

    with patch.object(
        sqlite_backend_mod.SQLiteStore,
        "save_ledger_and_registry_atomic",
        _broken,
    ):
        with pytest.raises(AccountingPersistenceError):
            await save_ledger_and_registry(
                state_manager,
                ledger=_make_ledger_entry(),
                registry=_make_registry_row(),
                mode="registry",
            )


# =============================================================================
# D3.F6 — cross-table parity invariant (8-scenario property test)
# =============================================================================


@pytest.mark.asyncio
async def test_no_path_strands_one_row(temp_db_path):
    """D3.F6 — for every accepted input shape, ledger and registry are atomic.

    Scenario matrix per UAT card. Each scenario uses a fresh state_manager so
    the invariant is checked on independent state.
    """
    deployment_id = "TestStrat:abc123"
    chain = "arbitrum"
    primitive_str = "lp"
    pih = "HASH_PARITY"

    async def _fresh_manager():
        config = StateManagerConfig(
            warm_backend=WarmBackendType.SQLITE,
            sqlite_config=SQLiteConfigLight(db_path=":memory:", wal_mode=False),
            load_state_on_startup=False,
        )
        m = StateManager(config)
        await m.initialize()
        return m

    def _check_parity(conn: sqlite3.Connection, expected_ledger: int, expected_registry: int):
        ledger_count = _row_count(conn, "transaction_ledger")
        reg_count = _row_count(
            conn, "position_registry",
            "deployment_id=? AND chain=? AND primitive=? AND physical_identity_hash=?",
            (deployment_id, chain, primitive_str, pih),
        )
        assert ledger_count == expected_ledger, (
            f"ledger mismatch: got {ledger_count} expected {expected_ledger}"
        )
        assert reg_count == expected_registry, (
            f"registry mismatch: got {reg_count} expected {expected_registry}"
        )

    def _conn(manager: StateManager) -> sqlite3.Connection:
        store = manager._warm  # type: ignore[attr-defined]
        return store._conn  # type: ignore[union-attr,attr-defined]

    # Scenario 1: first OPEN, registry mode, no handle.
    m = await _fresh_manager()
    try:
        await save_ledger_and_registry(
            m, ledger=_make_ledger_entry(id_="s1"),
            registry=_make_registry_row(physical_identity_hash=pih),
            mode="registry",
        )
        _check_parity(_conn(m), expected_ledger=1, expected_registry=1)
    finally:
        await m.close()

    # Scenario 2: first OPEN with handle.
    m = await _fresh_manager()
    try:
        await save_ledger_and_registry(
            m, ledger=_make_ledger_entry(id_="s2"),
            registry=_make_registry_row(physical_identity_hash=pih, handle="leg_a"),
            handle=HandleMapping(
                handle="leg_a", deployment_id=deployment_id,
                accounting_category=AccountingCategory.LP,
            ),
            mode="registry",
        )
        _check_parity(_conn(m), expected_ledger=1, expected_registry=1)
    finally:
        await m.close()

    # Scenario 3: accounting_only — ledger only, no registry row for this hash.
    m = await _fresh_manager()
    try:
        await save_ledger_and_registry(
            m, ledger=_make_ledger_entry(id_="s3"), mode="accounting_only",
        )
        _check_parity(_conn(m), expected_ledger=1, expected_registry=0)
    finally:
        await m.close()

    # Scenario 4: row open → CLOSE update.
    m = await _fresh_manager()
    try:
        await save_ledger_and_registry(
            m, ledger=_make_ledger_entry(id_="s4-open"),
            registry=_make_registry_row(physical_identity_hash=pih, status="open"),
            mode="registry",
        )
        await save_ledger_and_registry(
            m, ledger=_make_ledger_entry(id_="s4-close", intent_type="LP_CLOSE"),
            registry=_make_registry_row(
                physical_identity_hash=pih, status="closed",
                closed_at_block=1500, closed_tx="0xclose",
            ),
            mode="registry",
        )
        # Ledger has 2 rows (open + close); registry stays at 1 (UPSERT).
        _check_parity(_conn(m), expected_ledger=2, expected_registry=1)
        # Status reflects the close.
        cursor = _conn(m).execute(
            "SELECT status FROM position_registry WHERE physical_identity_hash=?", (pih,),
        )
        assert cursor.fetchone()[0] == "closed"
    finally:
        await m.close()

    # Scenario 5: lost-RPC retry — same identity, same status.
    m = await _fresh_manager()
    try:
        await save_ledger_and_registry(
            m, ledger=_make_ledger_entry(id_="s5"),
            registry=_make_registry_row(physical_identity_hash=pih, status="open"),
            mode="registry",
        )
        await save_ledger_and_registry(
            m, ledger=_make_ledger_entry(id_="s5"),  # same id; INSERT OR REPLACE
            registry=_make_registry_row(physical_identity_hash=pih, status="open"),
            mode="registry",
        )
        # Ledger uses INSERT OR REPLACE on id → 1 row even after retry.
        _check_parity(_conn(m), expected_ledger=1, expected_registry=1)
    finally:
        await m.close()

    # Scenario 6: stale retry status=open after status=closed.
    # Registry status MUST stay closed (priority guard).
    m = await _fresh_manager()
    try:
        await save_ledger_and_registry(
            m, ledger=_make_ledger_entry(id_="s6-open"),
            registry=_make_registry_row(physical_identity_hash=pih, status="open"),
            mode="registry",
        )
        await save_ledger_and_registry(
            m, ledger=_make_ledger_entry(id_="s6-close", intent_type="LP_CLOSE"),
            registry=_make_registry_row(
                physical_identity_hash=pih, status="closed",
                closed_at_block=1500, closed_tx="0xclose",
            ),
            mode="registry",
        )
        await save_ledger_and_registry(
            m, ledger=_make_ledger_entry(id_="s6-stale"),
            registry=_make_registry_row(physical_identity_hash=pih, status="open"),
            mode="registry",
        )
        cursor = _conn(m).execute(
            "SELECT status FROM position_registry WHERE physical_identity_hash=?", (pih,),
        )
        assert cursor.fetchone()[0] == "closed", (
            "scenario 6 — status must stay closed under stale-open retry "
            "(catches a >= regression of the priority guard)"
        )
        _check_parity(_conn(m), expected_ledger=3, expected_registry=1)
    finally:
        await m.close()

    # Scenario 7: empty physical_identity_hash → ValueError pre-tx; no rows.
    m = await _fresh_manager()
    try:
        with pytest.raises(ValueError):
            await save_ledger_and_registry(
                m, ledger=_make_ledger_entry(id_="s7"),
                registry=_make_registry_row(physical_identity_hash=""),
                mode="registry",
            )
        _check_parity(_conn(m), expected_ledger=0, expected_registry=0)
    finally:
        await m.close()

    # Scenario 8: registry CHECK violation → both empty.
    m = await _fresh_manager()
    try:
        bad = RegistryRow(
            deployment_id=deployment_id,
            chain=chain,
            primitive=Primitive.LP,
            accounting_category=AccountingCategory.LP,
            physical_identity_hash=pih,
            semantic_grouping_key="g",
            grouping_policy_version="v1",
            handle=None,
            status="BAD",  # type: ignore[arg-type]
            payload={},
            matching_policy_version=1,
        )
        with pytest.raises((AccountingPersistenceError, sqlite3.IntegrityError)):
            await save_ledger_and_registry(
                m, ledger=_make_ledger_entry(id_="s8"), registry=bad, mode="registry",
            )
        _check_parity(_conn(m), expected_ledger=0, expected_registry=0)
    finally:
        await m.close()
