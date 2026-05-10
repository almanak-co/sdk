"""Reorg invalidation contract for the atomic commit primitive.

VIB-4197 / T11. Per UAT card §D3.F3 and blueprint 28 §4.4. T11 lands the
**structural** reorg-invalidation contract:

- The same atomic primitive (`save_ledger_and_registry` in `mode='registry'`)
  can flip a registry row's status from `open` to `reorg_invalidated`.
- A re-opened position (after a reorg evicts the original opening tx) MUST
  mint a NEW `physical_identity_hash` — the partial unique index
  `ix_registry_auto_mode` (filters on `status='open' AND handle IS NULL`)
  does NOT block the new row because the invalidated row is no longer in
  the open set.
- A stale `status='open'` retry against the invalidated `physical_identity_hash`
  is REJECTED by the monotone status-priority guard (terminal status is
  preserved).

Compensation for the accounting side (writing a typed `REORG_INVALIDATION`
event to `accounting_events`) lives in T26 / VIB-4212 and is OUT OF SCOPE
for this PR.

Live Anvil reorg discovery (the gateway-side `PositionService.discover_positions`
flow that DETECTS the reorg) is part of T24 / VIB-4210 and the T12 cutover
rehearsal — also out of scope here. This test exercises only the local-DB
structural primitive; reorg detection is provided by the test as a known input.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import UTC, datetime

import pytest
import pytest_asyncio

from almanak.framework.accounting.commit import (
    RegistryRow,
    save_ledger_and_registry,
)
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.primitives.types import AccountingCategory, Primitive
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
        yield os.path.join(tmpdir, "test_reorg.db")


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


# =============================================================================
# SHARED CONSTANTS — deterministic, no hidden randomness.
# =============================================================================


_DEPLOYMENT = "TestStrat:reorg01"
_CHAIN = "arbitrum"
_PRIMITIVE = Primitive.LP
_CATEGORY = AccountingCategory.LP
_GROUP_KEY = "arbitrum:0xpool_reorg"

# Hash A is the original (reorged-out) opening identity.
# Hash B is the re-open after the reorg lands a NEW NFT mint.
_HASH_A = "HASH_REORG_A"
_HASH_B = "HASH_REORG_B"


def _ledger(*, id_: str, intent_type: str = "LP_OPEN", tx_hash: str = "0xtx") -> LedgerEntry:
    return LedgerEntry(
        id=id_,
        cycle_id="reorg-cycle",
        strategy_id="reorg-test",
        deployment_id=_DEPLOYMENT,
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
        chain=_CHAIN,
        protocol="uniswap_v3",
        success=True,
        error="",
    )


def _registry(
    *,
    physical_identity_hash: str,
    status: str,
    opened_tx: str | None = None,
    opened_at_block: int | None = None,
    closed_tx: str | None = None,
    closed_at_block: int | None = None,
) -> RegistryRow:
    return RegistryRow(
        deployment_id=_DEPLOYMENT,
        chain=_CHAIN,
        primitive=_PRIMITIVE,
        accounting_category=_CATEGORY,
        physical_identity_hash=physical_identity_hash,
        semantic_grouping_key=_GROUP_KEY,
        grouping_policy_version="univ3_lp@v1",
        handle=None,
        status=status,
        payload={"token_id": 12345 if physical_identity_hash == _HASH_A else 67890},
        opened_at_block=opened_at_block,
        opened_tx=opened_tx,
        closed_at_block=closed_at_block,
        closed_tx=closed_tx,
        last_reconciled_at_block=None,
        matching_policy_version=1,
    )


# =============================================================================
# CONTRACT TESTS
# =============================================================================


@pytest.mark.asyncio
async def test_open_then_reorg_invalidate_preserves_row(state_manager, temp_db_path):
    """Open at block 100, reorg flips status to reorg_invalidated.

    Asserts: the row stays (we never DELETE) but its status reflects the
    reorg outcome. The opening tx anchors persist for audit; only `status`
    flips.
    """
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="open-A", tx_hash="0xtxA"),
        registry=_registry(
            physical_identity_hash=_HASH_A, status="open",
            opened_tx="0xtxA", opened_at_block=100,
        ),
        mode="registry",
    )
    # Caller-side reorg detection (out of scope here) decides this row is
    # phantom. Caller invokes the same primitive with status flipped.
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="reorg-A", intent_type="LP_REORG_DETECTED"),
        registry=_registry(physical_identity_hash=_HASH_A, status="reorg_invalidated"),
        mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT status, opened_tx, opened_at_block, physical_identity_hash "
            "FROM position_registry WHERE physical_identity_hash=?",
            (_HASH_A,),
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == "reorg_invalidated"
        # Opening anchors stay — they're the audit trail for the reorged
        # opening tx. Only status moves.
        assert row[1] == "0xtxA"
        assert row[2] == 100
        assert row[3] == _HASH_A
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_reopen_after_reorg_mints_new_hash(state_manager, temp_db_path):
    """After reorg invalidation, a re-open with a NEW hash lands cleanly.

    The partial unique index `ix_registry_auto_mode` filters on
    `status='open' AND handle IS NULL`; a `status='reorg_invalidated'` row
    is NOT in that set, so a fresh `status='open'` row in the same
    semantic_grouping_key is admitted.

    Asserts double-spend impossibility: the re-open uses a NEW
    `physical_identity_hash` (HASH_B); HASH_A is NEVER reused — it's
    permanently associated with the invalidated/orphaned identity.
    """
    # 1) Open at HASH_A (block 100).
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="open-A", tx_hash="0xtxA"),
        registry=_registry(
            physical_identity_hash=_HASH_A, status="open",
            opened_tx="0xtxA", opened_at_block=100,
        ),
        mode="registry",
    )
    # 2) Reorg invalidation.
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="reorg-A", intent_type="LP_REORG_DETECTED"),
        registry=_registry(physical_identity_hash=_HASH_A, status="reorg_invalidated"),
        mode="registry",
    )
    # 3) Re-open AFTER the reorg with a fresh tx → fresh NFT → fresh hash.
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="open-B", tx_hash="0xtxB"),
        registry=_registry(
            physical_identity_hash=_HASH_B, status="open",
            opened_tx="0xtxB", opened_at_block=120,
        ),
        mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        # Two registry rows for the same semantic group: one
        # reorg_invalidated (HASH_A), one open (HASH_B).
        cursor = conn.execute(
            "SELECT physical_identity_hash, status FROM position_registry "
            "WHERE deployment_id=? AND chain=? AND primitive=? "
            "ORDER BY physical_identity_hash",
            (_DEPLOYMENT, _CHAIN, "lp"),
        )
        rows = cursor.fetchall()
        assert len(rows) == 2
        by_hash = {r[0]: r[1] for r in rows}
        assert by_hash[_HASH_A] == "reorg_invalidated"
        assert by_hash[_HASH_B] == "open"

        # The auto-mode partial unique index admits the new open row even
        # though the semantic_grouping_key matches because the invalidated
        # row is filtered out by `status = 'open' AND handle IS NULL`.
        # Verify by counting rows in the open-set for this group:
        cursor = conn.execute(
            "SELECT COUNT(*) FROM position_registry "
            "WHERE deployment_id=? AND chain=? AND accounting_category=? "
            "  AND semantic_grouping_key=? AND status='open' AND handle IS NULL",
            (_DEPLOYMENT, _CHAIN, "lp", _GROUP_KEY),
        )
        assert cursor.fetchone()[0] == 1, "auto-mode index must admit exactly one open row"
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_invalidated_hash_cannot_be_resurrected(state_manager, temp_db_path):
    """Stale retry attempts to re-open the invalidated HASH_A as 'open'.

    The atomic primitive accepts the call (no exception) but the monotone
    status-priority guard preserves `reorg_invalidated` (priority 1). A
    silent regression — registry showing HASH_A 'open' again after the
    reorg — would be the structural twin of bug #2130 (a fund-stranding
    pattern). The test pins the invariant directly.
    """
    # 1) Open HASH_A.
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="open-A"),
        registry=_registry(
            physical_identity_hash=_HASH_A, status="open",
            opened_tx="0xtxA", opened_at_block=100,
        ),
        mode="registry",
    )
    # 2) Invalidate.
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="reorg-A", intent_type="LP_REORG_DETECTED"),
        registry=_registry(physical_identity_hash=_HASH_A, status="reorg_invalidated"),
        mode="registry",
    )
    # 3) Stale-retry: another caller (perhaps a queued retry from before
    # the reorg-detection path fired) tries to re-open HASH_A.
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="stale-open-A"),
        registry=_registry(
            physical_identity_hash=_HASH_A, status="open",
            opened_tx="0xtxA", opened_at_block=100,
        ),
        mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT status FROM position_registry WHERE physical_identity_hash=?",
            (_HASH_A,),
        )
        status = cursor.fetchone()[0]
        assert status == "reorg_invalidated", (
            "stale-open retry against an invalidated hash MUST be rejected by "
            "the monotone status-priority guard. Resurrecting HASH_A would "
            "leave the registry recording two simultaneously-open positions "
            "for the same semantic group — the structural failure mode that "
            "this contract exists to prevent."
        )
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_invalidated_then_closed_is_blocked(state_manager, temp_db_path):
    """closed cannot overwrite reorg_invalidated (both terminal, priority 1).

    Per blueprint 28 §4.3 the strict-`>` guard means terminal-priority-1
    states never overwrite each other. A `LP_CLOSE` intent that lands AFTER
    a reorg invalidation has been recorded MUST NOT regress status to
    `closed` (the row is already terminal).
    """
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="open-A"),
        registry=_registry(
            physical_identity_hash=_HASH_A, status="open",
            opened_tx="0xtxA", opened_at_block=100,
        ),
        mode="registry",
    )
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="reorg-A", intent_type="LP_REORG_DETECTED"),
        registry=_registry(physical_identity_hash=_HASH_A, status="reorg_invalidated"),
        mode="registry",
    )
    # A late LP_CLOSE arrives — registry must not regress.
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="late-close", intent_type="LP_CLOSE"),
        registry=_registry(
            physical_identity_hash=_HASH_A, status="closed",
            closed_tx="0xtxClose", closed_at_block=130,
        ),
        mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        cursor = conn.execute(
            "SELECT status FROM position_registry WHERE physical_identity_hash=?",
            (_HASH_A,),
        )
        assert cursor.fetchone()[0] == "reorg_invalidated", (
            "closed must NOT overwrite reorg_invalidated (priority-equal terminal states)"
        )
    finally:
        conn.close()
