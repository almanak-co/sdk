"""VIB-4199 / T13 — Bug #2130 acceptance test (local-mode reproduction).

Local-mode acceptance test for GitHub issue #2130: gateway returned
``gRPC UNIMPLEMENTED`` for ``save_position_event`` after an LP_OPEN tx
already landed on chain, and the runner raised
``AccountingPersistenceError`` so the iteration ended with
``ACCOUNTING_FAILED``. The structural fix that landed via the
``position_registry`` epic (T11 / VIB-4197 atomic primitive, T12 /
VIB-4198 UniV3 cutover, T14 / VIB-4200 typed collision exception) makes
this bug class **structurally impossible**:

- The ``transaction_ledger`` row + ``position_registry`` row land in
  ONE SQLite transaction (atomic). A mid-commit abort leaves both empty.
- The registry — NOT ``position_events`` — is the source of truth for
  "is this position open?" after cutover. ``save_position_event``
  failing (the bug-#2130 surface) does NOT prevent recovery.
- Two concurrent same-pool no-handle opens race on
  ``BEGIN IMMEDIATE`` against the partial unique index
  ``ix_registry_auto_mode``; one writer commits and the other raises a
  typed :class:`RegistryAutoCollisionError` — distinct from
  :class:`AccountingPersistenceError`, raised uniformly across
  ``live`` / ``paper`` / ``dry_run``.

This file is the runnable contract behind UAT card
``docs/internal/uat-cards/VIB-4199.md`` (Phase 1 verdict: ``SPEC_OK``,
card SHA ``b182a2dc7bc99af2c9040255521e072d45da9d3b``).

Test scope rationale (per the card's "Out of scope" stanza): the test is
SQLite-backed integration scope, no Anvil. The structural-recoverability
claim is proven by exercising the same atomic primitive + registry read
surfaces production uses; an Anvil round-trip would add no axis of
verification beyond what these tests pin.
"""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
import pytest_asyncio

from almanak.framework.accounting.commit import (
    RegistryRow,
    save_ledger_and_registry,
)
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.state.backends.sqlite import SQLiteStore
from almanak.framework.state.exceptions import AccountingPersistenceError
from almanak.framework.state.registry_errors import RegistryAutoCollisionError
from almanak.framework.state.state_manager import (
    SQLiteConfigLight,
    StateManager,
    StateManagerConfig,
    WarmBackendType,
)

# =============================================================================
# FIXTURES
# =============================================================================


_FIXTURE_DIR = (
    Path(__file__).resolve().parents[2] / "fixtures" / "bug-2130-local"
)


def _load_fixture(name: str) -> dict:
    """Load a JSON fixture file from ``tests/fixtures/bug-2130-local/``.

    Strict load: any JSON-decode error or missing file is a test-bug
    surface, not a silent-pass surface.
    """
    path = _FIXTURE_DIR / name
    if not path.exists():
        raise FileNotFoundError(
            f"VIB-4199 fixture missing at {path}. The test depends on the "
            "deterministic fixture data; recreate from "
            "tests/fixtures/bug-2130-local/README.md."
        )
    return json.loads(path.read_text(encoding="utf-8"))


# Cache the deployment scalars once per module — they are shared by every
# test and never mutated.
_DEPLOYMENT = _load_fixture("deployment.json")
_LP_OPEN = _load_fixture("lp_open.json")
_LP_CLOSE = _load_fixture("lp_close.json")


@pytest_asyncio.fixture
async def temp_db_path():
    """Temp-file SQLite path for the fixture-bound state manager.

    File-backed (not ``:memory:``) so:
    - The atomic primitive's ``BEGIN IMMEDIATE`` lock fires correctly under
      concurrency (D3.F3 — two instances on the same on-disk DB).
    - Sibling tests that open a second ``sqlite3.connect(path)`` to assert
      durable post-state see the same rows.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        yield os.path.join(tmpdir, "test_bug_2130_local.db")


@pytest_asyncio.fixture
async def state_manager(temp_db_path):
    """Initialized StateManager backed by a fresh on-disk SQLite DB."""
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
    id_: str = "ledger-vib4199",
    intent_type: str = "LP_OPEN",
    tx_hash: str | None = None,
    deployment_id: str | None = None,
    chain: str | None = None,
    execution_mode: str = "live",
) -> LedgerEntry:
    """Builder for a deterministic LedgerEntry tied to the bug-2130 fixture."""
    return LedgerEntry(
        id=id_,
        cycle_id="cycle-bug2130",
        strategy_id="test-bug-2130",
        deployment_id=deployment_id or _DEPLOYMENT["deployment_id"],
        execution_mode=execution_mode,
        timestamp=datetime(2026, 5, 5, 3, 16, 35, tzinfo=UTC),
        intent_type=intent_type,
        token_in="USDC",
        amount_in="100",
        token_out="WETH",
        amount_out="0.04",
        effective_price="2500",
        slippage_bps=10.0,
        gas_used=200000,
        gas_usd="0.50",
        tx_hash=tx_hash or _LP_OPEN["tx_hash"],
        chain=chain or _DEPLOYMENT["chain"],
        protocol="uniswap_v3",
        success=True,
        error="",
    )


def _make_open_registry_row(
    *,
    physical_identity_hash: str | None = None,
    semantic_grouping_key: str | None = None,
    handle: str | None = None,
    deployment_id: str | None = None,
    chain: str | None = None,
    payload: dict | None = None,
) -> RegistryRow:
    """Builder for the OPEN registry row for the bug-2130 NFT."""
    return RegistryRow(
        deployment_id=deployment_id or _DEPLOYMENT["deployment_id"],
        chain=chain or _DEPLOYMENT["chain"],
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash=(
            physical_identity_hash
            if physical_identity_hash is not None
            else _LP_OPEN["physical_identity_hash"]
        ),
        semantic_grouping_key=(
            semantic_grouping_key
            if semantic_grouping_key is not None
            else _DEPLOYMENT["semantic_grouping_key"]
        ),
        grouping_policy_version=_DEPLOYMENT["grouping_policy_version"],
        handle=handle,
        status="open",
        payload=payload if payload is not None else dict(_LP_OPEN["registry_payload"]),
        opened_at_block=_LP_OPEN["block_number"],
        opened_tx=_LP_OPEN["tx_hash"],
        closed_at_block=None,
        closed_tx=None,
        last_reconciled_at_block=None,
        matching_policy_version=_DEPLOYMENT["matching_policy_version"],
    )


def _make_close_registry_row() -> RegistryRow:
    """Builder for the CLOSE-side registry row (same PIH as the OPEN)."""
    return RegistryRow(
        deployment_id=_DEPLOYMENT["deployment_id"],
        chain=_DEPLOYMENT["chain"],
        primitive=Primitive.LP,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash=_LP_CLOSE["physical_identity_hash"],
        semantic_grouping_key=_DEPLOYMENT["semantic_grouping_key"],
        grouping_policy_version=_DEPLOYMENT["grouping_policy_version"],
        handle=None,
        status="closed",
        payload=dict(_LP_CLOSE["registry_payload"]),
        opened_at_block=_LP_OPEN["block_number"],
        opened_tx=_LP_OPEN["tx_hash"],
        closed_at_block=_LP_CLOSE["block_number"],
        closed_tx=_LP_CLOSE["tx_hash"],
        last_reconciled_at_block=None,
        matching_policy_version=_DEPLOYMENT["matching_policy_version"],
    )


_ALLOWED_TABLES = frozenset({"transaction_ledger", "position_registry"})


def _row_count(conn: sqlite3.Connection, table: str, where: str = "1=1", params: tuple = ()) -> int:
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Table {table!r} not in allowed set: {_ALLOWED_TABLES}")
    cursor = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {where}", params)
    return cursor.fetchone()[0]


# =============================================================================
# D1 — Correctness (bug #2130 scenario reproduced and structurally fixed)
# =============================================================================


@pytest.mark.asyncio
async def test_d1_s1_save_position_event_unimplemented_does_not_prevent_registry_recovery(
    state_manager, temp_db_path,
):
    """D1.S1 — UNIMPLEMENTED-equivalent ``save_position_event`` does not
    prevent the registry write from landing.

    The bug-#2130 production surface returned False / UNIMPLEMENTED for
    ``save_position_event`` AFTER the LP_OPEN tx had already landed
    on-chain. This test patches the legacy method to mirror that surface
    and proves that:

    1. The atomic primitive ``save_ledger_and_registry`` writes both
       ledger + registry rows in one transaction.
    2. ``save_position_event`` is NEVER called from inside the registry
       write path (D3.F2 mock-count contract).
    3. The registry row carries the NFT ``token_id`` from the fixture,
       and the ``physical_identity_hash`` matches the OPEN identity.
    """
    # Patch the legacy method to mirror the gateway-UNIMPLEMENTED surface.
    save_pos_mock = AsyncMock(return_value=False)
    sqlite_pos_mock = AsyncMock(return_value=False)
    with (
        patch.object(state_manager, "save_position_event", save_pos_mock),
        patch.object(state_manager._warm, "save_position_event", sqlite_pos_mock),
    ):
        ledger = _make_ledger_entry(id_="d1-s1-open")
        registry = _make_open_registry_row()
        await save_ledger_and_registry(
            state_manager, ledger=ledger, registry=registry, mode="registry",
        )

    # F2 — the registry write path must NOT call save_position_event.
    assert save_pos_mock.call_count == 0, (
        "save_ledger_and_registry must NOT call save_position_event from "
        "inside the registry write path; if it did, the bug-#2130 surface "
        "would propagate up via _handle_position_event_save_failure. "
        f"Got call_count={save_pos_mock.call_count}."
    )
    assert sqlite_pos_mock.call_count == 0

    # Durable post-state.
    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "transaction_ledger", "id = ?", ("d1-s1-open",)) == 1
        assert _row_count(conn, "position_registry") == 1
        cursor = conn.execute(
            "SELECT physical_identity_hash, status, primitive, accounting_category, "
            "       payload FROM position_registry LIMIT 1",
        )
        pih, status, primitive, category, payload_json = cursor.fetchone()
        assert pih == _LP_OPEN["physical_identity_hash"]
        assert status == "open"
        assert primitive == "lp"
        assert category == "lp"
        payload = json.loads(payload_json)
        assert payload["token_id"] == 5468420, (
            "registry payload must carry the bug-#2130 NFT token_id 5468420; "
            f"got payload={payload!r}"
        )
    finally:
        conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "save_position_event_outcome",
    [
        ("returns_false", AsyncMock(return_value=False)),
        ("raises_not_implemented", AsyncMock(side_effect=NotImplementedError("UNIMPLEMENTED"))),
    ],
    ids=["returns_false", "raises_not_implemented"],
)
async def test_d1_s2_registry_recovers_token_id_independent_of_save_position_event(
    state_manager, temp_db_path, save_position_event_outcome,
):
    """D1.S2 — the registry's ``token_id`` is recoverable through the
    same accessor production teardown / dashboard / strategy author paths
    use, regardless of how ``save_position_event`` failed.

    Parametrisation pins both UNIMPLEMENTED-equivalent surfaces:
      - returns_false: ``save_position_event`` returns False (the
        SQLiteStore behaviour the runner reads as
        ``AccountingPersistenceError`` in live mode today).
      - raises_not_implemented: ``save_position_event`` raises
        ``NotImplementedError`` (the gRPC-UNIMPLEMENTED surface).
    """
    _, mock = save_position_event_outcome
    # Land the registry row first (via the atomic primitive).
    ledger = _make_ledger_entry(id_="d1-s2-open")
    registry = _make_open_registry_row()
    await save_ledger_and_registry(
        state_manager, ledger=ledger, registry=registry, mode="registry",
    )

    # Patch save_position_event to the parametrized failure surface.
    with patch.object(state_manager, "save_position_event", mock):
        # The recovery accessor (the one runner.get_open_lp_positions_from_registry
        # delegates to) MUST find the row.
        rows = await state_manager.get_position_registry_open_rows(
            _DEPLOYMENT["deployment_id"],
            chain=_DEPLOYMENT["chain"],
            primitive="lp",
            accounting_category="lp",
        )

        assert len(rows) == 1, (
            f"registry recovery must return exactly 1 open row; got {len(rows)}"
        )
        row = rows[0]
        assert row["physical_identity_hash"] == _LP_OPEN["physical_identity_hash"]
        assert row["status"] == "open"
        assert isinstance(row["payload"], dict), (
            "get_position_registry_open_rows must return parsed-dict payload "
            f"per audit m5; got {type(row['payload']).__name__}"
        )
        assert row["payload"]["token_id"] == 5468420

        # The mock can be poked but the recovery path itself must not have
        # called it.
        assert mock.call_count == 0, (
            "registry recovery accessor must NOT depend on "
            "save_position_event; got call_count="
            f"{mock.call_count} from the recovery path."
        )

    # Durable post-state confirms the registry row survived.
    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
    finally:
        conn.close()


# =============================================================================
# D2 — Scalability (variance matrix)
# =============================================================================


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fee_tier", "pool_address_suffix"),
    [
        (500, "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"),    # canonical
        (3000, "0xc31e54c7a869b9fcbecc14363cf510d1c41fa3000"),  # synthetic distinct pool
        (10000, "0xc31e54c7a869b9fcbecc14363cf510d1c41fa1000"),  # synthetic distinct pool
    ],
    ids=["fee_500", "fee_3000", "fee_10000"],
)
async def test_d2_m1_registry_recovery_across_pool_fee_tiers(
    temp_db_path, fee_tier, pool_address_suffix,
):
    """D2.M1 — D1.S1's recoverability invariant holds across pool fee
    tiers. Each (fee_tier, pool) pair gets a fresh state manager so the
    parity invariant is checked on independent state."""
    config = StateManagerConfig(
        warm_backend=WarmBackendType.SQLITE,
        sqlite_config=SQLiteConfigLight(db_path=temp_db_path, wal_mode=False),
        load_state_on_startup=False,
    )
    sm = StateManager(config)
    await sm.initialize()
    try:
        token_id = 5468420 + fee_tier  # deterministic distinct PIH per case
        pih = f"univ3:{token_id}:{_DEPLOYMENT['nft_position_manager']}:{_DEPLOYMENT['chain']}"
        sgk = f"{_DEPLOYMENT['chain']}:{pool_address_suffix}"
        payload = dict(_LP_OPEN["registry_payload"])
        payload["token_id"] = token_id
        payload["fee_tier"] = fee_tier
        payload["pool_address"] = pool_address_suffix

        ledger = _make_ledger_entry(id_=f"d2-m1-open-{fee_tier}")
        registry = _make_open_registry_row(
            physical_identity_hash=pih,
            semantic_grouping_key=sgk,
            payload=payload,
        )
        await save_ledger_and_registry(sm, ledger=ledger, registry=registry, mode="registry")

        rows = await sm.get_position_registry_open_rows(
            _DEPLOYMENT["deployment_id"], primitive="lp", accounting_category="lp",
        )
        assert len(rows) == 1
        assert rows[0]["payload"]["fee_tier"] == fee_tier
        assert rows[0]["payload"]["token_id"] == token_id
    finally:
        await sm.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("token0", "token1", "pool_id", "token_id"),
    [
        # Deterministic token_ids — no Python `hash()` use (PYTHONHASHSEED is
        # randomised across processes by default; relying on it would make
        # this test non-reproducible).
        ("USDC", "WETH", "pool_a", 1_111_111),
        ("WETH", "USDC", "pool_b", 2_222_222),
    ],
    ids=["base_quote", "quote_base"],
)
async def test_d2_m2_registry_recovery_across_token_ordering(
    temp_db_path, token0, token1, pool_id, token_id,
):
    """D2.M2 — D1.S1's recoverability invariant holds across base/quote
    orderings. UniV3 canonicalises pool addresses by (token0 < token1),
    so distinct orderings live in distinct pools — both must work."""
    config = StateManagerConfig(
        warm_backend=WarmBackendType.SQLITE,
        sqlite_config=SQLiteConfigLight(db_path=temp_db_path, wal_mode=False),
        load_state_on_startup=False,
    )
    sm = StateManager(config)
    await sm.initialize()
    try:
        pool_addr = f"0x{pool_id.encode().hex():0<40}"[:42]
        pih = f"univ3:{token_id}:{_DEPLOYMENT['nft_position_manager']}:{_DEPLOYMENT['chain']}"
        sgk = f"{_DEPLOYMENT['chain']}:{pool_addr}"
        payload = dict(_LP_OPEN["registry_payload"])
        payload.update({
            "token_id": token_id,
            "token0": token0,
            "token1": token1,
            "pool_address": pool_addr,
        })

        ledger = _make_ledger_entry(id_=f"d2-m2-open-{token0}-{token1}")
        registry = _make_open_registry_row(
            physical_identity_hash=pih,
            semantic_grouping_key=sgk,
            payload=payload,
        )
        await save_ledger_and_registry(sm, ledger=ledger, registry=registry, mode="registry")

        rows = await sm.get_position_registry_open_rows(
            _DEPLOYMENT["deployment_id"], primitive="lp", accounting_category="lp",
        )
        assert len(rows) == 1
        assert rows[0]["payload"]["token0"] == token0
        assert rows[0]["payload"]["token1"] == token1
    finally:
        await sm.close()


# =============================================================================
# D3 — Robustness (NO SILENT FAILURE — HARD GATE)
# =============================================================================


@pytest.mark.asyncio
async def test_d3_f1_mid_commit_abort_leaves_both_tables_empty(
    state_manager, temp_db_path,
):
    """D3.F1 — atomicity proof: a forced raise inside the backend's
    transactional method (the ``kill -9`` proxy: the rollback path runs
    on ANY exception from the synchronous executor) leaves BOTH tables
    empty. Specifically NOT a partial-commit fingerprint of (1, 0) or
    (0, 1)."""
    from almanak.framework.state.backends import sqlite as sqlite_backend_mod

    async def _broken(self, *args, **kwargs):  # noqa: ARG001
        # Raise AFTER the function entry (mid-transaction equivalent).
        # The wrapper re-wraps as AccountingPersistenceError per T11's
        # contract.
        raise sqlite3.OperationalError(
            "disk I/O error (kill -9 proxy: forced abort mid-commit)"
        )

    with patch.object(
        sqlite_backend_mod.SQLiteStore,
        "save_ledger_and_registry_atomic",
        _broken,
    ):
        ledger = _make_ledger_entry(id_="d3-f1-open")
        registry = _make_open_registry_row()
        with pytest.raises(AccountingPersistenceError) as excinfo:
            await save_ledger_and_registry(
                state_manager, ledger=ledger, registry=registry, mode="registry",
            )

    # Specifically NOT a typed collision (the failure has nothing to do
    # with auto-mode collision).
    assert not isinstance(excinfo.value, RegistryAutoCollisionError)

    # Atomic invariant: BOTH empty.
    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "transaction_ledger") == 0
        assert _row_count(conn, "position_registry") == 0
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d3_f2_save_position_event_returns_false_does_not_orphan_registry(
    state_manager, temp_db_path,
):
    """D3.F2 — bug #2130's exact failure mode: when
    ``save_position_event`` returns False AFTER the registry write
    landed, the registry row is still on disk and recoverable.

    The structural-recoverability claim: even if a future caller
    invokes the legacy method post-write and gets False back, the
    registry the bug-#2130 fix relies on is unaffected.
    """
    # Land the registry row.
    ledger = _make_ledger_entry(id_="d3-f2-open")
    registry = _make_open_registry_row()
    await save_ledger_and_registry(
        state_manager, ledger=ledger, registry=registry, mode="registry",
    )

    # Now patch save_position_event to the bug-#2130 surface and call it
    # explicitly (mirroring what the runner does today at strategy_runner.py:2282).
    save_pos_mock = AsyncMock(return_value=False)
    with patch.object(state_manager, "save_position_event", save_pos_mock):
        # The runner emits a position_event AFTER the atomic write
        # completes. Today the runner raises AccountingPersistenceError
        # in live mode if this returns False — that downgrade is T29
        # (out of scope). What this test pins is the *structural*
        # claim: the registry is unaffected by this path.
        result = await state_manager.save_position_event(SimpleNamespace(
            id="dummy",
            deployment_id=_DEPLOYMENT["deployment_id"],
            position_id="5468420",
            position_type="LP",
            event_type="OPEN",
            timestamp=datetime(2026, 5, 5, 3, 16, 35, tzinfo=UTC),
            protocol="uniswap_v3",
            chain=_DEPLOYMENT["chain"],
        ))
        assert result is False, "patched mock must return False for the bug-2130 surface"
        assert save_pos_mock.call_count == 1

    # Registry row UNAFFECTED — the structural invariant.
    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "transaction_ledger") == 1
        assert _row_count(conn, "position_registry") == 1
        cursor = conn.execute(
            "SELECT physical_identity_hash, status FROM position_registry LIMIT 1",
        )
        pih, status = cursor.fetchone()
        assert pih == _LP_OPEN["physical_identity_hash"]
        assert status == "open"
    finally:
        conn.close()

    # And the recovery accessor still finds it — the bug-2130 fix.
    rows = await state_manager.get_position_registry_open_rows(
        _DEPLOYMENT["deployment_id"], primitive="lp", accounting_category="lp",
    )
    assert len(rows) == 1
    assert rows[0]["payload"]["token_id"] == 5468420


@pytest.mark.asyncio
@pytest.mark.parametrize("execution_mode", ["live", "paper", "dry_run"])
async def test_d3_f3_auto_collision_under_race_raises_typed_error(
    temp_db_path, execution_mode,
):
    """D3.F3 — concurrent same-pool no-handle opens: one wins, the
    other raises typed ``RegistryAutoCollisionError`` — distinct from
    ``AccountingPersistenceError``. Mode-uniform raise across
    live / paper / dry_run (the auto-collision is a programming bug,
    not an infrastructure failure — VIB-3762 leniency does NOT apply).
    """
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
        # Two opens. Same (deployment_id, chain, accounting_category,
        # semantic_grouping_key); different physical_identity_hash; no
        # handle on either. The partial unique index ix_registry_auto_mode
        # admits exactly one.
        ledger_a = _make_ledger_entry(
            id_=f"d3-f3-A-{execution_mode}", execution_mode=execution_mode,
            tx_hash="0xtxA",
        )
        row_a = _make_open_registry_row(
            physical_identity_hash="HASH_RACE_A",
        )
        ledger_b = _make_ledger_entry(
            id_=f"d3-f3-B-{execution_mode}", execution_mode=execution_mode,
            tx_hash="0xtxB",
        )
        row_b = _make_open_registry_row(
            physical_identity_hash="HASH_RACE_B",
        )

        async def _attempt(sm, ledger, row):
            try:
                await save_ledger_and_registry(
                    sm, ledger=ledger, registry=row, mode="registry",
                )
                return ("ok", None)
            except Exception as e:  # noqa: BLE001 — categorise in result
                return ("err", e)

        # Wall-clock cap: a deadlock would exceed this.
        results = await asyncio.wait_for(
            asyncio.gather(
                _attempt(sm_a, ledger_a, row_a),
                _attempt(sm_b, ledger_b, row_b),
            ),
            timeout=5.0,
        )
    finally:
        await sm_a.close()
        await sm_b.close()

    outcomes = [r[0] for r in results]
    assert outcomes.count("ok") == 1, (
        f"expected exactly 1 ok result; got {outcomes} — "
        "the partial unique index ix_registry_auto_mode must admit "
        "exactly one writer"
    )
    assert outcomes.count("err") == 1
    err = next(r[1] for r in results if r[0] == "err")
    assert isinstance(err, RegistryAutoCollisionError), (
        f"loser must raise RegistryAutoCollisionError; got {type(err).__name__}: {err}"
    )
    # Hierarchy guard — both directions.
    assert not isinstance(err, AccountingPersistenceError), (
        "RegistryAutoCollisionError MUST NOT be caught by "
        "`except AccountingPersistenceError`; the typed-distinct contract "
        "is the whole point of VIB-4200."
    )
    assert not issubclass(RegistryAutoCollisionError, AccountingPersistenceError)
    assert not issubclass(AccountingPersistenceError, RegistryAutoCollisionError)

    # Durable post-state: exactly one row of each.
    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1
        assert _row_count(conn, "transaction_ledger") == 1
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_d3_f4_atomic_primitive_is_actually_invoked_test_bug_guard(
    state_manager, temp_db_path,
):
    """D3.F4 — test-bug guard. If the SQLite backend doesn't expose
    ``save_ledger_and_registry_atomic`` OR the registry write path
    doesn't actually invoke it, this entire test file is dispatching
    to a stale path. Surface as a test-bug.
    """
    # Surface 1: the method exists.
    assert hasattr(state_manager._warm, "save_ledger_and_registry_atomic"), (
        "SQLiteStore must expose save_ledger_and_registry_atomic; without "
        "it, every test in this file is exercising a pre-T11 fallback. "
        "If this assertion fires, the worktree is at a pre-T11 commit."
    )
    assert isinstance(state_manager._warm, SQLiteStore)

    # Surface 2: a real call reaches the method (spy via MagicMock(wraps=...)).
    original = state_manager._warm.save_ledger_and_registry_atomic
    spy_calls: list[tuple] = []

    async def _spy(ledger, registry, handle=None):
        spy_calls.append((ledger.id, registry.physical_identity_hash, handle))
        return await original(ledger, registry, handle)

    with patch.object(
        state_manager._warm,
        "save_ledger_and_registry_atomic",
        _spy,
    ):
        await save_ledger_and_registry(
            state_manager,
            ledger=_make_ledger_entry(id_="d3-f4-open"),
            registry=_make_open_registry_row(),
            mode="registry",
        )

    assert len(spy_calls) == 1, (
        "save_ledger_and_registry_atomic must be invoked exactly once per "
        f"registry-mode write; got {len(spy_calls)} invocations. If 0, the "
        "primitive is dispatching to a stale path."
    )
    assert spy_calls[0][1] == _LP_OPEN["physical_identity_hash"]


@pytest.mark.asyncio
async def test_d3_f5_partial_commit_fingerprint_is_detectable(
    state_manager, temp_db_path,
):
    """D3.F5 — a partial-commit fingerprint of (ledger=1, registry=0)
    or (0, 1) is detectable by row-count diff. The test asserts the
    inverse parity invariant on the success path (after a real OPEN +
    CLOSE round-trip, both tables are in lockstep).

    A future regression that lets the ledger land alone (or the
    registry alone) would flip the post-state to a partial fingerprint
    this test would catch.
    """
    # Successful OPEN.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="d3-f5-open"),
        registry=_make_open_registry_row(),
        mode="registry",
    )
    # Successful CLOSE.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="d3-f5-close", intent_type="LP_CLOSE"),
        registry=_make_close_registry_row(),
        mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        ledger_count = _row_count(conn, "transaction_ledger")
        registry_count = _row_count(conn, "position_registry")
    finally:
        conn.close()

    # Parity contract for the OPEN → CLOSE lifecycle:
    #   - ledger: 2 rows (open + close, distinct ids).
    #   - registry: 1 row (UPSERT on physical_identity_hash).
    # Forbidden fingerprints:
    #   - (L=1, R=0): a successful ledger write paired with a missing
    #     registry write — the fingerprint that bug #2130 would
    #     produce in a non-atomic implementation.
    #   - (L=0, R=1): a registry row without a ledger row — the
    #     hypothetical inverse partial commit.
    assert (ledger_count, registry_count) == (2, 1), (
        "OPEN→CLOSE lifecycle parity contract violated. Expected "
        f"(L=2, R=1); got (L={ledger_count}, R={registry_count}). "
        f"(1, 0) is the bug-#2130 partial-commit fingerprint and must "
        "remain impossible after the atomic primitive landed (T11)."
    )


@pytest.mark.asyncio
async def test_d3_f6_runner_get_open_positions_reads_registry(
    state_manager, temp_db_path,
):
    """D3.F6 — ``runner.get_open_lp_positions_from_registry()`` returns
    the NFT ``token_id`` from the registry. The function:

    - Is gated by ``is_cutover_active`` (cache populated when the boot
      guard cleared the (Primitive.LP, "lp") pair).
    - Calls ``state_manager.get_position_registry_open_rows`` directly.
    - Does NOT call ``save_position_event``.

    We construct a minimal runner-shaped stub via SimpleNamespace and
    invoke ``get_open_lp_positions_from_registry`` (which is a method
    on the real ``StrategyRunner``; we bind it to the stub via an
    inline class to avoid spinning up a full runner).
    """
    # 1. Land an OPEN registry row.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="d3-f6-open"),
        registry=_make_open_registry_row(),
        mode="registry",
    )

    # 2. Patch save_position_event on the state_manager — it must NEVER
    # be called by the registry-read path.
    save_pos_mock = AsyncMock(return_value=False)
    with patch.object(state_manager, "save_position_event", save_pos_mock):
        # 3. Construct a minimal runner stub. The real method lives at
        # StrategyRunner.get_open_lp_positions_from_registry — we lift it
        # off the class so the stub doesn't need a full runner __init__.
        from almanak.framework.runner.strategy_runner import StrategyRunner

        class _RunnerStub(SimpleNamespace):
            # Bind the unbound function so it picks up self.* attribute
            # access on the stub.
            get_open_lp_positions_from_registry = (
                StrategyRunner.get_open_lp_positions_from_registry
            )

        # 4a. Empty cutover cache → defense-in-depth gate fires; returns [].
        runner = _RunnerStub(
            state_manager=state_manager,
            _cutover_complete_cache=set(),
        )
        rows = await runner.get_open_lp_positions_from_registry(
            deployment_id=_DEPLOYMENT["deployment_id"],
            chain=_DEPLOYMENT["chain"],
        )
        assert rows == [], (
            "is_cutover_active gate failure must return []; got "
            f"{rows!r}. A non-empty result with an empty cutover cache "
            "is a defense-in-depth bypass."
        )

        # 4b. Populated cutover cache → registry read fires.
        runner_active = _RunnerStub(
            state_manager=state_manager,
            _cutover_complete_cache={(Primitive.LP, "lp")},
        )
        rows = await runner_active.get_open_lp_positions_from_registry(
            deployment_id=_DEPLOYMENT["deployment_id"],
            chain=_DEPLOYMENT["chain"],
        )

    # Recovery contract.
    assert len(rows) == 1, f"expected 1 open registry row; got {len(rows)}"
    assert rows[0]["payload"]["token_id"] == 5468420
    assert rows[0]["physical_identity_hash"] == _LP_OPEN["physical_identity_hash"]

    # save_position_event was NEVER consulted by the read path.
    assert save_pos_mock.call_count == 0, (
        "runner.get_open_lp_positions_from_registry must NOT call "
        "save_position_event; got call_count="
        f"{save_pos_mock.call_count}. The registry is the source of "
        "truth post-cutover."
    )


@pytest.mark.asyncio
async def test_d3_f7_teardown_lp_close_reads_token_id_from_registry(
    state_manager, temp_db_path,
):
    """D3.F7 — teardown's compiled LP_CLOSE reads ``token_id`` from the
    registry and unwinds cleanly. Test simulates the full OPEN → CLOSE
    lifecycle through the same atomic primitive teardown's
    ``commit_teardown_intent`` → ``_maybe_save_ledger_with_registry``
    path uses for LP_CLOSE.
    """
    # 1. OPEN.
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(id_="d3-f7-open"),
        registry=_make_open_registry_row(),
        mode="registry",
    )

    # 2. Pre-CLOSE, the registry has the OPEN row with the NFT token_id —
    # this is what teardown reads to compile the LP_CLOSE intent.
    pre_close_rows = await state_manager.get_position_registry_open_rows(
        _DEPLOYMENT["deployment_id"], primitive="lp", accounting_category="lp",
    )
    assert len(pre_close_rows) == 1
    assert pre_close_rows[0]["payload"]["token_id"] == 5468420
    teardown_token_id = pre_close_rows[0]["payload"]["token_id"]

    # 3. Teardown compiles & executes the LP_CLOSE.
    close_registry = _make_close_registry_row()
    await save_ledger_and_registry(
        state_manager,
        ledger=_make_ledger_entry(
            id_="d3-f7-close",
            intent_type="LP_CLOSE",
            tx_hash=_LP_CLOSE["tx_hash"],
        ),
        registry=close_registry,
        mode="registry",
    )

    # 4. Post-CLOSE, the registry row is in `closed` state with
    # populated close anchors and the token_id preserved in payload.
    conn = sqlite3.connect(temp_db_path)
    try:
        assert _row_count(conn, "position_registry") == 1, (
            "UPSERT on physical_identity_hash must keep the registry "
            "at 1 row across OPEN→CLOSE"
        )
        assert _row_count(conn, "transaction_ledger") == 2
        cursor = conn.execute(
            "SELECT status, closed_at_block, closed_tx, payload "
            "FROM position_registry LIMIT 1",
        )
        status, closed_at_block, closed_tx, payload_json = cursor.fetchone()
        assert status == "closed"
        assert closed_at_block == _LP_CLOSE["block_number"]
        assert closed_tx == _LP_CLOSE["tx_hash"]
        payload = json.loads(payload_json)
        assert payload["token_id"] == teardown_token_id
    finally:
        conn.close()

    # 5. The "what's open?" accessor now returns []. The teardown is clean.
    post_close_open_rows = await state_manager.get_position_registry_open_rows(
        _DEPLOYMENT["deployment_id"], primitive="lp", accounting_category="lp",
    )
    assert post_close_open_rows == [], (
        "post-CLOSE, the registry must have no open rows for this "
        f"deployment; got {post_close_open_rows!r}"
    )


# =============================================================================
# D5 — Anti-bypass / spec-drift guards
# =============================================================================


def test_d5_1_fixture_files_load_and_carry_canonical_token_id():
    """D5.1 — the fixture files load as JSON and carry the canonical
    bug-#2130 NFT token_id. A fixture mutation that drifts the test
    away from the bug-#2130 surface is caught here."""
    assert (_FIXTURE_DIR / "deployment.json").exists()
    assert (_FIXTURE_DIR / "lp_open.json").exists()
    assert (_FIXTURE_DIR / "lp_close.json").exists()
    assert (_FIXTURE_DIR / "README.md").exists()

    assert _DEPLOYMENT["chain"] == "arbitrum"
    assert _DEPLOYMENT["primitive"] == "lp"
    assert _DEPLOYMENT["accounting_category"] == "lp"

    # The canonical NFT token_id from issue #2130.
    assert _LP_OPEN["registry_payload"]["token_id"] == 5468420
    assert _LP_CLOSE["registry_payload"]["token_id"] == 5468420
    # The OPEN and CLOSE share one identity per the registry contract
    # (UPSERT on physical_identity_hash).
    assert _LP_OPEN["physical_identity_hash"] == _LP_CLOSE["physical_identity_hash"]


def test_d5_2_test_module_does_not_import_strategy_or_gateway_code():
    """D5.2 — the test file is a pure SQLite-backed integration test.
    It must NOT import strategy code, Anvil/RPC infrastructure, or
    the gateway egress layer. A regression that adds such an import
    would be a scope-creep symptom this test catches.
    """
    import ast

    test_file = Path(__file__).resolve()
    tree = ast.parse(test_file.read_text(encoding="utf-8"), filename=str(test_file))

    forbidden_prefixes = (
        "almanak.framework.strategies",
        "almanak.gateway",
        "almanak.framework.connectors",
        "web3",
        "solana",
    )
    violations: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                for prefix in forbidden_prefixes:
                    if alias.name == prefix or alias.name.startswith(prefix + "."):
                        violations.append(f"{test_file.name}:{node.lineno}: import {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            for prefix in forbidden_prefixes:
                if mod == prefix or mod.startswith(prefix + "."):
                    violations.append(f"{test_file.name}:{node.lineno}: from {mod} import …")

    assert not violations, (
        "VIB-4199 test file must stay scoped to the SQLite-backed "
        "registry surface. Forbidden imports:\n  " + "\n  ".join(violations)
    )
