"""Unit tests for the UniV3 LP backfill driver loop (VIB-4198 / T12).

Exercises the cutover-spec contracts in
``docs/internal/migration-cutover-position-registry.md`` §3 — fold,
identity hash, idempotent backfill, OPEN/CLOSE pair, and the
``BackfillReader`` driver loop's restart safety.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.migration import (
    BackfillFailedError,
    physical_identity_hash_univ3,
    semantic_grouping_key_univ3,
)
from almanak.framework.migration.backfill import (
    UniV3LPCutoverReader,
    fold_position_events_for_univ3,
)
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager, StateManagerConfig

POOL_ADDR = "0xc31e54c7a869b9fcbecc14363cf510d1c41fa443"
NPM_ARB = "0xc36442b4a4522e871399cd717abdd847ab11fe88"


def _make_state_manager(tmp_path: Path) -> StateManager:
    db_path = str(tmp_path / "backfill.db")
    sqlite = SQLiteStore(SQLiteConfig(db_path=db_path))
    return StateManager(StateManagerConfig(), warm_backend=sqlite)


def _open_event(
    *, deployment_id: str, position_id: str, chain: str = "arbitrum"
) -> dict:
    return {
        "id": f"evt-open-{position_id}",
        "deployment_id": deployment_id,
        "position_id": position_id,
        "position_type": "LP",
        "event_type": "OPEN",
        "timestamp": "2026-05-01T12:00:00+00:00",
        "protocol": "uniswap_v3",
        "chain": chain,
        "tx_hash": f"0x{int(position_id):064x}",
        "tick_lower": -199740,
        "tick_upper": -197740,
        "liquidity": "1042017676194",
        "amount0": "1000000000000000",
        "amount1": "3000000",
        "attribution_json": f'{{"pool_address": "{POOL_ADDR}"}}',
    }


def _close_event(
    *, deployment_id: str, position_id: str, chain: str = "arbitrum"
) -> dict:
    return {
        "id": f"evt-close-{position_id}",
        "deployment_id": deployment_id,
        "position_id": position_id,
        "position_type": "LP",
        "event_type": "CLOSE",
        "timestamp": "2026-05-01T12:01:00+00:00",
        "protocol": "uniswap_v3",
        "chain": chain,
        "tx_hash": f"0x{int(position_id):064x}",
        "attribution_json": f'{{"pool_address": "{POOL_ADDR}"}}',
    }


# =============================================================================
# Hash + grouping key invariants
# =============================================================================


def test_hash_is_deterministic_and_chain_aware() -> None:
    h1 = physical_identity_hash_univ3(
        chain="arbitrum", nft_manager_addr=NPM_ARB, token_id=5467895
    )
    h2 = physical_identity_hash_univ3(
        chain="arbitrum", nft_manager_addr=NPM_ARB, token_id=5467895
    )
    assert h1 == h2  # deterministic
    h3 = physical_identity_hash_univ3(
        chain="ethereum", nft_manager_addr=NPM_ARB, token_id=5467895
    )
    assert h1 != h3  # chain-aware


def test_hash_rejects_zero_token_id() -> None:
    with pytest.raises(ValueError):
        physical_identity_hash_univ3(chain="arbitrum", nft_manager_addr=NPM_ARB, token_id=0)


def test_hash_rejects_empty_chain() -> None:
    with pytest.raises(ValueError):
        physical_identity_hash_univ3(chain="", nft_manager_addr=NPM_ARB, token_id=1)


def test_hash_rejects_empty_nft_manager() -> None:
    with pytest.raises(ValueError):
        physical_identity_hash_univ3(chain="arbitrum", nft_manager_addr="", token_id=1)


def test_grouping_key_format() -> None:
    sgk = semantic_grouping_key_univ3(chain="arbitrum", pool_address=POOL_ADDR.upper())
    assert sgk == f"arbitrum:{POOL_ADDR.lower()}"


# =============================================================================
# Fold contract
# =============================================================================


def test_fold_produces_open_status_when_no_close() -> None:
    group = [_open_event(deployment_id="dep:1", position_id="5467895")]
    row = fold_position_events_for_univ3(deployment_id="dep:1", group=group)
    assert row is not None
    assert row.status == "open"
    assert row.physical_identity_hash == physical_identity_hash_univ3(
        chain="arbitrum", nft_manager_addr=NPM_ARB, token_id=5467895
    )
    assert row.payload["legacy_position_id"] == "5467895"


def test_fold_produces_closed_status_when_close_present() -> None:
    group = [
        _open_event(deployment_id="dep:1", position_id="5467895"),
        _close_event(deployment_id="dep:1", position_id="5467895"),
    ]
    row = fold_position_events_for_univ3(deployment_id="dep:1", group=group)
    assert row is not None
    assert row.status == "closed"
    assert row.closed_tx is not None


def test_fold_is_commutative_on_event_order() -> None:
    """Per cutover spec §3.5: fold is commutative within group."""
    open_e = _open_event(deployment_id="dep:1", position_id="42")
    close_e = _close_event(deployment_id="dep:1", position_id="42")
    row_a = fold_position_events_for_univ3(deployment_id="dep:1", group=[open_e, close_e])
    row_b = fold_position_events_for_univ3(deployment_id="dep:1", group=[close_e, open_e])
    assert row_a is not None and row_b is not None
    assert row_a.physical_identity_hash == row_b.physical_identity_hash
    assert row_a.status == row_b.status == "closed"


def test_fold_skips_group_without_open() -> None:
    """A group with only CLOSE events (no OPEN) is pathological — skip, do not synthesize."""
    group = [_close_event(deployment_id="dep:1", position_id="42")]
    row = fold_position_events_for_univ3(deployment_id="dep:1", group=group)
    assert row is None


def test_fold_skips_non_univ3_protocol() -> None:
    ev = _open_event(deployment_id="dep:1", position_id="42")
    ev["protocol"] = "pendle"
    row = fold_position_events_for_univ3(deployment_id="dep:1", group=[ev])
    assert row is None


def test_fold_skips_when_pool_address_missing() -> None:
    """Per CLAUDE.md "Empty ≠ zero": missing pool_address → skip, no fabrication."""
    ev = _open_event(deployment_id="dep:1", position_id="42")
    ev["attribution_json"] = "{}"
    ev["pool_address"] = None
    row = fold_position_events_for_univ3(deployment_id="dep:1", group=[ev])
    assert row is None


def test_fold_skips_when_token_id_zero() -> None:
    ev = _open_event(deployment_id="dep:1", position_id="0")
    row = fold_position_events_for_univ3(deployment_id="dep:1", group=[ev])
    assert row is None


# =============================================================================
# Driver loop — happy path + idempotency
# =============================================================================


@pytest.mark.asyncio
async def test_backfill_O_N_streaming_bound(tmp_path) -> None:
    """N positions → O(N) inserts, no quadratic blow-up.

    Synthesize 10 distinct OPEN events for distinct token_ids; assert the
    backfill produces 10 registry rows.
    """
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:abc"
        # Seed position_events directly via the SQLite backend.
        warm = sm._warm
        assert warm is not None
        with warm._db_lock:  # type: ignore[union-attr]
            for i in range(10):
                ev = _open_event(deployment_id=deployment_id, position_id=str(1000 + i))
                warm._conn.execute(  # type: ignore[union-attr]
                    """
                    INSERT INTO position_events (id, deployment_id, position_id,
                        position_type, event_type, timestamp, protocol, chain,
                        tick_lower, tick_upper, liquidity, amount0, amount1,
                        tx_hash, attribution_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ev["id"], ev["deployment_id"], ev["position_id"],
                        ev["position_type"], ev["event_type"], ev["timestamp"],
                        ev["protocol"], ev["chain"], ev["tick_lower"],
                        ev["tick_upper"], ev["liquidity"], ev["amount0"],
                        ev["amount1"], ev["tx_hash"], ev["attribution_json"],
                    ),
                )
            warm._conn.commit()  # type: ignore[union-attr]

        reader = UniV3LPCutoverReader(state_manager=sm)
        report = await reader.run(deployment_id=deployment_id)
        assert report.rows_synthesized == 10
        assert report.rows_skipped_already_present == 0
        assert not report.already_complete

        rows = await sm.get_position_registry_open_rows(
            deployment_id, primitive="lp", accounting_category="lp"
        )
        assert len(rows) == 10
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_idempotent_under_rerun(tmp_path) -> None:
    """A re-run after ``complete=1`` is a no-op.

    Per cutover spec §3.4: ``DO NOTHING`` ON CONFLICT keeps existing rows
    untouched. The flag check short-circuits subsequent runs.
    """
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:idem"
        warm = sm._warm
        assert warm is not None
        ev = _open_event(deployment_id=deployment_id, position_id="42")
        with warm._db_lock:  # type: ignore[union-attr]
            warm._conn.execute(  # type: ignore[union-attr]
                """
                INSERT INTO position_events (id, deployment_id, position_id,
                    position_type, event_type, timestamp, protocol, chain,
                    tick_lower, tick_upper, liquidity, amount0, amount1,
                    tx_hash, attribution_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ev["id"], ev["deployment_id"], ev["position_id"],
                    ev["position_type"], ev["event_type"], ev["timestamp"],
                    ev["protocol"], ev["chain"], ev["tick_lower"],
                    ev["tick_upper"], ev["liquidity"], ev["amount0"],
                    ev["amount1"], ev["tx_hash"], ev["attribution_json"],
                ),
            )
            warm._conn.commit()  # type: ignore[union-attr]

        reader = UniV3LPCutoverReader(state_manager=sm)
        first = await reader.run(deployment_id=deployment_id)
        assert first.rows_synthesized == 1

        # Re-run: short-circuit via complete=1.
        second = await reader.run(deployment_id=deployment_id)
        assert second.already_complete
        assert second.rows_synthesized == 0  # no-op report
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_insert_or_ignore_does_not_overwrite(tmp_path) -> None:
    """If a registry row already exists with the same physical_identity_hash,
    the backfill leaves it unchanged (DO NOTHING semantic)."""
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:ignore"

        # Pre-existing registry row (from a runtime registry-mode write
        # that landed before the backfill ran).
        row = RegistryRow(
            deployment_id=deployment_id,
            chain="arbitrum",
            primitive=Primitive.LP,
            accounting_category=AccountingCategory.LP,
            physical_identity_hash=physical_identity_hash_univ3(
                chain="arbitrum", nft_manager_addr=NPM_ARB, token_id=42
            ),
            semantic_grouping_key=semantic_grouping_key_univ3(
                chain="arbitrum", pool_address=POOL_ADDR
            ),
            grouping_policy_version="univ3_lp@v1",
            handle="runtime-handle",
            status="open",
            payload={
                "token_id": "42",
                "pool_address": POOL_ADDR,
                "nft_manager_addr": NPM_ARB,
                "_runtime_marker": True,
            },
            opened_at_block=99999,
            opened_tx="0xrunpre",
            matching_policy_version=3,
        )
        inserted = await sm.insert_position_registry_row_if_absent(row=row)
        assert inserted

        # Seed a position_events row that would synthesize the SAME row.
        warm = sm._warm
        assert warm is not None
        ev = _open_event(deployment_id=deployment_id, position_id="42")
        with warm._db_lock:  # type: ignore[union-attr]
            warm._conn.execute(  # type: ignore[union-attr]
                """
                INSERT INTO position_events (id, deployment_id, position_id,
                    position_type, event_type, timestamp, protocol, chain,
                    tick_lower, tick_upper, liquidity, amount0, amount1,
                    tx_hash, attribution_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ev["id"], ev["deployment_id"], ev["position_id"],
                    ev["position_type"], ev["event_type"], ev["timestamp"],
                    ev["protocol"], ev["chain"], ev["tick_lower"],
                    ev["tick_upper"], ev["liquidity"], ev["amount0"],
                    ev["amount1"], ev["tx_hash"], ev["attribution_json"],
                ),
            )
            warm._conn.commit()  # type: ignore[union-attr]

        reader = UniV3LPCutoverReader(state_manager=sm)
        report = await reader.run(deployment_id=deployment_id)
        # The runtime row is already present → backfill skips.
        assert report.rows_synthesized == 0
        assert report.rows_skipped_already_present == 1

        # The runtime row's payload survives unchanged (DO NOTHING).
        rows = await sm.get_position_registry_open_rows(
            deployment_id, primitive="lp", accounting_category="lp"
        )
        assert len(rows) == 1
        payload = rows[0]["payload"]
        assert payload.get("_runtime_marker") is True, (
            "DO NOTHING should preserve the existing runtime row's payload"
        )
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_clean_anvil_db_marks_complete(tmp_path) -> None:
    """A deployment with zero position_events still flips complete=1 — the
    correctness invariant per cutover spec §3.3 step 4."""
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:anvil_clean"
        reader = UniV3LPCutoverReader(state_manager=sm)
        report = await reader.run(deployment_id=deployment_id)
        assert report.rows_synthesized == 0
        # State row must show complete=1.
        state = await sm.get_migration_state(
            deployment_id=deployment_id, primitive="lp", cutover_key="lp"
        )
        assert state is not None
        assert state.position_registry_backfill_complete is True
        assert state.backfill_completed_at is not None
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_does_not_touch_non_univ3_lp_rows(tmp_path) -> None:
    """A position_events row from Pendle / TraderJoe is NOT folded into the
    UniV3 backfill — protocol filter is load-bearing."""
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:multi_protocol"
        warm = sm._warm
        assert warm is not None
        # One UniV3 row + one Pendle row.
        ev_univ3 = _open_event(deployment_id=deployment_id, position_id="500")
        ev_pendle = _open_event(deployment_id=deployment_id, position_id="600")
        ev_pendle["protocol"] = "pendle"
        with warm._db_lock:  # type: ignore[union-attr]
            for ev in (ev_univ3, ev_pendle):
                warm._conn.execute(  # type: ignore[union-attr]
                    """
                    INSERT INTO position_events (id, deployment_id, position_id,
                        position_type, event_type, timestamp, protocol, chain,
                        tick_lower, tick_upper, liquidity, amount0, amount1,
                        tx_hash, attribution_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ev["id"], ev["deployment_id"], ev["position_id"],
                        ev["position_type"], ev["event_type"], ev["timestamp"],
                        ev["protocol"], ev["chain"], ev["tick_lower"],
                        ev["tick_upper"], ev["liquidity"], ev["amount0"],
                        ev["amount1"], ev["tx_hash"], ev["attribution_json"],
                    ),
                )
            warm._conn.commit()  # type: ignore[union-attr]

        reader = UniV3LPCutoverReader(state_manager=sm)
        report = await reader.run(deployment_id=deployment_id)
        assert report.rows_synthesized == 1, (
            "Pendle row must NOT be folded by the UniV3 backfill"
        )
        rows = await sm.get_position_registry_open_rows(
            deployment_id, primitive="lp", accounting_category="lp"
        )
        assert len(rows) == 1
        assert rows[0]["payload"]["token_id"] == "500"
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_handles_open_close_pair_as_closed(tmp_path) -> None:
    """An OPEN+CLOSE pair in position_events synthesizes a closed registry row."""
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:closed"
        warm = sm._warm
        assert warm is not None
        with warm._db_lock:  # type: ignore[union-attr]
            for ev in (
                _open_event(deployment_id=deployment_id, position_id="700"),
                _close_event(deployment_id=deployment_id, position_id="700"),
            ):
                warm._conn.execute(  # type: ignore[union-attr]
                    """
                    INSERT INTO position_events (id, deployment_id, position_id,
                        position_type, event_type, timestamp, protocol, chain,
                        tick_lower, tick_upper, liquidity, amount0, amount1,
                        tx_hash, attribution_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        ev["id"], ev["deployment_id"], ev["position_id"],
                        ev["position_type"], ev["event_type"], ev["timestamp"],
                        ev["protocol"], ev["chain"],
                        ev.get("tick_lower"), ev.get("tick_upper"),
                        ev.get("liquidity"), ev.get("amount0"), ev.get("amount1"),
                        ev["tx_hash"], ev["attribution_json"],
                    ),
                )
            warm._conn.commit()  # type: ignore[union-attr]

        reader = UniV3LPCutoverReader(state_manager=sm)
        report = await reader.run(deployment_id=deployment_id)
        assert report.rows_synthesized == 1
        # OPEN-set is empty (the row landed as 'closed').
        rows = await sm.get_position_registry_open_rows(
            deployment_id, primitive="lp", accounting_category="lp"
        )
        assert len(rows) == 0
    finally:
        await sm.close()


# =============================================================================
# Failure mode — backfill driver crash wraps as BackfillFailedError
# =============================================================================


@pytest.mark.asyncio
async def test_backfill_wraps_exceptions_as_BackfillFailedError(tmp_path) -> None:
    """An unexpected exception inside the driver loop raises ``BackfillFailedError``.

    Per cutover spec §3.3 — the runner halts; restart re-runs the
    (idempotent) loop.
    """
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:crash"
        reader = UniV3LPCutoverReader(state_manager=sm)

        # Monkey-patch the SM's get_position_events_filtered to raise.
        async def boom(*_, **__):
            raise RuntimeError("simulated DB read crash")

        sm.get_position_events_filtered = boom  # type: ignore[assignment]

        with pytest.raises(BackfillFailedError) as excinfo:
            await reader.run(deployment_id=deployment_id)
        assert "simulated DB read crash" in str(excinfo.value)

        # The migration_state flag stays at 0 — a restart re-enters the
        # backfill (idempotent ON CONFLICT DO NOTHING).
        state = await sm.get_migration_state(
            deployment_id=deployment_id, primitive="lp", cutover_key="lp"
        )
        assert state is not None
        assert state.position_registry_backfill_complete is False
    finally:
        await sm.close()
