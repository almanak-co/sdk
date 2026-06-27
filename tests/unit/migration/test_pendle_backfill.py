"""Unit tests for the Pendle backfill driver loop (TD-03 / VIB-5461).

Exercises the Pendle cutover's identity helpers, the position_events fold
(PT-only — see :class:`PendleCutoverReader`), and the ``PendleCutoverReader``
driver loop end-to-end against a real SQLite-backed ``StateManager`` (fold →
identity hash → idempotent backfill → restart safety).

Both Pendle kinds (PT + LP) share one isolated registry partition
(``Primitive.SWAP`` / ``cutover_key='pendle'``). The backfill covers the PT kind
(legacy ``position_events`` carry the maturity-bearing symbol anchor); the LP
kind is a runtime-only write whose legacy events carry no market column.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.migration import (
    PendleCutoverReader,
    fold_position_events_for_pendle,
    pendle_kind_for_position_type,
    pendle_registry_anchor,
    physical_identity_hash_pendle,
    semantic_grouping_key_pendle,
)
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager, StateManagerConfig

DEP = "PendleDep:abc123"
SYMBOL = "pt-wsteth-25jun2026"


def _make_state_manager(tmp_path: Path) -> StateManager:
    db_path = str(tmp_path / "pendle_backfill.db")
    sqlite = SQLiteStore(SQLiteConfig(db_path=db_path))
    return StateManager(StateManagerConfig(), warm_backend=sqlite)


def _pt_event(
    *,
    deployment_id: str = DEP,
    symbol: str = SYMBOL,
    position_type: str = "PENDLE_PT",
    event_type: str = "OPEN",
    protocol: str = "pendle",
    chain: str = "ethereum",
    suffix: str = "",
    timestamp: str = "2026-06-01T12:00:00+00:00",
) -> dict:
    wallet = "0xwallet"
    pos_id = f"pendle_pt:{chain}:{wallet}:{symbol}"
    tx_seed = f"{symbol}:{event_type}:{suffix}".encode()
    return {
        "id": f"evt-{position_type}-{event_type}-{symbol}{suffix}",
        "deployment_id": deployment_id,
        "position_id": pos_id,
        "position_type": position_type,
        "event_type": event_type,
        "timestamp": timestamp,
        "protocol": protocol,
        "chain": chain,
        "token0": symbol,
        "amount0": "1000000",
        "tx_hash": "0x" + hashlib.sha256(tx_seed).hexdigest(),
    }


def _insert_event(sm: StateManager, ev: dict) -> None:
    warm = sm._warm
    assert warm is not None
    with warm._db_lock:  # type: ignore[union-attr]
        warm._conn.execute(  # type: ignore[union-attr]
            """
            INSERT INTO position_events (id, deployment_id, position_id,
                position_type, event_type, timestamp, protocol, chain,
                token0, amount0, tx_hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ev["id"],
                ev["deployment_id"],
                ev["position_id"],
                ev["position_type"],
                ev["event_type"],
                ev["timestamp"],
                ev["protocol"],
                ev["chain"],
                ev["token0"],
                ev["amount0"],
                ev["tx_hash"],
            ),
        )
        warm._conn.commit()  # type: ignore[union-attr]


# =============================================================================
# Identity helper invariants
# =============================================================================


def test_hash_is_deterministic_and_axis_aware() -> None:
    h = physical_identity_hash_pendle(chain="Ethereum", anchor=SYMBOL.upper(), kind="pt")
    # Case-insensitive on chain + anchor.
    assert h == physical_identity_hash_pendle(chain="ethereum", anchor=SYMBOL, kind="pt")
    # Every identity axis flips the hash.
    assert h != physical_identity_hash_pendle(chain="arbitrum", anchor=SYMBOL, kind="pt")
    assert h != physical_identity_hash_pendle(chain="ethereum", anchor="pt-other-25jun2026", kind="pt")
    # Kind is part of the physical identity — a PT and an LP on the same anchor
    # never collide.
    assert h != physical_identity_hash_pendle(chain="ethereum", anchor=SYMBOL, kind="lp")


def test_hash_rejects_empty_anchor_and_bad_kind() -> None:
    with pytest.raises(ValueError):
        physical_identity_hash_pendle(chain="", anchor=SYMBOL, kind="pt")
    with pytest.raises(ValueError):
        physical_identity_hash_pendle(chain="ethereum", anchor="", kind="pt")
    with pytest.raises(ValueError):
        physical_identity_hash_pendle(chain="ethereum", anchor=SYMBOL, kind="yt")


def test_grouping_key_format() -> None:
    sgk = semantic_grouping_key_pendle(chain="Ethereum", anchor="0xMARKET", kind="lp")
    assert sgk == "ethereum:pendle:0xmarket:lp"


def test_anchor_kind_driven() -> None:
    # LP → market address.
    assert pendle_registry_anchor(kind="lp", market_address="0xMARKET", pt_symbol=None) == "0xmarket"
    # PT → maturity-bearing symbol.
    assert pendle_registry_anchor(kind="pt", market_address=None, pt_symbol=SYMBOL.upper()) == SYMBOL
    with pytest.raises(ValueError):
        pendle_registry_anchor(kind="lp", market_address="", pt_symbol=None)
    with pytest.raises(ValueError):
        pendle_registry_anchor(kind="pt", market_address=None, pt_symbol="")
    with pytest.raises(ValueError):
        pendle_registry_anchor(kind="yt", market_address="0xM", pt_symbol="x")


def test_kind_for_position_type_pt_only() -> None:
    assert pendle_kind_for_position_type("PENDLE_PT") == "pt"
    assert pendle_kind_for_position_type("pendle_pt") == "pt"
    # LP legacy events are NOT backfillable (no market column) → not mapped here.
    assert pendle_kind_for_position_type("LP") is None
    assert pendle_kind_for_position_type("") is None


# =============================================================================
# Fold contract
# =============================================================================


def test_fold_pt_open() -> None:
    row = fold_position_events_for_pendle(deployment_id=DEP, group=[_pt_event()])
    assert row is not None
    assert row.primitive == Primitive.SWAP
    assert row.accounting_category == AccountingCategory.SWAP
    assert row.grouping_policy_version == "pendle@v1"
    assert row.status == "open"
    assert row.payload["kind"] == "pt"
    assert row.payload["market_id"] == SYMBOL
    assert row.payload["pt_symbol"] == SYMBOL
    assert row.payload["protocol"] == "pendle"
    assert row.payload["source"] == "backfill"
    # Maturity is intrinsic to the anchor (the maturity-bearing symbol) — there
    # is no separate maturity field on the registry row.
    assert "maturity_ts" not in row.payload
    assert row.physical_identity_hash == physical_identity_hash_pendle(chain="ethereum", anchor=SYMBOL, kind="pt")


def test_fold_pt_close_event_closes_holding() -> None:
    group = [
        _pt_event(event_type="OPEN"),
        _pt_event(event_type="CLOSE", suffix="-c"),
    ]
    row = fold_position_events_for_pendle(deployment_id=DEP, group=group)
    assert row is not None
    assert row.status == "closed"
    assert row.closed_tx is not None


def test_fold_close_then_rebuy_is_open() -> None:
    """A Pendle (market, kind) identity is reused across buy→sell→rebuy: a CLOSE
    followed by a later OPEN is an OPEN holding (chronological last-state-wins)."""
    group = [
        _pt_event(event_type="OPEN", timestamp="2026-06-01T10:00:00+00:00", suffix="-o1"),
        _pt_event(event_type="CLOSE", timestamp="2026-06-01T11:00:00+00:00", suffix="-c1"),
        _pt_event(event_type="OPEN", timestamp="2026-06-01T12:00:00+00:00", suffix="-o2"),
    ]
    row = fold_position_events_for_pendle(deployment_id=DEP, group=group)
    assert row is not None
    assert row.status == "open"
    # Order-independent.
    row2 = fold_position_events_for_pendle(deployment_id=DEP, group=list(reversed(group)))
    assert row2 is not None
    assert row2.status == "open"


def test_fold_skips_non_pendle_pt_position_type() -> None:
    # An LP event is NOT backfillable here (handled runtime-only).
    assert fold_position_events_for_pendle(deployment_id=DEP, group=[_pt_event(position_type="LP")]) is None


def test_fold_skips_empty_group() -> None:
    assert fold_position_events_for_pendle(deployment_id=DEP, group=[]) is None


def test_fold_skips_group_with_no_usable_event() -> None:
    assert fold_position_events_for_pendle(deployment_id=DEP, group=[_pt_event(event_type="SNAPSHOT")]) is None


def test_fold_skips_missing_chain() -> None:
    ev = _pt_event()
    ev["chain"] = ""
    assert fold_position_events_for_pendle(deployment_id=DEP, group=[ev]) is None


def test_fold_skips_unparseable_symbol_anchor() -> None:
    # A position_id with no colon segment leaves no symbol anchor → skip, never
    # fabricate (Empty != Zero).
    ev = _pt_event()
    ev["position_id"] = ""
    assert fold_position_events_for_pendle(deployment_id=DEP, group=[ev]) is None


# =============================================================================
# Driver loop — end-to-end backfill against real SQLite
# =============================================================================


@pytest.mark.asyncio
async def test_backfill_synthesizes_pt_holding(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        _insert_event(sm, _pt_event(event_type="OPEN"))

        report = await PendleCutoverReader(state_manager=sm).run(deployment_id=DEP)
        assert report.rows_synthesized == 1

        rows = await sm.get_position_registry_open_rows(DEP, primitive="swap")
        assert len(rows) == 1
        assert rows[0]["payload"]["kind"] == "pt"
        assert rows[0]["payload"]["market_id"] == SYMBOL
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_is_idempotent_on_restart(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        _insert_event(sm, _pt_event(event_type="OPEN"))
        first = await PendleCutoverReader(state_manager=sm).run(deployment_id=DEP)
        assert first.rows_synthesized == 1
        second = await PendleCutoverReader(state_manager=sm).run(deployment_id=DEP)
        assert second.rows_synthesized == 0
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_preserves_existing_runtime_row(tmp_path) -> None:
    """A runtime registry-mode write that landed before the backfill ran must be
    left unchanged (DO NOTHING) — the backfill row has the SAME identity hash."""
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        pih = physical_identity_hash_pendle(chain="ethereum", anchor=SYMBOL, kind="pt")
        sgk = semantic_grouping_key_pendle(chain="ethereum", anchor=SYMBOL, kind="pt")
        runtime_row = RegistryRow(
            deployment_id=DEP,
            chain="ethereum",
            primitive=Primitive.SWAP,
            accounting_category=AccountingCategory.SWAP,
            physical_identity_hash=pih,
            semantic_grouping_key=sgk,
            grouping_policy_version="pendle@v1",
            status="open",
            payload={"protocol": "pendle", "kind": "pt", "market_id": SYMBOL, "_runtime_marker": True},
            opened_at_block=12345,
            opened_tx="0xrun",
            matching_policy_version=1,
        )
        assert await sm.insert_position_registry_row_if_absent(row=runtime_row)

        _insert_event(sm, _pt_event(event_type="OPEN"))
        report = await PendleCutoverReader(state_manager=sm).run(deployment_id=DEP)
        assert report.rows_synthesized == 0
        assert report.rows_skipped_already_present == 1

        rows = await sm.get_position_registry_open_rows(DEP, primitive="swap")
        assert len(rows) == 1
        assert rows[0]["payload"].get("_runtime_marker") is True
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_clean_db_marks_complete(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        report = await PendleCutoverReader(state_manager=sm).run(deployment_id="PendleDep:clean")
        assert report.rows_synthesized == 0
        state = await sm.get_migration_state(deployment_id="PendleDep:clean", primitive="swap", cutover_key="pendle")
        assert state is not None and state.position_registry_backfill_complete
    finally:
        await sm.close()
