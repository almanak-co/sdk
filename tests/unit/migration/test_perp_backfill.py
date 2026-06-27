"""Unit tests for the perp backfill driver loop (TD-02 / VIB-5460).

Exercises the perp cutover's identity helpers, the position_events fold, and the
``PerpCutoverReader`` driver loop end-to-end against a real SQLite-backed
``StateManager`` (fold → identity hash → idempotent backfill → restart safety).

GMX V2 is the canonical implementation; the venue position key
(``position_events.position_id`` — the GMX V2 ``positionKey``) is the identity
anchor, and the descriptive market/collateral/direction/size axes ride in the
JSON payload (no new registry columns).
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.migration import (
    PerpCutoverReader,
    fold_position_events_for_perp,
    perp_direction_label,
    physical_identity_hash_perp,
    semantic_grouping_key_perp,
)
from almanak.framework.migration import backfill as backfill_mod
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager, StateManagerConfig

DEP = "PerpDep:abc123"
KEY = "0xPositionKeyAAA"


def _make_state_manager(tmp_path: Path) -> StateManager:
    db_path = str(tmp_path / "perp_backfill.db")
    sqlite = SQLiteStore(SQLiteConfig(db_path=db_path))
    return StateManager(StateManagerConfig(), warm_backend=sqlite)


def _perp_event(
    *,
    deployment_id: str = DEP,
    position_key: str = KEY,
    event_type: str = "OPEN",
    protocol: str = "gmx_v2",
    chain: str = "arbitrum",
    collateral: str = "USDC",
    is_long: bool | None = True,
    suffix: str = "",
    timestamp: str = "2026-06-01T12:00:00+00:00",
) -> dict:
    tx_seed = f"{position_key}:{event_type}:{suffix}".encode()
    return {
        "id": f"evt-PERP-{event_type}-{position_key}{suffix}",
        "deployment_id": deployment_id,
        "position_id": position_key,
        "position_type": "PERP",
        "event_type": event_type,
        "timestamp": timestamp,
        "protocol": protocol,
        "chain": chain,
        "token0": collateral,
        "is_long": is_long,
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
                token0, is_long, tx_hash)
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
                ev["is_long"],
                ev["tx_hash"],
            ),
        )
        warm._conn.commit()  # type: ignore[union-attr]


# =============================================================================
# Identity helper invariants
# =============================================================================


def test_hash_is_deterministic_and_axis_aware() -> None:
    h = physical_identity_hash_perp(chain="arbitrum", protocol="gmx_v2", position_key="0xABC")
    # Case-insensitive on the key (GMX positionKey is a hex hash).
    assert h == physical_identity_hash_perp(chain="Arbitrum", protocol="GMX_V2", position_key="0xabc")
    # Every identity axis flips the hash.
    assert h != physical_identity_hash_perp(chain="avalanche", protocol="gmx_v2", position_key="0xABC")
    assert h != physical_identity_hash_perp(chain="arbitrum", protocol="other", position_key="0xABC")
    assert h != physical_identity_hash_perp(chain="arbitrum", protocol="gmx_v2", position_key="0xDEF")


def test_hash_rejects_empty_anchors() -> None:
    with pytest.raises(ValueError):
        physical_identity_hash_perp(chain="", protocol="gmx_v2", position_key="0xABC")
    with pytest.raises(ValueError):
        physical_identity_hash_perp(chain="arbitrum", protocol="", position_key="0xABC")
    with pytest.raises(ValueError):
        physical_identity_hash_perp(chain="arbitrum", protocol="gmx_v2", position_key="")


def test_grouping_key_format_and_singleton() -> None:
    sgk = semantic_grouping_key_perp(chain="Arbitrum", protocol="GMX_V2", position_key="0xABC")
    assert sgk == "arbitrum:gmx_v2:0xabc"


def test_direction_label() -> None:
    assert perp_direction_label(True) == "long"
    assert perp_direction_label(False) == "short"
    # SQLite round-trips the persisted boolean as an int — still measured.
    assert perp_direction_label(1) == "long"
    assert perp_direction_label(0) == "short"
    # Empty ≠ Zero — unmeasured direction stays None, never a fabricated side.
    assert perp_direction_label(None) is None
    assert perp_direction_label("") is None


# =============================================================================
# Fold contract
# =============================================================================


def test_fold_open() -> None:
    row = fold_position_events_for_perp(deployment_id=DEP, group=[_perp_event()])
    assert row is not None
    assert row.primitive == Primitive.PERP
    assert row.accounting_category == AccountingCategory.PERP
    assert row.status == "open"
    assert row.payload["position_id"] == KEY.lower()
    assert row.payload["protocol"] == "gmx_v2"
    assert row.payload["collateral_token"] == "USDC"
    assert row.payload["direction"] == "long"
    assert row.payload["source"] == "backfill"
    assert row.physical_identity_hash == physical_identity_hash_perp(
        chain="arbitrum", protocol="gmx_v2", position_key=KEY
    )


def test_fold_close_event_closes_position() -> None:
    group = [
        _perp_event(event_type="OPEN"),
        _perp_event(event_type="CLOSE", suffix="-c"),
    ]
    row = fold_position_events_for_perp(deployment_id=DEP, group=group)
    assert row is not None
    assert row.status == "closed"
    assert row.closed_tx is not None


def test_fold_close_then_reopen_is_open() -> None:
    """A perp venue position key is a reused identity (the GMX positionKey is
    deterministic): a CLOSE followed by a later OPEN is an OPEN position. The
    chronological last-state-wins fold must NOT mark it closed."""
    group = [
        _perp_event(event_type="OPEN", timestamp="2026-06-01T10:00:00+00:00", suffix="-o1"),
        _perp_event(event_type="CLOSE", timestamp="2026-06-01T11:00:00+00:00", suffix="-c1"),
        _perp_event(event_type="OPEN", timestamp="2026-06-01T12:00:00+00:00", suffix="-o2"),
    ]
    row = fold_position_events_for_perp(deployment_id=DEP, group=group)
    assert row is not None
    assert row.status == "open"
    # Order-independent: shuffled input yields the same verdict (sorted by ts).
    row2 = fold_position_events_for_perp(deployment_id=DEP, group=list(reversed(group)))
    assert row2 is not None
    assert row2.status == "open"


def test_fold_reopen_then_close_is_closed() -> None:
    group = [
        _perp_event(event_type="OPEN", timestamp="2026-06-01T10:00:00+00:00", suffix="-o1"),
        _perp_event(event_type="OPEN", timestamp="2026-06-01T11:00:00+00:00", suffix="-o2"),
        _perp_event(event_type="CLOSE", timestamp="2026-06-01T12:00:00+00:00", suffix="-c1"),
    ]
    row = fold_position_events_for_perp(deployment_id=DEP, group=group)
    assert row is not None
    assert row.status == "closed"


def test_fold_closed_position_takes_metadata_from_open_event() -> None:
    """Descriptive metadata (collateral / direction) is the open-time fact: a
    CLOSE event that does not re-emit token0 / is_long must NOT blank the
    payload — the fold reads them from the OPEN event."""
    open_ev = _perp_event(event_type="OPEN", collateral="USDC", is_long=True, timestamp="2026-06-01T10:00:00+00:00", suffix="-o")
    close_ev = _perp_event(event_type="CLOSE", timestamp="2026-06-01T11:00:00+00:00", suffix="-c")
    close_ev["token0"] = ""  # CLOSE didn't re-emit the collateral asset
    close_ev["is_long"] = None  # nor the direction
    row = fold_position_events_for_perp(deployment_id=DEP, group=[open_ev, close_ev])
    assert row is not None
    assert row.status == "closed"
    assert row.payload["collateral_token"] == "USDC"  # from the OPEN event
    assert row.payload["direction"] == "long"  # from the OPEN event


def test_fold_skips_non_enabled_protocol() -> None:
    assert fold_position_events_for_perp(deployment_id=DEP, group=[_perp_event(protocol="hyperliquid")]) is None


def test_fold_skips_missing_position_key() -> None:
    ev = _perp_event()
    ev["position_id"] = ""
    assert fold_position_events_for_perp(deployment_id=DEP, group=[ev]) is None


def test_fold_skips_empty_group() -> None:
    assert fold_position_events_for_perp(deployment_id=DEP, group=[]) is None


def test_fold_skips_group_with_no_usable_event() -> None:
    assert fold_position_events_for_perp(deployment_id=DEP, group=[_perp_event(event_type="SNAPSHOT")]) is None


def test_fold_skips_missing_chain() -> None:
    ev = _perp_event()
    ev["chain"] = ""
    assert fold_position_events_for_perp(deployment_id=DEP, group=[ev]) is None


def test_fold_short_direction_from_is_long_false() -> None:
    row = fold_position_events_for_perp(deployment_id=DEP, group=[_perp_event(is_long=False)])
    assert row is not None
    assert row.payload["direction"] == "short"


# =============================================================================
# Generalisability — the shape is protocol-agnostic (config add, not reshape)
# =============================================================================


def test_fold_generalises_to_another_gmx_shape_venue(monkeypatch: pytest.MonkeyPatch) -> None:
    """The SAME fold/identity shape works for any GMX-shape perp venue — enabling
    one is a one-line ``_PERP_REGISTRY_PROTOCOLS`` (protocol-family) add, NOT a
    reshape of the registry row."""
    monkeypatch.setattr(backfill_mod, "_PERP_REGISTRY_PROTOCOLS", frozenset({"gmx_v2", "aster_perps"}))
    ev = _perp_event(protocol="aster_perps", chain="bnb", collateral="USDT")
    row = fold_position_events_for_perp(deployment_id=DEP, group=[ev])
    assert row is not None
    assert row.primitive == Primitive.PERP
    assert row.payload["protocol"] == "aster_perps"
    assert row.payload["collateral_token"] == "USDT"
    assert row.physical_identity_hash == physical_identity_hash_perp(
        chain="bnb", protocol="aster_perps", position_key=KEY
    )


# =============================================================================
# Driver loop — end-to-end backfill against real SQLite
# =============================================================================


@pytest.mark.asyncio
async def test_backfill_synthesizes_open_perp(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        _insert_event(sm, _perp_event(position_key=KEY, event_type="OPEN"))
        report = await PerpCutoverReader(state_manager=sm).run(deployment_id=DEP)
        assert report.rows_synthesized == 1

        rows = await sm.get_position_registry_open_rows(DEP, primitive="perp", accounting_category="perp")
        assert len(rows) == 1
        assert rows[0]["payload"]["position_id"] == KEY.lower()
        assert rows[0]["payload"]["direction"] == "long"
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_closed_perp_not_open(tmp_path) -> None:
    """An OPEN→CLOSE history backfills a CLOSED row, so the teardown open-rows read
    surfaces nothing — no phantom open perp on a wiped restart."""
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        _insert_event(sm, _perp_event(event_type="OPEN", timestamp="2026-06-01T10:00:00+00:00", suffix="-o"))
        _insert_event(sm, _perp_event(event_type="CLOSE", timestamp="2026-06-01T11:00:00+00:00", suffix="-c"))
        report = await PerpCutoverReader(state_manager=sm).run(deployment_id=DEP)
        assert report.rows_synthesized == 1
        open_rows = await sm.get_position_registry_open_rows(DEP, primitive="perp", accounting_category="perp")
        assert open_rows == []
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_is_idempotent_on_restart(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        _insert_event(sm, _perp_event(event_type="OPEN"))
        first = await PerpCutoverReader(state_manager=sm).run(deployment_id=DEP)
        assert first.rows_synthesized == 1
        second = await PerpCutoverReader(state_manager=sm).run(deployment_id=DEP)
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
        pih = physical_identity_hash_perp(chain="arbitrum", protocol="gmx_v2", position_key=KEY)
        sgk = semantic_grouping_key_perp(chain="arbitrum", protocol="gmx_v2", position_key=KEY)
        runtime_row = RegistryRow(
            deployment_id=DEP,
            chain="arbitrum",
            primitive=Primitive.PERP,
            accounting_category=AccountingCategory.PERP,
            physical_identity_hash=pih,
            semantic_grouping_key=sgk,
            grouping_policy_version="perp@v1",
            status="open",
            payload={"protocol": "gmx_v2", "position_id": KEY.lower(), "_runtime_marker": True},
            opened_at_block=12345,
            opened_tx="0xrun",
            matching_policy_version=1,
        )
        assert await sm.insert_position_registry_row_if_absent(row=runtime_row)

        _insert_event(sm, _perp_event(event_type="OPEN"))
        report = await PerpCutoverReader(state_manager=sm).run(deployment_id=DEP)
        assert report.rows_synthesized == 0
        assert report.rows_skipped_already_present == 1

        rows = await sm.get_position_registry_open_rows(DEP, primitive="perp", accounting_category="perp")
        assert len(rows) == 1
        assert rows[0]["payload"].get("_runtime_marker") is True
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_clean_db_marks_complete(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        report = await PerpCutoverReader(state_manager=sm).run(deployment_id="PerpDep:clean")
        assert report.rows_synthesized == 0
        state = await sm.get_migration_state(deployment_id="PerpDep:clean", primitive="perp", cutover_key="perp")
        assert state is not None and state.position_registry_backfill_complete
    finally:
        await sm.close()
