"""Unit tests for the lending backfill driver loop (TD-04 / VIB-5462).

Exercises the lending cutover's identity helpers, the position_events fold, and
the ``LendingCutoverReader`` driver loop end-to-end against a real SQLite-backed
``StateManager`` (fold → identity hash → idempotent backfill → restart safety).

Aave V3 is the canonical implementation; the SHAPE-generalisation tests prove
that enabling a non-Aave lending protocol (Spark) is a one-line frozenset add to
``_LENDING_REGISTRY_PROTOCOLS`` — not a reshape of the registry row.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from almanak.framework.accounting.commit import RegistryRow
from almanak.framework.migration import (
    LendingCutoverReader,
    fold_position_events_for_lending,
    lending_registry_market_id,
    physical_identity_hash_lending,
    semantic_grouping_key_lending,
)
from almanak.framework.migration import backfill as backfill_mod
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager, StateManagerConfig

DEP = "LendingDep:abc123"


def _make_state_manager(tmp_path: Path) -> StateManager:
    db_path = str(tmp_path / "lending_backfill.db")
    sqlite = SQLiteStore(SQLiteConfig(db_path=db_path))
    return StateManager(StateManagerConfig(), warm_backend=sqlite)


def _lending_event(
    *,
    deployment_id: str = DEP,
    asset: str = "USDC",
    position_type: str = "LENDING_COLLATERAL",
    event_type: str = "OPEN",
    protocol: str = "aave_v3",
    chain: str = "arbitrum",
    amount0: str = "1000000",
    suffix: str = "",
    timestamp: str = "2026-06-01T12:00:00+00:00",
) -> dict:
    wallet = "0xwallet"
    pos_id = f"lending:{chain}:{protocol}:{wallet}:{asset.lower()}"
    # Deterministic tx_hash (not Python's hash-seed-randomised builtin).
    tx_seed = f"{asset}:{position_type}:{event_type}:{suffix}".encode()
    return {
        "id": f"evt-{position_type}-{event_type}-{asset}{suffix}",
        "deployment_id": deployment_id,
        "position_id": pos_id,
        "position_type": position_type,
        "event_type": event_type,
        "timestamp": timestamp,
        "protocol": protocol,
        "chain": chain,
        "token0": asset,
        "amount0": amount0,
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
    h = physical_identity_hash_lending(chain="arbitrum", protocol="aave_v3", market_id="USDC", leg="collateral")
    assert h == physical_identity_hash_lending(chain="arbitrum", protocol="aave_v3", market_id="usdc", leg="collateral")
    # Every identity axis flips the hash.
    assert h != physical_identity_hash_lending(chain="ethereum", protocol="aave_v3", market_id="USDC", leg="collateral")
    assert h != physical_identity_hash_lending(chain="arbitrum", protocol="spark", market_id="USDC", leg="collateral")
    assert h != physical_identity_hash_lending(chain="arbitrum", protocol="aave_v3", market_id="DAI", leg="collateral")
    # Leg is part of the physical identity — collateral USDC != debt USDC.
    assert h != physical_identity_hash_lending(chain="arbitrum", protocol="aave_v3", market_id="USDC", leg="debt")


def test_hash_rejects_empty_anchors_and_bad_leg() -> None:
    with pytest.raises(ValueError):
        physical_identity_hash_lending(chain="", protocol="aave_v3", market_id="USDC", leg="collateral")
    with pytest.raises(ValueError):
        physical_identity_hash_lending(chain="arbitrum", protocol="", market_id="USDC", leg="collateral")
    with pytest.raises(ValueError):
        physical_identity_hash_lending(chain="arbitrum", protocol="aave_v3", market_id="", leg="collateral")
    with pytest.raises(ValueError):
        physical_identity_hash_lending(chain="arbitrum", protocol="aave_v3", market_id="USDC", leg="liquidity")


def test_grouping_key_format() -> None:
    sgk = semantic_grouping_key_lending(chain="Arbitrum", protocol="AAVE_V3", market_id="USDC", leg="debt")
    assert sgk == "arbitrum:aave_v3:usdc:debt"


def test_market_id_prefers_explicit_market_then_token() -> None:
    # Morpho-style isolated market id wins.
    assert lending_registry_market_id(market_id="0xMARKET", token="USDC") == "0xmarket"
    # Aave-style unified pool falls back to the reserve token.
    assert lending_registry_market_id(market_id=None, token="USDC") == "usdc"
    assert lending_registry_market_id(market_id="  ", token="USDC") == "usdc"
    with pytest.raises(ValueError):
        lending_registry_market_id(market_id=None, token="")


# =============================================================================
# Fold contract
# =============================================================================


def test_fold_collateral_open() -> None:
    row = fold_position_events_for_lending(deployment_id=DEP, group=[_lending_event()])
    assert row is not None
    assert row.primitive == Primitive.LENDING
    assert row.accounting_category == AccountingCategory.LENDING
    assert row.status == "open"
    assert row.payload["leg"] == "collateral"
    assert row.payload["market_id"] == "usdc"
    assert row.payload["protocol"] == "aave_v3"
    assert row.payload["source"] == "backfill"
    assert row.physical_identity_hash == physical_identity_hash_lending(
        chain="arbitrum", protocol="aave_v3", market_id="usdc", leg="collateral"
    )


def test_fold_debt_open_distinct_from_collateral() -> None:
    coll = fold_position_events_for_lending(deployment_id=DEP, group=[_lending_event(position_type="LENDING_COLLATERAL")])
    debt = fold_position_events_for_lending(
        deployment_id=DEP, group=[_lending_event(position_type="LENDING_DEBT", asset="DAI")]
    )
    assert coll is not None and debt is not None
    assert coll.payload["leg"] == "collateral"
    assert debt.payload["leg"] == "debt"
    assert coll.physical_identity_hash != debt.physical_identity_hash


def test_fold_close_event_closes_leg() -> None:
    group = [
        _lending_event(event_type="OPEN"),
        _lending_event(event_type="CLOSE", suffix="-c"),
    ]
    row = fold_position_events_for_lending(deployment_id=DEP, group=group)
    assert row is not None
    assert row.status == "closed"
    assert row.closed_tx is not None


def test_fold_close_then_reopen_is_open() -> None:
    """A lending leg is a reused identity (unlike an LP NFT): a CLOSE followed by
    a later SUPPLY is an OPEN leg. The chronological last-state-wins fold must NOT
    mark it closed (a stale 'any close ⇒ closed' fold would strand the reopened
    active position at teardown)."""
    group = [
        _lending_event(event_type="OPEN", timestamp="2026-06-01T10:00:00+00:00", suffix="-o1"),
        _lending_event(event_type="CLOSE", timestamp="2026-06-01T11:00:00+00:00", suffix="-c1"),
        _lending_event(event_type="OPEN", timestamp="2026-06-01T12:00:00+00:00", suffix="-o2"),
    ]
    row = fold_position_events_for_lending(deployment_id=DEP, group=group)
    assert row is not None
    assert row.status == "open"
    # Order-independent: shuffled input yields the same verdict (sorted by ts).
    row2 = fold_position_events_for_lending(deployment_id=DEP, group=list(reversed(group)))
    assert row2 is not None
    assert row2.status == "open"


def test_fold_reopen_then_close_is_closed() -> None:
    """The mirror: OPEN → reopen-OPEN → CLOSE ends closed (final state wins)."""
    group = [
        _lending_event(event_type="OPEN", timestamp="2026-06-01T10:00:00+00:00", suffix="-o1"),
        _lending_event(event_type="OPEN", timestamp="2026-06-01T11:00:00+00:00", suffix="-o2"),
        _lending_event(event_type="CLOSE", timestamp="2026-06-01T12:00:00+00:00", suffix="-c1"),
    ]
    row = fold_position_events_for_lending(deployment_id=DEP, group=group)
    assert row is not None
    assert row.status == "closed"


def test_fold_partial_decrease_keeps_leg_open() -> None:
    """A DECREASE (partial withdraw/repay) must NOT close the leg — bias-to-open."""
    group = [
        _lending_event(event_type="OPEN"),
        _lending_event(event_type="DECREASE", suffix="-d"),
    ]
    row = fold_position_events_for_lending(deployment_id=DEP, group=group)
    assert row is not None
    assert row.status == "open"


def test_fold_skips_non_enabled_protocol() -> None:
    ev = _lending_event(protocol="compound_v3")
    assert fold_position_events_for_lending(deployment_id=DEP, group=[ev]) is None


def test_fold_skips_missing_asset() -> None:
    ev = _lending_event()
    ev["token0"] = ""
    assert fold_position_events_for_lending(deployment_id=DEP, group=[ev]) is None


def test_fold_skips_non_lending_position_type() -> None:
    ev = _lending_event(position_type="LP")
    assert fold_position_events_for_lending(deployment_id=DEP, group=[ev]) is None


def test_fold_skips_empty_group() -> None:
    assert fold_position_events_for_lending(deployment_id=DEP, group=[]) is None


def test_fold_skips_group_with_no_usable_event() -> None:
    # A group of only non-lifecycle events (e.g. SNAPSHOT) yields no anchor.
    ev = _lending_event(event_type="SNAPSHOT")
    assert fold_position_events_for_lending(deployment_id=DEP, group=[ev]) is None


def test_fold_skips_missing_chain() -> None:
    ev = _lending_event()
    ev["chain"] = ""
    assert fold_position_events_for_lending(deployment_id=DEP, group=[ev]) is None


# =============================================================================
# Generalisability — Spark proves enabling a non-Aave protocol is a config add
# =============================================================================


def test_fold_generalises_to_spark_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """The SAME fold/identity shape works for Spark — enabling it is purely a
    one-line ``_LENDING_REGISTRY_PROTOCOLS`` add, NOT a reshape (AC2 / AC3)."""
    monkeypatch.setattr(backfill_mod, "_LENDING_REGISTRY_PROTOCOLS", frozenset({"aave_v3", "spark"}))
    ev = _lending_event(protocol="spark", asset="DAI", position_type="LENDING_DEBT", chain="ethereum")
    row = fold_position_events_for_lending(deployment_id=DEP, group=[ev])
    assert row is not None
    # Identical row SHAPE to Aave — only protocol/market/chain differ.
    assert row.primitive == Primitive.LENDING
    assert row.payload["protocol"] == "spark"
    assert row.payload["leg"] == "debt"
    assert row.payload["market_id"] == "dai"
    assert row.physical_identity_hash == physical_identity_hash_lending(
        chain="ethereum", protocol="spark", market_id="dai", leg="debt"
    )


# =============================================================================
# Driver loop — end-to-end backfill against real SQLite
# =============================================================================


@pytest.mark.asyncio
async def test_backfill_synthesizes_both_legs(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        _insert_event(sm, _lending_event(asset="USDC", position_type="LENDING_COLLATERAL"))
        _insert_event(sm, _lending_event(asset="DAI", position_type="LENDING_DEBT"))

        report = await LendingCutoverReader(state_manager=sm).run(deployment_id=DEP)
        assert report.rows_synthesized == 2

        rows = await sm.get_position_registry_open_rows(DEP, primitive="lending", accounting_category="lending")
        legs = {r["payload"]["leg"]: r["payload"]["market_id"] for r in rows}
        assert legs == {"collateral": "usdc", "debt": "dai"}
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_same_asset_both_legs_yields_two_rows(tmp_path) -> None:
    """A SUPPLY and a BORROW of the SAME asset share one position_id but are two
    distinct registry rows — the driver's position_type-aware grouping keeps the
    legs separate (would silently drop one without it)."""
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        _insert_event(sm, _lending_event(asset="USDC", position_type="LENDING_COLLATERAL"))
        _insert_event(sm, _lending_event(asset="USDC", position_type="LENDING_DEBT"))

        report = await LendingCutoverReader(state_manager=sm).run(deployment_id=DEP)
        assert report.rows_synthesized == 2
        rows = await sm.get_position_registry_open_rows(DEP, primitive="lending", accounting_category="lending")
        assert {r["payload"]["leg"] for r in rows} == {"collateral", "debt"}
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_is_idempotent_on_restart(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        _insert_event(sm, _lending_event(asset="USDC", position_type="LENDING_COLLATERAL"))
        first = await LendingCutoverReader(state_manager=sm).run(deployment_id=DEP)
        assert first.rows_synthesized == 1
        # Second run = already complete → no-op.
        second = await LendingCutoverReader(state_manager=sm).run(deployment_id=DEP)
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
        pih = physical_identity_hash_lending(chain="arbitrum", protocol="aave_v3", market_id="usdc", leg="collateral")
        sgk = semantic_grouping_key_lending(chain="arbitrum", protocol="aave_v3", market_id="usdc", leg="collateral")
        runtime_row = RegistryRow(
            deployment_id=DEP,
            chain="arbitrum",
            primitive=Primitive.LENDING,
            accounting_category=AccountingCategory.LENDING,
            physical_identity_hash=pih,
            semantic_grouping_key=sgk,
            grouping_policy_version="lending@v1",
            status="open",
            payload={"protocol": "aave_v3", "market_id": "usdc", "leg": "collateral", "_runtime_marker": True},
            opened_at_block=12345,
            opened_tx="0xrun",
            matching_policy_version=3,
        )
        assert await sm.insert_position_registry_row_if_absent(row=runtime_row)

        _insert_event(sm, _lending_event(asset="USDC", position_type="LENDING_COLLATERAL"))
        report = await LendingCutoverReader(state_manager=sm).run(deployment_id=DEP)
        assert report.rows_synthesized == 0
        assert report.rows_skipped_already_present == 1

        rows = await sm.get_position_registry_open_rows(DEP, primitive="lending", accounting_category="lending")
        assert len(rows) == 1
        assert rows[0]["payload"].get("_runtime_marker") is True
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_backfill_clean_db_marks_complete(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        report = await LendingCutoverReader(state_manager=sm).run(deployment_id="LendingDep:clean")
        assert report.rows_synthesized == 0
        state = await sm.get_migration_state(
            deployment_id="LendingDep:clean", primitive="lending", cutover_key="lending"
        )
        assert state is not None and state.position_registry_backfill_complete
    finally:
        await sm.close()
