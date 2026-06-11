"""Unit tests for the UniV4 LP backfill driver loop (VIB-4583).

Mirrors ``test_univ3_backfill.py`` for the Uniswap V4 cutover
(``Primitive.LP_V4`` / ``'lp_v4'``). Exercises the V4 identity-hash + grouping
contracts, the OPEN/CLOSE fold, idempotent backfill, the missing-PositionManager
fail-closed skip, and the grouping-key collision handle synthesis — proving V4
parity with the V3 registry discipline and that the two families never
cross-pollinate.

Design authority: ``docs/internal/VIB-4583-v4-registry-design.md``.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from almanak.framework.migration import (
    fold_position_events_for_univ4,
    physical_identity_hash_univ4,
    semantic_grouping_key_univ4,
)
from almanak.framework.migration.backfill import (
    _UNIV4_GROUPING_POLICY_VERSION,
    _UNIV4_LP_PROTOCOLS,
    UniV4LPCutoverReader,
)
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.state.state_manager import StateManager, StateManagerConfig

# A 66-char canonical V4 pool_id (PoolKey hash), not a contract address.
POOL_ID = "0x" + "ab" * 32
POOL_ID_2 = "0x" + "cd" * 32
# Base V4 PositionManager (lowercased, as the derived view returns it).
PM_BASE = "0x7c5f5a4bbd8fd63184577525326123b519429bdc"


def _make_state_manager(tmp_path: Path) -> StateManager:
    db_path = str(tmp_path / "v4_backfill.db")
    sqlite = SQLiteStore(SQLiteConfig(db_path=db_path))
    return StateManager(StateManagerConfig(), warm_backend=sqlite)


def _open_event(*, deployment_id: str, position_id: str, chain: str = "base", pool_id: str = POOL_ID) -> dict:
    return {
        "id": f"evt-open-{position_id}",
        "deployment_id": deployment_id,
        "position_id": position_id,
        "position_type": "LP",
        "event_type": "OPEN",
        "timestamp": "2026-05-01T12:00:00+00:00",
        "protocol": "uniswap_v4",
        "chain": chain,
        "tx_hash": f"0x{int(position_id):064x}",
        "tick_lower": -199740,
        "tick_upper": -197740,
        "liquidity": "1042017676194",
        "amount0": "1000000000000000",
        "amount1": "3000000",
        # V4 reports the pool_id under pool_address; the legacy extractor reads
        # the attribution_json pool_address slot.
        "attribution_json": f'{{"pool_address": "{pool_id}"}}',
    }


def _close_event(*, deployment_id: str, position_id: str, chain: str = "base", pool_id: str = POOL_ID) -> dict:
    return {
        "id": f"evt-close-{position_id}",
        "deployment_id": deployment_id,
        "position_id": position_id,
        "position_type": "LP",
        "event_type": "CLOSE",
        "timestamp": "2026-05-01T12:01:00+00:00",
        "protocol": "uniswap_v4",
        "chain": chain,
        "tx_hash": f"0x{int(position_id) + 1:064x}",
        "attribution_json": f'{{"pool_address": "{pool_id}"}}',
    }


_PE_COLUMNS = (
    "id",
    "deployment_id",
    "position_id",
    "position_type",
    "event_type",
    "timestamp",
    "protocol",
    "chain",
    "tick_lower",
    "tick_upper",
    "liquidity",
    "amount0",
    "amount1",
    "tx_hash",
    "attribution_json",
)


def _seed_events(sm: StateManager, events: list[dict]) -> None:
    warm = sm._warm
    assert warm is not None
    with warm._db_lock:  # type: ignore[union-attr]
        for ev in events:
            warm._conn.execute(  # type: ignore[union-attr]
                f"INSERT INTO position_events ({', '.join(_PE_COLUMNS)}) "
                f"VALUES ({', '.join(['?'] * len(_PE_COLUMNS))})",
                tuple(ev.get(c) for c in _PE_COLUMNS),
            )
        warm._conn.commit()  # type: ignore[union-attr]


# =============================================================================
# Hash + grouping-key invariants (golden)
# =============================================================================


def test_v4_hash_matches_design_formula() -> None:
    """Golden: sha256(chain:positionManager:tokenId) — byte-fidelity vs design §2.1."""
    import hashlib

    seed = f"base:{PM_BASE}:12345"
    expected = "0x" + hashlib.sha256(seed.encode()).hexdigest()
    assert physical_identity_hash_univ4(chain="base", position_manager_addr=PM_BASE, token_id=12345) == expected


def test_v4_hash_is_deterministic_and_chain_aware() -> None:
    h1 = physical_identity_hash_univ4(chain="base", position_manager_addr=PM_BASE, token_id=42)
    h2 = physical_identity_hash_univ4(chain="base", position_manager_addr=PM_BASE, token_id=42)
    assert h1 == h2
    h3 = physical_identity_hash_univ4(chain="ethereum", position_manager_addr=PM_BASE, token_id=42)
    assert h1 != h3


def test_v4_hash_is_case_insensitive_on_position_manager() -> None:
    lower = physical_identity_hash_univ4(chain="base", position_manager_addr=PM_BASE, token_id=42)
    upper = physical_identity_hash_univ4(chain="base", position_manager_addr=PM_BASE.upper(), token_id=42)
    assert lower == upper


@pytest.mark.parametrize("bad_token", [0, -1, "0"])
def test_v4_hash_rejects_non_positive_token_id(bad_token) -> None:
    with pytest.raises(ValueError):
        physical_identity_hash_univ4(chain="base", position_manager_addr=PM_BASE, token_id=bad_token)


def test_v4_hash_rejects_empty_chain_and_pm() -> None:
    with pytest.raises(ValueError):
        physical_identity_hash_univ4(chain="", position_manager_addr=PM_BASE, token_id=1)
    with pytest.raises(ValueError):
        physical_identity_hash_univ4(chain="base", position_manager_addr="", token_id=1)


def test_v4_grouping_key_is_chain_and_pool_id() -> None:
    sgk = semantic_grouping_key_univ4(chain="BASE", pool_id=POOL_ID.upper())
    assert sgk == f"base:{POOL_ID.lower()}"


def test_v4_grouping_key_rejects_empty() -> None:
    with pytest.raises(ValueError):
        semantic_grouping_key_univ4(chain="base", pool_id="")
    with pytest.raises(ValueError):
        semantic_grouping_key_univ4(chain="", pool_id=POOL_ID)


# =============================================================================
# Family membership — V4 distinct from V3
# =============================================================================


def test_univ4_lp_protocols_membership_is_exact() -> None:
    assert _UNIV4_LP_PROTOCOLS == frozenset({"uniswap_v4"})
    assert isinstance(_UNIV4_LP_PROTOCOLS, frozenset)


@pytest.mark.parametrize("protocol", ["uniswap_v3", "sushiswap_v3", "pancakeswap_v3", "pendle", "", "unknown"])
def test_univ4_lp_protocols_excludes_non_family(protocol: str) -> None:
    assert protocol not in _UNIV4_LP_PROTOCOLS


# =============================================================================
# Fold contract
# =============================================================================


def test_v4_fold_open_status_and_payload_shape() -> None:
    row = fold_position_events_for_univ4(deployment_id="dep:1", group=[_open_event(deployment_id="dep:1", position_id="12345")])
    assert row is not None
    assert row.status == "open"
    assert row.primitive == Primitive.LP_V4
    assert row.accounting_category == AccountingCategory.LP
    assert row.grouping_policy_version == _UNIV4_GROUPING_POLICY_VERSION
    assert row.physical_identity_hash == physical_identity_hash_univ4(
        chain="base", position_manager_addr=PM_BASE, token_id=12345
    )
    assert row.semantic_grouping_key == semantic_grouping_key_univ4(chain="base", pool_id=POOL_ID)
    # V4 identity tuple serialized into the existing payload JSON (no DDL).
    assert row.payload["token_id"] == "12345"
    assert row.payload["pool_id"] == POOL_ID.lower()
    assert row.payload["position_manager"] == PM_BASE
    # NO V3 keys leak in.
    assert "pool_address" not in row.payload
    assert "nft_manager_addr" not in row.payload


def test_v4_fold_closed_status_when_close_present() -> None:
    group = [
        _open_event(deployment_id="dep:1", position_id="42"),
        _close_event(deployment_id="dep:1", position_id="42"),
    ]
    row = fold_position_events_for_univ4(deployment_id="dep:1", group=group)
    assert row is not None
    assert row.status == "closed"
    assert row.closed_tx is not None


def test_v4_fold_is_commutative_on_event_order() -> None:
    open_e = _open_event(deployment_id="dep:1", position_id="42")
    close_e = _close_event(deployment_id="dep:1", position_id="42")
    a = fold_position_events_for_univ4(deployment_id="dep:1", group=[open_e, close_e])
    b = fold_position_events_for_univ4(deployment_id="dep:1", group=[close_e, open_e])
    assert a is not None and b is not None
    assert a.physical_identity_hash == b.physical_identity_hash
    assert a.status == b.status == "closed"


def test_v4_fold_skips_group_without_open() -> None:
    row = fold_position_events_for_univ4(deployment_id="dep:1", group=[_close_event(deployment_id="dep:1", position_id="42")])
    assert row is None


def test_v4_fold_skips_non_univ4_protocol() -> None:
    ev = _open_event(deployment_id="dep:1", position_id="42")
    ev["protocol"] = "uniswap_v3"
    assert fold_position_events_for_univ4(deployment_id="dep:1", group=[ev]) is None


def test_v4_fold_skips_when_pool_id_missing() -> None:
    """Empty ≠ Zero: missing pool_id → skip, never fabricate."""
    ev = _open_event(deployment_id="dep:1", position_id="42")
    ev["attribution_json"] = "{}"
    ev["pool_address"] = None
    assert fold_position_events_for_univ4(deployment_id="dep:1", group=[ev]) is None


@pytest.mark.parametrize("bad_id", ["0", "", "not-an-int"])
def test_v4_fold_skips_bad_token_id(bad_id: str) -> None:
    ev = _open_event(deployment_id="dep:1", position_id="1")
    ev["position_id"] = bad_id
    assert fold_position_events_for_univ4(deployment_id="dep:1", group=[ev]) is None


def test_v4_fold_skips_when_position_manager_unknown() -> None:
    """Missing PositionManager for a chain → fail-closed skip (design §3.1).

    No V4 PositionManager is registered for ``zksync``; the fold must return
    None (no row, no raise, no fabricated identity).
    """
    ev = _open_event(deployment_id="dep:1", position_id="42", chain="zksync")
    assert fold_position_events_for_univ4(deployment_id="dep:1", group=[ev]) is None


# =============================================================================
# Driver loop — happy path, idempotency, isolation, collision
# =============================================================================


@pytest.mark.asyncio
async def test_v4_backfill_streaming_and_keys_lp_v4(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:v4"
        _seed_events(sm, [_open_event(deployment_id=deployment_id, position_id=str(1000 + i)) for i in range(5)])
        reader = UniV4LPCutoverReader(state_manager=sm)
        report = await reader.run(deployment_id=deployment_id)
        assert report.rows_synthesized == 5
        rows = await sm.get_position_registry_open_rows(deployment_id, primitive="lp_v4", accounting_category="lp")
        assert len(rows) == 5
        # The V3 stream is empty — V4 rows never land under primitive='lp'.
        v3_rows = await sm.get_position_registry_open_rows(deployment_id, primitive="lp", accounting_category="lp")
        assert v3_rows == []
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_v4_backfill_idempotent_under_rerun(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:v4_idem"
        _seed_events(sm, [_open_event(deployment_id=deployment_id, position_id="42")])
        reader = UniV4LPCutoverReader(state_manager=sm)
        first = await reader.run(deployment_id=deployment_id)
        assert first.rows_synthesized == 1
        second = await reader.run(deployment_id=deployment_id)
        assert second.already_complete
        assert second.rows_synthesized == 0
        # Exactly one row — no duplicate on re-run (idempotent on physical hash).
        rows = await sm.get_position_registry_open_rows(deployment_id, primitive="lp_v4", accounting_category="lp")
        assert len(rows) == 1
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_v4_backfill_clean_db_marks_complete(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:v4_clean"
        report = await UniV4LPCutoverReader(state_manager=sm).run(deployment_id=deployment_id)
        assert report.rows_synthesized == 0
        state = await sm.get_migration_state(deployment_id=deployment_id, primitive="lp_v4", cutover_key="lp_v4")
        assert state is not None
        assert state.position_registry_backfill_complete is True
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_v4_backfill_ignores_v3_lp_rows(tmp_path) -> None:
    """A uniswap_v3 LP row is NOT folded by the V4 backfill (family isolation)."""
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:v4_iso"
        ev_v4 = _open_event(deployment_id=deployment_id, position_id="500")
        ev_v3 = _open_event(deployment_id=deployment_id, position_id="600")
        ev_v3["protocol"] = "uniswap_v3"
        _seed_events(sm, [ev_v4, ev_v3])
        report = await UniV4LPCutoverReader(state_manager=sm).run(deployment_id=deployment_id)
        assert report.rows_synthesized == 1, "uniswap_v3 row must NOT be folded by the V4 backfill"
        rows = await sm.get_position_registry_open_rows(deployment_id, primitive="lp_v4", accounting_category="lp")
        assert len(rows) == 1
        assert rows[0]["payload"]["token_id"] == "500"
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_v4_backfill_synthesizes_handle_on_pool_collision(tmp_path) -> None:
    """Two V4 NFTs in the SAME pool collide on the grouping key → second gets a
    deterministic ``__legacy_…`` handle, first keeps handle=None."""
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:v4_collide"
        # Two distinct tokenIds, SAME pool_id.
        _seed_events(
            sm,
            [
                _open_event(deployment_id=deployment_id, position_id="111", pool_id=POOL_ID),
                _open_event(deployment_id=deployment_id, position_id="222", pool_id=POOL_ID),
            ],
        )
        report = await UniV4LPCutoverReader(state_manager=sm).run(deployment_id=deployment_id)
        assert report.rows_synthesized == 2
        rows = await sm.get_position_registry_open_rows(deployment_id, primitive="lp_v4", accounting_category="lp")
        handles = sorted((r.get("handle") or "") for r in rows)
        # Exactly one row keeps handle=None (""), one gets a synthesized handle.
        assert handles[0] == ""
        assert handles[1].startswith("__legacy_lp_v4_")
    finally:
        await sm.close()


@pytest.mark.asyncio
async def test_v4_backfill_distinct_pools_do_not_collide(tmp_path) -> None:
    sm = _make_state_manager(tmp_path)
    await sm.initialize()
    try:
        deployment_id = "TestDep:v4_two_pools"
        _seed_events(
            sm,
            [
                _open_event(deployment_id=deployment_id, position_id="111", pool_id=POOL_ID),
                _open_event(deployment_id=deployment_id, position_id="222", pool_id=POOL_ID_2),
            ],
        )
        await UniV4LPCutoverReader(state_manager=sm).run(deployment_id=deployment_id)
        rows = await sm.get_position_registry_open_rows(deployment_id, primitive="lp_v4", accounting_category="lp")
        # Distinct pools → no collision → both keep handle=None.
        assert all((r.get("handle") or "") == "" for r in rows)
    finally:
        await sm.close()
