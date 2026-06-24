"""VIB-5360 / VIB-5409 — regression pin for the V4 LP close→reopen registry
collision that strands the position and leaves teardown blind.

Symptom (Arbitrum ``uniswap_v4_hooks``): a rebalance closed V4 LP position
183495, swapped, and re-opened a NEW LP in the SAME pool (WETH/USDC/3000). The
new mint landed on-chain (183524) but the SDK ledger write THREW, so the
position was never recorded in the DB. ~1 min later teardown reported
``positions_closed=0`` — blind to the live on-chain position — stranding ~$2.6.

VIB-5360 characterized the two defects; **VIB-5409 ships the fix** and this file
flips from "the collision reproduces" to "the fix frees the group".

Defect 1 — registry collision on same-pool reopen
--------------------------------------------------
The auto-mode partial unique index ``ix_registry_auto_mode`` is defined
``WHERE status = 'open' AND handle IS NULL``. A successful ``status='closed'``
write for the SAME ``physical_identity_hash`` frees the group, and a reopen in
the same pool then succeeds (this is already proven by
``test_d3_f4_closed_row_in_group_does_not_block_reopen``).

The collision in VIB-5360 therefore does NOT come from "close fails to free a
group it freed correctly". It came from the CLOSE registry row **never landing
as ``status='closed'``** for the old position — so the OLD row stayed
``status='open'`` and occupied the group when the reopen minted a NEW
``physical_identity_hash`` (new NFT token id) in the same pool.

**VIB-5409 layer 1 fix**: when the V4 LP_CLOSE receipt parser refuses but the
runner matched the OPEN-side registry row, the runner now builds a *degraded*
close row from the OPEN payload (``_build_v4_close_fallback_payload``) — close
legs stay unmeasured (Empty ≠ Zero) but the ``status='closed'`` transition lands
and frees the group. ``test_defect1_fixed_*`` pins that mechanism:
open → degraded-close-from-OPEN-payload → reopen-same-pool now SUCCEEDS.

The storage layer was always correct (a landed close frees the group): the
``test_defect1_control_*`` test keeps that invariant pinned, and
``test_defect1_storage_collision_when_close_never_lands`` keeps the negative
control — if NO close row of any kind lands, the index still (correctly) rejects
a same-pool reopen.

Defect 2 — the typed collision must survive the runner's exception handling
---------------------------------------------------------------------------
``RegistryAutoCollisionError`` is intentionally NOT a subclass of
``AccountingPersistenceError`` (``registry_errors.py`` docstring; VIB-4200) so
that the typed programming-bug class is never confused with an infra failure.
Before VIB-5409, ``StrategyRunner._write_ledger_entry``'s outer ``except Exception``
re-wrapped the collision as ``AccountingPersistenceError(write_kind="ledger")``,
laundering the typed signal. **VIB-5409 layer 3** adds an explicit
``except RegistryAutoCollisionError: raise`` arm BEFORE the broad handler, so the
typed class survives. ``test_defect2_typed_collision_survives_runner_handler``
pins that the typed class is no longer laundered into a generic LEDGER error.
"""

from __future__ import annotations

import os
import sqlite3
import tempfile
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio

from almanak.framework.accounting.commit import (
    RegistryRow,
    save_ledger_and_registry,
)
from almanak.framework.accounting.payload_schemas import MATCHING_POLICY_VERSIONS
from almanak.framework.observability.ledger import LedgerEntry
from almanak.framework.primitives.types import AccountingCategory, Primitive
from almanak.framework.runner.strategy_runner import StrategyRunner
from almanak.framework.state.exceptions import (
    AccountingPersistenceError,
    AccountingWriteKind,
)
from almanak.framework.state.registry_errors import RegistryAutoCollisionError
from almanak.framework.state.state_manager import (
    SQLiteConfigLight,
    StateManager,
    StateManagerConfig,
    WarmBackendType,
)

# Real Arbitrum WETH/USDC/3000 pool group from the ticket: the V4 grouping
# key is ``chain:pool_id``. We use the ticket's reported group key.
_POOL_GROUP = "arbitrum:0xc9bc8043294146424a4e4607d8ad837d6a659142822bbaaabc83bb57e7447461"
_POOL_ID = _POOL_GROUP.split(":", 1)[1]
_PM = "0xd88f38f930b7952f2db2432cb002e7abbf3dd869"  # V4 PositionManager (arbitrum)


@pytest_asyncio.fixture
async def temp_db_path():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield os.path.join(tmpdir, "vib5360.db")


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


def _ledger(*, id_: str, intent_type: str, tx_hash: str) -> LedgerEntry:
    return LedgerEntry(
        id=id_,
        cycle_id="cycle-1",
        deployment_id="deployment:41c82149b490",
        execution_mode="live",
        timestamp=datetime(2026, 6, 22, 1, 45, 0, tzinfo=UTC),
        intent_type=intent_type,
        token_in="USDC",
        amount_in="2.6",
        token_out="WETH",
        amount_out="0.001",
        effective_price="2500",
        slippage_bps=10.0,
        gas_used=200000,
        gas_usd="0.10",
        tx_hash=tx_hash,
        chain="arbitrum",
        protocol="uniswap_v4",
        success=True,
        error="",
    )


def _v4_row(
    *,
    token_id: int,
    status: str,
    opened_tx: str,
    closed_tx: str | None = None,
    payload: dict | None = None,
) -> RegistryRow:
    """A V4 LP registry row keyed by NFT token id.

    V4 primitive is ``lp_v4`` but accounting_category is ``lp`` — exactly the
    ticket's ``accounting_category='lp'``. The semantic group is the pool id.
    """
    return RegistryRow(
        deployment_id="deployment:41c82149b490",
        chain="arbitrum",
        primitive=Primitive.LP_V4,
        accounting_category=AccountingCategory.LP,
        physical_identity_hash=f"v4:arbitrum:pm:{token_id}",
        semantic_grouping_key=_POOL_GROUP,
        grouping_policy_version="univ4_lp@v1",
        handle=None,
        status=status,  # type: ignore[arg-type]
        payload=payload or {"token_id": token_id, "pool_id": _POOL_ID, "position_manager": _PM},
        opened_at_block=1000,
        opened_tx=opened_tx,
        closed_at_block=2000 if status == "closed" else None,
        closed_tx=closed_tx,
        last_reconciled_at_block=None,
        matching_policy_version=MATCHING_POLICY_VERSIONS[Primitive.LP_V4],
    )


# =============================================================================
# Defect 1 (VIB-5409 fix) — the degraded close from the OPEN payload frees the
# group, so the same-pool reopen now succeeds.
# =============================================================================


def test_defect1_fallback_payload_built_from_open_when_parser_refuses():
    """``_build_v4_close_fallback_payload`` recovers the close identity from the
    matched OPEN payload (token_id + pool_id + PositionManager) so a
    parser-refusing V4 close can still land a ``status='closed'`` row.

    Empty ≠ Zero: the close-leg amounts the burn receipt could not observe are
    ABSENT (unmeasured), never coerced to zero.
    """
    open_payload = {
        "token_id": 183495,
        "pool_id": _POOL_ID,
        "position_manager": _PM,
        "tick_lower": -1000,
        "tick_upper": 1000,
        "amount0": "100",
        "amount1": "0.04",
        "liquidity": "123456",
        "fee_tier": 3000,
    }
    payload = StrategyRunner._build_v4_close_fallback_payload(
        open_payload=open_payload,
        token_id=183495,
        position_manager=_PM,
        fee_tier=3000,
    )
    assert payload is not None
    # Identity anchors recovered.
    assert payload["token_id"] == "183495"
    assert payload["pool_id"] == _POOL_ID
    assert payload["position_manager"] == _PM
    # OPEN-time anchors carried forward.
    assert payload["tick_lower"] == -1000
    assert payload["tick_upper"] == 1000
    assert payload["liquidity"] == "123456"
    assert payload["amount0_open"] == "100"
    assert payload["fee_tier"] == 3000
    # Close legs are UNMEASURED, not zero (Empty ≠ Zero).
    assert "amount0_close" not in payload
    assert "amount1_close" not in payload
    assert "fee_owed_0" not in payload


@pytest.mark.parametrize(
    "open_payload, token_id",
    [
        (None, 183495),  # no matched OPEN row → no identity to recover
        ({"token_id": 183495, "position_manager": _PM}, 183495),  # missing pool_id
        ({"token_id": 183495, "pool_id": _POOL_ID}, 183495),  # missing PositionManager
        ({"token_id": 183495, "pool_id": _POOL_ID, "position_manager": _PM}, 0),  # bad token_id
    ],
)
def test_defect1_fallback_payload_none_without_identity(open_payload, token_id):
    """Without the full identity (pool_id + PositionManager + a usable token_id)
    the fallback returns ``None`` — degrade, never fabricate an identity."""
    assert (
        StrategyRunner._build_v4_close_fallback_payload(
            open_payload=open_payload,
            token_id=token_id,
            position_manager=_PM,
            fee_tier=None,
        )
        is None
    )


@pytest.mark.parametrize(
    "open_payload, close_token_id, close_position_manager",
    [
        # OPEN row's own token_id disagrees with the close being written →
        # building from it would free the WRONG registry group.
        (
            {"token_id": 999999, "pool_id": _POOL_ID, "position_manager": _PM},
            183495,
            _PM,
        ),
        # OPEN row's PositionManager disagrees with the close-side PM the runner
        # resolved → the OPEN row is a different position on a different manager.
        (
            {"token_id": 183495, "pool_id": _POOL_ID, "position_manager": _PM},
            183495,
            "0x000000000000000000000000000000000000dead",
        ),
        # OPEN row carries no parsable token_id at all → unsafe to attribute.
        (
            {"token_id": "not-an-int", "pool_id": _POOL_ID, "position_manager": _PM},
            183495,
            _PM,
        ),
    ],
)
def test_defect1_fallback_payload_none_on_identity_mismatch(open_payload, close_token_id, close_position_manager):
    """Identity guard (VIB-5409 / CodeRabbit): when the matched OPEN row's own
    ``token_id`` or ``position_manager`` does NOT match the close being written,
    the helper returns ``None`` rather than flip-closing (and freeing) the WRONG
    registry group. A lookup regression or direct helper misuse must degrade to
    ``save_ledger_entry``, never misattribute an identity."""
    assert (
        StrategyRunner._build_v4_close_fallback_payload(
            open_payload=open_payload,
            token_id=close_token_id,
            position_manager=close_position_manager,
            fee_tier=None,
        )
        is None
    )


def test_defect1_fallback_payload_pm_match_is_case_insensitive():
    """The matched-identity path is preserved: a PositionManager that differs
    only in checksum casing still matches (addresses are case-insensitive), so a
    legitimate close is not spuriously degraded by the new guard."""
    open_payload = {
        "token_id": 183495,
        "pool_id": _POOL_ID,
        "position_manager": _PM.upper(),
    }
    payload = StrategyRunner._build_v4_close_fallback_payload(
        open_payload=open_payload,
        token_id=183495,
        position_manager=_PM.lower(),
        fee_tier=None,
    )
    assert payload is not None
    assert payload["token_id"] == "183495"
    assert payload["pool_id"] == _POOL_ID


@pytest.mark.asyncio
async def test_defect1_fixed_degraded_close_frees_group_reopen_succeeds(state_manager, temp_db_path):
    """End-to-end at the storage layer: the VIB-5409 degraded close (built from
    the OPEN payload when the parser refused) lands as ``status='closed'`` for
    183495's ``physical_identity_hash``, frees the group, and the same-pool
    reopen of 183524 then SUCCEEDS — the VIB-5360 collision no longer occurs.
    """
    # 1) Open V4 position 183495.
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="open-183495", intent_type="LP_OPEN", tx_hash="0xopenA"),
        registry=_v4_row(token_id=183495, status="open", opened_tx="0xopenA"),
        mode="registry",
    )

    # 2) Rebalance closes 183495. The receipt parser refused, but the runner
    #    matched the OPEN row and built a degraded close payload from it
    #    (VIB-5409). The resulting close row keys on the SAME pih as the OPEN row.
    fallback_payload = StrategyRunner._build_v4_close_fallback_payload(
        open_payload={"token_id": 183495, "pool_id": _POOL_ID, "position_manager": _PM},
        token_id=183495,
        position_manager=_PM,
        fee_tier=None,
    )
    assert fallback_payload is not None
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="close-183495", intent_type="LP_CLOSE", tx_hash="0xcloseA"),
        registry=_v4_row(
            token_id=183495,
            status="closed",
            opened_tx="0xopenA",
            closed_tx="0xcloseA",
            payload=fallback_payload,
        ),
        mode="registry",
    )

    # 3) Reopen a NEW NFT (183524) in the SAME pool. Auto-mode, no handle.
    #    With the group freed by the degraded close, this no longer collides.
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="open-183524", intent_type="LP_OPEN", tx_hash="0xopenB"),
        registry=_v4_row(token_id=183524, status="open", opened_tx="0xopenB"),
        mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        cur = conn.execute(
            "SELECT physical_identity_hash, status FROM position_registry ORDER BY physical_identity_hash",
        )
        assert cur.fetchall() == [
            ("v4:arbitrum:pm:183495", "closed"),
            ("v4:arbitrum:pm:183524", "open"),
        ]
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_defect1_control_landed_close_frees_group_reopen_succeeds(state_manager, temp_db_path):
    """Control: when the CLOSE registry row lands the normal (parser-built) way,
    the same-pool reopen succeeds. Proves the storage layer is correct — the
    VIB-5360 bug was upstream (the V4 CLOSE row not landing), not in the
    partial-index group-freeing logic.
    """
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="open-183495", intent_type="LP_OPEN", tx_hash="0xopenA"),
        registry=_v4_row(token_id=183495, status="open", opened_tx="0xopenA"),
        mode="registry",
    )
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="close-183495", intent_type="LP_CLOSE", tx_hash="0xcloseA"),
        registry=_v4_row(token_id=183495, status="closed", opened_tx="0xopenA", closed_tx="0xcloseA"),
        mode="registry",
    )
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="open-183524", intent_type="LP_OPEN", tx_hash="0xopenB"),
        registry=_v4_row(token_id=183524, status="open", opened_tx="0xopenB"),
        mode="registry",
    )

    conn = sqlite3.connect(temp_db_path)
    try:
        cur = conn.execute(
            "SELECT physical_identity_hash, status FROM position_registry ORDER BY physical_identity_hash",
        )
        assert cur.fetchall() == [
            ("v4:arbitrum:pm:183495", "closed"),
            ("v4:arbitrum:pm:183524", "open"),
        ]
    finally:
        conn.close()


@pytest.mark.asyncio
async def test_defect1_storage_collision_when_close_never_lands(state_manager, temp_db_path):
    """Negative control: if NO close row of any kind lands (the OLD row stays
    ``status='open'``), the auto-mode index still — correctly — rejects a
    same-pool reopen with ``RegistryAutoCollisionError``.

    This pins that the VIB-5409 fix works by *landing the close* (freeing the
    group), NOT by relaxing the index. The index must keep guarding against a
    genuine un-closed same-group double-open.
    """
    await save_ledger_and_registry(
        state_manager,
        ledger=_ledger(id_="open-183495", intent_type="LP_OPEN", tx_hash="0xopenA"),
        registry=_v4_row(token_id=183495, status="open", opened_tx="0xopenA"),
        mode="registry",
    )
    with pytest.raises(RegistryAutoCollisionError) as excinfo:
        await save_ledger_and_registry(
            state_manager,
            ledger=_ledger(id_="open-183524", intent_type="LP_OPEN", tx_hash="0xopenB"),
            registry=_v4_row(token_id=183524, status="open", opened_tx="0xopenB"),
            mode="registry",
        )
    err = excinfo.value
    assert err.accounting_category == "lp"
    assert err.semantic_grouping_key == _POOL_GROUP
    assert err.existing_physical_identity_hash == "v4:arbitrum:pm:183495"

    conn = sqlite3.connect(temp_db_path)
    try:
        cur = conn.execute(
            "SELECT physical_identity_hash, status FROM position_registry ORDER BY physical_identity_hash",
        )
        assert cur.fetchall() == [("v4:arbitrum:pm:183495", "open")]
    finally:
        conn.close()


# =============================================================================
# Defect 2 (VIB-5409 layer 3) — the typed collision survives the runner handler
# =============================================================================


def test_defect2_typed_collision_class_is_distinct_from_accounting_error():
    """Pre-condition for the layer-3 fix: ``RegistryAutoCollisionError`` is NOT a
    subclass of ``AccountingPersistenceError`` (``registry_errors.py``; VIB-4200).
    If this ever regresses, the runner's ``except AccountingPersistenceError`` arm
    would swallow the collision before the typed arm could re-raise it."""
    assert not issubclass(RegistryAutoCollisionError, AccountingPersistenceError)


@pytest.mark.asyncio
@pytest.mark.parametrize("live_mode", [True, False])
async def test_defect2_typed_collision_survives_runner_handler(monkeypatch, live_mode):
    """VIB-5409 layer 3 — drive the REAL ``StrategyRunner._write_ledger_entry``.

    A registry auto-mode collision surfaces from ``_maybe_save_ledger_with_registry``
    inside the ``_write_ledger_entry`` try-body. The fix adds an explicit
    ``except RegistryAutoCollisionError: raise`` arm BEFORE the broad
    ``except Exception`` re-wrap, so the typed class propagates verbatim instead
    of being laundered into a generic ``AccountingPersistenceError(write_kind=
    'ledger')`` (VIB-5360 defect 2). Earlier this test re-created the handler
    order locally, so a regression in the real method's arm ORDER would not have
    been caught — now we exercise the production path and assert on the surfaced
    type in BOTH live and non-live mode (the collision must surface uniformly).
    """
    assert not issubclass(RegistryAutoCollisionError, AccountingPersistenceError)

    collision = RegistryAutoCollisionError(
        semantic_grouping_key=_POOL_GROUP,
        existing_physical_identity_hash="v4:arbitrum:pm:183495",
        opened_tx="0xopenA",
        accounting_category="lp",
    )

    # ``_write_ledger_entry`` does ``from ..observability.ledger import
    # build_ledger_entry`` at call time — patch it there so the heavy ledger
    # build is a no-op and the try-body reaches the registry dispatch.
    import almanak.framework.observability.ledger as ledger_mod

    fake_entry = SimpleNamespace(id="entry-1", tx_hash="0xclose", execution_mode=None)
    monkeypatch.setattr(ledger_mod, "build_ledger_entry", lambda **kwargs: fake_entry)

    # Bind the REAL unbound method onto a runner-shaped namespace (same pattern as
    # test_v4_registry_dispatch.py) and stub only the collaborators the try-body
    # touches before the registry dispatch raises.
    runner = SimpleNamespace()
    runner.config = SimpleNamespace(chain="arbitrum")
    runner.state_manager = SimpleNamespace(save_ledger_entry=AsyncMock())
    runner._maybe_enrich_result_with_runner_hooks = MagicMock()
    runner._derive_execution_mode = MagicMock(return_value="live" if live_mode else "paper")
    runner._is_live_mode = MagicMock(return_value=live_mode)
    # The collision originates here (the atomic ledger+registry primitive).
    runner._maybe_save_ledger_with_registry = AsyncMock(side_effect=collision)

    with pytest.raises(RegistryAutoCollisionError) as excinfo:
        await StrategyRunner._write_ledger_entry(
            runner,
            strategy=SimpleNamespace(deployment_id="deployment:41c82149b490", chain="arbitrum"),
            intent=SimpleNamespace(),
            result=SimpleNamespace(success=True),
            success=True,
        )

    surfaced = excinfo.value
    # The typed signal survives the real handler — NOT laundered into a generic
    # LEDGER-shaped AccountingPersistenceError.
    assert isinstance(surfaced, RegistryAutoCollisionError)
    assert not isinstance(surfaced, AccountingPersistenceError)
    assert surfaced.accounting_category == "lp"
    assert surfaced.semantic_grouping_key == _POOL_GROUP
    assert getattr(surfaced, "write_kind", None) != AccountingWriteKind.LEDGER.value
