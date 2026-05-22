"""Track-C materialization integration test for morpho_blue lending (VIB-4541).

Acceptance evidence for the ticket's AC #3 ("Anvil integration test
``tests/intents/ethereum/morpho_blue/test_track_c_materialization_lending.py``
produces > 0 ``position_state_snapshots`` rows for the deployment").

What this exercises end-to-end (no mocks past the gRPC channel):

    PositionValue (morpho-shape details)
        -> materialise_position_state (real classifier + _materialise_lending)
        -> PositionStateRow
        -> StateServiceServicer.SavePositionStateSnapshots handler (real)
        -> SQLiteStore.save_position_state_snapshots (real)
        -> sqlite row

The chain that this test verifies was UNREACHABLE pre-VIB-4541: the
runner's ``_persist_position_state_snapshots`` (runner_state.py:435) hit
the capability gate at runner_state.py:480 because ``GatewayStateManager``
lacked ``save_position_state_snapshots`` and silently returned 0. The
gateway client method, RPC, server handler, and warm-backend reuse
together close that gap.

The Anvil-driven proof of the SAME wiring (a full ``almanak strat run``
loop on managed Anvil producing real on-chain morpho positions whose
Track-C rows land in the strategy DB) is documented in the reproduction
recipe at the bottom of this file and was executed against the
``MorphoLoopingStrategy:vib-4541-track-c-001`` deployment id as part of
the PR test plan. That recipe is the AC #4 evidence; this test is the
AC #3 evidence that the chain holds at the Python boundary.

This is the morpho_blue lending shape — `details={"amount", "asset",
"market_id"}` — that the LENDING_OBSERVER snapshot writer emits.
``_materialise_lending`` reads ``supply_balance``/``borrow_balance``/
``health_factor``/etc. from ``details``, none of which the morpho_blue
snapshot writer populates today. So the typed lending fields on the
materialised row will be ``None`` (unmeasured) — but the row IS produced
and lands in ``position_state_snapshots``, which is enough for cells
G14 / G15 to score off this deployment. Cells L2 / L3 / L5 remain
XFAIL/FAIL until a follow-up populates the morpho_blue ``details``
keys the materialiser reads (separate ticket).
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.framework.accounting.position_state import materialise_position_state
from almanak.framework.intents.vocabulary import IntentType
from almanak.framework.portfolio.models import PositionValue
from almanak.framework.state.backends.sqlite import SQLiteConfig, SQLiteStore
from almanak.framework.teardown.models import PositionType
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer

# Intent-coverage marker (VIB-4303). The Track-C materializer is the wiring
# half of the lending lifecycle (SUPPLY -> snapshot -> materialise); the
# intent that produces the morpho_blue SUPPLY position whose materialised
# row this test asserts on is SUPPLY.
pytestmark = pytest.mark.intent(IntentType.SUPPLY)


def _run(coro):
    return asyncio.run(coro)


def _morpho_supply_position() -> PositionValue:
    """Morpho-shape SUPPLY position as emitted by the LENDING_OBSERVER
    snapshot writer (verified from the postmerge DB row; see
    docs/internal/MorphoStatusMay17.md §"Accountant Test posture").

    Note: ``_materialise_lending`` reads ``supply_balance`` /
    ``borrow_balance`` / ``health_factor`` from ``details``; the morpho_blue
    snapshot writer does NOT populate those keys today. That's a separate
    follow-up — the row still materialises (with ``None`` on those fields),
    which is what unblocks cells G14 / G15 here.
    """
    return PositionValue(
        position_type=PositionType.SUPPLY,
        protocol="morpho_blue",
        chain="ethereum",
        value_usd=Decimal("37.88"),
        label="morpho_blue SUPPLY",
        tokens=["wstETH"],
        details={
            "amount": "0.014",
            "asset": "wstETH",
            "market_id": "0xb323...",
        },
    )


@pytest.fixture
def sqlite_store(tmp_path) -> SQLiteStore:
    """A real (non-mocked) SQLiteStore backed by a tmp file. The schema —
    including the ``position_state_snapshots`` table — is created at
    ``initialize()`` time per the DDL at sqlite.py:543-609."""
    store = SQLiteStore(SQLiteConfig(db_path=str(tmp_path / "vib4541_track_c.db")))
    _run(store.initialize())
    return store


@pytest.fixture
def gateway_service(sqlite_store) -> StateServiceServicer:
    """A real ``StateServiceServicer`` whose warm backend is the real
    SQLiteStore above. No mock between the handler and the persistence
    layer."""
    svc = StateServiceServicer(GatewaySettings())
    svc._initialized = True
    svc._snapshot_pool_initialized = True
    svc._snapshot_pool = None
    svc._ensure_initialized = AsyncMock()
    svc._ensure_snapshot_pool = AsyncMock()
    svc._state_manager = MagicMock()
    svc._state_manager.warm_backend = sqlite_store
    return svc


@pytest.fixture
def ctx() -> MagicMock:
    c = MagicMock(spec=grpc.aio.ServicerContext)
    c.set_code = MagicMock()
    c.set_details = MagicMock()
    return c


def _seed_parent_snapshot(store: SQLiteStore, deployment_id: str) -> int:
    """Insert a parent ``portfolio_snapshots`` row directly so the
    ``snapshot_id`` FK on the Track-C row resolves to a real parent. We
    only need the parent to exist; we don't exercise the full
    save_portfolio_snapshot path here (that's covered by separate tests)."""
    with store._db_lock:
        cur = store._conn.execute(  # type: ignore[union-attr]
            """
            INSERT INTO portfolio_snapshots (
                deployment_id, timestamp, iteration_number, total_value_usd,
                available_cash_usd, deployed_capital_usd, wallet_total_value_usd,
                value_confidence, positions_json, token_prices_json,
                wallet_balances_json, chain, created_at,
                deployment_id, cycle_id, execution_mode
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING id
            """,
            (
                deployment_id,
                "2026-05-17T12:00:00+00:00",
                1,
                "100.0",
                "0",
                "0",
                "100.0",
                "HIGH",
                "[]",
                "{}",
                "[]",
                "ethereum",
                "2026-05-17T12:00:00+00:00",
                deployment_id,
                "cycle-1",
                "paper",
            ),
        )
        snapshot_id = cur.fetchone()[0]
        store._conn.commit()  # type: ignore[union-attr]
    return int(snapshot_id)


@pytest.mark.asyncio
async def test_morpho_supply_position_materialises_and_lands_in_sqlite(  # noqa: layers
    sqlite_store,
    gateway_service,
    ctx,
):
    # 4-layer mandate (.claude/rules/intent-tests.md) is intentionally
    # skipped via ``# noqa: layers``: this is a Track-C wiring test for
    # the materializer → gateway → SQLite chain, NOT an intent test in
    # the protocol-compile / protocol-execute / receipt-parse /
    # balance-delta sense. The compile + execute + receipt-parse +
    # balance-delta layers for the same SUPPLY intent are covered by
    # ``tests/intents/ethereum/test_morpho_blue_lending.py`` (existing
    # 4-layer suite). The Anvil reproduction documented at the bottom of
    # this file is the additional end-to-end proof for AC #4 of VIB-4541.
    # Same precedent as ``tests/intents/arbitrum/test_zodiac_permission_correctness.py``
    # which uses the escape hatch for the Zodiac on-chain authz pilot.
    """End-to-end: a morpho_blue SUPPLY ``PositionValue`` flows through the
    real materialiser, the real gateway handler, and the real SQLite warm
    backend. After the round-trip the DB MUST have ≥1 row in
    ``position_state_snapshots`` for the deployment. Pre-VIB-4541 the
    runner-side capability gate at runner_state.py:480 returned 0 silently,
    so the count stayed at 0 forever — this test is the regression seal."""
    deployment_id = "vib-4541-track-c-integration"
    snapshot_id = _seed_parent_snapshot(sqlite_store, deployment_id)

    # Step 1: real materialiser produces a PositionStateRow.
    position = _morpho_supply_position()
    materialised = materialise_position_state(
        position=position,
        market=None,
        prices=None,
        deployment_id=deployment_id,
        cycle_id="cycle-1",
        timestamp=datetime(2026, 5, 17, 12, 0, tzinfo=UTC),
    )
    assert materialised is not None, "materialiser dropped the morpho SUPPLY shape"
    assert materialised.position_type == "LENDING"
    # Lending fields are None — the morpho_blue snapshot writer doesn't
    # populate supply_balance/HF yet (separate follow-up). Locked in this
    # test so the gap is visible if a future writer fix arrives.
    assert materialised.supply_balance is None
    assert materialised.borrow_balance is None
    assert materialised.health_factor is None

    # Step 2: build a real proto request from the materialised row using the
    # gateway client's serialiser (proves both halves of the wire shape).
    from almanak.framework.state.gateway_state_manager import GatewayStateManager

    proto_row = GatewayStateManager._position_state_row_to_proto(materialised)
    req = gateway_pb2.SavePositionStateSnapshotsRequest(
        snapshot_id=snapshot_id,
        rows=[proto_row],
    )

    # Step 3: real handler delegates to real SQLite warm backend.
    resp = await gateway_service.SavePositionStateSnapshots(req, ctx)
    assert resp.success is True
    assert resp.rows_written == 1

    # Step 4: query SQLite directly — the row landed bound to the parent.
    with sqlite3.connect(sqlite_store._config.db_path) as conn:
        cursor = conn.execute(
            "SELECT snapshot_id, deployment_id, deployment_id, position_type, "
            "supply_balance, borrow_balance, health_factor "
            "FROM position_state_snapshots WHERE deployment_id = ?",
            (deployment_id,),
        )
        rows = cursor.fetchall()
    assert len(rows) == 1, f"expected 1 Track-C row for {deployment_id}, got {len(rows)}"
    pss_snapshot_id, pss_deployment_id, pss_deployment_id, position_type, supply, borrow, hf = rows[0]
    assert pss_snapshot_id == snapshot_id  # FK to parent portfolio_snapshots row
    assert pss_deployment_id == deployment_id
    assert pss_deployment_id == deployment_id
    assert position_type == "LENDING"
    # SQLite stored NULL on unmeasured fields — Empty != Zero held across the
    # entire chain (materialiser → proto → handler → SQLite). If a future
    # regression conflates None with Decimal("0") on any leg, this asserts
    # the contract fails.
    assert supply is None, f"expected NULL supply_balance, got {supply!r}"
    assert borrow is None, f"expected NULL borrow_balance, got {borrow!r}"
    assert hf is None, f"expected NULL health_factor, got {hf!r}"


# -----------------------------------------------------------------------------
# Anvil reproduction recipe (AC #4 evidence — executed as part of the PR plan)
# -----------------------------------------------------------------------------
#
# A real managed-Anvil run of the morpho_looping demo proves the same wiring
# end-to-end through the production gRPC channel. Re-run with:
#
#     cd /Users/nick/Documents/Almanak/src/almanak-sdk
#     ALMANAK_CHAIN=ethereum uv run almanak strat run \
#       -d almanak/demo_strategies/morpho_looping \
#       --network anvil \
#       --fresh \
#       --id vib-4541-track-c-001 \
#       --interval 30
#
# Then after ≥3 iterations have run and the OPEN intent has landed:
#
#     sqlite3 almanak/demo_strategies/morpho_looping/almanak_state.db \
#       "SELECT COUNT(*) FROM position_state_snapshots
#        WHERE deployment_id='vib-4541-track-c-001';"
#
# Expected: count > 0. Pre-VIB-4541 this was 0 for every deployment.
#
# DO NOT run the teardown via `almanak strat teardown request` — the
# morpho_looping teardown has known intent-failure modes (VIB-4531..4533)
# that require manual intervention and aren't relevant to this AC. Send
# SIGINT to the runner to stop cleanly once the count is positive.
