"""VIB-4493 Phase 1B — tests for GetPositions + GetPositionRangeHistory.

Kept in a sibling file rather than appended to test_dashboard_service.py
(2006 lines already) so the Phase 1 work has a contained, reviewable surface.

Convention matches test_dashboard_service.py:
  - @pytest.mark.asyncio
  - mock_context (MagicMock spec=ServicerContext)
  - dashboard_service fixture (DashboardServiceServicer instance)
  - manual `_initialized=True`
  - mocked state_manager

Coverage per RPC:
  - invalid_strategy_id → INVALID_ARGUMENT
  - no state_manager   → degraded empty response (not an error)
  - empty backend      → empty rows, cutover_states populated with
                         PRE_BACKFILL (no migration_state row)
  - happy path         → registry rows + cutover derivation
  - include_legacy_unverified → events surface in `unverified` lane
  - cutover state machine derivation (state-table coverage)
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.framework.migration.backfill import MigrationStateRow
from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services._dashboard_phase1 import (
    CutoverDerivation,
    build_cutover_state_entry,
    cutover_lookup_key,
    derive_confidence,
    derive_cutover_state,
)
from almanak.gateway.services.dashboard_service import DashboardServiceServicer


@pytest.fixture
def settings() -> GatewaySettings:
    return GatewaySettings()


@pytest.fixture
def mock_context() -> MagicMock:
    return MagicMock(spec=grpc.aio.ServicerContext)


@pytest.fixture
def dashboard_service(settings: GatewaySettings) -> DashboardServiceServicer:
    return DashboardServiceServicer(settings)


def _registry_row(
    *,
    handle: str = "1234",
    physical_identity_hash: str = "deadbeef",
    deployment_id: str = "test_strategy",
    chain: str = "base",
    primitive: str = "lp",
    accounting_category: str = "LP_UNIV3",
    status: str = "open",
    opened_at_block: int = 1000,
    opened_tx: str = "0xabc",
    last_reconciled_at_block: int = 1010,
) -> dict:
    """Build a registry-row dict matching the gateway_state_manager shape."""
    return {
        "deployment_id": deployment_id,
        "chain": chain,
        "primitive": primitive,
        "accounting_category": accounting_category,
        "physical_identity_hash": physical_identity_hash,
        "semantic_grouping_key": "test:grouping",
        "grouping_policy_version": 1,
        "handle": handle,
        "status": status,
        "payload": {"tick_lower": -887220, "tick_upper": 887220},
        "opened_at_block": opened_at_block,
        "opened_tx": opened_tx,
        "closed_at_block": None,
        "closed_tx": None,
        "last_reconciled_at_block": last_reconciled_at_block,
        "matching_policy_version": 1,
    }


def _migration_state_row(
    *,
    deployment_id: str = "test_strategy",
    primitive: str = "lp",
    cutover_key: str = "lp",
    complete: bool = True,
    started_at: str | None = "2026-05-01T00:00:00+00:00",
    completed_at: str | None = "2026-05-01T00:01:00+00:00",
    rows_synthesized: int = 5,
    rows_skipped: int = 0,
) -> MigrationStateRow:
    return MigrationStateRow(
        deployment_id=deployment_id,
        primitive=primitive,
        cutover_key=cutover_key,
        position_registry_backfill_complete=complete,
        backfill_started_at=started_at,
        backfill_completed_at=completed_at if complete else None,
        backfill_source_table="position_events",
        backfill_reader_version=1,
        rows_synthesized=rows_synthesized,
        rows_skipped_already_present=rows_skipped,
        notes={},
        created_at="2026-05-01T00:00:00+00:00",
        updated_at="2026-05-01T00:01:00+00:00",
    )


# =============================================================================
# CutoverState derivation — covers the v5 state-machine table directly.
# =============================================================================


class TestDeriveCutoverState:
    """Unit tests for the pure cutover-state derivation."""

    def test_no_migration_state_row_returns_pre_backfill(self) -> None:
        state = derive_cutover_state(
            None,
            last_reconciled_unix_seconds=0,
            now_unix_seconds=1_700_000_000,
        )
        assert state == gateway_pb2.CUTOVER_STATE_PRE_BACKFILL

    def test_incomplete_no_started_at_returns_pre_backfill(self) -> None:
        row = _migration_state_row(complete=False, started_at=None)
        state = derive_cutover_state(
            row,
            last_reconciled_unix_seconds=0,
            now_unix_seconds=1_700_000_000,
        )
        assert state == gateway_pb2.CUTOVER_STATE_PRE_BACKFILL

    def test_incomplete_with_started_at_returns_in_progress(self) -> None:
        row = _migration_state_row(complete=False, started_at="2026-05-01T00:00:00+00:00")
        state = derive_cutover_state(
            row,
            last_reconciled_unix_seconds=0,
            now_unix_seconds=1_700_000_000,
        )
        assert state == gateway_pb2.CUTOVER_STATE_BACKFILL_IN_PROGRESS

    def test_complete_no_reconcile_returns_complete(self) -> None:
        row = _migration_state_row(complete=True)
        state = derive_cutover_state(
            row,
            last_reconciled_unix_seconds=0,  # never reconciled
            now_unix_seconds=1_700_000_000,
        )
        assert state == gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE

    def test_complete_fresh_reconcile_returns_complete(self) -> None:
        row = _migration_state_row(complete=True)
        state = derive_cutover_state(
            row,
            last_reconciled_unix_seconds=1_700_000_000 - 100,  # 100s ago
            now_unix_seconds=1_700_000_000,
        )
        assert state == gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE

    def test_complete_stale_reconcile_still_returns_complete(self) -> None:
        """Stale reconciliation does NOT regress cutover state.

        v5 design: freshness pill carries the staleness signal; the cutover
        state machine stays at BACKFILL_COMPLETE because the writer-side
        gate already passed. Regressing to IN_PROGRESS would force operators
        to re-run backfill unnecessarily.
        """
        row = _migration_state_row(complete=True)
        state = derive_cutover_state(
            row,
            last_reconciled_unix_seconds=1_700_000_000 - 90_000,  # 25h ago
            now_unix_seconds=1_700_000_000,
            fresh_threshold_seconds=86400,
        )
        assert state == gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE


class TestCutoverLookupKey:
    def test_uniV3_maps_to_lp(self) -> None:
        assert cutover_lookup_key("LP_UNIV3") == ("lp", "lp")

    def test_pendle_lp_maps_to_lp_today(self) -> None:
        # v1 reality: cutover_key='lp' shared across LP families until
        # T16/T23/T28 emit typed cutover_keys.
        assert cutover_lookup_key("LP_PENDLE") == ("lp", "lp")

    def test_unknown_category_falls_through(self) -> None:
        # AAVE_COLLATERAL is not in the v1 map → primitive='aave_collateral',
        # cutover_key='AAVE_COLLATERAL'. Future ticket adds typed mapping.
        primitive, cutover_key = cutover_lookup_key("AAVE_COLLATERAL")
        assert primitive == "aave_collateral"
        assert cutover_key == "AAVE_COLLATERAL"

    def test_empty_category_defaults_to_lp(self) -> None:
        assert cutover_lookup_key("") == ("lp", "lp")


class TestDeriveConfidence:
    def test_three_sources_agree_returns_high(self) -> None:
        assert (
            derive_confidence(registry_present=True, snapshot_present=True, opened_tx_present=True)
            == gateway_pb2.POSITION_CONFIDENCE_HIGH
        )

    def test_two_of_three_returns_medium(self) -> None:
        assert (
            derive_confidence(registry_present=True, snapshot_present=True, opened_tx_present=False)
            == gateway_pb2.POSITION_CONFIDENCE_MEDIUM
        )

    def test_one_of_three_returns_low(self) -> None:
        assert (
            derive_confidence(registry_present=True, snapshot_present=False, opened_tx_present=False)
            == gateway_pb2.POSITION_CONFIDENCE_LOW
        )

    def test_zero_returns_unspecified(self) -> None:
        assert (
            derive_confidence(registry_present=False, snapshot_present=False, opened_tx_present=False)
            == gateway_pb2.POSITION_CONFIDENCE_UNSPECIFIED
        )


class TestBuildCutoverStateEntry:
    def test_no_migration_state_row_returns_zeros(self) -> None:
        derivation = CutoverDerivation(
            state=gateway_pb2.CUTOVER_STATE_PRE_BACKFILL,
            migration_state_row=None,
            last_reconciled_at_block=0,
            last_reconciled_unix_seconds=0,
        )
        entry = build_cutover_state_entry(accounting_category="LP_UNIV3", derivation=derivation)
        assert entry.accounting_category == "LP_UNIV3"
        assert entry.state == gateway_pb2.CUTOVER_STATE_PRE_BACKFILL
        assert entry.rows_synthesized == 0
        assert entry.backfill_started_at == ""

    def test_complete_row_surfaces_counters(self) -> None:
        ms_row = _migration_state_row(complete=True, rows_synthesized=42, rows_skipped=7)
        derivation = CutoverDerivation(
            state=gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE,
            migration_state_row=ms_row,
            last_reconciled_at_block=5_000,
            last_reconciled_unix_seconds=1_700_000_000,
        )
        entry = build_cutover_state_entry(accounting_category="LP_UNIV3", derivation=derivation)
        assert entry.rows_synthesized == 42
        assert entry.rows_skipped_already_present == 7
        assert entry.backfill_started_at == "2026-05-01T00:00:00+00:00"
        assert entry.backfill_completed_at == "2026-05-01T00:01:00+00:00"
        assert entry.last_reconciled_at_block == 5_000
        assert entry.last_reconciled_unix_seconds == 1_700_000_000


# =============================================================================
# GetPositions handler tests
# =============================================================================


@pytest.mark.asyncio
class TestGetPositions:
    async def test_invalid_strategy_id_returns_invalid_argument(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True
        request = gateway_pb2.GetPositionsRequest(strategy_id="")
        response = await dashboard_service.GetPositions(request, mock_context)
        assert isinstance(response, gateway_pb2.GetPositionsResponse)
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    async def test_no_state_manager_returns_empty_response(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True
        dashboard_service._state_manager = None
        request = gateway_pb2.GetPositionsRequest(strategy_id="test_strategy")
        response = await dashboard_service.GetPositions(request, mock_context)
        assert len(response.positions) == 0
        assert len(response.unverified) == 0
        assert len(response.cutover_states) == 0

    async def test_empty_backend_returns_empty_rows(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True
        sm = MagicMock()
        sm.get_position_registry_open_rows = AsyncMock(return_value=[])
        sm.get_latest_snapshot = AsyncMock(return_value=None)
        sm.get_migration_state = AsyncMock(return_value=None)
        sm.get_position_events_filtered = AsyncMock(return_value=[])
        dashboard_service._state_manager = sm

        request = gateway_pb2.GetPositionsRequest(strategy_id="test_strategy")
        response = await dashboard_service.GetPositions(request, mock_context)
        assert len(response.positions) == 0
        assert len(response.unverified) == 0
        sm.get_position_registry_open_rows.assert_awaited_once()

    async def test_registry_rows_surface_with_cutover_states(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True

        registry_rows = [
            _registry_row(handle="1234", accounting_category="LP_UNIV3"),
            _registry_row(handle="5678", physical_identity_hash="cafe", accounting_category="LP_UNIV3"),
        ]
        snapshot = PortfolioSnapshot(
            strategy_id="test_strategy",
            timestamp=datetime(2026, 5, 17, 12, 0, tzinfo=UTC),
            total_value_usd=1000.0,
            available_cash_usd=100.0,
            deployed_capital_usd=900.0,
            wallet_total_value_usd=1000.0,
            value_confidence=ValueConfidence.HIGH,
            positions=[],
            token_prices={},
            wallet_balances=[],
            chain="base",
        )

        sm = MagicMock()
        sm.get_position_registry_open_rows = AsyncMock(return_value=registry_rows)
        sm.get_latest_snapshot = AsyncMock(return_value=snapshot)
        sm.get_migration_state = AsyncMock(return_value=_migration_state_row(complete=True))
        sm.get_position_events_filtered = AsyncMock(return_value=[])
        dashboard_service._state_manager = sm

        request = gateway_pb2.GetPositionsRequest(strategy_id="test_strategy")
        response = await dashboard_service.GetPositions(request, mock_context)

        assert len(response.positions) == 2
        assert all(p.cutover_state == gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE for p in response.positions)
        # One cutover_state entry per unique accounting_category.
        assert len(response.cutover_states) == 1
        assert response.cutover_states[0].accounting_category == "LP_UNIV3"
        assert response.cutover_states[0].rows_synthesized == 5

    async def test_include_legacy_unverified_surfaces_events(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True

        # Registry empty (pre-cutover) but position_events have a row.
        events = [
            {
                "position_id": "evt_pos_1",
                "deployment_id": "test_strategy",
                "chain": "base",
                "position_type": "LP",
                "event_type": "OPEN",
                "tx_hash": "0xevt",
                "timestamp": "2026-05-10T00:00:00+00:00",
                "tick_lower": -887220,
                "tick_upper": 887220,
                "liquidity": "1234567890",
            },
        ]
        sm = MagicMock()
        sm.get_position_registry_open_rows = AsyncMock(return_value=[])
        sm.get_latest_snapshot = AsyncMock(return_value=None)
        sm.get_migration_state = AsyncMock(return_value=_migration_state_row(complete=False, started_at=None))
        sm.get_position_events_filtered = AsyncMock(return_value=events)
        dashboard_service._state_manager = sm

        request = gateway_pb2.GetPositionsRequest(
            strategy_id="test_strategy",
            include_legacy_unverified=True,
        )
        response = await dashboard_service.GetPositions(request, mock_context)

        assert len(response.positions) == 0
        assert len(response.unverified) == 1
        assert response.unverified[0].source == gateway_pb2.POSITION_SOURCE_LEGACY
        assert response.unverified[0].handle == "evt_pos_1"

    # ------------------------------------------------------------------
    # Codex review fix — unverified lane honors request filters
    # ------------------------------------------------------------------

    async def test_unverified_lane_drops_events_on_wrong_chain(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        """A filtered GetPositions request must NOT surface unverified
        rows from other chains (Codex review fix)."""
        dashboard_service._initialized = True
        events = [
            {"position_id": "p_base", "chain": "base", "position_type": "LP", "accounting_category": "LP_UNIV3"},
            {"position_id": "p_eth", "chain": "ethereum", "position_type": "LP", "accounting_category": "LP_UNIV3"},
        ]
        sm = MagicMock()
        sm.get_position_registry_open_rows = AsyncMock(return_value=[])
        sm.get_latest_snapshot = AsyncMock(return_value=None)
        sm.get_migration_state = AsyncMock(return_value=None)
        sm.get_position_events_filtered = AsyncMock(return_value=events)
        dashboard_service._state_manager = sm

        response = await dashboard_service.GetPositions(
            gateway_pb2.GetPositionsRequest(
                strategy_id="test_strategy",
                chain="base",
                include_legacy_unverified=True,
            ),
            mock_context,
        )
        handles = {u.handle for u in response.unverified}
        assert handles == {"p_base"}, "ethereum event must be dropped by chain filter"

    async def test_unverified_lane_drops_events_on_wrong_accounting_category(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True
        events = [
            {"position_id": "p_univ3", "chain": "base", "position_type": "LP", "accounting_category": "LP_UNIV3"},
            {"position_id": "p_aero", "chain": "base", "position_type": "LP", "accounting_category": "LP_AERODROME"},
        ]
        sm = MagicMock()
        sm.get_position_registry_open_rows = AsyncMock(return_value=[])
        sm.get_latest_snapshot = AsyncMock(return_value=None)
        sm.get_migration_state = AsyncMock(return_value=None)
        sm.get_position_events_filtered = AsyncMock(return_value=events)
        dashboard_service._state_manager = sm

        response = await dashboard_service.GetPositions(
            gateway_pb2.GetPositionsRequest(
                strategy_id="test_strategy",
                accounting_category="LP_UNIV3",
                include_legacy_unverified=True,
            ),
            mock_context,
        )
        handles = {u.handle for u in response.unverified}
        assert handles == {"p_univ3"}

    async def test_unverified_lane_lending_filter_returns_empty(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        """Primitive filter for 'lending' yields no rows since the
        evidence-set today only covers LP + PERP (lending events live in
        accounting_events — pending VIB-4501)."""
        dashboard_service._initialized = True
        sm = MagicMock()
        sm.get_position_registry_open_rows = AsyncMock(return_value=[])
        sm.get_latest_snapshot = AsyncMock(return_value=None)
        sm.get_migration_state = AsyncMock(return_value=None)
        # No call expected, but mock for safety
        sm.get_position_events_filtered = AsyncMock(return_value=[])
        dashboard_service._state_manager = sm

        response = await dashboard_service.GetPositions(
            gateway_pb2.GetPositionsRequest(
                strategy_id="test_strategy",
                primitive="lending",
                include_legacy_unverified=True,
            ),
            mock_context,
        )
        assert len(response.unverified) == 0
        # Skipped — we short-circuit before the events fetch.
        sm.get_position_events_filtered.assert_not_awaited()


# =============================================================================
# _build_snapshot_position_index — extracted helper (covered via the public
# `GetPositions` happy paths already, but the error / fallback / dict-coercion
# branches need explicit coverage to satisfy the project's CRAP gate).
# =============================================================================


@pytest.mark.asyncio
class TestBuildSnapshotPositionIndex:
    async def test_get_latest_snapshot_exception_returns_empty(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        sm = MagicMock()
        sm.get_latest_snapshot = AsyncMock(side_effect=RuntimeError("rpc dead"))
        dashboard_service._state_manager = sm
        index, ts = await dashboard_service._build_snapshot_position_index("sid")
        assert index == {}
        assert ts == 0

    async def test_no_latest_snapshot_returns_empty(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        sm = MagicMock()
        sm.get_latest_snapshot = AsyncMock(return_value=None)
        dashboard_service._state_manager = sm
        index, ts = await dashboard_service._build_snapshot_position_index("sid")
        assert index == {}
        assert ts == 0

    async def test_uses_to_dict_when_available(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        class FakePos:
            def to_dict(self) -> dict:
                return {"position_id": "lp-1", "value_usd": "100"}

        snapshot = MagicMock()
        snapshot.timestamp = datetime(2026, 5, 17, 12, 0, tzinfo=UTC)
        snapshot.positions = [FakePos()]
        sm = MagicMock()
        sm.get_latest_snapshot = AsyncMock(return_value=snapshot)
        dashboard_service._state_manager = sm

        index, ts = await dashboard_service._build_snapshot_position_index("sid")
        assert "lp-1" in index
        assert index["lp-1"]["value_usd"] == "100"
        assert ts == int(snapshot.timestamp.timestamp())

    async def test_uses_dict_copy_when_pos_is_mapping(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        snapshot = MagicMock()
        snapshot.timestamp = datetime(2026, 5, 17, 12, 0, tzinfo=UTC)
        snapshot.positions = [{"position_id": "lp-2", "value_usd": "200"}]
        sm = MagicMock()
        sm.get_latest_snapshot = AsyncMock(return_value=snapshot)
        dashboard_service._state_manager = sm

        index, _ = await dashboard_service._build_snapshot_position_index("sid")
        assert index["lp-2"]["value_usd"] == "200"

    async def test_falls_back_to_vars_for_dataclass_like(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        """No `to_dict`, not a dict — fall back to `vars()`."""

        class FakeDataclass:
            def __init__(self) -> None:
                self.position_id = "lp-3"
                self.value_usd = "300"

        snapshot = MagicMock()
        snapshot.timestamp = datetime(2026, 5, 17, 12, 0, tzinfo=UTC)
        snapshot.positions = [FakeDataclass()]
        sm = MagicMock()
        sm.get_latest_snapshot = AsyncMock(return_value=snapshot)
        dashboard_service._state_manager = sm

        index, _ = await dashboard_service._build_snapshot_position_index("sid")
        assert "lp-3" in index
        assert index["lp-3"]["value_usd"] == "300"

    async def test_skips_position_with_no_id(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        snapshot = MagicMock()
        snapshot.timestamp = datetime(2026, 5, 17, 12, 0, tzinfo=UTC)
        snapshot.positions = [{"value_usd": "no-id-here"}]
        sm = MagicMock()
        sm.get_latest_snapshot = AsyncMock(return_value=snapshot)
        dashboard_service._state_manager = sm

        index, _ = await dashboard_service._build_snapshot_position_index("sid")
        assert index == {}

    async def test_handle_or_symbol_used_as_fallback_key(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        snapshot = MagicMock()
        snapshot.timestamp = datetime(2026, 5, 17, 12, 0, tzinfo=UTC)
        snapshot.positions = [
            {"handle": "h-1", "value_usd": "10"},
            {"symbol": "BTC", "value_usd": "20"},
        ]
        sm = MagicMock()
        sm.get_latest_snapshot = AsyncMock(return_value=snapshot)
        dashboard_service._state_manager = sm

        index, _ = await dashboard_service._build_snapshot_position_index("sid")
        assert index["h-1"]["value_usd"] == "10"
        assert index["BTC"]["value_usd"] == "20"

    async def test_snapshot_without_timestamp_returns_zero(
        self,
        dashboard_service: DashboardServiceServicer,
    ) -> None:
        snapshot = MagicMock()
        snapshot.timestamp = None
        snapshot.positions = []
        sm = MagicMock()
        sm.get_latest_snapshot = AsyncMock(return_value=snapshot)
        dashboard_service._state_manager = sm

        _, ts = await dashboard_service._build_snapshot_position_index("sid")
        assert ts == 0


# =============================================================================
# GetPositionRangeHistory handler tests
# =============================================================================


@pytest.mark.asyncio
class TestGetPositionRangeHistory:
    async def test_invalid_strategy_id_returns_invalid_argument(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True
        request = gateway_pb2.GetPositionRangeHistoryRequest(strategy_id="")
        response = await dashboard_service.GetPositionRangeHistory(request, mock_context)
        assert isinstance(response, gateway_pb2.GetPositionRangeHistoryResponse)
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    async def test_missing_chain_returns_invalid_argument(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True
        request = gateway_pb2.GetPositionRangeHistoryRequest(
            strategy_id="test_strategy",
            accounting_category="LP_UNIV3",
            handle="1234",
        )
        await dashboard_service.GetPositionRangeHistory(request, mock_context)
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    async def test_missing_handle_and_hash_returns_invalid_argument(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True
        request = gateway_pb2.GetPositionRangeHistoryRequest(
            strategy_id="test_strategy",
            chain="base",
            accounting_category="LP_UNIV3",
        )
        await dashboard_service.GetPositionRangeHistory(request, mock_context)
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    async def test_lending_returns_v1_stub(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True
        dashboard_service._state_manager = MagicMock()
        request = gateway_pb2.GetPositionRangeHistoryRequest(
            strategy_id="test_strategy",
            chain="base",
            accounting_category="AAVE_COLLATERAL",
            handle="0xpos",
        )
        response = await dashboard_service.GetPositionRangeHistory(request, mock_context)
        assert "VIB-4501" in response.stub_message
        assert len(response.entries) == 0

    async def test_swap_returns_na_stub(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True
        dashboard_service._state_manager = MagicMock()
        request = gateway_pb2.GetPositionRangeHistoryRequest(
            strategy_id="test_strategy",
            chain="base",
            accounting_category="SWAP_RSI",
            handle="0xpos",
        )
        response = await dashboard_service.GetPositionRangeHistory(request, mock_context)
        assert "doesn't apply" in response.stub_message
        assert len(response.entries) == 0

    async def test_lp_returns_position_events(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True
        events = [
            {
                "timestamp": "2026-05-10T00:00:00+00:00",
                "block_number": 1000,
                "event_type": "OPEN",
                "ledger_entry_id": "ledger_abc",
                "tx_hash": "0xabc",
                "tick_lower": -887220,
                "tick_upper": 887220,
                "liquidity": "1234567890",
                "in_range": True,
            },
            {
                "timestamp": "2026-05-11T00:00:00+00:00",
                "block_number": 1010,
                "event_type": "COLLECT",
                "ledger_entry_id": "ledger_def",
                "tx_hash": "0xdef",
                "fees_token0": "10",
                "fees_token1": "20",
            },
        ]
        sm = MagicMock()
        sm.get_position_history = AsyncMock(return_value=events)
        dashboard_service._state_manager = sm

        request = gateway_pb2.GetPositionRangeHistoryRequest(
            strategy_id="test_strategy",
            chain="base",
            accounting_category="LP_UNIV3",
            handle="1234",
        )
        response = await dashboard_service.GetPositionRangeHistory(request, mock_context)

        assert len(response.entries) == 2
        assert response.entries[0].event_type == "OPEN"
        assert response.entries[0].source_table == "position_events"
        assert response.entries[0].ledger_entry_id == "ledger_abc"
        assert response.entries[1].event_type == "COLLECT"

    async def test_time_window_filter_applied(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True
        events = [
            {
                "timestamp": "2026-05-10T00:00:00+00:00",
                "block_number": 1000,
                "event_type": "OPEN",
            },
            {
                "timestamp": "2026-05-15T00:00:00+00:00",
                "block_number": 1010,
                "event_type": "ADJUST",
            },
            {
                "timestamp": "2026-05-20T00:00:00+00:00",
                "block_number": 1020,
                "event_type": "CLOSE",
            },
        ]
        sm = MagicMock()
        sm.get_position_history = AsyncMock(return_value=events)
        dashboard_service._state_manager = sm

        from_unix = int(datetime(2026, 5, 12, tzinfo=UTC).timestamp())
        to_unix = int(datetime(2026, 5, 18, tzinfo=UTC).timestamp())
        request = gateway_pb2.GetPositionRangeHistoryRequest(
            strategy_id="test_strategy",
            chain="base",
            accounting_category="LP_UNIV3",
            handle="1234",
            from_unix_seconds=from_unix,
            to_unix_seconds=to_unix,
        )
        response = await dashboard_service.GetPositionRangeHistory(request, mock_context)

        # Only the May-15 ADJUST event falls in the window.
        assert len(response.entries) == 1
        assert response.entries[0].event_type == "ADJUST"

    # ------------------------------------------------------------------
    # Codex review fixes — chain / accounting_category honored + hash → handle
    # ------------------------------------------------------------------

    async def test_physical_identity_hash_resolved_to_handle_via_registry(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        """When the caller supplies physical_identity_hash, the handler
        must look it up in position_registry (filtered by chain +
        accounting_category) and use the matching row's handle as the
        position_events query key. Previously the handler passed the
        hash straight through, which usually fails because
        position_events.position_id == registry.handle, not hash."""
        dashboard_service._initialized = True
        sm = MagicMock()
        sm.get_position_registry_open_rows = AsyncMock(
            return_value=[
                _registry_row(
                    handle="lp-resolved",
                    physical_identity_hash="0xdeadbeef",
                    chain="base",
                    accounting_category="LP_UNIV3",
                )
            ]
        )
        captured_position_id: list[str] = []

        async def _fake_history(strategy: str, position_id: str) -> list:
            captured_position_id.append(position_id)
            return [
                {
                    "timestamp": datetime(2026, 5, 17, tzinfo=UTC),
                    "block_number": 100,
                    "event_type": "OPEN",
                    "chain": "base",
                    "accounting_category": "LP_UNIV3",
                    "position_id": "lp-resolved",
                }
            ]

        sm.get_position_history = _fake_history
        dashboard_service._state_manager = sm

        response = await dashboard_service.GetPositionRangeHistory(
            gateway_pb2.GetPositionRangeHistoryRequest(
                strategy_id="test_strategy",
                chain="base",
                accounting_category="LP_UNIV3",
                physical_identity_hash="0xdeadbeef",
            ),
            mock_context,
        )

        assert captured_position_id == ["lp-resolved"], (
            "physical_identity_hash must be resolved to the registry row's handle"
        )
        assert len(response.entries) == 1
        sm.get_position_registry_open_rows.assert_awaited_once()

    async def test_physical_identity_hash_no_registry_match_returns_stub(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        """Hash supplied but no registry row matches → stub message,
        not a wrong-query fall-through."""
        dashboard_service._initialized = True
        sm = MagicMock()
        sm.get_position_registry_open_rows = AsyncMock(return_value=[])
        sm.get_position_history = AsyncMock(return_value=[])
        dashboard_service._state_manager = sm

        response = await dashboard_service.GetPositionRangeHistory(
            gateway_pb2.GetPositionRangeHistoryRequest(
                strategy_id="test_strategy",
                chain="base",
                accounting_category="LP_UNIV3",
                physical_identity_hash="0xnonexistent",
            ),
            mock_context,
        )

        assert "no registry row matched" in response.stub_message
        assert len(response.entries) == 0
        # Crucially: we never fell through to get_position_history with a bogus key.
        sm.get_position_history.assert_not_awaited()

    async def test_chain_filter_drops_cross_chain_events(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        """Two positions can share a handle across chains. The post-filter
        must drop events whose chain doesn't match the request."""
        dashboard_service._initialized = True
        sm = MagicMock()

        async def _history(*_args, **_kwargs) -> list:
            return [
                {
                    "timestamp": datetime(2026, 5, 17, tzinfo=UTC),
                    "block_number": 100,
                    "event_type": "OPEN",
                    "chain": "base",
                    "accounting_category": "LP_UNIV3",
                    "position_id": "lp-shared",
                },
                {
                    "timestamp": datetime(2026, 5, 17, tzinfo=UTC),
                    "block_number": 200,
                    "event_type": "OPEN",
                    "chain": "ethereum",
                    "accounting_category": "LP_UNIV3",
                    "position_id": "lp-shared",
                },
            ]

        sm.get_position_history = _history
        dashboard_service._state_manager = sm

        response = await dashboard_service.GetPositionRangeHistory(
            gateway_pb2.GetPositionRangeHistoryRequest(
                strategy_id="test_strategy",
                chain="base",
                accounting_category="LP_UNIV3",
                handle="lp-shared",
            ),
            mock_context,
        )

        assert len(response.entries) == 1
        assert response.entries[0].block_number == 100  # the base-chain row

    async def test_accounting_category_filter_drops_cross_category_events(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        dashboard_service._initialized = True
        sm = MagicMock()

        async def _history(*_args, **_kwargs) -> list:
            return [
                {
                    "timestamp": datetime(2026, 5, 17, tzinfo=UTC),
                    "block_number": 100,
                    "event_type": "OPEN",
                    "chain": "base",
                    "accounting_category": "LP_UNIV3",
                    "position_id": "lp-shared",
                },
                {
                    "timestamp": datetime(2026, 5, 17, tzinfo=UTC),
                    "block_number": 101,
                    "event_type": "OPEN",
                    "chain": "base",
                    "accounting_category": "LP_AERODROME",
                    "position_id": "lp-shared",
                },
            ]

        sm.get_position_history = _history
        dashboard_service._state_manager = sm

        response = await dashboard_service.GetPositionRangeHistory(
            gateway_pb2.GetPositionRangeHistoryRequest(
                strategy_id="test_strategy",
                chain="base",
                accounting_category="LP_UNIV3",
                handle="lp-shared",
            ),
            mock_context,
        )

        assert len(response.entries) == 1
        assert response.entries[0].block_number == 100

    async def test_legacy_events_without_chain_field_are_kept(
        self,
        dashboard_service: DashboardServiceServicer,
        mock_context: MagicMock,
    ) -> None:
        """Permissive policy: an event lacking the typed chain field
        (pre-cutover schema) is not dropped — the registry-side
        position_id lookup already pinned the right position."""
        dashboard_service._initialized = True
        sm = MagicMock()

        async def _history(*_args, **_kwargs) -> list:
            return [
                {
                    "timestamp": datetime(2026, 5, 17, tzinfo=UTC),
                    "block_number": 50,
                    "event_type": "OPEN",
                    # chain / accounting_category absent — legacy row
                    "position_id": "lp-legacy",
                }
            ]

        sm.get_position_history = _history
        dashboard_service._state_manager = sm

        response = await dashboard_service.GetPositionRangeHistory(
            gateway_pb2.GetPositionRangeHistoryRequest(
                strategy_id="test_strategy",
                chain="base",
                accounting_category="LP_UNIV3",
                handle="lp-legacy",
            ),
            mock_context,
        )
        assert len(response.entries) == 1
