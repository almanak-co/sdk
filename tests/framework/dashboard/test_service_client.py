"""Tests for the Phase 2 dashboard service facade.

Coverage:

* The read-only / operator class boundary is REAL — ``DashboardServiceClient``
  must not expose mutation methods; ``OperatorDashboardServiceClient`` is the
  only class that does. Phase 4's CI lint relies on this boundary.
* Each proto → dataclass converter handles its fields verbatim, including
  the ``bytes`` ↔ ``str`` coercion for JSON payload fields.
* Each RPC method sends the right request shape to the stub and wraps the
  response into the expected dataclass.
* Singleton accessors don't leak state across tests.

These tests run without a live gateway — the dashboard stub is mocked at
the ``GatewayClient.dashboard`` property boundary.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from almanak.framework.dashboard.service_client import (
    ApplyReconcileResult,
    CutoverState,
    CutoverStateEntry,
    DashboardClientError,
    DashboardServiceClient,
    GetPositionsResult,
    GetRangeHistoryResult,
    MatchedPosition,
    OperatorDashboardServiceClient,
    PhantomMissingPosition,
    PositionConfidence,
    PositionEntry,
    PositionSource,
    PositionStatus,
    PreviewReconcileResult,
    PrimitiveError,
    RebuiltRow,
    ReconciliationFinding,
    ReconciliationReport,
    ReconciliationSeverity,
    RefreshRegistryResult,
    _convert_cutover_state_entry,
    _convert_finding,
    _convert_matched,
    _convert_phantom_missing,
    _convert_position_entry,
    _convert_primitive_error,
    _convert_primitive_stub,
    _convert_range_history_entry,
    _convert_rebuilt,
    _convert_stranded,
    get_dashboard_service_client,
    get_operator_dashboard_service_client,
    reset_dashboard_service_clients,
)
from almanak.gateway.proto import gateway_pb2

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(autouse=True)
def _reset_singletons():
    """Make every test see fresh singleton state."""
    reset_dashboard_service_clients()
    yield
    reset_dashboard_service_clients()


@pytest.fixture
def mock_stub() -> MagicMock:
    """A fake DashboardService gRPC stub."""
    return MagicMock()


@pytest.fixture
def mock_gateway(mock_stub) -> MagicMock:
    """A fake GatewayClient wired so ``.dashboard`` returns the stub."""
    gw = MagicMock()
    gw.is_connected = True
    gw.health_check.return_value = True
    gw.dashboard = mock_stub
    return gw


@pytest.fixture
def read_client(mock_gateway) -> DashboardServiceClient:
    client = DashboardServiceClient(gateway_client=mock_gateway)
    client.connect()
    return client


@pytest.fixture
def operator_client(mock_gateway) -> OperatorDashboardServiceClient:
    client = OperatorDashboardServiceClient(gateway_client=mock_gateway)
    client.connect()
    return client


# =============================================================================
# Class boundary — the heart of the two-facade contract
# =============================================================================


class TestFacadeBoundary:
    """The read-only / operator split must be real, not convention."""

    @pytest.mark.parametrize(
        "method_name",
        ["preview_reconcile", "apply_reconcile", "refresh_registry_from_chain"],
    )
    def test_read_client_lacks_mutation_methods(self, method_name: str) -> None:
        """DashboardServiceClient must NOT carry mutation methods."""
        assert not hasattr(DashboardServiceClient, method_name), (
            f"DashboardServiceClient should not expose mutation method '{method_name}'. "
            "Renderers rely on this boundary for static safety."
        )

    @pytest.mark.parametrize(
        "method_name",
        ["preview_reconcile", "apply_reconcile", "refresh_registry_from_chain"],
    )
    def test_operator_client_has_mutation_methods(self, method_name: str) -> None:
        assert hasattr(OperatorDashboardServiceClient, method_name)

    @pytest.mark.parametrize(
        "method_name",
        ["get_positions", "get_position_range_history", "get_reconciliation_report"],
    )
    def test_read_client_has_read_methods(self, method_name: str) -> None:
        assert hasattr(DashboardServiceClient, method_name)

    @pytest.mark.parametrize(
        "method_name",
        ["get_positions", "get_position_range_history", "get_reconciliation_report"],
    )
    def test_operator_client_inherits_read_methods(self, method_name: str) -> None:
        """OperatorDashboardServiceClient must inherit reads, not redefine them."""
        assert hasattr(OperatorDashboardServiceClient, method_name)

    def test_operator_subclasses_reader(self) -> None:
        assert issubclass(OperatorDashboardServiceClient, DashboardServiceClient)


# =============================================================================
# Enum round-trips
# =============================================================================


class TestEnumConversion:
    @pytest.mark.parametrize(
        "proto_value, expected",
        [
            (gateway_pb2.CUTOVER_STATE_UNSPECIFIED, CutoverState.UNSPECIFIED),
            (gateway_pb2.CUTOVER_STATE_PRE_BACKFILL, CutoverState.PRE_BACKFILL),
            (gateway_pb2.CUTOVER_STATE_BACKFILL_IN_PROGRESS, CutoverState.BACKFILL_IN_PROGRESS),
            (gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE, CutoverState.BACKFILL_COMPLETE),
            (gateway_pb2.CUTOVER_STATE_REGISTRY_AUTHORITATIVE, CutoverState.REGISTRY_AUTHORITATIVE),
        ],
    )
    def test_cutover_state_from_proto(self, proto_value: int, expected: CutoverState) -> None:
        assert CutoverState.from_proto(proto_value) == expected

    def test_cutover_state_unknown_value_falls_back_to_unspecified(self) -> None:
        """A future proto value the client doesn't know yet must not blow up."""
        assert CutoverState.from_proto(999) == CutoverState.UNSPECIFIED

    @pytest.mark.parametrize(
        "proto_value, expected",
        [
            (gateway_pb2.POSITION_STATUS_OPEN, PositionStatus.OPEN),
            (gateway_pb2.POSITION_STATUS_CLOSED, PositionStatus.CLOSED),
            (gateway_pb2.POSITION_STATUS_REORG_INVALIDATED, PositionStatus.REORG_INVALIDATED),
        ],
    )
    def test_position_status_round_trip(self, proto_value: int, expected: PositionStatus) -> None:
        assert PositionStatus.from_proto(proto_value) == expected
        assert expected.to_proto() == proto_value

    def test_position_status_to_proto_unspecified_default(self) -> None:
        assert PositionStatus.UNSPECIFIED.to_proto() == gateway_pb2.POSITION_STATUS_UNSPECIFIED

    @pytest.mark.parametrize(
        "proto_value, expected",
        [
            (gateway_pb2.RECONCILIATION_SEVERITY_INFO, ReconciliationSeverity.INFO),
            (gateway_pb2.RECONCILIATION_SEVERITY_WARN, ReconciliationSeverity.WARN),
            (gateway_pb2.RECONCILIATION_SEVERITY_DIVERGED, ReconciliationSeverity.DIVERGED),
        ],
    )
    def test_severity_from_proto(self, proto_value: int, expected: ReconciliationSeverity) -> None:
        assert ReconciliationSeverity.from_proto(proto_value) == expected


# =============================================================================
# Dataclass property helpers
# =============================================================================


def _make_position_entry(**overrides) -> PositionEntry:
    """Build a PositionEntry with sensible defaults; override only what each test needs."""
    base = {
        "handle": "my-lp-position",
        "physical_identity_hash": "hash123",
        "deployment_id": "dep1",
        "chain": "avalanche",
        "primitive": "lp",
        "accounting_category": "LP_UNIV3",
        "status": PositionStatus.OPEN,
        "opened_at_block": 1000,
        "closed_at_block": 0,
        "opened_tx": "0xabc",
        "closed_tx": "",
        "value_usd": Decimal("100.50"),
        "value_token0": Decimal("50.00"),
        "value_token1": Decimal("50.25"),
        "source": PositionSource.REGISTRY,
        "confidence": PositionConfidence.HIGH,
        "last_reconciled_at_block": 2000,
        "cutover_state": CutoverState.BACKFILL_COMPLETE,
        "primitive_payload_json": '{"tick_lower":-1000}',
        "value_as_of": "2026-05-17T00:00:00Z",
    }
    base.update(overrides)
    return PositionEntry(**base)


class TestPositionEntryProperties:
    def test_is_lp_true_for_lp_primitive(self) -> None:
        assert _make_position_entry(primitive="lp").is_lp is True

    def test_is_lp_false_for_lending(self) -> None:
        assert _make_position_entry(primitive="lending").is_lp is False

    def test_is_open(self) -> None:
        assert _make_position_entry(status=PositionStatus.OPEN).is_open is True
        assert _make_position_entry(status=PositionStatus.CLOSED).is_open is False

    def test_value_as_of_datetime_parses_iso(self) -> None:
        e = _make_position_entry(value_as_of="2026-05-17T12:30:00Z")
        parsed = e.value_as_of_datetime
        assert parsed == datetime(2026, 5, 17, 12, 30, 0, tzinfo=UTC)

    def test_value_as_of_datetime_empty(self) -> None:
        assert _make_position_entry(value_as_of="").value_as_of_datetime is None

    def test_value_as_of_datetime_invalid(self) -> None:
        assert _make_position_entry(value_as_of="not-a-date").value_as_of_datetime is None


class TestGetPositionsResultGrouping:
    def test_by_accounting_category(self) -> None:
        result = GetPositionsResult(
            positions=[
                _make_position_entry(handle="lp1", accounting_category="LP_UNIV3"),
                _make_position_entry(handle="lp2", accounting_category="LP_UNIV3"),
                _make_position_entry(handle="aave1", accounting_category="AAVE_COLLATERAL"),
            ],
            cutover_states=[],
        )
        grouped = result.by_accounting_category()
        assert set(grouped.keys()) == {"LP_UNIV3", "AAVE_COLLATERAL"}
        assert len(grouped["LP_UNIV3"]) == 2
        assert len(grouped["AAVE_COLLATERAL"]) == 1

    def test_cutover_for_returns_matching_entry(self) -> None:
        cutover = CutoverStateEntry(
            accounting_category="LP_UNIV3",
            state=CutoverState.BACKFILL_COMPLETE,
            rows_synthesized=10,
            rows_skipped_already_present=2,
            backfill_started_at="2026-05-01T00:00:00Z",
            backfill_completed_at="2026-05-02T00:00:00Z",
            backfill_reader_version=1,
            last_reconciled_at_block=5000,
            last_reconciled_unix_seconds=1747440000,
        )
        result = GetPositionsResult(positions=[], cutover_states=[cutover])
        assert result.cutover_for("LP_UNIV3") is cutover

    def test_cutover_for_returns_none_if_missing(self) -> None:
        result = GetPositionsResult(positions=[], cutover_states=[])
        assert result.cutover_for("LP_UNIV3") is None


class TestCutoverStateEntryProperties:
    def test_last_reconciled_at_with_seconds(self) -> None:
        entry = CutoverStateEntry(
            accounting_category="LP_UNIV3",
            state=CutoverState.BACKFILL_COMPLETE,
            rows_synthesized=10,
            rows_skipped_already_present=0,
            backfill_started_at="",
            backfill_completed_at="",
            backfill_reader_version=1,
            last_reconciled_at_block=5000,
            last_reconciled_unix_seconds=1747440000,
        )
        assert entry.last_reconciled_at == datetime.fromtimestamp(1747440000, tz=UTC)

    def test_last_reconciled_at_zero_returns_none(self) -> None:
        entry = CutoverStateEntry(
            accounting_category="LP_UNIV3",
            state=CutoverState.UNSPECIFIED,
            rows_synthesized=0,
            rows_skipped_already_present=0,
            backfill_started_at="",
            backfill_completed_at="",
            backfill_reader_version=0,
            last_reconciled_at_block=0,
            last_reconciled_unix_seconds=0,
        )
        assert entry.last_reconciled_at is None


class TestReconciliationReportProperties:
    def test_severity_counts(self) -> None:
        report = ReconciliationReport(
            findings=[
                ReconciliationFinding(
                    accounting_category="LP_UNIV3",
                    physical_identity_hash="h1",
                    severity=ReconciliationSeverity.DIVERGED,
                    delta="ledger says 100, registry says 95",
                    ledger_has_row=True,
                    snapshot_has_row=True,
                    registry_has_row=True,
                    suggested_action="PreviewReconcile",
                ),
                ReconciliationFinding(
                    accounting_category="LP_UNIV3",
                    physical_identity_hash="h2",
                    severity=ReconciliationSeverity.WARN,
                    delta="2-of-3",
                    ledger_has_row=True,
                    snapshot_has_row=False,
                    registry_has_row=True,
                    suggested_action="",
                ),
                ReconciliationFinding(
                    accounting_category="LP_UNIV3",
                    physical_identity_hash="h3",
                    severity=ReconciliationSeverity.INFO,
                    delta="",
                    ledger_has_row=True,
                    snapshot_has_row=True,
                    registry_has_row=True,
                    suggested_action="",
                ),
            ],
            primitive_stubs=[],
            reconciliation_id="recon-1",
            source_block_number=50000,
            as_of="2026-05-17T01:00:00Z",
        )
        assert report.diverged_count == 1
        assert report.warn_count == 1


class TestPreviewReconcileResultProperties:
    def test_has_diff_true_when_phantom_missing(self) -> None:
        result = PreviewReconcileResult(
            preview_token="t1",
            matched=[],
            phantom_missing=[
                PhantomMissingPosition(
                    physical_identity_hash="h1",
                    primitive="lp",
                    accounting_category="LP_UNIV3",
                    semantic_grouping_key="",
                    payload_json="{}",
                    opened_at_block=0,
                    opened_tx="",
                )
            ],
            stranded=[],
            primitive_stubs=[],
            reconciliation_id="r1",
            source_block_number=100,
            expires_at_unix_seconds=1747445000,
        )
        assert result.has_diff is True

    def test_has_diff_false_when_all_matched(self) -> None:
        result = PreviewReconcileResult(
            preview_token="t1",
            matched=[
                MatchedPosition(
                    physical_identity_hash="h1",
                    primitive="lp",
                    accounting_category="LP_UNIV3",
                    confirmed_at_block=100,
                )
            ],
            phantom_missing=[],
            stranded=[],
            primitive_stubs=[],
            reconciliation_id="r1",
            source_block_number=100,
            expires_at_unix_seconds=0,
        )
        assert result.has_diff is False

    def test_expires_at(self) -> None:
        result = PreviewReconcileResult(
            preview_token="t1",
            matched=[],
            phantom_missing=[],
            stranded=[],
            primitive_stubs=[],
            reconciliation_id="r1",
            source_block_number=100,
            expires_at_unix_seconds=1747445000,
        )
        assert result.expires_at == datetime.fromtimestamp(1747445000, tz=UTC)


class TestApplyReconcileResultProperties:
    @pytest.mark.parametrize(
        "result_str, expect_success, expect_retry",
        [
            ("SUCCESS", True, False),
            ("PARTIAL_SUCCESS", False, False),
            ("STATE_DRIFT", False, True),
            ("EXPIRED", False, True),
            ("NOT_FOUND", False, True),
        ],
    )
    def test_outcome_helpers(self, result_str: str, expect_success: bool, expect_retry: bool) -> None:
        outcome = ApplyReconcileResult(result=result_str, detail="")
        assert outcome.is_success is expect_success
        assert outcome.needs_retry is expect_retry


# =============================================================================
# Converters — proto → dataclass
# =============================================================================


class TestConverters:
    def test_position_entry_round_trip(self) -> None:
        proto = gateway_pb2.PositionEntry(
            handle="lp-1",
            physical_identity_hash="hash-abc",
            deployment_id="dep1",
            chain="avalanche",
            primitive="lp",
            accounting_category="LP_UNIV3",
            status=gateway_pb2.POSITION_STATUS_OPEN,
            opened_at_block=1234,
            closed_at_block=0,
            opened_tx="0xopen",
            closed_tx="",
            value_usd="100.50",
            value_token0="50.00",
            value_token1="50.25",
            source=gateway_pb2.POSITION_SOURCE_REGISTRY,
            confidence=gateway_pb2.POSITION_CONFIDENCE_HIGH,
            last_reconciled_at_block=2000,
            cutover_state=gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE,
            primitive_payload_json=b'{"tick_lower":-1000,"tick_upper":1000}',
            value_as_of="2026-05-17T00:00:00Z",
        )
        result = _convert_position_entry(proto)
        assert result.handle == "lp-1"
        assert result.value_usd == Decimal("100.50")
        assert result.status == PositionStatus.OPEN
        assert result.source == PositionSource.REGISTRY
        assert result.cutover_state == CutoverState.BACKFILL_COMPLETE
        # The bytes payload must become a str so renderers can json.loads() it.
        assert result.primitive_payload_json == '{"tick_lower":-1000,"tick_upper":1000}'

    def test_position_entry_handles_empty_value_strings(self) -> None:
        proto = gateway_pb2.PositionEntry(
            handle="lp-1",
            physical_identity_hash="h",
            value_usd="",
            value_token0="",
            value_token1="",
        )
        result = _convert_position_entry(proto)
        assert result.value_usd == Decimal("0")
        assert result.value_token0 == Decimal("0")
        assert result.value_token1 == Decimal("0")

    def test_position_entry_handles_malformed_decimals(self) -> None:
        """A garbage ``value_usd`` must not crash the dashboard render."""
        proto = gateway_pb2.PositionEntry(
            handle="lp-1", physical_identity_hash="h", value_usd="not-a-number"
        )
        result = _convert_position_entry(proto)
        assert result.value_usd == Decimal("0")

    def test_cutover_state_entry_round_trip(self) -> None:
        proto = gateway_pb2.CutoverStateEntry(
            accounting_category="LP_UNIV3",
            state=gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE,
            rows_synthesized=42,
            rows_skipped_already_present=3,
            backfill_started_at="2026-05-01T00:00:00Z",
            backfill_completed_at="2026-05-02T00:00:00Z",
            backfill_reader_version=2,
            last_reconciled_at_block=5000,
            last_reconciled_unix_seconds=1747440000,
        )
        result = _convert_cutover_state_entry(proto)
        assert result.state == CutoverState.BACKFILL_COMPLETE
        assert result.rows_synthesized == 42
        assert result.rows_skipped_already_present == 3
        assert result.backfill_completed_at == "2026-05-02T00:00:00Z"

    def test_range_history_entry_round_trip(self) -> None:
        proto = gateway_pb2.RangeHistoryEntry(
            timestamp_unix_seconds=1747440000,
            block_number=5000,
            event_type="OPEN",
            source_table="position_events",
            ledger_entry_id="ledger-42",
            tx_hash="0xtx",
            payload_json=b'{"tick_lower":-1000}',
        )
        result = _convert_range_history_entry(proto)
        assert result.event_type == "OPEN"
        assert result.source_table == "position_events"
        assert result.ledger_entry_id == "ledger-42"
        assert result.payload_json == '{"tick_lower":-1000}'
        assert result.timestamp == datetime.fromtimestamp(1747440000, tz=UTC)

    def test_range_history_entry_no_timestamp(self) -> None:
        proto = gateway_pb2.RangeHistoryEntry(timestamp_unix_seconds=0)
        assert _convert_range_history_entry(proto).timestamp is None

    def test_finding_round_trip(self) -> None:
        proto = gateway_pb2.ReconciliationFinding(
            accounting_category="LP_UNIV3",
            physical_identity_hash="h1",
            severity=gateway_pb2.RECONCILIATION_SEVERITY_DIVERGED,
            delta="ledger says 100, registry says 95",
            ledger_has_row=True,
            snapshot_has_row=True,
            registry_has_row=False,
            suggested_action="PreviewReconcile to inspect rebuilt rows",
        )
        result = _convert_finding(proto)
        assert result.severity == ReconciliationSeverity.DIVERGED
        assert result.registry_has_row is False
        assert result.delta == "ledger says 100, registry says 95"

    def test_primitive_stub_round_trip(self) -> None:
        proto = gateway_pb2.PrimitiveCoverageStub(
            primitive="lending",
            message="Reconciliation for lending — pending VIB-4501",
            ticket="VIB-4501",
        )
        result = _convert_primitive_stub(proto)
        assert result.primitive == "lending"
        assert result.ticket == "VIB-4501"

    def test_matched_round_trip(self) -> None:
        proto = gateway_pb2.MatchedPosition(
            physical_identity_hash="h1",
            primitive="lp",
            accounting_category="LP_UNIV3",
            confirmed_at_block=2000,
        )
        result = _convert_matched(proto)
        assert isinstance(result, MatchedPosition)
        assert result.confirmed_at_block == 2000

    def test_phantom_missing_round_trip_with_bytes_payload(self) -> None:
        proto = gateway_pb2.PhantomMissingPosition(
            physical_identity_hash="h1",
            primitive="lp",
            accounting_category="LP_UNIV3",
            semantic_grouping_key="key1",
            payload_json=b'{"on_chain":"data"}',
            opened_at_block=1000,
            opened_tx="0xtx",
        )
        result = _convert_phantom_missing(proto)
        assert result.payload_json == '{"on_chain":"data"}'
        assert result.opened_at_block == 1000

    def test_stranded_round_trip(self) -> None:
        proto = gateway_pb2.StrandedRow(
            physical_identity_hash="h1",
            primitive="lp",
            accounting_category="LP_UNIV3",
            handle="my-lp",
            registry_row_json=b'{"row":"data"}',
            confirmed_absent_at_block=3000,
            absent_reason="position not found on chain",
        )
        result = _convert_stranded(proto)
        assert result.handle == "my-lp"
        assert result.registry_row_json == '{"row":"data"}'

    def test_rebuilt_round_trip(self) -> None:
        proto = gateway_pb2.RebuiltRow(
            physical_identity_hash="h1",
            primitive="lp",
            accounting_category="LP_UNIV3",
            source="reconciliation_discovery",
            last_reconciled_at_block=4000,
            reconciliation_id="recon-1",
            registry_row_json=b'{}',
        )
        result = _convert_rebuilt(proto)
        assert isinstance(result, RebuiltRow)
        assert result.source == "reconciliation_discovery"

    def test_primitive_error_round_trip(self) -> None:
        proto = gateway_pb2.PrimitiveError(
            primitive="lp",
            chain="avalanche",
            code="RPC_FANOUT_FAILED",
            message="timeout reading on-chain position",
            recoverable=True,
        )
        result = _convert_primitive_error(proto)
        assert isinstance(result, PrimitiveError)
        assert result.code == "RPC_FANOUT_FAILED"
        assert result.recoverable is True


# =============================================================================
# Read-only RPC methods
# =============================================================================


class TestGetPositions:
    def test_request_shape(self, read_client: DashboardServiceClient, mock_stub: MagicMock) -> None:
        mock_stub.GetPositions.return_value = gateway_pb2.GetPositionsResponse()

        read_client.get_positions(
            "aave-avax",
            chain="avalanche",
            primitive="lp",
            accounting_category="LP_UNIV3",
            status=PositionStatus.OPEN,
        )

        assert mock_stub.GetPositions.called
        request = mock_stub.GetPositions.call_args[0][0]
        assert request.strategy_id == "aave-avax"
        assert request.chain == "avalanche"
        assert request.primitive == "lp"
        assert request.accounting_category == "LP_UNIV3"
        assert request.status == gateway_pb2.POSITION_STATUS_OPEN

    def test_request_shape_defaults(self, read_client: DashboardServiceClient, mock_stub: MagicMock) -> None:
        """Only strategy_id supplied — all filters empty / unspecified."""
        mock_stub.GetPositions.return_value = gateway_pb2.GetPositionsResponse()

        read_client.get_positions("aave-avax")

        request = mock_stub.GetPositions.call_args[0][0]
        assert request.strategy_id == "aave-avax"
        assert request.chain == ""
        assert request.primitive == ""
        assert request.status == gateway_pb2.POSITION_STATUS_UNSPECIFIED

    def test_parses_full_response(self, read_client: DashboardServiceClient, mock_stub: MagicMock) -> None:
        response = gateway_pb2.GetPositionsResponse(
            positions=[
                gateway_pb2.PositionEntry(
                    handle="lp-1",
                    physical_identity_hash="h1",
                    chain="avalanche",
                    primitive="lp",
                    accounting_category="LP_UNIV3",
                    status=gateway_pb2.POSITION_STATUS_OPEN,
                    value_usd="100.00",
                    source=gateway_pb2.POSITION_SOURCE_REGISTRY,
                    confidence=gateway_pb2.POSITION_CONFIDENCE_HIGH,
                    cutover_state=gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE,
                )
            ],
            cutover_states=[
                gateway_pb2.CutoverStateEntry(
                    accounting_category="LP_UNIV3",
                    state=gateway_pb2.CUTOVER_STATE_BACKFILL_COMPLETE,
                    rows_synthesized=5,
                )
            ],
        )
        mock_stub.GetPositions.return_value = response

        result = read_client.get_positions("aave-avax")

        assert isinstance(result, GetPositionsResult)
        assert len(result.positions) == 1
        assert result.positions[0].handle == "lp-1"
        assert result.positions[0].confidence == PositionConfidence.HIGH
        assert len(result.cutover_states) == 1

    def test_grpc_error_wrapped(self, read_client: DashboardServiceClient, mock_stub: MagicMock) -> None:
        import grpc as _grpc

        err = _grpc.RpcError("boom")
        mock_stub.GetPositions.side_effect = err

        with pytest.raises(DashboardClientError, match="GetPositions failed"):
            read_client.get_positions("aave-avax")


class TestGetPositionRangeHistory:
    def test_handle_routing(self, read_client: DashboardServiceClient, mock_stub: MagicMock) -> None:
        mock_stub.GetPositionRangeHistory.return_value = gateway_pb2.GetPositionRangeHistoryResponse()

        read_client.get_position_range_history(
            "aave-avax",
            chain="avalanche",
            accounting_category="LP_UNIV3",
            handle="my-lp",
        )
        request = mock_stub.GetPositionRangeHistory.call_args[0][0]
        assert request.handle == "my-lp"
        assert request.physical_identity_hash == ""

    def test_physical_identity_hash_routing(
        self, read_client: DashboardServiceClient, mock_stub: MagicMock
    ) -> None:
        mock_stub.GetPositionRangeHistory.return_value = gateway_pb2.GetPositionRangeHistoryResponse()

        read_client.get_position_range_history(
            "aave-avax",
            chain="avalanche",
            accounting_category="LP_UNIV3",
            physical_identity_hash="hash-abc",
        )
        request = mock_stub.GetPositionRangeHistory.call_args[0][0]
        assert request.physical_identity_hash == "hash-abc"
        assert request.handle == ""

    def test_neither_handle_nor_hash_raises(self, read_client: DashboardServiceClient) -> None:
        with pytest.raises(ValueError, match="handle or physical_identity_hash"):
            read_client.get_position_range_history(
                "aave-avax",
                chain="avalanche",
                accounting_category="LP_UNIV3",
            )

    def test_time_window_converted_to_unix(
        self, read_client: DashboardServiceClient, mock_stub: MagicMock
    ) -> None:
        mock_stub.GetPositionRangeHistory.return_value = gateway_pb2.GetPositionRangeHistoryResponse()
        start = datetime(2026, 5, 17, 0, 0, 0, tzinfo=UTC)
        end = datetime(2026, 5, 17, 12, 0, 0, tzinfo=UTC)

        read_client.get_position_range_history(
            "aave-avax",
            chain="avalanche",
            accounting_category="LP_UNIV3",
            handle="lp",
            from_time=start,
            to_time=end,
        )
        request = mock_stub.GetPositionRangeHistory.call_args[0][0]
        assert request.from_unix_seconds == int(start.timestamp())
        assert request.to_unix_seconds == int(end.timestamp())

    def test_parses_stub_message(
        self, read_client: DashboardServiceClient, mock_stub: MagicMock
    ) -> None:
        """Swap-only primitives surface a stub message instead of empty entries."""
        mock_stub.GetPositionRangeHistory.return_value = gateway_pb2.GetPositionRangeHistoryResponse(
            stub_message="No held position; see trade tape for swap history.",
        )

        result = read_client.get_position_range_history(
            "aave-avax",
            chain="avalanche",
            accounting_category="SWAP",
            handle="x",
        )
        assert isinstance(result, GetRangeHistoryResult)
        assert result.stub_message == "No held position; see trade tape for swap history."
        assert result.entries == []

    def test_parses_entries(self, read_client: DashboardServiceClient, mock_stub: MagicMock) -> None:
        mock_stub.GetPositionRangeHistory.return_value = gateway_pb2.GetPositionRangeHistoryResponse(
            entries=[
                gateway_pb2.RangeHistoryEntry(
                    timestamp_unix_seconds=1747440000,
                    block_number=5000,
                    event_type="OPEN",
                    source_table="position_events",
                    ledger_entry_id="ledger-1",
                    tx_hash="0xtx1",
                    payload_json=b'{}',
                ),
                gateway_pb2.RangeHistoryEntry(
                    timestamp_unix_seconds=1747443600,
                    block_number=5100,
                    event_type="ADJUST",
                    source_table="position_events",
                    ledger_entry_id="ledger-2",
                    tx_hash="0xtx2",
                    payload_json=b'{}',
                ),
            ],
        )
        result = read_client.get_position_range_history(
            "aave-avax", chain="avalanche", accounting_category="LP_UNIV3", handle="lp"
        )
        assert len(result.entries) == 2
        assert result.entries[0].event_type == "OPEN"
        assert result.entries[1].event_type == "ADJUST"


class TestGetReconciliationReport:
    def test_request_shape(self, read_client: DashboardServiceClient, mock_stub: MagicMock) -> None:
        mock_stub.GetReconciliationReport.return_value = gateway_pb2.GetReconciliationReportResponse()
        read_client.get_reconciliation_report("aave-avax")
        request = mock_stub.GetReconciliationReport.call_args[0][0]
        assert request.strategy_id == "aave-avax"

    def test_parses_findings_and_stubs(
        self, read_client: DashboardServiceClient, mock_stub: MagicMock
    ) -> None:
        mock_stub.GetReconciliationReport.return_value = gateway_pb2.GetReconciliationReportResponse(
            findings=[
                gateway_pb2.ReconciliationFinding(
                    accounting_category="LP_UNIV3",
                    physical_identity_hash="h1",
                    severity=gateway_pb2.RECONCILIATION_SEVERITY_DIVERGED,
                    delta="100 vs 95",
                    ledger_has_row=True,
                    snapshot_has_row=True,
                    registry_has_row=True,
                    suggested_action="PreviewReconcile",
                )
            ],
            primitive_stubs=[
                gateway_pb2.PrimitiveCoverageStub(
                    primitive="lending",
                    message="pending VIB-4501",
                    ticket="VIB-4501",
                )
            ],
            reconciliation_id="recon-1",
            source_block_number=50000,
            as_of="2026-05-17T01:00:00Z",
        )

        report = read_client.get_reconciliation_report("aave-avax")

        assert isinstance(report, ReconciliationReport)
        assert len(report.findings) == 1
        assert report.findings[0].severity == ReconciliationSeverity.DIVERGED
        assert len(report.primitive_stubs) == 1
        assert report.primitive_stubs[0].ticket == "VIB-4501"
        assert report.reconciliation_id == "recon-1"
        assert report.diverged_count == 1


# =============================================================================
# Operator RPC methods
# =============================================================================


class TestPreviewReconcile:
    def test_request_shape(self, operator_client, mock_stub: MagicMock) -> None:
        mock_stub.PreviewReconcile.return_value = gateway_pb2.PreviewReconcileResponse()
        operator_client.preview_reconcile("aave-avax")
        request = mock_stub.PreviewReconcile.call_args[0][0]
        assert request.strategy_id == "aave-avax"

    def test_parses_diff_buckets(self, operator_client, mock_stub: MagicMock) -> None:
        mock_stub.PreviewReconcile.return_value = gateway_pb2.PreviewReconcileResponse(
            preview_token="tok-abc-123",
            matched=[
                gateway_pb2.MatchedPosition(
                    physical_identity_hash="h1",
                    primitive="lp",
                    accounting_category="LP_UNIV3",
                    confirmed_at_block=2000,
                )
            ],
            phantom_missing=[
                gateway_pb2.PhantomMissingPosition(
                    physical_identity_hash="h2",
                    primitive="lp",
                    accounting_category="LP_UNIV3",
                    payload_json=b'{}',
                )
            ],
            stranded=[],
            primitive_stubs=[
                gateway_pb2.PrimitiveCoverageStub(
                    primitive="perp", message="pending VIB-4202", ticket="VIB-4202"
                )
            ],
            reconciliation_id="recon-2",
            source_block_number=100,
            expires_at_unix_seconds=1747445000,
        )

        result = operator_client.preview_reconcile("aave-avax")

        assert isinstance(result, PreviewReconcileResult)
        assert result.preview_token == "tok-abc-123"
        assert len(result.matched) == 1
        assert len(result.phantom_missing) == 1
        assert len(result.stranded) == 0
        assert len(result.primitive_stubs) == 1
        assert result.has_diff is True


class TestApplyReconcile:
    def test_empty_token_raises(self, operator_client) -> None:
        with pytest.raises(ValueError, match="preview_token"):
            operator_client.apply_reconcile("aave-avax", "")

    def test_success_path(self, operator_client, mock_stub: MagicMock) -> None:
        mock_stub.ApplyReconcile.return_value = gateway_pb2.ApplyReconcileResponse(
            result="SUCCESS",
            detail="2 rows rebuilt",
            rebuilt=[
                gateway_pb2.RebuiltRow(
                    physical_identity_hash="h1",
                    primitive="lp",
                    accounting_category="LP_UNIV3",
                    source="reconciliation_discovery",
                    last_reconciled_at_block=4000,
                    reconciliation_id="recon-3",
                    registry_row_json=b'{}',
                )
            ],
            reconciliation_id="recon-3",
        )

        outcome = operator_client.apply_reconcile("aave-avax", "tok-1")
        request = mock_stub.ApplyReconcile.call_args[0][0]
        assert request.strategy_id == "aave-avax"
        assert request.preview_token == "tok-1"
        assert outcome.result == "SUCCESS"
        assert outcome.is_success is True
        assert outcome.needs_retry is False
        assert len(outcome.rebuilt) == 1

    def test_state_drift_path(self, operator_client, mock_stub: MagicMock) -> None:
        mock_stub.ApplyReconcile.return_value = gateway_pb2.ApplyReconcileResponse(
            result="STATE_DRIFT",
            detail="registry changed since preview",
        )
        outcome = operator_client.apply_reconcile("aave-avax", "tok-1")
        assert outcome.needs_retry is True
        assert outcome.is_success is False

    def test_partial_success_carries_errors(self, operator_client, mock_stub: MagicMock) -> None:
        mock_stub.ApplyReconcile.return_value = gateway_pb2.ApplyReconcileResponse(
            result="PARTIAL_SUCCESS",
            detail="1 of 2 rows applied",
            rebuilt=[
                gateway_pb2.RebuiltRow(
                    physical_identity_hash="h1",
                    primitive="lp",
                    accounting_category="LP_UNIV3",
                    source="reconciliation_discovery",
                    reconciliation_id="recon-4",
                )
            ],
            primitive_errors=[
                gateway_pb2.PrimitiveError(
                    primitive="lp",
                    chain="avalanche",
                    code="RPC_FANOUT_FAILED",
                    message="timeout",
                    recoverable=True,
                )
            ],
            reconciliation_id="recon-4",
        )
        outcome = operator_client.apply_reconcile("aave-avax", "tok-1")
        assert outcome.result == "PARTIAL_SUCCESS"
        assert outcome.is_success is False
        assert outcome.needs_retry is False  # PARTIAL is terminal for THIS call
        assert len(outcome.rebuilt) == 1
        assert len(outcome.primitive_errors) == 1
        assert outcome.primitive_errors[0].recoverable is True


class TestRefreshRegistryFromChain:
    def test_request_shape(self, operator_client, mock_stub: MagicMock) -> None:
        mock_stub.RefreshRegistryFromChain.return_value = gateway_pb2.RefreshRegistryFromChainResponse(
            result="SUCCESS"
        )
        operator_client.refresh_registry_from_chain("aave-avax")
        request = mock_stub.RefreshRegistryFromChain.call_args[0][0]
        assert request.strategy_id == "aave-avax"

    def test_rate_limited_outcome(self, operator_client, mock_stub: MagicMock) -> None:
        mock_stub.RefreshRegistryFromChain.return_value = gateway_pb2.RefreshRegistryFromChainResponse(
            result="RATE_LIMITED",
            detail="another refresh in flight",
        )
        outcome = operator_client.refresh_registry_from_chain("aave-avax")
        assert outcome.result == "RATE_LIMITED"
        assert outcome.is_success is False

    def test_success_counters(self, operator_client, mock_stub: MagicMock) -> None:
        mock_stub.RefreshRegistryFromChain.return_value = gateway_pb2.RefreshRegistryFromChainResponse(
            result="SUCCESS",
            detail="",
            positions_refreshed=7,
            events_emitted=2,
            source_block_number=100,
            reconciliation_id="recon-5",
        )
        outcome = operator_client.refresh_registry_from_chain("aave-avax")
        assert isinstance(outcome, RefreshRegistryResult)
        assert outcome.is_success is True
        assert outcome.positions_refreshed == 7
        assert outcome.events_emitted == 2


# =============================================================================
# Connection / lifecycle
# =============================================================================


class TestLifecycle:
    def test_stub_raises_when_not_connected(self) -> None:
        gw = MagicMock()
        gw.is_connected = False
        gw.health_check.return_value = True
        client = DashboardServiceClient(gateway_client=gw)
        # Don't call connect()
        with pytest.raises(DashboardClientError, match="Not connected"):
            client.get_positions("x")

    def test_connect_raises_when_health_check_fails(self) -> None:
        gw = MagicMock()
        gw.is_connected = False
        gw.connect.return_value = None
        gw.health_check.return_value = False
        # Pretend connect() succeeded → is_connected flips true post-connect
        gw.is_connected = True
        gw.health_check.return_value = False
        client = DashboardServiceClient(gateway_client=gw)
        with pytest.raises(DashboardClientError, match="not healthy"):
            client.connect()

    def test_owns_client_disconnects_on_disconnect(self) -> None:
        """A client constructed without an injected gateway owns + disconnects it."""
        from almanak.framework import dashboard as _dash_mod
        # The "owns" branch only fires when gateway_client=None at construction
        client = DashboardServiceClient(gateway_client=None)
        # Avoid actually connecting; just verify the disconnect path is wired.
        client._client = MagicMock()  # type: ignore[attr-defined]
        client.disconnect()
        client._client.disconnect.assert_called_once()  # type: ignore[attr-defined]
        # Sanity import — module loaded cleanly
        assert _dash_mod is not None


class TestSingletons:
    def test_get_dashboard_service_client_is_singleton(self) -> None:
        a = get_dashboard_service_client()
        b = get_dashboard_service_client()
        assert a is b

    def test_get_operator_dashboard_service_client_is_singleton(self) -> None:
        a = get_operator_dashboard_service_client()
        b = get_operator_dashboard_service_client()
        assert a is b

    def test_singletons_are_distinct(self) -> None:
        """Reader and operator share no state."""
        reader = get_dashboard_service_client()
        operator = get_operator_dashboard_service_client()
        assert reader is not operator

    def test_reset_clears_both(self) -> None:
        a = get_dashboard_service_client()
        reset_dashboard_service_clients()
        b = get_dashboard_service_client()
        assert a is not b
