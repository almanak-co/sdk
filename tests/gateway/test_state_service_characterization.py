"""Characterization tests for StateService — Phase 5b.

Adds focused branch coverage on top of an already-substantial existing inventory
(test_state_service_outbox_rpcs.py 561 lines, test_state_service_agent_id.py
157 lines, test_save_portfolio_metrics_characterization.py 564 lines,
test_get_position_history_rpc.py 679 lines, test_portfolio_metrics_rpc.py
357 lines, test_accounting_position_event_rpcs.py 592 lines,
test_ledger_entry_rpc.py, test_get_accounting_events_rpc.py — all pre-existing).

The Phase 0 CRAP scan reported state_service.py at 12%; that was an artifact
of running only the 3 narrow characterization tests. Real coverage on main
including the full gateway test inventory is **65%** — same Phase 6
misplacement pattern. This PR targets the genuine remaining gaps:

  TestLoadState / TestSaveState / TestDeleteState (5b-1, 21 tests)
      The 3 core CRUD RPCs strategy containers hit every iteration. Branch
      coverage on input validation, exception → status mapping, AGENT_ID
      legacy-key fallback (deployed-mode path).

  TestSavePortfolioSnapshot (8 tests)
      Largest single uncovered chunk in the file (~132 stmts at lines 557-688
      pre-PR). Validation, JSON envelope shape (legacy list / new envelope /
      malformed), PG ON CONFLICT path, SQLite warm-backend dispatch,
      exception → INTERNAL.

  TestGetLatestSnapshot / TestGetSnapshotsSince (~10 tests)
      Symmetric coverage of the read paths.

  TestEnsureInitialized (2 tests)
      Backend selection + idempotent re-init.

Net: state_service.py 65% → ~85% (estimated; CI will report exact).
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import grpc
import pytest

from almanak.framework.state.state_manager import StateNotFoundError, StateTier
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer
from tests.gateway.grpc_harness import (
    assert_grpc_error,
    assert_set_code_not_called,
    make_grpc_context,
)


# ──────────────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────────────


def _make_settings(database_url: str | None = None, standalone: bool = False) -> SimpleNamespace:
    """Minimal GatewaySettings shim. StateService reads .database_url + .standalone."""
    return SimpleNamespace(database_url=database_url, standalone=standalone)


@pytest.fixture(autouse=True)
def _isolate_agent_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip AGENT_ID from os.environ so resolve_agent_id() passes through
    the strategy_id unchanged in every test (unless a test sets it explicitly).
    """
    monkeypatch.delenv("AGENT_ID", raising=False)


@pytest.fixture
def context() -> MagicMock:
    return make_grpc_context()


@pytest.fixture
def state_manager() -> AsyncMock:
    """Mock StateManager. Tests configure load/save/delete return values per case."""
    sm = AsyncMock()
    return sm


@pytest.fixture
def service(state_manager: AsyncMock) -> StateServiceServicer:
    """Servicer pre-initialised with the mock state manager. Bypasses
    _ensure_initialized's StateManager import + initialize() call."""
    svc = StateServiceServicer(_make_settings())
    svc._state_manager = state_manager
    svc._initialized = True
    return svc


def _make_state_data_obj(
    *,
    strategy_id: str = "strat-1",
    version: int = 1,
    state: dict | None = None,
    schema_version: int = 1,
    checksum: str = "abc123",
    created_at: datetime | None = None,
    loaded_from: StateTier | None = None,
) -> SimpleNamespace:
    """Build a StateData-shaped object the way StateManager.load_state returns.

    ``loaded_from`` defaults to the real :class:`StateTier` enum so the
    serialiser hits production's ``state.loaded_from.name.lower()`` path,
    which the wire contract (``gateway.proto:236``) defines as lowercase
    ``"hot"`` / ``"warm"`` (issue #2053).
    """
    return SimpleNamespace(
        strategy_id=strategy_id,
        version=version,
        state=state if state is not None else {"foo": "bar"},
        schema_version=schema_version,
        checksum=checksum,
        created_at=created_at or datetime(2026, 5, 4, 12, 0, tzinfo=UTC),
        loaded_from=loaded_from if loaded_from is not None else StateTier.WARM,
    )


# ──────────────────────────────────────────────────────────────────────────────
# LoadState
# ──────────────────────────────────────────────────────────────────────────────


class TestLoadState:
    @pytest.mark.asyncio
    async def test_invalid_strategy_id_returns_invalid_argument_before_init(
        self, service, state_manager, context,
    ):
        # Reset _initialized so we can assert the validation runs first.
        service._initialized = False
        request = gateway_pb2.LoadStateRequest(strategy_id="has spaces!")
        response = await service.LoadState(request, context)
        assert response.data == b""
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        # Validation must short-circuit before any state-manager call.
        state_manager.load_state.assert_not_called()
        # And before _ensure_initialized's import-side-effect chain.
        assert service._initialized is False

    @pytest.mark.asyncio
    async def test_state_not_found_returns_not_found(self, service, state_manager, context):
        state_manager.load_state.return_value = None
        request = gateway_pb2.LoadStateRequest(strategy_id="strat-1")
        response = await service.LoadState(request, context)
        assert response.data == b""
        context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)

    @pytest.mark.asyncio
    async def test_happy_path_returns_serialised_state(self, service, state_manager, context):
        state_manager.load_state.return_value = _make_state_data_obj(
            strategy_id="strat-1",
            version=42,
            state={"counter": 7, "name": "alice"},
            schema_version=2,
            checksum="deadbeef",
        )
        request = gateway_pb2.LoadStateRequest(strategy_id="strat-1")
        response = await service.LoadState(request, context)

        assert response.strategy_id == "strat-1"
        assert response.version == 42
        assert json.loads(response.data.decode()) == {"counter": 7, "name": "alice"}
        assert response.schema_version == 2
        assert response.checksum == "deadbeef"
        # Wire contract is lowercase ("hot"/"warm") per gateway.proto:236;
        # both the enum branch and the None fallback now agree (issue #2053).
        assert response.loaded_from == "warm"
        assert response.created_at == int(datetime(2026, 5, 4, 12, 0, tzinfo=UTC).timestamp())
        assert response.updated_at > 0  # set to "now" by the servicer
        assert_set_code_not_called(context)

    @pytest.mark.asyncio
    async def test_loaded_from_hot_lowercased_on_wire(self, service, state_manager, context):
        # Mirrors the WARM happy-path assertion to lock down the second
        # StateTier member as well (issue #2053). Cheap guard against a
        # future enum rename that would silently change the wire value.
        state_manager.load_state.return_value = _make_state_data_obj(loaded_from=StateTier.HOT)
        request = gateway_pb2.LoadStateRequest(strategy_id="strat-1")
        response = await service.LoadState(request, context)
        assert response.loaded_from == "hot"

    @pytest.mark.asyncio
    async def test_loaded_from_falls_back_to_warm_when_none(
        self, service, state_manager, context,
    ):
        state_manager.load_state.return_value = _make_state_data_obj(loaded_from=None)
        # Override the SimpleNamespace default to actually be None on the attribute.
        sd = state_manager.load_state.return_value
        sd.loaded_from = None
        request = gateway_pb2.LoadStateRequest(strategy_id="strat-1")
        response = await service.LoadState(request, context)
        assert response.loaded_from == "warm"

    @pytest.mark.asyncio
    async def test_state_not_found_error_returns_not_found(self, service, state_manager, context):
        state_manager.load_state.side_effect = StateNotFoundError("brand-new strategy")
        request = gateway_pb2.LoadStateRequest(strategy_id="strat-1")
        response = await service.LoadState(request, context)
        assert response.data == b""
        context.set_code.assert_called_once_with(grpc.StatusCode.NOT_FOUND)

    @pytest.mark.asyncio
    async def test_unexpected_exception_returns_internal(self, service, state_manager, context):
        state_manager.load_state.side_effect = RuntimeError("db connection lost")
        request = gateway_pb2.LoadStateRequest(strategy_id="strat-1")
        response = await service.LoadState(request, context)
        assert response.data == b""
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)
        # The exception message is surfaced to the caller via set_details.
        details_args = context.set_details.call_args.args[0]
        assert "db connection lost" in details_args

    @pytest.mark.asyncio
    async def test_agent_id_fallback_to_original_when_resolved_key_missing(
        self, service, state_manager, context, monkeypatch,
    ):
        """In deployed mode (AGENT_ID set), if the platform-resolved key has no
        state, the servicer falls back to the original strategy_id — bridges
        legacy state written before the AGENT_ID normalisation was deployed."""
        monkeypatch.setenv("AGENT_ID", "platform-injected-agent-id")
        # First call (with the resolved AGENT_ID) returns None; second (with original) hits.
        legacy_state = _make_state_data_obj(strategy_id="strat-1", version=1)
        state_manager.load_state.side_effect = [None, legacy_state]

        request = gateway_pb2.LoadStateRequest(strategy_id="strat-1")
        response = await service.LoadState(request, context)

        assert response.strategy_id == "strat-1"
        assert response.version == 1
        # Both keys were tried in order: resolved AGENT_ID first, then original
        # strategy_id as the legacy fallback.
        assert state_manager.load_state.call_count == 2
        calls = state_manager.load_state.call_args_list
        assert calls[0].args[0] == "platform-injected-agent-id"
        assert calls[1].args[0] == "strat-1"
        assert_set_code_not_called(context)


# ──────────────────────────────────────────────────────────────────────────────
# SaveState
# ──────────────────────────────────────────────────────────────────────────────


class TestSaveState:
    @pytest.mark.asyncio
    async def test_invalid_strategy_id_returns_invalid_argument(self, service, state_manager, context):
        request = gateway_pb2.SaveStateRequest(strategy_id="bad id!", data=b"{}")
        response = await service.SaveState(request, context)
        assert_grpc_error(context, response, expected_status=grpc.StatusCode.INVALID_ARGUMENT)
        state_manager.save_state.assert_not_called()

    @pytest.mark.asyncio
    async def test_oversize_state_returns_invalid_argument(self, service, state_manager, context):
        oversize = b"x" * (10 * 1024 * 1024 + 1)  # exceeds typical state-size cap
        request = gateway_pb2.SaveStateRequest(strategy_id="strat-1", data=oversize)
        response = await service.SaveState(request, context)
        assert_grpc_error(context, response, expected_status=grpc.StatusCode.INVALID_ARGUMENT)
        state_manager.save_state.assert_not_called()

    @pytest.mark.asyncio
    async def test_happy_path_returns_new_version_and_checksum(
        self, service, state_manager, context,
    ):
        state_manager.save_state.return_value = SimpleNamespace(version=42, checksum="newhash")
        request = gateway_pb2.SaveStateRequest(
            strategy_id="strat-1",
            expected_version=41,
            data=json.dumps({"counter": 7}).encode(),
            schema_version=2,
        )
        response = await service.SaveState(request, context)

        assert response.success is True
        assert response.new_version == 42
        assert response.checksum == "newhash"
        assert_set_code_not_called(context)

        # Inspect what got passed to the framework: version=41, schema_version=2,
        # state_dict={"counter": 7}.
        call = state_manager.save_state.call_args
        state_arg = call.args[0]
        expected_version_arg = call.args[1]
        assert state_arg.strategy_id == "strat-1"
        assert state_arg.version == 41
        assert state_arg.state == {"counter": 7}
        assert state_arg.schema_version == 2
        # expected_version > 0 → passed through untouched.
        assert expected_version_arg == 41

    @pytest.mark.asyncio
    async def test_expected_version_zero_passes_none_to_framework(
        self, service, state_manager, context,
    ):
        """expected_version=0 means "new state, skip the optimistic check"."""
        state_manager.save_state.return_value = SimpleNamespace(version=1, checksum="h")
        request = gateway_pb2.SaveStateRequest(
            strategy_id="strat-1", expected_version=0, data=b"{}",
        )
        await service.SaveState(request, context)

        # Second positional arg to save_state should be None (no version check).
        assert state_manager.save_state.call_args.args[1] is None

    @pytest.mark.asyncio
    async def test_default_schema_version_is_one(self, service, state_manager, context):
        state_manager.save_state.return_value = SimpleNamespace(version=1, checksum="h")
        # Don't set schema_version on the request → proto defaults to 0 → servicer maps to 1.
        request = gateway_pb2.SaveStateRequest(
            strategy_id="strat-1", expected_version=0, data=b"{}",
        )
        await service.SaveState(request, context)
        assert state_manager.save_state.call_args.args[0].schema_version == 1

    @pytest.mark.asyncio
    async def test_version_conflict_exception_returns_aborted(
        self, service, state_manager, context,
    ):
        state_manager.save_state.side_effect = RuntimeError("version conflict: expected 5, got 6")
        request = gateway_pb2.SaveStateRequest(strategy_id="strat-1", expected_version=5, data=b"{}")
        response = await service.SaveState(request, context)
        assert_grpc_error(
            context,
            response,
            expected_status=grpc.StatusCode.ABORTED,
            error_substring="version conflict",
        )

    @pytest.mark.asyncio
    async def test_conflict_keyword_alone_also_maps_to_aborted(
        self, service, state_manager, context,
    ):
        # "conflict" keyword alone (without "version") still trips the ABORTED branch.
        state_manager.save_state.side_effect = RuntimeError("write conflict on row")
        request = gateway_pb2.SaveStateRequest(strategy_id="strat-1", expected_version=5, data=b"{}")
        response = await service.SaveState(request, context)
        assert_grpc_error(
            context,
            response,
            expected_status=grpc.StatusCode.ABORTED,
            error_substring="conflict",
        )

    @pytest.mark.asyncio
    async def test_other_exception_returns_internal(self, service, state_manager, context):
        state_manager.save_state.side_effect = RuntimeError("disk full")
        request = gateway_pb2.SaveStateRequest(strategy_id="strat-1", expected_version=5, data=b"{}")
        response = await service.SaveState(request, context)
        assert_grpc_error(
            context,
            response,
            expected_status=grpc.StatusCode.INTERNAL,
            error_substring="disk full",
        )


# ──────────────────────────────────────────────────────────────────────────────
# DeleteState
# ──────────────────────────────────────────────────────────────────────────────


class TestDeleteState:
    @pytest.mark.asyncio
    async def test_invalid_strategy_id_returns_invalid_argument(self, service, state_manager, context):
        request = gateway_pb2.DeleteStateRequest(strategy_id="bad id!")
        response = await service.DeleteState(request, context)
        assert_grpc_error(context, response, expected_status=grpc.StatusCode.INVALID_ARGUMENT)
        state_manager.delete_state.assert_not_called()

    @pytest.mark.asyncio
    async def test_happy_path_returns_success(self, service, state_manager, context):
        state_manager.delete_state.return_value = True
        request = gateway_pb2.DeleteStateRequest(strategy_id="strat-1")
        response = await service.DeleteState(request, context)
        assert response.success is True
        assert_set_code_not_called(context)

    @pytest.mark.asyncio
    async def test_state_not_found_returns_success_false_no_grpc_code(
        self, service, state_manager, context,
    ):
        """Not-found is a soft response — the servicer returns success=False
        in the proto but does NOT set a gRPC status code (that would surface
        as an exception on the client; semantically this is "nothing to delete",
        not an error)."""
        state_manager.delete_state.return_value = False
        request = gateway_pb2.DeleteStateRequest(strategy_id="strat-1")
        response = await service.DeleteState(request, context)
        assert response.success is False
        assert "not found" in response.error.lower()
        assert_set_code_not_called(context)

    @pytest.mark.asyncio
    async def test_unexpected_exception_returns_internal(self, service, state_manager, context):
        state_manager.delete_state.side_effect = RuntimeError("db down")
        request = gateway_pb2.DeleteStateRequest(strategy_id="strat-1")
        response = await service.DeleteState(request, context)
        assert_grpc_error(
            context,
            response,
            expected_status=grpc.StatusCode.INTERNAL,
            error_substring="db down",
        )


# ──────────────────────────────────────────────────────────────────────────────
# _ensure_initialized — backend selection (SQLite vs PostgreSQL)
# ──────────────────────────────────────────────────────────────────────────────


class TestEnsureInitialized:
    @pytest.mark.asyncio
    async def test_postgresql_backend_chosen_when_database_url_set(self):
        svc = StateServiceServicer(_make_settings(database_url="postgres://localhost/db"))
        with patch("almanak.framework.state.state_manager.StateManager") as fake_sm_cls:
            fake_sm = AsyncMock()
            fake_sm_cls.return_value = fake_sm
            await svc._ensure_initialized()
            assert svc._initialized is True
            fake_sm.initialize.assert_awaited_once()
            # Config should have specified PostgreSQL.
            cfg = fake_sm_cls.call_args.args[0]
            assert cfg.database_url == "postgres://localhost/db"

    @pytest.mark.asyncio
    async def test_idempotent_when_already_initialized(self):
        svc = StateServiceServicer(_make_settings())
        svc._initialized = True
        # Should not import StateManager or call initialize again.
        with patch("almanak.framework.state.state_manager.StateManager") as fake_sm_cls:
            await svc._ensure_initialized()
            fake_sm_cls.assert_not_called()


# ──────────────────────────────────────────────────────────────────────────────
# Portfolio snapshot fixtures + helpers
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def warm_backend() -> AsyncMock:
    """Mock SQLite warm backend with the snapshot methods state_service calls."""
    wb = AsyncMock()
    wb.save_portfolio_snapshot.return_value = 99
    wb.get_latest_snapshot.return_value = None
    wb.get_snapshots_since.return_value = []
    return wb


@pytest.fixture
def sqlite_service(state_manager: AsyncMock, warm_backend: AsyncMock) -> StateServiceServicer:
    """Servicer in SQLite mode (no _snapshot_pool)."""
    svc = StateServiceServicer(_make_settings())
    svc._state_manager = state_manager
    state_manager.warm_backend = warm_backend
    svc._initialized = True
    svc._snapshot_pool_initialized = True
    svc._snapshot_pool = None
    return svc


@pytest.fixture
def pg_service(state_manager: AsyncMock) -> StateServiceServicer:
    """Servicer in PostgreSQL mode (_snapshot_pool present, helpers patchable)."""
    svc = StateServiceServicer(_make_settings(database_url="postgres://x/y"))
    svc._state_manager = state_manager
    svc._initialized = True
    svc._snapshot_pool_initialized = True
    # Just needs to be non-None; the test patches the snapshot helpers anyway.
    svc._snapshot_pool = MagicMock()
    return svc


def _make_save_snapshot_request(
    *,
    strategy_id: str = "strat-1",
    timestamp: int = 1_725_000_000,
    iteration_number: int = 1,
    total_value_usd: str = "1000.00",
    available_cash_usd: str = "500.00",
    value_confidence: str = "HIGH",
    positions_json: bytes = b"[]",
    chain: str = "arbitrum",
) -> gateway_pb2.SaveSnapshotRequest:
    return gateway_pb2.SaveSnapshotRequest(
        strategy_id=strategy_id,
        timestamp=timestamp,
        iteration_number=iteration_number,
        total_value_usd=total_value_usd,
        available_cash_usd=available_cash_usd,
        value_confidence=value_confidence,
        positions_json=positions_json,
        chain=chain,
    )


# ──────────────────────────────────────────────────────────────────────────────
# SavePortfolioSnapshot
# ──────────────────────────────────────────────────────────────────────────────


class TestSavePortfolioSnapshot:
    @pytest.mark.asyncio
    async def test_invalid_strategy_id_returns_invalid_argument(self, sqlite_service, warm_backend, context):
        request = _make_save_snapshot_request(strategy_id="bad id!")
        response = await sqlite_service.SavePortfolioSnapshot(request, context)
        assert response.success is False
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        warm_backend.save_portfolio_snapshot.assert_not_called()

    @pytest.mark.asyncio
    async def test_zero_timestamp_returns_invalid_argument(self, sqlite_service, warm_backend, context):
        request = _make_save_snapshot_request(timestamp=0)
        response = await sqlite_service.SavePortfolioSnapshot(request, context)
        assert response.success is False
        assert "timestamp must be positive" in response.error
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_malformed_positions_json_rejected(self, sqlite_service, warm_backend, context):
        request = _make_save_snapshot_request(positions_json=b"not-json")
        response = await sqlite_service.SavePortfolioSnapshot(request, context)
        assert response.success is False
        assert "valid JSON" in response.error
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_envelope_shape_validation_rejects_scalar(self, sqlite_service, warm_backend, context):
        # Valid JSON but wrong shape (must be list or {positions, metadata}).
        request = _make_save_snapshot_request(positions_json=b'"oops"')
        response = await sqlite_service.SavePortfolioSnapshot(request, context)
        assert response.success is False
        assert "must be a list or" in response.error

    @pytest.mark.asyncio
    async def test_legacy_list_envelope_accepted(self, sqlite_service, warm_backend, context):
        # Legacy: positions_json is a bare list. Empty so we don't trip on
        # PortfolioSnapshot.from_dict's per-position field validation.
        request = _make_save_snapshot_request(
            positions_json=json.dumps([]).encode(),
        )
        response = await sqlite_service.SavePortfolioSnapshot(request, context)
        assert response.success is True
        assert response.snapshot_id == 99

    @pytest.mark.asyncio
    async def test_envelope_with_positions_and_metadata_accepted(self, sqlite_service, warm_backend, context):
        envelope = {"positions": [], "metadata": {"version": 2}}
        request = _make_save_snapshot_request(positions_json=json.dumps(envelope).encode())
        response = await sqlite_service.SavePortfolioSnapshot(request, context)
        assert response.success is True

    @pytest.mark.asyncio
    async def test_sqlite_backend_exception_returns_internal(
        self, sqlite_service, warm_backend, context,
    ):
        warm_backend.save_portfolio_snapshot.side_effect = RuntimeError("disk full")
        request = _make_save_snapshot_request()
        response = await sqlite_service.SavePortfolioSnapshot(request, context)
        assert response.success is False
        assert response.error == "internal server error"
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)

    @pytest.mark.asyncio
    async def test_pg_path_inserts_and_returns_id(self, pg_service, context):
        # Patch _snapshot_fetchrow to return the inserted row.
        async def _fake_fetchrow(query, *args):
            return {"id": 42}

        with patch.object(pg_service, "_snapshot_fetchrow", new=AsyncMock(side_effect=_fake_fetchrow)) as fake:
            response = await pg_service.SavePortfolioSnapshot(_make_save_snapshot_request(), context)
        assert response.success is True
        assert response.snapshot_id == 42
        fake.assert_awaited_once()
        # Verify the INSERT was issued with strategy_id as agent_id (first param).
        query = fake.call_args.args[0]
        assert "INSERT INTO portfolio_snapshots" in query
        assert "ON CONFLICT" in query

    @pytest.mark.asyncio
    async def test_pg_path_exception_returns_internal(self, pg_service, context):
        with patch.object(pg_service, "_snapshot_fetchrow", new=AsyncMock(side_effect=RuntimeError("pg down"))):
            response = await pg_service.SavePortfolioSnapshot(_make_save_snapshot_request(), context)
        assert response.success is False
        assert response.error == "internal server error"
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)


# ──────────────────────────────────────────────────────────────────────────────
# GetLatestSnapshot
# ──────────────────────────────────────────────────────────────────────────────


class TestGetLatestSnapshot:
    @pytest.mark.asyncio
    async def test_invalid_strategy_id_returns_invalid_argument(self, sqlite_service, warm_backend, context):
        request = gateway_pb2.GetLatestSnapshotRequest(strategy_id="bad id!")
        response = await sqlite_service.GetLatestSnapshot(request, context)
        assert response.found is False
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)
        warm_backend.get_latest_snapshot.assert_not_called()

    @pytest.mark.asyncio
    async def test_sqlite_no_snapshot_returns_found_false(self, sqlite_service, warm_backend, context):
        warm_backend.get_latest_snapshot.return_value = None
        request = gateway_pb2.GetLatestSnapshotRequest(strategy_id="strat-1")
        response = await sqlite_service.GetLatestSnapshot(request, context)
        assert response.found is False
        assert_set_code_not_called(context)

    @pytest.mark.asyncio
    async def test_sqlite_happy_path(self, sqlite_service, warm_backend, context):
        from decimal import Decimal

        from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
        snapshot = PortfolioSnapshot(
            timestamp=datetime(2026, 5, 4, 12, 0, tzinfo=UTC),
            strategy_id="strat-1",
            total_value_usd=Decimal("1234.56"),
            available_cash_usd=Decimal("500"),
            value_confidence=ValueConfidence.HIGH,
            chain="arbitrum",
            iteration_number=7,
        )
        warm_backend.get_latest_snapshot.return_value = snapshot
        request = gateway_pb2.GetLatestSnapshotRequest(strategy_id="strat-1")
        response = await sqlite_service.GetLatestSnapshot(request, context)
        assert response.found is True
        assert response.strategy_id == "strat-1"
        assert response.total_value_usd == "1234.56"
        assert response.iteration_number == 7
        assert response.chain == "arbitrum"
        assert_set_code_not_called(context)

    @pytest.mark.asyncio
    async def test_sqlite_exception_returns_internal(self, sqlite_service, warm_backend, context):
        warm_backend.get_latest_snapshot.side_effect = RuntimeError("db down")
        request = gateway_pb2.GetLatestSnapshotRequest(strategy_id="strat-1")
        response = await sqlite_service.GetLatestSnapshot(request, context)
        assert response.found is False
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)

    # PG-vs-SQLite read-path tests removed post-rebase: PR #2024 (`refactor(state-service):
    # delegate snapshot read RPCs to PostgresStore`) consolidated GetLatestSnapshot to a
    # single warm-backend dispatch. The SQLite happy/exception/no-snapshot tests above now
    # cover both deployment modes (the warm backend is the dispatch point in either case).


# ──────────────────────────────────────────────────────────────────────────────
# GetSnapshotsSince
# ──────────────────────────────────────────────────────────────────────────────


class TestGetSnapshotsSince:
    @pytest.mark.asyncio
    async def test_invalid_strategy_id_returns_invalid_argument(self, sqlite_service, warm_backend, context):
        request = gateway_pb2.GetSnapshotsSinceRequest(strategy_id="bad id!", since=0, limit=10)
        response = await sqlite_service.GetSnapshotsSince(request, context)
        assert response.snapshots == []
        context.set_code.assert_called_once_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_default_limit_when_zero(self, sqlite_service, warm_backend, context):
        warm_backend.get_snapshots_since.return_value = []
        request = gateway_pb2.GetSnapshotsSinceRequest(strategy_id="strat-1", since=0, limit=0)
        await sqlite_service.GetSnapshotsSince(request, context)
        # limit=0 → default of 168.
        call = warm_backend.get_snapshots_since.call_args
        assert call.args[2] == 168

    @pytest.mark.asyncio
    async def test_limit_capped_at_max_snapshots(self, sqlite_service, warm_backend, context):
        warm_backend.get_snapshots_since.return_value = []
        # Request 5000 → capped at MAX_SNAPSHOTS = 1000.
        request = gateway_pb2.GetSnapshotsSinceRequest(strategy_id="strat-1", since=0, limit=5000)
        await sqlite_service.GetSnapshotsSince(request, context)
        call = warm_backend.get_snapshots_since.call_args
        assert call.args[2] == 1000

    @pytest.mark.asyncio
    async def test_sqlite_happy_path_returns_snapshots(self, sqlite_service, warm_backend, context):
        from decimal import Decimal

        from almanak.framework.portfolio.models import PortfolioSnapshot, ValueConfidence
        warm_backend.get_snapshots_since.return_value = [
            PortfolioSnapshot(
                timestamp=datetime(2026, 5, 4, 12, i, tzinfo=UTC),
                strategy_id="strat-1",
                total_value_usd=Decimal(str(1000 + i)),
                available_cash_usd=Decimal("0"),
                value_confidence=ValueConfidence.HIGH,
                chain="arbitrum",
                iteration_number=i,
            )
            for i in range(3)
        ]
        request = gateway_pb2.GetSnapshotsSinceRequest(strategy_id="strat-1", since=0, limit=10)
        response = await sqlite_service.GetSnapshotsSince(request, context)
        assert len(response.snapshots) == 3
        assert response.snapshots[0].total_value_usd == "1000"

    @pytest.mark.asyncio
    async def test_sqlite_exception_returns_empty(self, sqlite_service, warm_backend, context):
        warm_backend.get_snapshots_since.side_effect = RuntimeError("db down")
        request = gateway_pb2.GetSnapshotsSinceRequest(strategy_id="strat-1", since=0, limit=10)
        response = await sqlite_service.GetSnapshotsSince(request, context)
        assert response.snapshots == []
        context.set_code.assert_called_once_with(grpc.StatusCode.INTERNAL)

    # PG-vs-SQLite read-path tests removed post-rebase: PR #2024 (`refactor(state-service):
    # delegate snapshot read RPCs to PostgresStore`) consolidated GetSnapshotsSince to a
    # single warm-backend dispatch. The SQLite happy/exception tests above now cover
    # both deployment modes (the warm backend is the dispatch point in either case).
