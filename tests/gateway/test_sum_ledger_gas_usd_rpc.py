"""Tests for the SumLedgerGasUsd StateService RPC (VIB-4247)."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.framework.state.exceptions import AccountingPersistenceError
from almanak.framework.state.gateway_state_manager import GatewayStateManager
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer


class _FakeRpcError(grpc.RpcError):
    def __init__(self, code: grpc.StatusCode) -> None:
        super().__init__()
        self._code = code

    def code(self) -> grpc.StatusCode:
        return self._code


@pytest.fixture
def mock_context() -> MagicMock:
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


@pytest.fixture
def sqlite_service() -> StateServiceServicer:
    svc = StateServiceServicer(GatewaySettings())
    svc._initialized = True
    svc._snapshot_pool_initialized = True
    svc._snapshot_pool = None
    return svc


@pytest.fixture
def pg_service() -> StateServiceServicer:
    svc = StateServiceServicer(GatewaySettings(database_url="postgres://example/metrics"))
    svc._initialized = True
    svc._snapshot_pool_initialized = True
    svc._snapshot_pool = MagicMock()
    svc._ensure_snapshot_pool = AsyncMock()
    svc._snapshot_fetchrow = AsyncMock(return_value={"total": Decimal("0.0059")})
    return svc


class TestSumLedgerGasUsdServiceValidation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("deployment_id", ["", "  "])
    async def test_missing_deployment_id(self, sqlite_service, mock_context, deployment_id):
        req = gateway_pb2.SumLedgerGasUsdRequest(deployment_id=deployment_id)

        resp = await sqlite_service.SumLedgerGasUsd(req, mock_context)

        assert resp.success is False
        assert resp.error == "deployment_id is required"
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_invalid_deployment_id(self, sqlite_service, mock_context):
        req = gateway_pb2.SumLedgerGasUsdRequest(deployment_id="../bad")

        resp = await sqlite_service.SumLedgerGasUsd(req, mock_context)

        assert resp.success is False
        assert "invalid format" in resp.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


class TestSumLedgerGasUsdPostgres:
    @pytest.mark.asyncio
    async def test_pg_sums_by_deployment_id_only(self, pg_service, mock_context):
        """Blueprint 29 §4: the query filters the single deployment_id column.

        VIB-4722 removed the deployment_id translation and the legacy
        ``deployment_id = ''`` fallback — one identity column, one param.
        """
        req = gateway_pb2.SumLedgerGasUsdRequest(deployment_id="Strategy:local")

        resp = await pg_service.SumLedgerGasUsd(req, mock_context)

        assert resp.success is True
        assert resp.gas_usd_total == "0.0059"
        pg_service._snapshot_fetchrow.assert_awaited_once()
        sql, deployment_id = pg_service._snapshot_fetchrow.await_args.args
        assert "BTRIM(gas_usd)" in sql
        assert "::numeric" in sql
        assert "[eE][+-]?[0-9]+" in sql
        assert "WHERE deployment_id = $1" in sql
        assert "strategy_id" not in sql
        assert "agent_id" not in sql
        assert deployment_id == "Strategy:local"

    @pytest.mark.asyncio
    async def test_pg_no_rows_returns_zero(self, pg_service, mock_context):
        pg_service._snapshot_fetchrow = AsyncMock(return_value=None)

        resp = await pg_service.SumLedgerGasUsd(
            gateway_pb2.SumLedgerGasUsdRequest(deployment_id="demo"),
            mock_context,
        )

        assert resp.success is True
        assert resp.gas_usd_total == "0"

    @pytest.mark.asyncio
    async def test_pg_error_is_sanitized(self, pg_service, mock_context):
        pg_service._snapshot_fetchrow = AsyncMock(side_effect=RuntimeError("password leaked in dsn"))

        resp = await pg_service.SumLedgerGasUsd(
            gateway_pb2.SumLedgerGasUsdRequest(deployment_id="demo"),
            mock_context,
        )

        assert resp.success is False
        assert resp.error == "internal server error"
        mock_context.set_code.assert_called_with(grpc.StatusCode.INTERNAL)
        mock_context.set_details.assert_called_with("internal server error")


class TestSumLedgerGasUsdSqlite:
    @pytest.mark.asyncio
    async def test_sqlite_delegates_to_warm_backend(self, sqlite_service, mock_context):
        warm = MagicMock()
        warm.sum_ledger_gas_usd = AsyncMock(return_value=Decimal("0.42"))
        sqlite_service._state_manager = MagicMock(warm_backend=warm)

        resp = await sqlite_service.SumLedgerGasUsd(
            gateway_pb2.SumLedgerGasUsdRequest(deployment_id="demo"),
            mock_context,
        )

        assert resp.success is True
        assert resp.gas_usd_total == "0.42"
        # One identity (blueprint 29 §4): the warm backend is called with the
        # single canonical deployment_id — no second deployment_id argument.
        warm.sum_ledger_gas_usd.assert_awaited_once_with("demo")


class TestGatewayStateManagerSumLedgerGasUsd:
    @pytest.mark.asyncio
    async def test_calls_gateway_rpc(self):
        state_stub = MagicMock()
        state_stub.SumLedgerGasUsd.return_value = gateway_pb2.SumLedgerGasUsdResponse(
            success=True,
            gas_usd_total="0.1234",
        )
        client = MagicMock(state=state_stub)
        manager = GatewayStateManager(client)

        total = await manager.sum_ledger_gas_usd("dep-A")

        assert total == Decimal("0.1234")
        state_stub.SumLedgerGasUsd.assert_called_once()
        request = state_stub.SumLedgerGasUsd.call_args.args[0]
        assert request.deployment_id == "dep-A"

    @pytest.mark.asyncio
    async def test_response_failure_raises_accounting_persistence_error(self):
        state_stub = MagicMock()
        state_stub.SumLedgerGasUsd.return_value = gateway_pb2.SumLedgerGasUsdResponse(
            success=False,
            error="internal server error",
        )
        manager = GatewayStateManager(MagicMock(state=state_stub))

        with pytest.raises(AccountingPersistenceError) as excinfo:
            await manager.sum_ledger_gas_usd("dep-A")

        assert excinfo.value.write_kind == "metrics"
        assert excinfo.value.deployment_id == "dep-A"
        assert "internal server error" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_old_gateway_unimplemented_maps_to_not_implemented(self):
        state_stub = MagicMock()
        state_stub.SumLedgerGasUsd.side_effect = _FakeRpcError(grpc.StatusCode.UNIMPLEMENTED)
        manager = GatewayStateManager(MagicMock(state=state_stub))

        with pytest.raises(NotImplementedError):
            await manager.sum_ledger_gas_usd("dep-A")

    @pytest.mark.asyncio
    async def test_invalid_decimal_raises_accounting_persistence_error(self):
        state_stub = MagicMock()
        state_stub.SumLedgerGasUsd.return_value = gateway_pb2.SumLedgerGasUsdResponse(
            success=True,
            gas_usd_total="not-a-decimal",
        )
        manager = GatewayStateManager(MagicMock(state=state_stub))

        with pytest.raises(AccountingPersistenceError) as excinfo:
            await manager.sum_ledger_gas_usd("dep-A")

        assert excinfo.value.write_kind == "metrics"
        assert excinfo.value.deployment_id == "dep-A"
