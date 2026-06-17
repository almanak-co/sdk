"""Tests for SavePortfolioMetrics and GetPortfolioMetrics gRPC endpoints.

Verifies that:
- SavePortfolioMetrics delegates to the warm backend
- GetPortfolioMetrics returns data in the correct proto format
- Error handling works for missing deployment_id and backend failures
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.state_service import StateServiceServicer


@pytest.fixture
def settings():
    return GatewaySettings()


@pytest.fixture
def mock_context():
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


@pytest.fixture
def state_service(settings):
    svc = StateServiceServicer(settings)
    svc._initialized = True
    return svc


class TestSavePortfolioMetrics:
    """Tests for SavePortfolioMetrics RPC."""

    @pytest.mark.asyncio
    async def test_save_metrics_success(self, state_service, mock_context):
        """SavePortfolioMetrics persists via warm backend."""
        mock_warm = AsyncMock()
        mock_warm.save_portfolio_metrics = AsyncMock(return_value=True)
        mock_sm = MagicMock()
        mock_sm.warm_backend = mock_warm
        state_service._state_manager = mock_sm

        request = gateway_pb2.SaveMetricsRequest(
            deployment_id="test-strategy",
            initial_value_usd="10000.50",
            initial_timestamp=1712000000,
            deposits_usd="500.00",
            withdrawals_usd="100.00",
            gas_spent_usd="25.00",
        )
        response = await state_service.SavePortfolioMetrics(request, mock_context)

        assert response.success is True
        assert response.error == ""
        mock_warm.save_portfolio_metrics.assert_called_once()

        saved_metrics = mock_warm.save_portfolio_metrics.call_args[0][0]
        assert saved_metrics.deployment_id == "test-strategy"
        assert saved_metrics.initial_value_usd == Decimal("10000.50")
        assert saved_metrics.deposits_usd == Decimal("500.00")
        assert saved_metrics.withdrawals_usd == Decimal("100.00")
        assert saved_metrics.gas_spent_usd == Decimal("25.00")

    @pytest.mark.parametrize("deployment_id", ["", "   "])
    @pytest.mark.asyncio
    async def test_save_metrics_missing_deployment_id(self, state_service, mock_context, deployment_id):
        """SavePortfolioMetrics rejects empty or whitespace-only deployment_id."""
        request = gateway_pb2.SaveMetricsRequest(deployment_id=deployment_id)
        response = await state_service.SavePortfolioMetrics(request, mock_context)

        assert response.success is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_save_metrics_no_warm_backend(self, state_service, mock_context):
        """SavePortfolioMetrics fails gracefully with no warm backend."""
        mock_sm = MagicMock()
        mock_sm.warm_backend = None
        state_service._state_manager = mock_sm

        request = gateway_pb2.SaveMetricsRequest(
            deployment_id="test-strategy",
            initial_value_usd="10000",
        )
        response = await state_service.SavePortfolioMetrics(request, mock_context)

        assert response.success is False
        assert "No warm backend" in response.error

    @pytest.mark.asyncio
    async def test_save_metrics_backend_failure(self, state_service, mock_context):
        """SavePortfolioMetrics handles backend exceptions."""
        mock_warm = AsyncMock()
        mock_warm.save_portfolio_metrics = AsyncMock(side_effect=RuntimeError("DB connection lost"))
        mock_sm = MagicMock()
        mock_sm.warm_backend = mock_warm
        state_service._state_manager = mock_sm

        request = gateway_pb2.SaveMetricsRequest(
            deployment_id="test-strategy",
            initial_value_usd="10000",
        )
        response = await state_service.SavePortfolioMetrics(request, mock_context)

        assert response.success is False
        assert response.error == "internal server error"
        mock_context.set_code.assert_called_with(grpc.StatusCode.INTERNAL)
        mock_context.set_details.assert_called_with("internal server error")


    @pytest.mark.asyncio
    async def test_save_metrics_invalid_decimal(self, state_service, mock_context):
        """SavePortfolioMetrics rejects malformed decimal strings."""
        mock_sm = MagicMock()
        mock_sm.warm_backend = None
        state_service._state_manager = mock_sm

        request = gateway_pb2.SaveMetricsRequest(
            deployment_id="test-strategy",
            initial_value_usd="not-a-number",
        )
        response = await state_service.SavePortfolioMetrics(request, mock_context)

        assert response.success is False
        assert "valid decimal" in response.error
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)


class TestGetPortfolioMetrics:
    """Tests for GetPortfolioMetrics RPC."""

    @pytest.mark.asyncio
    async def test_get_metrics_success(self, state_service, mock_context):
        """GetPortfolioMetrics returns data from warm backend."""
        from almanak.framework.portfolio.models import PortfolioMetrics

        mock_metrics = PortfolioMetrics(
            deployment_id="test-strategy",
            timestamp=datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC),
            total_value_usd=Decimal("12000"),
            initial_value_usd=Decimal("10000"),
            deposits_usd=Decimal("500"),
            withdrawals_usd=Decimal("100"),
            gas_spent_usd=Decimal("25"),
        )

        mock_warm = AsyncMock()
        mock_warm.get_portfolio_metrics = AsyncMock(return_value=mock_metrics)
        mock_sm = MagicMock()
        mock_sm.warm_backend = mock_warm
        state_service._state_manager = mock_sm

        request = gateway_pb2.GetMetricsRequest(deployment_id="test-strategy")
        response = await state_service.GetPortfolioMetrics(request, mock_context)

        assert response.found is True
        assert response.deployment_id == "test-strategy"
        assert response.initial_value_usd == "10000"
        assert response.deposits_usd == "500"
        assert response.withdrawals_usd == "100"
        assert response.gas_spent_usd == "25"
        assert response.updated_at > 0

    @pytest.mark.asyncio
    async def test_get_metrics_not_found(self, state_service, mock_context):
        """GetPortfolioMetrics returns found=False when no metrics exist."""
        mock_warm = AsyncMock()
        mock_warm.get_portfolio_metrics = AsyncMock(return_value=None)
        mock_sm = MagicMock()
        mock_sm.warm_backend = mock_warm
        state_service._state_manager = mock_sm

        request = gateway_pb2.GetMetricsRequest(deployment_id="nonexistent")
        response = await state_service.GetPortfolioMetrics(request, mock_context)

        assert response.found is False

    @pytest.mark.parametrize("deployment_id", ["", "   "])
    @pytest.mark.asyncio
    async def test_get_metrics_missing_deployment_id(self, state_service, mock_context, deployment_id):
        """GetPortfolioMetrics rejects empty or whitespace-only deployment_id."""
        request = gateway_pb2.GetMetricsRequest(deployment_id=deployment_id)
        response = await state_service.GetPortfolioMetrics(request, mock_context)

        assert response.found is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_get_metrics_backend_failure(self, state_service, mock_context):
        """GetPortfolioMetrics handles backend exceptions."""
        mock_warm = AsyncMock()
        mock_warm.get_portfolio_metrics = AsyncMock(side_effect=RuntimeError("DB error"))
        mock_sm = MagicMock()
        mock_sm.warm_backend = mock_warm
        state_service._state_manager = mock_sm

        request = gateway_pb2.GetMetricsRequest(deployment_id="test-strategy")
        response = await state_service.GetPortfolioMetrics(request, mock_context)

        assert response.found is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INTERNAL)
        mock_context.set_details.assert_called_with("internal server error")

    @pytest.mark.asyncio
    async def test_get_metrics_pg_success(self, state_service, mock_context):
        """GetPortfolioMetrics returns data from the hosted Postgres path."""
        updated_at = datetime(2026, 4, 1, 12, 30, 0, tzinfo=UTC)
        initial_ts = datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC)
        state_service._snapshot_pool = object()
        state_service._ensure_snapshot_pool = AsyncMock()
        state_service._snapshot_fetchrow = AsyncMock(
            return_value={
                "initial_value_usd": "10000",
                "initial_timestamp": initial_ts,
                "deposits_usd": "500",
                "withdrawals_usd": "100",
                "gas_spent_usd": "25",
                "updated_at": updated_at,
                "deployment_id": "test-strategy",
                "cycle_id": "cycle-1",
                "execution_mode": "paper",
                "is_complete": False,
            }
        )

        response = await state_service.GetPortfolioMetrics(
            gateway_pb2.GetMetricsRequest(deployment_id="test-strategy"),
            mock_context,
        )

        assert response.found is True
        assert response.deployment_id == "test-strategy"
        assert response.initial_timestamp == int(initial_ts.timestamp())
        assert response.updated_at == int(updated_at.timestamp())
        assert response.cycle_id == "cycle-1"
        assert response.execution_mode == "paper"
        assert response.is_complete is False
        state_service._snapshot_fetchrow.assert_awaited_once()  # type: ignore[attr-defined]

    @pytest.mark.asyncio
    async def test_get_metrics_pg_not_found(self, state_service, mock_context):
        """Postgres row miss maps to found=False."""
        state_service._snapshot_pool = object()
        state_service._ensure_snapshot_pool = AsyncMock()
        state_service._snapshot_fetchrow = AsyncMock(return_value=None)

        response = await state_service.GetPortfolioMetrics(
            gateway_pb2.GetMetricsRequest(deployment_id="missing"),
            mock_context,
        )

        assert response.found is False
        mock_context.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_metrics_pg_backend_failure(self, state_service, mock_context):
        """Postgres read failure maps to INTERNAL + found=False."""
        state_service._snapshot_pool = object()
        state_service._ensure_snapshot_pool = AsyncMock()
        state_service._snapshot_fetchrow = AsyncMock(side_effect=RuntimeError("pg down"))

        response = await state_service.GetPortfolioMetrics(
            gateway_pb2.GetMetricsRequest(deployment_id="test-strategy"),
            mock_context,
        )

        assert response.found is False
        mock_context.set_code.assert_called_with(grpc.StatusCode.INTERNAL)
        mock_context.set_details.assert_called_with("internal server error")


class TestGatewayStateManagerMetrics:
    """Tests for GatewayStateManager portfolio metrics methods."""

    @pytest.mark.asyncio
    async def test_save_metrics_via_gateway_client(self):
        """GatewayStateManager.save_portfolio_metrics calls gRPC."""
        from almanak.framework.portfolio.models import PortfolioMetrics
        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        mock_client = MagicMock()
        mock_state_stub = MagicMock()
        mock_state_stub.SavePortfolioMetrics.return_value = gateway_pb2.SaveMetricsResponse(success=True)
        mock_client.state = mock_state_stub

        gsm = GatewayStateManager(mock_client)

        metrics = PortfolioMetrics(
            deployment_id="test-strategy",
            timestamp=datetime(2026, 4, 1, 12, 0, 0, tzinfo=UTC),
            total_value_usd=Decimal("12000"),
            initial_value_usd=Decimal("10000"),
            deposits_usd=Decimal("500"),
            withdrawals_usd=Decimal("100"),
            gas_spent_usd=Decimal("25"),
        )

        result = await gsm.save_portfolio_metrics(metrics)
        assert result is True
        mock_state_stub.SavePortfolioMetrics.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_metrics_via_gateway_client(self):
        """GatewayStateManager.get_portfolio_metrics calls gRPC."""
        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        mock_client = MagicMock()
        mock_state_stub = MagicMock()
        mock_state_stub.GetPortfolioMetrics.return_value = gateway_pb2.PortfolioMetricsData(
            deployment_id="test-strategy",
            initial_value_usd="10000",
            initial_timestamp=1712000000,
            deposits_usd="500",
            withdrawals_usd="100",
            gas_spent_usd="25",
            updated_at=1712000000,
            found=True,
        )
        mock_client.state = mock_state_stub

        gsm = GatewayStateManager(mock_client)
        result = await gsm.get_portfolio_metrics("test-strategy")

        assert result is not None
        assert result.deployment_id == "test-strategy"
        assert result.initial_value_usd == Decimal("10000")
        assert result.deposits_usd == Decimal("500")
        mock_state_stub.GetPortfolioMetrics.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_metrics_not_found_via_gateway(self):
        """GatewayStateManager.get_portfolio_metrics returns None when not found."""
        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        mock_client = MagicMock()
        mock_state_stub = MagicMock()
        mock_state_stub.GetPortfolioMetrics.return_value = gateway_pb2.PortfolioMetricsData(found=False)
        mock_client.state = mock_state_stub

        gsm = GatewayStateManager(mock_client)
        result = await gsm.get_portfolio_metrics("nonexistent")

        assert result is None

    @pytest.mark.asyncio
    async def test_save_metrics_gateway_failure(self):
        """GatewayStateManager.save_portfolio_metrics raises on gRPC failures.

        VIB-3157: the legacy ``return False`` swallow-on-failure contract was
        a silent accounting-loss footgun. Failures now propagate so the
        runner can halt the cycle and alert the operator.

        Exercises the transport-exception branch *and* asserts the typed
        ``write_kind`` / ``deployment_id`` metadata on the raised exception.
        """
        from almanak.framework.portfolio.models import PortfolioMetrics
        from almanak.framework.state.exceptions import AccountingPersistenceError
        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        mock_client = MagicMock()
        mock_state_stub = MagicMock()
        mock_state_stub.SavePortfolioMetrics.side_effect = Exception("gRPC unavailable")
        mock_client.state = mock_state_stub

        gsm = GatewayStateManager(mock_client)
        metrics = PortfolioMetrics(
            deployment_id="test",
            timestamp=datetime.now(UTC),
            total_value_usd=Decimal("0"),
            initial_value_usd=Decimal("10000"),
        )

        with pytest.raises(AccountingPersistenceError) as excinfo:
            await gsm.save_portfolio_metrics(metrics)

        # Use public ``cause`` attribute, not ``__cause__`` dunder — public API
        # is part of AccountingPersistenceError's contract; __cause__ couples
        # the test to Python's ``raise X from Y`` implementation detail.
        assert "gRPC unavailable" in str(excinfo.value) or excinfo.value.cause is not None
        assert excinfo.value.write_kind == "metrics"
        assert excinfo.value.deployment_id == "test"

    @pytest.mark.asyncio
    async def test_save_metrics_response_failure(self):
        """GatewayStateManager.save_portfolio_metrics raises on response.success=False.

        Complements ``test_save_metrics_gateway_failure``: covers the
        gateway-returned-failure branch (no transport exception) that the
        earlier "returns False" contract used to hide.
        """
        from almanak.framework.portfolio.models import PortfolioMetrics
        from almanak.framework.state.exceptions import AccountingPersistenceError
        from almanak.framework.state.gateway_state_manager import GatewayStateManager

        mock_client = MagicMock()
        mock_state_stub = MagicMock()
        mock_response = gateway_pb2.SaveMetricsResponse(success=False, error="backend rejected metrics")
        mock_state_stub.SavePortfolioMetrics.return_value = mock_response
        mock_client.state = mock_state_stub

        gsm = GatewayStateManager(mock_client)
        metrics = PortfolioMetrics(
            deployment_id="test",
            timestamp=datetime.now(UTC),
            total_value_usd=Decimal("0"),
            initial_value_usd=Decimal("10000"),
        )

        with pytest.raises(AccountingPersistenceError) as excinfo:
            await gsm.save_portfolio_metrics(metrics)

        assert excinfo.value.write_kind == "metrics"
        assert excinfo.value.deployment_id == "test"
        assert "backend rejected metrics" in str(excinfo.value)
