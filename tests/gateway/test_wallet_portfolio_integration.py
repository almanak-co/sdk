"""Tests for wallet portfolio gRPC handlers in IntegrationServiceServicer."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import grpc
import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.integrations.zerion import ZerionPortfolioSnapshot, ZerionPosition
from almanak.gateway.proto import gateway_pb2


def _make_context() -> MagicMock:
    ctx = MagicMock(spec=grpc.aio.ServicerContext)
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


class TestWalletPortfolioHandlers:
    """Tests for IntegrationServiceServicer wallet portfolio methods."""

    @pytest.fixture
    def service(self):
        from almanak.gateway.services.integration_service import IntegrationServiceServicer

        svc = IntegrationServiceServicer.__new__(IntegrationServiceServicer)
        svc.settings = GatewaySettings(portfolio_api_key="test-portfolio-key", portfolio_api_provider="zerion")
        svc._initialized = True
        svc._binance = None
        svc._coingecko = None
        svc._thegraph = None
        svc._zerion = AsyncMock()
        return svc

    @pytest.mark.asyncio
    async def test_invalid_chain_returns_invalid_argument(self, service):
        ctx = _make_context()
        request = gateway_pb2.WalletPortfolioRequest(
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="avalanche; DROP TABLE",
        )

        await service.GetWalletPortfolio(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_invalid_wallet_returns_invalid_argument(self, service):
        ctx = _make_context()
        request = gateway_pb2.WalletPortfolioRequest(wallet_address="not-an-address", chain="avalanche")

        await service.GetWalletPositions(request, ctx)

        ctx.set_code.assert_called_with(grpc.StatusCode.INVALID_ARGUMENT)

    @pytest.mark.asyncio
    async def test_missing_provider_config_returns_failed_precondition(self, service):
        ctx = _make_context()
        service._zerion = None
        request = gateway_pb2.WalletPortfolioRequest(
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="avalanche",
        )

        response = await service.GetWalletPortfolio(request, ctx)

        assert response.success is False
        ctx.set_code.assert_called_with(grpc.StatusCode.FAILED_PRECONDITION)

    @pytest.mark.asyncio
    async def test_get_wallet_portfolio_success(self, service):
        ctx = _make_context()
        request = gateway_pb2.WalletPortfolioRequest(
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="avalanche",
        )
        service._zerion.get_wallet_portfolio.return_value = ZerionPortfolioSnapshot(
            provider="zerion",
            wallet_address=request.wallet_address,
            chain="avalanche",
            total_value_usd="4.70",
            fetched_at=datetime(2026, 4, 3, tzinfo=UTC),
            cache_hit=False,
        )

        response = await service.GetWalletPortfolio(request, ctx)

        assert response.success is True
        assert response.provider == "zerion"
        assert response.total_value_usd == "4.70"
        service._zerion.get_wallet_portfolio.assert_awaited_once()
        ctx.set_code.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_wallet_positions_success(self, service):
        ctx = _make_context()
        request = gateway_pb2.WalletPortfolioRequest(
            wallet_address="0x1234567890123456789012345678901234567890",
            chain="avalanche",
        )
        service._zerion.get_wallet_positions.return_value = ZerionPortfolioSnapshot(
            provider="zerion",
            wallet_address=request.wallet_address,
            chain="avalanche",
            total_value_usd="4.70",
            fetched_at=datetime(2026, 4, 3, tzinfo=UTC),
            cache_hit=True,
            positions=[
                ZerionPosition(
                    position_id="pos-1",
                    protocol="traderjoe_v2",
                    label="WAVAX/USDT LB",
                    position_type="liquidity_position",
                    value_usd="4.70",
                    pool_address="0xpool",
                    token_symbols=["WAVAX", "USDT"],
                    details={"source": "zerion"},
                )
            ],
        )

        response = await service.GetWalletPositions(request, ctx)

        assert response.success is True
        assert response.cache_hit is True
        assert len(response.positions) == 1
        assert response.positions[0].protocol == "traderjoe_v2"
        assert response.positions[0].pool_address == "0xpool"
        assert response.positions[0].token_symbols == ["WAVAX", "USDT"]
