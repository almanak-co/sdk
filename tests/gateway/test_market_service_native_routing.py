"""Service-level native-symbol routing for ``MarketService`` (VIB-4851 A1).

A1 moved native-symbol detection onto the chain registry
(``_is_native_symbol`` -> ``native_symbols_for``). These tests prove the gateway
routing contract at the **endpoint** level (not just the predicate): a symbol that
is native to THIS chain routes to ``provider.get_native_balance()``; everything
else routes to ``provider.get_balance(token)``. This guards the security-boundary
balance path through both ``GetBalance`` and ``BatchGetBalances``, and pins the
X-Layer OKB mis-route that A1 incidentally fixed.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.market_service import MarketServiceServicer

_WALLET = "0x1234567890123456789012345678901234567890"


@pytest.fixture
def settings():
    from almanak.gateway.core.settings import GatewaySettings

    return GatewaySettings(grpc_host="localhost", grpc_port=50051, network="mainnet")


@pytest.fixture
def market_service(settings):
    return MarketServiceServicer(settings)


@pytest.fixture
def mock_context():
    ctx = AsyncMock()
    ctx.set_code = MagicMock()
    ctx.set_details = MagicMock()
    return ctx


def _balance_result() -> MagicMock:
    r = MagicMock()
    r.balance = 1.0
    r.address = "0xToken"
    r.decimals = 18
    r.raw_balance = 1000000000000000000
    r.timestamp = MagicMock()
    r.timestamp.timestamp.return_value = 1234567890
    r.stale = False
    return r


def _mock_provider() -> MagicMock:
    provider = MagicMock()
    provider.get_native_balance = AsyncMock(return_value=_balance_result())
    provider.get_balance = AsyncMock(return_value=_balance_result())
    return provider


async def _route_get_balance(market_service, mock_context, token: str, chain: str) -> MagicMock:
    """Drive ``GetBalance`` with a mocked provider and return the provider for assertions."""
    provider = _mock_provider()
    with patch.object(market_service, "_get_balance_provider", return_value=provider):
        market_service._initialized = True
        with patch.object(market_service, "_price_aggregator") as agg:
            agg.get_aggregated_price = AsyncMock(side_effect=Exception("skip USD valuation"))
            request = gateway_pb2.BalanceRequest(token=token, chain=chain, wallet_address=_WALLET)
            await market_service.GetBalance(request, mock_context)
    return provider


class TestGetBalanceNativeRouting:
    @pytest.mark.asyncio
    async def test_pol_on_polygon_routes_native(self, market_service, mock_context):
        provider = await _route_get_balance(market_service, mock_context, "POL", "polygon")
        provider.get_native_balance.assert_awaited_once()
        provider.get_balance.assert_not_called()

    @pytest.mark.asyncio
    async def test_matic_on_polygon_routes_native(self, market_service, mock_context):
        provider = await _route_get_balance(market_service, mock_context, "MATIC", "polygon")
        provider.get_native_balance.assert_awaited_once()
        provider.get_balance.assert_not_called()

    @pytest.mark.asyncio
    async def test_pol_on_ethereum_routes_erc20(self, market_service, mock_context):
        # The whole point of the chain-scoped check: POL is NOT native on Ethereum.
        provider = await _route_get_balance(market_service, mock_context, "POL", "ethereum")
        provider.get_balance.assert_awaited_once_with("POL")
        provider.get_native_balance.assert_not_called()

    @pytest.mark.asyncio
    async def test_okb_on_xlayer_routes_native(self, market_service, mock_context):
        # VIB-4851 A1 fix: the legacy map keyed X-Layer as "x-layer", so the
        # validated "xlayer" form missed and OKB mis-routed to the ERC-20 path.
        provider = await _route_get_balance(market_service, mock_context, "OKB", "xlayer")
        provider.get_native_balance.assert_awaited_once()
        provider.get_balance.assert_not_called()


class TestBatchGetBalancesNativeRouting:
    @pytest.mark.asyncio
    async def test_batch_routes_per_chain(self, market_service, mock_context):
        # POL is native on polygon but an ERC-20 on ethereum — the same routing
        # rule must hold per-entry inside the batch endpoint.
        providers = {"polygon": _mock_provider(), "ethereum": _mock_provider()}
        with patch.object(
            market_service,
            "_get_balance_provider",
            side_effect=lambda chain, _wallet: providers[chain],
        ):
            market_service._initialized = True
            with patch.object(market_service, "_price_aggregator") as agg:
                agg.get_aggregated_price = AsyncMock(side_effect=Exception("skip USD valuation"))
                request = gateway_pb2.BatchBalanceRequest(
                    requests=[
                        gateway_pb2.BalanceRequest(token="POL", chain="polygon", wallet_address=_WALLET),
                        gateway_pb2.BalanceRequest(token="POL", chain="ethereum", wallet_address=_WALLET),
                    ]
                )
                await market_service.BatchGetBalances(request, mock_context)

        providers["polygon"].get_native_balance.assert_awaited_once()
        providers["polygon"].get_balance.assert_not_called()
        providers["ethereum"].get_balance.assert_awaited_once_with("POL")
        providers["ethereum"].get_native_balance.assert_not_called()
