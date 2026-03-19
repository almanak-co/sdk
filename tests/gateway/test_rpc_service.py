"""Tests for RpcService gateway implementation."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rpc_service import (
    ALLOWED_CHAINS,
    ChainRateLimiter,
    RpcServiceServicer,
)


@pytest.fixture
def settings():
    """Create test settings."""
    return GatewaySettings()


@pytest.fixture
def rpc_service(settings):
    """Create RpcService instance."""
    return RpcServiceServicer(settings)


@pytest.fixture
def mock_context():
    """Create mock gRPC context."""
    context = MagicMock()
    context.set_code = MagicMock()
    context.set_details = MagicMock()
    return context


class TestChainRateLimiter:
    """Tests for ChainRateLimiter."""

    @pytest.mark.asyncio
    async def test_allows_requests_under_limit(self):
        """Requests under limit are allowed."""
        limiter = ChainRateLimiter(requests_per_minute=10)

        for _ in range(5):
            allowed, wait_time = await limiter.check_rate_limit()
            assert allowed is True
            assert wait_time == 0.0
            await limiter.record_request()

    @pytest.mark.asyncio
    async def test_blocks_requests_over_limit(self):
        """Requests over limit are blocked."""
        limiter = ChainRateLimiter(requests_per_minute=3)

        # Make 3 requests
        for _ in range(3):
            await limiter.record_request()

        # 4th request should be blocked
        allowed, wait_time = await limiter.check_rate_limit()
        assert allowed is False
        assert wait_time > 0


class TestRpcServiceCall:
    """Tests for RpcService.Call."""

    @pytest.mark.asyncio
    async def test_rejects_unknown_chain(self, rpc_service, mock_context):
        """Call rejects unknown chains."""
        request = gateway_pb2.RpcRequest(
            chain="unknown_chain",
            method="eth_blockNumber",
            params="[]",
            id="1",
        )

        response = await rpc_service.Call(request, mock_context)

        assert response.success is False
        assert "not allowed" in json.loads(response.error)["message"]
        mock_context.set_code.assert_called()

    @pytest.mark.asyncio
    async def test_accepts_allowed_chains(self, rpc_service, mock_context):
        """Allowed chains list is correct."""
        # Verify expected chains are in allowlist
        expected_chains = {"ethereum", "arbitrum", "base", "optimism", "polygon", "avalanche"}
        assert expected_chains.issubset(ALLOWED_CHAINS)

    @pytest.mark.asyncio
    async def test_validates_params_json(self, rpc_service, mock_context):
        """Call validates params JSON."""
        request = gateway_pb2.RpcRequest(
            chain="arbitrum",
            method="eth_call",
            params="invalid json {{{",
            id="1",
        )

        # Mock the RPC URL lookup to return None (chain not configured)
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            response = await rpc_service.Call(request, mock_context)

            assert response.success is False
            assert "Invalid params JSON" in json.loads(response.error)["message"]

    @pytest.mark.asyncio
    async def test_successful_rpc_call(self, rpc_service, mock_context):
        """Successful RPC call returns result."""
        request = gateway_pb2.RpcRequest(
            chain="arbitrum",
            method="eth_blockNumber",
            params="[]",
            id="1",
        )

        # Mock the RPC call
        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service,
                "_make_rpc_call",
                return_value=("0x123", None),
            ):
                response = await rpc_service.Call(request, mock_context)

                assert response.success is True
                assert json.loads(response.result) == "0x123"

    @pytest.mark.asyncio
    async def test_rpc_call_error_handling(self, rpc_service, mock_context):
        """RPC call errors are returned correctly."""
        request = gateway_pb2.RpcRequest(
            chain="arbitrum",
            method="eth_call",
            params="[]",
            id="1",
        )

        error = {"code": -32000, "message": "execution reverted"}

        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service,
                "_make_rpc_call",
                return_value=(None, error),
            ):
                response = await rpc_service.Call(request, mock_context)

                assert response.success is False
                assert json.loads(response.error) == error


class TestRpcServiceBatchCall:
    """Tests for RpcService.BatchCall."""

    @pytest.mark.asyncio
    async def test_batch_rejects_unknown_chain(self, rpc_service, mock_context):
        """BatchCall rejects unknown chains."""
        request = gateway_pb2.RpcBatchRequest(
            chain="unknown_chain",
            requests=[
                gateway_pb2.RpcRequest(method="eth_blockNumber", params="[]", id="1"),
            ],
        )

        response = await rpc_service.BatchCall(request, mock_context)

        assert len(response.responses) == 0
        mock_context.set_code.assert_called()

    @pytest.mark.asyncio
    async def test_batch_executes_multiple_calls(self, rpc_service, mock_context):
        """BatchCall executes multiple RPC calls."""
        request = gateway_pb2.RpcBatchRequest(
            chain="arbitrum",
            requests=[
                gateway_pb2.RpcRequest(method="eth_blockNumber", params="[]", id="1"),
                gateway_pb2.RpcRequest(method="eth_chainId", params="[]", id="2"),
            ],
        )

        with patch.object(rpc_service, "_get_rpc_url", return_value="http://test"):
            with patch.object(
                rpc_service,
                "_make_rpc_call",
                side_effect=[
                    ("0x100", None),
                    ("0xa4b1", None),  # Arbitrum chain ID
                ],
            ):
                response = await rpc_service.BatchCall(request, mock_context)

                assert len(response.responses) == 2
                assert response.responses[0].success is True
                assert response.responses[1].success is True


class TestRpcServiceMetrics:
    """Tests for RpcService metrics."""

    def test_metrics_initialized(self, rpc_service):
        """Metrics are initialized to zero."""
        metrics = rpc_service.get_metrics()

        assert metrics["total_requests"] == 0
        assert metrics["successful_requests"] == 0
        assert metrics["failed_requests"] == 0
        assert metrics["rate_limited_requests"] == 0


class TestMakeRpcCallErrorMessages:
    """Tests for _make_rpc_call error message clarity."""

    def _make_client_error_session(self, error_cls, *args):
        """Build a mock aiohttp session whose post() raises a ClientError from __aenter__."""
        import aiohttp

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(side_effect=error_cls(*args))
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        mock_session = MagicMock()
        mock_session.post.return_value = mock_cm
        return mock_session

    @pytest.mark.asyncio
    async def test_localhost_connection_error_mentions_local_rpc(self, rpc_service):
        """When connecting to localhost fails, error message mentions the local RPC."""
        import aiohttp

        mock_session = self._make_client_error_session(
            aiohttp.ClientConnectorError, MagicMock(), OSError("Connect call failed")
        )

        with patch.object(rpc_service, "_get_session", new=AsyncMock(return_value=mock_session)):
            result, error = await rpc_service._make_rpc_call(
                "http://127.0.0.1:8546", "eth_blockNumber", [], "test"
            )

        assert result is None
        assert error is not None
        assert "local RPC" in error["message"]
        assert "127.0.0.1" in error["message"]

    @pytest.mark.asyncio
    async def test_external_connection_error_generic_message(self, rpc_service):
        """When connecting to an external RPC fails, error message is generic."""
        import aiohttp

        mock_session = self._make_client_error_session(
            aiohttp.ClientConnectorError, MagicMock(), OSError("Connection refused")
        )

        with patch.object(rpc_service, "_get_session", new=AsyncMock(return_value=mock_session)):
            result, error = await rpc_service._make_rpc_call(
                "https://arb1.arbitrum.io/rpc", "eth_blockNumber", [], "test"
            )

        assert result is None
        assert error is not None
        assert "Network error" in error["message"]
        # Should NOT mention Anvil for external RPCs
        assert "Anvil" not in error["message"]
