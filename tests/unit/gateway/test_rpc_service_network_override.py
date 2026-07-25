"""Tests for RpcService per-request network override behavior (VIB-1713)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import grpc
import pytest

from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.rpc_service import RpcServiceServicer


class TestGetRpcUrlNetworkOverride:
    """Test _get_rpc_url respects the network_override parameter."""

    def _make_service(self, default_network: str = "anvil") -> RpcServiceServicer:
        """Create an RpcService with a mocked settings object."""
        settings = MagicMock()
        settings.network = default_network
        settings.rate_limits = {}
        return RpcServiceServicer(settings)

    @patch("almanak.gateway.utils.get_rpc_url")
    def test_default_uses_settings_network(self, mock_get_rpc_url):
        """Without override, _get_rpc_url uses the gateway default network."""
        mock_get_rpc_url.return_value = "http://localhost:8545"
        svc = self._make_service(default_network="anvil")

        svc._get_rpc_url("arbitrum")

        mock_get_rpc_url.assert_called_once_with("arbitrum", network="anvil")

    @patch("almanak.gateway.utils.get_rpc_url")
    def test_override_takes_precedence(self, mock_get_rpc_url):
        """Per-request network override should take precedence over default."""
        mock_get_rpc_url.return_value = "https://arb-mainnet.g.alchemy.com/v2/key"
        svc = self._make_service(default_network="anvil")

        svc._get_rpc_url("arbitrum", network_override="mainnet")

        mock_get_rpc_url.assert_called_once_with("arbitrum", network="mainnet")

    @patch("almanak.gateway.utils.get_rpc_url")
    def test_none_override_falls_back_to_default(self, mock_get_rpc_url):
        """Explicit None override should fall back to gateway default."""
        mock_get_rpc_url.return_value = "http://localhost:8545"
        svc = self._make_service(default_network="anvil")

        svc._get_rpc_url("arbitrum", network_override=None)

        mock_get_rpc_url.assert_called_once_with("arbitrum", network="anvil")

    @patch("almanak.gateway.utils.get_rpc_url")
    def test_empty_string_override_falls_back_to_default(self, mock_get_rpc_url):
        """Empty string override should fall back to gateway default (proto default)."""
        mock_get_rpc_url.return_value = "http://localhost:8545"
        svc = self._make_service(default_network="anvil")

        # Proto default for string fields is "", which should be treated as "no override"
        svc._get_rpc_url("arbitrum", network_override="")

        # Empty string is falsy, so it should fall back to the gateway default
        mock_get_rpc_url.assert_called_once_with("arbitrum", network="anvil")

    @patch("almanak.gateway.utils.get_rpc_url")
    def test_unsupported_chain_returns_none(self, mock_get_rpc_url):
        """Unsupported chain should return None regardless of network override."""
        mock_get_rpc_url.side_effect = ValueError("Unsupported chain")
        svc = self._make_service()

        result = svc._get_rpc_url("unsupported_chain", network_override="mainnet")

        assert result is None


class TestAnvilElevationGuard:
    """A caller override must never elevate a non-anvil gateway to anvil semantics.

    ``ANVIL_ONLY_RPC_METHODS`` (``evm_increaseTime``, ``anvil_setCode``,
    ``eth_sendTransaction``, ...) and localhost fork routing are authorized by the
    gateway's own launch configuration. Honouring a request-supplied
    ``network="anvil"`` on a gateway launched for any other network would let the
    strategy container unlock fork-mutation methods and redirect gateway egress
    to the local fork URL. The legitimate override direction (VIB-1713: an
    anvil-launched gateway serving a per-request "mainnet" read) must keep
    working.
    """

    def _make_service(self, default_network: str) -> RpcServiceServicer:
        settings = MagicMock()
        settings.network = default_network
        settings.rate_limits = {}
        settings.chains = []
        return RpcServiceServicer(settings)

    @pytest.mark.asyncio
    async def test_call_rejects_anvil_override_on_non_anvil_gateway(self):
        svc = self._make_service("mainnet")
        context = MagicMock()
        request = gateway_pb2.RpcRequest(
            chain="arbitrum",
            method="evm_increaseTime",
            params="[315]",
            id="1",
            network="anvil",
        )

        with patch.object(svc, "_get_rpc_url") as mock_url:
            response = await svc.Call(request, context)

        assert response.success is False
        context.set_code.assert_called_once_with(grpc.StatusCode.PERMISSION_DENIED)
        mock_url.assert_not_called()

    @pytest.mark.asyncio
    async def test_call_anvil_override_on_anvil_gateway_passes_guard(self):
        svc = self._make_service("anvil")
        context = MagicMock()
        request = gateway_pb2.RpcRequest(
            chain="arbitrum",
            method="evm_increaseTime",
            params="[315]",
            id="1",
            network="anvil",
        )

        # Reaching URL resolution proves the request got past the guard and
        # the method allowlist without touching any real endpoint.
        with patch.object(svc, "_get_rpc_url", return_value=None):
            response = await svc.Call(request, context)

        assert response.success is False
        context.set_code.assert_called_once_with(grpc.StatusCode.FAILED_PRECONDITION)

    @pytest.mark.asyncio
    async def test_call_mainnet_override_on_anvil_gateway_still_allowed(self):
        """VIB-1713 direction: fork-launched gateway serving a live read."""
        svc = self._make_service("anvil")
        context = MagicMock()
        request = gateway_pb2.RpcRequest(
            chain="arbitrum",
            method="eth_call",
            params="[]",
            id="1",
            network="mainnet",
        )

        with patch.object(svc, "_get_rpc_url", return_value=None) as mock_url:
            response = await svc.Call(request, context)

        assert response.success is False
        context.set_code.assert_called_once_with(grpc.StatusCode.FAILED_PRECONDITION)
        mock_url.assert_called_once_with("arbitrum", network_override="mainnet")

    def test_batch_rejects_anvil_override_on_non_anvil_gateway(self):
        svc = self._make_service("mainnet")
        context = MagicMock()
        requests = [
            gateway_pb2.RpcRequest(
                chain="arbitrum",
                method="evm_increaseTime",
                params="[315]",
                id="1",
                network="anvil",
            )
        ]

        override, response = svc._resolve_batch_network_or_response(requests, context, 1)

        assert override is None
        assert response is not None
        context.set_code.assert_called_once_with(grpc.StatusCode.PERMISSION_DENIED)

    @pytest.mark.asyncio
    async def test_call_mixed_case_anvil_override_is_normalized(self):
        """'Anvil' must behave exactly like 'anvil' on both sides of the guard."""
        denied_svc = self._make_service("mainnet")
        denied_context = MagicMock()
        request = gateway_pb2.RpcRequest(
            chain="arbitrum",
            method="evm_increaseTime",
            params="[315]",
            id="1",
            network="Anvil",
        )

        with patch.object(denied_svc, "_get_rpc_url") as mock_url:
            response = await denied_svc.Call(request, denied_context)

        assert response.success is False
        denied_context.set_code.assert_called_once_with(grpc.StatusCode.PERMISSION_DENIED)
        mock_url.assert_not_called()

        allowed_svc = self._make_service("anvil")
        allowed_context = MagicMock()

        # The normalized override must also satisfy the exact network == "anvil"
        # method-allowlist comparison and reach URL resolution canonicalized.
        with patch.object(allowed_svc, "_get_rpc_url", return_value=None) as mock_url:
            response = await allowed_svc.Call(request, allowed_context)

        assert response.success is False
        allowed_context.set_code.assert_called_once_with(grpc.StatusCode.FAILED_PRECONDITION)
        mock_url.assert_called_once_with("arbitrum", network_override="anvil")

    def test_batch_mainnet_override_on_anvil_gateway_still_allowed(self):
        """VIB-1713 direction on the batch path, symmetric with the single-call test."""
        svc = self._make_service("anvil")
        context = MagicMock()
        requests = [
            gateway_pb2.RpcRequest(
                chain="arbitrum",
                method="eth_call",
                params="[]",
                id="1",
                network="mainnet",
            )
        ]

        override, response = svc._resolve_batch_network_or_response(requests, context, 1)

        assert override == "mainnet"
        assert response is None
        context.set_code.assert_not_called()

    def test_batch_anvil_override_on_anvil_gateway_passes_guard(self):
        svc = self._make_service("anvil")
        context = MagicMock()
        requests = [
            gateway_pb2.RpcRequest(
                chain="arbitrum",
                method="evm_increaseTime",
                params="[315]",
                id="1",
                network="anvil",
            )
        ]

        override, response = svc._resolve_batch_network_or_response(requests, context, 1)

        assert override == "anvil"
        assert response is None
        context.set_code.assert_not_called()
