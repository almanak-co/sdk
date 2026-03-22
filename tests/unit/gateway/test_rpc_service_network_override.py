"""Tests for RpcService per-request network override behavior (VIB-1713)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

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
