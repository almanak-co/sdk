"""Tests for IntentCompiler Enso gateway routing.

Verifies the _get_enso_route dispatcher correctly:
- Routes through gateway gRPC when gateway_client is connected
- Fails fast when gateway_client is configured but not connected
- Falls back to direct EnsoClient when no gateway_client (local dev)
- Fails in deployed mode (AGENT_ID set) without gateway_client
"""

import os
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.intents.compiler import IntentCompiler, IntentCompilerConfig


def _make_compiler(gateway_client=None):
    """Create compiler with placeholder prices for testing."""
    return IntentCompiler(
        chain="arbitrum",
        wallet_address="0x1234567890abcdef1234567890abcdef12345678",
        config=IntentCompilerConfig(allow_placeholder_prices=True),
        gateway_client=gateway_client,
    )


class TestEnsoGatewayRouting:
    """Tests for _get_enso_route dispatcher."""

    def test_gateway_connected_uses_grpc(self):
        """When gateway_client is connected, uses gRPC path."""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_response = SimpleNamespace(
            success=True,
            to="0xrouter",
            data="0xcalldata",
            value="0",
            gas="250000",
            gas_estimate="",
            amount_out="1000000",
            price_impact=15,
            is_cross_chain=False,
        )
        mock_client.enso.GetRoute.return_value = mock_response

        compiler = _make_compiler(gateway_client=mock_client)
        result = compiler._get_enso_route("0xtoken_in", "0xtoken_out", "1000000", 50)

        assert result["to"] == "0xrouter"
        assert result["data"] == "0xcalldata"
        assert result["gas"] == 250000
        assert result["amount_out"] == "1000000"
        assert result["price_impact"] == 15
        mock_client.enso.GetRoute.assert_called_once()

    def test_gateway_configured_but_disconnected_raises(self):
        """When gateway_client exists but is_connected is False, raises RuntimeError."""
        mock_client = MagicMock()
        mock_client.is_connected = False

        compiler = _make_compiler(gateway_client=mock_client)

        with pytest.raises(RuntimeError, match="not connected"):
            compiler._get_enso_route("0xtoken_in", "0xtoken_out", "1000000", 50)

    @patch.dict(os.environ, {"AGENT_ID": ""}, clear=False)
    def test_no_gateway_local_dev_uses_direct(self):
        """When no gateway_client (local dev), falls back to _get_enso_route_direct."""
        compiler = _make_compiler(gateway_client=None)

        direct_result = {
            "to": "0xrouter_direct",
            "data": "0xcalldata_direct",
            "value": "0",
            "gas": 200000,
            "amount_out": "999000",
            "price_impact": 10,
        }

        with patch.object(compiler, "_get_enso_route_direct", return_value=direct_result) as mock_direct:
            result = compiler._get_enso_route("0xtoken_in", "0xtoken_out", "1000000", 50)

        assert result["to"] == "0xrouter_direct"
        assert result["gas"] == 200000
        mock_direct.assert_called_once_with(
            "0xtoken_in", "0xtoken_out", 1000000, 50,
            chain=None, destination_chain_id=None, receiver=None, refund_receiver=None,
        )

    @patch.dict(os.environ, {"AGENT_ID": "agent-test-123"}, clear=False)
    def test_no_gateway_deployed_mode_raises(self):
        """When AGENT_ID is set but no gateway_client, raises RuntimeError."""
        compiler = _make_compiler(gateway_client=None)

        with pytest.raises(RuntimeError, match="no gateway client configured"):
            compiler._get_enso_route("0xtoken_in", "0xtoken_out", "1000000", 50)

    def test_gateway_gas_none_returns_none(self):
        """When gateway returns empty gas, result gas is None."""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_response = SimpleNamespace(
            success=True,
            to="0xrouter",
            data="0xcalldata",
            value="0",
            gas="",
            gas_estimate="",
            amount_out="1000000",
            price_impact=0,
            is_cross_chain=False,
        )
        mock_client.enso.GetRoute.return_value = mock_response

        compiler = _make_compiler(gateway_client=mock_client)
        result = compiler._get_enso_route("0xtoken_in", "0xtoken_out", "1000000", 50)

        assert result["gas"] is None

    def test_gateway_error_raises(self):
        """When gateway returns success=False, raises RuntimeError."""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_response = SimpleNamespace(
            success=False,
            error="Enso API key not configured",
        )
        mock_client.enso.GetRoute.return_value = mock_response

        compiler = _make_compiler(gateway_client=mock_client)

        with pytest.raises(RuntimeError, match="Gateway Enso GetRoute failed"):
            compiler._get_enso_route("0xtoken_in", "0xtoken_out", "1000000", 50)

    def test_cross_chain_params_forwarded(self):
        """Cross-chain params are forwarded to gateway gRPC."""
        mock_client = MagicMock()
        mock_client.is_connected = True
        mock_response = SimpleNamespace(
            success=True,
            to="0xrouter",
            data="0xcalldata",
            value="0",
            gas="300000",
            gas_estimate="",
            amount_out="1000000",
            price_impact=5,
            is_cross_chain=True,
            bridge_fee="1000",
            estimated_time=180,
        )
        mock_client.enso.GetRoute.return_value = mock_response

        compiler = _make_compiler(gateway_client=mock_client)
        result = compiler._get_enso_route(
            "0xtoken_in", "0xtoken_out", "1000000", 50,
            chain="base",
            destination_chain_id=42161,
            refund_receiver="0xrefund",
        )

        assert result["is_cross_chain"] is True
        assert result["bridge_fee"] == "1000"
        assert result["estimated_time"] == 180

        # Verify the request was built with cross-chain params
        call_args = mock_client.enso.GetRoute.call_args
        request = call_args[0][0]
        assert request.chain == "base"
        assert request.destination_chain_id == 42161
        assert request.refund_receiver == "0xrefund"
