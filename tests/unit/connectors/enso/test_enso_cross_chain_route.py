"""Unit tests for EnsoClient.get_cross_chain_route() (VIB-1684).

This method has never been tested. Tests verify:
1. Chain ID resolution for destination chain (string and int)
2. Correct parameter forwarding to get_route()
3. Error handling for unsupported destination chains
4. Cross-chain vs same-chain route differentiation
5. RouteTransaction.is_cross_chain property

All tests use mocked HTTP responses — no Enso API calls needed.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from almanak.framework.connectors.enso.client import (
    CHAIN_MAPPING,
    EnsoClient,
    EnsoConfig,
)
from almanak.framework.connectors.enso.exceptions import EnsoValidationError
from almanak.framework.connectors.enso.models import RouteTransaction


# =============================================================================
# Helpers
# =============================================================================

WALLET = "0x1234567890abcdef1234567890abcdef12345678"
USDC_BASE = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"
WETH_ARBITRUM = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"
USDC_DECIMALS = 10**6


def _make_client(chain: str = "base") -> EnsoClient:
    """Create an EnsoClient with a mocked API key."""
    with patch.dict("os.environ", {"ENSO_API_KEY": "test-key-123"}):
        config = EnsoConfig(chain=chain, wallet_address=WALLET)
    return EnsoClient(config)


def _mock_route_response(
    chain_id: int = 8453,
    destination_chain_id: int | None = None,
) -> RouteTransaction:
    """Create a mock RouteTransaction."""
    tx = MagicMock()
    tx.to = "0xrouter"
    tx.data = "0xdeadbeef"
    tx.value = "0"
    is_cross_chain = destination_chain_id is not None and destination_chain_id != chain_id
    return RouteTransaction(
        gas="200000",
        tx=tx,
        amount_out={"0x82aF": "500000000000000000"},
        chain_id=chain_id,
        destination_chain_id=destination_chain_id,
        bridge_fee="1000000000000000" if is_cross_chain else None,
        estimated_time=180 if is_cross_chain else None,
    )


# =============================================================================
# resolve_chain_id tests
# =============================================================================


class TestResolveChainId:
    def test_resolve_string_name(self):
        assert EnsoClient.resolve_chain_id("arbitrum") == 42161
        assert EnsoClient.resolve_chain_id("base") == 8453
        assert EnsoClient.resolve_chain_id("ethereum") == 1
        assert EnsoClient.resolve_chain_id("optimism") == 10

    def test_resolve_case_insensitive(self):
        assert EnsoClient.resolve_chain_id("Arbitrum") == 42161
        assert EnsoClient.resolve_chain_id("BASE") == 8453

    def test_resolve_integer_passthrough(self):
        assert EnsoClient.resolve_chain_id(42161) == 42161
        assert EnsoClient.resolve_chain_id(1) == 1

    def test_resolve_unsupported_raises(self):
        with pytest.raises(EnsoValidationError, match="Unsupported chain"):
            EnsoClient.resolve_chain_id("fantom")

    def test_all_chain_mapping_entries_resolvable(self):
        """Every entry in CHAIN_MAPPING should be resolvable."""
        for name, chain_id in CHAIN_MAPPING.items():
            assert EnsoClient.resolve_chain_id(name) == chain_id


# =============================================================================
# get_cross_chain_route tests
# =============================================================================


class TestGetCrossChainRoute:
    def test_forwards_destination_chain_id(self):
        """Verify destination_chain is resolved and passed to get_route."""
        client = _make_client(chain="base")
        mock_route = _mock_route_response(
            chain_id=8453,
            destination_chain_id=42161,
        )

        with patch.object(client, "get_route", return_value=mock_route) as mock_get:
            result = client.get_cross_chain_route(
                token_in=USDC_BASE,
                token_out=WETH_ARBITRUM,
                amount_in=1000 * USDC_DECIMALS,
                destination_chain="arbitrum",
            )

            mock_get.assert_called_once_with(
                token_in=USDC_BASE,
                token_out=WETH_ARBITRUM,
                amount_in=1000 * USDC_DECIMALS,
                slippage_bps=50,
                receiver=None,
                max_price_impact_bps=None,
                destination_chain_id=42161,
                refund_receiver=WALLET,
            )
            assert result is mock_route

    def test_forwards_integer_chain_id(self):
        """Verify integer destination_chain is passed through."""
        client = _make_client(chain="base")
        mock_route = _mock_route_response(chain_id=8453, destination_chain_id=10)

        with patch.object(client, "get_route", return_value=mock_route) as mock_get:
            client.get_cross_chain_route(
                token_in=USDC_BASE,
                token_out="0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",  # USDC on Optimism
                amount_in=500 * USDC_DECIMALS,
                destination_chain=10,
            )

            assert mock_get.call_args.kwargs["destination_chain_id"] == 10

    def test_custom_slippage(self):
        """Verify custom slippage is forwarded."""
        client = _make_client(chain="base")
        mock_route = _mock_route_response(chain_id=8453, destination_chain_id=42161)

        with patch.object(client, "get_route", return_value=mock_route) as mock_get:
            client.get_cross_chain_route(
                token_in=USDC_BASE,
                token_out=WETH_ARBITRUM,
                amount_in=1000 * USDC_DECIMALS,
                destination_chain="arbitrum",
                slippage_bps=100,
            )

            assert mock_get.call_args.kwargs["slippage_bps"] == 100

    def test_custom_receiver(self):
        """Verify custom receiver is forwarded."""
        client = _make_client(chain="base")
        receiver = "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        mock_route = _mock_route_response(chain_id=8453, destination_chain_id=42161)

        with patch.object(client, "get_route", return_value=mock_route) as mock_get:
            client.get_cross_chain_route(
                token_in=USDC_BASE,
                token_out=WETH_ARBITRUM,
                amount_in=1000 * USDC_DECIMALS,
                destination_chain="arbitrum",
                receiver=receiver,
            )

            assert mock_get.call_args.kwargs["receiver"] == receiver

    def test_max_price_impact(self):
        """Verify max_price_impact_bps is forwarded."""
        client = _make_client(chain="base")
        mock_route = _mock_route_response(chain_id=8453, destination_chain_id=42161)

        with patch.object(client, "get_route", return_value=mock_route) as mock_get:
            client.get_cross_chain_route(
                token_in=USDC_BASE,
                token_out=WETH_ARBITRUM,
                amount_in=1000 * USDC_DECIMALS,
                destination_chain="arbitrum",
                max_price_impact_bps=200,
            )

            assert mock_get.call_args.kwargs["max_price_impact_bps"] == 200

    def test_refund_receiver_is_wallet(self):
        """Verify refund_receiver is set to the client's wallet address."""
        client = _make_client(chain="base")
        mock_route = _mock_route_response(chain_id=8453, destination_chain_id=42161)

        with patch.object(client, "get_route", return_value=mock_route) as mock_get:
            client.get_cross_chain_route(
                token_in=USDC_BASE,
                token_out=WETH_ARBITRUM,
                amount_in=1000 * USDC_DECIMALS,
                destination_chain="arbitrum",
            )

            assert mock_get.call_args.kwargs["refund_receiver"] == WALLET

    def test_unsupported_destination_raises(self):
        """Verify unsupported destination chain raises validation error."""
        client = _make_client(chain="base")

        with pytest.raises(EnsoValidationError, match="Unsupported chain"):
            client.get_cross_chain_route(
                token_in=USDC_BASE,
                token_out=WETH_ARBITRUM,
                amount_in=1000 * USDC_DECIMALS,
                destination_chain="fantom",
            )


# =============================================================================
# RouteTransaction.is_cross_chain tests
# =============================================================================


class TestRouteTransactionCrossChain:
    def test_is_cross_chain_true(self):
        route = _mock_route_response(chain_id=8453, destination_chain_id=42161)
        assert route.is_cross_chain is True

    def test_is_cross_chain_false_same_chain(self):
        route = _mock_route_response(chain_id=8453, destination_chain_id=8453)
        assert route.is_cross_chain is False

    def test_is_cross_chain_false_none(self):
        route = _mock_route_response(chain_id=8453, destination_chain_id=None)
        assert route.is_cross_chain is False

    def test_cross_chain_has_bridge_fee(self):
        route = _mock_route_response(chain_id=8453, destination_chain_id=42161)
        assert route.bridge_fee is not None
        assert int(route.bridge_fee) > 0

    def test_cross_chain_has_estimated_time(self):
        route = _mock_route_response(chain_id=8453, destination_chain_id=42161)
        assert route.estimated_time is not None
        assert route.estimated_time > 0
