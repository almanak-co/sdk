"""Integration tests for TokenService with real Anvil fork.

Tests verify the TokenService gRPC implementation against real
on-chain token contracts using Anvil forks.

To run:
    uv run pytest tests/integration/gateway/test_token_service_integration.py -v -s

Requirements:
    - ALCHEMY_API_KEY environment variable set
"""

import pytest

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.proto import gateway_pb2
from almanak.gateway.services.token_service import TokenServiceServicer
from tests.conftest_gateway import AnvilFixture

# Import fixtures for pytest to discover
pytest_plugins = ["tests.conftest_gateway"]


# =============================================================================
# Known Token Addresses for Testing
# =============================================================================

# Well-known tokens with verified metadata for testing
KNOWN_TOKENS = {
    "arbitrum": {
        "USDC": {
            "address": "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "expected_symbol": "USDC",
            "expected_decimals": 6,
        },
        "WETH": {
            "address": "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            "expected_symbol": "WETH",
            "expected_decimals": 18,
        },
        "ARB": {
            "address": "0x912CE59144191C1204E64559FE8253a0e49E6548",
            "expected_symbol": "ARB",
            "expected_decimals": 18,
        },
    },
    "ethereum": {
        "USDC": {
            "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "expected_symbol": "USDC",
            "expected_decimals": 6,
        },
        "WETH": {
            "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
            "expected_symbol": "WETH",
            "expected_decimals": 18,
        },
        "DAI": {
            "address": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
            "expected_symbol": "DAI",
            "expected_decimals": 18,
        },
    },
    "base": {
        "USDC": {
            "address": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
            "expected_symbol": "USDC",
            "expected_decimals": 6,
        },
        "WETH": {
            "address": "0x4200000000000000000000000000000000000006",
            "expected_symbol": "WETH",
            "expected_decimals": 18,
        },
    },
}


# =============================================================================
# Fixtures
# =============================================================================


class MockGrpcContext:
    """Mock gRPC context for testing servicer methods directly."""

    def __init__(self):
        self.code = None
        self.details = None

    def set_code(self, code):
        self.code = code

    def set_details(self, details):
        self.details = details


@pytest.fixture
def mock_context():
    """Create mock gRPC context."""
    return MockGrpcContext()


# =============================================================================
# Test Class
# =============================================================================


class TestTokenServiceIntegration:
    """Integration tests for TokenService with Anvil forks."""

    @pytest.mark.asyncio
    async def test_resolve_usdc_arbitrum(self, anvil_arbitrum: AnvilFixture, mock_context):
        """ResolveToken returns USDC metadata on Arbitrum."""
        settings = GatewaySettings(network="anvil")
        service = TokenServiceServicer(settings)

        request = gateway_pb2.ResolveTokenRequest(token="USDC", chain="arbitrum")
        response = await service.ResolveToken(request, mock_context)

        assert response.success is True
        assert response.symbol == "USDC"
        assert response.decimals == 6
        assert response.is_verified is True

    @pytest.mark.asyncio
    async def test_resolve_by_address_arbitrum(self, anvil_arbitrum: AnvilFixture, mock_context):
        """ResolveToken resolves USDC by address on Arbitrum."""
        settings = GatewaySettings(network="anvil")
        service = TokenServiceServicer(settings)
        token = KNOWN_TOKENS["arbitrum"]["USDC"]

        request = gateway_pb2.ResolveTokenRequest(
            token=token["address"],
            chain="arbitrum",
        )
        response = await service.ResolveToken(request, mock_context)

        assert response.success is True
        assert response.symbol == token["expected_symbol"]
        assert response.decimals == token["expected_decimals"]

    @pytest.mark.asyncio
    async def test_resolve_weth_base(self, anvil_base: AnvilFixture, mock_context):
        """ResolveToken returns WETH metadata on Base."""
        settings = GatewaySettings(network="anvil")
        service = TokenServiceServicer(settings)

        request = gateway_pb2.ResolveTokenRequest(token="WETH", chain="base")
        response = await service.ResolveToken(request, mock_context)

        assert response.success is True
        assert response.symbol == "WETH"
        assert response.decimals == 18

    @pytest.mark.asyncio
    async def test_get_decimals_usdc_arbitrum(self, anvil_arbitrum: AnvilFixture, mock_context):
        """GetTokenDecimals returns correct decimals for USDC."""
        settings = GatewaySettings(network="anvil")
        service = TokenServiceServicer(settings)

        request = gateway_pb2.GetTokenDecimalsRequest(token="USDC", chain="arbitrum")
        response = await service.GetTokenDecimals(request, mock_context)

        assert response.success is True
        assert response.decimals == 6

    @pytest.mark.asyncio
    async def test_get_decimals_weth_ethereum(self, anvil_ethereum: AnvilFixture, mock_context):
        """GetTokenDecimals returns correct decimals for WETH."""
        settings = GatewaySettings(network="anvil")
        service = TokenServiceServicer(settings)

        request = gateway_pb2.GetTokenDecimalsRequest(token="WETH", chain="ethereum")
        response = await service.GetTokenDecimals(request, mock_context)

        assert response.success is True
        assert response.decimals == 18

    @pytest.mark.asyncio
    async def test_batch_resolve_tokens_arbitrum(self, anvil_arbitrum: AnvilFixture, mock_context):
        """BatchResolveTokens returns metadata for multiple tokens."""
        settings = GatewaySettings(network="anvil")
        service = TokenServiceServicer(settings)

        request = gateway_pb2.BatchResolveTokensRequest(
            tokens=["USDC", "WETH"],
            chain="arbitrum",
        )
        response = await service.BatchResolveTokens(request, mock_context)

        assert response.success is True
        assert len(response.tokens) == 2
        assert response.tokens[0].symbol == "USDC"
        assert response.tokens[0].decimals == 6
        assert response.tokens[1].symbol == "WETH"
        assert response.tokens[1].decimals == 18

    @pytest.mark.asyncio
    async def test_get_metadata_on_chain_lookup(self, anvil_arbitrum: AnvilFixture, mock_context):
        """GetTokenMetadata performs on-chain lookup for known token address."""
        import os

        settings = GatewaySettings(network="anvil")
        # Set the Anvil port env var for the service to use
        os.environ["ANVIL_ARBITRUM_PORT"] = str(anvil_arbitrum.port)

        try:
            service = TokenServiceServicer(settings)
            token = KNOWN_TOKENS["arbitrum"]["USDC"]

            request = gateway_pb2.GetTokenMetadataRequest(
                address=token["address"],
                chain="arbitrum",
            )
            response = await service.GetTokenMetadata(request, mock_context)

            # Should find in static registry first (fast path)
            assert response.success is True
            assert response.symbol == token["expected_symbol"]
            assert response.decimals == token["expected_decimals"]
        finally:
            # Clean up env var
            os.environ.pop("ANVIL_ARBITRUM_PORT", None)

    @pytest.mark.asyncio
    async def test_resolve_unknown_token_fails(self, anvil_arbitrum: AnvilFixture, mock_context):
        """ResolveToken returns error for unknown token symbol."""
        import grpc

        settings = GatewaySettings(network="anvil")
        service = TokenServiceServicer(settings)

        request = gateway_pb2.ResolveTokenRequest(token="NOTAREATOKEN", chain="arbitrum")
        response = await service.ResolveToken(request, mock_context)

        assert response.success is False
        assert "NOTAREATOKEN" in response.error
        assert mock_context.code == grpc.StatusCode.NOT_FOUND

    @pytest.mark.asyncio
    async def test_resolve_invalid_chain_fails(self, anvil_arbitrum: AnvilFixture, mock_context):
        """ResolveToken returns error for invalid chain."""
        import grpc

        settings = GatewaySettings(network="anvil")
        service = TokenServiceServicer(settings)

        request = gateway_pb2.ResolveTokenRequest(token="USDC", chain="notachain")
        response = await service.ResolveToken(request, mock_context)

        assert response.success is False
        assert mock_context.code == grpc.StatusCode.INVALID_ARGUMENT

    @pytest.mark.asyncio
    async def test_service_cleanup(self, anvil_arbitrum: AnvilFixture, mock_context):
        """TokenService cleanup properly closes OnChainLookup instances."""
        import os

        settings = GatewaySettings(network="anvil")
        os.environ["ANVIL_ARBITRUM_PORT"] = str(anvil_arbitrum.port)

        try:
            service = TokenServiceServicer(settings)

            # Trigger creation of an OnChainLookup by calling GetTokenMetadata
            # for a token not in static registry (force on-chain lookup)
            # Use a random address that won't be in registry
            request = gateway_pb2.GetTokenMetadataRequest(
                address="0x1234567890123456789012345678901234567890",
                chain="arbitrum",
            )
            # This should fail (not a real contract) but creates the lookup instance
            await service.GetTokenMetadata(request, mock_context)

            # Verify lookup was created
            assert "arbitrum" in service._onchain_lookups

            # Close service
            await service.close()

            # Verify cleanup
            assert service._onchain_lookups == {}
        finally:
            os.environ.pop("ANVIL_ARBITRUM_PORT", None)


class TestTokenServiceMultiChain:
    """Tests for TokenService across multiple chains."""

    @pytest.mark.asyncio
    async def test_resolve_usdc_multiple_chains(
        self,
        anvil_arbitrum: AnvilFixture,
        anvil_ethereum: AnvilFixture,
        anvil_base: AnvilFixture,
        mock_context,
    ):
        """USDC resolves correctly on multiple chains."""
        settings = GatewaySettings(network="anvil")
        service = TokenServiceServicer(settings)

        # Arbitrum
        request = gateway_pb2.ResolveTokenRequest(token="USDC", chain="arbitrum")
        response = await service.ResolveToken(request, mock_context)
        assert response.success is True
        assert response.decimals == 6
        arb_address = response.address

        # Ethereum
        request = gateway_pb2.ResolveTokenRequest(token="USDC", chain="ethereum")
        response = await service.ResolveToken(request, mock_context)
        assert response.success is True
        assert response.decimals == 6
        eth_address = response.address

        # Base
        request = gateway_pb2.ResolveTokenRequest(token="USDC", chain="base")
        response = await service.ResolveToken(request, mock_context)
        assert response.success is True
        assert response.decimals == 6
        base_address = response.address

        # Addresses should be different across chains
        assert arb_address != eth_address
        assert eth_address != base_address
        assert arb_address != base_address

    @pytest.mark.asyncio
    async def test_resolve_native_wrapped_tokens(
        self,
        anvil_arbitrum: AnvilFixture,
        anvil_avalanche: AnvilFixture,
        mock_context,
    ):
        """Wrapped native tokens resolve correctly across chains."""
        settings = GatewaySettings(network="anvil")
        service = TokenServiceServicer(settings)

        # WETH on Arbitrum
        request = gateway_pb2.ResolveTokenRequest(token="WETH", chain="arbitrum")
        response = await service.ResolveToken(request, mock_context)
        assert response.success is True
        assert response.symbol == "WETH"
        assert response.decimals == 18

        # WAVAX on Avalanche
        request = gateway_pb2.ResolveTokenRequest(token="WAVAX", chain="avalanche")
        response = await service.ResolveToken(request, mock_context)
        assert response.success is True
        assert response.symbol == "WAVAX"
        assert response.decimals == 18
