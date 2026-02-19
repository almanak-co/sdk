"""Integration tests for OnChainLookup with real Anvil fork.

Tests verify actual on-chain token metadata fetching using Anvil forks.

To run:
    uv run pytest tests/integration/gateway/test_onchain_lookup_integration.py -v -s

Requirements:
    - ALCHEMY_API_KEY environment variable set
"""

import pytest

from almanak.gateway.services.onchain_lookup import (
    NATIVE_SENTINEL_ADDRESS,
    OnChainLookup,
)
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
    "polygon": {
        "WMATIC": {
            "address": "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270",
            "expected_symbol": "WPOL",  # On-chain symbol after Polygon MATIC->POL rebrand
            "expected_decimals": 18,
        },
        "USDC": {
            "address": "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359",
            "expected_symbol": "USDC",
            "expected_decimals": 6,
        },
    },
    "avalanche": {
        "WAVAX": {
            "address": "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
            "expected_symbol": "WAVAX",
            "expected_decimals": 18,
        },
        "USDC": {
            "address": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            "expected_symbol": "USDC",
            "expected_decimals": 6,
        },
    },
}


# =============================================================================
# Test Class
# =============================================================================


class TestOnChainLookupIntegration:
    """Integration tests for OnChainLookup with Anvil forks."""

    @pytest.mark.asyncio
    async def test_lookup_usdc_arbitrum(self, anvil_arbitrum: AnvilFixture):
        """Lookup USDC on Arbitrum returns correct metadata."""
        lookup = OnChainLookup(anvil_arbitrum.get_rpc_url())
        token = KNOWN_TOKENS["arbitrum"]["USDC"]

        result = await lookup.lookup("arbitrum", token["address"])

        assert result is not None
        assert result.symbol == token["expected_symbol"]
        assert result.decimals == token["expected_decimals"]
        assert result.is_native is False
        assert result.name is not None  # USDC has a name

    @pytest.mark.asyncio
    async def test_lookup_weth_arbitrum(self, anvil_arbitrum: AnvilFixture):
        """Lookup WETH on Arbitrum returns correct metadata."""
        lookup = OnChainLookup(anvil_arbitrum.get_rpc_url())
        token = KNOWN_TOKENS["arbitrum"]["WETH"]

        result = await lookup.lookup("arbitrum", token["address"])

        assert result is not None
        assert result.symbol == token["expected_symbol"]
        assert result.decimals == token["expected_decimals"]
        assert result.is_native is False

    @pytest.mark.asyncio
    async def test_lookup_native_token_arbitrum(self, anvil_arbitrum: AnvilFixture):
        """Lookup native ETH on Arbitrum via sentinel address."""
        lookup = OnChainLookup(anvil_arbitrum.get_rpc_url())

        result = await lookup.lookup("arbitrum", NATIVE_SENTINEL_ADDRESS)

        assert result is not None
        assert result.symbol == "ETH"
        assert result.decimals == 18
        assert result.is_native is True

    @pytest.mark.asyncio
    async def test_lookup_usdc_ethereum(self, anvil_ethereum: AnvilFixture):
        """Lookup USDC on Ethereum returns correct metadata."""
        lookup = OnChainLookup(anvil_ethereum.get_rpc_url())
        token = KNOWN_TOKENS["ethereum"]["USDC"]

        result = await lookup.lookup("ethereum", token["address"])

        assert result is not None
        assert result.symbol == token["expected_symbol"]
        assert result.decimals == token["expected_decimals"]

    @pytest.mark.asyncio
    async def test_lookup_dai_ethereum(self, anvil_ethereum: AnvilFixture):
        """Lookup DAI on Ethereum returns correct metadata."""
        lookup = OnChainLookup(anvil_ethereum.get_rpc_url())
        token = KNOWN_TOKENS["ethereum"]["DAI"]

        result = await lookup.lookup("ethereum", token["address"])

        assert result is not None
        assert result.symbol == token["expected_symbol"]
        assert result.decimals == token["expected_decimals"]

    @pytest.mark.asyncio
    async def test_lookup_usdc_base(self, anvil_base: AnvilFixture):
        """Lookup USDC on Base returns correct metadata."""
        lookup = OnChainLookup(anvil_base.get_rpc_url())
        token = KNOWN_TOKENS["base"]["USDC"]

        result = await lookup.lookup("base", token["address"])

        assert result is not None
        assert result.symbol == token["expected_symbol"]
        assert result.decimals == token["expected_decimals"]

    @pytest.mark.asyncio
    async def test_lookup_wmatic_polygon(self, anvil_polygon: AnvilFixture):
        """Lookup WMATIC on Polygon returns correct metadata."""
        lookup = OnChainLookup(anvil_polygon.get_rpc_url())
        token = KNOWN_TOKENS["polygon"]["WMATIC"]

        result = await lookup.lookup("polygon", token["address"])

        assert result is not None
        assert result.symbol == token["expected_symbol"]
        assert result.decimals == token["expected_decimals"]

    @pytest.mark.asyncio
    async def test_lookup_native_token_polygon(self, anvil_polygon: AnvilFixture):
        """Lookup native MATIC on Polygon via sentinel address."""
        lookup = OnChainLookup(anvil_polygon.get_rpc_url())

        result = await lookup.lookup("polygon", NATIVE_SENTINEL_ADDRESS)

        assert result is not None
        assert result.symbol == "MATIC"
        assert result.decimals == 18
        assert result.is_native is True

    @pytest.mark.asyncio
    async def test_lookup_wavax_avalanche(self, anvil_avalanche: AnvilFixture):
        """Lookup WAVAX on Avalanche returns correct metadata."""
        lookup = OnChainLookup(anvil_avalanche.get_rpc_url())
        token = KNOWN_TOKENS["avalanche"]["WAVAX"]

        result = await lookup.lookup("avalanche", token["address"])

        assert result is not None
        assert result.symbol == token["expected_symbol"]
        assert result.decimals == token["expected_decimals"]

    @pytest.mark.asyncio
    async def test_lookup_native_token_avalanche(self, anvil_avalanche: AnvilFixture):
        """Lookup native AVAX on Avalanche via sentinel address."""
        lookup = OnChainLookup(anvil_avalanche.get_rpc_url())

        result = await lookup.lookup("avalanche", NATIVE_SENTINEL_ADDRESS)

        assert result is not None
        assert result.symbol == "AVAX"
        assert result.decimals == 18
        assert result.is_native is True

    @pytest.mark.asyncio
    async def test_lookup_invalid_address_returns_none(self, anvil_arbitrum: AnvilFixture):
        """Lookup non-contract address returns None."""
        lookup = OnChainLookup(anvil_arbitrum.get_rpc_url())

        # Use a random EOA address that's not a contract
        eoa_address = "0x0000000000000000000000000000000000000001"
        result = await lookup.lookup("arbitrum", eoa_address)

        assert result is None

    @pytest.mark.asyncio
    async def test_lookup_with_lowercase_address(self, anvil_arbitrum: AnvilFixture):
        """Lookup works with lowercase addresses."""
        lookup = OnChainLookup(anvil_arbitrum.get_rpc_url())
        token = KNOWN_TOKENS["arbitrum"]["USDC"]

        # Use lowercase address
        result = await lookup.lookup("arbitrum", token["address"].lower())

        assert result is not None
        assert result.symbol == token["expected_symbol"]
        assert result.decimals == token["expected_decimals"]

    @pytest.mark.asyncio
    async def test_lookup_context_manager(self, anvil_arbitrum: AnvilFixture):
        """OnChainLookup works as async context manager."""
        async with OnChainLookup(anvil_arbitrum.get_rpc_url()) as lookup:
            token = KNOWN_TOKENS["arbitrum"]["WETH"]
            result = await lookup.lookup("arbitrum", token["address"])

            assert result is not None
            assert result.symbol == token["expected_symbol"]

    @pytest.mark.asyncio
    async def test_lookup_performance_under_2s(self, anvil_arbitrum: AnvilFixture):
        """Lookup completes within performance target (2s) for local Anvil/CI."""
        import time

        lookup = OnChainLookup(anvil_arbitrum.get_rpc_url())
        token = KNOWN_TOKENS["arbitrum"]["USDC"]

        start_time = time.time()
        result = await lookup.lookup("arbitrum", token["address"])
        elapsed_ms = (time.time() - start_time) * 1000

        assert result is not None
        # Local Anvil should be fast - allow 2000ms to accommodate CI shared runners
        assert elapsed_ms < 2000, f"Lookup took {elapsed_ms:.2f}ms, expected <2000ms"
