"""Tests for Aerodrome SDK.

This test suite covers:
- SDK initialization
- Token resolution
- Constants validation
- Data classes
"""

import pytest

from ..sdk import (
    AERODROME_ADDRESSES,
    AERODROME_GAS_ESTIMATES,
    AERODROME_TOKENS,
    MAX_UINT256,
    TOKEN_DECIMALS,
    AerodromeSDK,
    AerodromeSDKError,
    InsufficientLiquidityError,
    PoolInfo,
    PoolNotFoundError,
    SwapQuote,
    SwapRoute,
)

# =============================================================================
# SDK Initialization Tests
# =============================================================================


class TestSDKInit:
    """Tests for SDK initialization."""

    def test_sdk_creation_base(self) -> None:
        """Test SDK creation for Base chain."""
        sdk = AerodromeSDK(chain="base")

        assert sdk.chain == "base"
        assert sdk.addresses is not None
        assert sdk.tokens is not None

    def test_sdk_invalid_chain(self) -> None:
        """Test SDK with invalid chain."""
        with pytest.raises(ValueError, match="Unsupported chain"):
            AerodromeSDK(chain="ethereum")

    def test_sdk_has_router_address(self) -> None:
        """Test SDK has router address."""
        sdk = AerodromeSDK(chain="base")

        assert "router" in sdk.addresses
        assert sdk.addresses["router"].startswith("0x")
        assert len(sdk.addresses["router"]) == 42

    def test_sdk_has_factory_address(self) -> None:
        """Test SDK has factory address."""
        sdk = AerodromeSDK(chain="base")

        assert "factory" in sdk.addresses
        assert sdk.addresses["factory"].startswith("0x")

    def test_sdk_with_rpc_url(self) -> None:
        """Test SDK creation with RPC URL."""
        sdk = AerodromeSDK(chain="base", rpc_url="https://mainnet.base.org")

        assert sdk.rpc_url == "https://mainnet.base.org"


# =============================================================================
# Token Resolution Tests
# =============================================================================


class TestTokenResolution:
    """Tests for token resolution."""

    @pytest.fixture
    def sdk(self) -> AerodromeSDK:
        """Create SDK fixture."""
        return AerodromeSDK(chain="base")

    def test_resolve_token_by_symbol(self, sdk: AerodromeSDK) -> None:
        """Test resolving token by symbol."""
        result = sdk.resolve_token("USDC")
        assert result is not None
        assert result.startswith("0x")

    def test_resolve_token_by_lowercase_symbol(self, sdk: AerodromeSDK) -> None:
        """Test resolving token by lowercase symbol."""
        result = sdk.resolve_token("usdc")
        assert result is not None
        assert result == sdk.resolve_token("USDC")

    def test_resolve_token_by_address(self, sdk: AerodromeSDK) -> None:
        """Test resolving token by address."""
        address = AERODROME_TOKENS["base"]["USDC"]
        result = sdk.resolve_token(address)
        assert result == address

    def test_resolve_unknown_token(self, sdk: AerodromeSDK) -> None:
        """Test resolving unknown token."""
        result = sdk.resolve_token("UNKNOWN_TOKEN")
        assert result is None

    def test_get_token_symbol(self, sdk: AerodromeSDK) -> None:
        """Test getting token symbol from address."""
        address = AERODROME_TOKENS["base"]["WETH"]
        symbol = sdk.get_token_symbol(address)
        assert symbol == "WETH"

    def test_get_token_symbol_unknown(self, sdk: AerodromeSDK) -> None:
        """Test getting symbol for unknown address."""
        symbol = sdk.get_token_symbol("0x0000000000000000000000000000000000000001")
        assert symbol == "UNKNOWN"

    def test_get_token_decimals(self, sdk: AerodromeSDK) -> None:
        """Test getting token decimals."""
        assert sdk.get_token_decimals("USDC") == 6
        assert sdk.get_token_decimals("USDbC") == 6
        assert sdk.get_token_decimals("WETH") == 18
        assert sdk.get_token_decimals("DAI") == 18
        assert sdk.get_token_decimals("AERO") == 18

    def test_get_token_decimals_default(self, sdk: AerodromeSDK) -> None:
        """Test default token decimals."""
        assert sdk.get_token_decimals("UNKNOWN") == 18


# =============================================================================
# Data Classes Tests
# =============================================================================


class TestPoolInfo:
    """Tests for PoolInfo data class."""

    def test_pool_info_creation(self) -> None:
        """Test PoolInfo creation."""
        pool = PoolInfo(
            address="0xPoolAddress",
            token0="0xToken0",
            token1="0xToken1",
            stable=False,
            reserve0=1000000,
            reserve1=500,
        )

        assert pool.address == "0xPoolAddress"
        assert pool.stable is False
        assert pool.reserve0 == 1000000
        assert pool.reserve1 == 500

    def test_pool_info_stable(self) -> None:
        """Test PoolInfo for stable pool."""
        pool = PoolInfo(
            address="0xPoolAddress",
            token0="0xToken0",
            token1="0xToken1",
            stable=True,
        )

        assert pool.stable is True

    def test_pool_info_to_dict(self) -> None:
        """Test PoolInfo serialization."""
        pool = PoolInfo(
            address="0xPoolAddress",
            token0="0xToken0",
            token1="0xToken1",
            stable=False,
            reserve0=1000000,
            reserve1=500,
            decimals0=6,
            decimals1=18,
        )

        result = pool.to_dict()

        assert result["address"] == "0xPoolAddress"
        assert result["stable"] is False
        assert result["reserve0"] == "1000000"
        assert result["decimals0"] == 6


class TestSwapRoute:
    """Tests for SwapRoute data class."""

    def test_swap_route_creation(self) -> None:
        """Test SwapRoute creation."""
        route = SwapRoute(
            from_token="0xToken0",
            to_token="0xToken1",
            stable=False,
        )

        assert route.from_token == "0xToken0"
        assert route.to_token == "0xToken1"
        assert route.stable is False
        assert route.factory is None

    def test_swap_route_with_factory(self) -> None:
        """Test SwapRoute with factory address."""
        route = SwapRoute(
            from_token="0xToken0",
            to_token="0xToken1",
            stable=True,
            factory="0xFactory",
        )

        assert route.factory == "0xFactory"
        assert route.stable is True

    def test_swap_route_to_tuple(self) -> None:
        """Test SwapRoute to tuple conversion."""
        from web3 import Web3

        route = SwapRoute(
            from_token="0x" + "aa" * 20,
            to_token="0x" + "bb" * 20,
            stable=False,
        )

        result = route.to_tuple("0x" + "cc" * 20)

        assert result == (
            Web3.to_checksum_address("0x" + "aa" * 20),
            Web3.to_checksum_address("0x" + "bb" * 20),
            False,
            Web3.to_checksum_address("0x" + "cc" * 20),
        )

    def test_swap_route_to_tuple_with_factory(self) -> None:
        """Test SwapRoute to tuple with custom factory."""
        from web3 import Web3

        route = SwapRoute(
            from_token="0x" + "aa" * 20,
            to_token="0x" + "bb" * 20,
            stable=True,
            factory="0x" + "dd" * 20,
        )

        result = route.to_tuple("0x" + "cc" * 20)

        assert result == (
            Web3.to_checksum_address("0x" + "aa" * 20),
            Web3.to_checksum_address("0x" + "bb" * 20),
            True,
            Web3.to_checksum_address("0x" + "dd" * 20),
        )

    def test_swap_route_to_dict(self) -> None:
        """Test SwapRoute serialization."""
        route = SwapRoute(
            from_token="0xToken0",
            to_token="0xToken1",
            stable=False,
        )

        result = route.to_dict()

        assert result["from"] == "0xToken0"
        assert result["to"] == "0xToken1"
        assert result["stable"] is False


class TestSwapQuote:
    """Tests for SwapQuote data class."""

    def test_swap_quote_creation(self) -> None:
        """Test SwapQuote creation."""
        route = SwapRoute("0xToken0", "0xToken1", False)
        quote = SwapQuote(
            amount_in=1000000,
            amount_out=500,
            routes=[route],
        )

        assert quote.amount_in == 1000000
        assert quote.amount_out == 500
        assert len(quote.routes) == 1

    def test_swap_quote_to_dict(self) -> None:
        """Test SwapQuote serialization."""
        route = SwapRoute("0xToken0", "0xToken1", False)
        quote = SwapQuote(
            amount_in=1000000,
            amount_out=500,
            routes=[route],
            price_impact_bps=10,
        )

        result = quote.to_dict()

        assert result["amount_in"] == "1000000"
        assert result["amount_out"] == "500"
        assert result["price_impact_bps"] == 10
        assert len(result["routes"]) == 1


# =============================================================================
# Exceptions Tests
# =============================================================================


class TestExceptions:
    """Tests for exceptions."""

    def test_aerodrome_sdk_error(self) -> None:
        """Test base SDK error."""
        with pytest.raises(AerodromeSDKError):
            raise AerodromeSDKError("Test error")

    def test_pool_not_found_error(self) -> None:
        """Test PoolNotFoundError."""
        with pytest.raises(PoolNotFoundError):
            raise PoolNotFoundError("Pool not found")

        # Check inheritance
        with pytest.raises(AerodromeSDKError):
            raise PoolNotFoundError("Pool not found")

    def test_insufficient_liquidity_error(self) -> None:
        """Test InsufficientLiquidityError."""
        with pytest.raises(InsufficientLiquidityError):
            raise InsufficientLiquidityError("Insufficient liquidity")

        # Check inheritance
        with pytest.raises(AerodromeSDKError):
            raise InsufficientLiquidityError("Insufficient liquidity")


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for constants."""

    def test_aerodrome_addresses_structure(self) -> None:
        """Test AERODROME_ADDRESSES structure."""
        assert "base" in AERODROME_ADDRESSES
        assert "router" in AERODROME_ADDRESSES["base"]
        assert "factory" in AERODROME_ADDRESSES["base"]
        assert "voter" in AERODROME_ADDRESSES["base"]

    def test_aerodrome_tokens_structure(self) -> None:
        """Test AERODROME_TOKENS structure."""
        assert "base" in AERODROME_TOKENS
        base_tokens = AERODROME_TOKENS["base"]

        # Check common tokens
        assert "ETH" in base_tokens
        assert "WETH" in base_tokens
        assert "USDC" in base_tokens
        assert "USDbC" in base_tokens
        assert "DAI" in base_tokens
        assert "AERO" in base_tokens

    def test_token_decimals_structure(self) -> None:
        """Test TOKEN_DECIMALS structure (keys normalized to uppercase)."""
        # Stablecoins should have 6 decimals
        assert TOKEN_DECIMALS["USDC"] == 6
        assert TOKEN_DECIMALS["USDBC"] == 6  # Bridged USDC, normalized to uppercase

        # ETH derivatives should have 18 decimals
        assert TOKEN_DECIMALS["ETH"] == 18
        assert TOKEN_DECIMALS["WETH"] == 18
        assert TOKEN_DECIMALS["CBETH"] == 18  # Normalized to uppercase
        assert TOKEN_DECIMALS["RETH"] == 18  # Normalized to uppercase

    def test_gas_estimates_reasonable(self) -> None:
        """Test AERODROME_GAS_ESTIMATES are reasonable."""
        # All estimates should be positive
        for operation, gas in AERODROME_GAS_ESTIMATES.items():
            assert gas > 0, f"{operation} gas estimate should be positive"

        # Swap should be more than approve
        assert AERODROME_GAS_ESTIMATES["swap"] > AERODROME_GAS_ESTIMATES["approve"]

        # Add liquidity should be more than swap (more complex)
        assert AERODROME_GAS_ESTIMATES["add_liquidity"] > AERODROME_GAS_ESTIMATES["swap"]

    def test_max_uint256(self) -> None:
        """Test MAX_UINT256 constant."""
        assert MAX_UINT256 == 2**256 - 1
        assert MAX_UINT256 > 0


# =============================================================================
# ABI Loading Tests
# =============================================================================


class TestABILoading:
    """Tests for ABI loading."""

    def test_sdk_loads_abis(self) -> None:
        """Test SDK loads ABIs on initialization."""
        sdk = AerodromeSDK(chain="base")

        # ABIs should be loaded (might be empty lists if files don't exist)
        assert sdk._router_abi is not None
        assert sdk._factory_abi is not None
        assert sdk._pool_abi is not None
        assert sdk._erc20_abi is not None
        assert sdk._weth_abi is not None


# =============================================================================
# Address Validation Tests
# =============================================================================


class TestAddressValidation:
    """Tests for address validation."""

    def test_router_address_format(self) -> None:
        """Test router address is valid."""
        router = AERODROME_ADDRESSES["base"]["router"]
        assert router.startswith("0x")
        assert len(router) == 42

    def test_factory_address_format(self) -> None:
        """Test factory address is valid."""
        factory = AERODROME_ADDRESSES["base"]["factory"]
        assert factory.startswith("0x")
        assert len(factory) == 42

    def test_token_addresses_format(self) -> None:
        """Test all token addresses are valid."""
        for symbol, address in AERODROME_TOKENS["base"].items():
            assert address.startswith("0x"), f"{symbol} address should start with 0x"
            assert len(address) == 42, f"{symbol} address should be 42 chars"
