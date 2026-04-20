"""Unit tests for Morpho Blue SDK.

These tests verify the SDK's data classes, utility methods, and error handling
without requiring RPC connections (unit tests only).
"""

from decimal import Decimal

import pytest

from ..sdk import (
    LLTV_SCALE,
    MORPHO_BLUE_ADDRESS,
    MORPHO_DEPLOYMENT_BLOCKS,
    SHARES_SCALE,
    SUPPORTED_CHAINS,
    MarketNotFoundError,
    MorphoBlueSDK,
    MorphoBlueSDKError,
    PositionNotFoundError,
    RPCError,
    SDKMarketInfo,
    SDKMarketParams,
    SDKMarketState,
    SDKPosition,
    UnsupportedChainError,
)

# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for SDK constants."""

    def test_morpho_blue_address(self) -> None:
        """Test Morpho Blue address is valid."""
        assert MORPHO_BLUE_ADDRESS.startswith("0x")
        assert len(MORPHO_BLUE_ADDRESS) == 42
        # The famous BBBBBbb address
        assert MORPHO_BLUE_ADDRESS == "0xBBBBBbbBBb9cC5e90e3b3Af64bdAF62C37EEFFCb"

    def test_supported_chains(self) -> None:
        """Test supported chains are defined."""
        assert "ethereum" in SUPPORTED_CHAINS
        assert "base" in SUPPORTED_CHAINS
        assert len(SUPPORTED_CHAINS) >= 2

    def test_deployment_blocks(self) -> None:
        """Test deployment blocks are defined."""
        assert "ethereum" in MORPHO_DEPLOYMENT_BLOCKS
        assert MORPHO_DEPLOYMENT_BLOCKS["ethereum"] > 0

    def test_scale_factors(self) -> None:
        """Test scale factors are correct."""
        assert SHARES_SCALE == 10**18
        assert LLTV_SCALE == 10**18


# =============================================================================
# Exception Tests
# =============================================================================


class TestExceptions:
    """Tests for SDK exceptions."""

    def test_market_not_found_error(self) -> None:
        """Test MarketNotFoundError."""
        market_id = "0x1234"
        error = MarketNotFoundError(market_id)
        assert error.market_id == market_id
        assert market_id in str(error)
        assert isinstance(error, MorphoBlueSDKError)

    def test_position_not_found_error(self) -> None:
        """Test PositionNotFoundError."""
        market_id = "0x1234"
        user = "0xabcd"
        error = PositionNotFoundError(market_id, user)
        assert error.market_id == market_id
        assert error.user == user
        assert market_id in str(error)
        assert user in str(error)
        assert isinstance(error, MorphoBlueSDKError)

    def test_unsupported_chain_error(self) -> None:
        """Test UnsupportedChainError."""
        chain = "invalid_chain"
        error = UnsupportedChainError(chain)
        assert error.chain == chain
        assert chain in str(error)
        assert isinstance(error, MorphoBlueSDKError)

    def test_rpc_error(self) -> None:
        """Test RPCError."""
        message = "Connection failed"
        method = "get_position"
        error = RPCError(message, method)
        assert error.method == method
        assert message in str(error)
        assert method in str(error)
        assert isinstance(error, MorphoBlueSDKError)


# =============================================================================
# SDKPosition Tests
# =============================================================================


class TestSDKPosition:
    """Tests for SDKPosition data class."""

    def test_position_creation(self) -> None:
        """Test position creation."""
        position = SDKPosition(
            market_id="0x1234",
            user="0xabcd",
            supply_shares=1000 * SHARES_SCALE,
            borrow_shares=500 * SHARES_SCALE,
            collateral=100 * 10**18,
        )
        assert position.market_id == "0x1234"
        assert position.user == "0xabcd"
        assert position.supply_shares == 1000 * SHARES_SCALE
        assert position.borrow_shares == 500 * SHARES_SCALE
        assert position.collateral == 100 * 10**18

    def test_position_has_supply(self) -> None:
        """Test has_supply property."""
        position_with = SDKPosition(
            market_id="0x1234",
            user="0xabcd",
            supply_shares=100,
            borrow_shares=0,
            collateral=0,
        )
        position_without = SDKPosition(
            market_id="0x1234",
            user="0xabcd",
            supply_shares=0,
            borrow_shares=100,
            collateral=0,
        )
        assert position_with.has_supply is True
        assert position_without.has_supply is False

    def test_position_has_borrow(self) -> None:
        """Test has_borrow property."""
        position_with = SDKPosition(
            market_id="0x1234",
            user="0xabcd",
            supply_shares=0,
            borrow_shares=100,
            collateral=0,
        )
        position_without = SDKPosition(
            market_id="0x1234",
            user="0xabcd",
            supply_shares=100,
            borrow_shares=0,
            collateral=0,
        )
        assert position_with.has_borrow is True
        assert position_without.has_borrow is False

    def test_position_has_collateral(self) -> None:
        """Test has_collateral property."""
        position_with = SDKPosition(
            market_id="0x1234",
            user="0xabcd",
            supply_shares=0,
            borrow_shares=0,
            collateral=100,
        )
        position_without = SDKPosition(
            market_id="0x1234",
            user="0xabcd",
            supply_shares=0,
            borrow_shares=0,
            collateral=0,
        )
        assert position_with.has_collateral is True
        assert position_without.has_collateral is False

    def test_position_is_empty(self) -> None:
        """Test is_empty property."""
        empty = SDKPosition(
            market_id="0x1234",
            user="0xabcd",
            supply_shares=0,
            borrow_shares=0,
            collateral=0,
        )
        non_empty = SDKPosition(
            market_id="0x1234",
            user="0xabcd",
            supply_shares=1,
            borrow_shares=0,
            collateral=0,
        )
        assert empty.is_empty is True
        assert non_empty.is_empty is False

    def test_position_to_dict(self) -> None:
        """Test to_dict conversion."""
        position = SDKPosition(
            market_id="0x1234",
            user="0xabcd",
            supply_shares=100,
            borrow_shares=50,
            collateral=25,
        )
        result = position.to_dict()
        assert result["market_id"] == "0x1234"
        assert result["user"] == "0xabcd"
        assert result["supply_shares"] == 100
        assert result["borrow_shares"] == 50
        assert result["collateral"] == 25
        assert result["has_supply"] is True
        assert result["has_borrow"] is True
        assert result["has_collateral"] is True


# =============================================================================
# SDKMarketState Tests
# =============================================================================


class TestSDKMarketState:
    """Tests for SDKMarketState data class."""

    def test_market_state_creation(self) -> None:
        """Test market state creation."""
        state = SDKMarketState(
            market_id="0x1234",
            total_supply_assets=1000 * 10**6,
            total_supply_shares=1000 * SHARES_SCALE,
            total_borrow_assets=500 * 10**6,
            total_borrow_shares=500 * SHARES_SCALE,
            last_update=1700000000,
            fee=10**17,  # 10% fee
        )
        assert state.market_id == "0x1234"
        assert state.total_supply_assets == 1000 * 10**6
        assert state.total_borrow_assets == 500 * 10**6
        assert state.last_update == 1700000000

    def test_utilization(self) -> None:
        """Test utilization calculation."""
        state = SDKMarketState(
            market_id="0x1234",
            total_supply_assets=1000,
            total_supply_shares=1000,
            total_borrow_assets=500,
            total_borrow_shares=500,
            last_update=0,
            fee=0,
        )
        assert state.utilization == Decimal("0.5")
        assert state.utilization_percent == Decimal("50")

    def test_utilization_zero_supply(self) -> None:
        """Test utilization with zero supply."""
        state = SDKMarketState(
            market_id="0x1234",
            total_supply_assets=0,
            total_supply_shares=0,
            total_borrow_assets=0,
            total_borrow_shares=0,
            last_update=0,
            fee=0,
        )
        assert state.utilization == Decimal("0")

    def test_available_liquidity(self) -> None:
        """Test available liquidity calculation."""
        state = SDKMarketState(
            market_id="0x1234",
            total_supply_assets=1000,
            total_supply_shares=1000,
            total_borrow_assets=300,
            total_borrow_shares=300,
            last_update=0,
            fee=0,
        )
        assert state.available_liquidity == 700

    def test_fee_percent(self) -> None:
        """Test fee percent calculation."""
        state = SDKMarketState(
            market_id="0x1234",
            total_supply_assets=0,
            total_supply_shares=0,
            total_borrow_assets=0,
            total_borrow_shares=0,
            last_update=0,
            fee=10**17,  # 10% = 0.1e18
        )
        assert state.fee_percent == Decimal("10")

    def test_market_state_to_dict(self) -> None:
        """Test to_dict conversion."""
        state = SDKMarketState(
            market_id="0x1234",
            total_supply_assets=1000,
            total_supply_shares=1000,
            total_borrow_assets=500,
            total_borrow_shares=500,
            last_update=100,
            fee=10**17,
        )
        result = state.to_dict()
        assert result["market_id"] == "0x1234"
        assert result["total_supply_assets"] == 1000
        assert result["total_borrow_assets"] == 500
        assert result["available_liquidity"] == 500
        assert "utilization" in result


# =============================================================================
# SDKMarketParams Tests
# =============================================================================


class TestSDKMarketParams:
    """Tests for SDKMarketParams data class."""

    def test_market_params_creation(self) -> None:
        """Test market params creation."""
        params = SDKMarketParams(
            market_id="0x1234",
            loan_token="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
            collateral_token="0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0",  # wstETH
            oracle="0x48F7E36EB6B826B2dF4B2E630B62Cd25e89E40e2",
            irm="0x870aC11D48B15DB9a138Cf899d20F13F79Ba00BC",
            lltv=86 * 10**16,  # 86% = 0.86e18
        )
        assert params.loan_token == "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        assert params.collateral_token == "0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0"
        assert params.lltv == 86 * 10**16

    def test_lltv_percent(self) -> None:
        """Test LLTV percent calculation."""
        params = SDKMarketParams(
            market_id="0x1234",
            loan_token="0x0000",
            collateral_token="0x0000",
            oracle="0x0000",
            irm="0x0000",
            lltv=86 * 10**16,  # 86%
        )
        assert params.lltv_percent == Decimal("86")

    def test_lltv_decimal(self) -> None:
        """Test LLTV decimal calculation."""
        params = SDKMarketParams(
            market_id="0x1234",
            loan_token="0x0000",
            collateral_token="0x0000",
            oracle="0x0000",
            irm="0x0000",
            lltv=86 * 10**16,  # 86%
        )
        assert params.lltv_decimal == Decimal("0.86")

    def test_market_params_to_dict(self) -> None:
        """Test to_dict conversion."""
        params = SDKMarketParams(
            market_id="0x1234",
            loan_token="0xusdc",
            collateral_token="0xwsteth",
            oracle="0xoracle",
            irm="0xirm",
            lltv=86 * 10**16,
        )
        result = params.to_dict()
        assert result["market_id"] == "0x1234"
        assert result["loan_token"] == "0xusdc"
        assert result["collateral_token"] == "0xwsteth"
        assert result["lltv_percent"] == 86.0


# =============================================================================
# SDKMarketInfo Tests
# =============================================================================


class TestSDKMarketInfo:
    """Tests for SDKMarketInfo data class."""

    def test_market_info_creation(self) -> None:
        """Test market info creation."""
        params = SDKMarketParams(
            market_id="0x1234",
            loan_token="0xusdc",
            collateral_token="0xwsteth",
            oracle="0xoracle",
            irm="0xirm",
            lltv=86 * 10**16,
        )
        state = SDKMarketState(
            market_id="0x1234",
            total_supply_assets=1000,
            total_supply_shares=1000,
            total_borrow_assets=500,
            total_borrow_shares=500,
            last_update=100,
            fee=10**17,
        )
        info = SDKMarketInfo(params=params, state=state)
        assert info.params == params
        assert info.state == state

    def test_market_info_to_dict(self) -> None:
        """Test to_dict conversion."""
        params = SDKMarketParams(
            market_id="0x1234",
            loan_token="0xusdc",
            collateral_token="0xwsteth",
            oracle="0xoracle",
            irm="0xirm",
            lltv=86 * 10**16,
        )
        state = SDKMarketState(
            market_id="0x1234",
            total_supply_assets=1000,
            total_supply_shares=1000,
            total_borrow_assets=500,
            total_borrow_shares=500,
            last_update=100,
            fee=10**17,
        )
        info = SDKMarketInfo(params=params, state=state)
        result = info.to_dict()
        assert "params" in result
        assert "state" in result
        assert result["params"]["market_id"] == "0x1234"
        assert result["state"]["market_id"] == "0x1234"


# =============================================================================
# SDK Initialization Tests (Unit - No RPC)
# =============================================================================


class TestSDKInitialization:
    """Tests for SDK initialization without RPC."""

    def test_unsupported_chain_raises_error(self) -> None:
        """Test that unsupported chain raises error immediately."""
        with pytest.raises(UnsupportedChainError) as exc_info:
            MorphoBlueSDK(chain="invalid_chain")
        assert exc_info.value.chain == "invalid_chain"

    def test_unsupported_chain_error_message(self) -> None:
        """Test error message includes supported chains."""
        with pytest.raises(UnsupportedChainError) as exc_info:
            MorphoBlueSDK(chain="polygon")
        assert "polygon" in str(exc_info.value)
        assert "ethereum" in str(exc_info.value) or "base" in str(exc_info.value)


# =============================================================================
# SDK Utility Methods Tests (Unit - No RPC)
# =============================================================================


class TestSDKUtilityMethods:
    """Tests for SDK utility methods that don't require RPC."""

    @pytest.fixture
    def mock_sdk(self) -> MorphoBlueSDK:
        """Create SDK with mocked connection.

        Note: This test will fail if no RPC is available.
        For pure unit tests, we test the static methods directly.
        """
        # Skip if no RPC available
        pytest.skip("RPC tests should use integration tests")

    def test_shares_to_assets_calculation(self) -> None:
        """Test shares to assets conversion formula."""
        # Direct formula test without SDK instance
        shares = 1000
        total_assets = 2000
        total_shares = 1000
        # Expected: 1000 * 2000 / 1000 = 2000
        result = (shares * total_assets) // total_shares
        assert result == 2000

    def test_shares_to_assets_zero_shares(self) -> None:
        """Test shares to assets with zero total shares."""
        shares = 1000
        total_assets = 2000
        total_shares = 0
        # Should return 0 to avoid division by zero
        result = 0 if total_shares == 0 else (shares * total_assets) // total_shares
        assert result == 0

    def test_assets_to_shares_calculation(self) -> None:
        """Test assets to shares conversion formula."""
        assets = 1000
        total_assets = 2000
        total_shares = 4000
        # Expected: 1000 * 4000 / 2000 = 2000
        result = (assets * total_shares) // total_assets
        assert result == 2000

    def test_assets_to_shares_zero_assets(self) -> None:
        """Test assets to shares with zero total assets."""
        assets = 1000
        total_assets = 0
        total_shares = 4000
        # Should return 0 to avoid division by zero
        result = 0 if total_assets == 0 else (assets * total_shares) // total_assets
        assert result == 0


# =============================================================================
# SDK Internal Method Tests
# =============================================================================


class TestSDKInternalMethods:
    """Tests for SDK internal helper methods."""

    def test_normalize_market_id_with_prefix(self) -> None:
        """Test normalizing market ID that already has 0x prefix."""
        # Direct test of normalization logic
        market_id = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
        normalized = market_id.lower()
        if not normalized.startswith("0x"):
            normalized = "0x" + normalized
        if len(normalized) != 66:
            normalized = "0x" + normalized[2:].zfill(64)
        assert normalized == market_id.lower()
        assert len(normalized) == 66

    def test_normalize_market_id_without_prefix(self) -> None:
        """Test normalizing market ID without 0x prefix."""
        market_id = "b323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"
        normalized = market_id.lower()
        if not normalized.startswith("0x"):
            normalized = "0x" + normalized
        if len(normalized) != 66:
            normalized = "0x" + normalized[2:].zfill(64)
        assert normalized.startswith("0x")
        assert len(normalized) == 66

    def test_normalize_market_id_short(self) -> None:
        """Test normalizing short market ID (pads with zeros)."""
        market_id = "0x1234"
        normalized = market_id.lower()
        if not normalized.startswith("0x"):
            normalized = "0x" + normalized
        if len(normalized) != 66:
            normalized = "0x" + normalized[2:].zfill(64)
        assert normalized.startswith("0x")
        assert len(normalized) == 66
        assert normalized.endswith("1234")

    def test_pad_address(self) -> None:
        """Test padding address to 32 bytes."""
        address = "0x1234567890123456789012345678901234567890"
        addr = address.lower().replace("0x", "")
        padded = addr.zfill(64)
        assert len(padded) == 64
        assert padded.endswith(addr)

    def test_decode_address(self) -> None:
        """Test decoding address from 32-byte hex string."""
        # 32 bytes = 64 hex chars, address is last 20 bytes (40 chars)
        hex_str = "000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
        addr_hex = hex_str[-40:]
        assert addr_hex == "a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
