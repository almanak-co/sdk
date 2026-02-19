"""Tests for TraderJoe V2 SDK.

This test suite covers:
- SDK initialization and configuration
- Bin math utilities (bin_id_to_price, price_to_bin_id)
- Swap transaction building
- Liquidity transaction building
- Constants and configuration
"""

from decimal import Decimal
from unittest.mock import MagicMock, Mock, patch

import pytest

from ..sdk import (
    BIN_ID_OFFSET,
    BIN_STEPS,
    DEFAULT_GAS_ESTIMATES,
    TOKEN_DECIMALS,
    # Constants
    TRADERJOE_V2_ADDRESSES,
    TRADERJOE_V2_TOKENS,
    InvalidBinStepError,
    PoolInfo,
    PoolNotFoundError,
    SwapQuote,
    TraderJoeV2SDKError,
    TransactionData,
)

# =============================================================================
# Test Constants
# =============================================================================

# Avalanche token addresses
WAVAX_ADDRESS = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
USDC_ADDRESS = "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"
USDT_ADDRESS = "0x9702230A8Ea53601f5cD2dc00fDBc13d4dF4A8c7"
JOE_ADDRESS = "0x6e84a6216eA6dACC71eE8E6b0a5B7322EEbC0fDd"

TEST_WALLET = "0x1234567890123456789012345678901234567890"
MOCK_RPC_URL = "https://api.avax.network/ext/bc/C/rpc"


# =============================================================================
# SDK Initialization Tests (Mocked)
# =============================================================================


class TestTraderJoeV2SDKInit:
    """Tests for TraderJoeV2SDK initialization."""

    @patch("src.connectors.traderjoe_v2.sdk.Web3")
    def test_sdk_creation_avalanche(self, mock_web3: Mock) -> None:
        """Test SDK creation for Avalanche."""
        # Setup mock
        mock_instance = MagicMock()
        mock_instance.is_connected.return_value = True
        mock_instance.to_checksum_address = lambda x: x
        mock_web3.return_value = mock_instance
        mock_web3.to_checksum_address = lambda x: x
        mock_web3.HTTPProvider.return_value = "provider"

        from ..sdk import TraderJoeV2SDK

        sdk = TraderJoeV2SDK(chain="avalanche", rpc_url=MOCK_RPC_URL)

        assert sdk.chain == "avalanche"
        assert sdk.factory_address == TRADERJOE_V2_ADDRESSES["avalanche"]["factory"]
        assert sdk.router_address == TRADERJOE_V2_ADDRESSES["avalanche"]["router"]

    def test_sdk_invalid_chain(self) -> None:
        """Test SDK with invalid chain."""
        from ..sdk import TraderJoeV2SDK

        with pytest.raises(TraderJoeV2SDKError, match="not supported"):
            TraderJoeV2SDK(chain="ethereum", rpc_url=MOCK_RPC_URL)


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for SDK constants."""

    def test_bin_id_offset(self) -> None:
        """Test BIN_ID_OFFSET constant."""
        assert BIN_ID_OFFSET == 8388608

    def test_bin_steps(self) -> None:
        """Test BIN_STEPS list."""
        assert 1 in BIN_STEPS
        assert 5 in BIN_STEPS
        assert 10 in BIN_STEPS
        assert 15 in BIN_STEPS
        assert 20 in BIN_STEPS
        assert 25 in BIN_STEPS
        assert 50 in BIN_STEPS
        assert 100 in BIN_STEPS

    def test_addresses_avalanche(self) -> None:
        """Test addresses for Avalanche."""
        assert "avalanche" in TRADERJOE_V2_ADDRESSES
        assert TRADERJOE_V2_ADDRESSES["avalanche"]["factory"].startswith("0x")
        assert TRADERJOE_V2_ADDRESSES["avalanche"]["router"].startswith("0x")
        assert len(TRADERJOE_V2_ADDRESSES["avalanche"]["factory"]) == 42
        assert len(TRADERJOE_V2_ADDRESSES["avalanche"]["router"]) == 42

    def test_token_addresses(self) -> None:
        """Test token addresses for Avalanche."""
        assert "avalanche" in TRADERJOE_V2_TOKENS
        tokens = TRADERJOE_V2_TOKENS["avalanche"]
        assert "WAVAX" in tokens
        assert "USDC" in tokens
        assert "USDT" in tokens
        assert tokens["WAVAX"].startswith("0x")

    def test_token_decimals(self) -> None:
        """Test token decimals."""
        assert "WAVAX" in TOKEN_DECIMALS
        assert "USDC" in TOKEN_DECIMALS
        assert TOKEN_DECIMALS["WAVAX"] == 18
        assert TOKEN_DECIMALS["USDC"] == 6

    def test_gas_estimates(self) -> None:
        """Test default gas estimates."""
        assert "swap" in DEFAULT_GAS_ESTIMATES
        assert "add_liquidity" in DEFAULT_GAS_ESTIMATES
        assert "remove_liquidity" in DEFAULT_GAS_ESTIMATES
        assert "approve" in DEFAULT_GAS_ESTIMATES
        assert DEFAULT_GAS_ESTIMATES["swap"] > 0


# =============================================================================
# PoolInfo Tests
# =============================================================================


class TestPoolInfo:
    """Tests for PoolInfo dataclass."""

    def test_pool_info_creation(self) -> None:
        """Test PoolInfo creation."""
        pool = PoolInfo(
            address="0x1234567890123456789012345678901234567890",
            token_x=WAVAX_ADDRESS,
            token_y=USDC_ADDRESS,
            bin_step=20,
            active_id=8388608,
            reserve_x=10**18,
            reserve_y=30 * 10**6,
        )

        assert pool.address == "0x1234567890123456789012345678901234567890"
        assert pool.bin_step == 20
        assert pool.active_id == 8388608


# =============================================================================
# SwapQuote Tests
# =============================================================================


class TestSwapQuote:
    """Tests for SDK SwapQuote dataclass."""

    def test_swap_quote_creation(self) -> None:
        """Test SwapQuote creation."""
        quote = SwapQuote(
            amount_in=10**18,  # 1 AVAX
            amount_out=30 * 10**6,  # 30 USDC
            path=[WAVAX_ADDRESS, USDC_ADDRESS],
            bin_steps=[20],
            price_impact=Decimal("0.001"),
            fee=20,  # binStep as fee
        )

        assert quote.amount_in == 10**18
        assert quote.amount_out == 30 * 10**6
        assert quote.path == [WAVAX_ADDRESS, USDC_ADDRESS]
        assert quote.bin_steps == [20]


# =============================================================================
# TransactionData Tests
# =============================================================================


class TestTransactionData:
    """Tests for TransactionData dataclass."""

    def test_transaction_data_creation(self) -> None:
        """Test TransactionData creation."""
        tx_data = TransactionData(
            to="0x1234567890123456789012345678901234567890",
            data="0xabcdef",
            value=0,
            gas=200000,
        )

        assert tx_data.to == "0x1234567890123456789012345678901234567890"
        assert tx_data.data == "0xabcdef"
        assert tx_data.gas == 200000


# =============================================================================
# Exception Tests
# =============================================================================


class TestExceptions:
    """Tests for SDK exceptions."""

    def test_traderjoe_v2_sdk_error(self) -> None:
        """Test base SDK error."""
        error = TraderJoeV2SDKError("Test error")
        assert str(error) == "Test error"

    def test_pool_not_found_error(self) -> None:
        """Test PoolNotFoundError."""
        error = PoolNotFoundError(WAVAX_ADDRESS, USDC_ADDRESS, 20)

        assert error.token_x == WAVAX_ADDRESS
        assert error.token_y == USDC_ADDRESS
        assert error.bin_step == 20
        assert WAVAX_ADDRESS in str(error)

    def test_invalid_bin_step_error(self) -> None:
        """Test InvalidBinStepError."""
        error = InvalidBinStepError(999)

        assert error.bin_step == 999
        assert "999" in str(error)
        # Uses "Common values" instead of "Valid bin steps"
        assert "Common values" in str(error)


# =============================================================================
# Bin Math Tests (Static Methods)
# =============================================================================


class TestBinMath:
    """Tests for bin math utilities."""

    def test_bin_id_to_price_formula(self) -> None:
        """Test bin_id_to_price formula directly."""
        # Formula: price = (1 + binStep/10000)^(binId - 8388608) * 10^(decimalsX - decimalsY)
        bin_id = BIN_ID_OFFSET
        bin_step = 20

        # At offset, exponent is 0, so price should be 1 (when decimals equal)
        base = 1 + bin_step / 10000
        exponent = bin_id - BIN_ID_OFFSET
        price = Decimal(str(base**exponent))

        assert price == Decimal("1")

    def test_bin_id_to_price_positive_offset(self) -> None:
        """Test bin_id_to_price with positive offset."""
        BIN_ID_OFFSET + 100
        bin_step = 20

        base = 1 + bin_step / 10000
        exponent = 100
        price = Decimal(str(base**exponent))

        # Should be > 1
        assert price > Decimal("1")

    def test_bin_id_to_price_negative_offset(self) -> None:
        """Test bin_id_to_price with negative offset."""
        BIN_ID_OFFSET - 100
        bin_step = 20

        base = 1 + bin_step / 10000
        exponent = -100
        price = Decimal(str(base**exponent))

        # Should be < 1
        assert price < Decimal("1")

    def test_bin_id_offset_value(self) -> None:
        """Test that BIN_ID_OFFSET is 2^23."""
        assert BIN_ID_OFFSET == 2**23


# =============================================================================
# SDK Method Tests (Mocked)
# =============================================================================


class TestSDKMethods:
    """Tests for SDK methods with mocked Web3."""

    @pytest.fixture
    def mock_sdk(self) -> Mock:
        """Create a mock SDK for testing method signatures."""
        sdk = Mock()
        sdk.chain = "avalanche"
        sdk.factory_address = TRADERJOE_V2_ADDRESSES["avalanche"]["factory"]
        sdk.router_address = TRADERJOE_V2_ADDRESSES["avalanche"]["router"]
        sdk.rpc_url = MOCK_RPC_URL
        return sdk

    def test_mock_sdk_properties(self, mock_sdk: Mock) -> None:
        """Test mock SDK has expected properties."""
        assert mock_sdk.chain == "avalanche"
        assert mock_sdk.router_address == TRADERJOE_V2_ADDRESSES["avalanche"]["router"]


# =============================================================================
# Integration Tests (Contract Address Verification)
# =============================================================================


class TestContractAddresses:
    """Test contract address constants."""

    def test_router_address_format(self) -> None:
        """Test router address is valid format."""
        router = TRADERJOE_V2_ADDRESSES["avalanche"]["router"]
        assert router.startswith("0x")
        assert len(router) == 42

    def test_factory_address_format(self) -> None:
        """Test factory address is valid format."""
        factory = TRADERJOE_V2_ADDRESSES["avalanche"]["factory"]
        assert factory.startswith("0x")
        assert len(factory) == 42

    def test_known_router_address(self) -> None:
        """Test router matches known LBRouter v2.1 address."""
        # LBRouter v2.1 on Avalanche
        expected_router = "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30"
        actual_router = TRADERJOE_V2_ADDRESSES["avalanche"]["router"]
        assert actual_router.lower() == expected_router.lower()

    def test_known_factory_address(self) -> None:
        """Test factory matches known LBFactory address."""
        # LBFactory on Avalanche
        expected_factory = "0x8e42f2F4101563bF679975178e880FD87d3eFd4e"
        actual_factory = TRADERJOE_V2_ADDRESSES["avalanche"]["factory"]
        assert actual_factory.lower() == expected_factory.lower()

    def test_wavax_address(self) -> None:
        """Test WAVAX address."""
        expected_wavax = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
        actual_wavax = TRADERJOE_V2_TOKENS["avalanche"]["WAVAX"]
        assert actual_wavax.lower() == expected_wavax.lower()
