"""Tests for TraderJoe V2 Adapter.

This test suite covers:
- Adapter configuration
- Swap quote generation
- Position management
- Liquidity operations
"""

from decimal import Decimal
from unittest.mock import MagicMock, Mock, patch

import pytest

from ..adapter import (
    LiquidityPosition,
    SwapQuote,
    SwapResult,
    SwapType,
    TraderJoeV2Config,
    TransactionData,
)
from ..sdk import BIN_ID_OFFSET, DEFAULT_GAS_ESTIMATES

# =============================================================================
# Test Constants
# =============================================================================

WAVAX_ADDRESS = "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7"
USDC_ADDRESS = "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E"
TEST_WALLET = "0x1234567890123456789012345678901234567890"
MOCK_RPC_URL = "https://api.avax.network/ext/bc/C/rpc"


# =============================================================================
# Configuration Tests
# =============================================================================


class TestTraderJoeV2Config:
    """Tests for TraderJoeV2Config."""

    def test_config_creation(self) -> None:
        """Test config creation with required fields."""
        config = TraderJoeV2Config(
            chain="avalanche",
            wallet_address=TEST_WALLET,
            rpc_url=MOCK_RPC_URL,
        )

        assert config.chain == "avalanche"
        assert config.wallet_address == TEST_WALLET
        assert config.rpc_url == MOCK_RPC_URL

    def test_config_with_default_slippage(self) -> None:
        """Test config with default slippage."""
        config = TraderJoeV2Config(
            chain="avalanche",
            wallet_address=TEST_WALLET,
            rpc_url=MOCK_RPC_URL,
            default_slippage_bps=100,  # 1%
        )

        assert config.default_slippage_bps == 100

    def test_config_default_values(self) -> None:
        """Test config default values."""
        config = TraderJoeV2Config(
            chain="avalanche",
            wallet_address=TEST_WALLET,
            rpc_url=MOCK_RPC_URL,
        )

        assert config.default_slippage_bps == 50  # Default 0.5%
        assert config.default_deadline_seconds == 300  # Default 5 minutes
        assert config.private_key is None


# =============================================================================
# SwapQuote Tests
# =============================================================================


class TestSwapQuote:
    """Tests for Adapter SwapQuote dataclass."""

    def test_swap_quote_creation(self) -> None:
        """Test SwapQuote creation."""
        quote = SwapQuote(
            token_in=WAVAX_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=Decimal("1.0"),
            amount_out=Decimal("30.0"),
            price=Decimal("30.0"),
            price_impact=Decimal("0.001"),
            bin_step=20,
            path=[WAVAX_ADDRESS, USDC_ADDRESS],
            gas_estimate=DEFAULT_GAS_ESTIMATES["swap"],
        )

        assert quote.token_in == WAVAX_ADDRESS
        assert quote.token_out == USDC_ADDRESS
        assert quote.amount_in == Decimal("1.0")
        assert quote.amount_out == Decimal("30.0")
        assert quote.bin_step == 20


# =============================================================================
# SwapResult Tests
# =============================================================================


class TestSwapResult:
    """Tests for SwapResult dataclass."""

    def test_swap_result_creation(self) -> None:
        """Test SwapResult creation."""
        result = SwapResult(
            success=True,
            tx_hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            token_in=WAVAX_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=Decimal("1.0"),
            amount_out=Decimal("30.0"),
            gas_used=200000,
            block_number=12345678,
            timestamp=1700000000,
            error=None,
        )

        assert result.success is True
        assert result.amount_out == Decimal("30.0")

    def test_swap_result_failed(self) -> None:
        """Test SwapResult for failed transaction."""
        result = SwapResult(
            success=False,
            tx_hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            token_in=WAVAX_ADDRESS,
            token_out=USDC_ADDRESS,
            amount_in=Decimal("1.0"),
            amount_out=Decimal("0"),
            gas_used=50000,
            block_number=12345678,
            timestamp=1700000000,
            error="Transaction reverted",
        )

        assert result.success is False
        assert result.error == "Transaction reverted"


# =============================================================================
# LiquidityPosition Tests
# =============================================================================


class TestLiquidityPosition:
    """Tests for LiquidityPosition dataclass."""

    def test_liquidity_position_creation(self) -> None:
        """Test LiquidityPosition creation."""
        position = LiquidityPosition(
            pool_address="0x1234567890123456789012345678901234567890",
            token_x=WAVAX_ADDRESS,
            token_y=USDC_ADDRESS,
            bin_step=20,
            bin_ids=[BIN_ID_OFFSET - 1, BIN_ID_OFFSET, BIN_ID_OFFSET + 1],
            balances={  # Dict mapping bin ID to LB token balance
                BIN_ID_OFFSET - 1: 1000,
                BIN_ID_OFFSET: 2000,
                BIN_ID_OFFSET + 1: 1000,
            },
            amount_x=10 * 10**18,  # Amount in wei
            amount_y=300 * 10**6,  # Amount in smallest unit
            active_bin=BIN_ID_OFFSET,
        )

        assert position.token_x == WAVAX_ADDRESS
        assert position.bin_step == 20
        assert len(position.bin_ids) == 3
        assert sum(position.balances.values()) == 4000


# =============================================================================
# TransactionData Tests
# =============================================================================


class TestTransactionData:
    """Tests for Adapter TransactionData dataclass."""

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
        assert tx_data.chain_id == 43114  # Default Avalanche


# =============================================================================
# SwapType Tests
# =============================================================================


class TestSwapType:
    """Tests for SwapType enum."""

    def test_swap_type_values(self) -> None:
        """Test SwapType enum values."""
        assert SwapType.EXACT_INPUT.value == "exact_input"
        assert SwapType.EXACT_OUTPUT.value == "exact_output"


# =============================================================================
# Adapter Initialization Tests (Mocked)
# =============================================================================


class TestTraderJoeV2AdapterInit:
    """Tests for TraderJoeV2Adapter initialization."""

    @patch("src.connectors.traderjoe_v2.adapter.TraderJoeV2SDK")
    def test_adapter_creation(self, mock_sdk_class: Mock) -> None:
        """Test adapter creation."""
        from ..adapter import TraderJoeV2Adapter

        mock_sdk_instance = MagicMock()
        mock_sdk_class.return_value = mock_sdk_instance

        config = TraderJoeV2Config(
            chain="avalanche",
            wallet_address=TEST_WALLET,
            rpc_url=MOCK_RPC_URL,
        )
        adapter = TraderJoeV2Adapter(config)

        assert adapter.config == config
        mock_sdk_class.assert_called_once()

    @patch("src.connectors.traderjoe_v2.adapter.TraderJoeV2SDK")
    def test_adapter_has_sdk(self, mock_sdk_class: Mock) -> None:
        """Test adapter initializes SDK."""
        from ..adapter import TraderJoeV2Adapter

        mock_sdk_instance = MagicMock()
        mock_sdk_class.return_value = mock_sdk_instance

        config = TraderJoeV2Config(
            chain="avalanche",
            wallet_address=TEST_WALLET,
            rpc_url=MOCK_RPC_URL,
        )
        adapter = TraderJoeV2Adapter(config)

        assert adapter.sdk is not None


# =============================================================================
# Adapter Method Tests (Mocked)
# =============================================================================


class TestAdapterMethods:
    """Tests for adapter methods with mocked SDK."""

    @pytest.fixture
    @patch("src.connectors.traderjoe_v2.adapter.TraderJoeV2SDK")
    def adapter(self, mock_sdk_class: Mock):
        """Create adapter for testing."""
        from ..adapter import TraderJoeV2Adapter

        mock_sdk_instance = MagicMock()
        mock_sdk_instance.router_address = "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30"
        mock_sdk_instance.build_swap_exact_tokens_for_tokens.return_value = (
            {"to": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30", "data": "0x1234", "value": 0},
            200000,
        )
        mock_sdk_instance.build_add_liquidity.return_value = (
            {"to": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30", "data": "0x5678", "value": 0},
            400000,
        )
        mock_sdk_instance.build_remove_liquidity.return_value = (
            {"to": "0xb4315e873dBcf96Ffd0acd8EA43f689D8c20fB30", "data": "0x9abc", "value": 0},
            300000,
        )
        mock_sdk_class.return_value = mock_sdk_instance

        config = TraderJoeV2Config(
            chain="avalanche",
            wallet_address=TEST_WALLET,
            rpc_url=MOCK_RPC_URL,
        )
        return TraderJoeV2Adapter(config)

    def test_adapter_config(self, adapter) -> None:
        """Test adapter configuration."""
        assert adapter.config.chain == "avalanche"
        assert adapter.config.wallet_address == TEST_WALLET
