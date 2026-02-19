"""Tests for PancakeSwap V3 Adapter.

This module contains unit tests for the PancakeSwapV3Adapter class,
covering all operations including swap_exact_input, swap_exact_output,
configuration validation, and fee tier handling.
"""

from decimal import Decimal

import pytest

from ..adapter import (
    EXACT_INPUT_SINGLE_SELECTOR,
    EXACT_OUTPUT_SINGLE_SELECTOR,
    FEE_TIERS,
    PANCAKESWAP_V3_ADDRESSES,
    PANCAKESWAP_V3_TOKENS,
    TOKEN_DECIMALS,
    PancakeSwapV3Adapter,
    PancakeSwapV3Config,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def config_bnb() -> PancakeSwapV3Config:
    """Create a test configuration for BNB chain."""
    return PancakeSwapV3Config(
        chain="bnb",
        wallet_address="0x1234567890123456789012345678901234567890",
        allow_placeholder_prices=True,
    )


@pytest.fixture
def config_ethereum() -> PancakeSwapV3Config:
    """Create a test configuration for Ethereum chain."""
    return PancakeSwapV3Config(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
        allow_placeholder_prices=True,
    )


@pytest.fixture
def config_arbitrum() -> PancakeSwapV3Config:
    """Create a test configuration for Arbitrum chain."""
    return PancakeSwapV3Config(
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        allow_placeholder_prices=True,
    )


@pytest.fixture
def adapter_bnb(config_bnb: PancakeSwapV3Config) -> PancakeSwapV3Adapter:
    """Create a test adapter instance for BNB chain."""
    return PancakeSwapV3Adapter(config_bnb)


@pytest.fixture
def adapter_ethereum(config_ethereum: PancakeSwapV3Config) -> PancakeSwapV3Adapter:
    """Create a test adapter instance for Ethereum chain."""
    return PancakeSwapV3Adapter(config_ethereum)


# =============================================================================
# Configuration Tests
# =============================================================================


class TestPancakeSwapConfigValidation:
    """Tests for PancakeSwapV3Config validation."""

    def test_valid_config_bnb(self) -> None:
        """Test creating a valid configuration for BNB chain."""
        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,
        )
        assert config.chain == "bnb"
        assert config.wallet_address == "0x1234567890123456789012345678901234567890"
        assert config.default_slippage_bps == 50
        assert config.default_fee_tier == 2500

    def test_valid_config_ethereum(self) -> None:
        """Test creating a valid configuration for Ethereum chain."""
        config = PancakeSwapV3Config(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,
        )
        assert config.chain == "ethereum"

    def test_valid_config_arbitrum(self) -> None:
        """Test creating a valid configuration for Arbitrum chain."""
        config = PancakeSwapV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,
        )
        assert config.chain == "arbitrum"

    def test_valid_config_custom_slippage(self) -> None:
        """Test creating a valid configuration with custom slippage."""
        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address="0x1234567890123456789012345678901234567890",
            default_slippage_bps=100,
            allow_placeholder_prices=True,
        )
        assert config.default_slippage_bps == 100

    def test_valid_config_custom_fee_tier(self) -> None:
        """Test creating a valid configuration with custom fee tier."""
        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address="0x1234567890123456789012345678901234567890",
            default_fee_tier=500,  # 0.05%
            allow_placeholder_prices=True,
        )
        assert config.default_fee_tier == 500

    def test_config_requires_price_provider(self) -> None:
        """Test that config requires price_provider or allow_placeholder_prices."""
        with pytest.raises(ValueError, match="requires price_provider"):
            PancakeSwapV3Config(
                chain="bnb",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_config_with_price_provider(self) -> None:
        """Test creating a configuration with explicit price_provider."""
        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address="0x1234567890123456789012345678901234567890",
            price_provider={"WBNB": Decimal("700"), "USDT": Decimal("1")},
        )
        assert config.price_provider is not None
        assert config.price_provider["WBNB"] == Decimal("700")

    def test_invalid_chain(self) -> None:
        """Test that invalid chain raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            PancakeSwapV3Config(
                chain="invalid_chain",
                wallet_address="0x1234567890123456789012345678901234567890",
                allow_placeholder_prices=True,
            )

    def test_invalid_chain_base(self) -> None:
        """Test that unsupported chain (base) raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            PancakeSwapV3Config(
                chain="base",
                wallet_address="0x1234567890123456789012345678901234567890",
                allow_placeholder_prices=True,
            )

    def test_invalid_wallet_address_no_prefix(self) -> None:
        """Test that invalid wallet address without 0x prefix raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            PancakeSwapV3Config(
                chain="bnb",
                wallet_address="1234567890123456789012345678901234567890",
                allow_placeholder_prices=True,
            )

    def test_invalid_wallet_address_short(self) -> None:
        """Test that short wallet address raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            PancakeSwapV3Config(
                chain="bnb",
                wallet_address="0x12345",
                allow_placeholder_prices=True,
            )

    def test_invalid_wallet_address_long(self) -> None:
        """Test that long wallet address raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            PancakeSwapV3Config(
                chain="bnb",
                wallet_address="0x12345678901234567890123456789012345678901234567890",
                allow_placeholder_prices=True,
            )

    def test_invalid_slippage_negative(self) -> None:
        """Test that negative slippage raises error."""
        with pytest.raises(ValueError, match="Invalid slippage"):
            PancakeSwapV3Config(
                chain="bnb",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=-1,
                allow_placeholder_prices=True,
            )

    def test_invalid_slippage_too_high(self) -> None:
        """Test that slippage > 10000 raises error."""
        with pytest.raises(ValueError, match="Invalid slippage"):
            PancakeSwapV3Config(
                chain="bnb",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=10001,
                allow_placeholder_prices=True,
            )

    def test_invalid_fee_tier(self) -> None:
        """Test that invalid fee tier raises error."""
        with pytest.raises(ValueError, match="Invalid fee tier"):
            PancakeSwapV3Config(
                chain="bnb",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_fee_tier=3000,  # Uniswap fee tier, not PancakeSwap
                allow_placeholder_prices=True,
            )


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestPancakeSwapAdapterInit:
    """Tests for PancakeSwapV3Adapter initialization."""

    def test_init_bnb(self, config_bnb: PancakeSwapV3Config) -> None:
        """Test adapter initialization for BNB chain."""
        adapter = PancakeSwapV3Adapter(config_bnb)
        assert adapter.chain == "bnb"
        assert adapter.swap_router_address == PANCAKESWAP_V3_ADDRESSES["bnb"]["swap_router"]
        assert adapter.factory_address == PANCAKESWAP_V3_ADDRESSES["bnb"]["factory"]
        assert adapter.wallet_address == config_bnb.wallet_address

    def test_init_ethereum(self, config_ethereum: PancakeSwapV3Config) -> None:
        """Test adapter initialization for Ethereum chain."""
        adapter = PancakeSwapV3Adapter(config_ethereum)
        assert adapter.chain == "ethereum"
        assert adapter.swap_router_address == PANCAKESWAP_V3_ADDRESSES["ethereum"]["swap_router"]

    def test_init_arbitrum(self, config_arbitrum: PancakeSwapV3Config) -> None:
        """Test adapter initialization for Arbitrum chain."""
        adapter = PancakeSwapV3Adapter(config_arbitrum)
        assert adapter.chain == "arbitrum"
        assert adapter.swap_router_address == PANCAKESWAP_V3_ADDRESSES["arbitrum"]["swap_router"]

    def test_init_token_addresses_bnb(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test that token addresses are loaded correctly for BNB chain."""
        assert adapter_bnb.token_addresses == PANCAKESWAP_V3_TOKENS["bnb"]
        assert "WBNB" in adapter_bnb.token_addresses
        assert "USDT" in adapter_bnb.token_addresses
        assert "USDC" in adapter_bnb.token_addresses
        assert "CAKE" in adapter_bnb.token_addresses

    def test_init_token_addresses_ethereum(self, adapter_ethereum: PancakeSwapV3Adapter) -> None:
        """Test that token addresses are loaded correctly for Ethereum chain."""
        assert adapter_ethereum.token_addresses == PANCAKESWAP_V3_TOKENS["ethereum"]
        assert "WETH" in adapter_ethereum.token_addresses
        assert "USDC" in adapter_ethereum.token_addresses


# =============================================================================
# Swap Exact Input Transaction Tests
# =============================================================================


class TestSwapExactInputBuild:
    """Tests for swap_exact_input transaction building."""

    def test_swap_exact_input_usdt_wbnb(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test building a swap exact input transaction for USDT to WBNB."""
        result = adapter_bnb.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("100"),
        )

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter_bnb.swap_router_address
        assert result.tx_data["value"] == 0
        assert result.tx_data["data"].startswith(EXACT_INPUT_SINGLE_SELECTOR)
        assert "Swap 100 USDT for WBNB" in result.description

    def test_swap_exact_input_weth_usdc(self, adapter_ethereum: PancakeSwapV3Adapter) -> None:
        """Test building a swap exact input transaction for WETH to USDC."""
        result = adapter_ethereum.swap_exact_input(
            token_in="WETH",
            token_out="USDC",
            amount_in=Decimal("1.5"),
        )

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(EXACT_INPUT_SINGLE_SELECTOR)
        assert "Swap 1.5 WETH for USDC" in result.description

    def test_swap_exact_input_calldata_structure(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test that swap exact input calldata has correct structure."""
        result = adapter_bnb.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("100"),
        )

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure: selector (4 bytes) + struct with 8 fields (32 bytes each)
        # tokenIn, tokenOut, fee, recipient, deadline, amountIn, amountOutMinimum, sqrtPriceLimitX96
        # Total: 4 + 32*8 = 260 bytes = 520 hex chars + 2 for "0x" prefix = 522 chars
        assert calldata.startswith(EXACT_INPUT_SINGLE_SELECTOR)
        assert len(calldata) == 2 + 8 + 64 * 8

    def test_swap_exact_input_unknown_token_in(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap exact input with unknown input token fails."""
        result = adapter_bnb.swap_exact_input(
            token_in="UNKNOWN_TOKEN",
            token_out="WBNB",
            amount_in=Decimal("100"),
        )

        assert result.success is False
        assert result.error is not None
        assert "Unknown input token" in result.error

    def test_swap_exact_input_unknown_token_out(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap exact input with unknown output token fails."""
        result = adapter_bnb.swap_exact_input(
            token_in="USDT",
            token_out="UNKNOWN_TOKEN",
            amount_in=Decimal("100"),
        )

        assert result.success is False
        assert result.error is not None
        assert "Unknown output token" in result.error

    def test_swap_exact_input_custom_fee_tier(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap exact input with custom fee tier."""
        result = adapter_bnb.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("100"),
            fee_tier=500,  # 0.05%
        )

        assert result.success is True
        assert result.tx_data is not None

    def test_swap_exact_input_invalid_fee_tier(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap exact input with invalid fee tier fails."""
        result = adapter_bnb.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("100"),
            fee_tier=3000,  # Uniswap fee tier, not valid for PancakeSwap
        )

        assert result.success is False
        assert result.error is not None
        assert "Invalid fee tier" in result.error

    def test_swap_exact_input_custom_recipient(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap exact input with custom recipient."""
        recipient = "0x9876543210987654321098765432109876543210"
        result = adapter_bnb.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("100"),
            recipient=recipient,
        )

        assert result.success is True
        assert result.tx_data is not None

    def test_swap_exact_input_with_amount_out_min(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap exact input with specified minimum output amount."""
        result = adapter_bnb.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("100"),
            amount_out_min=Decimal("0.1"),
        )

        assert result.success is True
        assert result.tx_data is not None

    def test_swap_exact_input_case_insensitive(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test that token lookup is case-insensitive."""
        result1 = adapter_bnb.swap_exact_input("USDT", "WBNB", Decimal("100"))
        result2 = adapter_bnb.swap_exact_input("usdt", "wbnb", Decimal("100"))

        assert result1.success is True
        assert result2.success is True


# =============================================================================
# Swap Exact Output Transaction Tests
# =============================================================================


class TestSwapExactOutputBuild:
    """Tests for swap_exact_output transaction building."""

    def test_swap_exact_output_usdt_wbnb(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test building a swap exact output transaction for USDT to WBNB."""
        result = adapter_bnb.swap_exact_output(
            token_in="USDT",
            token_out="WBNB",
            amount_out=Decimal("1"),
        )

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter_bnb.swap_router_address
        assert result.tx_data["value"] == 0
        assert result.tx_data["data"].startswith(EXACT_OUTPUT_SINGLE_SELECTOR)
        assert "Swap USDT for 1 WBNB" in result.description

    def test_swap_exact_output_weth_usdc(self, adapter_ethereum: PancakeSwapV3Adapter) -> None:
        """Test building a swap exact output transaction for WETH to USDC."""
        result = adapter_ethereum.swap_exact_output(
            token_in="WETH",
            token_out="USDC",
            amount_out=Decimal("1000"),
        )

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(EXACT_OUTPUT_SINGLE_SELECTOR)
        assert "Swap WETH for 1000 USDC" in result.description

    def test_swap_exact_output_calldata_structure(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test that swap exact output calldata has correct structure."""
        result = adapter_bnb.swap_exact_output(
            token_in="USDT",
            token_out="WBNB",
            amount_out=Decimal("1"),
        )

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure: selector (4 bytes) + struct with 8 fields (32 bytes each)
        # tokenIn, tokenOut, fee, recipient, deadline, amountOut, amountInMaximum, sqrtPriceLimitX96
        # Total: 4 + 32*8 = 260 bytes = 520 hex chars + 2 for "0x" prefix = 522 chars
        assert calldata.startswith(EXACT_OUTPUT_SINGLE_SELECTOR)
        assert len(calldata) == 2 + 8 + 64 * 8

    def test_swap_exact_output_unknown_token_in(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap exact output with unknown input token fails."""
        result = adapter_bnb.swap_exact_output(
            token_in="UNKNOWN_TOKEN",
            token_out="WBNB",
            amount_out=Decimal("1"),
        )

        assert result.success is False
        assert result.error is not None
        assert "Unknown input token" in result.error

    def test_swap_exact_output_unknown_token_out(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap exact output with unknown output token fails."""
        result = adapter_bnb.swap_exact_output(
            token_in="USDT",
            token_out="UNKNOWN_TOKEN",
            amount_out=Decimal("1"),
        )

        assert result.success is False
        assert result.error is not None
        assert "Unknown output token" in result.error

    def test_swap_exact_output_custom_fee_tier(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap exact output with custom fee tier."""
        result = adapter_bnb.swap_exact_output(
            token_in="USDT",
            token_out="WBNB",
            amount_out=Decimal("1"),
            fee_tier=10000,  # 1%
        )

        assert result.success is True
        assert result.tx_data is not None

    def test_swap_exact_output_invalid_fee_tier(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap exact output with invalid fee tier fails."""
        result = adapter_bnb.swap_exact_output(
            token_in="USDT",
            token_out="WBNB",
            amount_out=Decimal("1"),
            fee_tier=3000,  # Uniswap fee tier, not valid for PancakeSwap
        )

        assert result.success is False
        assert result.error is not None
        assert "Invalid fee tier" in result.error

    def test_swap_exact_output_with_amount_in_max(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap exact output with specified maximum input amount."""
        result = adapter_bnb.swap_exact_output(
            token_in="USDT",
            token_out="WBNB",
            amount_out=Decimal("1"),
            amount_in_max=Decimal("500"),
        )

        assert result.success is True
        assert result.tx_data is not None


# =============================================================================
# Fee Tier Tests
# =============================================================================


class TestFeeTiers:
    """Tests for PancakeSwap V3 fee tier handling."""

    def test_fee_tiers_constant(self) -> None:
        """Test that FEE_TIERS contains PancakeSwap-specific fee tiers."""
        assert FEE_TIERS == {100, 500, 2500, 10000}

    def test_fee_tier_100(self) -> None:
        """Test 0.01% fee tier (100 bps) is valid."""
        assert 100 in FEE_TIERS

    def test_fee_tier_500(self) -> None:
        """Test 0.05% fee tier (500 bps) is valid."""
        assert 500 in FEE_TIERS

    def test_fee_tier_2500(self) -> None:
        """Test 0.25% fee tier (2500 bps) is valid."""
        assert 2500 in FEE_TIERS

    def test_fee_tier_10000(self) -> None:
        """Test 1% fee tier (10000 bps) is valid."""
        assert 10000 in FEE_TIERS

    def test_uniswap_fee_tier_3000_not_valid(self) -> None:
        """Test that Uniswap 0.3% fee tier (3000 bps) is NOT valid for PancakeSwap."""
        assert 3000 not in FEE_TIERS

    def test_default_fee_tier_is_2500(self) -> None:
        """Test that default fee tier is 2500 (0.25%)."""
        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,
        )
        assert config.default_fee_tier == 2500

    def test_swap_uses_config_default_fee_tier(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test that swap uses config default fee tier when not specified."""
        result = adapter_bnb.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("100"),
        )

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Fee tier is the 3rd parameter (after tokenIn and tokenOut)
        # Position: 2 + 8 + 64 + 64 = 138, length 64
        fee_hex = calldata[2 + 8 + 64 + 64 : 2 + 8 + 64 + 64 + 64]
        fee_value = int(fee_hex, 16)
        assert fee_value == 2500  # Default fee tier

    def test_swap_with_all_valid_fee_tiers(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test that swaps work with all valid fee tiers."""
        for fee_tier in FEE_TIERS:
            result = adapter_bnb.swap_exact_input(
                token_in="USDT",
                token_out="WBNB",
                amount_in=Decimal("100"),
                fee_tier=fee_tier,
            )
            assert result.success is True, f"Failed for fee tier {fee_tier}"


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_zero_amount_swap(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap with zero amount still builds tx."""
        result = adapter_bnb.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("0"),
        )
        assert result.success is True

    def test_very_large_amount(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap with very large amount."""
        result = adapter_bnb.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("1000000000"),
        )
        assert result.success is True

    def test_fractional_amount(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test swap with fractional amount."""
        result = adapter_bnb.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("0.000001"),
        )
        assert result.success is True

    def test_token_address_as_input(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test using token address directly instead of symbol."""
        usdt_address = PANCAKESWAP_V3_TOKENS["bnb"]["USDT"]
        wbnb_address = PANCAKESWAP_V3_TOKENS["bnb"]["WBNB"]
        result = adapter_bnb.swap_exact_input(
            token_in=usdt_address,
            token_out=wbnb_address,
            amount_in=Decimal("100"),
            amount_out_min=Decimal("0.1"),  # Required when using addresses (price lookup needs symbols)
        )

        assert result.success is True
        assert result.tx_data is not None


# =============================================================================
# Utility Method Tests
# =============================================================================


class TestUtilityMethods:
    """Tests for utility methods."""

    def test_resolve_token_by_symbol(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test resolving token by symbol."""
        address = adapter_bnb._resolve_token("USDT")
        assert address == PANCAKESWAP_V3_TOKENS["bnb"]["USDT"]

    def test_resolve_token_by_address(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test resolving token by address passthrough."""
        test_address = "0x1234567890123456789012345678901234567890"
        address = adapter_bnb._resolve_token(test_address)
        assert address == test_address

    def test_resolve_token_unknown(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test resolving unknown token returns None."""
        address = adapter_bnb._resolve_token("UNKNOWN")
        assert address is None

    def test_resolve_token_case_insensitive(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test resolving token is case-insensitive."""
        address1 = adapter_bnb._resolve_token("USDT")
        address2 = adapter_bnb._resolve_token("usdt")
        assert address1 == address2

    def test_get_decimals_usdt_bnb(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test getting decimals for USDT on BNB (18 decimals)."""
        decimals = adapter_bnb._get_decimals("USDT")
        assert decimals == 18  # USDT on BSC is 18 decimals

    def test_get_decimals_wbnb(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test getting decimals for WBNB (18 decimals)."""
        decimals = adapter_bnb._get_decimals("WBNB")
        assert decimals == 18

    def test_get_decimals_wbtc(self, adapter_ethereum: PancakeSwapV3Adapter) -> None:
        """Test getting decimals for WBTC (8 decimals)."""
        decimals = adapter_ethereum._get_decimals("WBTC")
        assert decimals == 8

    def test_get_decimals_unknown(self, adapter_bnb: PancakeSwapV3Adapter) -> None:
        """Test getting decimals for unknown asset defaults to 18."""
        decimals = adapter_bnb._get_decimals("UNKNOWN")
        assert decimals == 18

    def test_pad_address(self) -> None:
        """Test address padding to 32 bytes."""
        addr = "0x1234567890123456789012345678901234567890"
        padded = PancakeSwapV3Adapter._pad_address(addr)
        assert len(padded) == 64
        assert padded == "0000000000000000000000001234567890123456789012345678901234567890"

    def test_pad_uint256(self) -> None:
        """Test uint256 padding to 32 bytes."""
        value = 1000
        padded = PancakeSwapV3Adapter._pad_uint256(value)
        assert len(padded) == 64
        assert padded == "00000000000000000000000000000000000000000000000000000000000003e8"

    def test_pad_uint256_large(self) -> None:
        """Test uint256 padding for large value."""
        # Test with max uint256 - 1
        value = 2**256 - 1
        padded = PancakeSwapV3Adapter._pad_uint256(value)
        assert len(padded) == 64
        assert padded == "f" * 64


# =============================================================================
# Token Decimals Tests
# =============================================================================


class TestTokenDecimals:
    """Tests for token decimals configuration."""

    def test_bsc_usdt_18_decimals(self) -> None:
        """Test that USDT on BSC uses 18 decimals."""
        assert TOKEN_DECIMALS["USDT"] == 18

    def test_bsc_usdc_18_decimals(self) -> None:
        """Test that USDC on BSC uses 18 decimals."""
        assert TOKEN_DECIMALS["USDC"] == 18

    def test_wbnb_18_decimals(self) -> None:
        """Test that WBNB uses 18 decimals."""
        assert TOKEN_DECIMALS["WBNB"] == 18

    def test_weth_18_decimals(self) -> None:
        """Test that WETH uses 18 decimals."""
        assert TOKEN_DECIMALS["WETH"] == 18

    def test_wbtc_8_decimals(self) -> None:
        """Test that WBTC uses 8 decimals."""
        assert TOKEN_DECIMALS["WBTC"] == 8


# =============================================================================
# Slippage Protection Tests
# =============================================================================


class TestSlippageProtection:
    """Tests for slippage protection functionality."""

    def test_slippage_protection_exact_input(self) -> None:
        """Test that swap_exact_input calculates non-zero min output with slippage.

        When amount_out_min is not provided, the adapter should:
        1. Calculate expected output based on price oracle
        2. Apply default slippage (0.5%) to get minimum output
        3. The result should have non-zero amount_out_min in calldata
        """
        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address="0x1234567890123456789012345678901234567890",
            price_provider={
                "USDT": Decimal("1"),
                "WBNB": Decimal("300"),
            },
        )
        adapter = PancakeSwapV3Adapter(config)

        result = adapter.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("300"),  # $300 worth of USDT
            # amount_out_min NOT specified - should be calculated
        )

        assert result.success is True
        assert result.tx_data is not None

        # Extract amount_out_min from calldata
        # Position: 2 + 8 (selector) + 64*6 = 394, length 64
        calldata = result.tx_data["data"]
        amount_out_min_hex = calldata[2 + 8 + 64 * 6 : 2 + 8 + 64 * 7]
        amount_out_min_wei = int(amount_out_min_hex, 16)

        # Should be non-zero (with slippage protection)
        assert amount_out_min_wei > 0, "amount_out_min should be non-zero with slippage protection"

        # Expected: ~1 WBNB minus fees and slippage
        # 300 USDT / 300 = 1 WBNB
        # After 0.25% fee: 0.9975 WBNB
        # After 0.5% slippage: 0.9975 * 0.995 = ~0.9925 WBNB
        # In wei (18 decimals): ~0.9925 * 10^18
        expected_min_approx = int(Decimal("0.99") * Decimal(10**18))
        assert amount_out_min_wei >= expected_min_approx, (
            f"amount_out_min should be at least {expected_min_approx} wei, got {amount_out_min_wei}"
        )

    def test_slippage_protection_requires_price_data(self) -> None:
        """Test that swap fails gracefully when price data is missing."""
        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address="0x1234567890123456789012345678901234567890",
            price_provider={
                "USDT": Decimal("1"),
                # WBNB not in price_provider
            },
        )
        adapter = PancakeSwapV3Adapter(config)

        result = adapter.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("300"),
            # No amount_out_min - should fail because WBNB price not available
        )

        assert result.success is False
        assert result.error is not None
        assert "Price data not available" in result.error

    def test_slippage_protection_with_explicit_amount_out_min(self) -> None:
        """Test that explicit amount_out_min bypasses price calculation."""
        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address="0x1234567890123456789012345678901234567890",
            price_provider={
                "USDT": Decimal("1"),
                # No WBNB price - but we're providing explicit amount_out_min
            },
        )
        adapter = PancakeSwapV3Adapter(config)

        result = adapter.swap_exact_input(
            token_in="USDT",
            token_out="WBNB",
            amount_in=Decimal("300"),
            amount_out_min=Decimal("0.95"),  # Explicit - should work even without price data
        )

        assert result.success is True
        assert result.tx_data is not None

        # Extract and verify amount_out_min
        calldata = result.tx_data["data"]
        amount_out_min_hex = calldata[2 + 8 + 64 * 6 : 2 + 8 + 64 * 7]
        amount_out_min_wei = int(amount_out_min_hex, 16)

        # Should be 0.95 * 10^18
        expected = int(Decimal("0.95") * Decimal(10**18))
        assert amount_out_min_wei == expected

    def test_slippage_protection_exact_output(self) -> None:
        """Test that swap_exact_output calculates bounded max input with slippage.

        When amount_in_max is not provided, the adapter should:
        1. Calculate expected input based on price oracle
        2. Apply default slippage (0.5%) to get maximum input
        3. The result should have a reasonable amount_in_max in calldata (not uint256 max)
        """
        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address="0x1234567890123456789012345678901234567890",
            price_provider={
                "USDT": Decimal("1"),
                "WBNB": Decimal("300"),
            },
        )
        adapter = PancakeSwapV3Adapter(config)

        result = adapter.swap_exact_output(
            token_in="USDT",
            token_out="WBNB",
            amount_out=Decimal("1"),  # Want 1 WBNB ($300)
            # amount_in_max NOT specified - should be calculated
        )

        assert result.success is True
        assert result.tx_data is not None

        # Extract amount_in_max from calldata
        # Position: 2 + 8 (selector) + 64*6 = 394, length 64 (same as amount_out_min position)
        calldata = result.tx_data["data"]
        amount_in_max_hex = calldata[2 + 8 + 64 * 6 : 2 + 8 + 64 * 7]
        amount_in_max_wei = int(amount_in_max_hex, 16)

        # Should NOT be max uint256 (that's the old unsafe behavior)
        max_uint256 = 2**256 - 1
        assert amount_in_max_wei < max_uint256, "amount_in_max should not be max uint256"
        assert amount_in_max_wei > 0, "amount_in_max should be non-zero"

        # Expected: ~300 USDT plus fees and slippage
        # 1 WBNB * 300 = 300 USDT
        # Before 0.25% fee: 300 / 0.9975 = ~300.75 USDT
        # After 0.5% slippage: ~300.75 * 1.005 = ~302.25 USDT
        # In wei (18 decimals): ~302.25 * 10^18
        expected_max_approx = int(Decimal("303") * Decimal(10**18))  # Upper bound
        expected_min_approx = int(Decimal("300") * Decimal(10**18))  # Lower bound
        assert amount_in_max_wei >= expected_min_approx, (
            f"amount_in_max should be at least {expected_min_approx} wei, got {amount_in_max_wei}"
        )
        assert amount_in_max_wei <= expected_max_approx, (
            f"amount_in_max should be at most {expected_max_approx} wei, got {amount_in_max_wei}"
        )

    def test_slippage_protection_exact_output_requires_price_data(self) -> None:
        """Test that swap_exact_output fails gracefully when price data is missing."""
        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address="0x1234567890123456789012345678901234567890",
            price_provider={
                "WBNB": Decimal("300"),
                # USDT not in price_provider
            },
        )
        adapter = PancakeSwapV3Adapter(config)

        result = adapter.swap_exact_output(
            token_in="USDT",
            token_out="WBNB",
            amount_out=Decimal("1"),
            # No amount_in_max - should fail because USDT price not available
        )

        assert result.success is False
        assert result.error is not None
        assert "Price data not available" in result.error

    def test_slippage_protection_exact_output_with_explicit_amount_in_max(self) -> None:
        """Test that explicit amount_in_max bypasses price calculation for exact output."""
        config = PancakeSwapV3Config(
            chain="bnb",
            wallet_address="0x1234567890123456789012345678901234567890",
            price_provider={
                "WBNB": Decimal("300"),
                # No USDT price - but we're providing explicit amount_in_max
            },
        )
        adapter = PancakeSwapV3Adapter(config)

        result = adapter.swap_exact_output(
            token_in="USDT",
            token_out="WBNB",
            amount_out=Decimal("1"),
            amount_in_max=Decimal("350"),  # Explicit - should work even without price data
        )

        assert result.success is True
        assert result.tx_data is not None

        # Extract and verify amount_in_max
        calldata = result.tx_data["data"]
        amount_in_max_hex = calldata[2 + 8 + 64 * 6 : 2 + 8 + 64 * 7]
        amount_in_max_wei = int(amount_in_max_hex, 16)

        # Should be 350 * 10^18
        expected = int(Decimal("350") * Decimal(10**18))
        assert amount_in_max_wei == expected
