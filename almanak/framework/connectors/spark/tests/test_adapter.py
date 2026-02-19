"""Tests for Spark Adapter.

This module contains unit tests for the SparkAdapter class,
covering all operations including supply, borrow, repay, withdraw,
and configuration validation.
"""

from decimal import Decimal

import pytest

from ..adapter import (
    MAX_UINT256,
    SPARK_BORROW_SELECTOR,
    SPARK_POOL_ADDRESSES,
    SPARK_REPAY_SELECTOR,
    SPARK_SUPPLY_SELECTOR,
    SPARK_TOKEN_ADDRESSES,
    SPARK_VARIABLE_RATE_MODE,
    SPARK_WITHDRAW_SELECTOR,
    SparkAdapter,
    SparkConfig,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def config() -> SparkConfig:
    """Create a test configuration."""
    return SparkConfig(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
    )


@pytest.fixture
def adapter(config: SparkConfig) -> SparkAdapter:
    """Create a test adapter instance."""
    return SparkAdapter(config)


# =============================================================================
# Configuration Tests
# =============================================================================


class TestSparkConfigValidation:
    """Tests for SparkConfig validation."""

    def test_valid_config(self) -> None:
        """Test creating a valid configuration."""
        config = SparkConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        assert config.chain == "ethereum"
        assert config.wallet_address == "0x1234567890123456789012345678901234567890"
        assert config.default_slippage_bps == 50

    def test_valid_config_custom_slippage(self) -> None:
        """Test creating a valid configuration with custom slippage."""
        config = SparkConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            default_slippage_bps=100,
        )
        assert config.default_slippage_bps == 100

    def test_invalid_chain(self) -> None:
        """Test that invalid chain raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            SparkConfig(
                chain="invalid_chain",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_invalid_chain_arbitrum(self) -> None:
        """Test that unsupported chain (arbitrum) raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            SparkConfig(
                chain="arbitrum",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_invalid_wallet_address_no_prefix(self) -> None:
        """Test that invalid wallet address without 0x prefix raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            SparkConfig(
                chain="ethereum",
                wallet_address="1234567890123456789012345678901234567890",
            )

    def test_invalid_wallet_address_short(self) -> None:
        """Test that short wallet address raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            SparkConfig(
                chain="ethereum",
                wallet_address="0x12345",
            )

    def test_invalid_wallet_address_long(self) -> None:
        """Test that long wallet address raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            SparkConfig(
                chain="ethereum",
                wallet_address="0x12345678901234567890123456789012345678901234567890",
            )

    def test_invalid_slippage_negative(self) -> None:
        """Test that negative slippage raises error."""
        with pytest.raises(ValueError, match="Invalid slippage"):
            SparkConfig(
                chain="ethereum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=-1,
            )

    def test_invalid_slippage_too_high(self) -> None:
        """Test that slippage > 10000 raises error."""
        with pytest.raises(ValueError, match="Invalid slippage"):
            SparkConfig(
                chain="ethereum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=10001,
            )


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestSparkAdapterInit:
    """Tests for SparkAdapter initialization."""

    def test_init_ethereum(self, config: SparkConfig) -> None:
        """Test adapter initialization for Ethereum."""
        adapter = SparkAdapter(config)
        assert adapter.chain == "ethereum"
        assert adapter.pool_address == SPARK_POOL_ADDRESSES["ethereum"]
        assert adapter.wallet_address == config.wallet_address

    def test_init_token_addresses(self, adapter: SparkAdapter) -> None:
        """Test that token addresses are loaded correctly."""
        assert adapter.token_addresses == SPARK_TOKEN_ADDRESSES["ethereum"]
        assert "WETH" in adapter.token_addresses
        assert "USDC" in adapter.token_addresses
        assert "DAI" in adapter.token_addresses


# =============================================================================
# Supply Transaction Tests
# =============================================================================


class TestSupplyTransactionBuild:
    """Tests for supply transaction building."""

    def test_supply_usdc(self, adapter: SparkAdapter) -> None:
        """Test building a supply transaction for USDC."""
        result = adapter.supply("USDC", Decimal("1000"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter.pool_address
        assert result.tx_data["value"] == 0
        assert result.tx_data["data"].startswith(SPARK_SUPPLY_SELECTOR)
        assert "Supply 1000 USDC" in result.description

    def test_supply_dai(self, adapter: SparkAdapter) -> None:
        """Test building a supply transaction for DAI."""
        result = adapter.supply("DAI", Decimal("5000"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(SPARK_SUPPLY_SELECTOR)
        assert "Supply 5000 DAI" in result.description

    def test_supply_weth(self, adapter: SparkAdapter) -> None:
        """Test building a supply transaction for WETH."""
        result = adapter.supply("WETH", Decimal("5.5"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(SPARK_SUPPLY_SELECTOR)

    def test_supply_calldata_structure(self, adapter: SparkAdapter) -> None:
        """Test that supply calldata has correct structure."""
        result = adapter.supply("USDC", Decimal("1000"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure: selector (4 bytes) + asset (32 bytes) + amount (32 bytes)
        # + onBehalfOf (32 bytes) + referralCode (32 bytes)
        # Total: 4 + 32*4 = 132 bytes = 264 hex chars + 2 for "0x" prefix = 266 chars
        assert calldata.startswith(SPARK_SUPPLY_SELECTOR)
        # 2 chars for 0x + 8 chars for selector + 64*4 chars for params = 266 chars
        assert len(calldata) == 2 + 8 + 64 * 4

    def test_supply_unknown_asset(self, adapter: SparkAdapter) -> None:
        """Test supply with unknown asset fails."""
        result = adapter.supply("UNKNOWN_TOKEN", Decimal("100"))

        assert result.success is False
        assert result.error is not None
        assert "Unknown asset" in result.error

    def test_supply_on_behalf_of(self, adapter: SparkAdapter) -> None:
        """Test supply on behalf of another address."""
        other_address = "0x9876543210987654321098765432109876543210"
        result = adapter.supply("USDC", Decimal("1000"), on_behalf_of=other_address)

        assert result.success is True
        assert result.tx_data is not None

    def test_supply_case_insensitive(self, adapter: SparkAdapter) -> None:
        """Test that asset lookup is case-insensitive."""
        result1 = adapter.supply("USDC", Decimal("100"))
        result2 = adapter.supply("usdc", Decimal("100"))

        assert result1.success is True
        assert result2.success is True


# =============================================================================
# Borrow Transaction Tests
# =============================================================================


class TestBorrowTransactionBuild:
    """Tests for borrow transaction building."""

    def test_borrow_dai_variable(self, adapter: SparkAdapter) -> None:
        """Test building a variable rate borrow transaction for DAI."""
        result = adapter.borrow("DAI", Decimal("500"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter.pool_address
        assert result.tx_data["value"] == 0
        assert result.tx_data["data"].startswith(SPARK_BORROW_SELECTOR)
        assert "variable rate" in result.description

    def test_borrow_weth(self, adapter: SparkAdapter) -> None:
        """Test building a borrow transaction for WETH."""
        result = adapter.borrow("WETH", Decimal("1.5"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(SPARK_BORROW_SELECTOR)

    def test_borrow_calldata_structure(self, adapter: SparkAdapter) -> None:
        """Test that borrow calldata has correct structure."""
        result = adapter.borrow("DAI", Decimal("1000"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure: selector (4 bytes) + asset (32) + amount (32)
        # + interestRateMode (32) + referralCode (32) + onBehalfOf (32)
        # Total: 4 + 32*5 = 164 bytes = 328 hex chars + 2 for "0x" prefix = 330 chars
        assert calldata.startswith(SPARK_BORROW_SELECTOR)
        assert len(calldata) == 2 + 8 + 64 * 5

    def test_borrow_unknown_asset(self, adapter: SparkAdapter) -> None:
        """Test borrow with unknown asset fails."""
        result = adapter.borrow("UNKNOWN_TOKEN", Decimal("100"))

        assert result.success is False
        assert result.error is not None
        assert "Unknown asset" in result.error

    def test_borrow_custom_interest_rate_mode(self, adapter: SparkAdapter) -> None:
        """Test borrow with custom interest rate mode."""
        result = adapter.borrow(
            "DAI",
            Decimal("500"),
            interest_rate_mode=SPARK_VARIABLE_RATE_MODE,
        )

        assert result.success is True
        assert "variable rate" in result.description

    def test_borrow_on_behalf_of(self, adapter: SparkAdapter) -> None:
        """Test borrow on behalf of another address."""
        other_address = "0x9876543210987654321098765432109876543210"
        result = adapter.borrow("DAI", Decimal("500"), on_behalf_of=other_address)

        assert result.success is True
        assert result.tx_data is not None


# =============================================================================
# Repay Transaction Tests
# =============================================================================


class TestRepayTransactionBuild:
    """Tests for repay transaction building."""

    def test_repay_dai(self, adapter: SparkAdapter) -> None:
        """Test building a repay transaction for DAI."""
        result = adapter.repay("DAI", Decimal("250"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter.pool_address
        assert result.tx_data["value"] == 0
        assert result.tx_data["data"].startswith(SPARK_REPAY_SELECTOR)
        assert "Repay 250 DAI" in result.description

    def test_repay_weth(self, adapter: SparkAdapter) -> None:
        """Test building a repay transaction for WETH."""
        result = adapter.repay("WETH", Decimal("0.5"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(SPARK_REPAY_SELECTOR)
        assert "Repay 0.5 WETH" in result.description

    def test_repay_calldata_structure(self, adapter: SparkAdapter) -> None:
        """Test that repay calldata has correct structure."""
        result = adapter.repay("DAI", Decimal("1000"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure: selector (4 bytes) + asset (32) + amount (32)
        # + interestRateMode (32) + onBehalfOf (32)
        # Total: 4 + 32*4 = 132 bytes = 264 hex chars + 2 for "0x" prefix = 266 chars
        assert calldata.startswith(SPARK_REPAY_SELECTOR)
        assert len(calldata) == 2 + 8 + 64 * 4

    def test_repay_all(self, adapter: SparkAdapter) -> None:
        """Test repaying full debt with repay_all flag."""
        result = adapter.repay("DAI", Decimal("0"), repay_all=True)

        assert result.success is True
        assert result.tx_data is not None
        assert "full debt" in result.description

        # Verify MAX_UINT256 is encoded in the calldata
        calldata = result.tx_data["data"]
        # Amount is the second parameter after asset address
        # "0x" (2) + selector (8) + asset (64) + amount starts at position 74
        amount_hex = calldata[2 + 8 + 64 : 2 + 8 + 64 + 64]
        amount_value = int(amount_hex, 16)
        assert amount_value == MAX_UINT256

    def test_repay_unknown_asset(self, adapter: SparkAdapter) -> None:
        """Test repay with unknown asset fails."""
        result = adapter.repay("UNKNOWN_TOKEN", Decimal("100"))

        assert result.success is False
        assert result.error is not None
        assert "Unknown asset" in result.error

    def test_repay_on_behalf_of(self, adapter: SparkAdapter) -> None:
        """Test repay on behalf of another address."""
        other_address = "0x9876543210987654321098765432109876543210"
        result = adapter.repay("DAI", Decimal("250"), on_behalf_of=other_address)

        assert result.success is True
        assert result.tx_data is not None


# =============================================================================
# Withdraw Transaction Tests
# =============================================================================


class TestWithdrawTransactionBuild:
    """Tests for withdraw transaction building."""

    def test_withdraw_usdc(self, adapter: SparkAdapter) -> None:
        """Test building a withdraw transaction for USDC."""
        result = adapter.withdraw("USDC", Decimal("500"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter.pool_address
        assert result.tx_data["value"] == 0
        assert result.tx_data["data"].startswith(SPARK_WITHDRAW_SELECTOR)
        assert "Withdraw 500 USDC" in result.description

    def test_withdraw_dai(self, adapter: SparkAdapter) -> None:
        """Test building a withdraw transaction for DAI."""
        result = adapter.withdraw("DAI", Decimal("1000"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith(SPARK_WITHDRAW_SELECTOR)
        assert "Withdraw 1000 DAI" in result.description

    def test_withdraw_calldata_structure(self, adapter: SparkAdapter) -> None:
        """Test that withdraw calldata has correct structure."""
        result = adapter.withdraw("USDC", Decimal("500"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure: selector (4 bytes) + asset (32) + amount (32) + to (32)
        # Total: 4 + 32*3 = 100 bytes = 200 hex chars + 2 for "0x" prefix = 202 chars
        assert calldata.startswith(SPARK_WITHDRAW_SELECTOR)
        assert len(calldata) == 2 + 8 + 64 * 3

    def test_withdraw_all(self, adapter: SparkAdapter) -> None:
        """Test withdrawing all supplied assets."""
        result = adapter.withdraw("USDC", Decimal("0"), withdraw_all=True)

        assert result.success is True
        assert result.tx_data is not None
        assert "all USDC" in result.description

        # Verify MAX_UINT256 is encoded in the calldata
        calldata = result.tx_data["data"]
        # Amount is the second parameter after asset address
        # "0x" (2) + selector (8) + asset (64) + amount starts at position 74
        amount_hex = calldata[2 + 8 + 64 : 2 + 8 + 64 + 64]
        amount_value = int(amount_hex, 16)
        assert amount_value == MAX_UINT256

    def test_withdraw_unknown_asset(self, adapter: SparkAdapter) -> None:
        """Test withdraw with unknown asset fails."""
        result = adapter.withdraw("UNKNOWN_TOKEN", Decimal("100"))

        assert result.success is False
        assert result.error is not None
        assert "Unknown asset" in result.error

    def test_withdraw_to_different_address(self, adapter: SparkAdapter) -> None:
        """Test withdrawing to a different address."""
        other_address = "0x9876543210987654321098765432109876543210"
        result = adapter.withdraw("USDC", Decimal("500"), to=other_address)

        assert result.success is True
        assert result.tx_data is not None


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_zero_amount_supply(self, adapter: SparkAdapter) -> None:
        """Test supply with zero amount still builds tx."""
        result = adapter.supply("USDC", Decimal("0"))
        assert result.success is True

    def test_zero_amount_borrow(self, adapter: SparkAdapter) -> None:
        """Test borrow with zero amount still builds tx."""
        result = adapter.borrow("DAI", Decimal("0"))
        assert result.success is True

    def test_very_large_amount(self, adapter: SparkAdapter) -> None:
        """Test supply with very large amount."""
        result = adapter.supply("DAI", Decimal("1000000000"))
        assert result.success is True

    def test_fractional_amount(self, adapter: SparkAdapter) -> None:
        """Test supply with fractional amount."""
        result = adapter.supply("WETH", Decimal("0.000001"))
        assert result.success is True

    def test_asset_address_as_input(self, adapter: SparkAdapter) -> None:
        """Test using asset address directly instead of symbol."""
        usdc_address = SPARK_TOKEN_ADDRESSES["ethereum"]["USDC"]
        result = adapter.supply(usdc_address, Decimal("100"))

        assert result.success is True
        assert result.tx_data is not None


# =============================================================================
# Utility Method Tests
# =============================================================================


class TestUtilityMethods:
    """Tests for utility methods."""

    def test_resolve_asset_by_symbol(self, adapter: SparkAdapter) -> None:
        """Test resolving asset by symbol."""
        address = adapter._resolve_asset("USDC")
        assert address == SPARK_TOKEN_ADDRESSES["ethereum"]["USDC"]

    def test_resolve_asset_by_address(self, adapter: SparkAdapter) -> None:
        """Test resolving asset by address passthrough."""
        test_address = "0x1234567890123456789012345678901234567890"
        address = adapter._resolve_asset(test_address)
        assert address == test_address

    def test_resolve_asset_unknown(self, adapter: SparkAdapter) -> None:
        """Test resolving unknown asset returns None."""
        address = adapter._resolve_asset("UNKNOWN")
        assert address is None

    def test_get_decimals_usdc(self, adapter: SparkAdapter) -> None:
        """Test getting decimals for USDC (6 decimals)."""
        decimals = adapter._get_decimals("USDC")
        assert decimals == 6

    def test_get_decimals_dai(self, adapter: SparkAdapter) -> None:
        """Test getting decimals for DAI (18 decimals)."""
        decimals = adapter._get_decimals("DAI")
        assert decimals == 18

    def test_get_decimals_weth(self, adapter: SparkAdapter) -> None:
        """Test getting decimals for WETH (18 decimals)."""
        decimals = adapter._get_decimals("WETH")
        assert decimals == 18

    def test_get_decimals_unknown(self, adapter: SparkAdapter) -> None:
        """Test getting decimals for unknown asset defaults to 18."""
        decimals = adapter._get_decimals("UNKNOWN")
        assert decimals == 18

    def test_pad_address(self) -> None:
        """Test address padding to 32 bytes."""
        addr = "0x1234567890123456789012345678901234567890"
        padded = SparkAdapter._pad_address(addr)
        assert len(padded) == 64
        assert padded == "0000000000000000000000001234567890123456789012345678901234567890"

    def test_pad_uint256(self) -> None:
        """Test uint256 padding to 32 bytes."""
        value = 1000
        padded = SparkAdapter._pad_uint256(value)
        assert len(padded) == 64
        assert padded == "00000000000000000000000000000000000000000000000000000000000003e8"

    def test_pad_uint256_max(self) -> None:
        """Test uint256 padding for MAX_UINT256."""
        padded = SparkAdapter._pad_uint256(MAX_UINT256)
        assert len(padded) == 64
        assert padded == "f" * 64
