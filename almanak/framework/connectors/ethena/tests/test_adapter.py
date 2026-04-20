"""Tests for Ethena Adapter.

This module contains unit tests for the EthenaAdapter class,
covering all operations including stake_usde, unstake_susde,
and configuration validation.
"""

from decimal import Decimal

import pytest

from ..adapter import (
    DEFAULT_GAS_ESTIMATES,
    ETHENA_ADDRESSES,
    ETHENA_COOLDOWN_ASSETS_SELECTOR,
    ETHENA_DEPOSIT_SELECTOR,
    ETHENA_UNSTAKE_SELECTOR,
    EthenaAdapter,
    EthenaConfig,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def config() -> EthenaConfig:
    """Create a test configuration for Ethereum."""
    return EthenaConfig(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
    )


@pytest.fixture
def adapter(config: EthenaConfig) -> EthenaAdapter:
    """Create a test adapter instance."""
    return EthenaAdapter(config)


# =============================================================================
# Configuration Tests
# =============================================================================


class TestEthenaConfigValidation:
    """Tests for EthenaConfig validation."""

    def test_valid_config_ethereum(self) -> None:
        """Test creating a valid configuration for Ethereum."""
        config = EthenaConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        assert config.chain == "ethereum"
        assert config.wallet_address == "0x1234567890123456789012345678901234567890"

    def test_invalid_chain(self) -> None:
        """Test that invalid chain raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            EthenaConfig(
                chain="invalid_chain",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_invalid_chain_arbitrum(self) -> None:
        """Test that unsupported chain (arbitrum) raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            EthenaConfig(
                chain="arbitrum",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_invalid_chain_bnb(self) -> None:
        """Test that unsupported chain (bnb) raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            EthenaConfig(
                chain="bnb",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_invalid_chain_optimism(self) -> None:
        """Test that unsupported chain (optimism) raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            EthenaConfig(
                chain="optimism",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_invalid_wallet_address_no_prefix(self) -> None:
        """Test that invalid wallet address without 0x prefix raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            EthenaConfig(
                chain="ethereum",
                wallet_address="1234567890123456789012345678901234567890",
            )

    def test_invalid_wallet_address_short(self) -> None:
        """Test that short wallet address raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            EthenaConfig(
                chain="ethereum",
                wallet_address="0x12345",
            )

    def test_invalid_wallet_address_long(self) -> None:
        """Test that long wallet address raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            EthenaConfig(
                chain="ethereum",
                wallet_address="0x12345678901234567890123456789012345678901234567890",
            )


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestEthenaAdapterInit:
    """Tests for EthenaAdapter initialization."""

    def test_init_ethereum(self, config: EthenaConfig) -> None:
        """Test adapter initialization for Ethereum."""
        adapter = EthenaAdapter(config)
        assert adapter.chain == "ethereum"
        assert adapter.wallet_address == config.wallet_address
        assert adapter.usde_address == ETHENA_ADDRESSES["ethereum"]["usde"]
        assert adapter.susde_address == ETHENA_ADDRESSES["ethereum"]["susde"]

    def test_init_sets_correct_usde_address(self, config: EthenaConfig) -> None:
        """Test that USDe address is correctly set."""
        adapter = EthenaAdapter(config)
        assert adapter.usde_address == "0x4c9EDD5852cd905f086C759E8383e09bff1E68B3"

    def test_init_sets_correct_susde_address(self, config: EthenaConfig) -> None:
        """Test that sUSDe address is correctly set."""
        adapter = EthenaAdapter(config)
        assert adapter.susde_address == "0x9D39A5DE30e57443BfF2A8307A4256c8797A3497"


# =============================================================================
# Stake USDe Transaction Tests
# =============================================================================


class TestStakeUsdeTransactionBuild:
    """Tests for stake_usde transaction building."""

    def test_stake_usde_basic(self, adapter: EthenaAdapter) -> None:
        """Test building a stake_usde transaction."""
        result = adapter.stake_usde(Decimal("1000.0"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == ETHENA_ADDRESSES["ethereum"]["susde"]
        assert result.tx_data["value"] == 0  # No ETH sent for staking
        assert result.tx_data["data"].startswith(ETHENA_DEPOSIT_SELECTOR)
        assert "Stake 1000.0 USDe" in result.description
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["stake"]

    def test_stake_usde_with_custom_receiver(self, adapter: EthenaAdapter) -> None:
        """Test stake_usde with custom receiver address."""
        receiver = "0x9876543210987654321098765432109876543210"
        result = adapter.stake_usde(Decimal("1000.0"), receiver=receiver)

        assert result.success is True
        assert result.tx_data is not None
        # Check that the receiver address is in the calldata
        calldata = result.tx_data["data"]
        assert receiver.lower().replace("0x", "") in calldata.lower()

    def test_stake_usde_with_default_receiver(self, adapter: EthenaAdapter) -> None:
        """Test stake_usde uses wallet_address as default receiver."""
        result = adapter.stake_usde(Decimal("1000.0"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        # Wallet address should be in calldata (lowercased, without 0x prefix)
        wallet_hex = adapter.wallet_address.lower().replace("0x", "")
        assert wallet_hex in calldata.lower()

    def test_stake_usde_calldata_structure(self, adapter: EthenaAdapter) -> None:
        """Test that stake_usde calldata has correct structure."""
        result = adapter.stake_usde(Decimal("1000.0"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure: selector (4 bytes) + amount (32 bytes) + receiver (32 bytes)
        # Total: 4 + 32 + 32 = 68 bytes = 136 hex chars + 2 for "0x" prefix = 138 chars
        assert calldata.startswith(ETHENA_DEPOSIT_SELECTOR)
        assert len(calldata) == 2 + 8 + 64 + 64  # 0x + selector + 2 params

    def test_stake_usde_amount_encoding(self, adapter: EthenaAdapter) -> None:
        """Test that stake_usde amount is correctly encoded."""
        result = adapter.stake_usde(Decimal("2500.5"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Extract amount from calldata (after selector)
        amount_hex = calldata[10:74]  # Skip "0x" + 8 char selector, take 64 chars
        amount_wei = int(amount_hex, 16)
        expected_wei = int(Decimal("2500.5") * 10**18)
        assert amount_wei == expected_wei

    def test_stake_usde_small_amount(self, adapter: EthenaAdapter) -> None:
        """Test stake_usde with small amount."""
        result = adapter.stake_usde(Decimal("0.001"))

        assert result.success is True
        assert result.tx_data is not None

    def test_stake_usde_large_amount(self, adapter: EthenaAdapter) -> None:
        """Test stake_usde with large amount."""
        result = adapter.stake_usde(Decimal("1000000"))

        assert result.success is True
        assert result.tx_data is not None

    def test_stake_usde_one_unit(self, adapter: EthenaAdapter) -> None:
        """Test stake_usde with 1 USDe."""
        result = adapter.stake_usde(Decimal("1"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        amount_hex = calldata[10:74]
        amount_wei = int(amount_hex, 16)
        assert amount_wei == 10**18


# =============================================================================
# Unstake sUSDe Transaction Tests
# =============================================================================


class TestUnstakeSusdeTransactionBuild:
    """Tests for unstake_susde transaction building."""

    def test_unstake_susde_basic(self, adapter: EthenaAdapter) -> None:
        """Test building an unstake_susde transaction."""
        result = adapter.unstake_susde(Decimal("1000.0"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == ETHENA_ADDRESSES["ethereum"]["susde"]
        assert result.tx_data["value"] == 0  # No ETH sent for unstaking
        assert result.tx_data["data"].startswith(ETHENA_COOLDOWN_ASSETS_SELECTOR)
        assert "cooldown" in result.description.lower()
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["unstake_cooldown"]

    def test_unstake_susde_calldata_structure(self, adapter: EthenaAdapter) -> None:
        """Test that unstake_susde calldata has correct structure."""
        result = adapter.unstake_susde(Decimal("1000.0"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure: selector (4 bytes) + amount (32 bytes)
        # Total: 4 + 32 = 36 bytes = 72 hex chars + 2 for "0x" prefix = 74 chars
        assert calldata.startswith(ETHENA_COOLDOWN_ASSETS_SELECTOR)
        assert len(calldata) == 2 + 8 + 64  # 0x + selector + 1 param

    def test_unstake_susde_amount_encoding(self, adapter: EthenaAdapter) -> None:
        """Test that unstake_susde amount is correctly encoded."""
        result = adapter.unstake_susde(Decimal("3500.75"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Extract amount from calldata (after selector)
        amount_hex = calldata[10:]  # Skip "0x" + 8 char selector
        amount_wei = int(amount_hex, 16)
        expected_wei = int(Decimal("3500.75") * 10**18)
        assert amount_wei == expected_wei

    def test_unstake_susde_small_amount(self, adapter: EthenaAdapter) -> None:
        """Test unstake_susde with small amount."""
        result = adapter.unstake_susde(Decimal("0.0001"))

        assert result.success is True
        assert result.tx_data is not None

    def test_unstake_susde_large_amount(self, adapter: EthenaAdapter) -> None:
        """Test unstake_susde with large amount."""
        result = adapter.unstake_susde(Decimal("10000000"))

        assert result.success is True
        assert result.tx_data is not None

    def test_unstake_susde_one_unit(self, adapter: EthenaAdapter) -> None:
        """Test unstake_susde with 1 sUSDe."""
        result = adapter.unstake_susde(Decimal("1"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        amount_hex = calldata[10:]
        amount_wei = int(amount_hex, 16)
        assert amount_wei == 10**18


# =============================================================================
# Complete Unstake Transaction Tests
# =============================================================================


class TestCompleteUnstakeTransactionBuild:
    """Tests for complete_unstake transaction building."""

    def test_complete_unstake_basic(self, adapter: EthenaAdapter) -> None:
        """Test building a complete_unstake transaction."""
        result = adapter.complete_unstake()

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == ETHENA_ADDRESSES["ethereum"]["susde"]
        assert result.tx_data["value"] == 0  # No ETH sent
        assert result.tx_data["data"].startswith(ETHENA_UNSTAKE_SELECTOR)
        assert "Complete unstaking" in result.description
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["unstake_complete"]

    def test_complete_unstake_with_custom_receiver(self, adapter: EthenaAdapter) -> None:
        """Test complete_unstake with custom receiver address."""
        receiver = "0x9876543210987654321098765432109876543210"
        result = adapter.complete_unstake(receiver=receiver)

        assert result.success is True
        assert result.tx_data is not None
        # Check that the receiver address is in the calldata
        calldata = result.tx_data["data"]
        assert receiver.lower().replace("0x", "") in calldata.lower()

    def test_complete_unstake_with_default_receiver(self, adapter: EthenaAdapter) -> None:
        """Test complete_unstake uses wallet_address as default receiver."""
        result = adapter.complete_unstake()

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        # Wallet address should be in calldata (lowercased, without 0x prefix)
        wallet_hex = adapter.wallet_address.lower().replace("0x", "")
        assert wallet_hex in calldata.lower()

    def test_complete_unstake_calldata_structure(self, adapter: EthenaAdapter) -> None:
        """Test that complete_unstake calldata has correct structure."""
        result = adapter.complete_unstake()

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure: selector (4 bytes) + receiver (32 bytes)
        # Total: 4 + 32 = 36 bytes = 72 hex chars + 2 for "0x" prefix = 74 chars
        assert calldata.startswith(ETHENA_UNSTAKE_SELECTOR)
        assert len(calldata) == 2 + 8 + 64  # 0x + selector + 1 param

    def test_complete_unstake_receiver_encoding(self, adapter: EthenaAdapter) -> None:
        """Test that receiver address is correctly encoded in calldata."""
        receiver = "0xABCDEF1234567890ABCDEF1234567890ABCDEF12"
        result = adapter.complete_unstake(receiver=receiver)

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Extract address from calldata (after selector)
        address_hex = calldata[10:]  # Skip "0x" + 8 char selector
        # Address should be left-padded with zeros to 32 bytes
        assert len(address_hex) == 64
        # Last 40 chars should be the address (lowercased)
        assert address_hex[24:] == receiver.lower().replace("0x", "")

    def test_complete_unstake_gas_estimate(self, adapter: EthenaAdapter) -> None:
        """Test that complete_unstake has correct gas estimate."""
        result = adapter.complete_unstake()

        assert result.success is True
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["unstake_complete"]
        assert result.gas_estimate == 100000

    def test_complete_unstake_targets_susde_contract(self, adapter: EthenaAdapter) -> None:
        """Test that complete_unstake targets the sUSDe contract."""
        result = adapter.complete_unstake()

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == ETHENA_ADDRESSES["ethereum"]["susde"]

    def test_complete_unstake_description_includes_receiver(self, adapter: EthenaAdapter) -> None:
        """Test that description includes truncated receiver address."""
        receiver = "0xABCDEF1234567890ABCDEF1234567890ABCDEF12"
        result = adapter.complete_unstake(receiver=receiver)

        assert result.success is True
        assert "0xABCDEF12" in result.description

    def test_complete_unstake_no_value(self, adapter: EthenaAdapter) -> None:
        """Test that complete_unstake does not send ETH."""
        result = adapter.complete_unstake()

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["value"] == 0


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_zero_amount_stake(self, adapter: EthenaAdapter) -> None:
        """Test stake_usde with zero amount still builds tx."""
        result = adapter.stake_usde(Decimal("0"))
        assert result.success is True
        assert result.tx_data is not None

    def test_zero_amount_unstake(self, adapter: EthenaAdapter) -> None:
        """Test unstake_susde with zero amount still builds tx."""
        result = adapter.unstake_susde(Decimal("0"))
        assert result.success is True
        assert result.tx_data is not None

    def test_very_small_fractional_amount(self, adapter: EthenaAdapter) -> None:
        """Test with very small fractional amount."""
        result = adapter.stake_usde(Decimal("0.000000000000000001"))  # 1 wei
        assert result.success is True
        assert result.tx_data is not None

    def test_large_amount_stake(self, adapter: EthenaAdapter) -> None:
        """Test stake_usde with very large amount."""
        result = adapter.stake_usde(Decimal("100000000"))  # 100 million USDe
        assert result.success is True
        assert result.tx_data is not None

    def test_large_amount_unstake(self, adapter: EthenaAdapter) -> None:
        """Test unstake_susde with very large amount."""
        result = adapter.unstake_susde(Decimal("100000000"))  # 100 million sUSDe
        assert result.success is True
        assert result.tx_data is not None

    def test_stake_and_unstake_same_amount(self, adapter: EthenaAdapter) -> None:
        """Test that stake and unstake can be called with same amount."""
        amount = Decimal("5000")

        stake_result = adapter.stake_usde(amount)
        unstake_result = adapter.unstake_susde(amount)

        assert stake_result.success is True
        assert unstake_result.success is True
        assert stake_result.tx_data is not None
        assert unstake_result.tx_data is not None

        # Both should target sUSDe contract
        assert stake_result.tx_data["to"] == unstake_result.tx_data["to"] == ETHENA_ADDRESSES["ethereum"]["susde"]


# =============================================================================
# Utility Method Tests
# =============================================================================


class TestUtilityMethods:
    """Tests for utility methods."""

    def test_pad_address_lowercase(self) -> None:
        """Test address padding with lowercase."""
        addr = "0x1234567890abcdef1234567890abcdef12345678"
        padded = EthenaAdapter._pad_address(addr)
        assert len(padded) == 64
        assert padded == "0000000000000000000000001234567890abcdef1234567890abcdef12345678"

    def test_pad_address_uppercase(self) -> None:
        """Test address padding with uppercase (should lowercase)."""
        addr = "0x1234567890ABCDEF1234567890ABCDEF12345678"
        padded = EthenaAdapter._pad_address(addr)
        assert len(padded) == 64
        assert padded == "0000000000000000000000001234567890abcdef1234567890abcdef12345678"

    def test_pad_address_zero_address(self) -> None:
        """Test padding the zero address."""
        addr = "0x0000000000000000000000000000000000000000"
        padded = EthenaAdapter._pad_address(addr)
        assert len(padded) == 64
        assert padded == "0" * 64

    def test_pad_uint256_small_value(self) -> None:
        """Test uint256 padding for small value."""
        value = 1000
        padded = EthenaAdapter._pad_uint256(value)
        assert len(padded) == 64
        assert padded == "00000000000000000000000000000000000000000000000000000000000003e8"

    def test_pad_uint256_one_usde(self) -> None:
        """Test uint256 padding for 1 USDe in wei (18 decimals)."""
        value = 10**18
        padded = EthenaAdapter._pad_uint256(value)
        assert len(padded) == 64
        # 1e18 = 0xde0b6b3a7640000
        assert padded == "0000000000000000000000000000000000000000000000000de0b6b3a7640000"

    def test_pad_uint256_zero(self) -> None:
        """Test uint256 padding for zero."""
        value = 0
        padded = EthenaAdapter._pad_uint256(value)
        assert len(padded) == 64
        assert padded == "0" * 64

    def test_pad_uint256_max_value(self) -> None:
        """Test uint256 padding for max uint256 value."""
        max_uint256 = 2**256 - 1
        padded = EthenaAdapter._pad_uint256(max_uint256)
        assert len(padded) == 64
        assert padded == "f" * 64


# =============================================================================
# Token Decimals Tests
# =============================================================================


class TestTokenDecimals:
    """Tests for token decimal handling."""

    def test_usde_has_18_decimals(self, adapter: EthenaAdapter) -> None:
        """Test that USDe amounts are converted with 18 decimals."""
        result = adapter.stake_usde(Decimal("1"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        amount_hex = calldata[10:74]
        amount_wei = int(amount_hex, 16)
        assert amount_wei == 10**18

    def test_susde_has_18_decimals(self, adapter: EthenaAdapter) -> None:
        """Test that sUSDe amounts are converted with 18 decimals."""
        result = adapter.unstake_susde(Decimal("1"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        amount_hex = calldata[10:]
        amount_wei = int(amount_hex, 16)
        assert amount_wei == 10**18

    def test_fractional_decimals_stake(self, adapter: EthenaAdapter) -> None:
        """Test fractional decimal handling for stake."""
        result = adapter.stake_usde(Decimal("1.123456789012345678"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        amount_hex = calldata[10:74]
        amount_wei = int(amount_hex, 16)
        expected_wei = int(Decimal("1.123456789012345678") * 10**18)
        assert amount_wei == expected_wei

    def test_fractional_decimals_unstake(self, adapter: EthenaAdapter) -> None:
        """Test fractional decimal handling for unstake."""
        result = adapter.unstake_susde(Decimal("1.123456789012345678"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        amount_hex = calldata[10:]
        amount_wei = int(amount_hex, 16)
        expected_wei = int(Decimal("1.123456789012345678") * 10**18)
        assert amount_wei == expected_wei


# =============================================================================
# Contract Address Tests
# =============================================================================


class TestContractAddresses:
    """Tests for contract address handling."""

    def test_stake_targets_susde_contract(self, adapter: EthenaAdapter) -> None:
        """Test that stake_usde targets the sUSDe contract."""
        result = adapter.stake_usde(Decimal("100"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == ETHENA_ADDRESSES["ethereum"]["susde"]

    def test_unstake_targets_susde_contract(self, adapter: EthenaAdapter) -> None:
        """Test that unstake_susde targets the sUSDe contract."""
        result = adapter.unstake_susde(Decimal("100"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == ETHENA_ADDRESSES["ethereum"]["susde"]

    def test_susde_address_is_checksum_valid(self, adapter: EthenaAdapter) -> None:
        """Test that sUSDe address is valid."""
        assert adapter.susde_address is not None
        assert adapter.susde_address.startswith("0x")
        assert len(adapter.susde_address) == 42

    def test_usde_address_is_checksum_valid(self, adapter: EthenaAdapter) -> None:
        """Test that USDe address is valid."""
        assert adapter.usde_address is not None
        assert adapter.usde_address.startswith("0x")
        assert len(adapter.usde_address) == 42


# =============================================================================
# Gas Estimate Tests
# =============================================================================


class TestGasEstimates:
    """Tests for gas estimate values."""

    def test_stake_gas_estimate(self, adapter: EthenaAdapter) -> None:
        """Test that stake has correct gas estimate."""
        result = adapter.stake_usde(Decimal("100"))

        assert result.success is True
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["stake"]
        assert result.gas_estimate == 150000

    def test_unstake_gas_estimate(self, adapter: EthenaAdapter) -> None:
        """Test that unstake has correct gas estimate."""
        result = adapter.unstake_susde(Decimal("100"))

        assert result.success is True
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["unstake_cooldown"]
        assert result.gas_estimate == 120000

    def test_gas_estimates_are_positive(self) -> None:
        """Test that all gas estimates are positive."""
        for operation, estimate in DEFAULT_GAS_ESTIMATES.items():
            assert estimate > 0, f"Gas estimate for {operation} should be positive"


# =============================================================================
# Compile Stake Intent Tests
# =============================================================================


class TestCompileStakeIntent:
    """Tests for compile_stake_intent method."""

    def test_compile_stake_intent_basic(self, adapter: EthenaAdapter) -> None:
        """Test compiling a StakeIntent for Ethena."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="ethena",
            token_in="USDe",
            amount=Decimal("1000.0"),
            receive_wrapped=True,  # Not used for Ethena but should not cause issues
            chain="ethereum",
        )
        bundle = adapter.compile_stake_intent(intent)

        # Should have 1 transaction: stake USDe -> sUSDe
        assert len(bundle.transactions) == 1
        assert bundle.intent_type == "STAKE"

        # Transaction should be stake
        stake_tx = bundle.transactions[0]
        assert stake_tx["action_type"] == "stake"
        assert stake_tx["to"] == ETHENA_ADDRESSES["ethereum"]["susde"]
        assert stake_tx["value"] == 0  # No ETH sent
        assert stake_tx["data"].startswith(ETHENA_DEPOSIT_SELECTOR)

        # Check metadata
        assert bundle.metadata["protocol"] == "ethena"
        assert bundle.metadata["output_token"] == "sUSDe"
        assert bundle.metadata["num_transactions"] == 1
        assert bundle.metadata["total_gas_estimate"] == DEFAULT_GAS_ESTIMATES["stake"]

    def test_compile_stake_intent_chain_amount_raises(self, adapter: EthenaAdapter) -> None:
        """Test that compile_stake_intent raises when amount='all' is not resolved."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="ethena",
            token_in="USDe",
            amount="all",
            chain="ethereum",
        )

        with pytest.raises(ValueError, match="amount='all' must be resolved"):
            adapter.compile_stake_intent(intent)

    def test_compile_stake_intent_small_amount(self, adapter: EthenaAdapter) -> None:
        """Test compiling a StakeIntent with small amount."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="ethena",
            token_in="USDe",
            amount=Decimal("0.001"),
            chain="ethereum",
        )
        bundle = adapter.compile_stake_intent(intent)

        assert len(bundle.transactions) == 1
        assert bundle.transactions[0]["value"] == 0
        assert bundle.metadata["amount"] == "0.001"

    def test_compile_stake_intent_large_amount(self, adapter: EthenaAdapter) -> None:
        """Test compiling a StakeIntent with large amount."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="ethena",
            token_in="USDe",
            amount=Decimal("10000000"),  # 10 million USDe
            chain="ethereum",
        )
        bundle = adapter.compile_stake_intent(intent)

        assert len(bundle.transactions) == 1
        assert bundle.metadata["amount"] == "10000000"

    def test_compile_stake_intent_metadata_completeness(self, adapter: EthenaAdapter) -> None:
        """Test that compile_stake_intent includes all expected metadata."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="ethena",
            token_in="USDe",
            amount=Decimal("1000.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_stake_intent(intent)

        # Check all expected metadata fields
        assert "intent_id" in bundle.metadata
        assert bundle.metadata["protocol"] == "ethena"
        assert bundle.metadata["token_in"] == "USDe"
        assert bundle.metadata["amount"] == "1000.0"
        assert bundle.metadata["output_token"] == "sUSDe"
        assert bundle.metadata["chain"] == "ethereum"
        assert bundle.metadata["total_gas_estimate"] == DEFAULT_GAS_ESTIMATES["stake"]
        assert bundle.metadata["num_transactions"] == 1

    def test_compile_stake_intent_gas_estimate(self, adapter: EthenaAdapter) -> None:
        """Test that compile_stake_intent has correct gas estimate."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="ethena",
            token_in="USDe",
            amount=Decimal("1000.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_stake_intent(intent)

        assert bundle.metadata["total_gas_estimate"] == DEFAULT_GAS_ESTIMATES["stake"]
        assert bundle.metadata["total_gas_estimate"] == 150000

    def test_compile_stake_intent_calldata_encoding(self, adapter: EthenaAdapter) -> None:
        """Test that compile_stake_intent has correctly encoded calldata."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="ethena",
            token_in="USDe",
            amount=Decimal("1000.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_stake_intent(intent)

        stake_tx = bundle.transactions[0]
        calldata = stake_tx["data"]

        # Calldata should start with deposit selector
        assert calldata.startswith(ETHENA_DEPOSIT_SELECTOR)

        # Extract and verify amount
        amount_hex = calldata[10:74]
        amount_wei = int(amount_hex, 16)
        expected_wei = int(Decimal("1000.0") * 10**18)
        assert amount_wei == expected_wei

    def test_compile_stake_intent_receiver_is_wallet(self, adapter: EthenaAdapter) -> None:
        """Test that compile_stake_intent uses wallet address as receiver."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="ethena",
            token_in="USDe",
            amount=Decimal("1000.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_stake_intent(intent)

        stake_tx = bundle.transactions[0]
        calldata = stake_tx["data"]

        # Extract receiver from calldata (last 64 chars after amount)
        receiver_hex = calldata[74:]
        # Last 40 chars should be the wallet address (lowercased)
        wallet_hex = adapter.wallet_address.lower().replace("0x", "")
        assert receiver_hex[24:] == wallet_hex


# =============================================================================
# Compile Unstake Intent Tests
# =============================================================================


class TestCompileUnstakeIntent:
    """Tests for compile_unstake_intent method."""

    def test_compile_unstake_intent_basic(self, adapter: EthenaAdapter) -> None:
        """Test compiling an UnstakeIntent for Ethena."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("1000.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        # Should have 1 transaction: initiate cooldown
        assert len(bundle.transactions) == 1
        assert bundle.intent_type == "UNSTAKE"

        # Transaction should be cooldown
        cooldown_tx = bundle.transactions[0]
        assert cooldown_tx["action_type"] == "cooldown"
        assert cooldown_tx["to"] == ETHENA_ADDRESSES["ethereum"]["susde"]
        assert cooldown_tx["value"] == 0  # No ETH sent
        assert cooldown_tx["data"].startswith(ETHENA_COOLDOWN_ASSETS_SELECTOR)

        # Check metadata
        assert bundle.metadata["protocol"] == "ethena"
        assert bundle.metadata["output_token"] == "USDe"
        assert bundle.metadata["num_transactions"] == 1
        assert bundle.metadata["total_gas_estimate"] == DEFAULT_GAS_ESTIMATES["unstake_cooldown"]
        assert bundle.metadata["cooldown_required"] is True

    def test_compile_unstake_intent_chain_amount_raises(self, adapter: EthenaAdapter) -> None:
        """Test that compile_unstake_intent raises when amount='all' is not resolved."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount="all",
            chain="ethereum",
        )

        with pytest.raises(ValueError, match="amount='all' must be resolved"):
            adapter.compile_unstake_intent(intent)

    def test_compile_unstake_intent_small_amount(self, adapter: EthenaAdapter) -> None:
        """Test compiling an UnstakeIntent with small amount."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("0.001"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        assert len(bundle.transactions) == 1
        assert bundle.transactions[0]["value"] == 0
        assert bundle.metadata["amount"] == "0.001"

    def test_compile_unstake_intent_large_amount(self, adapter: EthenaAdapter) -> None:
        """Test compiling an UnstakeIntent with large amount."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("10000000"),  # 10 million sUSDe
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        assert len(bundle.transactions) == 1
        assert bundle.metadata["amount"] == "10000000"

    def test_compile_unstake_intent_metadata_completeness(self, adapter: EthenaAdapter) -> None:
        """Test that compile_unstake_intent includes all expected metadata."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("1000.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        # Check all expected metadata fields
        assert "intent_id" in bundle.metadata
        assert bundle.metadata["protocol"] == "ethena"
        assert bundle.metadata["token_in"] == "sUSDe"
        assert bundle.metadata["amount"] == "1000.0"
        assert bundle.metadata["output_token"] == "USDe"
        assert bundle.metadata["chain"] == "ethereum"
        assert bundle.metadata["total_gas_estimate"] > 0
        assert bundle.metadata["num_transactions"] == 1
        assert bundle.metadata["cooldown_required"] is True
        assert "note" in bundle.metadata

    def test_compile_unstake_intent_gas_estimate(self, adapter: EthenaAdapter) -> None:
        """Test that compile_unstake_intent provides correct gas estimate."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("1000.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        # Gas should be cooldown gas estimate
        expected_gas = DEFAULT_GAS_ESTIMATES["unstake_cooldown"]
        assert bundle.metadata["total_gas_estimate"] == expected_gas
        assert bundle.transactions[0]["gas_estimate"] == expected_gas

    def test_compile_unstake_intent_calldata_encoding(self, adapter: EthenaAdapter) -> None:
        """Test that compile_unstake_intent encodes calldata correctly."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("1000.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        cooldown_tx = bundle.transactions[0]
        calldata = cooldown_tx["data"]

        # Calldata should start with cooldownAssets selector
        assert calldata.startswith(ETHENA_COOLDOWN_ASSETS_SELECTOR)

        # Extract and verify amount
        amount_hex = calldata[10:]  # Skip selector (0x + 8 hex chars)
        amount_wei = int(amount_hex, 16)
        expected_wei = int(Decimal("1000.0") * 10**18)
        assert amount_wei == expected_wei

    def test_compile_unstake_intent_targets_susde_contract(self, adapter: EthenaAdapter) -> None:
        """Test that compile_unstake_intent targets the sUSDe contract."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("1000.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        cooldown_tx = bundle.transactions[0]
        assert cooldown_tx["to"] == ETHENA_ADDRESSES["ethereum"]["susde"]

    def test_compile_unstake_intent_note_explains_cooldown(self, adapter: EthenaAdapter) -> None:
        """Test that compile_unstake_intent note explains the cooldown process."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("1000.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        # Note should explain that cooldown is initiated and completion is separate
        note = bundle.metadata.get("note", "")
        assert "cooldown" in note.lower()
        assert "complete_unstake" in note.lower() or "7 days" in note.lower()
