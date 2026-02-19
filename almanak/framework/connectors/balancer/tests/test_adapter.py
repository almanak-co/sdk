"""Unit tests for Balancer Flash Loan Adapter.

This module tests the BalancerFlashLoanAdapter including:
- Calldata generation
- Gas estimation
- Multi-token support
- ABI encoding

Usage:
    pytest src/connectors/balancer/tests/test_adapter.py -v
"""

import pytest

from ..adapter import (
    BALANCER_FLASH_LOAN_SELECTOR,
    BALANCER_VAULT_ADDRESSES,
    DEFAULT_GAS_ESTIMATES,
    BalancerFlashLoanAdapter,
)

# =============================================================================
# Test Constants
# =============================================================================

TEST_RECIPIENT = "0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266"
TEST_USDC = "0xaf88d065e77c8cC2239327C5EDb3A432268e5831"
TEST_WETH = "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1"


# =============================================================================
# Test Classes
# =============================================================================


class TestBalancerFlashLoanAdapter:
    """Tests for BalancerFlashLoanAdapter."""

    @pytest.fixture
    def adapter(self) -> BalancerFlashLoanAdapter:
        """Create a BalancerFlashLoanAdapter for Arbitrum."""
        return BalancerFlashLoanAdapter(chain="arbitrum")

    def test_adapter_initializes_with_correct_vault(
        self,
        adapter: BalancerFlashLoanAdapter,
    ) -> None:
        """Test that adapter initializes with correct Balancer Vault address."""
        assert adapter.chain == "arbitrum"
        assert adapter.vault_address == BALANCER_VAULT_ADDRESSES["arbitrum"]

    def test_get_vault_address(
        self,
        adapter: BalancerFlashLoanAdapter,
    ) -> None:
        """Test get_vault_address returns correct address."""
        vault = adapter.get_vault_address()
        assert vault == "0xBA12222222228d8Ba445958a75a0704d566BF2C8"

    def test_adapter_unsupported_chain_returns_zero_address(self) -> None:
        """Test that unsupported chain returns zero address."""
        adapter = BalancerFlashLoanAdapter(chain="fantom")
        assert adapter.vault_address == "0x0000000000000000000000000000000000000000"

    def test_flash_loan_calldata_starts_with_selector(
        self,
        adapter: BalancerFlashLoanAdapter,
    ) -> None:
        """Test that flash loan calldata starts with correct selector."""
        calldata = adapter.get_flash_loan_calldata(
            recipient=TEST_RECIPIENT,
            tokens=[TEST_USDC],
            amounts=[1000000000],  # 1000 USDC
        )

        # Calldata should start with selector (4 bytes)
        assert calldata[:4].hex() == BALANCER_FLASH_LOAN_SELECTOR[2:]

    def test_flash_loan_simple_calldata(
        self,
        adapter: BalancerFlashLoanAdapter,
    ) -> None:
        """Test single-token flash loan calldata generation."""
        calldata = adapter.get_flash_loan_simple_calldata(
            recipient=TEST_RECIPIENT,
            token=TEST_USDC,
            amount=1000000000,  # 1000 USDC
        )

        # Should use same format as multi-token but with single token
        assert calldata[:4].hex() == BALANCER_FLASH_LOAN_SELECTOR[2:]
        # Verify calldata length is reasonable
        assert len(calldata) > 4

    def test_flash_loan_multi_token_calldata(
        self,
        adapter: BalancerFlashLoanAdapter,
    ) -> None:
        """Test multi-token flash loan calldata generation."""
        calldata = adapter.get_flash_loan_calldata(
            recipient=TEST_RECIPIENT,
            tokens=[TEST_USDC, TEST_WETH],
            amounts=[1000000000, 500000000000000000],  # 1000 USDC, 0.5 WETH
        )

        assert calldata[:4].hex() == BALANCER_FLASH_LOAN_SELECTOR[2:]
        # Multi-token should be longer than single token
        single_calldata = adapter.get_flash_loan_simple_calldata(
            recipient=TEST_RECIPIENT,
            token=TEST_USDC,
            amount=1000000000,
        )
        assert len(calldata) > len(single_calldata)

    def test_flash_loan_with_user_data(
        self,
        adapter: BalancerFlashLoanAdapter,
    ) -> None:
        """Test flash loan calldata with user data."""
        user_data = b"test data for callback"
        calldata = adapter.get_flash_loan_simple_calldata(
            recipient=TEST_RECIPIENT,
            token=TEST_USDC,
            amount=1000000000,
            user_data=user_data,
        )

        # Calldata with user_data should be longer
        calldata_no_data = adapter.get_flash_loan_simple_calldata(
            recipient=TEST_RECIPIENT,
            token=TEST_USDC,
            amount=1000000000,
        )
        assert len(calldata) > len(calldata_no_data)

    def test_flash_loan_mismatched_arrays_raises(
        self,
        adapter: BalancerFlashLoanAdapter,
    ) -> None:
        """Test that mismatched token/amount arrays raise ValueError."""
        with pytest.raises(ValueError, match="same length"):
            adapter.get_flash_loan_calldata(
                recipient=TEST_RECIPIENT,
                tokens=[TEST_USDC, TEST_WETH],
                amounts=[1000000000],  # Only one amount for two tokens
            )

    def test_gas_estimate_flash_loan(
        self,
        adapter: BalancerFlashLoanAdapter,
    ) -> None:
        """Test multi-token flash loan gas estimation."""
        gas = adapter.estimate_flash_loan_gas()
        assert gas == DEFAULT_GAS_ESTIMATES["flash_loan"]
        assert gas == 400000

    def test_gas_estimate_flash_loan_simple(
        self,
        adapter: BalancerFlashLoanAdapter,
    ) -> None:
        """Test single-token flash loan gas estimation."""
        gas = adapter.estimate_flash_loan_simple_gas()
        assert gas == DEFAULT_GAS_ESTIMATES["flash_loan_simple"]
        assert gas == 250000


class TestBalancerVaultAddresses:
    """Tests for Balancer Vault address constants."""

    def test_all_chains_have_same_vault_address(self) -> None:
        """Test that Balancer Vault address is the same on all supported chains."""
        expected_address = "0xBA12222222228d8Ba445958a75a0704d566BF2C8"
        for chain, address in BALANCER_VAULT_ADDRESSES.items():
            assert address == expected_address, f"Chain {chain} has unexpected vault address"

    def test_supported_chains(self) -> None:
        """Test that all expected chains are supported."""
        expected_chains = {"ethereum", "arbitrum", "optimism", "polygon", "base"}
        actual_chains = set(BALANCER_VAULT_ADDRESSES.keys())
        assert expected_chains == actual_chains


class TestCalldataEncoding:
    """Tests for ABI encoding correctness."""

    @pytest.fixture
    def adapter(self) -> BalancerFlashLoanAdapter:
        return BalancerFlashLoanAdapter(chain="ethereum")

    def test_recipient_address_padded_correctly(
        self,
        adapter: BalancerFlashLoanAdapter,
    ) -> None:
        """Test that recipient address is padded to 32 bytes."""
        calldata = adapter.get_flash_loan_simple_calldata(
            recipient=TEST_RECIPIENT,
            token=TEST_USDC,
            amount=1000,
        )

        # After selector (4 bytes), first 32 bytes should be padded recipient
        recipient_in_calldata = calldata[4:36].hex()
        expected_padded = TEST_RECIPIENT.lower().replace("0x", "").zfill(64)
        assert recipient_in_calldata == expected_padded

    def test_amount_encoded_correctly(
        self,
        adapter: BalancerFlashLoanAdapter,
    ) -> None:
        """Test that amount is encoded as uint256."""
        amount = 12345678901234567890
        calldata = adapter.get_flash_loan_simple_calldata(
            recipient=TEST_RECIPIENT,
            token=TEST_USDC,
            amount=amount,
        )

        # Amount should be somewhere in the calldata
        amount_hex = hex(amount)[2:].zfill(64)
        assert amount_hex in calldata.hex()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
