"""Tests for Lido Adapter.

This module contains unit tests for the LidoAdapter class,
covering all operations including stake, wrap, unwrap,
and configuration validation.
"""

from decimal import Decimal

import pytest

from ..adapter import (
    DEFAULT_GAS_ESTIMATES,
    LIDO_ADDRESSES,
    LIDO_CLAIM_WITHDRAWALS_SELECTOR,
    LIDO_REQUEST_WITHDRAWALS_SELECTOR,
    LIDO_STAKE_SELECTOR,
    LIDO_UNWRAP_SELECTOR,
    LIDO_WRAP_SELECTOR,
    LidoAdapter,
    LidoConfig,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def config() -> LidoConfig:
    """Create a test configuration for Ethereum."""
    return LidoConfig(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
    )


@pytest.fixture
def adapter(config: LidoConfig) -> LidoAdapter:
    """Create a test adapter instance."""
    return LidoAdapter(config)


@pytest.fixture
def arbitrum_config() -> LidoConfig:
    """Create a test configuration for Arbitrum."""
    return LidoConfig(
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
    )


@pytest.fixture
def arbitrum_adapter(arbitrum_config: LidoConfig) -> LidoAdapter:
    """Create a test adapter instance for Arbitrum."""
    return LidoAdapter(arbitrum_config)


# =============================================================================
# Configuration Tests
# =============================================================================


class TestLidoConfigValidation:
    """Tests for LidoConfig validation."""

    def test_valid_config_ethereum(self) -> None:
        """Test creating a valid configuration for Ethereum."""
        config = LidoConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        assert config.chain == "ethereum"
        assert config.wallet_address == "0x1234567890123456789012345678901234567890"

    def test_valid_config_arbitrum(self) -> None:
        """Test creating a valid configuration for Arbitrum."""
        config = LidoConfig(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        assert config.chain == "arbitrum"

    def test_valid_config_optimism(self) -> None:
        """Test creating a valid configuration for Optimism."""
        config = LidoConfig(
            chain="optimism",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        assert config.chain == "optimism"

    def test_valid_config_polygon(self) -> None:
        """Test creating a valid configuration for Polygon."""
        config = LidoConfig(
            chain="polygon",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        assert config.chain == "polygon"

    def test_invalid_chain(self) -> None:
        """Test that invalid chain raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            LidoConfig(
                chain="invalid_chain",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_invalid_chain_bnb(self) -> None:
        """Test that unsupported chain (bnb) raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            LidoConfig(
                chain="bnb",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_invalid_wallet_address_no_prefix(self) -> None:
        """Test that invalid wallet address without 0x prefix raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            LidoConfig(
                chain="ethereum",
                wallet_address="1234567890123456789012345678901234567890",
            )

    def test_invalid_wallet_address_short(self) -> None:
        """Test that short wallet address raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            LidoConfig(
                chain="ethereum",
                wallet_address="0x12345",
            )

    def test_invalid_wallet_address_long(self) -> None:
        """Test that long wallet address raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            LidoConfig(
                chain="ethereum",
                wallet_address="0x12345678901234567890123456789012345678901234567890",
            )


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestLidoAdapterInit:
    """Tests for LidoAdapter initialization."""

    def test_init_ethereum(self, config: LidoConfig) -> None:
        """Test adapter initialization for Ethereum."""
        adapter = LidoAdapter(config)
        assert adapter.chain == "ethereum"
        assert adapter.wallet_address == config.wallet_address
        assert adapter.steth_address == LIDO_ADDRESSES["ethereum"]["steth"]
        assert adapter.wsteth_address == LIDO_ADDRESSES["ethereum"]["wsteth"]
        assert adapter.withdrawal_queue_address == LIDO_ADDRESSES["ethereum"]["withdrawal_queue"]

    def test_init_arbitrum(self, arbitrum_config: LidoConfig) -> None:
        """Test adapter initialization for Arbitrum."""
        adapter = LidoAdapter(arbitrum_config)
        assert adapter.chain == "arbitrum"
        assert adapter.steth_address is None  # No stETH on L2s
        assert adapter.wsteth_address == LIDO_ADDRESSES["arbitrum"]["wsteth"]
        assert adapter.withdrawal_queue_address is None

    def test_init_optimism(self) -> None:
        """Test adapter initialization for Optimism."""
        config = LidoConfig(
            chain="optimism",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = LidoAdapter(config)
        assert adapter.chain == "optimism"
        assert adapter.steth_address is None
        assert adapter.wsteth_address == LIDO_ADDRESSES["optimism"]["wsteth"]

    def test_init_polygon(self) -> None:
        """Test adapter initialization for Polygon."""
        config = LidoConfig(
            chain="polygon",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = LidoAdapter(config)
        assert adapter.chain == "polygon"
        assert adapter.steth_address is None
        assert adapter.wsteth_address == LIDO_ADDRESSES["polygon"]["wsteth"]


# =============================================================================
# Stake Transaction Tests
# =============================================================================


class TestStakeTransactionBuild:
    """Tests for stake transaction building."""

    def test_stake_eth(self, adapter: LidoAdapter) -> None:
        """Test building a stake transaction."""
        result = adapter.stake(Decimal("1.0"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == LIDO_ADDRESSES["ethereum"]["steth"]
        assert result.tx_data["value"] == 10**18  # 1 ETH in wei
        assert result.tx_data["data"].startswith(LIDO_STAKE_SELECTOR)
        assert "Stake 1.0 ETH" in result.description
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["stake"]

    def test_stake_with_custom_referral(self, adapter: LidoAdapter) -> None:
        """Test stake with custom referral address."""
        referral = "0x9876543210987654321098765432109876543210"
        result = adapter.stake(Decimal("1.0"), referral=referral)

        assert result.success is True
        assert result.tx_data is not None
        # Check that the referral address is in the calldata
        calldata = result.tx_data["data"]
        assert referral.lower().replace("0x", "") in calldata.lower()

    def test_stake_with_default_referral(self, adapter: LidoAdapter) -> None:
        """Test stake uses zero address as default referral."""
        result = adapter.stake(Decimal("1.0"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        # Zero address should be padded in calldata
        assert "0000000000000000000000000000000000000000" in calldata

    def test_stake_calldata_structure(self, adapter: LidoAdapter) -> None:
        """Test that stake calldata has correct structure."""
        result = adapter.stake(Decimal("1.0"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure: selector (4 bytes) + referral address (32 bytes)
        # Total: 4 + 32 = 36 bytes = 72 hex chars + 2 for "0x" prefix = 74 chars
        assert calldata.startswith(LIDO_STAKE_SELECTOR)
        assert len(calldata) == 2 + 8 + 64  # 0x + selector + 1 param

    def test_stake_not_available_on_arbitrum(self, arbitrum_adapter: LidoAdapter) -> None:
        """Test that staking is not available on L2s."""
        result = arbitrum_adapter.stake(Decimal("1.0"))

        assert result.success is False
        assert result.error is not None
        assert "not available on arbitrum" in result.error.lower()

    def test_stake_not_available_on_optimism(self) -> None:
        """Test that staking is not available on Optimism."""
        config = LidoConfig(
            chain="optimism",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = LidoAdapter(config)
        result = adapter.stake(Decimal("1.0"))

        assert result.success is False
        assert result.error is not None
        assert "not available on optimism" in result.error.lower()

    def test_stake_small_amount(self, adapter: LidoAdapter) -> None:
        """Test stake with small amount."""
        result = adapter.stake(Decimal("0.001"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["value"] == 10**15  # 0.001 ETH in wei

    def test_stake_large_amount(self, adapter: LidoAdapter) -> None:
        """Test stake with large amount."""
        result = adapter.stake(Decimal("1000"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["value"] == 1000 * 10**18


# =============================================================================
# Wrap Transaction Tests
# =============================================================================


class TestWrapTransactionBuild:
    """Tests for wrap transaction building."""

    def test_wrap_steth(self, adapter: LidoAdapter) -> None:
        """Test building a wrap transaction."""
        result = adapter.wrap(Decimal("1.0"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == LIDO_ADDRESSES["ethereum"]["wsteth"]
        assert result.tx_data["value"] == 0  # No ETH sent for wrap
        assert result.tx_data["data"].startswith(LIDO_WRAP_SELECTOR)
        assert "Wrap 1.0 stETH" in result.description
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["wrap"]

    def test_wrap_calldata_structure(self, adapter: LidoAdapter) -> None:
        """Test that wrap calldata has correct structure."""
        result = adapter.wrap(Decimal("1.0"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure: selector (4 bytes) + amount (32 bytes)
        # Total: 4 + 32 = 36 bytes = 72 hex chars + 2 for "0x" prefix = 74 chars
        assert calldata.startswith(LIDO_WRAP_SELECTOR)
        assert len(calldata) == 2 + 8 + 64  # 0x + selector + 1 param

    def test_wrap_amount_encoding(self, adapter: LidoAdapter) -> None:
        """Test that wrap amount is correctly encoded."""
        result = adapter.wrap(Decimal("2.5"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Extract amount from calldata (after selector)
        amount_hex = calldata[10:]  # Skip "0x" + 8 char selector
        amount_wei = int(amount_hex, 16)
        expected_wei = int(Decimal("2.5") * 10**18)
        assert amount_wei == expected_wei

    def test_wrap_on_arbitrum(self, arbitrum_adapter: LidoAdapter) -> None:
        """Test that wrap works on Arbitrum (has wstETH address)."""
        # Note: On L2s, users would receive wstETH via bridge, but wrap still works
        # if they somehow have stETH (e.g., bridged or transferred)
        result = arbitrum_adapter.wrap(Decimal("1.0"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == LIDO_ADDRESSES["arbitrum"]["wsteth"]

    def test_wrap_small_amount(self, adapter: LidoAdapter) -> None:
        """Test wrap with small amount."""
        result = adapter.wrap(Decimal("0.0001"))

        assert result.success is True
        assert result.tx_data is not None

    def test_wrap_large_amount(self, adapter: LidoAdapter) -> None:
        """Test wrap with large amount."""
        result = adapter.wrap(Decimal("10000"))

        assert result.success is True
        assert result.tx_data is not None


# =============================================================================
# Unwrap Transaction Tests
# =============================================================================


class TestUnwrapTransactionBuild:
    """Tests for unwrap transaction building."""

    def test_unwrap_wsteth(self, adapter: LidoAdapter) -> None:
        """Test building an unwrap transaction."""
        result = adapter.unwrap(Decimal("1.0"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == LIDO_ADDRESSES["ethereum"]["wsteth"]
        assert result.tx_data["value"] == 0  # No ETH sent for unwrap
        assert result.tx_data["data"].startswith(LIDO_UNWRAP_SELECTOR)
        assert "Unwrap 1.0 wstETH" in result.description
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["unwrap"]

    def test_unwrap_calldata_structure(self, adapter: LidoAdapter) -> None:
        """Test that unwrap calldata has correct structure."""
        result = adapter.unwrap(Decimal("1.0"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure: selector (4 bytes) + amount (32 bytes)
        # Total: 4 + 32 = 36 bytes = 72 hex chars + 2 for "0x" prefix = 74 chars
        assert calldata.startswith(LIDO_UNWRAP_SELECTOR)
        assert len(calldata) == 2 + 8 + 64  # 0x + selector + 1 param

    def test_unwrap_amount_encoding(self, adapter: LidoAdapter) -> None:
        """Test that unwrap amount is correctly encoded."""
        result = adapter.unwrap(Decimal("3.5"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Extract amount from calldata (after selector)
        amount_hex = calldata[10:]  # Skip "0x" + 8 char selector
        amount_wei = int(amount_hex, 16)
        expected_wei = int(Decimal("3.5") * 10**18)
        assert amount_wei == expected_wei

    def test_unwrap_on_arbitrum(self, arbitrum_adapter: LidoAdapter) -> None:
        """Test that unwrap works on Arbitrum."""
        result = arbitrum_adapter.unwrap(Decimal("1.0"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == LIDO_ADDRESSES["arbitrum"]["wsteth"]

    def test_unwrap_on_optimism(self) -> None:
        """Test that unwrap works on Optimism."""
        config = LidoConfig(
            chain="optimism",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = LidoAdapter(config)
        result = adapter.unwrap(Decimal("1.0"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == LIDO_ADDRESSES["optimism"]["wsteth"]

    def test_unwrap_on_polygon(self) -> None:
        """Test that unwrap works on Polygon."""
        config = LidoConfig(
            chain="polygon",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = LidoAdapter(config)
        result = adapter.unwrap(Decimal("1.0"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == LIDO_ADDRESSES["polygon"]["wsteth"]


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_zero_amount_stake(self, adapter: LidoAdapter) -> None:
        """Test stake with zero amount still builds tx."""
        result = adapter.stake(Decimal("0"))
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["value"] == 0

    def test_zero_amount_wrap(self, adapter: LidoAdapter) -> None:
        """Test wrap with zero amount still builds tx."""
        result = adapter.wrap(Decimal("0"))
        assert result.success is True
        assert result.tx_data is not None

    def test_zero_amount_unwrap(self, adapter: LidoAdapter) -> None:
        """Test unwrap with zero amount still builds tx."""
        result = adapter.unwrap(Decimal("0"))
        assert result.success is True
        assert result.tx_data is not None

    def test_very_small_fractional_amount(self, adapter: LidoAdapter) -> None:
        """Test with very small fractional amount."""
        result = adapter.stake(Decimal("0.000000000000000001"))  # 1 wei
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["value"] == 1

    def test_large_amount_stake(self, adapter: LidoAdapter) -> None:
        """Test stake with very large amount."""
        result = adapter.stake(Decimal("1000000"))  # 1 million ETH
        assert result.success is True
        assert result.tx_data is not None


# =============================================================================
# Utility Method Tests
# =============================================================================


class TestUtilityMethods:
    """Tests for utility methods."""

    def test_pad_address_lowercase(self) -> None:
        """Test address padding with lowercase."""
        addr = "0x1234567890abcdef1234567890abcdef12345678"
        padded = LidoAdapter._pad_address(addr)
        assert len(padded) == 64
        assert padded == "0000000000000000000000001234567890abcdef1234567890abcdef12345678"

    def test_pad_address_uppercase(self) -> None:
        """Test address padding with uppercase (should lowercase)."""
        addr = "0x1234567890ABCDEF1234567890ABCDEF12345678"
        padded = LidoAdapter._pad_address(addr)
        assert len(padded) == 64
        assert padded == "0000000000000000000000001234567890abcdef1234567890abcdef12345678"

    def test_pad_address_zero_address(self) -> None:
        """Test padding the zero address."""
        addr = "0x0000000000000000000000000000000000000000"
        padded = LidoAdapter._pad_address(addr)
        assert len(padded) == 64
        assert padded == "0" * 64

    def test_pad_uint256_small_value(self) -> None:
        """Test uint256 padding for small value."""
        value = 1000
        padded = LidoAdapter._pad_uint256(value)
        assert len(padded) == 64
        assert padded == "00000000000000000000000000000000000000000000000000000000000003e8"

    def test_pad_uint256_one_eth(self) -> None:
        """Test uint256 padding for 1 ETH in wei."""
        value = 10**18
        padded = LidoAdapter._pad_uint256(value)
        assert len(padded) == 64
        # 1e18 = 0xde0b6b3a7640000
        assert padded == "0000000000000000000000000000000000000000000000000de0b6b3a7640000"

    def test_pad_uint256_zero(self) -> None:
        """Test uint256 padding for zero."""
        value = 0
        padded = LidoAdapter._pad_uint256(value)
        assert len(padded) == 64
        assert padded == "0" * 64


# =============================================================================
# Token Decimals Tests
# =============================================================================


class TestTokenDecimals:
    """Tests for token decimal handling."""

    def test_eth_has_18_decimals(self, adapter: LidoAdapter) -> None:
        """Test that ETH amounts are converted with 18 decimals."""
        result = adapter.stake(Decimal("1"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["value"] == 10**18

    def test_steth_has_18_decimals(self, adapter: LidoAdapter) -> None:
        """Test that stETH amounts are converted with 18 decimals."""
        result = adapter.wrap(Decimal("1"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        amount_hex = calldata[10:]
        amount_wei = int(amount_hex, 16)
        assert amount_wei == 10**18

    def test_wsteth_has_18_decimals(self, adapter: LidoAdapter) -> None:
        """Test that wstETH amounts are converted with 18 decimals."""
        result = adapter.unwrap(Decimal("1"))

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        amount_hex = calldata[10:]
        amount_wei = int(amount_hex, 16)
        assert amount_wei == 10**18


# =============================================================================
# Request Withdrawal Transaction Tests
# =============================================================================


class TestRequestWithdrawalTransactionBuild:
    """Tests for request_withdrawal transaction building."""

    def test_request_withdrawal_single_amount(self, adapter: LidoAdapter) -> None:
        """Test building a request_withdrawal transaction with single amount."""
        amounts = [Decimal("1.0")]
        result = adapter.request_withdrawal(amounts)

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == LIDO_ADDRESSES["ethereum"]["withdrawal_queue"]
        assert result.tx_data["value"] == 0
        assert result.tx_data["data"].startswith(LIDO_REQUEST_WITHDRAWALS_SELECTOR)
        assert "Request withdrawal of 1.0 stETH" in result.description
        assert "(1 request(s))" in result.description
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["request_withdrawal"]

    def test_request_withdrawal_multiple_amounts(self, adapter: LidoAdapter) -> None:
        """Test building a request_withdrawal transaction with multiple amounts."""
        amounts = [Decimal("1.0"), Decimal("2.0"), Decimal("3.0")]
        result = adapter.request_withdrawal(amounts)

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == LIDO_ADDRESSES["ethereum"]["withdrawal_queue"]
        assert "Request withdrawal of 6.0 stETH" in result.description
        assert "(3 request(s))" in result.description
        # Gas should increase for multiple requests
        expected_gas = DEFAULT_GAS_ESTIMATES["request_withdrawal"] + 2 * 30000
        assert result.gas_estimate == expected_gas

    def test_request_withdrawal_with_explicit_owner(self, adapter: LidoAdapter) -> None:
        """Test request_withdrawal with explicit owner address."""
        custom_owner = "0xabcdef1234567890abcdef1234567890abcdef12"
        amounts = [Decimal("1.0")]
        result = adapter.request_withdrawal(amounts, owner=custom_owner)

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        # Owner address should be in the calldata (padded)
        assert custom_owner.lower().replace("0x", "") in calldata.lower()

    def test_request_withdrawal_uses_wallet_address_as_default_owner(self, adapter: LidoAdapter) -> None:
        """Test that request_withdrawal uses wallet_address as default owner."""
        amounts = [Decimal("1.0")]
        result = adapter.request_withdrawal(amounts)

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        # Wallet address should be in the calldata
        expected_addr = adapter.wallet_address.lower().replace("0x", "")
        assert expected_addr in calldata.lower()

    def test_request_withdrawal_calldata_structure(self, adapter: LidoAdapter) -> None:
        """Test that request_withdrawal calldata has correct structure."""
        amounts = [Decimal("1.0")]
        result = adapter.request_withdrawal(amounts)

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure for requestWithdrawals(uint256[],address):
        # - selector: 4 bytes (8 hex chars)
        # - offset to amounts array: 32 bytes (64 hex chars)
        # - owner: 32 bytes (64 hex chars)
        # - array length: 32 bytes (64 hex chars)
        # - array element: 32 bytes (64 hex chars)
        # Total: 4 + 32 + 32 + 32 + 32 = 132 bytes = 264 hex chars
        expected_len = 2 + 8 + 64 + 64 + 64 + 64  # 0x + selector + 4 params
        assert len(calldata) == expected_len

    def test_request_withdrawal_amount_encoding(self, adapter: LidoAdapter) -> None:
        """Test that request_withdrawal amounts are correctly encoded in wei."""
        amounts = [Decimal("2.5")]
        result = adapter.request_withdrawal(amounts)

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Extract the amount from calldata
        # Structure: selector(8) + offset(64) + owner(64) + length(64) + amount(64)
        amount_hex = calldata[2 + 8 + 64 + 64 + 64 :][:64]
        amount_wei = int(amount_hex, 16)
        expected_wei = int(Decimal("2.5") * 10**18)
        assert amount_wei == expected_wei

    def test_request_withdrawal_not_available_on_arbitrum(self, arbitrum_adapter: LidoAdapter) -> None:
        """Test that request_withdrawal is not available on L2s."""
        amounts = [Decimal("1.0")]
        result = arbitrum_adapter.request_withdrawal(amounts)

        assert result.success is False
        assert result.error is not None
        assert "not available on arbitrum" in result.error.lower()

    def test_request_withdrawal_not_available_on_optimism(self) -> None:
        """Test that request_withdrawal is not available on Optimism."""
        config = LidoConfig(
            chain="optimism",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = LidoAdapter(config)
        amounts = [Decimal("1.0")]
        result = adapter.request_withdrawal(amounts)

        assert result.success is False
        assert result.error is not None
        assert "not available on optimism" in result.error.lower()

    def test_request_withdrawal_empty_amounts(self, adapter: LidoAdapter) -> None:
        """Test that request_withdrawal fails with empty amounts list."""
        amounts: list[Decimal] = []
        result = adapter.request_withdrawal(amounts)

        assert result.success is False
        assert result.error is not None
        assert "at least one withdrawal amount" in result.error.lower()

    def test_request_withdrawal_small_amounts(self, adapter: LidoAdapter) -> None:
        """Test request_withdrawal with small amounts."""
        amounts = [Decimal("0.001"), Decimal("0.002")]
        result = adapter.request_withdrawal(amounts)

        assert result.success is True
        assert result.tx_data is not None
        assert "0.003 stETH" in result.description

    def test_request_withdrawal_large_amount(self, adapter: LidoAdapter) -> None:
        """Test request_withdrawal with large amount."""
        amounts = [Decimal("1000")]
        result = adapter.request_withdrawal(amounts)

        assert result.success is True
        assert result.tx_data is not None


# =============================================================================
# Claim Withdrawals Transaction Tests
# =============================================================================


class TestClaimWithdrawalsTransactionBuild:
    """Tests for claim_withdrawals transaction building."""

    def test_claim_withdrawals_single_request(self, adapter: LidoAdapter) -> None:
        """Test building a claim_withdrawals transaction with single request."""
        request_ids = [12345]
        result = adapter.claim_withdrawals(request_ids)

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == LIDO_ADDRESSES["ethereum"]["withdrawal_queue"]
        assert result.tx_data["value"] == 0
        assert result.tx_data["data"].startswith(LIDO_CLAIM_WITHDRAWALS_SELECTOR)
        assert "Claim 1 withdrawal request(s)" in result.description
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["claim_withdrawal"]

    def test_claim_withdrawals_multiple_requests(self, adapter: LidoAdapter) -> None:
        """Test building a claim_withdrawals transaction with multiple requests."""
        request_ids = [100, 200, 300]
        result = adapter.claim_withdrawals(request_ids)

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == LIDO_ADDRESSES["ethereum"]["withdrawal_queue"]
        assert "Claim 3 withdrawal request(s)" in result.description
        # Gas should increase for multiple requests
        expected_gas = DEFAULT_GAS_ESTIMATES["claim_withdrawal"] + 2 * 20000
        assert result.gas_estimate == expected_gas

    def test_claim_withdrawals_with_hints(self, adapter: LidoAdapter) -> None:
        """Test claim_withdrawals with explicit hints."""
        request_ids = [100, 200]
        hints = [5, 6]
        result = adapter.claim_withdrawals(request_ids, hints=hints)

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]
        # Verify hints are encoded in calldata
        # hints are encoded after requestIds array
        # Hint value 5 = 0x05 and 6 = 0x06
        assert "0000000000000000000000000000000000000000000000000000000000000005" in calldata
        assert "0000000000000000000000000000000000000000000000000000000000000006" in calldata

    def test_claim_withdrawals_without_hints_uses_zeros(self, adapter: LidoAdapter) -> None:
        """Test that claim_withdrawals uses zeros when hints not provided."""
        request_ids = [100]
        result = adapter.claim_withdrawals(request_ids)

        assert result.success is True
        assert result.tx_data is not None
        # Should still have hints array with same length as request_ids

    def test_claim_withdrawals_calldata_structure(self, adapter: LidoAdapter) -> None:
        """Test that claim_withdrawals calldata has correct structure."""
        request_ids = [100]
        hints = [5]
        result = adapter.claim_withdrawals(request_ids, hints=hints)

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Calldata structure for claimWithdrawals(uint256[],uint256[]):
        # - selector: 4 bytes (8 hex chars)
        # - offset to requestIds: 32 bytes (64 hex chars)
        # - offset to hints: 32 bytes (64 hex chars)
        # - requestIds array length: 32 bytes (64 hex chars)
        # - requestIds element: 32 bytes (64 hex chars)
        # - hints array length: 32 bytes (64 hex chars)
        # - hints element: 32 bytes (64 hex chars)
        # Total: 4 + 32*6 = 196 bytes = 392 hex chars + 2 for "0x"
        expected_len = 2 + 8 + 64 * 6  # 0x + selector + 6 32-byte params
        assert len(calldata) == expected_len

    def test_claim_withdrawals_request_id_encoding(self, adapter: LidoAdapter) -> None:
        """Test that claim_withdrawals request IDs are correctly encoded."""
        request_ids = [12345]
        result = adapter.claim_withdrawals(request_ids)

        assert result.success is True
        assert result.tx_data is not None
        calldata = result.tx_data["data"]

        # Extract request ID from calldata
        # Structure: selector(8) + offset1(64) + offset2(64) + len(64) + requestId(64)
        request_id_hex = calldata[2 + 8 + 64 + 64 + 64 :][:64]
        request_id = int(request_id_hex, 16)
        assert request_id == 12345

    def test_claim_withdrawals_not_available_on_arbitrum(self, arbitrum_adapter: LidoAdapter) -> None:
        """Test that claim_withdrawals is not available on L2s."""
        request_ids = [100]
        result = arbitrum_adapter.claim_withdrawals(request_ids)

        assert result.success is False
        assert result.error is not None
        assert "not available on arbitrum" in result.error.lower()

    def test_claim_withdrawals_not_available_on_optimism(self) -> None:
        """Test that claim_withdrawals is not available on Optimism."""
        config = LidoConfig(
            chain="optimism",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        adapter = LidoAdapter(config)
        request_ids = [100]
        result = adapter.claim_withdrawals(request_ids)

        assert result.success is False
        assert result.error is not None
        assert "not available on optimism" in result.error.lower()

    def test_claim_withdrawals_empty_request_ids(self, adapter: LidoAdapter) -> None:
        """Test that claim_withdrawals fails with empty request IDs list."""
        request_ids: list[int] = []
        result = adapter.claim_withdrawals(request_ids)

        assert result.success is False
        assert result.error is not None
        assert "at least one request id" in result.error.lower()

    def test_claim_withdrawals_mismatched_hints_length(self, adapter: LidoAdapter) -> None:
        """Test that claim_withdrawals fails when hints length doesn't match."""
        request_ids = [100, 200, 300]
        hints = [5, 6]  # Only 2 hints for 3 request IDs
        result = adapter.claim_withdrawals(request_ids, hints=hints)

        assert result.success is False
        assert result.error is not None
        assert "must match" in result.error.lower()

    def test_claim_withdrawals_large_request_ids(self, adapter: LidoAdapter) -> None:
        """Test claim_withdrawals with large request IDs."""
        request_ids = [999999999, 888888888]
        result = adapter.claim_withdrawals(request_ids)

        assert result.success is True
        assert result.tx_data is not None


# =============================================================================
# Compile Stake Intent Tests
# =============================================================================


class TestCompileStakeIntent:
    """Tests for compile_stake_intent method."""

    def test_compile_stake_intent_wrapped(self, adapter: LidoAdapter) -> None:
        """Test compiling a StakeIntent with receive_wrapped=True."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("1.0"),
            receive_wrapped=True,
            chain="ethereum",
        )
        bundle = adapter.compile_stake_intent(intent)

        # Should have 2 transactions: stake + wrap
        assert len(bundle.transactions) == 2
        assert bundle.intent_type == "STAKE"

        # First transaction should be stake
        stake_tx = bundle.transactions[0]
        assert stake_tx["action_type"] == "stake"
        assert stake_tx["to"] == LIDO_ADDRESSES["ethereum"]["steth"]
        assert stake_tx["value"] == 10**18  # 1 ETH in wei
        assert stake_tx["data"].startswith(LIDO_STAKE_SELECTOR)

        # Second transaction should be wrap
        wrap_tx = bundle.transactions[1]
        assert wrap_tx["action_type"] == "wrap"
        assert wrap_tx["to"] == LIDO_ADDRESSES["ethereum"]["wsteth"]
        assert wrap_tx["value"] == 0
        assert wrap_tx["data"].startswith(LIDO_WRAP_SELECTOR)

        # Check metadata
        assert bundle.metadata["protocol"] == "lido"
        assert bundle.metadata["output_token"] == "wstETH"
        assert bundle.metadata["receive_wrapped"] is True
        assert bundle.metadata["num_transactions"] == 2
        assert bundle.metadata["total_gas_estimate"] == (DEFAULT_GAS_ESTIMATES["stake"] + DEFAULT_GAS_ESTIMATES["wrap"])

    def test_compile_stake_intent_unwrapped(self, adapter: LidoAdapter) -> None:
        """Test compiling a StakeIntent with receive_wrapped=False."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("2.5"),
            receive_wrapped=False,
            chain="ethereum",
        )
        bundle = adapter.compile_stake_intent(intent)

        # Should have 1 transaction: stake only
        assert len(bundle.transactions) == 1
        assert bundle.intent_type == "STAKE"

        # Transaction should be stake
        stake_tx = bundle.transactions[0]
        assert stake_tx["action_type"] == "stake"
        assert stake_tx["to"] == LIDO_ADDRESSES["ethereum"]["steth"]
        assert stake_tx["value"] == int(Decimal("2.5") * 10**18)
        assert stake_tx["data"].startswith(LIDO_STAKE_SELECTOR)

        # Check metadata
        assert bundle.metadata["protocol"] == "lido"
        assert bundle.metadata["output_token"] == "stETH"
        assert bundle.metadata["receive_wrapped"] is False
        assert bundle.metadata["num_transactions"] == 1
        assert bundle.metadata["total_gas_estimate"] == DEFAULT_GAS_ESTIMATES["stake"]

    def test_compile_stake_intent_chain_amount_raises(self, adapter: LidoAdapter) -> None:
        """Test that compile_stake_intent raises when amount='all' is not resolved."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount="all",
            chain="ethereum",
        )

        with pytest.raises(ValueError, match="amount='all' must be resolved"):
            adapter.compile_stake_intent(intent)

    def test_compile_stake_intent_not_available_on_l2(self, arbitrum_adapter: LidoAdapter) -> None:
        """Test compile_stake_intent returns error bundle on L2."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("1.0"),
            receive_wrapped=True,
            chain="arbitrum",
        )
        bundle = arbitrum_adapter.compile_stake_intent(intent)

        # Should have empty transactions and error in metadata
        assert len(bundle.transactions) == 0
        assert bundle.intent_type == "STAKE"
        assert "error" in bundle.metadata
        assert "not available on arbitrum" in bundle.metadata["error"].lower()

    def test_compile_stake_intent_small_amount(self, adapter: LidoAdapter) -> None:
        """Test compiling a StakeIntent with small amount."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("0.001"),
            receive_wrapped=True,
            chain="ethereum",
        )
        bundle = adapter.compile_stake_intent(intent)

        assert len(bundle.transactions) == 2
        assert bundle.transactions[0]["value"] == 10**15  # 0.001 ETH in wei
        assert bundle.metadata["amount"] == "0.001"

    def test_compile_stake_intent_large_amount(self, adapter: LidoAdapter) -> None:
        """Test compiling a StakeIntent with large amount."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("1000"),
            receive_wrapped=False,
            chain="ethereum",
        )
        bundle = adapter.compile_stake_intent(intent)

        assert len(bundle.transactions) == 1
        assert bundle.transactions[0]["value"] == 1000 * 10**18

    def test_compile_stake_intent_metadata_completeness(self, adapter: LidoAdapter) -> None:
        """Test that compile_stake_intent includes all expected metadata."""
        from almanak.framework.intents.vocabulary import StakeIntent

        intent = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("1.0"),
            receive_wrapped=True,
            chain="ethereum",
        )
        bundle = adapter.compile_stake_intent(intent)

        # Check all expected metadata fields
        assert "intent_id" in bundle.metadata
        assert bundle.metadata["intent_id"] == intent.intent_id
        assert bundle.metadata["protocol"] == "lido"
        assert bundle.metadata["token_in"] == "ETH"
        assert bundle.metadata["amount"] == "1.0"
        assert bundle.metadata["output_token"] == "wstETH"
        assert bundle.metadata["receive_wrapped"] is True
        assert bundle.metadata["chain"] == "ethereum"
        assert "total_gas_estimate" in bundle.metadata
        assert "num_transactions" in bundle.metadata

    def test_compile_stake_intent_gas_estimates(self, adapter: LidoAdapter) -> None:
        """Test that compile_stake_intent calculates correct gas estimates."""
        from almanak.framework.intents.vocabulary import StakeIntent

        # Test wrapped (stake + wrap)
        intent_wrapped = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("1.0"),
            receive_wrapped=True,
            chain="ethereum",
        )
        bundle_wrapped = adapter.compile_stake_intent(intent_wrapped)
        expected_gas_wrapped = DEFAULT_GAS_ESTIMATES["stake"] + DEFAULT_GAS_ESTIMATES["wrap"]
        assert bundle_wrapped.metadata["total_gas_estimate"] == expected_gas_wrapped

        # Test unwrapped (stake only)
        intent_unwrapped = StakeIntent(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("1.0"),
            receive_wrapped=False,
            chain="ethereum",
        )
        bundle_unwrapped = adapter.compile_stake_intent(intent_unwrapped)
        expected_gas_unwrapped = DEFAULT_GAS_ESTIMATES["stake"]
        assert bundle_unwrapped.metadata["total_gas_estimate"] == expected_gas_unwrapped


# =============================================================================
# Compile Unstake Intent Tests
# =============================================================================


class TestCompileUnstakeIntent:
    """Tests for compile_unstake_intent method."""

    def test_compile_unstake_intent_wsteth(self, adapter: LidoAdapter) -> None:
        """Test compiling an UnstakeIntent with wstETH (requires unwrap + withdrawal)."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="lido",
            token_in="wstETH",
            amount=Decimal("1.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        # Should have 2 transactions: unwrap + request_withdrawal
        assert len(bundle.transactions) == 2
        assert bundle.intent_type == "UNSTAKE"

        # First transaction should be unwrap
        unwrap_tx = bundle.transactions[0]
        assert unwrap_tx["action_type"] == "unwrap"
        assert unwrap_tx["to"] == LIDO_ADDRESSES["ethereum"]["wsteth"]
        assert unwrap_tx["value"] == 0
        assert unwrap_tx["data"].startswith(LIDO_UNWRAP_SELECTOR)

        # Second transaction should be request_withdrawal
        withdrawal_tx = bundle.transactions[1]
        assert withdrawal_tx["action_type"] == "request_withdrawal"
        assert withdrawal_tx["to"] == LIDO_ADDRESSES["ethereum"]["withdrawal_queue"]
        assert withdrawal_tx["value"] == 0
        assert withdrawal_tx["data"].startswith(LIDO_REQUEST_WITHDRAWALS_SELECTOR)

        # Check metadata
        assert bundle.metadata["protocol"] == "lido"
        assert bundle.metadata["token_in"] == "wstETH"
        assert bundle.metadata["output_token"] == "ETH"
        assert bundle.metadata["requires_unwrap"] is True
        assert bundle.metadata["num_transactions"] == 2
        assert bundle.metadata["total_gas_estimate"] == (
            DEFAULT_GAS_ESTIMATES["unwrap"] + DEFAULT_GAS_ESTIMATES["request_withdrawal"]
        )

    def test_compile_unstake_intent_steth(self, adapter: LidoAdapter) -> None:
        """Test compiling an UnstakeIntent with stETH (direct withdrawal)."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="lido",
            token_in="stETH",
            amount=Decimal("2.5"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        # Should have 1 transaction: request_withdrawal only
        assert len(bundle.transactions) == 1
        assert bundle.intent_type == "UNSTAKE"

        # Transaction should be request_withdrawal
        withdrawal_tx = bundle.transactions[0]
        assert withdrawal_tx["action_type"] == "request_withdrawal"
        assert withdrawal_tx["to"] == LIDO_ADDRESSES["ethereum"]["withdrawal_queue"]
        assert withdrawal_tx["value"] == 0
        assert withdrawal_tx["data"].startswith(LIDO_REQUEST_WITHDRAWALS_SELECTOR)

        # Check metadata
        assert bundle.metadata["protocol"] == "lido"
        assert bundle.metadata["token_in"] == "stETH"
        assert bundle.metadata["output_token"] == "ETH"
        assert bundle.metadata["requires_unwrap"] is False
        assert bundle.metadata["num_transactions"] == 1
        assert bundle.metadata["total_gas_estimate"] == DEFAULT_GAS_ESTIMATES["request_withdrawal"]

    def test_compile_unstake_intent_chain_amount_raises(self, adapter: LidoAdapter) -> None:
        """Test that compile_unstake_intent raises when amount='all' is not resolved."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="lido",
            token_in="wstETH",
            amount="all",
            chain="ethereum",
        )

        with pytest.raises(ValueError, match="amount='all' must be resolved"):
            adapter.compile_unstake_intent(intent)

    def test_compile_unstake_intent_not_available_on_l2(self, arbitrum_adapter: LidoAdapter) -> None:
        """Test compile_unstake_intent returns error bundle on L2."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="lido",
            token_in="wstETH",
            amount=Decimal("1.0"),
            chain="arbitrum",
        )
        bundle = arbitrum_adapter.compile_unstake_intent(intent)

        # Should have empty transactions and error in metadata
        assert len(bundle.transactions) == 0
        assert bundle.intent_type == "UNSTAKE"
        assert "error" in bundle.metadata
        # On L2, withdrawal queue is not available
        assert "not available on arbitrum" in bundle.metadata["error"].lower()

    def test_compile_unstake_intent_small_amount(self, adapter: LidoAdapter) -> None:
        """Test compiling an UnstakeIntent with small amount."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="lido",
            token_in="stETH",
            amount=Decimal("0.001"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        assert len(bundle.transactions) == 1
        assert bundle.metadata["amount"] == "0.001"

    def test_compile_unstake_intent_large_amount(self, adapter: LidoAdapter) -> None:
        """Test compiling an UnstakeIntent with large amount."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="lido",
            token_in="wstETH",
            amount=Decimal("1000"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        # Should have 2 transactions for wstETH
        assert len(bundle.transactions) == 2
        assert bundle.metadata["amount"] == "1000"

    def test_compile_unstake_intent_metadata_completeness(self, adapter: LidoAdapter) -> None:
        """Test that compile_unstake_intent includes all expected metadata."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="lido",
            token_in="wstETH",
            amount=Decimal("1.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        # Check all expected metadata fields
        assert "intent_id" in bundle.metadata
        assert bundle.metadata["intent_id"] == intent.intent_id
        assert bundle.metadata["protocol"] == "lido"
        assert bundle.metadata["token_in"] == "wstETH"
        assert bundle.metadata["amount"] == "1.0"
        assert bundle.metadata["output_token"] == "ETH"
        assert bundle.metadata["requires_unwrap"] is True
        assert bundle.metadata["chain"] == "ethereum"
        assert "total_gas_estimate" in bundle.metadata
        assert "num_transactions" in bundle.metadata
        assert "note" in bundle.metadata
        assert "finalization" in bundle.metadata["note"].lower()

    def test_compile_unstake_intent_gas_estimates(self, adapter: LidoAdapter) -> None:
        """Test that compile_unstake_intent calculates correct gas estimates."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        # Test wstETH (unwrap + withdrawal)
        intent_wrapped = UnstakeIntent(
            protocol="lido",
            token_in="wstETH",
            amount=Decimal("1.0"),
            chain="ethereum",
        )
        bundle_wrapped = adapter.compile_unstake_intent(intent_wrapped)
        expected_gas_wrapped = DEFAULT_GAS_ESTIMATES["unwrap"] + DEFAULT_GAS_ESTIMATES["request_withdrawal"]
        assert bundle_wrapped.metadata["total_gas_estimate"] == expected_gas_wrapped

        # Test stETH (withdrawal only)
        intent_steth = UnstakeIntent(
            protocol="lido",
            token_in="stETH",
            amount=Decimal("1.0"),
            chain="ethereum",
        )
        bundle_steth = adapter.compile_unstake_intent(intent_steth)
        expected_gas_steth = DEFAULT_GAS_ESTIMATES["request_withdrawal"]
        assert bundle_steth.metadata["total_gas_estimate"] == expected_gas_steth

    def test_compile_unstake_intent_case_insensitive_token(self, adapter: LidoAdapter) -> None:
        """Test that token_in matching is case-insensitive."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        # Test lowercase wsteth
        intent_lower = UnstakeIntent(
            protocol="lido",
            token_in="wsteth",
            amount=Decimal("1.0"),
            chain="ethereum",
        )
        bundle_lower = adapter.compile_unstake_intent(intent_lower)
        assert bundle_lower.metadata["requires_unwrap"] is True
        assert len(bundle_lower.transactions) == 2

        # Test uppercase WSTETH
        intent_upper = UnstakeIntent(
            protocol="lido",
            token_in="WSTETH",
            amount=Decimal("1.0"),
            chain="ethereum",
        )
        bundle_upper = adapter.compile_unstake_intent(intent_upper)
        assert bundle_upper.metadata["requires_unwrap"] is True
        assert len(bundle_upper.transactions) == 2

    def test_compile_unstake_intent_withdrawal_owner(self, adapter: LidoAdapter) -> None:
        """Test that withdrawal request uses the wallet address as owner."""
        from almanak.framework.intents.vocabulary import UnstakeIntent

        intent = UnstakeIntent(
            protocol="lido",
            token_in="stETH",
            amount=Decimal("1.0"),
            chain="ethereum",
        )
        bundle = adapter.compile_unstake_intent(intent)

        # Check that the wallet address is encoded in the withdrawal request
        withdrawal_tx = bundle.transactions[0]
        calldata = withdrawal_tx["data"]
        # Wallet address should be in the calldata (padded)
        expected_addr = adapter.wallet_address.lower().replace("0x", "")
        assert expected_addr in calldata.lower()
