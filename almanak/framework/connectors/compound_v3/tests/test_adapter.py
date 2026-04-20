"""Tests for Compound V3 Adapter.

These tests verify the CompoundV3Adapter for:
- Configuration validation
- Supply/withdraw base asset operations
- Supply/withdraw collateral operations
- Borrow/repay operations
- Market information queries
- Health factor calculations
- Transaction building
"""

from decimal import Decimal
from unittest.mock import patch

import pytest

from ..adapter import (
    COMPOUND_V3_COMET_ADDRESSES,
    DEFAULT_GAS_ESTIMATES,
    CompoundV3Adapter,
    CompoundV3Config,
    CompoundV3HealthFactor,
    CompoundV3MarketInfo,
    CompoundV3Position,
    TransactionResult,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def ethereum_config():
    """Create Ethereum USDC market config."""
    return CompoundV3Config(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
        market="usdc",
    )


@pytest.fixture
def ethereum_weth_config():
    """Create Ethereum WETH market config."""
    return CompoundV3Config(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
        market="weth",
    )


@pytest.fixture
def arbitrum_config():
    """Create Arbitrum USDC market config."""
    return CompoundV3Config(
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        market="usdc",
    )


@pytest.fixture
def adapter(ethereum_config):
    """Create adapter with Ethereum USDC config."""
    return CompoundV3Adapter(ethereum_config)


@pytest.fixture
def weth_adapter(ethereum_weth_config):
    """Create adapter with Ethereum WETH config."""
    return CompoundV3Adapter(ethereum_weth_config)


@pytest.fixture
def arbitrum_adapter(arbitrum_config):
    """Create adapter with Arbitrum USDC config."""
    return CompoundV3Adapter(arbitrum_config)


# =============================================================================
# Configuration Tests
# =============================================================================


class TestCompoundV3Config:
    """Test CompoundV3Config validation."""

    def test_valid_ethereum_config(self):
        """Test valid Ethereum configuration."""
        config = CompoundV3Config(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            market="usdc",
        )
        assert config.chain == "ethereum"
        assert config.market == "usdc"
        assert config.default_slippage_bps == 50

    def test_valid_arbitrum_config(self):
        """Test valid Arbitrum configuration."""
        config = CompoundV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            market="usdc",
        )
        assert config.chain == "arbitrum"
        assert config.market == "usdc"

    def test_invalid_chain(self):
        """Test invalid chain raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            CompoundV3Config(
                chain="invalid",
                wallet_address="0x1234567890123456789012345678901234567890",
                market="usdc",
            )

    def test_invalid_wallet_address_no_prefix(self):
        """Test wallet without 0x prefix raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            CompoundV3Config(
                chain="ethereum",
                wallet_address="1234567890123456789012345678901234567890",
                market="usdc",
            )

    def test_invalid_wallet_address_wrong_length(self):
        """Test wallet with wrong length raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            CompoundV3Config(
                chain="ethereum",
                wallet_address="0x12345",
                market="usdc",
            )

    def test_invalid_market(self):
        """Test invalid market raises error."""
        with pytest.raises(ValueError, match="Invalid market"):
            CompoundV3Config(
                chain="ethereum",
                wallet_address="0x1234567890123456789012345678901234567890",
                market="invalid",
            )

    def test_invalid_slippage_negative(self):
        """Test negative slippage raises error."""
        with pytest.raises(ValueError, match="Invalid slippage"):
            CompoundV3Config(
                chain="ethereum",
                wallet_address="0x1234567890123456789012345678901234567890",
                market="usdc",
                default_slippage_bps=-1,
            )

    def test_invalid_slippage_too_high(self):
        """Test slippage > 100% raises error."""
        with pytest.raises(ValueError, match="Invalid slippage"):
            CompoundV3Config(
                chain="ethereum",
                wallet_address="0x1234567890123456789012345678901234567890",
                market="usdc",
                default_slippage_bps=10001,
            )


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestCompoundV3AdapterInit:
    """Test CompoundV3Adapter initialization."""

    def test_adapter_initialization(self, adapter):
        """Test adapter initializes correctly."""
        assert adapter.chain == "ethereum"
        assert adapter.market == "usdc"
        assert adapter.comet_address == COMPOUND_V3_COMET_ADDRESSES["ethereum"]["usdc"]

    def test_adapter_with_custom_oracle(self, ethereum_config):
        """Test adapter with custom price oracle."""

        def custom_oracle(token: str) -> Decimal:
            return Decimal("1.0")

        adapter = CompoundV3Adapter(ethereum_config, price_oracle=custom_oracle)
        assert adapter._price_oracle("USDC") == Decimal("1.0")

    def test_adapter_market_config_loaded(self, adapter):
        """Test market config is loaded."""
        assert adapter.market_config.get("base_token") == "USDC"
        assert "WETH" in adapter.market_config.get("collaterals", {})


# =============================================================================
# Supply Operations Tests
# =============================================================================


class TestSupplyOperations:
    """Test supply operations."""

    def test_supply_base_asset(self, adapter):
        """Test supplying base asset."""
        result = adapter.supply(amount=Decimal("1000"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter.comet_address
        assert result.tx_data["value"] == 0
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["supply"]
        assert "Supply 1000 USDC" in result.description

    def test_supply_base_asset_on_behalf_of(self, adapter):
        """Test supplying on behalf of another address."""
        recipient = "0xabcdef1234567890abcdef1234567890abcdef12"
        result = adapter.supply(
            amount=Decimal("500"),
            on_behalf_of=recipient,
        )

        assert result.success is True
        assert result.tx_data is not None
        # Should use supplyTo selector
        assert result.tx_data["data"].startswith("0x4232cd63")

    def test_supply_with_decimal_amount(self, adapter):
        """Test supplying with decimal amount."""
        result = adapter.supply(amount=Decimal("100.50"))

        assert result.success is True
        assert result.tx_data is not None

    def test_supply_collateral(self, adapter):
        """Test supplying collateral."""
        result = adapter.supply_collateral(
            asset="WETH",
            amount=Decimal("1.5"),
        )

        assert result.success is True
        assert result.tx_data is not None
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["supply_collateral"]
        assert "Supply 1.5 WETH as collateral" in result.description

    def test_supply_collateral_on_behalf_of(self, adapter):
        """Test supplying collateral on behalf of another."""
        recipient = "0xabcdef1234567890abcdef1234567890abcdef12"
        result = adapter.supply_collateral(
            asset="WETH",
            amount=Decimal("2.0"),
            on_behalf_of=recipient,
        )

        assert result.success is True
        # Should use supplyTo selector
        assert result.tx_data["data"].startswith("0x4232cd63")

    def test_supply_unsupported_collateral(self, adapter):
        """Test supplying unsupported collateral fails."""
        result = adapter.supply_collateral(
            asset="INVALID",
            amount=Decimal("1.0"),
        )

        assert result.success is False
        assert "Unsupported collateral" in result.error


# =============================================================================
# Withdraw Operations Tests
# =============================================================================


class TestWithdrawOperations:
    """Test withdraw operations."""

    def test_withdraw_base_asset(self, adapter):
        """Test withdrawing base asset."""
        result = adapter.withdraw(amount=Decimal("500"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter.comet_address
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["withdraw"]
        assert "Withdraw 500 USDC" in result.description

    def test_withdraw_base_asset_all(self, adapter):
        """Test withdrawing all base asset."""
        result = adapter.withdraw(
            amount=Decimal("0"),
            withdraw_all=True,
        )

        assert result.success is True
        assert "Withdraw all" in result.description

    def test_withdraw_to_receiver(self, adapter):
        """Test withdrawing to different address."""
        receiver = "0xabcdef1234567890abcdef1234567890abcdef12"
        result = adapter.withdraw(
            amount=Decimal("100"),
            receiver=receiver,
        )

        assert result.success is True
        # Should use withdrawTo selector
        assert result.tx_data["data"].startswith("0x8013f3a7")

    def test_withdraw_collateral(self, adapter):
        """Test withdrawing collateral."""
        result = adapter.withdraw_collateral(
            asset="WETH",
            amount=Decimal("0.5"),
        )

        assert result.success is True
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["withdraw_collateral"]
        assert "Withdraw 0.5 WETH collateral" in result.description

    def test_withdraw_collateral_all(self, adapter):
        """Test withdrawing all collateral."""
        result = adapter.withdraw_collateral(
            asset="WETH",
            amount=Decimal("0"),
            withdraw_all=True,
        )

        assert result.success is True
        assert "Withdraw all" in result.description

    def test_withdraw_unsupported_collateral(self, adapter):
        """Test withdrawing unsupported collateral fails."""
        result = adapter.withdraw_collateral(
            asset="INVALID",
            amount=Decimal("1.0"),
        )

        assert result.success is False
        assert "Unsupported collateral" in result.error


# =============================================================================
# Borrow Operations Tests
# =============================================================================


class TestBorrowOperations:
    """Test borrow operations."""

    def test_borrow_base_asset(self, adapter):
        """Test borrowing base asset."""
        result = adapter.borrow(amount=Decimal("1000"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["borrow"]
        assert "Borrow 1000 USDC" in result.description

    def test_borrow_to_receiver(self, adapter):
        """Test borrowing to different address."""
        receiver = "0xabcdef1234567890abcdef1234567890abcdef12"
        result = adapter.borrow(
            amount=Decimal("500"),
            receiver=receiver,
        )

        assert result.success is True
        # Should use withdrawTo selector (borrow is via withdraw in V3)
        assert result.tx_data["data"].startswith("0x8013f3a7")

    def test_borrow_uses_withdraw_internally(self, adapter):
        """Test that borrow uses withdraw function internally."""
        result = adapter.borrow(amount=Decimal("100"))

        assert result.success is True
        # In Compound V3, borrow is implemented via withdraw
        assert result.tx_data["data"].startswith("0xf3fef3a3")


# =============================================================================
# Repay Operations Tests
# =============================================================================


class TestRepayOperations:
    """Test repay operations."""

    def test_repay_base_asset(self, adapter):
        """Test repaying base asset."""
        result = adapter.repay(amount=Decimal("500"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["repay"]
        assert "Repay 500 USDC" in result.description

    def test_repay_all(self, adapter):
        """Test repaying all debt."""
        result = adapter.repay(
            amount=Decimal("0"),
            repay_all=True,
        )

        assert result.success is True
        assert "Repay full debt" in result.description

    def test_repay_on_behalf_of(self, adapter):
        """Test repaying on behalf of another address."""
        borrower = "0xabcdef1234567890abcdef1234567890abcdef12"
        result = adapter.repay(
            amount=Decimal("100"),
            on_behalf_of=borrower,
        )

        assert result.success is True
        # Should use supplyTo selector (repay is via supply in V3)
        assert result.tx_data["data"].startswith("0x4232cd63")

    def test_repay_uses_supply_internally(self, adapter):
        """Test that repay uses supply function internally."""
        result = adapter.repay(amount=Decimal("100"))

        assert result.success is True
        # In Compound V3, repay is implemented via supply
        assert result.tx_data["data"].startswith("0xf2b9fdb8")


# =============================================================================
# Market Information Tests
# =============================================================================


class TestMarketInformation:
    """Test market information queries."""

    def test_get_market_info(self, adapter):
        """Test getting market info."""
        info = adapter.get_market_info()

        assert isinstance(info, CompoundV3MarketInfo)
        assert info.market_id == "usdc"
        assert info.base_token == "USDC"
        assert info.comet_address == adapter.comet_address
        assert "WETH" in info.collaterals

    def test_get_market_info_weth_market(self, weth_adapter):
        """Test getting WETH market info."""
        info = weth_adapter.get_market_info()

        assert info.base_token == "WETH"
        assert "wstETH" in info.collaterals

    def test_get_supported_collaterals(self, adapter):
        """Test getting supported collaterals."""
        collaterals = adapter.get_supported_collaterals()

        assert isinstance(collaterals, list)
        assert "WETH" in collaterals
        assert "WBTC" in collaterals

    def test_get_collateral_info(self, adapter):
        """Test getting collateral info."""
        info = adapter.get_collateral_info("WETH")

        assert info is not None
        assert "address" in info
        assert "borrow_collateral_factor" in info
        assert "liquidation_collateral_factor" in info

    def test_get_collateral_info_unsupported(self, adapter):
        """Test getting info for unsupported collateral."""
        info = adapter.get_collateral_info("INVALID")

        assert info is None


# =============================================================================
# Health Factor Tests
# =============================================================================


class TestHealthFactor:
    """Test health factor calculations."""

    def test_calculate_health_factor_no_debt(self, adapter):
        """Test health factor with no debt."""
        hf = adapter.calculate_health_factor(
            collateral_balances={"WETH": Decimal("1.0")},
            borrow_balance=Decimal("0"),
        )

        assert hf.health_factor == Decimal("999999")
        assert hf.is_healthy is True
        assert hf.is_liquidatable is False

    def test_calculate_health_factor_with_debt(self, adapter):
        """Test health factor with debt."""
        hf = adapter.calculate_health_factor(
            collateral_balances={"WETH": Decimal("1.0")},  # $2500 at default price
            borrow_balance=Decimal("1000"),  # $1000 USDC
        )

        assert hf.borrow_value_usd == Decimal("1000")
        assert hf.health_factor > Decimal("1.0")
        assert hf.is_healthy is True
        assert hf.is_liquidatable is False

    def test_calculate_health_factor_liquidatable(self, adapter):
        """Test health factor when liquidatable."""
        # Create custom oracle with high borrow
        hf = adapter.calculate_health_factor(
            collateral_balances={"WETH": Decimal("0.4")},  # ~$1000 at $2500
            borrow_balance=Decimal("1000"),  # $1000 USDC but LTV is 89.5%
        )

        # With 0.4 ETH at $2500 = $1000 collateral
        # Liquidation CF for WETH = 0.895 -> threshold = $895
        # Borrow = $1000 -> HF = 895/1000 = 0.895 < 1.0
        assert hf.health_factor < Decimal("1.0")
        assert hf.is_liquidatable is True

    def test_calculate_health_factor_multiple_collaterals(self, adapter):
        """Test health factor with multiple collaterals."""
        hf = adapter.calculate_health_factor(
            collateral_balances={
                "WETH": Decimal("1.0"),
                "WBTC": Decimal("0.1"),
            },
            borrow_balance=Decimal("5000"),
        )

        # Should have combined collateral value
        assert hf.collateral_value_usd > Decimal("8000")  # 1 ETH + 0.1 BTC


# =============================================================================
# Approval Tests
# =============================================================================


class TestApproval:
    """Test approval operations."""

    def test_build_approve_base_token(self, adapter):
        """Test building approval for base token."""
        result = adapter.build_approve_transaction(token="USDC")

        assert result.success is True
        assert result.tx_data is not None
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["approve"]
        assert "Approve unlimited" in result.description

    def test_build_approve_collateral(self, adapter):
        """Test building approval for collateral."""
        result = adapter.build_approve_transaction(token="WETH")

        assert result.success is True
        assert result.tx_data is not None

    def test_build_approve_specific_amount(self, adapter):
        """Test building approval for specific amount."""
        result = adapter.build_approve_transaction(
            token="USDC",
            amount=Decimal("1000"),
        )

        assert result.success is True
        assert "Approve 1000 USDC" in result.description

    def test_build_approve_unknown_token(self, adapter):
        """Test building approval for unknown token fails."""
        result = adapter.build_approve_transaction(token="UNKNOWN")

        assert result.success is False
        assert "Unknown token" in result.error


# =============================================================================
# Data Class Tests
# =============================================================================


class TestDataClasses:
    """Test data class functionality."""

    def test_market_info_to_dict(self):
        """Test CompoundV3MarketInfo serialization."""
        info = CompoundV3MarketInfo(
            market_id="usdc",
            name="USDC Market",
            base_token="USDC",
            base_token_address="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            comet_address="0xc3d688B66703497DAA19211EEdff47f25384cdc3",
            collaterals={
                "WETH": {
                    "address": "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2",
                    "borrow_collateral_factor": Decimal("0.825"),
                    "liquidation_collateral_factor": Decimal("0.895"),
                    "liquidation_factor": Decimal("0.95"),
                },
            },
        )

        data = info.to_dict()
        assert data["market_id"] == "usdc"
        assert data["base_token"] == "USDC"
        assert "WETH" in data["collaterals"]

    def test_position_to_dict(self):
        """Test CompoundV3Position serialization."""
        position = CompoundV3Position(
            market_id="usdc",
            base_balance=Decimal("1000"),
            collateral_balances={"WETH": Decimal("1.5")},
        )

        data = position.to_dict()
        assert data["market_id"] == "usdc"
        assert data["supply_balance"] == "1000"
        assert data["is_supplier"] is True
        assert data["is_borrower"] is False

    def test_position_borrower_properties(self):
        """Test CompoundV3Position borrower properties."""
        position = CompoundV3Position(
            market_id="usdc",
            base_balance=Decimal("-500"),  # Negative = borrow
            collateral_balances={"WETH": Decimal("1.0")},
        )

        assert position.is_borrower is True
        assert position.is_supplier is False
        assert position.borrow_balance == Decimal("500")
        assert position.supply_balance == Decimal("0")
        assert position.has_collateral is True

    def test_health_factor_to_dict(self):
        """Test CompoundV3HealthFactor serialization."""
        hf = CompoundV3HealthFactor(
            collateral_value_usd=Decimal("10000"),
            borrow_value_usd=Decimal("5000"),
            borrow_capacity_usd=Decimal("8000"),
            liquidation_threshold_usd=Decimal("9000"),
            health_factor=Decimal("1.8"),
        )

        data = hf.to_dict()
        assert data["collateral_value_usd"] == "10000"
        assert data["health_factor"] == "1.8"
        assert data["is_healthy"] is True
        assert data["available_borrow_usd"] == "3000"

    def test_transaction_result_to_dict(self):
        """Test TransactionResult serialization."""
        result = TransactionResult(
            success=True,
            tx_data={"to": "0x123", "value": 0, "data": "0x456"},
            gas_estimate=150000,
            description="Test transaction",
        )

        data = result.to_dict()
        assert data["success"] is True
        assert data["gas_estimate"] == 150000


# =============================================================================
# Cross-Chain Tests
# =============================================================================


class TestCrossChain:
    """Test cross-chain functionality."""

    def test_arbitrum_usdc_market(self, arbitrum_adapter):
        """Test Arbitrum USDC market."""
        info = arbitrum_adapter.get_market_info()

        assert info.base_token == "USDC"
        assert arbitrum_adapter.comet_address == COMPOUND_V3_COMET_ADDRESSES["arbitrum"]["usdc"]

    def test_arbitrum_collaterals(self, arbitrum_adapter):
        """Test Arbitrum collateral support."""
        collaterals = arbitrum_adapter.get_supported_collaterals()

        assert "WETH" in collaterals
        assert "ARB" in collaterals

    def test_different_markets_have_different_addresses(self, adapter, weth_adapter):
        """Test different markets have different Comet addresses."""
        assert adapter.comet_address != weth_adapter.comet_address


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandling:
    """Test error handling."""

    def test_supply_handles_exception(self, adapter):
        """Test supply handles exceptions gracefully."""
        with patch.object(adapter, "_get_decimals", side_effect=Exception("Test error")):
            result = adapter.supply(amount=Decimal("100"))

        assert result.success is False
        assert "Test error" in result.error

    def test_withdraw_handles_exception(self, adapter):
        """Test withdraw handles exceptions gracefully."""
        with patch.object(adapter, "_get_decimals", side_effect=Exception("Test error")):
            result = adapter.withdraw(amount=Decimal("100"))

        assert result.success is False
        assert "Test error" in result.error

    def test_borrow_handles_exception(self, adapter):
        """Test borrow handles exceptions gracefully."""
        with patch.object(adapter, "_get_decimals", side_effect=Exception("Test error")):
            result = adapter.borrow(amount=Decimal("100"))

        assert result.success is False
        assert "Test error" in result.error
