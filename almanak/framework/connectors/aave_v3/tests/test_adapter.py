"""Tests for Aave V3 Adapter.

This module contains comprehensive tests for the AaveV3Adapter class,
covering all operations including supply, borrow, repay, withdraw,
flash loans, E-Mode, health factor calculations, and more.
"""

from decimal import Decimal

import pytest

from ..adapter import (
    AAVE_V3_POOL_ADDRESSES,
    AAVE_V3_TOKEN_ADDRESSES,
    AaveV3Adapter,
    AaveV3Config,
    AaveV3EModeCategory,
    AaveV3InterestRateMode,
    AaveV3Position,
    AaveV3ReserveData,
    create_adapter_from_price_oracle_dict,
    create_adapter_with_prices,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def config() -> AaveV3Config:
    """Create a test configuration."""
    return AaveV3Config(
        chain="arbitrum",
        wallet_address="0x1234567890123456789012345678901234567890",
        allow_placeholder_prices=True,  # Only for testing
    )


@pytest.fixture
def test_prices() -> dict[str, Decimal]:
    """Create test prices for health factor calculations."""
    return {
        "WETH": Decimal("3100"),
        "wstETH": Decimal("3500"),
        "USDC": Decimal("1"),
        "USDT": Decimal("1"),
        "DAI": Decimal("1"),
        "WBTC": Decimal("100000"),
    }


@pytest.fixture
def adapter(config: AaveV3Config) -> AaveV3Adapter:
    """Create a test adapter instance with placeholder prices allowed for testing."""
    return AaveV3Adapter(config)


@pytest.fixture
def sample_reserve_data() -> dict[str, AaveV3ReserveData]:
    """Create sample reserve data for testing."""
    return {
        "USDC": AaveV3ReserveData(
            asset="USDC",
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            ltv=8000,  # 80%
            liquidation_threshold=8500,  # 85%
            liquidation_bonus=10500,  # 5% bonus
            usage_as_collateral_enabled=True,
            borrowing_enabled=True,
            emode_ltv=9700,
            emode_liquidation_threshold=9750,
            emode_category=2,  # Stablecoins
        ),
        "WETH": AaveV3ReserveData(
            asset="WETH",
            asset_address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            ltv=8250,  # 82.5%
            liquidation_threshold=8600,  # 86%
            liquidation_bonus=10500,  # 5% bonus
            usage_as_collateral_enabled=True,
            borrowing_enabled=True,
            emode_ltv=9300,
            emode_liquidation_threshold=9500,
            emode_category=1,  # ETH correlated
        ),
        "wstETH": AaveV3ReserveData(
            asset="wstETH",
            asset_address="0x5979D7b546E38E414F7E9822514be443A4800529",
            ltv=7000,  # 70%
            liquidation_threshold=7900,  # 79%
            liquidation_bonus=10700,  # 7% bonus
            usage_as_collateral_enabled=True,
            borrowing_enabled=True,
            emode_ltv=9300,
            emode_liquidation_threshold=9500,
            emode_category=1,  # ETH correlated
        ),
    }


@pytest.fixture
def sample_positions() -> list[AaveV3Position]:
    """Create sample positions for testing."""
    return [
        AaveV3Position(
            asset="USDC",
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=Decimal("10000"),  # 10,000 USDC supplied
            current_variable_debt=Decimal("0"),
            usage_as_collateral_enabled=True,
        ),
        AaveV3Position(
            asset="WETH",
            asset_address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
            current_atoken_balance=Decimal("0"),
            current_variable_debt=Decimal("2"),  # 2 WETH borrowed
            usage_as_collateral_enabled=False,
        ),
    ]


# =============================================================================
# Configuration Tests
# =============================================================================


class TestAaveV3Config:
    """Tests for AaveV3Config."""

    def test_valid_config(self) -> None:
        """Test creating a valid configuration."""
        config = AaveV3Config(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        assert config.chain == "ethereum"
        assert config.wallet_address == "0x1234567890123456789012345678901234567890"
        assert config.default_slippage_bps == 50

    def test_invalid_chain(self) -> None:
        """Test that invalid chain raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            AaveV3Config(
                chain="invalid_chain",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_invalid_wallet_address(self) -> None:
        """Test that invalid wallet address raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            AaveV3Config(
                chain="arbitrum",
                wallet_address="invalid_address",
            )

    def test_invalid_slippage(self) -> None:
        """Test that invalid slippage raises error."""
        with pytest.raises(ValueError, match="Invalid slippage"):
            AaveV3Config(
                chain="arbitrum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=20000,  # > 10000
            )


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestAaveV3AdapterInit:
    """Tests for AaveV3Adapter initialization."""

    def test_init_arbitrum(self, config: AaveV3Config) -> None:
        """Test adapter initialization for Arbitrum with placeholder prices allowed."""
        adapter = AaveV3Adapter(config)
        assert adapter.chain == "arbitrum"
        assert adapter.pool_address == AAVE_V3_POOL_ADDRESSES["arbitrum"]
        assert adapter.wallet_address == config.wallet_address

    def test_init_all_chains(self) -> None:
        """Test adapter initialization for all supported chains."""
        chains = ["ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche"]
        for chain in chains:
            config = AaveV3Config(
                chain=chain,
                wallet_address="0x1234567890123456789012345678901234567890",
                allow_placeholder_prices=True,  # Required for testing without real oracle
            )
            adapter = AaveV3Adapter(config)
            assert adapter.chain == chain
            assert adapter.pool_address == AAVE_V3_POOL_ADDRESSES[chain]

    def test_init_requires_price_oracle_in_production(self) -> None:
        """Test that adapter requires price oracle when placeholder prices not allowed."""
        config = AaveV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=False,  # Production setting
        )
        with pytest.raises(ValueError, match="requires a price_oracle"):
            AaveV3Adapter(config)

    def test_init_with_real_price_oracle(self) -> None:
        """Test adapter initialization with real price oracle."""
        config = AaveV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=False,
        )

        def price_oracle(asset: str) -> Decimal:
            prices = {"WETH": Decimal("3100"), "USDC": Decimal("1")}
            return prices.get(asset, Decimal("1"))

        adapter = AaveV3Adapter(config, price_oracle=price_oracle)
        assert adapter.chain == "arbitrum"
        assert adapter._using_placeholder_prices is False

    def test_init_placeholder_prices_logs_warning(self, caplog) -> None:
        """Test that placeholder prices mode logs warning."""
        import logging

        caplog.set_level(logging.WARNING)

        config = AaveV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            allow_placeholder_prices=True,
        )
        adapter = AaveV3Adapter(config)

        assert adapter._using_placeholder_prices is True
        assert "PLACEHOLDER PRICES" in caplog.text


# =============================================================================
# Factory Function Tests
# =============================================================================


class TestFactoryFunctions:
    """Tests for adapter factory functions."""

    def test_create_adapter_with_prices(self) -> None:
        """Test creating adapter with a prices dictionary."""
        config = AaveV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        prices = {
            "WETH": Decimal("3100"),
            "USDC": Decimal("1"),
            "WBTC": Decimal("100000"),
        }

        adapter = create_adapter_with_prices(config, prices)

        assert adapter.chain == "arbitrum"
        assert adapter._using_placeholder_prices is False
        # Verify the price oracle works
        assert adapter._price_oracle("WETH") == Decimal("3100")
        assert adapter._price_oracle("USDC") == Decimal("1")

    def test_create_adapter_from_price_oracle_dict(self) -> None:
        """Test creating adapter with convenience function."""
        prices = {
            "WETH": Decimal("3100"),
            "USDC": Decimal("1"),
        }

        adapter = create_adapter_from_price_oracle_dict(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
            price_oracle_dict=prices,
        )

        assert adapter.chain == "arbitrum"
        assert adapter._using_placeholder_prices is False
        assert adapter._price_oracle("WETH") == Decimal("3100")

    def test_create_adapter_with_prices_missing_asset_raises(self) -> None:
        """Test that missing asset in prices raises KeyError."""
        config = AaveV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        prices = {
            "WETH": Decimal("3100"),
            # Missing USDC
        }

        adapter = create_adapter_with_prices(config, prices)

        # Accessing a missing asset should raise KeyError
        with pytest.raises(KeyError, match="No price found for"):
            adapter._price_oracle("USDC")

    def test_create_adapter_with_prices_case_insensitive(self) -> None:
        """Test that price lookup is case-insensitive."""
        config = AaveV3Config(
            chain="arbitrum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        prices = {
            "WETH": Decimal("3100"),  # uppercase
        }

        adapter = create_adapter_with_prices(config, prices)

        # Both cases should work
        assert adapter._price_oracle("WETH") == Decimal("3100")
        assert adapter._price_oracle("weth") == Decimal("3100")


# =============================================================================
# Supply Operation Tests
# =============================================================================


class TestSupplyOperations:
    """Tests for supply operations."""

    def test_supply_usdc(self, adapter: AaveV3Adapter) -> None:
        """Test building a supply transaction for USDC."""
        result = adapter.supply("USDC", Decimal("1000"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter.pool_address
        assert result.tx_data["value"] == 0
        assert result.tx_data["data"].startswith("0x617ba037")  # supply selector
        assert "Supply 1000 USDC" in result.description

    def test_supply_weth(self, adapter: AaveV3Adapter) -> None:
        """Test building a supply transaction for WETH."""
        result = adapter.supply("WETH", Decimal("5.5"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith("0x617ba037")

    def test_supply_unknown_asset(self, adapter: AaveV3Adapter) -> None:
        """Test supply with unknown asset fails."""
        result = adapter.supply("UNKNOWN_TOKEN", Decimal("100"))

        assert result.success is False
        assert "Unknown asset" in (result.error or "")

    def test_supply_on_behalf_of(self, adapter: AaveV3Adapter) -> None:
        """Test supply on behalf of another address."""
        other_address = "0x9876543210987654321098765432109876543210"
        result = adapter.supply("USDC", Decimal("1000"), on_behalf_of=other_address)

        assert result.success is True
        assert result.tx_data is not None


# =============================================================================
# Withdraw Operation Tests
# =============================================================================


class TestWithdrawOperations:
    """Tests for withdraw operations."""

    def test_withdraw_usdc(self, adapter: AaveV3Adapter) -> None:
        """Test building a withdraw transaction for USDC."""
        result = adapter.withdraw("USDC", Decimal("500"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter.pool_address
        assert result.tx_data["data"].startswith("0x69328dec")  # withdraw selector
        assert "Withdraw 500 USDC" in result.description

    def test_withdraw_all(self, adapter: AaveV3Adapter) -> None:
        """Test withdrawing all supplied assets."""
        result = adapter.withdraw("USDC", Decimal("0"), withdraw_all=True)

        assert result.success is True
        assert result.tx_data is not None
        assert "Withdraw all USDC" in result.description

    def test_withdraw_to_different_address(self, adapter: AaveV3Adapter) -> None:
        """Test withdrawing to a different address."""
        other_address = "0x9876543210987654321098765432109876543210"
        result = adapter.withdraw("USDC", Decimal("500"), to=other_address)

        assert result.success is True
        assert result.tx_data is not None


# =============================================================================
# Borrow Operation Tests
# =============================================================================


class TestBorrowOperations:
    """Tests for borrow operations."""

    def test_borrow_weth_variable(self, adapter: AaveV3Adapter) -> None:
        """Test building a variable rate borrow transaction."""
        result = adapter.borrow(
            "WETH",
            Decimal("1.5"),
            interest_rate_mode=AaveV3InterestRateMode.VARIABLE,
        )

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter.pool_address
        assert result.tx_data["data"].startswith("0xa415bcad")  # borrow selector
        assert "variable rate" in result.description

    def test_borrow_usdc_stable(self, adapter: AaveV3Adapter) -> None:
        """Test building a stable rate borrow transaction."""
        result = adapter.borrow(
            "USDC",
            Decimal("5000"),
            interest_rate_mode=AaveV3InterestRateMode.STABLE,
        )

        assert result.success is True
        assert result.tx_data is not None
        assert "stable rate" in result.description

    def test_borrow_unknown_asset(self, adapter: AaveV3Adapter) -> None:
        """Test borrow with unknown asset fails."""
        result = adapter.borrow("UNKNOWN_TOKEN", Decimal("100"))

        assert result.success is False
        assert "Unknown asset" in (result.error or "")


# =============================================================================
# Repay Operation Tests
# =============================================================================


class TestRepayOperations:
    """Tests for repay operations."""

    def test_repay_weth(self, adapter: AaveV3Adapter) -> None:
        """Test building a repay transaction."""
        result = adapter.repay("WETH", Decimal("0.5"))

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter.pool_address
        assert result.tx_data["data"].startswith("0x573ade81")  # repay selector
        assert "Repay 0.5 WETH" in result.description

    def test_repay_all(self, adapter: AaveV3Adapter) -> None:
        """Test repaying full debt."""
        result = adapter.repay("WETH", Decimal("0"), repay_all=True)

        assert result.success is True
        assert result.tx_data is not None
        assert "Repay full debt WETH" in result.description

    def test_repay_on_behalf_of(self, adapter: AaveV3Adapter) -> None:
        """Test repaying on behalf of another address."""
        other_address = "0x9876543210987654321098765432109876543210"
        result = adapter.repay("WETH", Decimal("0.5"), on_behalf_of=other_address)

        assert result.success is True


# =============================================================================
# Collateral Operation Tests
# =============================================================================


class TestCollateralOperations:
    """Tests for collateral operations."""

    def test_enable_collateral(self, adapter: AaveV3Adapter) -> None:
        """Test enabling an asset as collateral."""
        result = adapter.set_user_use_reserve_as_collateral("USDC", True)

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith("0x5a3b74b9")  # setUserUseReserveAsCollateral
        assert "Enable USDC as collateral" in result.description

    def test_disable_collateral(self, adapter: AaveV3Adapter) -> None:
        """Test disabling an asset as collateral."""
        result = adapter.set_user_use_reserve_as_collateral("USDC", False)

        assert result.success is True
        assert "Disable USDC as collateral" in result.description


# =============================================================================
# E-Mode Tests
# =============================================================================


class TestEModeOperations:
    """Tests for E-Mode operations."""

    def test_set_emode_eth_correlated(self, adapter: AaveV3Adapter) -> None:
        """Test setting E-Mode to ETH correlated."""
        result = adapter.set_user_emode(AaveV3EModeCategory.ETH_CORRELATED)

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith("0x28530a47")  # setUserEMode
        assert "ETH Correlated" in result.description

    def test_set_emode_stablecoins(self, adapter: AaveV3Adapter) -> None:
        """Test setting E-Mode to stablecoins."""
        result = adapter.set_user_emode(AaveV3EModeCategory.STABLECOINS)

        assert result.success is True
        assert "Stablecoins" in result.description

    def test_disable_emode(self, adapter: AaveV3Adapter) -> None:
        """Test disabling E-Mode."""
        result = adapter.set_user_emode(AaveV3EModeCategory.NONE)

        assert result.success is True
        assert "None" in result.description

    def test_get_emode_category_data(self, adapter: AaveV3Adapter) -> None:
        """Test getting E-Mode category data."""
        data = adapter.get_emode_category_data(1)

        assert data["id"] == 1
        assert data["label"] == "ETH correlated"
        assert data["ltv"] == 9300  # 93%


# =============================================================================
# Flash Loan Tests
# =============================================================================


class TestFlashLoanOperations:
    """Tests for flash loan operations."""

    def test_flash_loan_simple(self, adapter: AaveV3Adapter) -> None:
        """Test building a simple flash loan transaction."""
        receiver = "0x9876543210987654321098765432109876543210"
        result = adapter.flash_loan_simple(
            receiver_address=receiver,
            asset="USDC",
            amount=Decimal("100000"),
        )

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter.pool_address
        assert result.tx_data["data"].startswith("0x42b0b77c")  # flashLoanSimple
        assert "Simple flash loan: 100000 USDC" in result.description

    def test_flash_loan_multiple_assets(self, adapter: AaveV3Adapter) -> None:
        """Test building a multi-asset flash loan transaction."""
        receiver = "0x9876543210987654321098765432109876543210"
        result = adapter.flash_loan(
            receiver_address=receiver,
            assets=["USDC", "WETH"],
            amounts=[Decimal("100000"), Decimal("50")],
            modes=[0, 0],  # No debt
        )

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["data"].startswith("0xab9c4b5d")  # flashLoan
        assert "USDC" in result.description
        assert "WETH" in result.description

    def test_flash_loan_mismatched_arrays(self, adapter: AaveV3Adapter) -> None:
        """Test flash loan with mismatched array lengths fails."""
        receiver = "0x9876543210987654321098765432109876543210"
        result = adapter.flash_loan(
            receiver_address=receiver,
            assets=["USDC", "WETH"],
            amounts=[Decimal("100000")],  # Missing second amount
            modes=[0, 0],
        )

        assert result.success is False
        assert "same length" in (result.error or "")


# =============================================================================
# Liquidation Tests
# =============================================================================


class TestLiquidationOperations:
    """Tests for liquidation operations."""

    def test_liquidation_call(self, adapter: AaveV3Adapter) -> None:
        """Test building a liquidation call transaction."""
        user_to_liquidate = "0x9876543210987654321098765432109876543210"
        result = adapter.liquidation_call(
            collateral_asset="WETH",
            debt_asset="USDC",
            user=user_to_liquidate,
            debt_to_cover=Decimal("1000"),
        )

        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == adapter.pool_address
        assert result.tx_data["data"].startswith("0x00a718a9")  # liquidationCall
        assert "Liquidate" in result.description

    def test_liquidation_receive_atoken(self, adapter: AaveV3Adapter) -> None:
        """Test liquidation with aToken receipt."""
        user_to_liquidate = "0x9876543210987654321098765432109876543210"
        result = adapter.liquidation_call(
            collateral_asset="WETH",
            debt_asset="USDC",
            user=user_to_liquidate,
            debt_to_cover=Decimal("1000"),
            receive_atoken=True,
        )

        assert result.success is True


# =============================================================================
# Health Factor Calculation Tests
# =============================================================================


class TestHealthFactorCalculations:
    """Tests for health factor calculations."""

    def test_calculate_health_factor_healthy(
        self,
        adapter: AaveV3Adapter,
        sample_positions: list[AaveV3Position],
        sample_reserve_data: dict[str, AaveV3ReserveData],
    ) -> None:
        """Test health factor calculation for a healthy position."""
        prices = {
            "USDC": Decimal("1"),
            "WETH": Decimal("2000"),
        }

        result = adapter.calculate_health_factor(
            positions=sample_positions,
            reserve_data=sample_reserve_data,
            prices=prices,
        )

        # 10,000 USDC at 85% LT = $8,500 effective collateral
        # 2 WETH at $2000 = $4,000 debt
        # HF = 8,500 / 4,000 = 2.125
        assert result.is_healthy is True
        assert result.health_factor > Decimal("2")
        assert result.total_collateral_usd == Decimal("10000")
        assert result.total_debt_usd == Decimal("4000")

    def test_calculate_health_factor_liquidatable(
        self,
        adapter: AaveV3Adapter,
        sample_reserve_data: dict[str, AaveV3ReserveData],
    ) -> None:
        """Test health factor calculation for a liquidatable position."""
        # Create position that's close to liquidation
        positions = [
            AaveV3Position(
                asset="USDC",
                asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                current_atoken_balance=Decimal("1000"),  # 1,000 USDC
                current_variable_debt=Decimal("0"),
                usage_as_collateral_enabled=True,
            ),
            AaveV3Position(
                asset="WETH",
                asset_address="0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
                current_atoken_balance=Decimal("0"),
                current_variable_debt=Decimal("0.5"),  # 0.5 WETH = $1000
                usage_as_collateral_enabled=False,
            ),
        ]

        prices = {
            "USDC": Decimal("1"),
            "WETH": Decimal("2000"),
        }

        result = adapter.calculate_health_factor(
            positions=positions,
            reserve_data=sample_reserve_data,
            prices=prices,
        )

        # $1000 USDC at 85% LT = $850 effective collateral
        # $1000 WETH debt
        # HF = 850 / 1000 = 0.85 (liquidatable)
        assert result.is_healthy is False
        assert result.health_factor < Decimal("1")

    def test_calculate_health_factor_no_debt(
        self,
        adapter: AaveV3Adapter,
        sample_reserve_data: dict[str, AaveV3ReserveData],
    ) -> None:
        """Test health factor with no debt."""
        positions = [
            AaveV3Position(
                asset="USDC",
                asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                current_atoken_balance=Decimal("10000"),
                current_variable_debt=Decimal("0"),
                usage_as_collateral_enabled=True,
            ),
        ]

        prices = {"USDC": Decimal("1")}

        result = adapter.calculate_health_factor(
            positions=positions,
            reserve_data=sample_reserve_data,
            prices=prices,
        )

        # No debt = very high health factor
        assert result.is_healthy is True
        assert result.health_factor > Decimal("1000")

    def test_calculate_health_factor_with_emode(
        self,
        adapter: AaveV3Adapter,
        sample_reserve_data: dict[str, AaveV3ReserveData],
    ) -> None:
        """Test health factor calculation with E-Mode enabled."""
        positions = [
            AaveV3Position(
                asset="USDC",
                asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                current_atoken_balance=Decimal("10000"),
                current_variable_debt=Decimal("9500"),  # Borrowed close to limit
                usage_as_collateral_enabled=True,
            ),
        ]

        prices = {"USDC": Decimal("1")}

        # Without E-Mode
        result_normal = adapter.calculate_health_factor(
            positions=positions,
            reserve_data=sample_reserve_data,
            prices=prices,
            emode_category=0,
        )

        # With E-Mode (stablecoins)
        result_emode = adapter.calculate_health_factor(
            positions=positions,
            reserve_data=sample_reserve_data,
            prices=prices,
            emode_category=2,  # Stablecoins
        )

        # E-Mode should give higher health factor due to higher LT (97.5% vs 85%)
        assert result_emode.health_factor > result_normal.health_factor


# =============================================================================
# Liquidation Price Calculation Tests
# =============================================================================


class TestLiquidationPriceCalculations:
    """Tests for liquidation price calculations."""

    def test_calculate_liquidation_price(self, adapter: AaveV3Adapter) -> None:
        """Test liquidation price calculation."""
        # 1 ETH collateral, $1000 debt, 80% LT
        liquidation_price = adapter.calculate_liquidation_price(
            collateral_asset="WETH",
            collateral_amount=Decimal("1"),
            debt_usd=Decimal("1000"),
            liquidation_threshold_bps=8000,
        )

        # Liquidation at: Price * Amount * LT = Debt
        # Price = Debt / (Amount * LT) = 1000 / (1 * 0.8) = 1250
        assert liquidation_price == Decimal("1250")

    def test_calculate_max_borrow(self, adapter: AaveV3Adapter) -> None:
        """Test max borrow calculation."""
        max_borrow = adapter.calculate_max_borrow(
            collateral_value_usd=Decimal("10000"),
            current_debt_usd=Decimal("3000"),
            ltv_bps=8000,
        )

        # Max = 10000 * 0.8 = 8000, available = 8000 - 3000 = 5000
        assert max_borrow == Decimal("5000")

    def test_calculate_health_factor_after_borrow(
        self,
        adapter: AaveV3Adapter,
        sample_positions: list[AaveV3Position],
        sample_reserve_data: dict[str, AaveV3ReserveData],
    ) -> None:
        """Test projected health factor after borrow."""
        prices = {
            "USDC": Decimal("1"),
            "WETH": Decimal("2000"),
        }

        current_calc = adapter.calculate_health_factor(
            positions=sample_positions,
            reserve_data=sample_reserve_data,
            prices=prices,
        )

        new_hf = adapter.calculate_health_factor_after_borrow(
            current_hf_calc=current_calc,
            borrow_amount_usd=Decimal("2000"),
        )

        # Additional $2000 debt should reduce health factor
        assert new_hf < current_calc.health_factor


# =============================================================================
# Approve Transaction Tests
# =============================================================================


class TestApproveOperations:
    """Tests for approve operations."""

    def test_build_approve_tx(self, adapter: AaveV3Adapter) -> None:
        """Test building an approve transaction."""
        result = adapter.build_approve_tx("USDC", Decimal("1000"))

        assert result.success is True
        assert result.tx_data is not None
        asset_address = AAVE_V3_TOKEN_ADDRESSES["arbitrum"]["USDC"]
        assert result.tx_data["to"] == asset_address
        assert result.tx_data["data"].startswith("0x095ea7b3")  # approve selector

    def test_build_unlimited_approve_tx(self, adapter: AaveV3Adapter) -> None:
        """Test building an unlimited approve transaction."""
        result = adapter.build_approve_tx("USDC")  # No amount = unlimited

        assert result.success is True
        assert "unlimited" in result.description


# =============================================================================
# Data Class Tests
# =============================================================================


class TestDataClasses:
    """Tests for data classes."""

    def test_reserve_data_is_isolated(self) -> None:
        """Test reserve data isolation mode detection."""
        isolated = AaveV3ReserveData(
            asset="NEW_TOKEN",
            asset_address="0x1234567890123456789012345678901234567890",
            debt_ceiling=Decimal("1000000"),  # Has debt ceiling
        )
        assert isolated.is_isolated is True

        not_isolated = AaveV3ReserveData(
            asset="USDC",
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            debt_ceiling=Decimal("0"),  # No debt ceiling
        )
        assert not_isolated.is_isolated is False

    def test_position_properties(self) -> None:
        """Test position property methods."""
        position = AaveV3Position(
            asset="USDC",
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=Decimal("1000"),
            current_stable_debt=Decimal("100"),
            current_variable_debt=Decimal("200"),
            usage_as_collateral_enabled=True,
        )

        assert position.total_debt == Decimal("300")
        assert position.has_supply is True
        assert position.has_debt is True
        assert position.is_collateral is True

    def test_position_to_dict(self) -> None:
        """Test position serialization."""
        position = AaveV3Position(
            asset="USDC",
            asset_address="0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            current_atoken_balance=Decimal("1000"),
        )

        data = position.to_dict()
        assert data["asset"] == "USDC"
        assert data["current_atoken_balance"] == "1000"


# =============================================================================
# Edge Case Tests
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases."""

    def test_zero_amount_operations(self, adapter: AaveV3Adapter) -> None:
        """Test operations with zero amounts."""
        # Supply with zero should still build tx (contract will reject)
        result = adapter.supply("USDC", Decimal("0"))
        assert result.success is True

    def test_max_uint256_operations(self, adapter: AaveV3Adapter) -> None:
        """Test operations using MAX_UINT256."""
        result = adapter.withdraw("USDC", Decimal("0"), withdraw_all=True)
        assert result.success is True

        result = adapter.repay("WETH", Decimal("0"), repay_all=True)
        assert result.success is True

    def test_case_insensitive_asset_lookup(self, adapter: AaveV3Adapter) -> None:
        """Test that asset lookup is case-insensitive."""
        result1 = adapter.supply("USDC", Decimal("100"))
        result2 = adapter.supply("usdc", Decimal("100"))

        assert result1.success is True
        assert result2.success is True

    def test_asset_address_as_input(self, adapter: AaveV3Adapter) -> None:
        """Test using asset address directly."""
        usdc_address = AAVE_V3_TOKEN_ADDRESSES["arbitrum"]["USDC"]
        result = adapter.supply(usdc_address, Decimal("100"))

        assert result.success is True
