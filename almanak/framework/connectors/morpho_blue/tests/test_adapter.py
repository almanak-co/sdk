"""Unit tests for Morpho Blue Adapter."""

from decimal import Decimal

import pytest

from ..adapter import (
    DEFAULT_GAS_ESTIMATES,
    MORPHO_BLUE_ADDRESS,
    MORPHO_BLUE_ADDRESSES,
    MORPHO_BORROW_SELECTOR,
    MORPHO_MARKETS,
    MORPHO_REPAY_SELECTOR,
    MORPHO_SUPPLY_COLLATERAL_SELECTOR,
    MORPHO_SUPPLY_SELECTOR,
    MORPHO_TOKEN_ADDRESSES,
    MORPHO_WITHDRAW_COLLATERAL_SELECTOR,
    MORPHO_WITHDRAW_SELECTOR,
    TOKEN_DECIMALS,
    MorphoBlueAdapter,
    MorphoBlueConfig,
    MorphoBlueHealthFactor,
    MorphoBlueMarketParams,
    MorphoBlueMarketState,
    MorphoBluePosition,
    TransactionResult,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def ethereum_config() -> MorphoBlueConfig:
    """Create Ethereum config fixture."""
    return MorphoBlueConfig(
        chain="ethereum",
        wallet_address="0x1234567890123456789012345678901234567890",
    )


@pytest.fixture
def base_config() -> MorphoBlueConfig:
    """Create Base config fixture."""
    return MorphoBlueConfig(
        chain="base",
        wallet_address="0x1234567890123456789012345678901234567890",
    )


@pytest.fixture
def ethereum_adapter(ethereum_config: MorphoBlueConfig) -> MorphoBlueAdapter:
    """Create Ethereum adapter fixture."""
    return MorphoBlueAdapter(ethereum_config)


@pytest.fixture
def base_adapter(base_config: MorphoBlueConfig) -> MorphoBlueAdapter:
    """Create Base adapter fixture."""
    return MorphoBlueAdapter(base_config)


# wstETH/USDC market on Ethereum
WSTETH_USDC_MARKET_ID = "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc"


# =============================================================================
# Config Tests
# =============================================================================


class TestMorphoBlueConfig:
    """Tests for MorphoBlueConfig."""

    def test_valid_ethereum_config(self) -> None:
        """Test valid Ethereum config."""
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
        )
        assert config.chain == "ethereum"
        assert config.wallet_address == "0x1234567890123456789012345678901234567890"
        assert config.default_slippage_bps == 50

    def test_valid_base_config(self) -> None:
        """Test valid Base config."""
        config = MorphoBlueConfig(
            chain="base",
            wallet_address="0xabcdefabcdefabcdefabcdefabcdefabcdefabcd",
        )
        assert config.chain == "base"

    def test_custom_slippage(self) -> None:
        """Test custom slippage configuration."""
        config = MorphoBlueConfig(
            chain="ethereum",
            wallet_address="0x1234567890123456789012345678901234567890",
            default_slippage_bps=100,
        )
        assert config.default_slippage_bps == 100

    def test_invalid_chain(self) -> None:
        """Test invalid chain raises error."""
        with pytest.raises(ValueError, match="Invalid chain"):
            MorphoBlueConfig(
                chain="invalid_chain",
                wallet_address="0x1234567890123456789012345678901234567890",
            )

    def test_invalid_wallet_address(self) -> None:
        """Test invalid wallet address raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            MorphoBlueConfig(
                chain="ethereum",
                wallet_address="invalid_address",
            )

    def test_invalid_wallet_address_short(self) -> None:
        """Test short wallet address raises error."""
        with pytest.raises(ValueError, match="Invalid wallet address"):
            MorphoBlueConfig(
                chain="ethereum",
                wallet_address="0x1234",
            )

    def test_invalid_slippage_negative(self) -> None:
        """Test negative slippage raises error."""
        with pytest.raises(ValueError, match="Invalid slippage"):
            MorphoBlueConfig(
                chain="ethereum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=-1,
            )

    def test_invalid_slippage_too_high(self) -> None:
        """Test slippage > 100% raises error."""
        with pytest.raises(ValueError, match="Invalid slippage"):
            MorphoBlueConfig(
                chain="ethereum",
                wallet_address="0x1234567890123456789012345678901234567890",
                default_slippage_bps=10001,
            )


# =============================================================================
# Adapter Initialization Tests
# =============================================================================


class TestMorphoBlueAdapterInit:
    """Tests for MorphoBlueAdapter initialization."""

    def test_ethereum_adapter_init(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test Ethereum adapter initialization."""
        assert ethereum_adapter.chain == "ethereum"
        assert ethereum_adapter.morpho_address == MORPHO_BLUE_ADDRESS
        assert len(ethereum_adapter.markets) > 0
        assert len(ethereum_adapter.token_addresses) > 0

    def test_base_adapter_init(self, base_adapter: MorphoBlueAdapter) -> None:
        """Test Base adapter initialization."""
        assert base_adapter.chain == "base"
        assert base_adapter.morpho_address == MORPHO_BLUE_ADDRESS
        assert len(base_adapter.markets) > 0

    def test_morpho_address_same_on_all_chains(self) -> None:
        """Test Morpho Blue address is the same on all chains."""
        for _chain, address in MORPHO_BLUE_ADDRESSES.items():
            assert address == MORPHO_BLUE_ADDRESS


# =============================================================================
# Market Information Tests
# =============================================================================


class TestMarketInfo:
    """Tests for market information retrieval."""

    def test_get_market_info(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test getting market info."""
        market_info = ethereum_adapter.get_market_info(WSTETH_USDC_MARKET_ID)
        assert market_info is not None
        assert market_info["name"] == "wstETH/USDC"
        assert market_info["loan_token"] == "USDC"
        assert market_info["collateral_token"] == "wstETH"

    def test_get_market_info_lowercase(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test getting market info with lowercase market_id."""
        market_info = ethereum_adapter.get_market_info(WSTETH_USDC_MARKET_ID.lower())
        assert market_info is not None

    def test_get_market_info_without_0x(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test getting market info without 0x prefix."""
        market_id_no_prefix = WSTETH_USDC_MARKET_ID[2:]
        market_info = ethereum_adapter.get_market_info(market_id_no_prefix)
        assert market_info is not None

    def test_get_market_info_unknown(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test getting unknown market returns None."""
        market_info = ethereum_adapter.get_market_info("0x" + "0" * 64)
        assert market_info is None

    def test_get_markets(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test getting all markets."""
        markets = ethereum_adapter.get_markets()
        assert len(markets) > 0
        assert WSTETH_USDC_MARKET_ID in markets

    def test_get_market_params(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test getting market params."""
        params = ethereum_adapter.get_market_params(WSTETH_USDC_MARKET_ID)
        assert params is not None
        assert isinstance(params, MorphoBlueMarketParams)
        assert params.lltv == 860000000000000000  # 86%


# =============================================================================
# Supply Tests
# =============================================================================


class TestSupply:
    """Tests for supply operation."""

    def test_supply_success(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test successful supply transaction build."""
        result = ethereum_adapter.supply(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("1000"),
        )
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == MORPHO_BLUE_ADDRESS
        assert result.tx_data["value"] == 0
        assert result.tx_data["data"].startswith(MORPHO_SUPPLY_SELECTOR)
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["supply"]
        assert "Supply" in result.description

    def test_supply_shares_mode(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test supply with shares mode."""
        result = ethereum_adapter.supply(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("1000000000000000000"),  # 1e18 shares
            shares_mode=True,
        )
        assert result.success is True
        assert "shares" in result.description

    def test_supply_on_behalf_of(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test supply on behalf of another address."""
        recipient = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        result = ethereum_adapter.supply(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("1000"),
            on_behalf_of=recipient,
        )
        assert result.success is True

    def test_supply_unknown_market(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test supply with unknown market fails."""
        result = ethereum_adapter.supply(
            market_id="0x" + "0" * 64,
            amount=Decimal("1000"),
        )
        assert result.success is False
        assert result.error is not None
        assert "Unknown market" in result.error


# =============================================================================
# Withdraw Tests
# =============================================================================


class TestWithdraw:
    """Tests for withdraw operation."""

    def test_withdraw_success(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test successful withdraw transaction build."""
        result = ethereum_adapter.withdraw(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("500"),
        )
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == MORPHO_BLUE_ADDRESS
        assert result.tx_data["data"].startswith(MORPHO_WITHDRAW_SELECTOR)
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["withdraw"]
        assert "Withdraw" in result.description

    def test_withdraw_all(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test withdraw all."""
        result = ethereum_adapter.withdraw(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("0"),
            withdraw_all=True,
        )
        assert result.success is True
        assert "all" in result.description

    def test_withdraw_shares_mode(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test withdraw with shares mode."""
        result = ethereum_adapter.withdraw(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("1000000000000000000"),
            shares_mode=True,
        )
        assert result.success is True
        assert "shares" in result.description

    def test_withdraw_to_different_receiver(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test withdraw to different receiver."""
        receiver = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        result = ethereum_adapter.withdraw(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("500"),
            receiver=receiver,
        )
        assert result.success is True


# =============================================================================
# Supply Collateral Tests
# =============================================================================


class TestSupplyCollateral:
    """Tests for supply collateral operation."""

    def test_supply_collateral_success(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test successful supply collateral transaction build."""
        result = ethereum_adapter.supply_collateral(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("1.5"),
        )
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == MORPHO_BLUE_ADDRESS
        assert result.tx_data["data"].startswith(MORPHO_SUPPLY_COLLATERAL_SELECTOR)
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["supply_collateral"]
        assert "wstETH" in result.description
        assert "collateral" in result.description

    def test_supply_collateral_on_behalf_of(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test supply collateral on behalf of another address."""
        recipient = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        result = ethereum_adapter.supply_collateral(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("1.0"),
            on_behalf_of=recipient,
        )
        assert result.success is True


# =============================================================================
# Withdraw Collateral Tests
# =============================================================================


class TestWithdrawCollateral:
    """Tests for withdraw collateral operation."""

    def test_withdraw_collateral_success(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test successful withdraw collateral transaction build."""
        result = ethereum_adapter.withdraw_collateral(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("0.5"),
        )
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == MORPHO_BLUE_ADDRESS
        assert result.tx_data["data"].startswith(MORPHO_WITHDRAW_COLLATERAL_SELECTOR)
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["withdraw_collateral"]
        assert "collateral" in result.description

    def test_withdraw_collateral_all(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test withdraw all collateral."""
        result = ethereum_adapter.withdraw_collateral(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("0"),
            withdraw_all=True,
        )
        assert result.success is True
        assert "all" in result.description


# =============================================================================
# Borrow Tests
# =============================================================================


class TestBorrow:
    """Tests for borrow operation."""

    def test_borrow_success(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test successful borrow transaction build."""
        result = ethereum_adapter.borrow(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("1000"),
        )
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == MORPHO_BLUE_ADDRESS
        assert result.tx_data["data"].startswith(MORPHO_BORROW_SELECTOR)
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["borrow"]
        assert "Borrow" in result.description
        assert "USDC" in result.description

    def test_borrow_shares_mode(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test borrow with shares mode."""
        result = ethereum_adapter.borrow(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("1000000000000000000"),
            shares_mode=True,
        )
        assert result.success is True
        assert "shares" in result.description

    def test_borrow_to_different_receiver(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test borrow to different receiver."""
        receiver = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        result = ethereum_adapter.borrow(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("500"),
            receiver=receiver,
        )
        assert result.success is True

    def test_borrow_unknown_market(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test borrow with unknown market fails."""
        result = ethereum_adapter.borrow(
            market_id="0x" + "0" * 64,
            amount=Decimal("1000"),
        )
        assert result.success is False
        assert result.error is not None
        assert "Unknown market" in result.error


# =============================================================================
# Repay Tests
# =============================================================================


class TestRepay:
    """Tests for repay operation."""

    def test_repay_success(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test successful repay transaction build."""
        result = ethereum_adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("500"),
        )
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == MORPHO_BLUE_ADDRESS
        assert result.tx_data["data"].startswith(MORPHO_REPAY_SELECTOR)
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["repay"]
        assert "Repay" in result.description

    def test_repay_all(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test repay all."""
        result = ethereum_adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("0"),
            repay_all=True,
        )
        assert result.success is True
        assert "full debt" in result.description

    def test_repay_shares_mode(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test repay with shares mode."""
        result = ethereum_adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("1000000000000000000"),
            shares_mode=True,
        )
        assert result.success is True
        assert "shares" in result.description

    def test_repay_on_behalf_of(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test repay on behalf of another address."""
        borrower = "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
        result = ethereum_adapter.repay(
            market_id=WSTETH_USDC_MARKET_ID,
            amount=Decimal("500"),
            on_behalf_of=borrower,
        )
        assert result.success is True


# =============================================================================
# Health Factor Tests
# =============================================================================


class TestHealthFactor:
    """Tests for health factor calculations."""

    def test_calculate_health_factor_healthy(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test health factor calculation for healthy position."""
        hf = ethereum_adapter.calculate_health_factor(
            collateral_amount=Decimal("10"),
            collateral_price_usd=Decimal("2000"),  # 10 ETH @ $2000 = $20,000
            debt_amount=Decimal("10000"),
            debt_price_usd=Decimal("1"),  # $10,000 debt
            lltv=Decimal("0.86"),  # 86% LLTV
        )
        assert hf.collateral_value_usd == Decimal("20000")
        assert hf.debt_value_usd == Decimal("10000")
        assert hf.lltv == Decimal("0.86")
        # Health factor = (20000 * 0.86) / 10000 = 1.72
        assert hf.health_factor == Decimal("1.72")
        assert hf.is_healthy is True
        # Max borrow = 20000 * 0.86 - 10000 = 7200
        assert hf.max_borrow_usd == Decimal("7200")

    def test_calculate_health_factor_liquidatable(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test health factor calculation for liquidatable position."""
        hf = ethereum_adapter.calculate_health_factor(
            collateral_amount=Decimal("10"),
            collateral_price_usd=Decimal("1000"),  # 10 ETH @ $1000 = $10,000
            debt_amount=Decimal("10000"),
            debt_price_usd=Decimal("1"),  # $10,000 debt
            lltv=Decimal("0.86"),
        )
        # Health factor = (10000 * 0.86) / 10000 = 0.86 < 1
        assert hf.health_factor == Decimal("0.86")
        assert hf.is_healthy is False
        assert hf.max_borrow_usd == Decimal("0")

    def test_calculate_health_factor_no_debt(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test health factor calculation with no debt."""
        hf = ethereum_adapter.calculate_health_factor(
            collateral_amount=Decimal("10"),
            collateral_price_usd=Decimal("2000"),
            debt_amount=Decimal("0"),
            debt_price_usd=Decimal("1"),
            lltv=Decimal("0.86"),
        )
        assert hf.health_factor == Decimal("999999")  # Effectively infinite
        assert hf.is_healthy is True

    def test_health_factor_liquidation_threshold(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test liquidation threshold calculation."""
        hf = ethereum_adapter.calculate_health_factor(
            collateral_amount=Decimal("10"),
            collateral_price_usd=Decimal("2000"),
            debt_amount=Decimal("10000"),
            debt_price_usd=Decimal("1"),
            lltv=Decimal("0.86"),
        )
        # Liquidation threshold = 20000 * 0.86 = 17200
        assert hf.liquidation_threshold_usd == Decimal("17200")


# =============================================================================
# Approve Tests
# =============================================================================


class TestApprove:
    """Tests for approve operation."""

    def test_build_approve_transaction(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test building approve transaction."""
        result = ethereum_adapter.build_approve_transaction(
            token="USDC",
            amount=Decimal("1000"),
        )
        assert result.success is True
        assert result.tx_data is not None
        assert result.tx_data["to"] == MORPHO_TOKEN_ADDRESSES["ethereum"]["USDC"]
        assert result.tx_data["value"] == 0
        assert result.gas_estimate == DEFAULT_GAS_ESTIMATES["approve"]

    def test_build_approve_unlimited(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test building unlimited approve transaction."""
        result = ethereum_adapter.build_approve_transaction(
            token="USDC",
            amount=None,
        )
        assert result.success is True
        assert "unlimited" in result.description

    def test_build_approve_by_address(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test building approve with token address."""
        usdc_address = MORPHO_TOKEN_ADDRESSES["ethereum"]["USDC"]
        result = ethereum_adapter.build_approve_transaction(
            token=usdc_address,
            amount=Decimal("1000"),
        )
        assert result.success is True

    def test_build_approve_unknown_token(self, ethereum_adapter: MorphoBlueAdapter) -> None:
        """Test approve with unknown token fails."""
        result = ethereum_adapter.build_approve_transaction(
            token="UNKNOWN_TOKEN",
        )
        assert result.success is False
        assert result.error is not None
        assert "Unknown token" in result.error


# =============================================================================
# Data Class Tests
# =============================================================================


class TestDataClasses:
    """Tests for data classes."""

    def test_market_params_to_tuple(self) -> None:
        """Test MorphoBlueMarketParams.to_tuple()."""
        params = MorphoBlueMarketParams(
            loan_token="0x1111111111111111111111111111111111111111",
            collateral_token="0x2222222222222222222222222222222222222222",
            oracle="0x3333333333333333333333333333333333333333",
            irm="0x4444444444444444444444444444444444444444",
            lltv=860000000000000000,
        )
        result = params.to_tuple()
        assert len(result) == 5
        assert result[0] == params.loan_token
        assert result[4] == params.lltv

    def test_market_params_to_dict(self) -> None:
        """Test MorphoBlueMarketParams.to_dict()."""
        params = MorphoBlueMarketParams(
            loan_token="0x1111111111111111111111111111111111111111",
            collateral_token="0x2222222222222222222222222222222222222222",
            oracle="0x3333333333333333333333333333333333333333",
            irm="0x4444444444444444444444444444444444444444",
            lltv=860000000000000000,
        )
        result = params.to_dict()
        assert result["loan_token"] == params.loan_token
        assert result["lltv_percent"] == 86.0

    def test_market_state_utilization(self) -> None:
        """Test MorphoBlueMarketState utilization calculation."""
        state = MorphoBlueMarketState(
            market_id="0x" + "1" * 64,
            total_supply_assets=Decimal("1000000"),
            total_borrow_assets=Decimal("500000"),
        )
        assert state.utilization == Decimal("0.5")

    def test_market_state_utilization_empty(self) -> None:
        """Test utilization with no supply."""
        state = MorphoBlueMarketState(
            market_id="0x" + "1" * 64,
            total_supply_assets=Decimal("0"),
            total_borrow_assets=Decimal("0"),
        )
        assert state.utilization == Decimal("0")

    def test_position_properties(self) -> None:
        """Test MorphoBluePosition properties."""
        position = MorphoBluePosition(
            market_id="0x" + "1" * 64,
            supply_shares=Decimal("1000"),
            borrow_shares=Decimal("500"),
            collateral=Decimal("10"),
        )
        assert position.has_supply is True
        assert position.has_borrow is True
        assert position.has_collateral is True

    def test_position_empty(self) -> None:
        """Test empty position."""
        position = MorphoBluePosition(market_id="0x" + "1" * 64)
        assert position.has_supply is False
        assert position.has_borrow is False
        assert position.has_collateral is False

    def test_transaction_result_to_dict(self) -> None:
        """Test TransactionResult.to_dict()."""
        result = TransactionResult(
            success=True,
            tx_data={"to": "0x123", "value": 0, "data": "0x456"},
            gas_estimate=150000,
            description="Test transaction",
        )
        d = result.to_dict()
        assert d["success"] is True
        assert d["tx_data"]["to"] == "0x123"
        assert d["gas_estimate"] == 150000

    def test_health_factor_to_dict(self) -> None:
        """Test MorphoBlueHealthFactor.to_dict()."""
        hf = MorphoBlueHealthFactor(
            collateral_value_usd=Decimal("20000"),
            debt_value_usd=Decimal("10000"),
            lltv=Decimal("0.86"),
            health_factor=Decimal("1.72"),
            max_borrow_usd=Decimal("7200"),
        )
        d = hf.to_dict()
        assert d["collateral_value_usd"] == "20000"
        assert d["is_healthy"] is True
        # Decimal may include trailing zeros
        assert Decimal(d["liquidation_threshold_usd"]) == Decimal("17200")


# =============================================================================
# Constants Tests
# =============================================================================


class TestConstants:
    """Tests for module constants."""

    def test_morpho_address_format(self) -> None:
        """Test Morpho Blue address format."""
        assert MORPHO_BLUE_ADDRESS.startswith("0x")
        assert len(MORPHO_BLUE_ADDRESS) == 42

    def test_gas_estimates_positive(self) -> None:
        """Test gas estimates are positive."""
        for operation, gas in DEFAULT_GAS_ESTIMATES.items():
            assert gas > 0, f"Gas estimate for {operation} should be positive"

    def test_token_decimals(self) -> None:
        """Test token decimals are reasonable."""
        for token, decimals in TOKEN_DECIMALS.items():
            assert 0 <= decimals <= 18, f"Decimals for {token} should be 0-18"

    def test_markets_have_required_fields(self) -> None:
        """Test all markets have required fields."""
        required_fields = [
            "name",
            "loan_token",
            "loan_token_address",
            "collateral_token",
            "collateral_token_address",
            "oracle",
            "irm",
            "lltv",
        ]
        for chain, markets in MORPHO_MARKETS.items():
            for market_id, market_info in markets.items():
                for field in required_fields:
                    assert field in market_info, f"Market {market_id} on {chain} missing {field}"

    def test_markets_lltv_range(self) -> None:
        """Test market LLTV is in valid range."""
        for _chain, markets in MORPHO_MARKETS.items():
            for market_id, market_info in markets.items():
                lltv = market_info["lltv"]
                # LLTV should be between 0 and 1e18 (0-100%)
                assert 0 < lltv < 10**18, f"LLTV for {market_id} out of range"
