"""Unit tests for fee models.

This module tests all fee model implementations to ensure correct fee calculations
for each protocol: Uniswap V3, PancakeSwap V3, Aerodrome, Curve, Aave V3,
Morpho, Compound V3, GMX, and Hyperliquid.
"""

from decimal import Decimal

import pytest

from almanak.framework.backtesting.models import IntentType
from almanak.framework.backtesting.pnl.fee_models import (
    AaveV3FeeModel,
    AerodromeFeeModel,
    AerodromePoolType,
    CompoundV3FeeModel,
    CompoundV3Market,
    CurveFeeModel,
    CurvePoolType,
    FeeModel,
    FeeModelMetadata,
    FeeModelRegistry,
    GMXFeeModel,
    HyperliquidFeeModel,
    HyperliquidFeeTier,
    MorphoFeeModel,
    PancakeSwapV3FeeModel,
    PancakeSwapV3FeeTier,
    UniswapV3FeeModel,
    UniswapV3FeeTier,
    UniswapV3SlippageModel,
    get_fee_model,
    get_fee_model_registry,
)

# =============================================================================
# FeeModelRegistry Tests
# =============================================================================


class TestFeeModelRegistry:
    """Test FeeModelRegistry class."""

    def test_get_registered_model(self) -> None:
        """Test getting a registered fee model by protocol name."""
        model_class = FeeModelRegistry.get("uniswap_v3")
        assert model_class is not None
        assert model_class == UniswapV3FeeModel

    def test_get_model_by_alias(self) -> None:
        """Test getting a model by alias."""
        # Uniswap aliases
        assert FeeModelRegistry.get("uniswap") == UniswapV3FeeModel
        assert FeeModelRegistry.get("uni_v3") == UniswapV3FeeModel

        # Aave aliases
        assert FeeModelRegistry.get("aave") == AaveV3FeeModel
        assert FeeModelRegistry.get("aave_v2") == AaveV3FeeModel

        # Hyperliquid aliases
        assert FeeModelRegistry.get("hl") == HyperliquidFeeModel
        assert FeeModelRegistry.get("hyper") == HyperliquidFeeModel

    def test_get_model_case_insensitive(self) -> None:
        """Test registry lookup is case-insensitive."""
        assert FeeModelRegistry.get("UNISWAP_V3") == UniswapV3FeeModel
        assert FeeModelRegistry.get("Aave_V3") == AaveV3FeeModel
        assert FeeModelRegistry.get("GMX") == GMXFeeModel

    def test_get_unknown_protocol_returns_none(self) -> None:
        """Test getting unknown protocol returns None."""
        assert FeeModelRegistry.get("unknown_protocol") is None

    def test_list_protocols(self) -> None:
        """Test listing all registered protocols."""
        protocols = FeeModelRegistry.list_protocols()
        assert "uniswap_v3" in protocols
        assert "pancakeswap_v3" in protocols
        assert "aerodrome" in protocols
        assert "curve" in protocols
        assert "aave_v3" in protocols
        assert "morpho" in protocols
        assert "compound_v3" in protocols
        assert "gmx" in protocols
        assert "hyperliquid" in protocols

    def test_get_metadata(self) -> None:
        """Test getting metadata for a registered model."""
        metadata = FeeModelRegistry.get_metadata("uniswap_v3")
        assert metadata is not None
        assert isinstance(metadata, FeeModelMetadata)
        assert metadata.name == "uniswap_v3"
        assert metadata.model_class == UniswapV3FeeModel
        assert "uni_v3" in metadata.protocols

    def test_list_all(self) -> None:
        """Test listing all models with metadata."""
        all_models = FeeModelRegistry.list_all()
        assert "uniswap_v3" in all_models
        assert "gmx" in all_models
        # Should not include aliases as separate entries
        assert len(all_models) >= 9


class TestGetFeeModel:
    """Test get_fee_model convenience function."""

    def test_get_fee_model_returns_instance(self) -> None:
        """Test that get_fee_model returns an instantiated model."""
        model = get_fee_model("uniswap_v3")
        assert model is not None
        assert isinstance(model, FeeModel)
        assert isinstance(model, UniswapV3FeeModel)

    def test_get_fee_model_unknown_returns_none(self) -> None:
        """Test that unknown protocol returns None."""
        model = get_fee_model("unknown_protocol")
        assert model is None

    def test_get_fee_model_registry_dict(self) -> None:
        """Test get_fee_model_registry returns dict."""
        registry = get_fee_model_registry()
        assert isinstance(registry, dict)
        assert "uniswap_v3" in registry
        assert registry["uniswap_v3"] == UniswapV3FeeModel


# =============================================================================
# Uniswap V3 Fee Model Tests
# =============================================================================


class TestUniswapV3FeeTier:
    """Test UniswapV3FeeTier enum."""

    def test_tier_values(self) -> None:
        """Test fee tier values."""
        assert UniswapV3FeeTier.LOWEST.value == "100"
        assert UniswapV3FeeTier.LOW.value == "500"
        assert UniswapV3FeeTier.MEDIUM.value == "3000"
        assert UniswapV3FeeTier.HIGH.value == "10000"

    def test_tier_fee_pct(self) -> None:
        """Test fee_pct property returns correct percentages."""
        assert UniswapV3FeeTier.LOWEST.fee_pct == Decimal("0.0001")  # 0.01%
        assert UniswapV3FeeTier.LOW.fee_pct == Decimal("0.0005")  # 0.05%
        assert UniswapV3FeeTier.MEDIUM.fee_pct == Decimal("0.003")  # 0.3%
        assert UniswapV3FeeTier.HIGH.fee_pct == Decimal("0.01")  # 1%

    def test_tier_fee_bps(self) -> None:
        """Test fee_bps property returns correct basis points."""
        assert UniswapV3FeeTier.LOWEST.fee_bps == 100
        assert UniswapV3FeeTier.LOW.fee_bps == 500
        assert UniswapV3FeeTier.MEDIUM.fee_bps == 3000
        assert UniswapV3FeeTier.HIGH.fee_bps == 10000


class TestUniswapV3FeeModel:
    """Test UniswapV3FeeModel class."""

    def test_model_name(self) -> None:
        """Test model_name property."""
        model = UniswapV3FeeModel()
        assert model.model_name == "uniswap_v3"

    def test_default_fee_tier(self) -> None:
        """Test default fee tier is MEDIUM (0.3%)."""
        model = UniswapV3FeeModel()
        assert model.default_fee_tier == UniswapV3FeeTier.MEDIUM

    def test_swap_fee_calculation(self) -> None:
        """Test swap fee calculation with default tier."""
        model = UniswapV3FeeModel()
        fee = model.calculate_fee(Decimal("1000"), intent_type=IntentType.SWAP)
        # 0.3% of 1000 = 3
        assert fee == Decimal("3")

    def test_swap_fee_with_explicit_tier(self) -> None:
        """Test swap fee with explicit fee tier."""
        model = UniswapV3FeeModel()
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            fee_tier=UniswapV3FeeTier.LOW,
        )
        # 0.05% of 1000 = 0.5
        assert fee == Decimal("0.5")

    def test_swap_fee_with_fee_tier_bps(self) -> None:
        """Test swap fee with fee_tier_bps parameter."""
        model = UniswapV3FeeModel()
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            fee_tier_bps=100,  # 0.01%
        )
        assert fee == Decimal("0.1")

    def test_lp_open_no_fee(self) -> None:
        """Test LP_OPEN has no protocol fee."""
        model = UniswapV3FeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.LP_OPEN)
        assert fee == Decimal("0")

    def test_lp_close_no_fee(self) -> None:
        """Test LP_CLOSE has no protocol fee."""
        model = UniswapV3FeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.LP_CLOSE)
        assert fee == Decimal("0")

    def test_token_pair_tier_lookup(self) -> None:
        """Test token pair specific fee tier lookup."""
        model = UniswapV3FeeModel(
            token_pair_tiers={
                ("USDC", "USDT"): UniswapV3FeeTier.LOWEST,
                ("WETH", "USDC"): UniswapV3FeeTier.MEDIUM,
            }
        )
        # USDC/USDT should use LOWEST tier
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            token_in="USDC",
            token_out="USDT",
        )
        assert fee == Decimal("0.1")  # 0.01%

    def test_token_pair_reverse_lookup(self) -> None:
        """Test token pair lookup works in both directions."""
        model = UniswapV3FeeModel(
            token_pair_tiers={
                ("USDC", "USDT"): UniswapV3FeeTier.LOWEST,
            }
        )
        # USDT/USDC should also use LOWEST tier
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            token_in="USDT",
            token_out="USDC",
        )
        assert fee == Decimal("0.1")

    def test_to_dict_serialization(self) -> None:
        """Test serialization to dictionary."""
        model = UniswapV3FeeModel(
            default_fee_tier=UniswapV3FeeTier.LOW,
            token_pair_tiers={("ETH", "USDC"): UniswapV3FeeTier.MEDIUM},
        )
        data = model.to_dict()
        assert data["model_name"] == "uniswap_v3"
        assert data["default_fee_tier"] == "500"
        assert "ETH/USDC" in data["token_pair_tiers"]


class TestUniswapV3SlippageModel:
    """Test UniswapV3SlippageModel class."""

    def test_model_name(self) -> None:
        """Test model_name property."""
        model = UniswapV3SlippageModel()
        assert model.model_name == "uniswap_v3"

    def test_swap_slippage_calculation(self) -> None:
        """Test slippage calculation for swap."""
        model = UniswapV3SlippageModel(
            base_slippage_pct=Decimal("0.0005"),
            liquidity_depth_usd=Decimal("1000000"),
        )
        # Small trade should have minimal slippage
        slippage = model.calculate_slippage(
            IntentType.SWAP, Decimal("1000"), market_state=None
        )
        assert slippage > Decimal("0")
        assert slippage < Decimal("0.01")

    def test_slippage_capped_at_max(self) -> None:
        """Test slippage is capped at max_slippage_pct."""
        model = UniswapV3SlippageModel(
            base_slippage_pct=Decimal("0.01"),
            liquidity_depth_usd=Decimal("1000"),  # Very low liquidity
            max_slippage_pct=Decimal("0.05"),
        )
        # Large trade on low liquidity should hit cap
        slippage = model.calculate_slippage(
            IntentType.SWAP, Decimal("100000"), market_state=None
        )
        assert slippage == Decimal("0.05")

    def test_zero_slippage_for_non_trading_intents(self) -> None:
        """Test zero slippage for non-trading intents."""
        model = UniswapV3SlippageModel()
        assert model.calculate_slippage(IntentType.HOLD, Decimal("1000"), market_state=None) == Decimal("0")
        assert model.calculate_slippage(IntentType.SUPPLY, Decimal("1000"), market_state=None) == Decimal("0")
        assert model.calculate_slippage(IntentType.BORROW, Decimal("1000"), market_state=None) == Decimal("0")


# =============================================================================
# PancakeSwap V3 Fee Model Tests
# =============================================================================


class TestPancakeSwapV3FeeTier:
    """Test PancakeSwapV3FeeTier enum."""

    def test_tier_values(self) -> None:
        """Test fee tier values - note MEDIUM is 0.25% (not 0.3%)."""
        assert PancakeSwapV3FeeTier.LOWEST.value == "100"
        assert PancakeSwapV3FeeTier.LOW.value == "500"
        assert PancakeSwapV3FeeTier.MEDIUM.value == "2500"  # Different from Uniswap
        assert PancakeSwapV3FeeTier.HIGH.value == "10000"

    def test_tier_fee_pct(self) -> None:
        """Test fee_pct property."""
        assert PancakeSwapV3FeeTier.MEDIUM.fee_pct == Decimal("0.0025")  # 0.25%


class TestPancakeSwapV3FeeModel:
    """Test PancakeSwapV3FeeModel class."""

    def test_model_name(self) -> None:
        """Test model_name property."""
        model = PancakeSwapV3FeeModel()
        assert model.model_name == "pancakeswap_v3"

    def test_swap_fee_calculation(self) -> None:
        """Test swap fee with default tier (0.25%)."""
        model = PancakeSwapV3FeeModel()
        fee = model.calculate_fee(Decimal("1000"), intent_type=IntentType.SWAP)
        # 0.25% of 1000 = 2.5
        assert fee == Decimal("2.5")

    def test_lp_operations_no_fee(self) -> None:
        """Test LP operations have no fee."""
        model = PancakeSwapV3FeeModel()
        assert model.calculate_fee(Decimal("1000"), intent_type=IntentType.LP_OPEN) == Decimal("0")
        assert model.calculate_fee(Decimal("1000"), intent_type=IntentType.LP_CLOSE) == Decimal("0")


# =============================================================================
# Aerodrome Fee Model Tests
# =============================================================================


class TestAerodromePoolType:
    """Test AerodromePoolType enum."""

    def test_pool_types(self) -> None:
        """Test pool type values."""
        assert AerodromePoolType.STABLE.value == "stable"
        assert AerodromePoolType.VOLATILE.value == "volatile"


class TestAerodromeFeeModel:
    """Test AerodromeFeeModel class."""

    def test_model_name(self) -> None:
        """Test model_name property."""
        model = AerodromeFeeModel()
        assert model.model_name == "aerodrome"

    def test_volatile_pool_fee(self) -> None:
        """Test volatile pool fee (default 0.3%)."""
        model = AerodromeFeeModel()
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=AerodromePoolType.VOLATILE,
        )
        assert fee == Decimal("3")  # 0.3%

    def test_stable_pool_fee(self) -> None:
        """Test stable pool fee (default 0.01%)."""
        model = AerodromeFeeModel()
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=AerodromePoolType.STABLE,
        )
        assert fee == Decimal("0.1")  # 0.01%

    def test_auto_detect_stable_pair(self) -> None:
        """Test automatic detection of stable pairs."""
        model = AerodromeFeeModel()
        # USDC/USDT should be detected as stable pair
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            token_in="USDC",
            token_out="USDT",
        )
        assert fee == Decimal("0.1")  # 0.01% stable fee

    def test_default_volatile_for_unknown_pairs(self) -> None:
        """Test unknown pairs default to volatile."""
        model = AerodromeFeeModel()
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            token_in="ETH",
            token_out="ARB",
        )
        assert fee == Decimal("3")  # 0.3% volatile fee

    def test_custom_fee_rates(self) -> None:
        """Test custom stable/volatile fee rates."""
        model = AerodromeFeeModel(
            stable_fee_pct=Decimal("0.0005"),  # 0.05%
            volatile_fee_pct=Decimal("0.002"),  # 0.2%
        )
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=AerodromePoolType.VOLATILE,
        )
        assert fee == Decimal("2")  # 0.2%


# =============================================================================
# Curve Fee Model Tests
# =============================================================================


class TestCurvePoolType:
    """Test CurvePoolType enum."""

    def test_pool_types(self) -> None:
        """Test pool type values."""
        assert CurvePoolType.STABLE.value == "stable"
        assert CurvePoolType.TRICRYPTO.value == "tricrypto"
        assert CurvePoolType.CRVUSD.value == "crvusd"


class TestCurveFeeModel:
    """Test CurveFeeModel class."""

    def test_model_name(self) -> None:
        """Test model_name property."""
        model = CurveFeeModel()
        assert model.model_name == "curve"

    def test_stable_pool_base_fee(self) -> None:
        """Test stable pool base fee (0.04%)."""
        model = CurveFeeModel()
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=CurvePoolType.STABLE,
        )
        assert fee == Decimal("0.4")  # 0.04%

    def test_tricrypto_pool_base_fee(self) -> None:
        """Test tricrypto pool base fee (0.13%)."""
        model = CurveFeeModel()
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=CurvePoolType.TRICRYPTO,
        )
        assert fee == Decimal("1.3")  # 0.13%

    def test_crvusd_pool_fee(self) -> None:
        """Test crvUSD pool fee (0.01%)."""
        model = CurveFeeModel()
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=CurvePoolType.CRVUSD,
        )
        assert fee == Decimal("0.1")  # 0.01%

    def test_dynamic_fee_with_imbalance(self) -> None:
        """Test dynamic fee increases with pool imbalance."""
        model = CurveFeeModel()
        # No imbalance
        fee_balanced = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=CurvePoolType.STABLE,
            pool_imbalance=Decimal("0"),
        )
        # 30% imbalance
        fee_imbalanced = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=CurvePoolType.STABLE,
            pool_imbalance=Decimal("0.3"),
        )
        assert fee_imbalanced > fee_balanced

    def test_fee_capped_at_max_multiplier(self) -> None:
        """Test fee is capped at max multiplier."""
        model = CurveFeeModel(max_fee_multiplier=Decimal("3"))
        # Extreme imbalance (100%)
        fee = model.calculate_fee(
            Decimal("1000"),
            intent_type=IntentType.SWAP,
            pool_type=CurvePoolType.STABLE,
            pool_imbalance=Decimal("1.0"),
        )
        # Should be capped at 3x base fee: 0.04% * 3 = 0.12%
        assert fee == Decimal("1.2")

    def test_estimate_imbalance_fee_impact(self) -> None:
        """Test imbalance fee impact estimation."""
        model = CurveFeeModel()
        impact = model.estimate_imbalance_fee_impact(
            Decimal("1000"),
            pool_type=CurvePoolType.STABLE,
        )
        assert "balanced" in impact
        assert "moderate" in impact
        assert "extreme" in impact
        # Balanced should be lowest
        assert impact["balanced"] < impact["extreme"]


# =============================================================================
# Aave V3 Fee Model Tests
# =============================================================================


class TestAaveV3FeeModel:
    """Test AaveV3FeeModel class."""

    def test_model_name(self) -> None:
        """Test model_name property."""
        model = AaveV3FeeModel()
        assert model.model_name == "aave_v3"

    def test_borrow_origination_fee(self) -> None:
        """Test borrow origination fee (default 0.01%)."""
        model = AaveV3FeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.BORROW)
        assert fee == Decimal("1")  # 0.01% of 10000

    def test_supply_no_fee(self) -> None:
        """Test supply has no fee."""
        model = AaveV3FeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.SUPPLY)
        assert fee == Decimal("0")

    def test_withdraw_no_fee(self) -> None:
        """Test withdraw has no fee."""
        model = AaveV3FeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.WITHDRAW)
        assert fee == Decimal("0")

    def test_repay_no_fee(self) -> None:
        """Test repay has no fee."""
        model = AaveV3FeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.REPAY)
        assert fee == Decimal("0")

    def test_flash_loan_fee(self) -> None:
        """Test flash loan fee (0.05%)."""
        model = AaveV3FeeModel()
        fee = model.calculate_fee(
            Decimal("10000"),
            intent_type=IntentType.BORROW,
            is_flash_loan=True,
        )
        assert fee == Decimal("5")  # 0.05%

    def test_asset_specific_fee(self) -> None:
        """Test asset-specific fee configuration."""
        model = AaveV3FeeModel(
            borrow_origination_fee_pct=Decimal("0.0001"),  # 0.01% default
            asset_fees={
                "USDC": Decimal("0"),  # No fee for USDC
                "WETH": Decimal("0.0002"),  # 0.02% for WETH
            },
        )
        # USDC should have no fee
        fee_usdc = model.calculate_fee(Decimal("10000"), intent_type=IntentType.BORROW, asset="USDC")
        assert fee_usdc == Decimal("0")

        # WETH should have 0.02% fee
        fee_weth = model.calculate_fee(Decimal("10000"), intent_type=IntentType.BORROW, asset="WETH")
        assert fee_weth == Decimal("2")


# =============================================================================
# Morpho Fee Model Tests
# =============================================================================


class TestMorphoFeeModel:
    """Test MorphoFeeModel class."""

    def test_model_name(self) -> None:
        """Test model_name property."""
        model = MorphoFeeModel()
        assert model.model_name == "morpho"

    def test_borrow_no_fee(self) -> None:
        """Test borrow has no fee (fee-free operations)."""
        model = MorphoFeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.BORROW)
        assert fee == Decimal("0")

    def test_supply_no_fee(self) -> None:
        """Test supply has no fee."""
        model = MorphoFeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.SUPPLY)
        assert fee == Decimal("0")

    def test_liquidation_fee(self) -> None:
        """Test liquidation fee (5% default)."""
        model = MorphoFeeModel()
        fee = model.calculate_fee(
            Decimal("10000"),
            intent_type=IntentType.BORROW,  # Intent type doesn't matter for liquidation
            is_liquidation=True,
        )
        # 5% of 10000 = 500
        assert fee == Decimal("500")

    def test_custom_liquidation_incentive(self) -> None:
        """Test custom liquidation incentive factor."""
        model = MorphoFeeModel(liquidation_incentive_factor=Decimal("1.10"))  # 10%
        fee = model.calculate_fee(
            Decimal("10000"),
            is_liquidation=True,
        )
        assert fee == Decimal("1000")  # 10%

    def test_asset_specific_liquidation_incentive(self) -> None:
        """Test asset-specific liquidation incentives."""
        model = MorphoFeeModel(
            liquidation_incentive_factor=Decimal("1.05"),
            asset_liquidation_incentives={
                "WETH": Decimal("1.04"),  # 4%
                "WBTC": Decimal("1.06"),  # 6%
            },
        )
        # WETH should use 4%
        fee_weth = model.calculate_fee(
            Decimal("10000"), is_liquidation=True, asset="WETH"
        )
        assert fee_weth == Decimal("400")

        # WBTC should use 6%
        fee_wbtc = model.calculate_fee(
            Decimal("10000"), is_liquidation=True, asset="WBTC"
        )
        assert fee_wbtc == Decimal("600")


# =============================================================================
# Compound V3 Fee Model Tests
# =============================================================================


class TestCompoundV3Market:
    """Test CompoundV3Market enum."""

    def test_market_values(self) -> None:
        """Test market enum values."""
        assert CompoundV3Market.USDC_MAINNET.value == "usdc_mainnet"
        assert CompoundV3Market.WETH_ARBITRUM.value == "weth_arbitrum"


class TestCompoundV3FeeModel:
    """Test CompoundV3FeeModel class."""

    def test_model_name(self) -> None:
        """Test model_name property."""
        model = CompoundV3FeeModel()
        assert model.model_name == "compound_v3"

    def test_borrow_no_fee(self) -> None:
        """Test borrow has no fee (interest handled separately)."""
        model = CompoundV3FeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.BORROW)
        assert fee == Decimal("0")

    def test_supply_no_fee(self) -> None:
        """Test supply has no fee."""
        model = CompoundV3FeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.SUPPLY)
        assert fee == Decimal("0")

    def test_liquidation_fee_default(self) -> None:
        """Test liquidation fee with default discount (8%)."""
        model = CompoundV3FeeModel()
        fee = model.calculate_fee(
            Decimal("10000"),
            is_liquidation=True,
        )
        assert fee == Decimal("800")  # 8%

    def test_liquidation_fee_by_asset(self) -> None:
        """Test asset-specific liquidation discounts."""
        model = CompoundV3FeeModel()
        # WETH should have 5% discount
        fee_weth = model.calculate_fee(
            Decimal("10000"),
            is_liquidation=True,
            asset="WETH",
        )
        assert fee_weth == Decimal("500")  # 5%

        # UNI should have 10% discount
        fee_uni = model.calculate_fee(
            Decimal("10000"),
            is_liquidation=True,
            asset="UNI",
        )
        assert fee_uni == Decimal("1000")  # 10%

    def test_get_liquidation_discount(self) -> None:
        """Test get_liquidation_discount helper."""
        model = CompoundV3FeeModel()
        assert model.get_liquidation_discount("WETH") == Decimal("0.05")
        assert model.get_liquidation_discount("USDC") == Decimal("0.03")
        # Unknown asset should return default
        assert model.get_liquidation_discount("UNKNOWN") == Decimal("0.08")


# =============================================================================
# GMX Fee Model Tests
# =============================================================================


class TestGMXFeeModel:
    """Test GMXFeeModel class."""

    def test_model_name(self) -> None:
        """Test model_name property."""
        model = GMXFeeModel()
        assert model.model_name == "gmx"

    def test_perp_open_fee(self) -> None:
        """Test perp open fee (0.1% + execution fee)."""
        model = GMXFeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.PERP_OPEN)
        # 0.1% of 10000 = 10, plus $0.50 execution fee = $10.50
        assert fee == Decimal("10.5")

    def test_perp_close_fee(self) -> None:
        """Test perp close fee (0.1% + execution fee)."""
        model = GMXFeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.PERP_CLOSE)
        assert fee == Decimal("10.5")

    def test_perp_fee_with_leverage(self) -> None:
        """Test perp fee applies to leveraged notional."""
        model = GMXFeeModel()
        fee = model.calculate_fee(
            Decimal("1000"),  # $1000 collateral
            intent_type=IntentType.PERP_OPEN,
            leverage=Decimal("10"),  # 10x leverage = $10,000 notional
        )
        # 0.1% of 10000 = 10, plus $0.50 = $10.50
        assert fee == Decimal("10.5")

    def test_perp_fee_without_execution_fee(self) -> None:
        """Test excluding execution fee."""
        model = GMXFeeModel()
        fee = model.calculate_fee(
            Decimal("10000"),
            intent_type=IntentType.PERP_OPEN,
            include_execution_fee=False,
        )
        assert fee == Decimal("10")  # Just 0.1%, no execution fee

    def test_swap_fee(self) -> None:
        """Test swap fee (0.05%)."""
        model = GMXFeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.SWAP)
        assert fee == Decimal("5")  # 0.05%

    def test_hold_no_fee(self) -> None:
        """Test HOLD has no fee."""
        model = GMXFeeModel()
        fee = model.calculate_fee(Decimal("10000"), intent_type=IntentType.HOLD)
        assert fee == Decimal("0")

    def test_asset_specific_fee(self) -> None:
        """Test asset-specific position fees."""
        model = GMXFeeModel(
            position_fee_pct=Decimal("0.001"),  # 0.1% default
            asset_fees={
                "ETH": Decimal("0.0008"),  # 0.08% for ETH
            },
        )
        fee = model.calculate_fee(
            Decimal("10000"),
            intent_type=IntentType.PERP_OPEN,
            asset="ETH",
            include_execution_fee=False,
        )
        assert fee == Decimal("8")  # 0.08%


# =============================================================================
# Hyperliquid Fee Model Tests
# =============================================================================


class TestHyperliquidFeeTier:
    """Test HyperliquidFeeTier enum."""

    def test_tier_values(self) -> None:
        """Test fee tier values."""
        assert HyperliquidFeeTier.VIP_0.value == "vip_0"
        assert HyperliquidFeeTier.VIP_6.value == "vip_6"


class TestHyperliquidFeeModel:
    """Test HyperliquidFeeModel class."""

    def test_model_name(self) -> None:
        """Test model_name property."""
        model = HyperliquidFeeModel()
        assert model.model_name == "hyperliquid"

    def test_default_taker_fee_vip0(self) -> None:
        """Test default taker fee at VIP 0 (0.045%)."""
        model = HyperliquidFeeModel(fee_tier=HyperliquidFeeTier.VIP_0)
        fee = model.calculate_fee(Decimal("10000"), is_maker=False)
        assert fee == Decimal("4.5")  # 0.045%

    def test_default_maker_fee_vip0(self) -> None:
        """Test default maker fee at VIP 0 (0.015%)."""
        model = HyperliquidFeeModel(fee_tier=HyperliquidFeeTier.VIP_0)
        fee = model.calculate_fee(Decimal("10000"), is_maker=True)
        assert fee == Decimal("1.5")  # 0.015%

    def test_vip6_maker_fee_zero(self) -> None:
        """Test VIP 6 has 0% maker fee."""
        model = HyperliquidFeeModel(fee_tier=HyperliquidFeeTier.VIP_6)
        fee = model.calculate_fee(Decimal("10000"), is_maker=True)
        assert fee == Decimal("0")

    def test_volume_based_tier_selection(self) -> None:
        """Test automatic tier selection from volume."""
        # $150M volume should select VIP 3
        model = HyperliquidFeeModel(volume_14d=Decimal("150000000"))
        assert model.fee_tier == HyperliquidFeeTier.VIP_3
        # VIP 3 taker fee is 0.030%
        fee = model.calculate_fee(Decimal("10000"), is_maker=False)
        assert fee == Decimal("3")

    def test_fee_with_leverage(self) -> None:
        """Test fee applies to leveraged notional."""
        model = HyperliquidFeeModel(fee_tier=HyperliquidFeeTier.VIP_0)
        fee = model.calculate_fee(
            Decimal("1000"),  # $1000 collateral
            is_maker=False,
            leverage=Decimal("10"),  # 10x
        )
        # 0.045% of $10,000 = $4.50
        assert fee == Decimal("4.5")

    def test_staking_discount(self) -> None:
        """Test HYPE staking discount."""
        # 10k HYPE staked = 5% discount
        model = HyperliquidFeeModel(
            fee_tier=HyperliquidFeeTier.VIP_0,
            staked_hype=Decimal("10000"),
        )
        fee = model.calculate_fee(Decimal("10000"), is_maker=False)
        # 0.045% * 0.95 = 0.04275%
        assert fee == Decimal("4.275")

    def test_hip3_market_multiplier(self) -> None:
        """Test HIP-3 market 2x fee multiplier."""
        model = HyperliquidFeeModel(
            fee_tier=HyperliquidFeeTier.VIP_0,
            is_hip3_market=True,
        )
        fee = model.calculate_fee(Decimal("10000"), is_maker=False)
        # 0.045% * 2 = 0.09%
        assert fee == Decimal("9")

    def test_convenience_methods(self) -> None:
        """Test calculate_maker_fee and calculate_taker_fee helpers."""
        model = HyperliquidFeeModel(fee_tier=HyperliquidFeeTier.VIP_0)
        assert model.calculate_taker_fee(Decimal("10000")) == Decimal("4.5")
        assert model.calculate_maker_fee(Decimal("10000")) == Decimal("1.5")

    def test_fee_properties(self) -> None:
        """Test fee rate properties."""
        model = HyperliquidFeeModel(fee_tier=HyperliquidFeeTier.VIP_0)
        assert model.taker_fee_rate == Decimal("0.00045")
        assert model.maker_fee_rate == Decimal("0.00015")
        assert model.taker_fee_bps == Decimal("4.5")
        assert model.maker_fee_bps == Decimal("1.5")

    def test_get_fee_summary(self) -> None:
        """Test fee summary dictionary."""
        model = HyperliquidFeeModel(
            fee_tier=HyperliquidFeeTier.VIP_0,
            staked_hype=Decimal("10000"),
        )
        summary = model.get_fee_summary()
        assert summary["tier"] == "vip_0"
        assert "taker_fee_pct" in summary
        assert summary["staking_discount_pct"] == "5.0%"

    def test_serialization_roundtrip(self) -> None:
        """Test to_dict and from_dict roundtrip."""
        model = HyperliquidFeeModel(
            fee_tier=HyperliquidFeeTier.VIP_3,
            volume_14d=Decimal("150000000"),
            staked_hype=Decimal("50000"),
            is_hip3_market=True,
        )
        data = model.to_dict()
        restored = HyperliquidFeeModel.from_dict(data)
        assert restored.fee_tier == model.fee_tier
        assert restored.staked_hype == model.staked_hype
        assert restored.is_hip3_market == model.is_hip3_market


# =============================================================================
# Cross-Protocol Tests
# =============================================================================


class TestFeeModelProtocol:
    """Test that all fee models implement the FeeModel protocol correctly."""

    @pytest.fixture
    def all_models(self) -> list[FeeModel]:
        """Create instances of all fee models."""
        return [
            UniswapV3FeeModel(),
            PancakeSwapV3FeeModel(),
            AerodromeFeeModel(),
            CurveFeeModel(),
            AaveV3FeeModel(),
            MorphoFeeModel(),
            CompoundV3FeeModel(),
            GMXFeeModel(),
            HyperliquidFeeModel(),
        ]

    def test_all_have_model_name(self, all_models: list[FeeModel]) -> None:
        """Test all models have a model_name property."""
        for model in all_models:
            assert hasattr(model, "model_name")
            assert isinstance(model.model_name, str)
            assert len(model.model_name) > 0

    def test_all_have_calculate_fee(self, all_models: list[FeeModel]) -> None:
        """Test all models have a calculate_fee method."""
        for model in all_models:
            assert hasattr(model, "calculate_fee")
            assert callable(model.calculate_fee)

    def test_all_have_to_dict(self, all_models: list[FeeModel]) -> None:
        """Test all models have a to_dict method."""
        for model in all_models:
            assert hasattr(model, "to_dict")
            data = model.to_dict()
            assert isinstance(data, dict)
            assert "model_name" in data

    def test_all_return_decimal_fees(self, all_models: list[FeeModel]) -> None:
        """Test all models return Decimal fees."""
        for model in all_models:
            fee = model.calculate_fee(Decimal("1000"))
            assert isinstance(fee, Decimal)

    def test_all_accept_trade_amount(self, all_models: list[FeeModel]) -> None:
        """Test all models accept trade_amount as first argument."""
        for model in all_models:
            # Should not raise
            fee = model.calculate_fee(Decimal("1000"))
            assert fee >= Decimal("0")
