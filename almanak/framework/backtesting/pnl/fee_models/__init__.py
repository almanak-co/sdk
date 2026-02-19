"""Protocol-specific fee and slippage models for PnL backtesting.

This module provides fee and slippage model implementations tailored
to specific DeFi protocols like Uniswap V3, PancakeSwap V3, Aerodrome,
Curve, Aave V3, Morpho, Compound V3, GMX, and Hyperliquid.

Available Models:
    - FeeModel: Abstract base class for all fee models
    - UniswapV3FeeModel: Fee model using Uniswap V3 pool fee tiers
    - UniswapV3SlippageModel: Liquidity-aware slippage for Uniswap V3
    - PancakeSwapV3FeeModel: Fee model for PancakeSwap V3 with tier-based fees
    - AerodromeFeeModel: Fee model for Aerodrome with stable/volatile pools
    - CurveFeeModel: Fee model for Curve with dynamic fee calculation
    - AaveV3FeeModel: Fee model for Aave V3 lending protocol
    - MorphoFeeModel: Fee model for Morpho lending protocol
    - CompoundV3FeeModel: Fee model for Compound V3 (Comet) lending protocol
    - GMXFeeModel: Fee model for GMX V2 perpetuals protocol
    - HyperliquidFeeModel: Fee model for Hyperliquid perpetuals with maker/taker fees

Registry Functions:
    - get_fee_model: Look up and instantiate a fee model by protocol
    - get_fee_model_registry: Get dict mapping protocols to model classes
    - register_fee_model: Decorator to register a custom fee model
    - FeeModelRegistry: Registry class for fee model discovery

Example:
    from almanak.framework.backtesting.pnl.fee_models import (
        FeeModel,
        UniswapV3FeeModel,
        UniswapV3SlippageModel,
        PancakeSwapV3FeeModel,
        AerodromeFeeModel,
        CurveFeeModel,
        AaveV3FeeModel,
        MorphoFeeModel,
        CompoundV3FeeModel,
        GMXFeeModel,
        get_fee_model,
    )

    # Use registry lookup
    model = get_fee_model("uniswap_v3")
    fee = model.calculate_fee(Decimal("1000"))

    # Or instantiate directly
    fee_model = UniswapV3FeeModel()
    slippage_model = UniswapV3SlippageModel()
    pancakeswap_fee_model = PancakeSwapV3FeeModel()
    aerodrome_fee_model = AerodromeFeeModel()
    curve_fee_model = CurveFeeModel()
    aave_fee_model = AaveV3FeeModel()
    morpho_fee_model = MorphoFeeModel()
    compound_v3_fee_model = CompoundV3FeeModel()
    gmx_fee_model = GMXFeeModel()
    hyperliquid_fee_model = HyperliquidFeeModel()
"""

# Base interface and registry
# Protocol-specific implementations
from almanak.framework.backtesting.pnl.fee_models.aave_v3 import AaveV3FeeModel
from almanak.framework.backtesting.pnl.fee_models.aerodrome import (
    AerodromeFeeModel,
    AerodromePoolType,
)
from almanak.framework.backtesting.pnl.fee_models.amm_math import (
    MAX_TICK,
    MIN_TICK,
    Q96,
    TICK_SPACING_MAP,
    PriceImpactResult,
    V2PoolState,
    V3PoolState,
    calculate_v2_output_amount,
    calculate_v2_price_impact,
    calculate_v2_price_impact_usd,
    calculate_v3_delta_amounts,
    calculate_v3_price_impact,
    calculate_v3_price_impact_usd,
    calculate_v3_swap_output,
    estimate_concentration_factor,
    get_pool_type_from_protocol,
    sqrt_price_x96_to_price,
    sqrt_price_x96_to_tick,
    tick_to_sqrt_price_x96,
)
from almanak.framework.backtesting.pnl.fee_models.base import (
    FeeModel,
    FeeModelMetadata,
    FeeModelRegistry,
    FeeModelRegistryDict,
    get_fee_model,
    get_fee_model_registry,
    register_fee_model,
)
from almanak.framework.backtesting.pnl.fee_models.compound_v3 import (
    CompoundV3FeeModel,
    CompoundV3Market,
)
from almanak.framework.backtesting.pnl.fee_models.curve import (
    CurveFeeModel,
    CurvePoolType,
)
from almanak.framework.backtesting.pnl.fee_models.gmx import GMXFeeModel
from almanak.framework.backtesting.pnl.fee_models.hyperliquid import (
    HyperliquidFeeModel,
    HyperliquidFeeTier,
)
from almanak.framework.backtesting.pnl.fee_models.liquidity import (
    DEFAULT_LIQUIDITY_USD,
    KNOWN_POOLS,
    PoolLiquidityResult,
    estimate_liquidity_for_trade,
    get_pool_address,
    query_pool_liquidity,
    query_pool_liquidity_sync,
)
from almanak.framework.backtesting.pnl.fee_models.morpho import MorphoFeeModel
from almanak.framework.backtesting.pnl.fee_models.pancakeswap_v3 import (
    PancakeSwapV3FeeModel,
    PancakeSwapV3FeeTier,
)
from almanak.framework.backtesting.pnl.fee_models.slippage_guard import (
    DEFAULT_CRITICAL_IMPACT_THRESHOLD,
    DEFAULT_HIGH_IMPACT_THRESHOLD,
    DEFAULT_MAX_SLIPPAGE_PCT,
    DEFAULT_SAFE_LIQUIDITY_PCT,
    DEFAULT_V2_FEE_BPS,
    DEFAULT_V3_CONCENTRATION_FACTOR,
    DEFAULT_V3_FEE_BPS,
    SLIPPAGE_SOURCE_CONSTANT_PRODUCT,
    SLIPPAGE_SOURCE_HISTORICAL,
    SLIPPAGE_SOURCE_TWAP,
    HistoricalSlippageModel,
    HistoricalSlippageResult,
    SlippageCapExceededError,
    SlippageCheckResult,
    SlippageGuard,
    SlippageGuardConfig,
    SlippageModelConfig,
    SlippageWarning,
    cap_slippage,
    check_trade_slippage,
)
from almanak.framework.backtesting.pnl.fee_models.uniswap_v3 import (
    UniswapV3FeeModel,
    UniswapV3FeeTier,
    UniswapV3SlippageModel,
)

# Register built-in fee models
FeeModelRegistry.register(
    "uniswap_v3",
    UniswapV3FeeModel,
    description="Uniswap V3 DEX fee model with tier-based fees",
    aliases=["uniswap", "uni_v3"],
)
FeeModelRegistry.register(
    "pancakeswap_v3",
    PancakeSwapV3FeeModel,
    description="PancakeSwap V3 DEX fee model with tier-based fees (0.01%, 0.05%, 0.25%, 1%)",
    aliases=["pancakeswap", "pancake_v3", "pcs_v3"],
)
FeeModelRegistry.register(
    "aerodrome",
    AerodromeFeeModel,
    description="Aerodrome DEX fee model with stable/volatile pool distinction",
    aliases=["aero", "velodrome"],
)
FeeModelRegistry.register(
    "curve",
    CurveFeeModel,
    description="Curve Finance DEX fee model with dynamic fee calculation",
    aliases=["curve_fi", "crv"],
)
FeeModelRegistry.register(
    "aave_v3",
    AaveV3FeeModel,
    description="Aave V3 lending protocol fee model",
    aliases=["aave", "aave_v2"],
)
FeeModelRegistry.register(
    "morpho",
    MorphoFeeModel,
    description="Morpho lending protocol fee model (fee-free operations)",
    aliases=["morpho_blue", "morpho_optimizer"],
)
FeeModelRegistry.register(
    "compound_v3",
    CompoundV3FeeModel,
    description="Compound V3 (Comet) lending protocol fee model",
    aliases=["compound", "comet"],
)
FeeModelRegistry.register(
    "gmx",
    GMXFeeModel,
    description="GMX V2 perpetuals protocol fee model",
    aliases=["gmx_v2"],
)
FeeModelRegistry.register(
    "hyperliquid",
    HyperliquidFeeModel,
    description="Hyperliquid perpetuals protocol fee model with maker/taker fees and volume tiers",
    aliases=["hl", "hyper"],
)

__all__ = [
    # Base interface
    "FeeModel",
    "FeeModelMetadata",
    "FeeModelRegistry",
    "FeeModelRegistryDict",
    # Registry functions
    "get_fee_model",
    "get_fee_model_registry",
    "register_fee_model",
    # Protocol implementations
    "UniswapV3FeeModel",
    "UniswapV3SlippageModel",
    "UniswapV3FeeTier",
    "PancakeSwapV3FeeModel",
    "PancakeSwapV3FeeTier",
    "AerodromeFeeModel",
    "AerodromePoolType",
    "CurveFeeModel",
    "CurvePoolType",
    "AaveV3FeeModel",
    "MorphoFeeModel",
    "CompoundV3FeeModel",
    "CompoundV3Market",
    "GMXFeeModel",
    "HyperliquidFeeModel",
    "HyperliquidFeeTier",
    # Pool liquidity querying
    "query_pool_liquidity",
    "query_pool_liquidity_sync",
    "get_pool_address",
    "estimate_liquidity_for_trade",
    "PoolLiquidityResult",
    "DEFAULT_LIQUIDITY_USD",
    "KNOWN_POOLS",
    # Slippage guard
    "SlippageGuard",
    "SlippageGuardConfig",
    "SlippageWarning",
    "SlippageCheckResult",
    "SlippageCapExceededError",
    "check_trade_slippage",
    "cap_slippage",
    "DEFAULT_MAX_SLIPPAGE_PCT",
    "DEFAULT_SAFE_LIQUIDITY_PCT",
    "DEFAULT_HIGH_IMPACT_THRESHOLD",
    "DEFAULT_CRITICAL_IMPACT_THRESHOLD",
    # Historical slippage model with liquidity depth support
    "HistoricalSlippageModel",
    "HistoricalSlippageResult",
    "SlippageModelConfig",
    "DEFAULT_V3_CONCENTRATION_FACTOR",
    "DEFAULT_V2_FEE_BPS",
    "DEFAULT_V3_FEE_BPS",
    "SLIPPAGE_SOURCE_HISTORICAL",
    "SLIPPAGE_SOURCE_CONSTANT_PRODUCT",
    "SLIPPAGE_SOURCE_TWAP",
    # AMM math for slippage calculations
    "V2PoolState",
    "V3PoolState",
    "PriceImpactResult",
    "calculate_v2_output_amount",
    "calculate_v2_price_impact",
    "calculate_v2_price_impact_usd",
    "calculate_v3_delta_amounts",
    "calculate_v3_swap_output",
    "calculate_v3_price_impact",
    "calculate_v3_price_impact_usd",
    "tick_to_sqrt_price_x96",
    "sqrt_price_x96_to_tick",
    "sqrt_price_x96_to_price",
    "estimate_concentration_factor",
    "get_pool_type_from_protocol",
    "Q96",
    "MIN_TICK",
    "MAX_TICK",
    "TICK_SPACING_MAP",
]
