"""Protocol-specific fee and slippage models for PnL backtesting.

The protocol fee-model implementations live in their owning connectors
(``almanak/connectors/<protocol>/fee_model.py``) and are declared on each
connector manifest via ``fee_model=FeeModelDecl(...)`` — adding a connector
adds its fee model with no edit here (VIB-4851 Phase D; previously this
package held the per-protocol modules and a hardcoded registration block).

This package keeps the protocol-agnostic surfaces eager (the ``FeeModel``
base + registry, AMM math, pool-liquidity helpers, the slippage guard) and
re-exports the connector-owned model classes lazily so importing the package
never imports a connector module.

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
        get_fee_model,
    )

    # Use registry lookup
    model = get_fee_model("uniswap_v3")
    fee = model.calculate_fee(Decimal("1000"))

    # Or instantiate directly
    fee_model = UniswapV3FeeModel()
"""

import importlib

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
from almanak.framework.backtesting.pnl.fee_models.liquidity import (
    DEFAULT_LIQUIDITY_USD,
    KNOWN_POOLS,
    PoolLiquidityResult,
    estimate_liquidity_for_trade,
    get_pool_address,
    query_pool_liquidity,
    query_pool_liquidity_sync,
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

# Connector-owned model classes (and their companion enums), re-exported
# lazily by name. Keys are the public names this package historically
# exported; values are the owning connector fee_model modules.
_CONNECTOR_EXPORTS: dict[str, str] = {
    "UniswapV3FeeModel": "almanak.connectors.uniswap_v3.fee_model",
    "UniswapV3SlippageModel": "almanak.connectors.uniswap_v3.fee_model",
    "UniswapV3FeeTier": "almanak.connectors.uniswap_v3.fee_model",
    "PancakeSwapV3FeeModel": "almanak.connectors.pancakeswap_v3.fee_model",
    "PancakeSwapV3FeeTier": "almanak.connectors.pancakeswap_v3.fee_model",
    "AerodromeFeeModel": "almanak.connectors.aerodrome.fee_model",
    "AerodromePoolType": "almanak.connectors.aerodrome.fee_model",
    "CurveFeeModel": "almanak.connectors.curve.fee_model",
    "CurvePoolType": "almanak.connectors.curve.fee_model",
    "AaveV3FeeModel": "almanak.connectors.aave_v3.fee_model",
    "MorphoFeeModel": "almanak.connectors.morpho_blue.fee_model",
    "CompoundV3FeeModel": "almanak.connectors.compound_v3.fee_model",
    "CompoundV3Market": "almanak.connectors.compound_v3.fee_model",
    "GMXFeeModel": "almanak.connectors.gmx_v2.fee_model",
    "HyperliquidFeeModel": "almanak.connectors.hyperliquid.fee_model",
    "HyperliquidFeeTier": "almanak.connectors.hyperliquid.fee_model",
}


def __getattr__(name: str):  # noqa: ANN202 - PEP 562 lazy re-export hook
    """Resolve connector-owned model classes on first attribute access."""
    module_path = _CONNECTOR_EXPORTS.get(name)
    if module_path is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(importlib.import_module(module_path), name)
    globals()[name] = value  # cache for subsequent lookups
    return value


def __dir__() -> list[str]:
    """Include the lazy connector exports in ``dir()`` output."""
    return sorted(set(globals()) | set(_CONNECTOR_EXPORTS))


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
    # Protocol implementations (lazy; connector-owned)
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
