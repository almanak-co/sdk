"""Public strategy authoring contracts. External I/O remains gateway-owned."""

from almanak.author_capabilities import DeclaredCapabilities, declared
from almanak.backtest.dependencies import DataDependencies
from almanak.backtest.pricing import (
    MAX_WINDOW,
    CoinGeckoPrice,
    PoolPrice,
    PoolSwapCandles,
    PriceBinding,
)
from almanak.caip import AssetId, AssetType, ChainId
from almanak.models import AssetAmount
from almanak.price_series import PricePoint, PriceSeries
from almanak.strategy_api import (
    DecisionContext,
    InvalidConfig,
    Memory,
    OutcomeContext,
    StrategyDecision,
    StrategyState,
)
from almanak.typed_parameters import (
    PositiveDecimal,
    TypedParameters,
    UnitFraction,
    parameter_base_units,
)

__all__ = [
    "declared",
    "DeclaredCapabilities",
    "AssetAmount",
    "AssetId",
    "AssetType",
    "ChainId",
    "CoinGeckoPrice",
    "DataDependencies",
    "MAX_WINDOW",
    "PoolPrice",
    "PoolSwapCandles",
    "PriceBinding",
    "PricePoint",
    "PriceSeries",
    "DecisionContext",
    "InvalidConfig",
    "Memory",
    "OutcomeContext",
    "StrategyDecision",
    "StrategyState",
    "PositiveDecimal",
    "TypedParameters",
    "UnitFraction",
    "parameter_base_units",
]
