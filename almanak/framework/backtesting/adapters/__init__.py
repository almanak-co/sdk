"""Strategy-specific backtest adapters.

This module provides adapter classes that implement strategy-specific
backtesting logic for different position types (LP, perps, lending, etc.).

Adapters encapsulate the custom behavior needed to accurately simulate
different strategy types during backtesting, including:

- Intent execution (how trades are simulated)
- Position updates (how positions evolve over time)
- Position valuation (how positions are valued for PnL)
- Rebalance triggers (when positions should be adjusted)

Example:
    from almanak.framework.backtesting.adapters import (
        StrategyBacktestAdapter,
        get_adapter,
        register_adapter,
        detect_strategy_type,
        get_adapter_for_strategy,
    )

    # Get an adapter for a strategy type
    adapter = get_adapter("lp")
    if adapter:
        fill = adapter.execute_intent(intent, portfolio, market_state)

    # Auto-detect adapter from strategy metadata
    hint = detect_strategy_type(my_strategy)
    adapter = get_adapter_for_strategy(my_strategy)

    # Register a custom adapter
    @register_adapter("custom_strategy")
    class CustomAdapter(StrategyBacktestAdapter):
        ...
"""

from almanak.framework.backtesting.adapters.arbitrage_adapter import (
    ArbitrageBacktestAdapter,
    ArbitrageBacktestConfig,
    ArbitrageExecutionResult,
    CumulativeSlippageModel,
    ExecutionStep,
)
from almanak.framework.backtesting.adapters.base import (
    AdapterMetadata,
    AdapterRegistry,
    StrategyBacktestAdapter,
    StrategyBacktestConfig,
    get_adapter,
    register_adapter,
)
from almanak.framework.backtesting.adapters.lending_adapter import (
    LendingBacktestAdapter,
    LendingBacktestConfig,
)
from almanak.framework.backtesting.adapters.lp_adapter import (
    LPBacktestAdapter,
    LPBacktestConfig,
    RangeStatus,
    RangeStatusResult,
)
from almanak.framework.backtesting.adapters.multi_protocol_adapter import (
    AggregatedRiskResult,
    MultiProtocolBacktestAdapter,
    MultiProtocolBacktestConfig,
    ProtocolExposure,
    UnifiedLiquidationModel,
)
from almanak.framework.backtesting.adapters.perp_adapter import (
    PerpBacktestAdapter,
    PerpBacktestConfig,
)
from almanak.framework.backtesting.adapters.registry import (
    KNOWN_STRATEGY_TYPES,
    STRATEGY_TYPE_ARBITRAGE,
    STRATEGY_TYPE_LENDING,
    STRATEGY_TYPE_LP,
    STRATEGY_TYPE_MULTI_PROTOCOL,
    STRATEGY_TYPE_PERP,
    STRATEGY_TYPE_SWAP,
    STRATEGY_TYPE_YIELD,
    StrategyTypeHint,
    detect_strategy_type,
    get_adapter_for_strategy,
    get_adapter_info,
    list_available_adapters,
)

__all__ = [
    # Base classes
    "AdapterMetadata",
    "AdapterRegistry",
    "StrategyBacktestAdapter",
    "StrategyBacktestConfig",
    # Base functions
    "get_adapter",
    "register_adapter",
    # Arbitrage adapter
    "ArbitrageBacktestAdapter",
    "ArbitrageBacktestConfig",
    "ArbitrageExecutionResult",
    "CumulativeSlippageModel",
    "ExecutionStep",
    # LP adapter
    "LPBacktestAdapter",
    "LPBacktestConfig",
    "RangeStatus",
    "RangeStatusResult",
    # Perp adapter
    "PerpBacktestAdapter",
    "PerpBacktestConfig",
    # Lending adapter
    "LendingBacktestAdapter",
    "LendingBacktestConfig",
    # Multi-protocol adapter
    "AggregatedRiskResult",
    "MultiProtocolBacktestAdapter",
    "MultiProtocolBacktestConfig",
    "ProtocolExposure",
    "UnifiedLiquidationModel",
    # Registry functions
    "detect_strategy_type",
    "get_adapter_for_strategy",
    "get_adapter_info",
    "list_available_adapters",
    # Type hint result
    "StrategyTypeHint",
    # Constants
    "KNOWN_STRATEGY_TYPES",
    "STRATEGY_TYPE_ARBITRAGE",
    "STRATEGY_TYPE_LENDING",
    "STRATEGY_TYPE_LP",
    "STRATEGY_TYPE_MULTI_PROTOCOL",
    "STRATEGY_TYPE_PERP",
    "STRATEGY_TYPE_SWAP",
    "STRATEGY_TYPE_YIELD",
]
