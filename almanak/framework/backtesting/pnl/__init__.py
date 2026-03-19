"""PnL Backtesting Engine - Historical simulation for strategy evaluation.

This package provides the PnL (Profit and Loss) backtesting engine, which
simulates strategy execution against historical price data to calculate
performance metrics without executing real transactions.

Key Components:
    - HistoricalDataProvider: Protocol for historical price data sources
    - MarketState: Point-in-time market data snapshot
    - SimulatedPortfolio: Portfolio state tracking during backtest
    - PnLBacktester: Main backtesting engine

Example:
    from almanak.framework.backtesting.pnl import PnLBacktester, PnLBacktestConfig

    config = PnLBacktestConfig(
        start_time=datetime(2024, 1, 1),
        end_time=datetime(2024, 6, 1),
        initial_capital_usd=Decimal("10000"),
    )

    backtester = PnLBacktester(data_provider, fee_models, slippage_models)
    result = await backtester.backtest(strategy, config)
    print(result.summary())
"""

# Data provider interface and models
# Configuration
# Monte Carlo price path generation
from .calculators.monte_carlo import (
    MonteCarloPathGenerator,
    PathGenerationMethod,
    PricePathConfig,
    PricePathResult,
    generate_price_paths,
)

# Monte Carlo simulation runner
from .calculators.monte_carlo_runner import (
    MonteCarloConfig,
    MonteCarloPathBacktestResult,
    MonteCarloSimulationResult,
    SimulatedPricePathProvider,
    run_monte_carlo,
    run_monte_carlo_sync,
)
from .config import PnLBacktestConfig
from .config_loader import (
    ConfigLoadError,
    ConfigLoadResult,
    ValidationResult,
    load_config_from_result,
    validate_loaded_config,
)
from .data_provider import (
    OHLCV,
    HistoricalDataCapability,
    HistoricalDataConfig,
    HistoricalDataProvider,
    MarketState,
)

# Engine
from .engine import (
    BacktestableStrategy,
    DefaultFeeModel,
    DefaultSlippageModel,
    FeeModel,
    LinearImpactSlippageModel,
    PnLBacktester,
    SlippageModel,
    create_market_snapshot_from_state,
)

# Error handling and circuit breaker
from .error_handling import (
    ERROR_CATEGORY_MAP,
    BacktestCircuitBreaker,
    BacktestCircuitBreakerState,
    BacktestErrorConfig,
    BacktestErrorHandler,
    ErrorCategory,
    ErrorClassification,
    ErrorRecord,
    ErrorType,
    HandleErrorResult,
    classify_error,
    is_fatal_error,
    is_non_critical_error,
    is_recoverable_error,
)

# Indicator engine for backtest-live parity
from .indicator_engine import BacktestIndicatorEngine

# Logging utilities
from .logging_utils import (
    BacktestLogger,
    JSONLogFormatter,
    PhaseTimer,
    PhaseTiming,
    configure_backtest_logging,
    configure_json_logging,
    log_trade_execution,
)

# MEV simulation
from .mev_simulator import (
    MEVSimulationResult,
    MEVSimulator,
    MEVSimulatorConfig,
    get_token_vulnerability,
    simulate_mev_cost,
)

# Bayesian optimization with Optuna
from .optuna_tuner import (
    METRIC_DIRECTIONS,
    OBJECTIVE_METRICS,
    OptimizationResult,
    OptunaParamRanges,
    OptunaTuner,
    OptunaTunerConfig,
    # Parameter types and factory functions
    ParamRange,
    ParamType,
    TypedParamRanges,
    categorical,
    continuous,
    discrete,
    log_uniform,
)

# Parallel execution and parameter search
from .parallel import (
    RANKING_METRICS,
    AggregatedSweepResults,
    BacktestTask,
    ParallelBacktestResult,
    ParamRanges,
    aggregate_results,
    generate_grid_configs,
    generate_random_configs,
    rank_results,
    run_parallel_backtests,
    run_parallel_backtests_sync,
    run_parallel_backtests_with_progress,
)

# Portfolio models
from .portfolio import (
    PositionType,
    SimulatedFill,
    SimulatedPortfolio,
    SimulatedPosition,
)

# Portfolio aggregator for multi-protocol tracking
from .portfolio_aggregator import (
    CascadeRiskResult,
    CascadeRiskWarning,
    PortfolioAggregator,
    PortfolioSnapshot,
    UnifiedRiskScore,
)

# Concrete providers and registry
from .providers import (
    AggregatedDataProvider,
    CachedPrice,
    ChainlinkDataProvider,
    ChainlinkStaleDataError,
    CoinGeckoDataProvider,
    FallbackStats,
    PriceCache,
    PriceWithSource,
    ProviderMetadata,
    ProviderRegistry,
)

# Receipt parsing utilities
from .receipt_utils import (
    DEFAULT_DISCREPANCY_THRESHOLD,
    DiscrepancyResult,
    TokenFlow,
    TokenFlows,
    TransferEvent,
    calculate_discrepancy,
    extract_token_flows,
    parse_transfer_events,
)

# Walk-forward optimization
from .walk_forward import (
    ParameterStability,
    WalkForwardConfig,
    WalkForwardResult,
    WalkForwardWindow,
    WalkForwardWindowResult,
    calculate_parameter_stability,
    run_walk_forward_optimization,
    run_walk_forward_optimization_sync,
    split_walk_forward,
    split_walk_forward_tuples,
)

__all__ = [
    # Data provider interface and models
    "HistoricalDataCapability",
    "OHLCV",
    "MarketState",
    "HistoricalDataConfig",
    "HistoricalDataProvider",
    # Concrete providers
    "CoinGeckoDataProvider",
    "ChainlinkDataProvider",
    "ChainlinkStaleDataError",
    "AggregatedDataProvider",
    # Caching
    "CachedPrice",
    "PriceCache",
    # Fallback support
    "PriceWithSource",
    "FallbackStats",
    # Provider registry
    "ProviderRegistry",
    "ProviderMetadata",
    # Portfolio models
    "PositionType",
    "SimulatedPosition",
    "SimulatedFill",
    "SimulatedPortfolio",
    # Portfolio aggregator
    "PortfolioAggregator",
    "PortfolioSnapshot",
    "UnifiedRiskScore",
    "CascadeRiskResult",
    "CascadeRiskWarning",
    # Configuration
    "PnLBacktestConfig",
    # Config loading
    "ConfigLoadError",
    "ConfigLoadResult",
    "ValidationResult",
    "load_config_from_result",
    "validate_loaded_config",
    # Engine
    "PnLBacktester",
    "BacktestableStrategy",
    "BacktestIndicatorEngine",
    "create_market_snapshot_from_state",
    # Fee and slippage models
    "FeeModel",
    "SlippageModel",
    "DefaultFeeModel",
    "DefaultSlippageModel",
    "LinearImpactSlippageModel",
    # Receipt parsing utilities
    "TransferEvent",
    "TokenFlow",
    "TokenFlows",
    "parse_transfer_events",
    "extract_token_flows",
    "DEFAULT_DISCREPANCY_THRESHOLD",
    "DiscrepancyResult",
    "calculate_discrepancy",
    # MEV simulation
    "MEVSimulator",
    "MEVSimulatorConfig",
    "MEVSimulationResult",
    "simulate_mev_cost",
    "get_token_vulnerability",
    # Parallel execution and parameter search
    "BacktestTask",
    "ParallelBacktestResult",
    "AggregatedSweepResults",
    "ParamRanges",
    "generate_grid_configs",
    "generate_random_configs",
    "run_parallel_backtests",
    "run_parallel_backtests_sync",
    "run_parallel_backtests_with_progress",
    "aggregate_results",
    "rank_results",
    "RANKING_METRICS",
    # Bayesian optimization with Optuna
    "OptunaTuner",
    "OptunaTunerConfig",
    "OptimizationResult",
    "OptunaParamRanges",
    "OBJECTIVE_METRICS",
    "METRIC_DIRECTIONS",
    # Parameter types and factory functions
    "ParamType",
    "ParamRange",
    "TypedParamRanges",
    "continuous",
    "discrete",
    "categorical",
    "log_uniform",
    # Walk-forward optimization
    "WalkForwardWindow",
    "WalkForwardConfig",
    "split_walk_forward",
    "split_walk_forward_tuples",
    # Walk-forward optimization results
    "WalkForwardWindowResult",
    "WalkForwardResult",
    # Parameter stability analysis
    "ParameterStability",
    "calculate_parameter_stability",
    # Walk-forward optimization loop
    "run_walk_forward_optimization",
    "run_walk_forward_optimization_sync",
    # Monte Carlo price path generation
    "MonteCarloPathGenerator",
    "PathGenerationMethod",
    "PricePathConfig",
    "PricePathResult",
    "generate_price_paths",
    # Monte Carlo simulation runner
    "MonteCarloConfig",
    "MonteCarloPathBacktestResult",
    "MonteCarloSimulationResult",
    "SimulatedPricePathProvider",
    "run_monte_carlo",
    "run_monte_carlo_sync",
    # Error handling and circuit breaker
    "ErrorCategory",
    "ErrorType",
    "ErrorClassification",
    "ErrorRecord",
    "HandleErrorResult",
    "BacktestCircuitBreakerState",
    "BacktestCircuitBreaker",
    "BacktestErrorConfig",
    "BacktestErrorHandler",
    "ERROR_CATEGORY_MAP",
    "classify_error",
    "is_recoverable_error",
    "is_fatal_error",
    "is_non_critical_error",
    # Logging utilities
    "JSONLogFormatter",
    "PhaseTimer",
    "PhaseTiming",
    "BacktestLogger",
    "configure_backtest_logging",
    "configure_json_logging",
    "log_trade_execution",
]
