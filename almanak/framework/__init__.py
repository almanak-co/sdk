"""Almanak Strategy Framework v2.0.

Public names are resolved lazily via :pep:`562` ``__getattr__`` so that
consumers (notably the gateway sidecar) only pay the import cost of the
subpackages they actually touch. The eager re-exports inside the
``TYPE_CHECKING`` block keep mypy / pyright / IDE autocomplete fully accurate;
at runtime each name resolves on first attribute access and is cached on the
module's ``globals()``.
"""

from typing import TYPE_CHECKING

from almanak._lazy import LazySpec, build_lazy_module_dispatch

if TYPE_CHECKING:
    from .alerting import (
        AlertManager,
        AlertSendResult,
        CooldownTracker,
        EscalationLevel,
        EscalationPolicy,
        EscalationResult,
        EscalationState,
        EscalationStatus,
    )
    from .api import (
        TimelineEvent,
        TimelineEventType,
        TimelineResponse,
        timeline_router,
    )
    from .backtesting import (
        BacktestMetrics,
        BacktestResult,
        TradeRecord,
    )
    from .cli import new_strategy
    from .data.exceptions import DataUnavailableError, LowConfidenceError
    from .data.models import (
        DataClassification,
        DataEnvelope,
        DataMeta,
        Instrument,
        resolve_instrument,
    )
    from .data.pools import (
        AggregatedPrice,
        LiquidityDepth,
        PoolAnalytics,
        PoolPrice,
        PoolSnapshot,
        SlippageEstimate,
    )
    from .data.risk import PortfolioRisk, RiskConventions, VaRMethod
    from .data.routing import CircuitBreaker, DataProvider, DataRouter
    from .data.volatility import VolatilityResult
    from .data.yields import YieldOpportunity
    from .deployment import (
        CanaryComparison,
        CanaryConfig,
        CanaryDecision,
        CanaryDeployment,
        CanaryEventType,
        CanaryMetrics,
        CanaryResult,
        CanaryState,
        CanaryStatus,
        DeployCanaryResult,
        PromotionCriteria,
    )
    from .intents import (
        DEFAULT_GAS_ESTIMATES,
        PROTOCOL_ROUTERS,
        BorrowIntent,
        CollectFeesIntent,
        CompilationResult,
        CompilationStatus,
        DefaultSwapAdapter,
        HoldIntent,
        Intent,
        IntentCompiler,
        IntentCompilerConfig,
        IntentType,
        LPCloseIntent,
        LPOpenIntent,
        PriceInfo,
        RepayIntent,
        SwapIntent,
        TokenInfo,
        TransactionData,
        VaultDepositIntent,
        VaultRedeemIntent,
    )
    from .services import (
        AllowanceInfo,
        BalanceInfo,
        BorrowPosition,
        EmergencyManager,
        EmergencyResult,
        FullPositionSummary,
        GetPositionCallback,
        LPPositionInfo,
        OperatorCardGenerator,
        PauseStrategyCallback,
        PendingTransaction,
        StrategySnapshot,
        StuckDetectionResult,
        StuckDetector,
        TokenPosition,
        create_emergency_manager,
    )
    from .state import (
        MigrationError,
        MigrationNotFoundError,
        MigrationRegistry,
        MigrationResult,
        PostgresConfig,
        RollbackInfo,
        RollbackNotSafeError,
        StateConflictError,
        StateData,
        StateManager,
        StateManagerConfig,
        StateMigration,
        StateNotFoundError,
        StateTier,
        TierMetrics,
        auto_migrate,
        check_rollback_safety,
        get_registry,
        get_rollback_safe_version,
        migrate,
        migrate_state_data,
        migration,
        needs_migration,
        register_migration,
    )
    from .strategies import (
        STRATEGY_REGISTRY,
        BalanceProvider,
        ConfigSnapshot,
        ConfigValidationError,
        ExecutionResult,
        IntentStrategy,
        MarketSnapshot,
        MultiStepStrategy,
        NotificationCallback,
        PriceData,
        PriceOracle,
        RiskGuard,
        RiskGuardConfig,
        RiskGuardGuidance,
        RiskGuardResult,
        RSIData,
        RSIProvider,
        StatelessStrategy,
        StrategyBase,
        StrategyMetadata,
        TokenBalance,
        almanak_strategy,
        get_strategy,
        list_strategies,
        register_strategy,
        unregister_strategy,
    )
    from .testing import (
        ABTest,
        ABTestConfig,
        ABTestEventType,
        ABTestManager,
        ABTestResult,
        ABTestStatus,
        CreateTestResult,
        EndTestResult,
        StatisticalResult,
        VariantComparison,
        VariantMetrics,
    )
    from .utils import (
        LogFormat,
        LogLevel,
        add_context,
        clear_context,
        configure_logging,
        get_logger,
    )


# --- Lazy resolution dispatch ---------------------------------------------------

# Maps each public name to the relative module path that supplies it. The map
# is the single source of truth; ``__all__`` is derived from its keys.
_LAZY_IMPORTS: dict[str, LazySpec] = {
    # alerting
    "AlertManager": ".alerting",
    "AlertSendResult": ".alerting",
    "CooldownTracker": ".alerting",
    "EscalationLevel": ".alerting",
    "EscalationPolicy": ".alerting",
    "EscalationResult": ".alerting",
    "EscalationState": ".alerting",
    "EscalationStatus": ".alerting",
    # api
    "TimelineEvent": ".api",
    "TimelineEventType": ".api",
    "TimelineResponse": ".api",
    "timeline_router": ".api",
    # backtesting
    "BacktestMetrics": ".backtesting",
    "BacktestResult": ".backtesting",
    "TradeRecord": ".backtesting",
    # cli
    "new_strategy": ".cli",
    # data.exceptions
    "DataUnavailableError": ".data.exceptions",
    "LowConfidenceError": ".data.exceptions",
    # data.models
    "DataClassification": ".data.models",
    "DataEnvelope": ".data.models",
    "DataMeta": ".data.models",
    "Instrument": ".data.models",
    "resolve_instrument": ".data.models",
    # data.pools
    "AggregatedPrice": ".data.pools",
    "LiquidityDepth": ".data.pools",
    "PoolAnalytics": ".data.pools",
    "PoolPrice": ".data.pools",
    "PoolSnapshot": ".data.pools",
    "SlippageEstimate": ".data.pools",
    # data.risk
    "PortfolioRisk": ".data.risk",
    "RiskConventions": ".data.risk",
    "VaRMethod": ".data.risk",
    # data.routing
    "CircuitBreaker": ".data.routing",
    "DataProvider": ".data.routing",
    "DataRouter": ".data.routing",
    # data.volatility
    "VolatilityResult": ".data.volatility",
    # data.yields
    "YieldOpportunity": ".data.yields",
    # deployment
    "CanaryComparison": ".deployment",
    "CanaryConfig": ".deployment",
    "CanaryDecision": ".deployment",
    "CanaryDeployment": ".deployment",
    "CanaryEventType": ".deployment",
    "CanaryMetrics": ".deployment",
    "CanaryResult": ".deployment",
    "CanaryState": ".deployment",
    "CanaryStatus": ".deployment",
    "DeployCanaryResult": ".deployment",
    "PromotionCriteria": ".deployment",
    # intents
    "DEFAULT_GAS_ESTIMATES": ".intents",
    "PROTOCOL_ROUTERS": ".intents",
    "BorrowIntent": ".intents",
    "CollectFeesIntent": ".intents",
    "CompilationResult": ".intents",
    "CompilationStatus": ".intents",
    "DefaultSwapAdapter": ".intents",
    "HoldIntent": ".intents",
    "Intent": ".intents",
    "IntentCompiler": ".intents",
    "IntentCompilerConfig": ".intents",
    "IntentType": ".intents",
    "LPCloseIntent": ".intents",
    "LPOpenIntent": ".intents",
    "PriceInfo": ".intents",
    "RepayIntent": ".intents",
    "SwapIntent": ".intents",
    "TokenInfo": ".intents",
    "TransactionData": ".intents",
    "VaultDepositIntent": ".intents",
    "VaultRedeemIntent": ".intents",
    # services
    "AllowanceInfo": ".services",
    "BalanceInfo": ".services",
    "BorrowPosition": ".services",
    "EmergencyManager": ".services",
    "EmergencyResult": ".services",
    "FullPositionSummary": ".services",
    "GetPositionCallback": ".services",
    "LPPositionInfo": ".services",
    "OperatorCardGenerator": ".services",
    "PauseStrategyCallback": ".services",
    "PendingTransaction": ".services",
    "StrategySnapshot": ".services",
    "StuckDetectionResult": ".services",
    "StuckDetector": ".services",
    "TokenPosition": ".services",
    "create_emergency_manager": ".services",
    # state
    "MigrationError": ".state",
    "MigrationNotFoundError": ".state",
    "MigrationRegistry": ".state",
    "MigrationResult": ".state",
    "PostgresConfig": ".state",
    "RollbackInfo": ".state",
    "RollbackNotSafeError": ".state",
    "StateConflictError": ".state",
    "StateData": ".state",
    "StateManager": ".state",
    "StateManagerConfig": ".state",
    "StateMigration": ".state",
    "StateNotFoundError": ".state",
    "StateTier": ".state",
    "TierMetrics": ".state",
    "auto_migrate": ".state",
    "check_rollback_safety": ".state",
    "get_registry": ".state",
    "get_rollback_safe_version": ".state",
    "migrate": ".state",
    "migrate_state_data": ".state",
    "migration": ".state",
    "needs_migration": ".state",
    "register_migration": ".state",
    # strategies
    "STRATEGY_REGISTRY": ".strategies",
    "BalanceProvider": ".strategies",
    "ConfigSnapshot": ".strategies",
    "ConfigValidationError": ".strategies",
    "ExecutionResult": ".strategies",
    "IntentStrategy": ".strategies",
    "MarketSnapshot": ".strategies",
    "MultiStepStrategy": ".strategies",
    "NotificationCallback": ".strategies",
    "PriceData": ".strategies",
    "PriceOracle": ".strategies",
    "RiskGuard": ".strategies",
    "RiskGuardConfig": ".strategies",
    "RiskGuardGuidance": ".strategies",
    "RiskGuardResult": ".strategies",
    "RSIData": ".strategies",
    "RSIProvider": ".strategies",
    "StatelessStrategy": ".strategies",
    "StrategyBase": ".strategies",
    "StrategyMetadata": ".strategies",
    "TokenBalance": ".strategies",
    "almanak_strategy": ".strategies",
    "get_strategy": ".strategies",
    "list_strategies": ".strategies",
    "register_strategy": ".strategies",
    "unregister_strategy": ".strategies",
    # testing
    "ABTest": ".testing",
    "ABTestConfig": ".testing",
    "ABTestEventType": ".testing",
    "ABTestManager": ".testing",
    "ABTestResult": ".testing",
    "ABTestStatus": ".testing",
    "CreateTestResult": ".testing",
    "EndTestResult": ".testing",
    "StatisticalResult": ".testing",
    "VariantComparison": ".testing",
    "VariantMetrics": ".testing",
    # utils
    "LogFormat": ".utils",
    "LogLevel": ".utils",
    "add_context": ".utils",
    "clear_context": ".utils",
    "configure_logging": ".utils",
    "get_logger": ".utils",
}

__all__ = [*sorted(_LAZY_IMPORTS)]

__getattr__, __dir__ = build_lazy_module_dispatch(_LAZY_IMPORTS, package=__name__, namespace=globals())
