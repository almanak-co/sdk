"""Almanak Strategy Framework v2.0"""

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

# Quant Data Layer - key public types
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
    # Compiler
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
    # Migrations
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
    ExecutionResult,
    # Intent Strategy
    IntentStrategy,
    MarketSnapshot,
    # Multi-Step Strategy
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
