"""Intent vocabulary for strategy authoring.

This module provides a high-level intent vocabulary that allows strategy
developers to express trading actions at a semantic level without worrying
about low-level transaction details.

Example:
    from almanak.framework.intents import Intent, IntentCompiler

    # In your strategy's decide() method:
    def decide(self, market: MarketSnapshot) -> Optional[Intent]:
        if market.rsi < 30:
            return Intent.swap(
                from_token="USDC",
                to_token="ETH",
                amount_usd=Decimal("1000"),
            )
        return Intent.hold()

    # Compile intent to ActionBundle:
    compiler = IntentCompiler(chain="arbitrum")
    result = compiler.compile(intent)
"""

# Re-export PredictionExitConditions for convenient access
from almanak.framework.services.prediction_monitor import PredictionExitConditions

from .bridge import (
    BridgeAmount,
    BridgeChainError,
    BridgeIntent,
    BridgeIntentType,
    BridgeTokenError,
    InvalidBridgeError,
)
from .compiler import (
    AAVE_STABLE_RATE_MODE,
    AAVE_VARIABLE_RATE_MODE,
    DEFAULT_GAS_ESTIMATES,
    LENDING_POOL_ADDRESSES,
    LP_POSITION_MANAGERS,
    PROTOCOL_ROUTERS,
    AaveV3Adapter,
    CompilationResult,
    CompilationStatus,
    DefaultSwapAdapter,
    IntentCompiler,
    IntentCompilerConfig,
    PriceInfo,
    TokenInfo,
    TransactionData,
    UniswapV3LPAdapter,
)
from .ensure_balance import (
    EnsureBalanceIntent,
    EnsureBalanceIntentType,
    InsufficientBalanceError,
    InvalidEnsureBalanceError,
)
from .state_machine import (
    IntentState,
    IntentStateMachine,
    MetricsCallback,
    RetryConfig,
    SadflowAction,
    # Sadflow hooks
    SadflowActionType,
    SadflowContext,
    SadflowEnterCallback,
    SadflowExitCallback,
    SadflowRetryCallback,
    StateMachineConfig,
    StateTransitionMetric,
    StepResult,
    clear_metrics,
    create_state_machine,
    generate_state_diagram,
    get_metrics,
    get_preparing_state,
    get_sadflow_state,
    get_validating_state,
    is_preparing_state,
    is_sadflow_state,
    is_validating_state,
)
from .vocabulary import (
    PROTOCOL_CAPABILITIES,
    BorrowIntent,
    ChainedAmount,
    DecideResult,
    HoldIntent,
    Intent,
    IntentSequence,
    IntentType,
    InterestRateMode,
    InvalidAmountError,
    InvalidChainError,
    InvalidProtocolParameterError,
    InvalidSequenceError,
    LPCloseIntent,
    LPOpenIntent,
    PerpCloseIntent,
    PerpOpenIntent,
    # Prediction market intents
    PredictionBuyIntent,
    PredictionOrderType,
    PredictionOutcome,
    PredictionRedeemIntent,
    PredictionSellIntent,
    PredictionShareAmount,
    PredictionTimeInForce,
    ProtocolRequiredError,
    RepayIntent,
    StakeIntent,
    SupplyIntent,
    SwapIntent,
    UnstakeIntent,
    VaultDepositIntent,
    VaultRedeemIntent,
    WithdrawIntent,
)

__all__ = [
    # Vocabulary
    "Intent",
    "IntentType",
    "InvalidChainError",
    "InvalidSequenceError",
    "InvalidAmountError",
    "InvalidProtocolParameterError",
    "ProtocolRequiredError",
    "ChainedAmount",
    "InterestRateMode",
    "PROTOCOL_CAPABILITIES",
    "SwapIntent",
    "LPOpenIntent",
    "LPCloseIntent",
    "BorrowIntent",
    "RepayIntent",
    "SupplyIntent",
    "WithdrawIntent",
    "PerpOpenIntent",
    "PerpCloseIntent",
    "StakeIntent",
    "UnstakeIntent",
    "HoldIntent",
    "IntentSequence",
    "DecideResult",
    # Vault Intents (MetaMorpho ERC-4626)
    "VaultDepositIntent",
    "VaultRedeemIntent",
    # Prediction Market Intents
    "PredictionBuyIntent",
    "PredictionSellIntent",
    "PredictionRedeemIntent",
    "PredictionOutcome",
    "PredictionOrderType",
    "PredictionTimeInForce",
    "PredictionShareAmount",
    "PredictionExitConditions",
    # Bridge Intent
    "BridgeIntent",
    "BridgeIntentType",
    "BridgeAmount",
    "InvalidBridgeError",
    "BridgeChainError",
    "BridgeTokenError",
    # Ensure Balance Intent
    "EnsureBalanceIntent",
    "EnsureBalanceIntentType",
    "InsufficientBalanceError",
    "InvalidEnsureBalanceError",
    # Compiler
    "IntentCompiler",
    "IntentCompilerConfig",
    "CompilationResult",
    "CompilationStatus",
    "TransactionData",
    "TokenInfo",
    "PriceInfo",
    "DefaultSwapAdapter",
    "UniswapV3LPAdapter",
    "AaveV3Adapter",
    "DEFAULT_GAS_ESTIMATES",
    "PROTOCOL_ROUTERS",
    "LP_POSITION_MANAGERS",
    "LENDING_POOL_ADDRESSES",
    "AAVE_VARIABLE_RATE_MODE",
    "AAVE_STABLE_RATE_MODE",
    # State Machine
    "IntentState",
    "IntentStateMachine",
    "RetryConfig",
    "StateMachineConfig",
    "StepResult",
    "StateTransitionMetric",
    "MetricsCallback",
    "create_state_machine",
    "generate_state_diagram",
    "get_preparing_state",
    "get_validating_state",
    "get_sadflow_state",
    "is_preparing_state",
    "is_validating_state",
    "is_sadflow_state",
    "get_metrics",
    "clear_metrics",
    # Sadflow hooks
    "SadflowActionType",
    "SadflowAction",
    "SadflowContext",
    "SadflowEnterCallback",
    "SadflowExitCallback",
    "SadflowRetryCallback",
]
