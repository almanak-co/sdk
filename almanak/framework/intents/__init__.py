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

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Re-export PredictionExitConditions for convenient access. Resolved lazily
    # at runtime via __getattr__ below to avoid a circular import:
    # ``services.prediction_monitor`` re-enters ``intents`` (via
    # auto_redemption -> api.actions -> strategies -> ..intents) before this
    # module finishes loading.
    from almanak.framework.services.prediction_monitor import PredictionExitConditions

# vocabulary is imported FIRST (before any submodule that may, via the
# connectors / execution / strategies graph, try to ``from ..intents import
# DecideResult``) so the intents package namespace has DecideResult and
# IntentSequence bound before any deeper cycle has a chance to fire.
from .bridge import (
    BridgeAmount,
    BridgeChainError,
    BridgeIntent,
    BridgeIntentType,
    BridgeTokenError,
    InvalidBridgeError,
)
from .compiler import (
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
from .compiler_lending import (
    AssetNotCollateralEligibleError,
    PoolReserveFrozenError,
    assert_lending_reserve_active,
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
from .tick_utils import (
    get_max_tick,
    get_min_tick,
    get_tick_spacing,
    price_to_tick,
    snap_to_tick_spacing,
    tick_to_price,
)
from .vocabulary import (
    PROTOCOL_CAPABILITIES,
    BorrowIntent,
    ChainedAmount,
    CollectFeesIntent,
    DecideResult,
    DeleverageIntent,
    FlashLoanIntent,
    HoldIntent,
    Intent,
    IntentSequence,
    IntentType,
    InterestRateMode,
    InvalidAmountError,
    InvalidChainError,
    InvalidCollateralForMarketError,
    InvalidProtocolParameterError,
    InvalidSequenceError,
    LPCloseIntent,
    LPOpenIntent,
    LpOpenZeroLiquidityError,
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
    UnwrapNativeIntent,
    VaultDepositIntent,
    VaultRedeemIntent,
    WithdrawIntent,
    WrapNativeIntent,
)

__all__ = [
    # Vocabulary
    "Intent",
    "IntentType",
    "InvalidChainError",
    "InvalidSequenceError",
    "InvalidAmountError",
    "InvalidCollateralForMarketError",
    "LpOpenZeroLiquidityError",
    "InvalidProtocolParameterError",
    "ProtocolRequiredError",
    "ChainedAmount",
    "InterestRateMode",
    "PROTOCOL_CAPABILITIES",
    "SwapIntent",
    "LPOpenIntent",
    "LPCloseIntent",
    "CollectFeesIntent",
    "BorrowIntent",
    "RepayIntent",
    "DeleverageIntent",
    "SupplyIntent",
    "WithdrawIntent",
    "PerpOpenIntent",
    "PerpCloseIntent",
    "StakeIntent",
    "UnstakeIntent",
    "FlashLoanIntent",
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
    # Wrap/Unwrap Native Intents
    "WrapNativeIntent",
    "UnwrapNativeIntent",
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
    # Lending pre-flight (VIB-3701, VIB-3749)
    "AssetNotCollateralEligibleError",
    "PoolReserveFrozenError",
    "assert_lending_reserve_active",
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
    # Tick/Price Utilities
    "price_to_tick",
    "tick_to_price",
    "get_tick_spacing",
    "snap_to_tick_spacing",
    "get_min_tick",
    "get_max_tick",
    # Sadflow hooks
    "SadflowActionType",
    "SadflowAction",
    "SadflowContext",
    "SadflowEnterCallback",
    "SadflowExitCallback",
    "SadflowRetryCallback",
]


def __getattr__(name: str) -> object:
    if name == "PredictionExitConditions":
        from almanak.framework.services.prediction_monitor import (
            PredictionExitConditions,
        )

        globals()["PredictionExitConditions"] = PredictionExitConditions
        return PredictionExitConditions
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(globals()))
