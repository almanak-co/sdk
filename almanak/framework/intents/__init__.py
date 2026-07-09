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

# NOTE: ``PROTOCOL_CAPABILITIES`` is intentionally NOT imported eagerly here.
# It is exposed via module-level ``__getattr__`` below so the import is
# deferred until a consumer actually reads the attribute. Eager import would
# trigger ``CapabilitiesRegistry.all_capabilities()`` -> connector module
# imports -> back into ``framework.intents`` while ``intents/__init__.py``
# is still mid-execution, producing a partial-module ImportError on cold
# boot.
from .vocabulary import (
    AnyIntent,
    BorrowIntent,
    BundledCollateralBorrowError,
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
    PerpCancelIntent,
    PerpCloseIntent,
    PerpOpenIntent,
    PerpWithdrawIntent,
    # Prediction market intents
    PredictionBuyIntent,
    PredictionOrderType,
    PredictionOutcome,
    PredictionRedeemIntent,
    PredictionSellIntent,
    PredictionShareAmount,
    PredictionTimeInForce,
    # LP range spec (typed concentrated-liquidity range)
    PriceBand,
    ProtocolRequiredError,
    RangeSpec,
    RepayIntent,
    StakeIntent,
    SupplyIntent,
    SwapIntent,
    TickBand,
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
    "BundledCollateralBorrowError",
    "LpOpenZeroLiquidityError",
    "InvalidProtocolParameterError",
    "ProtocolRequiredError",
    "ChainedAmount",
    "InterestRateMode",
    "PROTOCOL_CAPABILITIES",
    "SwapIntent",
    "LPOpenIntent",
    "LPCloseIntent",
    "RangeSpec",
    "PriceBand",
    "TickBand",
    "CollectFeesIntent",
    "BorrowIntent",
    "RepayIntent",
    "DeleverageIntent",
    "SupplyIntent",
    "WithdrawIntent",
    "PerpOpenIntent",
    "PerpCloseIntent",
    "PerpCancelIntent",
    "PerpWithdrawIntent",
    "StakeIntent",
    "UnstakeIntent",
    "FlashLoanIntent",
    "HoldIntent",
    "IntentSequence",
    "AnyIntent",
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
    "AaveV3Adapter",
    "DEFAULT_GAS_ESTIMATES",
    "PROTOCOL_ROUTERS",
    "LP_POSITION_MANAGERS",
    "LENDING_POOL_ADDRESSES",
    "AAVE_VARIABLE_RATE_MODE",
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
    if name == "PROTOCOL_CAPABILITIES":
        # Lazy passthrough to the connector-owned registry. Resolved on first
        # attribute access (after every framework package has finished
        # importing) so connector capability modules can safely re-enter the
        # intent layer without producing a partial-module ImportError. The
        # registry caches the aggregated dict so identity is stable across
        # repeated reads, matching the long-standing semantics of the
        # previously hand-written table.
        from .vocabulary import PROTOCOL_CAPABILITIES as _caps

        globals()["PROTOCOL_CAPABILITIES"] = _caps
        return _caps
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(__all__) | set(globals()))
