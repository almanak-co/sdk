"""IntentStrategy Base Class for simplified strategy authoring.

This module provides the IntentStrategy base class that allows developers to write
strategies using the high-level Intent pattern. Strategies only need to implement
a decide() method that returns an Intent, and the framework handles:

1. Auto-compiling intents to ActionBundles
2. Auto-generating state machines for execution
3. Managing hot-reloadable configuration
4. Providing market data through MarketSnapshot helper

Example:
    from almanak.framework.market import MarketSnapshot
    from almanak.framework.strategies import IntentStrategy, almanak_strategy
    from almanak.framework.intents import Intent
    from decimal import Decimal

    @almanak_strategy(
        name="simple_dca",
        description="Simple DCA strategy that buys on schedule",
        version="1.0.0",
    )
    class SimpleDCAStrategy(IntentStrategy):
        def decide(self, market: MarketSnapshot) -> Optional[Intent]:
            if market.price("ETH") < Decimal("2000"):
                return Intent.swap("USDC", "ETH", amount_usd=Decimal("100"))
            return Intent.hold(reason="Price too high")
"""

import asyncio
import logging
from abc import abstractmethod
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from ..data.wallet_activity import WalletActivityProvider
    from ..portfolio.models import PortfolioSnapshot
    from ..teardown.models import (
        TeardownMode,
        TeardownPositionSummary,
        TeardownProfile,
        TeardownRequest,
    )
    from ..vault.config import SettlementResult

from almanak.core.models.quote_asset import QuoteAsset

from ..intents import (
    CompilationStatus,
    DecideResult,
    HoldIntent,
    Intent,
    IntentCompiler,
    IntentSequence,
    IntentStateMachine,
    StateMachineConfig,
)
from ..intents.state_machine import (
    SadflowAction,
    SadflowActionType,
    SadflowContext,
    TransactionReceipt,
)
from ..intents.vocabulary import AnyIntent
from ..models.reproduction_bundle import ActionBundle
from .base import (
    ConfigT,
    NotificationCallback,
    RiskGuardConfig,
    StrategyBase,
)
from .exceptions import ConfigValidationError  # noqa: F401  (re-exported for backward compatibility)

# ---------------------------------------------------------------------------
# Re-exports from extracted modules
# ---------------------------------------------------------------------------
# Every symbol that was historically importable from this module MUST remain
# importable.  The canonical definitions now live in sibling modules; we
# re-export them here for backward compatibility.
from .indicator_models import (  # noqa: F401
    ADXData,
    ATRData,
    BollingerBandsData,
    CCIData,
    IchimokuData,
    IndicatorProvider,
    MACDData,
    MAData,
    OBVData,
    RSIData,
    StochasticData,
)
from .lp_position_tracker import PERSISTENT_STATE_KEY as _LP_TRACKER_STATE_KEY
from .lp_position_tracker import LPPositionTracker
from .metadata import (  # noqa: F401
    LEGACY_COMPAT_DATA_REQUIREMENTS,
    StrategyClassT,
    StrategyDataRequirements,
    StrategyMetadata,
    almanak_strategy,
)
from .multichain import (  # noqa: F401
    AaveAvailableBorrowProvider,
    AaveHealthFactorProvider,
    ChainHealth,
    ChainHealthStatus,
    ChainNotConfiguredError,
    DataFreshnessPolicy,
    GmxAvailableLiquidityProvider,
    GmxFundingRateProvider,
    MultiChainBalanceProvider,
    MultiChainMarketSnapshot,
    MultiChainPriceOracle,
    StaleDataError,
)
from .strategy_models import (  # noqa: F401
    BalanceProvider,
    ExecutionResult,
    PriceData,
    PriceOracle,
    RSIProvider,
    TokenBalance,
)

logger = logging.getLogger(__name__)

# VIB-4062: MarketSnapshot moved to almanak.framework.market.snapshot.
# All helpers, constants, and the class itself live in the canonical package.
# This module re-exports MarketSnapshot for backward compat with deep imports
# like ``from almanak.framework.strategies.intent_strategy import MarketSnapshot``;
# such deep imports remain DISCOURAGED — use ``from almanak import MarketSnapshot``
# or ``from almanak.framework.market import MarketSnapshot`` instead.
from ..market import MarketSnapshot  # noqa: F401  (re-export for deep-import callers)
from ..market.snapshot import (
    DEFAULT_TIMEFRAME,
)

# =============================================================================
# Intent Strategy Base Class
# =============================================================================


class IntentStrategy(StrategyBase[ConfigT]):
    """Base class for Intent-based strategies.

    IntentStrategy simplifies strategy development by allowing developers to
    write just a decide() method that returns an Intent. The framework handles:

    1. Market data access via MarketSnapshot
    2. Intent compilation to ActionBundle
    3. State machine generation for execution
    4. Hot-reloadable configuration
    5. Error handling and retries

    Subclasses must implement the abstract decide() method.

    Example:
        @almanak_strategy(name="simple_strategy")
        class SimpleStrategy(IntentStrategy):
            def decide(self, market: MarketSnapshot) -> Optional[Intent]:
                if market.rsi("ETH").is_oversold:
                    return Intent.swap("USDC", "ETH", amount_usd=Decimal("100"))
                return Intent.hold()

    Attributes:
        compiler: IntentCompiler for converting intents to action bundles
        state_machine_config: Configuration for state machine execution
        _current_intent: Currently executing intent (if any)
        _current_state_machine: Current state machine (if any)
    """

    # Default strategy metadata (can be overridden by decorator)
    STRATEGY_METADATA: StrategyMetadata | None = None
    STRATEGY_NAME: str = "INTENT_STRATEGY"

    def __init__(
        self,
        config: ConfigT,
        chain: str,
        wallet_address: str,
        risk_guard_config: RiskGuardConfig | None = None,
        notification_callback: NotificationCallback | None = None,
        compiler: IntentCompiler | None = None,
        state_machine_config: StateMachineConfig | None = None,
        price_oracle: PriceOracle | None = None,
        rsi_provider: RSIProvider | None = None,
        balance_provider: BalanceProvider | None = None,
        rpc_url: str | None = None,
        wallet_activity_provider: "WalletActivityProvider | None" = None,
        chains: list[str] | None = None,
        chain_wallets: dict[str, str] | None = None,
    ) -> None:
        """Initialize the intent strategy.

        Args:
            config: Hot-reloadable configuration
            chain: Chain to operate on (e.g., "arbitrum")
            wallet_address: Wallet address for transactions
            risk_guard_config: Risk guard configuration
            notification_callback: Callback for operator notifications
            compiler: Intent compiler (required for direct run() calls, optional for runner)
            state_machine_config: State machine configuration
            price_oracle: Function to fetch prices
            rsi_provider: Function to calculate RSI (token, period[, timeframe=]) -> RSIData
            balance_provider: Function to fetch balances
            rpc_url: RPC URL for on-chain queries (needed for LP close)
            wallet_activity_provider: Provider for leader wallet activity signals
            chains: List of all chains this strategy operates on (multi-chain)
            chain_wallets: Per-chain wallet addresses from wallet registry
        """
        super().__init__(config, risk_guard_config, notification_callback)

        # Wire identity / chain context BEFORE the validation hook so that
        # subclass overrides can validate chain-dependent invariants
        # (supported pairs, per-chain limits, etc.). StrategyBase seeds
        # self._chain from ``config.chain`` which is "unknown" for plain-dict
        # configs, so we overwrite it here with the constructor-passed value
        # before any user code observes self.chain / self.chains.
        self._chain = chain
        self._wallet_address = wallet_address
        self._rpc_url = rpc_url
        self._chains = chains or [chain]
        self._chain_wallets = {k.lower(): v for k, v in chain_wallets.items()} if chain_wallets else None

        # Performance quote asset (definition-only). Resolved from the
        # @almanak_strategy decorator default here; a per-deployment config.json
        # override is applied once at boot by the runner/CLI
        # (apply_quote_asset_override) and then frozen — it is intentionally NOT
        # part of the hot-reloadable config surface, since changing the
        # denomination mid-run would make the performance series discontinuous.
        # The SDK does not branch on this value; the hosted platform consumes it.
        _qa_meta = getattr(self.__class__, "STRATEGY_METADATA", None)
        self._quote_asset: QuoteAsset = getattr(_qa_meta, "quote_asset", None) or QuoteAsset.usd()

        # Strategy-defined config validation hook.
        # Called AFTER config load (super().__init__) AND chain/wallet wiring,
        # but BEFORE any compiler/provider/state setup that depends on config.
        # Subclasses may override validate_config() to enforce preconditions
        # (e.g. required fields, value ranges, cross-field invariants) and
        # raise ConfigValidationError when validation fails. Default
        # implementation is a no-op so existing strategies are unaffected.
        #
        # Note on super().__init__ ordering: if a subclass calls super().__init__
        # late in its own __init__, validate_config() only runs once this line
        # executes. Subclasses that need their own attributes populated before
        # validation should either (a) populate them before calling super(), or
        # (b) perform those checks in their own __init__ after super() returns.
        self.validate_config()

        # Store compiler if provided (runner creates its own with real prices)
        # Do NOT auto-create - that would require placeholder prices which is unsafe
        self._compiler = compiler

        # State machine configuration
        self.state_machine_config = state_machine_config or StateMachineConfig()

        # Market data providers
        self._price_oracle = price_oracle
        self._rsi_provider = rsi_provider
        self._balance_provider = balance_provider
        self._wallet_activity_provider = wallet_activity_provider
        self._prediction_provider: Any | None = None
        self._indicator_provider: IndicatorProvider | None = None
        # VIB-3783: per-strategy OHLCV deduper, set by _wire_indicators when
        # indicators are wired. Its cache is cleared per iteration in
        # create_market_snapshot() to match _macd_cache / _atr_cache lifetimes.
        self._ohlcv_dedup_provider: Any | None = None

        # VIB-4843 FR-5001: per-iteration MarketSnapshot memo. The runner
        # pre-warms ONE snapshot's _price_cache; decide(), the post-decide
        # portfolio valuation (PortfolioValuer / Track-C), and any other path
        # that calls create_market_snapshot() must reuse that SAME instance so
        # the warm cache is not thrown away (the redundant-CoinGecko-call bug).
        # The memo is keyed by an iteration token the runner stamps via
        # begin_market_snapshot_iteration(); a short TTL guards against serving
        # a stale snapshot across iterations when no token is stamped (e.g. a
        # strategy author calling create_market_snapshot() directly in a test).
        self._cached_market_snapshot: MarketSnapshot | None = None
        self._cached_market_snapshot_token: object | None = None
        self._cached_market_snapshot_at: float | None = None
        self._multi_dex_service: Any | None = None
        self._rate_monitor: Any | None = None
        self._funding_rate_provider: Any | None = None
        self._gateway_client: Any | None = None

        # Multi-chain providers (set by set_multi_chain_providers)
        self._multi_chain_price_oracle: MultiChainPriceOracle | None = None
        self._multi_chain_balance_provider: MultiChainBalanceProvider | None = None
        self._aave_health_factor_provider: AaveHealthFactorProvider | None = None

        # Current execution state
        self._current_intent: AnyIntent | None = None
        self._current_state_machine: IntentStateMachine | None = None

        # State persistence (set by runner via set_state_manager)
        self._state_manager: Any | None = None
        self._deployment_id: str = ""
        self._state_version: int = 0
        self._pending_save: Any | None = None

        # VIB-3742: framework-default LP position tracker.
        #
        # Captures bin_ids / NFT position_ids from successful LP_OPEN results
        # and auto-injects them into LP_CLOSE / LP_COLLECT_FEES intents so a
        # strategy author cannot silently leak liquidity by forgetting the
        # bin_ids round-trip. Strategies that already track manually keep
        # working — manual ``protocol_params['bin_ids']`` always wins; the
        # tracker only fills missing data, never overwrites.
        #
        # Wired into the runner via two seams:
        # 1. ``_framework_record_intent_execution`` is invoked from
        #    ``StrategyRunner`` immediately before ``on_intent_executed``,
        #    so the user callback (which may itself read tracker state)
        #    sees the up-to-date capture.
        # 2. ``_framework_inject_intent_params`` is invoked from
        #    ``StrategyRunner._step_extract_intents`` over each intent that
        #    came back from ``decide()`` before compilation, so the compiler
        #    sees populated ``protocol_params``.
        self._lp_position_tracker: LPPositionTracker = LPPositionTracker()

        logger.info(f"Initialized IntentStrategy on {chain} with wallet {wallet_address[:10]}...")

    @property
    def chain(self) -> str:
        """Get the primary chain name."""
        return self._chain

    @property
    def chains(self) -> list[str]:
        """Get all chains this strategy operates on."""
        return self._chains

    def get_wallet_for_chain(self, chain: str) -> str:
        """Get the wallet address for a specific chain.

        If a wallet registry provided per-chain wallets, returns the
        chain-specific wallet. Otherwise falls back to the default wallet.

        Args:
            chain: Chain name (e.g., "arbitrum", "base")

        Returns:
            Wallet address for the specified chain
        """
        if self._chain_wallets:
            return self._chain_wallets.get(chain.lower(), self._wallet_address)
        return self._wallet_address

    @property
    def wallet_address(self) -> str:
        """Get the wallet address."""
        return self._wallet_address

    @property
    def compiler(self) -> IntentCompiler:
        """Get the intent compiler.

        Raises:
            RuntimeError: If compiler was not provided and is accessed directly.
                The StrategyRunner creates its own compiler with real prices,
                so this is only needed for direct run() calls.
        """
        if self._compiler is None:
            raise RuntimeError(
                "IntentCompiler not configured. Either:\n"
                "1. Use StrategyRunner which creates a compiler with real prices, or\n"
                "2. Pass a compiler to the strategy constructor for direct run() calls.\n"
                "Do NOT use placeholder prices - always use real price feeds."
            )
        return self._compiler

    @compiler.setter
    def compiler(self, value: IntentCompiler | None) -> None:
        """Set the intent compiler."""
        self._compiler = value

    @property
    def current_intent(self) -> AnyIntent | None:
        """Get the currently executing intent."""
        return self._current_intent

    @property
    def current_state_machine(self) -> IntentStateMachine | None:
        """Get the current state machine."""
        return self._current_state_machine

    # =========================================================================
    # Configuration Validation
    # =========================================================================

    def validate_config(self) -> None:
        """Validate the strategy's configuration.

        Lifecycle hook invoked automatically from :py:meth:`__init__` AFTER the
        config has been loaded (via ``super().__init__``) and BEFORE any other
        setup that depends on config (chain wiring, providers, state machine,
        etc.).

        Subclasses override this method to enforce preconditions on their
        configuration — required fields, value ranges, cross-field invariants,
        or any other invariant that must hold before the strategy is usable.
        On failure, raise :py:class:`ConfigValidationError` with a clear
        message and the offending ``field`` when applicable.

        This hook exists so tooling like the Portfolio Manager's ``strat check``
        preflight can catch misconfigurations at construction time rather than
        at the first ``decide()`` call in production.

        The default implementation is a no-op, so existing strategies require
        no changes.

        Raises:
            ConfigValidationError: If the configuration is invalid. The error's
                ``field`` attribute identifies the offending field when
                applicable; otherwise ``None`` for cross-field errors.

        Example:
            from decimal import Decimal
            from almanak.framework.strategies.exceptions import ConfigValidationError

            class MyStrategy(IntentStrategy):
                def validate_config(self) -> None:
                    # NOTE: configs loaded from JSON / env come back as strings.
                    # Always coerce numerics through Decimal(str(...)) so that
                    # comparisons are numeric, not lexicographic (e.g. "9" >= "10"
                    # is True as strings but False as numbers).
                    size = Decimal(str(self.get_config("trade_size_usd", "0")))
                    if size <= 0:
                        raise ConfigValidationError(
                            "trade_size_usd must be > 0",
                            field="trade_size_usd",
                        )
                    oversold = Decimal(str(self.get_config("rsi_oversold", "30")))
                    overbought = Decimal(str(self.get_config("rsi_overbought", "70")))
                    if oversold >= overbought:
                        raise ConfigValidationError(
                            "rsi_oversold must be < rsi_overbought",
                            field="rsi_oversold",
                        )
        """
        return None

    # =========================================================================
    # State Persistence
    # =========================================================================

    def set_state_manager(self, state_manager: Any, deployment_id: str) -> None:
        """Set the state manager for persistence.

        Called by the runner to inject the state manager.

        Args:
            state_manager: StateManager instance
            deployment_id: Unique ID for this strategy instance
        """
        self._state_manager = state_manager
        self._deployment_id = deployment_id

    def get_persistent_state(self) -> dict[str, Any]:
        """Get strategy state to persist.

        Override this method to define what state should be persisted.
        Default implementation returns empty dict (no state).

        Returns:
            Dict of state key-value pairs to persist
        """
        return {}

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persisted state into the strategy.

        Override this method to restore state from persistence.
        Default implementation does nothing.

        Args:
            state: Dict of state key-value pairs loaded from storage
        """
        pass

    # crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
    def save_state(self) -> None:
        """Save current strategy state to persistence.

        Called by runner after each iteration.
        """
        if not self._state_manager or not self._deployment_id:
            return

        state = self.get_persistent_state() or {}

        # VIB-3742: append framework-owned LP tracker state under a reserved
        # key. Always overwrite the framework slot (even with an empty dict)
        # so a fully-cleared tracker — e.g. the strategy just closed its last
        # position — leaves an explicit empty payload in storage rather than
        # the stale prior state. Without this, a restart would resurrect
        # already-closed bin_ids / position_ids and re-inject them into the
        # next LP_CLOSE / LP_COLLECT_FEES.
        tracker = getattr(self, "_lp_position_tracker", None)
        if tracker is not None:
            state = dict(state)  # don't mutate user's dict in place
            state[_LP_TRACKER_STATE_KEY] = tracker.to_persistent_dict()

        if not state:
            return

        try:
            from ..state.state_manager import StateData

            # Create StateData object with the strategy state
            # Try to get existing version for proper CAS updates
            version = getattr(self, "_state_version", 0) + 1

            state_data = StateData(
                deployment_id=self.deployment_id,
                version=version,
                state=state,
            )

            # Run async save_state - handle both sync and async contexts
            try:
                asyncio.get_running_loop()
                # We're in an async context, schedule as task
                future = asyncio.ensure_future(self._state_manager.save_state(state_data))
                # Store future for potential awaiting
                self._pending_save = future
            except RuntimeError:
                # No running loop - create one and run
                asyncio.run(self._state_manager.save_state(state_data))

            # Update version for next save
            self._state_version = version

            logger.debug(f"Saved state for {self._deployment_id}: {list(state.keys())}")
        except Exception as e:
            logger.warning(f"Failed to save state: {e}")

    async def flush_pending_saves(self) -> None:
        """Wait for any pending save operations to complete.

        This should be called before disconnecting from the gateway to ensure
        all state saves have completed. Handles both successful completion and
        errors gracefully.
        """
        if self._pending_save is None:
            return

        if not self._pending_save.done():
            try:
                await self._pending_save
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Pending save failed during flush: {e}")
        else:
            # Task already completed, check for exceptions
            try:
                self._pending_save.result()
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Pending save had error: {e}")

        self._pending_save = None

    def _split_framework_state(self, state: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
        """Separate framework-owned state keys from user state.

        Framework-owned keys (``__framework_*__``) are restored into
        framework components (LPPositionTracker, etc.) before the strategy's
        own ``load_persistent_state`` is invoked. Returns
        ``(user_state, framework_state)``.
        """
        if not state:
            return {}, {}
        framework_state: dict[str, Any] = {}
        user_state: dict[str, Any] = {}
        for key, value in state.items():
            if isinstance(key, str) and key.startswith("__framework_") and key.endswith("__"):
                framework_state[key] = value
            else:
                user_state[key] = value
        return user_state, framework_state

    def _restore_framework_state(self, framework_state: dict[str, Any]) -> None:
        """Apply framework-owned state to internal components."""
        tracker = getattr(self, "_lp_position_tracker", None)
        if tracker is not None:
            tracker_data = framework_state.get(_LP_TRACKER_STATE_KEY)
            if isinstance(tracker_data, dict):
                tracker.load_persistent_dict(tracker_data)

    # crap-allowlist: VIB-4722 mechanical deployment_id rename in existing high-CRAP function.
    def load_state(self) -> bool:
        """Load strategy state from persistence.

        Called by runner on startup.

        Returns:
            True if state was found and loaded, False otherwise
        """
        if not self._state_manager or not self._deployment_id:
            return False

        try:
            # Run async load_state - handle both sync and async contexts
            try:
                asyncio.get_running_loop()
                # We're in an async context - can't block here
                logger.debug("Cannot load state synchronously in async context")
                return False
            except RuntimeError:
                # No running loop - create one and run
                state_data = asyncio.run(self._state_manager.load_state(self._deployment_id))

            if state_data and state_data.state:
                user_state, framework_state = self._split_framework_state(state_data.state)
                self._restore_framework_state(framework_state)
                self.load_persistent_state(user_state)
                # Store version for CAS updates
                self._state_version = state_data.version
                # Log state keys with summarized values for operator visibility
                state_summary = {
                    k: (f"{v:.6g}" if isinstance(v, float) else str(v)[:80]) for k, v in state_data.state.items()
                }
                logger.info(f"Loaded state for {self._deployment_id}: {state_summary}")
                return True
            return False
        except Exception as e:
            # StateNotFoundError is expected for fresh starts
            if "not found" in str(e).lower():
                logger.debug(f"No existing state for {self._deployment_id}")
            else:
                logger.warning(f"Failed to load state: {e}")
            return False

    async def load_state_async(self) -> bool:
        """Async variant of load_state() -- preferred when already in an event loop.

        Called by the CLI runner inside its async setup so that state is always
        restored correctly, regardless of whether a loop is already running.
        """
        if not self._state_manager or not self._deployment_id:
            return False
        try:
            state_data = await self._state_manager.load_state(self._deployment_id)
            if state_data and state_data.state:
                user_state, framework_state = self._split_framework_state(state_data.state)
                self._restore_framework_state(framework_state)
                self.load_persistent_state(user_state)
                self._state_version = state_data.version
                state_summary = {
                    k: (f"{v:.6g}" if isinstance(v, float) else str(v)[:80]) for k, v in state_data.state.items()
                }
                logger.info(f"Loaded state for {self._deployment_id}: {state_summary}")
                return True
            return False
        except Exception as e:
            if "not found" in str(e).lower():
                logger.debug(f"No existing state for {self._deployment_id}")
            else:
                logger.warning(f"Failed to load state: {e}")
            return False

    @abstractmethod
    def decide(self, market: MarketSnapshot) -> DecideResult:
        """Decide what action to take based on current market conditions.

        This is the main method that strategy developers need to implement.
        It receives a MarketSnapshot with current market data and should
        return an Intent, IntentSequence, list of intents, or None.

        Args:
            market: Current market snapshot with prices, balances, RSI, etc.

        Returns:
            One of:
            - Single Intent: Execute one action
            - IntentSequence: Execute multiple actions sequentially (dependent)
            - list[Intent | IntentSequence]: Execute items in parallel
            - None: Take no action (equivalent to Intent.hold())

            Returning None is equivalent to returning Intent.hold().

        Example:
            def decide(self, market: MarketSnapshot) -> DecideResult:
                # Single intent
                if market.rsi("ETH").is_oversold:
                    return Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))

                # Sequence of dependent actions (execute in order)
                if should_move_funds:
                    return Intent.sequence([
                        Intent.swap("USDC", "ETH", amount=Decimal("1000"), chain="base"),
                        Intent.supply(protocol="aave_v3", token="WETH", amount=Decimal("0.5"), chain="arbitrum"),
                    ])

                # Multiple independent actions (execute in parallel)
                if should_rebalance:
                    return [
                        Intent.swap("USDC", "ETH", amount=Decimal("500"), chain="arbitrum"),
                        Intent.swap("USDC", "ETH", amount=Decimal("500"), chain="optimism"),
                    ]

                # No action
                return Intent.hold(reason="RSI in neutral zone")
        """
        pass

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Called after each intent execution completes.

        Override this method to react to execution results, e.g., to track
        position IDs, log swap amounts, or update state based on results.

        The result object is enriched by the framework with extracted data
        that "just appears" based on intent type:
        - SWAP: result.swap_amounts (SwapAmounts)
        - LP_OPEN: result.position_id, result.extracted_data["liquidity"]
        - LP_CLOSE: result.lp_close_data (LPCloseData)
        - PERP_OPEN: result.extracted_data["entry_price"], ["leverage"]

        Args:
            intent: The intent that was executed
            success: Whether execution succeeded
            result: ExecutionResult with enriched data
        """
        pass

    # =========================================================================
    # Framework hooks — invoked by the runner. Strategies should NOT call or
    # override these directly. They route through the LPPositionTracker so
    # bin_ids and NFT position_ids round-trip across LP_OPEN / LP_CLOSE
    # without any caller boilerplate (VIB-3742).
    # =========================================================================

    def _framework_record_intent_execution(self, intent: Any, success: bool, result: Any) -> None:
        """Framework-only hook: capture LP position metadata from results.

        The runner calls this BEFORE invoking ``on_intent_executed`` so the
        user callback sees the freshest tracker state. Failures here are
        logged at WARNING and never propagated.
        """
        tracker = getattr(self, "_lp_position_tracker", None)
        if tracker is None:
            return
        tracker.record_intent_execution(
            intent=intent,
            success=success,
            result=result,
            default_chain=self._chain,
        )

    def _framework_inject_intent_params(self, intent: Any) -> Any:
        """Framework-only hook: auto-fill protocol_params on LP_CLOSE intents.

        The runner calls this on each intent returned from ``decide()`` before
        compilation. Returns the same intent if no injection is needed, or a
        copy with ``protocol_params['bin_ids']`` (and future analogues) filled
        in from previously captured LP_OPEN data.

        Manual ``protocol_params`` always wins — the tracker never overwrites.
        Failures here return the original intent unchanged.
        """
        tracker = getattr(self, "_lp_position_tracker", None)
        if tracker is None:
            return intent
        return tracker.maybe_inject(intent, default_chain=self._chain)

    @property
    def lp_position_tracker(self) -> LPPositionTracker:
        """Read-only accessor to the framework's LP position tracker.

        Exposed for tests and tooling. Strategies should not need to touch
        this directly — the runner manages it transparently.
        """
        return self._lp_position_tracker

    def _require_deployment_id(self, operation: str) -> str:
        deployment_id = (self._deployment_id or "").strip()
        if not deployment_id:
            raise RuntimeError(f"{operation} requires a resolved deployment_id")
        return deployment_id

    def valuate(self, market: MarketSnapshot) -> Decimal:
        """Calculate the total portfolio value in USD for vault settlement.

        Called by the framework during vault settlement to determine the
        current value of the strategy's holdings. The returned value is
        converted to underlying token units and proposed as the new
        totalAssets for the vault.

        The default implementation sums balance_usd for all known token
        balances in the market snapshot. Override this method for custom
        valuation logic (e.g., including LP positions, pending rewards,
        or off-chain assets).

        Args:
            market: Current market snapshot with prices and balances

        Returns:
            Total portfolio value in USD as a Decimal
        """
        return market.total_portfolio_usd()

    def on_vault_settled(self, settlement: "SettlementResult") -> None:
        """Called after a vault settlement cycle completes.

        Override this method to react to settlement results, e.g., to
        log deposit/redemption amounts or update internal state.

        Args:
            settlement: SettlementResult with deposit/redemption data
        """
        pass

    def set_multi_chain_providers(
        self,
        price_oracle: MultiChainPriceOracle | None = None,
        balance_provider: MultiChainBalanceProvider | None = None,
        aave_health_factor_provider: AaveHealthFactorProvider | None = None,
    ) -> None:
        """Set multi-chain data providers for cross-chain strategies.

        Call this method before running a multi-chain strategy to enable
        MultiChainMarketSnapshot creation.

        Args:
            price_oracle: Multi-chain price oracle
            balance_provider: Multi-chain balance provider
            aave_health_factor_provider: Aave health factor provider
        """
        self._multi_chain_price_oracle = price_oracle
        self._multi_chain_balance_provider = balance_provider
        self._aave_health_factor_provider = aave_health_factor_provider

    def is_multi_chain(self) -> bool:
        """Check if this strategy is running in multi-chain mode.

        Returns True only when SUPPORTED_CHAINS is explicitly set (manually or by
        the CLI multi-chain path) AND has >1 chain. Does NOT use decorator metadata
        because that is portability info, not a runtime signal. The CLI's
        is_multi_chain_strategy() makes the runtime decision based on config.chains.

        Returns:
            True if SUPPORTED_CHAINS has multiple chains
        """
        supported_chains = getattr(self.__class__, "SUPPORTED_CHAINS", None)
        if supported_chains and isinstance(supported_chains, list | tuple):
            return len(supported_chains) > 1
        return False

    def get_supported_chains(self) -> list[str]:
        """Get the chains supported by this strategy.

        Returns SUPPORTED_CHAINS if explicitly set, otherwise falls back to
        STRATEGY_METADATA.supported_chains (decorator portability metadata),
        then to [self._chain].

        Returns:
            List of supported chain names
        """
        chains = getattr(self.__class__, "SUPPORTED_CHAINS", None)
        if chains:
            return list(chains)
        # Fallback: decorator portability metadata (safe for informational use,
        # but does NOT affect is_multi_chain() or create_market_snapshot())
        metadata = getattr(self.__class__, "STRATEGY_METADATA", None)
        if metadata and hasattr(metadata, "supported_chains") and metadata.supported_chains:
            return list(metadata.supported_chains)
        return [self._chain]

    def begin_market_snapshot_iteration(self, token: object) -> None:
        """Open a new per-iteration MarketSnapshot scope (VIB-4843 FR-5001).

        The runner calls this once at the top of each ``run_iteration`` with a
        token unique to the iteration (the ``cycle_id``). Subsequent
        ``create_market_snapshot()`` calls within the same iteration — pre-warm,
        ``decide()``, and post-decide portfolio valuation — return the SAME
        instance so the pre-warmed ``_price_cache`` is reused instead of thrown
        away. Passing a new token (or ``None``) invalidates the memo.

        Idempotent: re-stamping the current token is a no-op so the runner can
        call it defensively without dropping a warm snapshot mid-iteration.
        """
        if token is not None and token == getattr(self, "_cached_market_snapshot_token", None):
            return
        self._cached_market_snapshot = None
        self._cached_market_snapshot_token = token
        self._cached_market_snapshot_at = None

    def _market_snapshot_cache_ttl_seconds(self) -> float:
        """Resolve the per-iteration snapshot memo TTL (seconds).

        Default ~5s (matching the data-layer balance/OHLCV short-cache TTLs)
        guards against serving a stale snapshot across iterations when no
        iteration token is stamped — e.g. a strategy author calling
        ``create_market_snapshot()`` directly in a tight loop or a test.
        Configurable via ``market_snapshot_cache_ttl_seconds``.
        """
        try:
            return float(self.get_config("market_snapshot_cache_ttl_seconds", 5.0))
        except (TypeError, ValueError):
            return 5.0

    def create_market_snapshot(self) -> MarketSnapshot:
        """Return the per-iteration market snapshot, building one if needed.

        VIB-4843 FR-5001: memoizes the snapshot per iteration so pre-warm →
        ``decide()`` → portfolio valuation all share ONE instance (and its
        ``_price_cache``). Reuse is keyed by the iteration token the runner
        stamps via :meth:`begin_market_snapshot_iteration`.

        Two invalidation regimes, never both:

        * **Iteration-token stamped** (the runner's per-iteration ``cycle_id``):
          the token IS the lifetime. The memo survives for the WHOLE iteration —
          pre-warm → decide() → portfolio valuation — regardless of wall-clock,
          because :meth:`begin_market_snapshot_iteration` invalidates it the
          moment the next iteration stamps a new token. Applying a TTL here would
          drop the warm ``_price_cache`` mid-iteration on a slow ``decide()`` and
          defeat the dedup (the Codex VIB-4843 finding).
        * **No token stamped** (direct callers / tests): a short TTL bounds reuse
          so prices are never served stale across loops with no iteration scope.

        Builds via :meth:`_build_market_snapshot`; see its docstring for the
        builder-contract details. Override ``_build_market_snapshot`` (not this
        method) to customize how market data is populated, so the memo still
        applies.
        """
        import time

        # Defensive getattr: some test/legacy construction paths build the
        # strategy via ``object.__new__`` and bypass ``__init__`` (the memo
        # attributes are then absent). Treat a missing memo as "no cache".
        cached = getattr(self, "_cached_market_snapshot", None)
        if cached is not None:
            token = getattr(self, "_cached_market_snapshot_token", None)
            if token is not None:
                # Iteration-scoped: the cycle_id token bounds the lifetime, so
                # the snapshot is stable for the full iteration. TTL does not
                # apply — only a new iteration token (re)builds.
                return cached
            # Token-less: bound reuse by the short TTL.
            cached_at = getattr(self, "_cached_market_snapshot_at", None)
            within_ttl = cached_at is None or (time.monotonic() - cached_at) < self._market_snapshot_cache_ttl_seconds()
            if within_ttl:
                return cached

        snapshot = self._build_market_snapshot()
        self._cached_market_snapshot = snapshot
        self._cached_market_snapshot_at = time.monotonic()
        return snapshot

    def _build_market_snapshot(self) -> MarketSnapshot:
        """Construct a fresh MarketSnapshot (cold cache).

        Routes through ``MarketSnapshotBuilder.for_strategy_runner`` so the
        snapshot's ``runtime_surface`` is correctly stamped — direct
        ``MarketSnapshot(...)`` calls bypass the builder contract that PRD §4.3
        promises ("every snapshot records the builder factory it came from").

        Multi-chain strategies pass ``chains=`` to the builder, which threads
        the multi-chain providers and ``aave_health_factor_provider`` through
        the canonical class.

        VIB-4843: this is the per-iteration mint. The per-strategy OHLCV
        deduper cache is cleared HERE (not in the memoizing wrapper) so it
        resets exactly once per fresh snapshot — matching the per-iteration
        lifetime of the ``_macd_cache`` / ``_atr_cache`` dicts (VIB-3783) —
        rather than on every cache-hit reuse within the same iteration.
        """
        # VIB-3783: clear the per-strategy OHLCV deduper cache at the start of
        # each iteration so we coalesce within an iteration but always refetch
        # between iterations.
        if self._ohlcv_dedup_provider is not None:
            self._ohlcv_dedup_provider.clear()

        from ..market.builders import MarketSnapshotBuilder

        if self.is_multi_chain():
            chains = self.get_supported_chains()
            logger.debug(f"Creating multi-chain MarketSnapshot for chains: {chains}")
            return MarketSnapshotBuilder.for_strategy_runner(
                strategy=self,
                runtime_context=getattr(self, "_runtime_context", None),
                gateway_client=self._gateway_client,
                chain=self._chain,
                wallet_address=self._wallet_address,
                chains=tuple(chains),
                multi_chain_price_oracle=self._multi_chain_price_oracle,
                multi_chain_balance_provider=self._multi_chain_balance_provider,
                aave_health_factor_provider=self._aave_health_factor_provider,
            )

        return MarketSnapshotBuilder.for_strategy_runner(
            strategy=self,
            runtime_context=getattr(self, "_runtime_context", None),
            gateway_client=self._gateway_client,
            chain=self._chain,
            wallet_address=self._wallet_address,
            default_timeframe=self.get_config("data_granularity"),
        )

    def run(self) -> ActionBundle | None:
        """Execute one iteration of the strategy.

        This method:
        1. Creates a MarketSnapshot
        2. Calls decide() to get an intent or DecideResult
        3. Compiles single intents to an ActionBundle
        4. Returns the ActionBundle for execution

        Note: For multi-intent results (list or IntentSequence), this method
        only compiles the first intent. Use run_multi() for full multi-intent
        execution with proper parallel/sequential handling.

        Returns:
            ActionBundle to execute, or None if HOLD intent or no action
        """
        import time

        start_time = time.time()

        try:
            # Create market snapshot
            market = self.create_market_snapshot()

            # Get result from strategy logic
            result = self.decide(market)

            # Handle None (treat as HOLD)
            if result is None:
                self._current_intent = Intent.hold(reason="decide() returned None")
                logger.info("HOLD: decide() returned None")
                return None

            # Normalize result to get the first intent for backward compatibility
            items = Intent.normalize_decide_result(result)
            if not items:
                self._current_intent = Intent.hold(reason="Empty result")
                logger.info("HOLD: Empty result from decide()")
                return None

            # Get the first item (for backward compatibility with single-intent strategies)
            first_item = items[0]

            # If it's a sequence, get the first intent from the sequence
            if isinstance(first_item, IntentSequence):
                intent = first_item.first
                logger.debug(
                    f"Strategy decision: IntentSequence with {len(first_item)} intents "
                    f"(sequence_id={first_item.sequence_id})"
                )
            else:
                intent = first_item

            self._current_intent = intent

            logger.debug(f"Strategy decision: {intent.intent_type.value} (intent_id={intent.intent_id})")

            # Handle HOLD intent - no action needed
            if isinstance(intent, HoldIntent):
                logger.info(f"HOLD intent: {intent.reason or 'no reason provided'}")
                return None

            # Log if there are multiple items for parallel execution
            if len(items) > 1:
                logger.info(
                    f"Note: decide() returned {len(items)} items for parallel execution. "
                    "Use run_multi() for full multi-intent support."
                )

            # VIB-3742: apply the framework intent-injection hook here too —
            # direct callers of run() (outside StrategyRunner) must benefit
            # from the same auto-injection of tracked LP metadata
            # (e.g. TraderJoe V2 bin_ids) that runner-driven strategies get.
            try:
                intent = self._framework_inject_intent_params(intent)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Framework intent-injection hook raised in run() (non-fatal, compiling original intent): %s",
                    exc,
                    exc_info=True,
                )

            # Compile intent to ActionBundle
            compilation_result = self.compiler.compile(intent)

            if compilation_result.status != CompilationStatus.SUCCESS:
                logger.error(f"Intent compilation failed: {compilation_result.error}")
                return None

            return compilation_result.action_bundle

        except Exception as e:
            logger.exception(f"Error in strategy run(): {e}")
            return None

        finally:
            elapsed_ms = (time.time() - start_time) * 1000
            logger.debug(f"Strategy iteration completed in {elapsed_ms:.2f}ms")

    def run_multi(self) -> DecideResult:
        """Execute one iteration of the strategy, returning the full DecideResult.

        Unlike run(), this method returns the full DecideResult from decide()
        without compiling to ActionBundle. This is useful for multi-chain
        execution via MultiChainOrchestrator.

        Returns:
            DecideResult: The raw result from decide() (may be None, single intent,
            IntentSequence, or list of intents/sequences)
        """
        import time

        start_time = time.time()

        try:
            # Create market snapshot
            market = self.create_market_snapshot()

            # Get result from strategy logic
            result = self.decide(market)

            # Store for reference - extract first AnyIntent for _current_intent
            current: AnyIntent | None = None
            if result is None:
                current = Intent.hold(reason="decide() returned None")
            elif isinstance(result, IntentSequence):
                current = result.first if result.intents else None
            elif isinstance(result, list):
                # For lists, store the first non-sequence item or first item in first sequence
                for item in result:
                    if isinstance(item, IntentSequence):
                        current = item.first
                        break
                    else:
                        current = item
                        break
            else:
                current = result

            self._current_intent = current

            intent_count = Intent.count_intents(result)
            logger.debug(f"Strategy decision: {intent_count} intent(s)")

            return result

        except Exception as e:
            logger.exception(f"Error in strategy run_multi(): {e}")
            return None

        finally:
            elapsed_ms = (time.time() - start_time) * 1000
            logger.debug(f"Strategy iteration completed in {elapsed_ms:.2f}ms")

    def run_with_state_machine(
        self,
        receipt_provider: Callable[[ActionBundle], TransactionReceipt] | None = None,
    ) -> ExecutionResult:
        """Execute strategy with full state machine lifecycle.

        This method provides full state machine execution including:
        - Intent compilation
        - Transaction execution (via receipt_provider)
        - Validation
        - Retry logic on failure

        Note: This method only handles single intents for backward compatibility.
        For multi-intent execution, use run_multi() with MultiChainOrchestrator.

        Args:
            receipt_provider: Function that executes an ActionBundle and returns
                a TransactionReceipt. If not provided, returns after compilation.

        Returns:
            ExecutionResult with full execution details
        """
        import time

        start_time = time.time()
        result = ExecutionResult(intent=None)

        try:
            # Create market snapshot and get intent
            market = self.create_market_snapshot()
            decide_result = self.decide(market)

            # Normalize to get the first single intent
            if decide_result is None:
                intent: AnyIntent = Intent.hold(reason="decide() returned None")
            elif isinstance(decide_result, IntentSequence):
                intent = decide_result.first
                logger.info(
                    f"Note: decide() returned IntentSequence with {len(decide_result)} intents. "
                    "Only first intent will be executed via state machine."
                )
            elif isinstance(decide_result, list):
                # Get first item from list
                if not decide_result:
                    intent = Intent.hold(reason="Empty result list")
                else:
                    first_item = decide_result[0]
                    if isinstance(first_item, IntentSequence):
                        intent = first_item.first
                    else:
                        intent = first_item
                logger.info(
                    f"Note: decide() returned {len(decide_result)} items for parallel execution. "
                    "Only first intent will be executed via state machine."
                )
            else:
                intent = decide_result

            result.intent = intent
            self._current_intent = intent

            # Handle HOLD intent
            if isinstance(intent, HoldIntent):
                logger.info(f"HOLD: {intent.reason or 'no reason'}")
                result.success = True
                return result

            # VIB-3742: apply framework intent-injection BEFORE the state
            # machine so direct callers benefit from auto-tracked LP metadata
            # (e.g. TraderJoe V2 bin_ids) just like StrategyRunner-driven
            # strategies do.
            try:
                intent = self._framework_inject_intent_params(intent)
                result.intent = intent
                self._current_intent = intent
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Framework intent-injection hook raised in run_with_state_machine "
                    "(non-fatal, proceeding with original intent): %s",
                    exc,
                    exc_info=True,
                )

            # Create state machine with sadflow hooks
            self._current_state_machine = IntentStateMachine(
                intent=intent,
                compiler=self.compiler,
                config=self.state_machine_config,
                on_sadflow_enter=self.on_sadflow_enter,
                on_sadflow_exit=self.on_sadflow_exit,
                on_retry=self.on_retry,
            )

            # Execute through state machine
            while not self._current_state_machine.is_complete:
                step_result = self._current_state_machine.step()
                result.state_machine_result = step_result

                if step_result.action_bundle:
                    result.action_bundle = step_result.action_bundle

                if step_result.needs_execution and step_result.action_bundle:
                    if receipt_provider:
                        # Execute and get receipt
                        receipt = receipt_provider(step_result.action_bundle)
                        self._current_state_machine.set_receipt(receipt)
                    else:
                        # No execution provider - return after compilation
                        result.success = True
                        return result

                if step_result.retry_delay:
                    # Wait for retry delay
                    time.sleep(step_result.retry_delay)

            # Set final result
            result.success = self._current_state_machine.success
            result.error = self._current_state_machine.error

            # VIB-3742: record framework intent-execution for tracker capture
            # (e.g. TraderJoe V2 bin_ids from LP_OPEN). Mirror what
            # StrategyRunner._notify_intent_executed does so direct callers of
            # run_with_state_machine() also feed the tracker. Hook failures are
            # logged but never fail the iteration.
            try:
                self._framework_record_intent_execution(intent, result.success, result)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Framework intent-execution hook raised in run_with_state_machine (non-fatal): %s",
                    exc,
                    exc_info=True,
                )

            return result

        except Exception as e:
            logger.exception(f"Error in run_with_state_machine(): {e}")
            result.success = False
            result.error = str(e)
            return result

        finally:
            elapsed_ms = (time.time() - start_time) * 1000
            result.execution_time_ms = elapsed_ms
            logger.debug(f"State machine execution completed in {elapsed_ms:.2f}ms")

    @property
    def quote_asset(self) -> QuoteAsset:
        """The resolved performance quote asset (decorator default or boot override).

        Definition-only: exposed as metadata for the hosted platform. The SDK does
        not change valuation/accounting/CLI behaviour based on it, and it is frozen
        after boot (not part of the hot-reloadable config surface).
        """
        return getattr(self, "_quote_asset", None) or QuoteAsset.usd()

    def apply_quote_asset_override(self, raw: QuoteAsset | str | dict[str, Any] | None) -> None:
        """Apply a per-deployment config.json ``quote_asset`` override at boot.

        Called once by the runner/CLI after construction. ``None`` keeps the
        decorator default. Frozen thereafter — there is no hot-reload path that
        mutates it, by design.
        """
        if raw is not None:
            self._quote_asset = QuoteAsset.parse(raw)

    def get_metadata(self) -> StrategyMetadata | None:
        """Get strategy metadata if available.

        Returns:
            StrategyMetadata if set via decorator, otherwise None
        """
        return getattr(self.__class__, "STRATEGY_METADATA", None)

    def to_dict(self) -> dict[str, Any]:
        """Serialize strategy state to dictionary.

        Returns:
            Dictionary representation of strategy state
        """
        metadata = self.get_metadata()

        return {
            "strategy_name": self.__class__.STRATEGY_NAME,
            "chain": self._chain,
            "wallet_address": self._wallet_address,
            "config": self.config.to_dict(),
            "config_version": self.get_current_config_version(),
            "current_intent": self._current_intent.serialize() if self._current_intent else None,
            "metadata": metadata.to_dict() if metadata else None,
            # Effective (boot-resolved) quote asset — reflects any config.json
            # override; the authoritative denomination for THIS deployment.
            # (metadata["quote_asset"] above is the un-overridden decorator default.)
            "quote_asset": self.quote_asset.to_dict(),
        }

    # =========================================================================
    # Sadflow Lifecycle Hooks
    # =========================================================================

    def on_sadflow_enter(
        self,
        error_type: str | None,
        attempt: int,
        context: SadflowContext,
    ) -> SadflowAction | None:
        """Hook called when entering sadflow state.

        Override this method to customize sadflow behavior for your strategy.
        This is called once when first entering sadflow, before any retry attempts.

        Args:
            error_type: Categorized error type (e.g., "INSUFFICIENT_FUNDS",
                "TIMEOUT", "SLIPPAGE", "REVERT"). May be None for uncategorized errors.
            attempt: Current attempt number (1-indexed).
            context: SadflowContext with error details and execution state.

        Returns:
            Optional[SadflowAction]: Action to take. Return None to use default
            retry behavior. Return SadflowAction to customize:
            - SadflowAction.retry(): Continue with default retry
            - SadflowAction.abort(reason): Stop immediately and fail
            - SadflowAction.modify(bundle): Retry with modified ActionBundle
            - SadflowAction.skip(reason): Skip intent and mark as completed

        Example:
            def on_sadflow_enter(self, error_type, attempt, context):
                # Abort immediately on insufficient funds
                if error_type == "INSUFFICIENT_FUNDS":
                    return SadflowAction.abort("Not enough funds for transaction")

                # Increase gas for gas errors
                if error_type == "GAS_ERROR" and context.action_bundle:
                    modified = self._increase_gas(context.action_bundle)
                    return SadflowAction.modify(modified, reason="Increased gas limit")

                # Use default retry for other errors
                return None
        """
        return None

    # =========================================================================
    # Teardown Interface
    # =========================================================================
    # These methods enable safe strategy teardown (closing all positions).
    # Override these in your strategy to support the teardown system.

    async def pause(self) -> None:
        """Pause the strategy during teardown.

        Called by TeardownManager before executing teardown intents.
        Default is a no-op; override if your strategy needs to stop
        background tasks or cancel pending orders before teardown.
        """

    # =========================================================================
    # Portfolio Value Tracking
    # =========================================================================
    # These methods enable portfolio value and PnL tracking for the dashboard.
    # The default implementation uses get_open_positions() if available.

    def get_portfolio_snapshot(self, market: "MarketSnapshot | None" = None) -> "PortfolioSnapshot":
        """Get current portfolio value and positions.

        This method is called by the StrategyRunner after each iteration to
        capture portfolio snapshots for:
        - Dashboard value display (Total Value, PnL)
        - Historical PnL charts
        - Position breakdown by type

        Default implementation:
        1. Calls get_open_positions() for position values (LP, lending, perps)
        2. Adds wallet token balances not captured by positions

        Override for strategies needing custom value calculation (CEX, prediction).

        Args:
            market: Optional MarketSnapshot. If None, creates one internally.

        Returns:
            PortfolioSnapshot with current values and confidence level.
            If value cannot be computed, returns snapshot with
            value_confidence=UNAVAILABLE instead of $0.

        Example:
            def get_portfolio_snapshot(self, market=None) -> PortfolioSnapshot:
                if market is None:
                    market = self.create_market_snapshot()

                # Custom CEX balance fetch
                cex_balance = self._fetch_cex_balance()

                return PortfolioSnapshot(
                    timestamp=datetime.now(UTC),
                    deployment_id=self.deployment_id,
                    total_value_usd=cex_balance,
                    available_cash_usd=cex_balance,
                    value_confidence=ValueConfidence.ESTIMATED,
                    chain=self.chain,
                )
        """
        from ..portfolio.models import PortfolioSnapshot, PositionValue, TokenBalance, ValueConfidence

        # Get or create market snapshot
        if market is None:
            try:
                market = self.create_market_snapshot()
            except Exception as e:  # noqa: BLE001  # Intentional graceful degradation
                logger.warning(f"Failed to create market snapshot for portfolio: {e}")
                return PortfolioSnapshot(
                    timestamp=datetime.now(UTC),
                    deployment_id=self.deployment_id,
                    total_value_usd=Decimal("0"),
                    available_cash_usd=Decimal("0"),
                    value_confidence=ValueConfidence.UNAVAILABLE,
                    error=f"Failed to create market snapshot: {e}",
                    chain=self._chain,
                )

        try:
            # Step 1: Get position values via existing teardown infrastructure
            positions: list[PositionValue] = []
            position_value = Decimal("0")
            positions_unavailable = False

            try:
                position_summary = self.get_open_positions()
                for p in position_summary.positions:
                    positions.append(
                        PositionValue(
                            position_type=p.position_type,
                            protocol=p.protocol,
                            chain=p.chain,
                            value_usd=p.value_usd,
                            label=f"{p.protocol} {p.position_type.value}",
                            tokens=p.details.get("tokens", []),
                            details=p.details,
                        )
                    )
                position_value = position_summary.total_value_usd
            except Exception as e:  # noqa: BLE001  # Intentional graceful degradation
                logger.warning(f"Failed to get open positions: {e}")
                positions_unavailable = True

            # Step 2: Add wallet balances (uninvested funds)
            wallet_balances: list[TokenBalance] = []
            wallet_value = Decimal("0")

            tracked_tokens = self._get_tracked_tokens()
            for token in tracked_tokens:
                try:
                    balance_data = market.balance(token)
                    # balance_data is TokenBalance with .balance attribute
                    if balance_data.balance > 0:
                        price = market.price(token)
                        value_usd = balance_data.balance * price
                        wallet_value += value_usd
                        wallet_balances.append(
                            TokenBalance(
                                symbol=token,
                                balance=balance_data.balance,
                                value_usd=value_usd,
                                price_usd=price,
                            )
                        )
                except Exception as e:  # noqa: BLE001  # Intentional graceful degradation
                    logger.debug(f"Could not get balance/price for {token}: {e}")
                    continue

            # VIB-3937 / VIB-4225 (ACC-02) — append the chain's NATIVE gas-token
            # to wallet_balances. Strategy stays fail-open: the runner inspects
            # ``snapshot_metadata["gas_native_status"]`` after this returns and
            # raises ``AccountingPersistenceError("snapshot", ...)`` in live
            # mode if the status is non-ok / non-already_tracked.
            gas_native_status, native_value_usd = self._append_native_gas_to_wallet(market, wallet_balances)
            wallet_value += native_value_usd

            return PortfolioSnapshot(
                timestamp=datetime.now(UTC),
                deployment_id=self.deployment_id,
                total_value_usd=position_value + wallet_value,
                available_cash_usd=wallet_value,
                value_confidence=ValueConfidence.ESTIMATED if positions_unavailable else ValueConfidence.HIGH,
                positions=positions,
                wallet_balances=wallet_balances,
                chain=self._chain,
                snapshot_metadata={"gas_native_status": gas_native_status},
            )

        except Exception as e:  # noqa: BLE001  # Intentional graceful degradation
            # Graceful degradation - return unavailable instead of $0
            logger.warning(f"Failed to compute portfolio snapshot: {e}")
            return PortfolioSnapshot(
                timestamp=datetime.now(UTC),
                deployment_id=self.deployment_id,
                total_value_usd=Decimal("0"),
                available_cash_usd=Decimal("0"),
                value_confidence=ValueConfidence.UNAVAILABLE,
                error=str(e),
                chain=self._chain,
            )

    def _append_native_gas_to_wallet(
        self,
        market: "MarketSnapshot",
        wallet_balances: list,
    ) -> tuple[str, Decimal]:
        """Append the chain's native gas token to ``wallet_balances`` in place.

        Returns ``(gas_native_status, native_value_usd_added)``. Status enum:

        - ``"ok"``: native row appended successfully.
        - ``"already_tracked"``: tracked-tokens list already includes the
          native (case-insensitive); no duplicate appended.
        - ``"unknown_chain"`` (F1): ``native_token_for_chain`` returned None.
        - ``"balance_failed"`` (F2): ``market.balance(native)`` raised.
        - ``"price_missing"`` (F3): ``market.price(native)`` raised or returned None.

        The method NEVER raises (fail-open contract for paper / dry_run mode);
        the runner-level enforcer at ``runner_state._enforce_native_gas_status_in_live``
        inspects the typed status afterward and raises in live mode on
        non-ok / non-already_tracked statuses.
        """
        from ..portfolio.models import TokenBalance

        try:
            from ..accounting.gas_pricing import native_token_for_chain

            native_symbol = native_token_for_chain(self._chain or "")
        except Exception as e:  # noqa: BLE001 — typed status path
            logger.debug(f"native gas-token chain resolve failed: {e}")
            return ("unknown_chain", Decimal("0"))

        if not native_symbol:
            return ("unknown_chain", Decimal("0"))

        # CodeRabbit 2026-05-04: case-insensitive dedupe so a tracked token
        # that already includes the native symbol in any casing ("eth", "ETH",
        # "Eth") doesn't double-add and overstate cash.
        native_symbol_canon = native_symbol.upper()
        if any((b.symbol or "").upper() == native_symbol_canon for b in wallet_balances):
            return ("already_tracked", Decimal("0"))

        # ``native_token_for_chain`` defaults to "ETH" for an empty/None chain,
        # so a falsy ``self._chain`` slips past the ``if not native_symbol``
        # guard above. Reading balance/price with ``chain=None`` on a
        # multi-chain snapshot would then raise ``AmbiguousChainError`` and be
        # misclassified as ``balance_failed`` (a hard live-mode halt). Treat an
        # unresolved chain as ``unknown_chain`` instead.
        if not self._chain:
            return ("unknown_chain", Decimal("0"))

        try:
            native_balance_data = market.balance(native_symbol, chain=self._chain)
        except Exception as e:  # noqa: BLE001 — typed status path
            logger.debug(f"native gas-token balance fetch failed: {e}")
            return ("balance_failed", Decimal("0"))

        try:
            native_price = market.price(native_symbol, chain=self._chain)
        except Exception as e:  # noqa: BLE001 — typed status path
            logger.debug(f"native gas-token price fetch failed: {e}")
            return ("price_missing", Decimal("0"))

        if native_price is None:
            return ("price_missing", Decimal("0"))

        # Defensive: a malformed market stub may return None or an object
        # without a numeric `.balance` attribute. Without this guard, the
        # outer snapshot-wide handler would convert the entire snapshot to
        # UNAVAILABLE and lose the typed gas_native_status trail this helper
        # is designed to preserve (CodeRabbit major #5).
        try:
            native_balance = native_balance_data.balance
            native_value_usd = native_balance * native_price
        except Exception as e:  # noqa: BLE001 — keep helper fail-open
            logger.debug(f"native gas-token balance shape invalid: {e}")
            return ("balance_failed", Decimal("0"))
        wallet_balances.append(
            TokenBalance(
                symbol=native_symbol_canon,
                balance=native_balance,
                value_usd=native_value_usd,
                price_usd=native_price,
            )
        )
        return ("ok", native_value_usd)

    def _get_tracked_tokens(self) -> list[str]:
        """Get list of tokens to track for wallet balance.

        Auto-derives tokens from the strategy's config by scanning for
        token-related fields (pool, base_token, collateral_token, etc.).

        Override to specify tokens manually if the auto-detection doesn't
        cover your use case.

        Returns:
            List of token symbols to track
        """
        tokens = self._derive_tokens_from_config()
        if tokens:
            return tokens
        # Fallback only if no tokens could be derived from config
        return ["USDC", "WETH"]

    def _derive_tokens_from_config(self) -> list[str]:
        """Extract token symbols from strategy config fields.

        Scans config for common token-related field names and extracts
        symbols from their values. Handles both direct symbol fields
        (e.g., base_token="WETH") and pool format fields
        (e.g., pool="WETH/USDC/500", "WETH/USDC/volatile").

        Returns:
            Deduplicated list of token symbols, or empty list if none found.
        """
        # Lazy import to avoid pulling in the full runner package at
        # strategies/ import time (strategies/__init__.py is loaded eagerly
        # by the strategy auto-discovery pipeline).
        from ..runner.token_extraction import is_fiat_quote_symbol, parse_pool_tokens

        config = self.config
        if config is None:
            return []

        # Field names that contain token symbols directly
        _TOKEN_FIELDS = {
            "base_token",
            "quote_token",
            "collateral_token",
            "borrow_token",
            "from_token",
            "to_token",
            "token_in",
            "token_out",
            "token",
            "token0",
            "token1",
            "base_token_symbol",
        }

        # Field names whose value is a slash-separated pool descriptor
        # like "WETH/USDC/500", "WETH/USDC/volatile", or "WETH/USDC"
        _POOL_FIELDS = {"pool", "pair", "market"}

        seen: set[str] = set()
        tokens: list[str] = []

        config_dict: dict = {}
        if hasattr(config, "to_dict"):
            try:
                config_dict = config.to_dict()
            except Exception as e:  # noqa: BLE001  # Intentional: config types are user-provided
                logger.debug(f"config.to_dict() failed, trying fallback: {e}")
        if not config_dict and hasattr(config, "__dataclass_fields__"):
            from dataclasses import asdict

            try:
                config_dict = asdict(config)
            except Exception as e:  # noqa: BLE001  # Intentional: config types are user-provided
                logger.debug(f"dataclasses.asdict() failed, trying fallback: {e}")
        if not config_dict and hasattr(config, "__dict__"):
            config_dict = {k: v for k, v in config.__dict__.items() if not k.startswith("_")}

        for key, value in config_dict.items():
            if not isinstance(value, str) or not value:
                continue

            if key in _POOL_FIELDS:
                # Delegate pool parsing to the canonical helper so the
                # trailing pool-type-suffix filter (volatile/stable/
                # concentrated/cl) stays in one place. Bare strings like
                # "usdc_e" have no "/" and return [] — correctly skipped.
                for symbol in parse_pool_tokens(value):
                    if symbol not in seen:
                        seen.add(symbol)
                        tokens.append(symbol)
            elif key in _TOKEN_FIELDS:
                symbol = value.strip()
                # Fiat quote symbols (e.g., quote_token="USD") name an
                # accounting unit, not an on-chain token — skip them so the
                # tracked-tokens loop doesn't try balance/price lookups that
                # always fail (no ERC20, no USD/USD Chainlink feed).
                if symbol and symbol not in seen and not is_fiat_quote_symbol(symbol):
                    seen.add(symbol)
                    tokens.append(symbol)

        return tokens

    @abstractmethod
    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get all open positions for this strategy.

        MUST query on-chain state - do not use cached state for safety.
        Called during teardown preview and execution to determine what
        positions need to be closed.

        For strategies with no positions, use StatelessStrategy as your base
        class, or return TeardownPositionSummary.empty(self.deployment_id).

        Returns:
            TeardownPositionSummary with all current positions

        Example:
            from almanak.framework.teardown import TeardownPositionSummary, PositionInfo, PositionType

            def get_open_positions(self) -> TeardownPositionSummary:
                positions = []

                # Query on-chain LP position
                lp_data = self._query_lp_position()
                if lp_data:
                    positions.append(PositionInfo(
                        position_type=PositionType.LP,
                        position_id=lp_data["token_id"],
                        chain=self.chain,
                        protocol="uniswap_v3",
                        value_usd=Decimal(str(lp_data["value_usd"])),
                    ))

                return TeardownPositionSummary(
                    deployment_id=self.deployment_id,
                    timestamp=datetime.now(timezone.utc),
                    positions=positions,
                )
        """
        ...

    @abstractmethod
    def generate_teardown_intents(self, mode: "TeardownMode", market: "MarketSnapshot | None" = None) -> list[Intent]:
        """Generate intents to close all positions.

        Return intents in the correct execution order:
        1. PERP - Close perpetuals first (highest liquidation risk)
        2. BORROW - Repay borrowed amounts (frees collateral)
        3. SUPPLY - Withdraw supplied collateral
        4. LP - Close LP positions and collect fees
        5. TOKEN - Swap all tokens to target token (USDC)

        For strategies with no positions, use StatelessStrategy as your base
        class, or return an empty list.

        Args:
            mode: TeardownMode.SOFT (graceful) or TeardownMode.HARD (emergency)
            market: Optional market snapshot with real prices. When called from the
                runner, this is the same snapshot used for normal decide() iterations.
                May be None for backward compatibility or when called outside the runner.

        Returns:
            List of intents to execute in order

        Example:
            from almanak.framework.teardown import TeardownMode

            def generate_teardown_intents(self, mode: TeardownMode, market=None) -> list[Intent]:
                intents = []

                # Get current positions
                positions = self.get_open_positions()

                # Use market data if available for smarter teardown
                if market:
                    eth_price = market.price("ETH")

                # Close LP position first
                for pos in positions.positions_by_type(PositionType.LP):
                    intents.append(Intent.lp_close(
                        position_id=pos.position_id,
                        pool=pos.details.get("pool"),
                        collect_fees=True,
                        protocol="uniswap_v3",
                    ))

                # Swap remaining tokens to USDC
                intents.append(Intent.swap(
                    from_token="WETH",
                    to_token="USDC",
                    amount=Decimal("0"),  # All remaining
                    swap_all=True,
                ))

                return intents
        """
        ...

    def on_teardown_started(self, mode: "TeardownMode") -> None:
        """Hook called when teardown starts.

        Override to perform any setup before teardown begins.
        This is called after the cancel window expires.

        Args:
            mode: The teardown mode (SOFT or HARD)

        Example:
            def on_teardown_started(self, mode: TeardownMode) -> None:
                logger.info(f"Teardown starting in {mode.value} mode")
                self._pause_monitoring()
        """
        pass

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        """Hook called when teardown completes.

        Override to perform cleanup after teardown.

        Args:
            success: Whether all positions were closed successfully
            recovered_usd: Total USD value recovered

        Example:
            def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
                if success:
                    logger.info(f"Teardown complete. Recovered ${recovered_usd:,.2f}")
                else:
                    logger.error("Teardown failed - manual intervention required")
        """
        pass

    def get_teardown_profile(self) -> "TeardownProfile":
        """Get teardown profile metadata for UX display.

        Override to provide better information about teardown expectations.
        This helps the dashboard show more accurate previews.

        Returns:
            TeardownProfile with strategy-specific metadata

        Example:
            from almanak.framework.teardown import TeardownProfile

            def get_teardown_profile(self) -> TeardownProfile:
                return TeardownProfile(
                    natural_exit_assets=["WETH", "USDC"],
                    original_entry_assets=["USDC"],
                    recommended_target="USDC",
                    estimated_steps=3,
                    chains_involved=[self.chain],
                    has_lp_positions=True,
                )
        """
        from almanak.framework.teardown import TeardownProfile

        # Default profile based on what we can determine
        return TeardownProfile(
            natural_exit_assets=[],
            original_entry_assets=[],
            recommended_target="USDC",
            estimated_steps=2,
            chains_involved=[self._chain],
        )

    def _check_teardown_request(self) -> Optional["TeardownRequest"]:
        """Check if there's a pending teardown request for this strategy.

        Called at the start of each iteration by the runner.
        Returns the request if one exists and is active.

        Local mode reads the strategy SQLite DB. Hosted mode routes through
        the gateway so the strategy process never needs database credentials.

        Returns:
            TeardownRequest if one exists and is active, None otherwise
        """
        # ALM-2705: narrow exception handling so a *missing-table* / init-failure
        # OperationalError surfaces loudly. The pre-fix behaviour of swallowing
        # every Exception under a single warning hid two genuinely-bug-class
        # failures (singleton constructed but ``_init_db`` lost a WAL race; DB
        # path resolved to a file the gateway pre-created without the
        # ``teardown_requests`` schema) as if they were the benign "no teardown
        # row exists" path. Per CLAUDE.md "Teardown lane accounting boundary":
        # init failures must be loud + durable, but must not block the runner —
        # halting on an init failure here would prevent the strategy from
        # running its next risk-reducing iteration.
        import sqlite3

        from almanak.framework.deployment import is_hosted
        from almanak.framework.local_paths import LocalPathError

        deployment_id = self._require_deployment_id("_check_teardown_request")
        try:
            from almanak.framework.teardown import get_teardown_state_manager_for_runtime

            manager = get_teardown_state_manager_for_runtime(gateway_client=getattr(self, "_gateway_client", None))

            request = manager.get_active_request(deployment_id)
            if request:
                logger.info(
                    f"Found active teardown request for {deployment_id}: "
                    f"mode={request.mode.value}, status={request.status.value}"
                )
            return request

        except LocalPathError as e:
            if is_hosted():
                raise
            # Benign: no strategy folder resolved (e.g. running in an
            # environment without ALMANAK_STRATEGY_FOLDER and no cwd hint).
            # Local-mode only. Log debug and skip.
            logger.debug("Skipping teardown request check (no local strategy DB): %s", e)
            return None

        except sqlite3.OperationalError as e:
            if is_hosted():
                raise
            # Loud + durable: this is the ALM-2705 failure mode. Either the
            # singleton's ``_init_db`` lost a contention race (now retried —
            # see ``SQLiteTeardownStateManager._init_db``) or the DB file's
            # schema is genuinely missing the ``teardown_requests`` table
            # (e.g. a gateway-pre-created DB the runner never bootstrapped).
            # Emit a grep-able structured log line and return None so the
            # runner keeps making risk-reducing progress.
            logger.error(
                "teardown.check_request_failed: deployment_id=%s strategy_class=%s error=%s — "
                "teardown signal channel is degraded; runner continuing without "
                "teardown polling this iteration",
                deployment_id,
                self.__class__.__name__,
                e,
            )
            return None

        except Exception as e:  # noqa: BLE001 — catch-all for unexpected channel failures
            if is_hosted():
                logger.error(
                    "teardown.check_request_unexpected_error: type=%s error=%s",
                    type(e).__name__,
                    e,
                )
                raise
            # Genuinely unexpected: not a path problem, not a DB schema/lock
            # issue. Keep the runner alive (same rationale as above) but log
            # at WARNING with the exception type so future incidents are
            # triagable from logs alone.
            logger.warning(
                "teardown.check_request_unexpected_error: type=%s error=%s",
                type(e).__name__,
                e,
            )
            return None

    def acknowledge_teardown_request(self) -> bool:
        """Acknowledge a pending teardown request.

        Called when the strategy picks up a teardown request and starts
        processing it. Local mode writes the strategy SQLite DB; hosted mode
        routes through the gateway.

        Returns:
            True if request was acknowledged, False otherwise
        """
        # ALM-2705: same narrow-exception treatment as ``_check_teardown_request``.
        import sqlite3

        from almanak.framework.deployment import is_hosted
        from almanak.framework.local_paths import LocalPathError

        deployment_id = self._require_deployment_id("acknowledge_teardown_request")
        try:
            from almanak.framework.teardown import get_teardown_state_manager_for_runtime

            manager = get_teardown_state_manager_for_runtime(gateway_client=getattr(self, "_gateway_client", None))

            request = manager.acknowledge_request(deployment_id)
            return request is not None

        except LocalPathError as e:
            if is_hosted():
                raise
            logger.debug("Skipping teardown ack (no local strategy DB): %s", e)
            return False

        except sqlite3.OperationalError as e:
            if is_hosted():
                raise
            logger.error(
                "teardown.ack_request_failed: deployment_id=%s strategy_class=%s error=%s — "
                "teardown signal channel is degraded; ack skipped this iteration",
                deployment_id,
                self.__class__.__name__,
                e,
            )
            return False

        except Exception as e:  # noqa: BLE001
            if is_hosted():
                logger.error(
                    "teardown.ack_request_unexpected_error: type=%s error=%s",
                    type(e).__name__,
                    e,
                )
                raise
            logger.warning(
                "teardown.ack_request_unexpected_error: type=%s error=%s",
                type(e).__name__,
                e,
            )
            return False

    def should_teardown(self) -> bool:
        """Check if the strategy should enter teardown mode.

        Checks for:
        1. Pending teardown request (from CLI, dashboard, config)
        2. Auto-protect triggers (health factor, loss limits)

        Returns:
            True if teardown should be initiated
        """
        # Check for explicit teardown request
        request = self._check_teardown_request()
        if request:
            return True

        # Check auto-protect triggers (if enabled)
        # These could be implemented by subclasses or checked here
        return False

    def on_sadflow_exit(self, success: bool, total_attempts: int) -> None:
        """Hook called when exiting sadflow (on completion or final failure).

        Override this method to perform cleanup or logging after sadflow resolution.
        This is called once when the intent completes (success or failure) after
        having been in sadflow.

        Args:
            success: Whether the intent eventually succeeded after retries.
            total_attempts: Total number of attempts made (including the final one).

        Example:
            def on_sadflow_exit(self, success, total_attempts):
                if success:
                    logger.info(f"Recovered after {total_attempts} attempts")
                else:
                    logger.error(f"Failed after {total_attempts} attempts")
                    self.notify_operator("Intent failed after all retries")
        """
        pass

    def on_retry(
        self,
        context: SadflowContext,
        action: SadflowAction,
    ) -> SadflowAction:
        """Hook called before each retry attempt.

        Override this method to customize individual retry behavior. This is
        called before each retry, after the initial on_sadflow_enter call.

        Args:
            context: SadflowContext with current error details and state.
            action: The default SadflowAction (RETRY with calculated delay).

        Returns:
            SadflowAction: The action to take. Return the input action unchanged
            for default behavior, or return a modified action:
            - SadflowAction.retry(custom_delay=5.0): Retry with custom delay
            - SadflowAction.abort(reason): Stop retrying and fail
            - SadflowAction.modify(bundle): Retry with modified ActionBundle
            - SadflowAction.skip(reason): Skip and mark as completed

        Example:
            def on_retry(self, context, action):
                # After 2 attempts, try with higher gas
                if context.attempt_number > 2 and context.action_bundle:
                    modified = self._increase_gas(context.action_bundle)
                    return SadflowAction.modify(modified)

                # Abort if we've been retrying too long
                if context.total_duration_seconds > 120:
                    return SadflowAction.abort("Retry timeout exceeded")

                # Use default retry
                return action
        """
        return action


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    # Market Snapshot
    "MarketSnapshot",
    "TokenBalance",
    "PriceData",
    "RSIData",
    "PriceOracle",
    "RSIProvider",
    "BalanceProvider",
    # Indicator Models
    "MACDData",
    "BollingerBandsData",
    "StochasticData",
    "ATRData",
    "MAData",
    "ADXData",
    "OBVData",
    "CCIData",
    "IchimokuData",
    "IndicatorProvider",
    "DEFAULT_TIMEFRAME",
    # Multi-Chain Market Snapshot
    "MultiChainMarketSnapshot",
    "MultiChainPriceOracle",
    "MultiChainBalanceProvider",
    "ChainNotConfiguredError",
    # Chain Health
    "ChainHealth",
    "ChainHealthStatus",
    "StaleDataError",
    "DataFreshnessPolicy",
    # Protocol Health Metric Providers
    "AaveHealthFactorProvider",
    "AaveAvailableBorrowProvider",
    "GmxAvailableLiquidityProvider",
    "GmxFundingRateProvider",
    # Sadflow Hooks
    "SadflowAction",
    "SadflowActionType",
    "SadflowContext",
    # Strategy
    "IntentStrategy",
    "ExecutionResult",
    # Decorator
    "almanak_strategy",
    "StrategyDataRequirements",
    "LEGACY_COMPAT_DATA_REQUIREMENTS",
    "StrategyMetadata",
    "StrategyClassT",
]
