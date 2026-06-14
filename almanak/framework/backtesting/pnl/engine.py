"""PnL Backtesting Engine - Historical simulation for strategy evaluation.

This module provides the main PnLBacktester engine that orchestrates historical
simulation of trading strategies. It uses historical price data to evaluate
strategy performance without executing real transactions.

Key Components:
    - PnLBacktester: Main backtesting engine class
    - BacktestableStrategy: Protocol for strategies compatible with backtesting

Examples:
    Basic usage with default settings:

        from almanak.framework.backtesting.pnl import PnLBacktester, PnLBacktestConfig
        from almanak.framework.backtesting.pnl.providers import CoinGeckoDataProvider

        # Create data provider
        data_provider = CoinGeckoDataProvider()

        # Create fee/slippage models
        fee_models = {"default": DefaultFeeModel()}
        slippage_models = {"default": DefaultSlippageModel()}

        # Create backtester
        backtester = PnLBacktester(data_provider, fee_models, slippage_models)

        # Run backtest
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            initial_capital_usd=Decimal("10000"),
        )
        result = await backtester.backtest(strategy, config)
        print(result.summary())

    Institutional mode for production-grade compliance:

        # Institutional mode enforces strict data quality, reproducibility,
        # and compliance requirements for institutional trading operations.
        config = PnLBacktestConfig(
            start_time=datetime(2024, 1, 1),
            end_time=datetime(2024, 6, 1),
            initial_capital_usd=Decimal("1000000"),
            institutional_mode=True,  # Enables strict requirements
            random_seed=42,  # Required for reproducibility
        )
        result = await backtester.backtest(strategy, config)

        # Check institutional compliance
        if result.institutional_compliance:
            print("Backtest meets institutional standards")
        else:
            print(f"Compliance violations: {result.compliance_violations}")

        # Access data quality metrics
        if result.data_quality:
            print(f"Data coverage: {result.data_quality.coverage_ratio:.1%}")
            print(f"Sources used: {result.data_quality.source_breakdown}")
"""

import logging
import uuid
from dataclasses import dataclass

# Note: There's a naming conflict between fee_models.py (module) and fee_models/ (package)
# Python prefers the package, so we need to use an alternative import approach
# We define the protocols inline and create simple default implementations
from dataclasses import dataclass as _dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Protocol, runtime_checkable

from almanak.core.chains import DEFAULT_CHAIN
from almanak.framework.backtesting.adapters.base import StrategyBacktestAdapter

# Import adapter registry for strategy type detection
from almanak.framework.backtesting.adapters.registry import (
    StrategyTypeHint,
    detect_strategy_type,
    get_adapter_for_strategy_with_config,
)
from almanak.framework.backtesting.config import BacktestDataConfig
from almanak.framework.backtesting.exceptions import DataSourceUnavailableError
from almanak.framework.backtesting.models import (
    BacktestMetrics,
    BacktestResult,
    GasPriceRecord,
    GasPriceSummary,
    IntentType,
    ParameterSource,
    ParameterSourceTracker,
    PreflightCheckResult,
    PreflightReport,
    TradeRecord,
)

# Phase helpers extracted from _run_backtest (Phase 6C.2).
from almanak.framework.backtesting.pnl import _engine_helpers
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import (
    HistoricalDataCapability,
    HistoricalDataProvider,
    MarketState,
)
from almanak.framework.backtesting.pnl.data_quality import DataQualityTracker
from almanak.framework.backtesting.pnl.error_handling import (
    BacktestErrorHandler,
)
from almanak.framework.backtesting.pnl.indicator_engine import BacktestIndicatorEngine
from almanak.framework.backtesting.pnl.logging_utils import (
    BacktestLogger,
    log_trade_execution,
)
from almanak.framework.backtesting.pnl.mev_simulator import (
    MEVSimulator,
    MEVSimulatorConfig,
)
from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio, SimulatedPosition
from almanak.framework.backtesting.pnl.providers.gas import GasPrice, GasPriceProvider
from almanak.framework.backtesting.pnl.simulated_result import (
    SimulatedExecutionResult,
    build_simulated_result,
)

# Import strategy-related types
from almanak.framework.market import MarketSnapshot

logger = logging.getLogger(__name__)


# =============================================================================
# Fee and Slippage Model Protocols (duplicated due to import conflict)
# =============================================================================
# Note: These are duplicated from fee_models.py because there's a naming
# conflict with the fee_models/ package. Python prefers packages over modules.


@runtime_checkable
class FeeModel(Protocol):
    """Protocol for calculating protocol/exchange fees."""

    def calculate_fee(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate the fee for an action."""
        ...

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        ...


@runtime_checkable
class SlippageModel(Protocol):
    """Protocol for estimating trade slippage."""

    def calculate_slippage(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate the slippage for an action."""
        ...

    @property
    def model_name(self) -> str:
        """Return the unique name of this slippage model."""
        ...


# Zero-fee intent types
_ZERO_FEE_INTENTS: frozenset[IntentType] = frozenset(
    {
        IntentType.HOLD,
    }
)

# Zero-slippage intent types
_ZERO_SLIPPAGE_INTENTS: frozenset[IntentType] = frozenset(
    {
        IntentType.HOLD,
        IntentType.SUPPLY,
        IntentType.WITHDRAW,
        IntentType.REPAY,
        IntentType.BORROW,
        IntentType.VAULT_DEPOSIT,
        IntentType.VAULT_REDEEM,
    }
)


@_dataclass
class DefaultFeeModel:
    """Default fee model with configurable percentage-based fees."""

    fee_pct: Decimal = Decimal("0.003")  # 0.3% default

    def calculate_fee(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate fee using configured percentages."""
        if intent_type in _ZERO_FEE_INTENTS:
            return Decimal("0")
        return amount_usd * self.fee_pct

    @property
    def model_name(self) -> str:
        """Return the unique name of this fee model."""
        return "default"


@_dataclass
class DefaultSlippageModel:
    """Default slippage model with configurable percentage-based slippage."""

    slippage_pct: Decimal = Decimal("0.001")  # 0.1% base
    max_slippage_pct: Decimal = Decimal("0.05")  # 5% max

    def calculate_slippage(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate slippage."""
        if intent_type in _ZERO_SLIPPAGE_INTENTS:
            return Decimal("0")
        return min(self.slippage_pct, self.max_slippage_pct)

    @property
    def model_name(self) -> str:
        """Return the unique name of this slippage model."""
        return "default"


@_dataclass
class LinearImpactSlippageModel:
    """Depth-aware slippage model with linear market impact scaling.

    Models slippage as a fixed base spread plus a market-impact term that
    grows linearly with trade size. This captures the intuition that large
    trades move the price more than small ones.

    Formula (in basis points)::

        slippage_bps = base_bps + impact_bps_per_million * (amount_usd / 1_000_000)
        slippage_pct = min(slippage_bps / 10_000, max_slippage_pct)

    Default parameters model a mid-tier DEX (e.g., Uniswap V3 on Arbitrum):
    - ``base_bps = 10`` — 0.10% fixed spread (bid-ask + rounding)
    - ``impact_bps_per_million = 5`` — 0.05% extra per $1 M notional
    - ``max_slippage_pct = 0.05`` — hard cap at 5%

    A $100 k trade incurs ~0.105%; a $1 M trade incurs ~0.15%; a $10 M trade
    incurs ~0.60% (still below the 5% cap).

    Per-protocol calibration examples:

    .. code-block:: python

        # Uniswap V3 ETH/USDC 0.05% pool (deep, low impact)
        slippage_models = {
            "uniswap_v3": LinearImpactSlippageModel(
                base_bps=Decimal("5"),
                impact_bps_per_million=Decimal("2"),
            ),
            # Smaller mid-cap DEX (shallower liquidity)
            "traderjoe_v2": LinearImpactSlippageModel(
                base_bps=Decimal("20"),
                impact_bps_per_million=Decimal("15"),
            ),
            "default": LinearImpactSlippageModel(),
        }

    Attributes:
        base_bps: Fixed slippage component in basis points (1 bps = 0.01%).
            Represents the bid-ask spread and routing overhead.
        impact_bps_per_million: Market-impact coefficient in bps per $1 M.
            Controls how steeply slippage grows with trade size.
        max_slippage_pct: Hard cap on slippage as a fraction (e.g., 0.05 = 5%).
    """

    base_bps: Decimal = Decimal("10")  # 0.10% base spread
    impact_bps_per_million: Decimal = Decimal("5")  # 0.05% per $1 M
    max_slippage_pct: Decimal = Decimal("0.05")  # 5% hard cap

    _BPS_DIVISOR: Decimal = Decimal("10000")
    _MILLION: Decimal = Decimal("1000000")

    def __post_init__(self) -> None:
        """Validate model parameters on initialization."""
        if self.base_bps < 0:
            raise ValueError("base_bps cannot be negative.")
        if self.impact_bps_per_million < 0:
            raise ValueError("impact_bps_per_million cannot be negative.")
        if not (0 <= self.max_slippage_pct <= 1):
            raise ValueError("max_slippage_pct must be a fraction between 0 and 1.")

    def calculate_slippage(
        self,
        intent_type: IntentType,
        amount_usd: Decimal,
        market_state: MarketState,
        protocol: str = "",
        **kwargs: Any,
    ) -> Decimal:
        """Calculate depth-aware slippage.

        Returns zero for intent types that don't incur price impact
        (e.g., HOLD, SUPPLY, WITHDRAW). For all other intents, applies
        the linear impact formula.

        Args:
            intent_type: Type of intent being executed.
            amount_usd: Trade notional in USD.
            market_state: Current market data (unused by this model but
                required by the SlippageModel protocol).
            protocol: Protocol name (unused; use per-protocol model keys instead).
            **kwargs: Ignored extra arguments.

        Returns:
            Slippage as a fraction (e.g., 0.001 = 0.1%).
        """
        if intent_type in _ZERO_SLIPPAGE_INTENTS:
            return Decimal("0")

        # Guard against negative amounts (shouldn't happen, but be defensive)
        safe_amount = max(amount_usd, Decimal("0"))
        impact_bps = self.impact_bps_per_million * (safe_amount / self._MILLION)
        total_bps = self.base_bps + impact_bps
        slippage_pct = total_bps / self._BPS_DIVISOR
        return min(slippage_pct, self.max_slippage_pct)

    @property
    def model_name(self) -> str:
        """Return the unique name of this slippage model."""
        return "linear_impact"


# =============================================================================
# Protocol for Backtest-Compatible Strategies
# =============================================================================


@runtime_checkable
class BacktestableStrategy(Protocol):
    """Protocol defining the interface for strategies that can be backtested.

    Strategies must implement:
    - deployment_id: Unique identifier for the strategy
    - decide(market): Method that returns an intent based on market data

    The decide method can return:
    - An Intent object (SwapIntent, LPIntent, etc.)
    - None (equivalent to HOLD)
    - A DecideResult (for IntentStrategy compatibility)
    """

    @property
    def deployment_id(self) -> str:
        """Return the unique identifier for this strategy."""
        ...

    def decide(self, market: MarketSnapshot) -> Any:
        """Make a trading decision based on current market state.

        Args:
            market: MarketSnapshot containing current prices, balances, indicators

        Returns:
            An Intent object, None (hold), or DecideResult
        """
        ...


# =============================================================================
# Market Snapshot Factory
# =============================================================================


def create_market_snapshot_from_state(
    market_state: MarketState,
    chain: str = DEFAULT_CHAIN,
    wallet_address: str = "",
    portfolio: SimulatedPortfolio | None = None,
) -> MarketSnapshot:
    """Create a MarketSnapshot from historical MarketState data.

    This function bridges the historical data provider's MarketState format
    to the MarketSnapshot format expected by strategies' decide() methods.

    Args:
        market_state: Historical market state from data provider
        chain: Chain identifier
        wallet_address: Wallet address (can be empty for simulation)
        portfolio: Optional portfolio for balance simulation

    Returns:
        MarketSnapshot populated with historical price data
    """
    # Create market snapshot with historical timestamp
    snapshot = MarketSnapshot(
        chain=chain,
        wallet_address=wallet_address,
        timestamp=market_state.timestamp,
    )

    # Populate prices from market state
    for token in market_state.available_tokens:
        try:
            price = market_state.get_price(token)
            snapshot.set_price(token, price)
        except KeyError:
            pass

    # If we have a portfolio, simulate balances
    if portfolio:
        from almanak.framework.market import TokenBalance

        # Add token balances from portfolio
        for token, amount in portfolio.tokens.items():
            try:
                price = market_state.get_price(token)
                balance_data = TokenBalance(
                    symbol=token,
                    balance=amount,
                    balance_usd=amount * price,
                )
                snapshot.set_balance(token, balance_data)
            except KeyError:
                # If price not available, use 0 for balance_usd
                balance_data = TokenBalance(
                    symbol=token,
                    balance=amount,
                    balance_usd=Decimal("0"),
                )
                snapshot.set_balance(token, balance_data)

        # Add cash as USD balance
        cash_balance = TokenBalance(
            symbol="USD",
            balance=portfolio.cash_usd,
            balance_usd=portfolio.cash_usd,
        )
        snapshot.set_balance("USD", cash_balance)

        # Expose cash under stablecoin symbols so strategies calling
        # market.balance("USDC") get the cash balance instead of ValueError.
        # Must stay in lockstep with the apply_fill cash model: these are the
        # symbols whose outflows debit cash_usd.
        from almanak.framework.backtesting.pnl.portfolio import CASH_EQUIVALENT_STABLECOINS

        stablecoin_aliases = CASH_EQUIVALENT_STABLECOINS
        for stable in stablecoin_aliases:
            if stable not in portfolio.tokens:
                stable_balance = TokenBalance(
                    symbol=stable,
                    balance=portfolio.cash_usd,
                    balance_usd=portfolio.cash_usd,
                )
                snapshot.set_balance(stable, stable_balance)

        # Expose zero balances for tracked tokens not in the portfolio
        # so strategies calling market.balance("WETH") get zero instead of ValueError
        for token in market_state.available_tokens:
            if token not in portfolio.tokens and token not in stablecoin_aliases and token != "USD":
                zero_balance = TokenBalance(
                    symbol=token,
                    balance=Decimal("0"),
                    balance_usd=Decimal("0"),
                )
                snapshot.set_balance(token, zero_balance)

    return snapshot


# =============================================================================
# Data Quality Tracker
# =============================================================================


# DataQualityTracker extracted to data_quality.py (imported at top of file)


# =============================================================================
# PnL Backtester Engine
# =============================================================================


@dataclass(frozen=True)
class _CloseResolution:
    """How a closing intent maps onto the portfolio's open positions.

    Produced by ``PnLBacktester._resolve_position_close``: exactly one of
    ``position_close_id`` (full close) or ``position_reduce_id`` (partial
    reduction, e.g. a partial lending WITHDRAW) is set on success; both are
    None with ``failure_reason`` set when a close-type intent matches no
    open position and the fill must be rejected (crediting the inflow
    without removing the position would mint value).

    Attributes:
        amount_usd: Effective notional for fees, slippage, and token flows.
            Full lending closes resolve this to principal + accrued
            interest so the close credit is value-neutral at the close
            instant (mirroring the perp close credit).
        position_close_id: Position to close in full, if any.
        position_reduce_id: Position to partially reduce, if any.
        interest_usd: Accrued interest realized by a lending close OR a
            boundary partial reduce (amount covers all principal + part of the
            accrued interest). Recorded as the trade's realized PnL via
            metadata. Positive for a WITHDRAW (interest earned), NEGATIVE for a
            REPAY (borrow interest owed -- VIB-5098).
        reduce_amounts: Explicit per-token principal to remove on a partial
            reduce. Set only for a BOUNDARY reduce (``interest_usd`` also set):
            the fill's flow covers principal + part of the accrued interest, so
            the principal to remove (the position's full held principal) is
            SMALLER than the flow. None means an ordinary sub-principal partial
            whose reduce debits the full flow (engine derives it from
            tokens_in / tokens_out). Only meaningful with ``position_reduce_id``.
        failure_reason: Set when the fill must be rejected unapplied.
    """

    amount_usd: Decimal
    position_close_id: str | None = None
    position_reduce_id: str | None = None
    interest_usd: Decimal = Decimal("0")
    reduce_amounts: dict[str, Decimal] | None = None
    failure_reason: str | None = None


@dataclass
class PnLBacktester:
    """Main PnL backtesting engine for historical strategy simulation.

    The PnLBacktester simulates strategy execution against historical price data
    to evaluate performance. It:

    1. Iterates through historical market data at configured intervals
    2. Calls strategy.decide() with a MarketSnapshot for each time step
    3. Simulates intent execution with configurable fee/slippage models
    4. Tracks portfolio state and builds an equity curve
    5. Calculates comprehensive performance metrics

    Attributes:
        data_provider: Historical data provider (e.g., CoinGeckoDataProvider)
        fee_models: Dict mapping protocol -> FeeModel (or "default" for all)
        slippage_models: Dict mapping protocol -> SlippageModel
        gas_provider: Optional gas price provider for historical gas prices.
            When provided and config.use_historical_gas_gwei=True, the engine
            will fetch historical gas prices at each simulation timestamp.
        mev_simulator: Optional MEV simulator (created dynamically based on config)
        strategy_type: Optional explicit strategy type for adapter selection.
            If "auto" (default), the type is detected from strategy metadata.
            Valid values: "lp", "perp", "lending", "arbitrage", "swap", "auto", or None.
        data_config: Optional BacktestDataConfig for controlling historical data
            providers in adapters. When provided, adapters will use historical
            volume, funding rates, and APY data from real sources instead of
            fallback values.

    Example:
        backtester = PnLBacktester(
            data_provider=CoinGeckoDataProvider(),
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
        )

        result = await backtester.backtest(my_strategy, config)
        print(result.summary())

        # With explicit strategy type:
        backtester = PnLBacktester(
            data_provider=CoinGeckoDataProvider(),
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
            strategy_type="lp",  # Force LP adapter
        )

        # With BacktestDataConfig for historical data:
        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(
            use_historical_volume=True,
            use_historical_funding=True,
            use_historical_apy=True,
        )
        backtester = PnLBacktester(
            data_provider=CoinGeckoDataProvider(),
            fee_models={"default": DefaultFeeModel()},
            slippage_models={"default": DefaultSlippageModel()},
            data_config=data_config,
        )
    """

    data_provider: HistoricalDataProvider
    fee_models: dict[str, FeeModel]
    slippage_models: dict[str, SlippageModel]
    strategy_type: str | None = "auto"
    gas_provider: GasPriceProvider | None = None
    """Optional gas price provider for historical gas prices.

    When provided and config.use_historical_gas_gwei=True, the engine will
    fetch historical gas prices at each simulation timestamp instead of
    using the static config.gas_price_gwei value.

    Example:
        from almanak.framework.backtesting.pnl.providers import EtherscanGasPriceProvider

        gas_provider = EtherscanGasPriceProvider(
            api_keys={"ethereum": "your-key"},
        )
        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models=fee_models,
            slippage_models=slippage_models,
            gas_provider=gas_provider,
        )
    """
    data_config: BacktestDataConfig | None = None
    """Optional BacktestDataConfig for controlling historical data providers.

    When provided, this configuration is passed to strategy-specific adapters
    (LP, Perp, Lending) to control historical data provider behavior:
    - use_historical_volume: Fetch LP fee data from subgraphs
    - use_historical_funding: Fetch perp funding rates from APIs
    - use_historical_apy: Fetch lending APY from subgraphs
    - strict_historical_mode: Fail if historical data unavailable
    - Fallback values for when historical data is unavailable
    - Rate limiting configuration for CoinGecko and The Graph
    - Cache settings for persistent data storage

    Example:
        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(
            use_historical_volume=True,
            use_historical_funding=True,
            use_historical_apy=True,
            strict_historical_mode=False,
        )
        backtester = PnLBacktester(
            data_provider=data_provider,
            fee_models=fee_models,
            slippage_models=slippage_models,
            data_config=data_config,
        )
    """
    _mev_simulator: MEVSimulator | None = None
    _current_backtest_id: str = ""
    _adapter: StrategyBacktestAdapter | None = None
    _detected_strategy_type: StrategyTypeHint | None = None
    _error_handler: BacktestErrorHandler | None = None
    _fallback_usage: dict[str, int] | None = None
    _gas_price_records: list["GasPriceRecord"] | None = None

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        # Ensure we have at least a default fee model
        if "default" not in self.fee_models:
            self.fee_models["default"] = DefaultFeeModel()

        # Ensure we have at least a default slippage model
        if "default" not in self.slippage_models:
            self.slippage_models["default"] = DefaultSlippageModel()

        # Initialize fallback usage tracking
        self._fallback_usage = {
            "hardcoded_price": 0,
            "default_gas_price": 0,
            "default_usd_amount": 0,
        }

        # Validate data_config if provided
        if self.data_config is not None:
            # Log data_config settings for transparency
            logger.info(
                "BacktestDataConfig provided: "
                f"price_provider={self.data_config.price_provider}, "
                f"use_historical_volume={self.data_config.use_historical_volume}, "
                f"use_historical_funding={self.data_config.use_historical_funding}, "
                f"use_historical_apy={self.data_config.use_historical_apy}, "
                f"strict_historical_mode={self.data_config.strict_historical_mode}"
            )

    async def close(self) -> None:
        """Close data provider and gas provider HTTP sessions."""
        if hasattr(self.data_provider, "close"):
            await self.data_provider.close()
        if self.gas_provider and hasattr(self.gas_provider, "close"):
            await self.gas_provider.close()

    def _track_fallback(self, fallback_type: str) -> None:
        """Track usage of a fallback value.

        Args:
            fallback_type: Type of fallback used (e.g., "hardcoded_price")
        """
        if self._fallback_usage is None:
            self._fallback_usage = {}
        if fallback_type in self._fallback_usage:
            self._fallback_usage[fallback_type] += 1
        else:
            self._fallback_usage[fallback_type] = 1

    def _build_callback_result(
        self,
        intent: Any,
        trade_record: TradeRecord | None,
        success: bool,
        error: str | None = None,
    ) -> SimulatedExecutionResult:
        """Build the result object passed to ``strategy.on_intent_executed``.

        VIB-2916: For LP_OPEN intents the real ``SimulatedPosition.position_id``
        is read from the trade record (populated by
        ``SimulatedFill.to_trade_record``) so a later
        ``Intent.lp_close(position_id=self._position_id)`` resolves against
        the open position the engine actually tracks.
        """
        return build_simulated_result(
            intent=intent,
            trade_record=trade_record,
            success=success,
            error=error,
        )

    def _notify_intent_failure(self, strategy: Any, intent: Any, error: Exception) -> None:
        """Notify the strategy that an intent failed to execute.

        Shared by the missing-data (``DataSourceUnavailableError``) and generic
        execution-error paths so a strategy state machine never advances past an
        intent that silently vanished. Best-effort: a raising callback is logged
        at debug, never propagated (a notification failure must not mask the
        original execution error).
        """
        if strategy is None or not hasattr(strategy, "on_intent_executed"):
            return
        try:
            callback_result = self._build_callback_result(intent, None, success=False, error=str(error))
            strategy.on_intent_executed(intent, False, callback_result)
        except Exception as notify_err:
            logger.debug(f"on_intent_executed (failure) raised: {notify_err}")

    def _create_parameter_source_tracker(
        self,
        config: PnLBacktestConfig,
    ) -> ParameterSourceTracker:
        """Create a tracker with sources for all configuration parameters.

        This method tracks where each configuration value came from for audit
        purposes. It examines the config and records the source of each parameter.

        Args:
            config: The backtest configuration to analyze

        Returns:
            ParameterSourceTracker populated with source records
        """
        tracker = ParameterSourceTracker()

        # Get default values from PnLBacktestConfig for comparison
        default_config = PnLBacktestConfig(
            start_time=config.start_time,  # Required fields
            end_time=config.end_time,
        )

        # Track config parameters
        config_params = [
            ("initial_capital_usd", config.initial_capital_usd, default_config.initial_capital_usd),
            ("interval_seconds", config.interval_seconds, default_config.interval_seconds),
            ("fee_model", config.fee_model, default_config.fee_model),
            ("slippage_model", config.slippage_model, default_config.slippage_model),
            ("include_gas_costs", config.include_gas_costs, default_config.include_gas_costs),
            ("inclusion_delay_blocks", config.inclusion_delay_blocks, default_config.inclusion_delay_blocks),
            ("chain", config.chain, default_config.chain),
            ("benchmark_token", config.benchmark_token, default_config.benchmark_token),
            ("risk_free_rate", config.risk_free_rate, default_config.risk_free_rate),
            ("trading_days_per_year", config.trading_days_per_year, default_config.trading_days_per_year),
            ("mev_simulation_enabled", config.mev_simulation_enabled, default_config.mev_simulation_enabled),
            ("auto_correct_positions", config.auto_correct_positions, default_config.auto_correct_positions),
            ("strict_reproducibility", config.strict_reproducibility, default_config.strict_reproducibility),
            ("institutional_mode", config.institutional_mode, default_config.institutional_mode),
            ("min_data_coverage", config.min_data_coverage, default_config.min_data_coverage),
            ("allow_hardcoded_fallback", config.allow_hardcoded_fallback, default_config.allow_hardcoded_fallback),
            ("use_historical_gas_prices", config.use_historical_gas_prices, default_config.use_historical_gas_prices),
            ("use_historical_gas_gwei", config.use_historical_gas_gwei, default_config.use_historical_gas_gwei),
            ("preflight_validation", config.preflight_validation, default_config.preflight_validation),
        ]

        for name, value, default_value in config_params:
            # Determine source: if value equals default, it's DEFAULT; otherwise EXPLICIT
            # Note: In a more sophisticated implementation, we could track if the value
            # came from a config file or env var by inspecting how the config was created
            source = ParameterSource.DEFAULT if value == default_value else ParameterSource.EXPLICIT
            tracker.record_parameter(name, value, source, category="config")

        # gas_price_gwei provenance comes from the config's own flag rather
        # than the value-equality heuristic above: the default is chain-aware
        # (VIB-5088), so comparing against a default_config built on
        # DEFAULT_CHAIN would mislabel defaults on other chains as explicit.
        # The source stays DEFAULT for unset values -- the value just became
        # honest -- preserving the audit trail's fabrication signal.
        tracker.record_parameter(
            "gas_price_gwei",
            config.gas_price_gwei,
            ParameterSource.DEFAULT if config.gas_price_gwei_is_default else ParameterSource.EXPLICIT,
            category="config",
        )

        # Track gas-related parameters
        if config.gas_eth_price_override is not None:
            tracker.record_parameter(
                "gas_eth_price_override",
                config.gas_eth_price_override,
                ParameterSource.EXPLICIT,
                category="gas",
            )
        else:
            # Will be determined at runtime - track as historical or provider
            source = ParameterSource.HISTORICAL if config.use_historical_gas_prices else ParameterSource.PROVIDER
            tracker.record_parameter(
                "gas_eth_price",
                "runtime_determined",
                source,
                category="gas",
                # The legacy $3000 terminal fallback was removed -- the engine
                # raises when no ETH/WETH price is available (VIB-5088 audit).
                fallback_chain=["historical_provider", "current_provider", "raise_if_unavailable"],
            )

        # Track margin/liquidation parameters
        tracker.record_parameter(
            "initial_margin_ratio",
            config.initial_margin_ratio,
            ParameterSource.DEFAULT
            if config.initial_margin_ratio == default_config.initial_margin_ratio
            else ParameterSource.EXPLICIT,
            category="liquidation",
        )
        tracker.record_parameter(
            "maintenance_margin_ratio",
            config.maintenance_margin_ratio,
            ParameterSource.DEFAULT
            if config.maintenance_margin_ratio == default_config.maintenance_margin_ratio
            else ParameterSource.EXPLICIT,
            category="liquidation",
        )
        tracker.record_parameter(
            "reconciliation_alert_threshold_pct",
            config.reconciliation_alert_threshold_pct,
            ParameterSource.DEFAULT
            if config.reconciliation_alert_threshold_pct == default_config.reconciliation_alert_threshold_pct
            else ParameterSource.EXPLICIT,
            category="liquidation",
        )

        # Track APY/funding rate sources - these are determined at runtime
        # We record the config that will determine how they're fetched
        if self._adapter is not None:
            adapter_type = type(self._adapter).__name__
            # Perp strategies have funding rates
            if "perp" in adapter_type.lower():
                tracker.record_parameter(
                    "funding_rate_source",
                    "historical" if config.strict_reproducibility else "provider",
                    ParameterSource.HISTORICAL if config.strict_reproducibility else ParameterSource.PROVIDER,
                    category="apy_funding",
                )
            # Lending strategies have APY rates
            if "lending" in adapter_type.lower():
                tracker.record_parameter(
                    "apy_source",
                    "historical" if config.strict_reproducibility else "provider",
                    ParameterSource.HISTORICAL if config.strict_reproducibility else ParameterSource.PROVIDER,
                    category="apy_funding",
                )

        return tracker

    def _detect_strategy_type(
        self,
        strategy: BacktestableStrategy,
    ) -> StrategyTypeHint:
        """Detect the strategy type from a strategy object.

        Uses the adapter registry's detection system to determine the
        appropriate adapter type for backtesting. Detection priority:
        1. Explicit strategy_type in backtester config (if not "auto")
        2. Strategy metadata tags
        3. Supported protocols
        4. Intent types used
        5. Fallback to None (generic backtesting)

        Args:
            strategy: Strategy to detect type for

        Returns:
            StrategyTypeHint with detected type and confidence
        """
        # Build config dict for detection
        config: dict[str, Any] | None = None
        if self.strategy_type is not None and self.strategy_type != "auto":
            config = {"strategy_type": self.strategy_type}

        hint = detect_strategy_type(strategy, config)
        self._detected_strategy_type = hint

        if hint.strategy_type:
            logger.info(
                f"Detected strategy type '{hint.strategy_type}' (confidence={hint.confidence}, source={hint.source})"
            )
            if hint.details:
                logger.debug(f"Detection details: {hint.details}")
        else:
            logger.debug("No strategy type detected, will use generic backtesting")

        return hint

    def _init_adapter(
        self,
        strategy: BacktestableStrategy,
    ) -> None:
        """Initialize the strategy adapter based on detected type.

        This method detects the strategy type and loads the appropriate
        adapter for strategy-specific backtesting behavior. The adapter
        is stored in self._adapter for use during the backtest.

        When data_config is provided to the PnLBacktester, it will be passed
        to the adapter to control historical data provider behavior.

        Args:
            strategy: Strategy to initialize adapter for
        """
        # Build config dict for adapter lookup
        config: dict[str, Any] | None = None
        if self.strategy_type is not None and self.strategy_type != "auto":
            config = {"strategy_type": self.strategy_type}

        # Use get_adapter_for_strategy_with_config to pass data_config to adapters
        adapter = get_adapter_for_strategy_with_config(
            strategy,
            data_config=self.data_config,
            config=config,
        )

        if adapter:
            self._adapter = adapter
            adapter_info = f"'{adapter.adapter_name}' for strategy '{strategy.deployment_id}'"
            if self.data_config is not None:
                logger.info(f"Loaded adapter {adapter_info} with BacktestDataConfig")
            else:
                logger.info(f"Loaded adapter {adapter_info}")
        else:
            self._adapter = None
            logger.debug(f"No adapter loaded for strategy '{strategy.deployment_id}', using generic backtesting")

    def _init_mev_simulator(self, config: PnLBacktestConfig) -> None:
        """Initialize MEV simulator based on config.

        Args:
            config: Backtest configuration with mev_simulation_enabled flag
        """
        if config.mev_simulation_enabled:
            self._mev_simulator = MEVSimulator(config=MEVSimulatorConfig())
            logger.info("MEV simulation enabled - trades may incur simulated MEV costs")
        else:
            self._mev_simulator = None

    @staticmethod
    def _create_indicator_engine(strategy: BacktestableStrategy) -> BacktestIndicatorEngine:
        """Create indicator engine based on strategy's declared requirements.

        Reads ``required_indicators`` from the strategy (if present) and
        configures the engine accordingly.  Falls back to the default set
        (RSI, MACD, Bollinger Bands) when the attribute is absent.

        Args:
            strategy: The strategy being backtested.

        Returns:
            Configured BacktestIndicatorEngine.
        """
        required = getattr(strategy, "required_indicators", None)
        if required is not None:
            indicator_set = set(required) if not isinstance(required, set) else required
        else:
            indicator_set = None  # will use defaults

        return BacktestIndicatorEngine(required_indicators=indicator_set)

    @staticmethod
    def _get_strategy_config_dict(strategy: BacktestableStrategy) -> dict:
        """Extract a plain dict config from a strategy for indicator parameter lookup.

        Tries ``strategy.config.to_dict()``, ``dict(strategy.config)``, and
        direct dict access.  Returns an empty dict if nothing works.
        """
        config = getattr(strategy, "config", None)
        if config is None:
            return {}
        if isinstance(config, dict):
            return config
        if hasattr(config, "to_dict"):
            try:
                return config.to_dict()
            except Exception:
                pass
        # Try dict() constructor (works for Mapping-like objects)
        try:
            return dict(config)
        except (TypeError, ValueError):
            return {}

    def _get_data_provider_info(self) -> dict[str, Any]:
        """Get information about the data provider for metadata.

        Collects provider name, version, and capabilities for reproducibility
        tracking in the backtest result. Uses error handler for non-critical
        errors during metadata collection.

        Returns:
            Dictionary with data provider information
        """
        info: dict[str, Any] = {
            "name": getattr(self.data_provider, "provider_name", "unknown"),
            "data_fetched_at": datetime.now(UTC).isoformat(),
        }

        # Helper function for safely getting attributes with error handling
        def safe_get_attr(attr_name: str, default: Any = None) -> Any:
            try:
                return getattr(self.data_provider, attr_name, default)
            except Exception as e:
                if self._error_handler:
                    self._error_handler.handle_error(
                        e,
                        context=f"get_data_provider_info:{attr_name}",
                    )
                return default

        # Try to get supported tokens/chains
        info["supported_tokens"] = safe_get_attr("supported_tokens", [])
        info["supported_chains"] = safe_get_attr("supported_chains", [])

        # Try to get provider version if available
        info["version"] = safe_get_attr("version", None)

        # Try to get min/max timestamps if available
        min_ts = safe_get_attr("min_timestamp", None)
        if min_ts:
            info["min_timestamp"] = min_ts.isoformat() if hasattr(min_ts, "isoformat") else str(min_ts)

        max_ts = safe_get_attr("max_timestamp", None)
        if max_ts:
            info["max_timestamp"] = max_ts.isoformat() if hasattr(max_ts, "isoformat") else str(max_ts)

        return info

    def _collect_data_source_capabilities(
        self,
        bt_logger: BacktestLogger,
    ) -> tuple[dict[str, HistoricalDataCapability], list[str]]:
        """Collect data source capabilities and generate warnings for limited providers.

        This method inspects the data provider to determine its historical data capability
        and generates appropriate warnings if the provider has limitations that may affect
        backtest accuracy.

        Args:
            bt_logger: BacktestLogger for logging warnings

        Returns:
            Tuple of (capabilities dict, warnings list):
            - capabilities: Dict mapping provider name to HistoricalDataCapability
            - warnings: List of warning messages about data limitations
        """
        capabilities: dict[str, HistoricalDataCapability] = {}
        warnings: list[str] = []

        provider_name = getattr(self.data_provider, "provider_name", "unknown")

        # Try to get the historical capability from the provider
        try:
            capability = getattr(
                self.data_provider,
                "historical_capability",
                HistoricalDataCapability.FULL,  # Default to FULL if not specified
            )
            capabilities[provider_name] = capability

            # Generate warnings for providers with limited capabilities
            if capability == HistoricalDataCapability.CURRENT_ONLY:
                warning_msg = (
                    f"Data provider '{provider_name}' has CURRENT_ONLY capability. "
                    "Historical prices will be fetched at backtest runtime, not at simulation timestamps. "
                    "This may significantly affect backtest accuracy for historical analysis."
                )
                warnings.append(warning_msg)
                bt_logger.warning(warning_msg)

            elif capability == HistoricalDataCapability.PRE_CACHE:
                warning_msg = (
                    f"Data provider '{provider_name}' has PRE_CACHE capability. "
                    "Historical data must be pre-fetched before backtest. "
                    "Ensure data cache is populated for the full backtest period."
                )
                warnings.append(warning_msg)
                bt_logger.warning(warning_msg)

        except Exception as e:
            # Handle errors gracefully using error handler
            if self._error_handler:
                self._error_handler.handle_error(
                    e,
                    context=f"collect_data_source_capabilities:{provider_name}",
                )
            # Default to unknown capability
            bt_logger.debug(f"Could not determine capability for provider {provider_name}: {e}")

        return capabilities, warnings

    async def run_preflight_validation(  # noqa: C901
        self,
        config: PnLBacktestConfig,
    ) -> PreflightReport:
        """Run preflight validation checks before starting a backtest.

        Performs validation checks to ensure data requirements can be met:
        - Checks price data availability for all tokens in config
        - Verifies data provider capabilities match requirements
        - Tests archive node accessibility if historical TWAP/Chainlink needed
        - Estimates data coverage based on provider capabilities

        Args:
            config: Backtest configuration specifying tokens, time range, etc.

        Returns:
            PreflightReport with pass/fail status and detailed check results.

        Example:
            preflight = await backtester.run_preflight_validation(config)
            if not preflight.passed:
                print(preflight.summary())
                # Handle validation failure
            else:
                result = await backtester.backtest(strategy, config)
        """
        import time

        start_time = time.time()

        checks: list[PreflightCheckResult] = []
        tokens_available: list[str] = []
        tokens_unavailable: list[str] = []
        provider_capabilities: dict[str, str] = {}
        recommendations: list[str] = []
        archive_accessible: bool | None = None

        # Get provider info
        provider_name = getattr(self.data_provider, "provider_name", "unknown")
        supported_tokens = getattr(self.data_provider, "supported_tokens", [])

        # Check 1: Data provider capability
        try:
            capability = getattr(
                self.data_provider,
                "historical_capability",
                HistoricalDataCapability.FULL,
            )
            provider_capabilities[provider_name] = capability.value

            if capability == HistoricalDataCapability.FULL:
                checks.append(
                    PreflightCheckResult(
                        check_name="provider_capability",
                        passed=True,
                        message=f"Provider '{provider_name}' has FULL historical data capability",
                        details={"provider": provider_name, "capability": capability.value},
                    )
                )
            elif capability == HistoricalDataCapability.CURRENT_ONLY:
                checks.append(
                    PreflightCheckResult(
                        check_name="provider_capability",
                        passed=False,
                        message=(
                            f"Provider '{provider_name}' has CURRENT_ONLY capability. "
                            "Historical prices may be inaccurate."
                        ),
                        details={"provider": provider_name, "capability": capability.value},
                        severity="warning",
                    )
                )
                recommendations.append(
                    "Consider using a provider with FULL historical capability "
                    "(e.g., CoinGecko) for accurate backtesting"
                )
            elif capability == HistoricalDataCapability.PRE_CACHE:
                checks.append(
                    PreflightCheckResult(
                        check_name="provider_capability",
                        passed=True,
                        message=(
                            f"Provider '{provider_name}' requires PRE_CACHE. Ensure historical data is pre-fetched."
                        ),
                        details={"provider": provider_name, "capability": capability.value},
                        severity="warning",
                    )
                )
                recommendations.append("Pre-fetch historical data before running backtest for optimal performance")

        except Exception as e:
            checks.append(
                PreflightCheckResult(
                    check_name="provider_capability",
                    passed=False,
                    message=f"Failed to check provider capability: {e}",
                    details={"error": str(e)},
                )
            )

        # Check 2: Token availability
        for token in config.tokens:
            token_upper = token.upper()

            # Check if token is in supported list (if available)
            if supported_tokens:
                if token_upper in [t.upper() for t in supported_tokens]:
                    tokens_available.append(token_upper)
                else:
                    tokens_unavailable.append(token_upper)
            else:
                # If no supported_tokens list, try to fetch price to check availability
                try:
                    # Try a simple price fetch at current time to check availability
                    await self.data_provider.get_price(token, config.start_time)
                    tokens_available.append(token_upper)
                except Exception:
                    tokens_unavailable.append(token_upper)

        # Create token availability check result
        if tokens_unavailable:
            checks.append(
                PreflightCheckResult(
                    check_name="token_availability",
                    passed=False,
                    message=f"{len(tokens_unavailable)} token(s) may not have price data available",
                    details={
                        "available": tokens_available,
                        "unavailable": tokens_unavailable,
                    },
                    severity="warning",
                )
            )
            recommendations.append(f"Check price data availability for: {', '.join(tokens_unavailable)}")
        else:
            checks.append(
                PreflightCheckResult(
                    check_name="token_availability",
                    passed=True,
                    message=f"All {len(tokens_available)} token(s) have price data available",
                    details={"available": tokens_available},
                )
            )

        # Check 3: Archive node accessibility (if provider supports it)
        if hasattr(self.data_provider, "verify_archive_access"):
            try:
                archive_accessible = await self.data_provider.verify_archive_access()
                if archive_accessible:
                    checks.append(
                        PreflightCheckResult(
                            check_name="archive_node_access",
                            passed=True,
                            message="Archive node is accessible for historical queries",
                        )
                    )
                else:
                    checks.append(
                        PreflightCheckResult(
                            check_name="archive_node_access",
                            passed=False,
                            message="Archive node is not accessible; historical queries may fail",
                            severity="warning",
                        )
                    )
                    recommendations.append(
                        "Configure an archive node RPC URL for accurate historical TWAP/Chainlink data"
                    )
            except Exception as e:
                checks.append(
                    PreflightCheckResult(
                        check_name="archive_node_access",
                        passed=False,
                        message=f"Failed to verify archive node access: {e}",
                        details={"error": str(e)},
                        severity="warning",
                    )
                )

        # Check 4: Time range validation
        provider_min_ts = getattr(self.data_provider, "min_timestamp", None)
        provider_max_ts = getattr(self.data_provider, "max_timestamp", None)

        time_range_valid = True
        time_range_details: dict[str, Any] = {
            "requested_start": config.start_time.isoformat(),
            "requested_end": config.end_time.isoformat(),
        }

        if provider_min_ts is not None and config.start_time < provider_min_ts:
            time_range_valid = False
            time_range_details["provider_min"] = provider_min_ts.isoformat()

        if provider_max_ts is not None and config.end_time > provider_max_ts:
            time_range_valid = False
            time_range_details["provider_max"] = provider_max_ts.isoformat()

        if time_range_valid:
            checks.append(
                PreflightCheckResult(
                    check_name="time_range_coverage",
                    passed=True,
                    message="Requested time range is within provider's data range",
                    details=time_range_details,
                )
            )
        else:
            checks.append(
                PreflightCheckResult(
                    check_name="time_range_coverage",
                    passed=False,
                    message="Requested time range extends beyond provider's data availability",
                    details=time_range_details,
                    severity="warning",
                )
            )
            recommendations.append("Adjust backtest time range to match data provider's coverage")

        # Check 5: Institutional mode requirements
        if config.institutional_mode:
            # Check for CURRENT_ONLY providers in institutional mode
            has_current_only = any(cap == "current_only" for cap in provider_capabilities.values())
            if has_current_only:
                checks.append(
                    PreflightCheckResult(
                        check_name="institutional_compliance",
                        passed=False,
                        message="CURRENT_ONLY provider not allowed in institutional mode",
                        details={"provider_capabilities": provider_capabilities},
                        severity="error",
                    )
                )
                recommendations.append("Use a provider with FULL historical capability for institutional mode")
            else:
                checks.append(
                    PreflightCheckResult(
                        check_name="institutional_compliance",
                        passed=True,
                        message="Provider meets institutional mode requirements",
                    )
                )

        # Calculate estimated coverage
        total_tokens = len(config.tokens)
        available_count = len(tokens_available)
        estimated_coverage = Decimal(available_count) / Decimal(total_tokens) if total_tokens > 0 else Decimal("1.0")

        # Determine overall pass/fail
        # Passed if no error-severity checks failed
        error_checks_failed = [c for c in checks if not c.passed and c.severity == "error"]
        overall_passed = len(error_checks_failed) == 0

        validation_time = time.time() - start_time

        return PreflightReport(
            passed=overall_passed,
            checks=checks,
            estimated_coverage=estimated_coverage,
            tokens_available=tokens_available,
            tokens_unavailable=tokens_unavailable,
            provider_capabilities=provider_capabilities,
            archive_node_accessible=archive_accessible,
            recommendations=recommendations,
            validation_time_seconds=validation_time,
        )

    async def backtest(
        self,
        strategy: BacktestableStrategy,
        config: PnLBacktestConfig,
    ) -> BacktestResult:
        """Run a backtest for a strategy over the configured period.

        This method:
        1. Initializes a simulated portfolio with initial capital
        2. Creates a HistoricalDataConfig from the backtest config
        3. Iterates through historical market states
        4. For each time step:
           a. Creates a MarketSnapshot from MarketState
           b. Calls strategy.decide(snapshot) to get intent
           c. Queues intent for execution (with inclusion delay)
           d. Executes queued intents
           e. Marks portfolio to market
        5. Calculates final metrics and returns BacktestResult

        Args:
            strategy: Strategy to backtest (must implement BacktestableStrategy)
            config: Backtest configuration (time range, capital, models, etc.)

        Returns:
            BacktestResult with metrics, trades, and equity curve

        Raises:
            ValueError: If strategy is not compatible with backtesting
        """
        run_started_at = datetime.now(UTC)

        # Generate unique backtest_id for correlation across all log messages
        backtest_id = str(uuid.uuid4())
        self._current_backtest_id = backtest_id  # Store for use in _execute_intent

        # Create backtest logger with phase timing support
        bt_logger = BacktestLogger(
            backtest_id=backtest_id,
            json_format=False,  # Use text format by default, can be configured
            logger=logger,
        )

        bt_logger.info(
            f"Starting backtest for {strategy.deployment_id} "
            f"from {config.start_time} to {config.end_time} "
            f"with ${config.initial_capital_usd:,.2f} capital"
        )

        # Wrap entire backtest body in try/finally to guarantee async resource cleanup.
        # Without this, exceptions from the data quality gate (ValueError) or preflight
        # validation (PreflightValidationError) can exit without closing the aiohttp
        # session, causing "RuntimeError: Event loop is closed" when GC collects the
        # leaked session after asyncio.run() shuts down the event loop.
        try:
            return await self._run_backtest(strategy, config, backtest_id, bt_logger, run_started_at)
        finally:
            try:
                await self.close()
            except (OSError, RuntimeError):
                logger.debug("Error during async resource cleanup", exc_info=True)

    async def _run_backtest(
        self,
        strategy: BacktestableStrategy,
        config: PnLBacktestConfig,
        backtest_id: str,
        bt_logger: BacktestLogger,
        run_started_at: datetime,
    ) -> BacktestResult:
        """Internal backtest implementation. Called by backtest() with guaranteed cleanup.

        Orchestrates the phase helpers extracted in Phase 6C.2. The body is a
        thin sequencer: preflight -> initialization -> simulation loop ->
        (error path | data quality gate + finalization). All semantics are
        preserved byte-for-byte by ``_engine_helpers``; see that module's
        docstring for details.
        """
        # Run preflight validation if enabled (no BacktestState yet, so a
        # PreflightValidationError propagates straight to the caller -- matches
        # pre-extraction behavior).
        preflight_report, preflight_passed = await _engine_helpers.run_preflight(
            backtester=self,
            config=config,
            bt_logger=bt_logger,
        )

        # Initialization phase: build the shared BacktestState.
        state = _engine_helpers.initialize_backtest(
            backtester=self,
            strategy=strategy,
            config=config,
            bt_logger=bt_logger,
        )

        # Simulation phase
        try:
            await _engine_helpers.execute_iteration_loop(
                backtester=self,
                strategy=strategy,
                config=config,
                bt_logger=bt_logger,
                state=state,
            )
        except Exception as e:
            return _engine_helpers.build_error_result(
                backtester=self,
                strategy=strategy,
                config=config,
                backtest_id=backtest_id,
                bt_logger=bt_logger,
                run_started_at=run_started_at,
                state=state,
                preflight_report=preflight_report,
                preflight_passed=preflight_passed,
                error=e,
            )

        # Data quality gate enforcement - check coverage ratio after simulation.
        # Raises ValueError in institutional mode; otherwise appends compliance
        # violation + logs warning.
        _engine_helpers.enforce_data_quality_gate(
            config=config,
            bt_logger=bt_logger,
            state=state,
        )

        # Metrics calculation + BacktestResult assembly
        return _engine_helpers.finalize_backtest_result(
            backtester=self,
            strategy=strategy,
            config=config,
            backtest_id=backtest_id,
            bt_logger=bt_logger,
            run_started_at=run_started_at,
            state=state,
            preflight_report=preflight_report,
            preflight_passed=preflight_passed,
        )

    def _extract_intent(self, decide_result: Any) -> Any:
        """Extract the intent from a decide() result. Delegates to intent_extraction module."""
        from .intent_extraction import extract_intent

        return extract_intent(decide_result)

    def _is_hold_intent(self, intent: Any) -> bool:
        """Check if an intent is a HOLD intent. Delegates to intent_extraction module."""
        from .intent_extraction import is_hold_intent

        return is_hold_intent(intent)

    def _update_positions_via_adapter(
        self,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        timestamp: datetime,
    ) -> None:
        """Update all positions via the strategy-specific adapter.

        This method calls the adapter's update_position() method for each position
        in the portfolio. This allows strategy-specific position updates such as:
        - LP: Fee accrual, impermanent loss tracking
        - Perp: Funding payments, liquidation price updates
        - Lending: Interest accrual, health factor updates

        Args:
            portfolio: The portfolio containing positions to update
            market_state: Current market state with prices
            timestamp: Current simulation timestamp

        Note:
            If no adapter is set, this method does nothing.
            The adapter's update_position modifies positions in-place.
        """
        if self._adapter is None:
            return

        # Calculate elapsed time since last update
        # Use the timestamp from the most recent equity curve point if available
        elapsed_seconds = 0.0
        if portfolio.equity_curve:
            last_timestamp = portfolio.equity_curve[-1].timestamp
            elapsed_seconds = (timestamp - last_timestamp).total_seconds()
            if elapsed_seconds < 0:
                elapsed_seconds = 0.0

        # Update each position via the adapter
        # Pass simulation timestamp for deterministic, reproducible updates
        for position in portfolio.positions:
            # Per-position clamp: a position accrued mid-tick (a partial
            # WITHDRAW accrues lending interest through the fill instant via
            # apply_fill) carries last_updated == this tick's timestamp; the
            # equity-curve basis would re-accrue that same interval on the
            # reduced principal. min() is a no-op for every other position:
            # adapters stamp last_updated with the tick timestamp on each
            # update (equal to the equity point), and positions never
            # updated carry None or a stale value (where the equity basis
            # is already the smaller elapsed).
            per_position_elapsed = elapsed_seconds
            if position.last_updated is not None:
                position_elapsed = (timestamp - position.last_updated).total_seconds()
                if position_elapsed >= 0:
                    per_position_elapsed = min(per_position_elapsed, position_elapsed)
            try:
                self._adapter.update_position(position, market_state, per_position_elapsed, timestamp)
            except DataSourceUnavailableError as e:
                # VIB-4849: a missing-data signal is a *deliberate fail-loud* from the
                # adapter -- it refused to fabricate a number. It must NEVER be
                # downgraded to a DEBUG log and silently swallowed, or the position
                # would accrue no fees this tick and the backtest would report a
                # silently-wrong number. Surface it: route to the error handler if one
                # is configured (so a stop policy is honoured), otherwise re-raise so
                # the backtest fails loudly and clearly flags the missing volume.
                logger.error(
                    "Missing data source while updating position %s: %s",
                    position.position_id,
                    e,
                )
                if self._error_handler:
                    result = self._error_handler.handle_error(
                        e,
                        context=f"adapter_update_position:{position.position_id}:{timestamp.isoformat()}",
                    )
                    if result.should_stop:
                        raise
                    # Handler explicitly chose to continue: do NOT silently re-raise,
                    # but the loud ERROR above guarantees the gap is visible.
                else:
                    # No handler configured -> propagate. Never swallow a
                    # missing-data signal into a DEBUG log.
                    raise
            except Exception as e:
                # Use error handler for position update errors (typically non-critical)
                if self._error_handler:
                    result = self._error_handler.handle_error(
                        e,
                        context=f"adapter_update_position:{position.position_id}:{timestamp.isoformat()}",
                    )
                    if result.should_stop:
                        logger.error(f"Fatal error updating position {position.position_id}: {e}")
                        raise
                    # Non-fatal: continue with other positions
                else:
                    logger.debug(
                        "Adapter update_position failed for %s: %s",
                        position.position_id,
                        e,
                    )

    async def _process_pending_intents(
        self,
        pending_intents: list[tuple[Any, datetime, int]],
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        config: PnLBacktestConfig,
        data_quality_tracker: DataQualityTracker | None = None,
        strategy: Any = None,
    ) -> list[tuple[Any, datetime, int]]:
        """Process pending intents, executing those ready and decrementing others.

        Implements inclusion delay simulation. Intents are queued when decided
        and executed after config.inclusion_delay_blocks ticks have passed.

        Args:
            pending_intents: List of (intent, decision_time, blocks_remaining)
            portfolio: Current portfolio state
            market_state: Current market state for execution
            config: Backtest config
            data_quality_tracker: Optional tracker for data quality metrics
            strategy: Optional strategy instance to notify via on_intent_executed

        Returns:
            Updated list of pending intents (those still waiting)
        """
        remaining: list[tuple[Any, datetime, int]] = []

        for intent, decision_time, blocks_remaining in pending_intents:
            if blocks_remaining <= 0:
                # Execute this intent
                try:
                    trade_record = await self._execute_intent(
                        intent=intent,
                        portfolio=portfolio,
                        market_state=market_state,
                        timestamp=market_state.timestamp,
                        config=config,
                        data_quality_tracker=data_quality_tracker,
                    )
                    # The portfolio may reject a fill (insufficient balance,
                    # producer-failed) — trade_record.success is authoritative.
                    if trade_record.success:
                        # Record successful execution in error handler
                        if self._error_handler:
                            self._error_handler.record_success()
                        logger.debug(
                            f"Executed intent at {market_state.timestamp} "
                            f"(decided at {decision_time}): "
                            f"type={trade_record.intent_type.value}, "
                            f"amount=${trade_record.amount_usd:,.2f}, "
                            f"fee=${trade_record.fee_usd:,.2f}"
                        )
                    else:
                        logger.warning(
                            f"Intent rejected by portfolio at {market_state.timestamp} "
                            f"(decided at {decision_time}): "
                            f"type={trade_record.intent_type.value}, "
                            f"reason={trade_record.metadata.get('failure_reason', 'fill rejected')}"
                        )
                    # Notify strategy with the real outcome so state machines
                    # do not advance past a trade that never applied
                    _engine_helpers.notify_intent_outcome(self, strategy, intent, trade_record, logger)
                except DataSourceUnavailableError as e:
                    # VIB-5088 (pattern from VIB-4849): a missing-data signal is a
                    # deliberate fail-loud -- the engine refused to fabricate a
                    # number (e.g. institutional mode rejecting the chain-default
                    # gas price). It must never degrade to a warn-and-skip: the
                    # trade would silently vanish from the results. Route to the
                    # error handler if configured (so a stop policy is honoured),
                    # otherwise re-raise so the backtest fails loudly.
                    logger.error(
                        "Missing data source while executing intent at %s: %s",
                        market_state.timestamp.isoformat(),
                        e,
                    )
                    # Notify the strategy of the failure (mirrors the generic
                    # exception path below). Without this, a state machine
                    # waiting on the intent never sees it fail when the error
                    # handler chooses to continue -- the trade silently vanishes.
                    self._notify_intent_failure(strategy, intent, e)
                    if self._error_handler:
                        result = self._error_handler.handle_error(
                            e,
                            context=f"execute_intent:{market_state.timestamp.isoformat()}:{type(intent).__name__}",
                        )
                        if result.should_stop:
                            raise
                        # Handler explicitly chose to continue: the loud ERROR
                        # above guarantees the gap is visible.
                    else:
                        raise
                except Exception as e:
                    # Notify strategy of execution failure
                    self._notify_intent_failure(strategy, intent, e)
                    # Use error handler for intent execution errors
                    if self._error_handler:
                        result = self._error_handler.handle_error(
                            e,
                            context=f"execute_intent:{market_state.timestamp.isoformat()}:{type(intent).__name__}",
                        )
                        if result.should_stop:
                            logger.error(f"Fatal error executing intent at {market_state.timestamp}: {e}")
                            raise
                        # Non-fatal: log warning and skip this intent
                        logger.warning(f"Failed to execute intent at {market_state.timestamp}: {e} - skipping")
                    else:
                        logger.warning(f"Failed to execute intent at {market_state.timestamp}: {e}")
            else:
                # Still waiting, decrement counter
                remaining.append((intent, decision_time, blocks_remaining - 1))

        return remaining

    def get_fee_model(self, protocol: str) -> FeeModel:
        """Get the fee model for a protocol.

        Args:
            protocol: Protocol name (e.g., "uniswap_v3", "aave_v3")

        Returns:
            FeeModel for the protocol, or default if not found
        """
        return self.fee_models.get(protocol, self.fee_models["default"])

    def get_slippage_model(self, protocol: str) -> SlippageModel:
        """Get the slippage model for a protocol.

        Args:
            protocol: Protocol name (e.g., "uniswap_v3", "gmx")

        Returns:
            SlippageModel for the protocol, or default if not found
        """
        return self.slippage_models.get(protocol, self.slippage_models["default"])

    async def _execute_intent(
        self,
        intent: Any,
        portfolio: SimulatedPortfolio,
        market_state: MarketState,
        timestamp: datetime,
        config: PnLBacktestConfig,
        delayed_at_end: bool = False,
        data_quality_tracker: DataQualityTracker | None = None,
    ) -> TradeRecord:
        """Execute an intent by simulating the trade with fees and slippage.

        This method simulates the execution of a trading intent by:
        1. Trying adapter-specific execution first (if adapter exists)
        2. Falling back to generic execution if adapter returns None
        3. Extracting intent details (type, protocol, tokens, amount)
        4. Looking up the appropriate fee and slippage models
        5. Calculating fee_usd based on intent type and amount
        6. Calculating slippage_pct and slippage_usd
        7. Calculating gas_cost_usd if include_gas_costs is True
        8. Creating a SimulatedFill with all execution details
        9. Applying the fill to the portfolio
        10. Returning a TradeRecord

        Args:
            intent: The intent to execute (SwapIntent, LPIntent, etc.)
            portfolio: The portfolio to update
            market_state: Current market state with prices
            timestamp: Time of execution
            config: Backtest configuration
            delayed_at_end: Whether this intent was executed at simulation end from pending queue
            data_quality_tracker: Optional tracker for data quality metrics

        Returns:
            TradeRecord with all execution details including fees, slippage, and gas
        """
        from almanak.framework.backtesting.pnl.portfolio import SimulatedFill

        # Try adapter-specific execution first
        if self._adapter is not None:
            adapter_fill = self._adapter.execute_intent(intent, portfolio, market_state)
            if adapter_fill is not None:
                # Adapter returned a SimulatedFill, use it
                logger.debug(f"Using adapter '{self._adapter.adapter_name}' execution for intent")
                # Set delayed_at_end flag on adapter fill
                adapter_fill.delayed_at_end = delayed_at_end
                # Gas is engine-owned: adapters own protocol math but have no
                # access to the gas lane (config gas price, historical gas
                # provider, data-quality tracking), so any adapter-side
                # gas_cost_usd is replaced with the resolved value here --
                # the LP adapter's old flat $20/$15 guesses overstated L2
                # gas costs by ~200x. Failed fills are skipped: apply_fill
                # zeroes their execution costs anyway, and resolving gas can
                # raise when no ETH price is available.
                if adapter_fill.success:
                    (
                        adapter_fill.gas_cost_usd,
                        adapter_fill.gas_price_gwei,
                        gas_gwei_source,
                    ) = await self._resolve_gas_cost(
                        intent_type=adapter_fill.intent_type,
                        market_state=market_state,
                        timestamp=timestamp,
                        config=config,
                        data_quality_tracker=data_quality_tracker,
                    )
                    if adapter_fill.metadata is None:
                        adapter_fill.metadata = {}
                    adapter_fill.metadata["gas_price_source"] = gas_gwei_source
                # Apply the adapter's fill to the portfolio (the adapter is
                # threaded through so a partial position reduction accrues
                # lending interest through the fill instant).
                portfolio.apply_fill(adapter_fill, market_state=market_state, adapter=self._adapter)

                # Log detailed trade execution
                log_trade_execution(
                    logger=logger,
                    backtest_id=self._current_backtest_id,
                    timestamp=timestamp,
                    intent_type=adapter_fill.intent_type.value,
                    protocol=adapter_fill.protocol,
                    tokens=adapter_fill.tokens,
                    amount_usd=adapter_fill.amount_usd,
                    fee_usd=adapter_fill.fee_usd,
                    slippage_usd=adapter_fill.slippage_usd,
                    gas_cost_usd=adapter_fill.gas_cost_usd,
                    executed_price=adapter_fill.executed_price,
                    mev_cost_usd=adapter_fill.estimated_mev_cost_usd,
                )

                # Convert to TradeRecord and return
                return adapter_fill.to_trade_record(pnl_usd=Decimal("0"))

            # Adapter returned None, fall back to generic execution
            logger.debug(f"Adapter '{self._adapter.adapter_name}' returned None, using generic execution")

        # Generic execution logic (fallback)
        # Extract intent details
        intent_type = self._get_intent_type(intent)
        protocol = self._get_intent_protocol(intent)
        tokens = self._get_intent_tokens(intent)
        amount_usd = self._get_intent_amount_usd(
            intent, market_state, strict_reproducibility=config.strict_reproducibility
        )

        # Close targets resolve before fee/slippage: a perp full close
        # (size_usd=None) and a lending withdraw_all take their fee
        # notional from the matched position.
        close_resolution = self._resolve_position_close(intent, intent_type, portfolio, amount_usd, market_state)
        position_close_id = close_resolution.position_close_id
        amount_usd = close_resolution.amount_usd

        # Get fee and slippage models for this protocol
        fee_model = self.get_fee_model(protocol)
        slippage_model = self.get_slippage_model(protocol)

        # Calculate fee
        fee_usd = fee_model.calculate_fee(
            intent_type=intent_type,
            amount_usd=amount_usd,
            market_state=market_state,
            protocol=protocol,
        )

        # Calculate base slippage
        slippage_pct = slippage_model.calculate_slippage(
            intent_type=intent_type,
            amount_usd=amount_usd,
            market_state=market_state,
            protocol=protocol,
        )

        # Simulate MEV costs if enabled
        estimated_mev_cost_usd, slippage_pct = self._simulate_mev_impact(
            intent_type=intent_type,
            tokens=tokens,
            amount_usd=amount_usd,
            slippage_pct=slippage_pct,
            config=config,
        )

        slippage_usd = amount_usd * slippage_pct

        # Calculate gas cost
        gas_cost_usd, gas_price_gwei, gas_gwei_source = await self._resolve_gas_cost(
            intent_type=intent_type,
            market_state=market_state,
            timestamp=timestamp,
            config=config,
            data_quality_tracker=data_quality_tracker,
        )

        # Get executed price (for swaps/perps, this is the price after slippage)
        executed_price = self._get_executed_price(intent, market_state, slippage_pct, intent_type)

        # Calculate token flows
        tokens_in, tokens_out = self._calculate_token_flows(
            intent=intent,
            intent_type=intent_type,
            amount_usd=amount_usd,
            executed_price=executed_price,
            fee_usd=fee_usd,
            slippage_usd=slippage_usd,
            market_state=market_state,
        )

        # Create position delta if this intent creates/modifies a position
        position_delta = self._create_position_delta(
            intent=intent,
            intent_type=intent_type,
            protocol=protocol,
            tokens=tokens,
            executed_price=executed_price,
            timestamp=timestamp,
            market_state=market_state,
            strict_reproducibility=config.strict_reproducibility,
        )

        # Create the simulated fill. A failed close resolution (e.g. a
        # WITHDRAW with no matching open supply position) is marked
        # success=False so apply_fill records it as a rejected trade with
        # zero state mutation -- crediting the inflow without a position
        # to debit would mint value.
        metadata: dict[str, Any] = {
            "intent": str(intent),
            "slippage_pct": str(slippage_pct),
            "gas_price_source": gas_gwei_source,
        }
        if close_resolution.failure_reason is not None:
            metadata["failure_reason"] = close_resolution.failure_reason
        if close_resolution.interest_usd != Decimal("0"):
            # str() round-trips Decimal losslessly; _calculate_trade_pnl
            # reads this back as the trade's realized interest PnL.
            metadata["interest_usd"] = str(close_resolution.interest_usd)
        fill = SimulatedFill(
            timestamp=timestamp,
            intent_type=intent_type,
            protocol=protocol,
            tokens=tokens,
            executed_price=executed_price,
            amount_usd=amount_usd,
            fee_usd=fee_usd,
            slippage_usd=slippage_usd,
            gas_cost_usd=gas_cost_usd,
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            success=close_resolution.failure_reason is None,
            position_delta=position_delta,
            position_close_id=position_close_id,
            position_reduce_id=close_resolution.position_reduce_id,
            # Partial reductions debit the position by exactly the fill's
            # flow on the position's side -- the credited inflow for a
            # WITHDRAW (tokens leave the supply), the debited outflow for a
            # REPAY (tokens extinguish the debt) -- keeping the reduction
            # value-conserving by construction. A BOUNDARY reduce instead
            # carries an explicit principal map (the flow also covers part of
            # the accrued interest, which is realized rather than removed as
            # principal -- VIB-5098); the gap is re-derived by the portfolio's
            # conservation guard.
            position_reduce_amounts=(
                close_resolution.reduce_amounts
                if close_resolution.reduce_amounts is not None
                else (
                    dict(tokens_out if intent_type == IntentType.REPAY else tokens_in)
                    if close_resolution.position_reduce_id
                    else {}
                )
            ),
            metadata=metadata,
            gas_price_gwei=gas_price_gwei,
            estimated_mev_cost_usd=estimated_mev_cost_usd,
            delayed_at_end=delayed_at_end,
        )

        # Apply fill to portfolio. The adapter is threaded through so a
        # partial position reduction can accrue lending interest through the
        # fill instant with the adapter lane's own rate sources.
        portfolio.apply_fill(fill, market_state=market_state, adapter=self._adapter)

        # Log detailed trade execution (DEBUG level - visible in verbose mode)
        log_trade_execution(
            logger=logger,
            backtest_id=self._current_backtest_id,
            timestamp=timestamp,
            intent_type=intent_type.value,
            protocol=protocol,
            tokens=tokens,
            amount_usd=amount_usd,
            fee_usd=fee_usd,
            slippage_usd=slippage_usd,
            gas_cost_usd=gas_cost_usd,
            executed_price=executed_price,
            mev_cost_usd=estimated_mev_cost_usd,
        )

        # Convert to TradeRecord and return
        return fill.to_trade_record(pnl_usd=Decimal("0"))

    def _simulate_mev_impact(
        self,
        intent_type: IntentType,
        tokens: list[str],
        amount_usd: Decimal,
        slippage_pct: Decimal,
        config: PnLBacktestConfig,
    ) -> tuple[Decimal | None, Decimal]:
        """Simulate MEV costs for an intent if the MEV simulator is enabled.

        Args:
            intent_type: Type of intent
            tokens: Tokens involved in the intent
            amount_usd: USD amount of the trade
            slippage_pct: Base slippage from the slippage model
            config: Backtest configuration

        Returns:
            Tuple of (estimated_mev_cost_usd or None when no simulator is
            configured, slippage_pct increased by MEV-induced slippage when
            the trade is sandwiched)
        """
        if self._mev_simulator is None:
            return None, slippage_pct

        # Get token symbols for MEV simulation
        token_in = tokens[0] if tokens else ""
        token_out = tokens[1] if len(tokens) > 1 else ""

        # Get gas price for inclusion delay simulation
        mev_gas_price = config.gas_price_gwei if config.include_gas_costs else None

        # Simulate MEV impact
        mev_result = self._mev_simulator.simulate_mev_cost(
            trade_amount_usd=amount_usd,
            token_in=token_in,
            token_out=token_out,
            gas_price_gwei=mev_gas_price,
            intent_type=intent_type,
        )

        # Add MEV-induced slippage to base slippage
        if mev_result.is_sandwiched:
            slippage_pct = slippage_pct + mev_result.additional_slippage_pct
            logger.debug(
                f"MEV simulation: Trade sandwiched, additional slippage "
                f"{mev_result.additional_slippage_pct * 100:.2f}%, "
                f"MEV cost ${mev_result.mev_cost_usd:.2f}"
            )

        return mev_result.mev_cost_usd, slippage_pct

    async def _resolve_gas_cost(
        self,
        intent_type: IntentType,
        market_state: MarketState,
        timestamp: datetime,
        config: PnLBacktestConfig,
        data_quality_tracker: DataQualityTracker | None,
    ) -> tuple[Decimal, Decimal | None, str | None]:
        """Resolve the simulated gas cost for an intent.

        Estimates gas units for the intent type, resolves the ETH price
        (:meth:`_resolve_gas_eth_price`) and the gas price in gwei
        (:meth:`_resolve_gas_price_gwei`), records tracking side effects
        (data-quality source, fallback usage, gas price records), and
        computes the final USD cost.

        Args:
            intent_type: Type of intent
            market_state: Current market state with prices
            timestamp: Time of execution
            config: Backtest configuration
            data_quality_tracker: Optional tracker for data quality metrics

        Returns:
            Tuple of (gas_cost_usd, gas_price_gwei, gas_gwei_source).
            Returns (Decimal("0"), None, None) when config.include_gas_costs
            is False.
        """
        if not config.include_gas_costs:
            return Decimal("0"), None, None

        # Estimate gas used based on intent type
        gas_used = self._estimate_gas_for_intent(intent_type)

        eth_price, gas_price_source = self._resolve_gas_eth_price(config, market_state, timestamp)

        # Track gas price source in data quality metrics
        if data_quality_tracker is not None:
            data_quality_tracker.record_gas_price_source(gas_price_source)

        gas_price_gwei, gas_gwei_source = await self._resolve_gas_price_gwei(config, market_state, timestamp)

        # Track gas gwei source in fallback usage. Both static lanes count:
        # "chain_default" (chain-aware registry default, VIB-5088) is an
        # honest fabrication but a fabrication nonetheless, and "config"
        # (user-set static value) is still not historical data -- the
        # compliance flag means "gas did not track historical conditions".
        if gas_gwei_source in ("config", "chain_default"):
            self._track_fallback("default_gas_price")

        # Calculate gas cost: gas_used * gas_price_gwei * ETH_price / 1e9
        gas_cost_eth = Decimal(gas_used) * gas_price_gwei / Decimal("1000000000")
        gas_cost_usd = gas_cost_eth * eth_price

        # Log gas cost details at debug level for troubleshooting
        logger.debug(
            "Gas cost: %d gas used × %.1f gwei (%s) × $%.2f ETH (%s) = $%.4f",
            gas_used,
            gas_price_gwei,
            gas_gwei_source,
            eth_price,
            gas_price_source,
            gas_cost_usd,
        )

        # Record gas price if tracking is enabled
        if self._gas_price_records is not None and gas_price_gwei is not None:
            self._gas_price_records.append(
                GasPriceRecord(
                    timestamp=timestamp,
                    gwei=gas_price_gwei,
                    source=gas_gwei_source or "unknown",
                    usd_cost=gas_cost_usd,
                    eth_price_usd=eth_price,
                )
            )

        return gas_cost_usd, gas_price_gwei, gas_gwei_source

    def _resolve_gas_eth_price(
        self,
        config: PnLBacktestConfig,
        market_state: MarketState,
        timestamp: datetime,
    ) -> tuple[Decimal, str]:
        """Resolve the ETH price used for gas-cost conversion.

        Priority order:
        1. gas_eth_price_override (takes precedence for reproducibility/testing)
        2. Historical ETH price (if use_historical_gas_prices enabled)
        3. Current market price (WETH or ETH from market_state)
        No silent fallback - fail if price unavailable.

        Args:
            config: Backtest configuration
            market_state: Current market state with prices
            timestamp: Time of execution

        Returns:
            Tuple of (eth_price, gas_price_source)

        Raises:
            ValueError: If no ETH/WETH price is available from the selected
                source and no gas_eth_price_override is set.
        """
        if config.gas_eth_price_override is not None:
            # Priority 1: Use explicit override
            eth_price = config.gas_eth_price_override
            gas_price_source = "override"
            logger.debug(
                "Gas ETH price: Using override value $%.2f",
                eth_price,
            )
        elif config.use_historical_gas_prices:
            # Priority 2: Try to get historical price from data provider
            try:
                eth_price = market_state.get_price("WETH")
                gas_price_source = "historical"
                logger.debug(
                    "Gas ETH price: Using historical WETH price $%.2f at %s",
                    eth_price,
                    timestamp.isoformat(),
                )
            except KeyError:
                try:
                    eth_price = market_state.get_price("ETH")
                    gas_price_source = "historical"
                    logger.debug(
                        "Gas ETH price: Using historical ETH price $%.2f at %s",
                        eth_price,
                        timestamp.isoformat(),
                    )
                except KeyError:
                    # Historical mode but no price available
                    if config.strict_reproducibility:
                        raise ValueError(
                            f"Gas ETH price: Historical price requested but ETH/WETH not "
                            f"available at {timestamp.isoformat()}. In strict_reproducibility mode, "
                            f"ETH price must be available. Set gas_eth_price_override for reproducibility."
                        ) from None
                    else:
                        raise ValueError(
                            f"Gas ETH price: Historical price requested but ETH/WETH not "
                            f"available at {timestamp.isoformat()}. Set gas_eth_price_override "
                            f"to provide an explicit ETH price for gas calculations."
                        ) from None
        else:
            # Priority 3: Use current market price (default behavior)
            try:
                eth_price = market_state.get_price("WETH")
                gas_price_source = "market"
            except KeyError:
                try:
                    eth_price = market_state.get_price("ETH")
                    gas_price_source = "market"
                except KeyError:
                    # No fallback allowed - fail with clear error
                    if config.strict_reproducibility:
                        raise ValueError(
                            f"Gas ETH price: ETH/WETH not available in market state at "
                            f"{timestamp.isoformat()}. In strict_reproducibility mode, "
                            f"ETH price must be available. Set gas_eth_price_override for reproducibility."
                        ) from None
                    else:
                        raise ValueError(
                            f"Gas ETH price: ETH/WETH not available in market state at "
                            f"{timestamp.isoformat()}. Set gas_eth_price_override to provide "
                            f"an explicit ETH price for gas calculations."
                        ) from None

        return eth_price, gas_price_source

    async def _resolve_gas_price_gwei(
        self,
        config: PnLBacktestConfig,
        market_state: MarketState,
        timestamp: datetime,
    ) -> tuple[Decimal, str]:
        """Resolve the gas price in gwei for gas-cost simulation.

        Priority order:
        1. Historical gas price from gas_provider (if use_historical_gas_gwei=True)
        2. MarketState.gas_price_gwei (if populated by data provider)
        3. config.gas_price_gwei -- source "config" when user-set, or
           "chain_default" when it is the chain-aware registry default
           (VIB-5088). Both are static fabrications for compliance purposes;
           "chain_default" additionally refuses to resolve in institutional
           mode (no user value + no historical datum = raise, never fabricate).

        Args:
            config: Backtest configuration
            market_state: Current market state
            timestamp: Time of execution

        Returns:
            Tuple of (gas_price_gwei, gas_gwei_source)

        Raises:
            DataSourceUnavailableError: In institutional mode when the gas
                price would fall back to the chain-registry default (i.e. no
                user-set value, no historical/market datum).
        """
        # config.gas_price_gwei is resolved to a Decimal in __post_init__.
        assert config.gas_price_gwei is not None
        gas_price_gwei = config.gas_price_gwei  # Default
        gas_gwei_source = "chain_default" if config.gas_price_gwei_is_default else "config"

        if config.use_historical_gas_gwei and self.gas_provider is not None:
            # Priority 1: Fetch historical gas price from provider
            try:
                historical_gas: GasPrice = await self.gas_provider.get_gas_price(
                    timestamp=timestamp,
                    chain=config.chain,
                )
                gas_price_gwei = historical_gas.effective_gas_price_gwei
                gas_gwei_source = f"historical_gas:{historical_gas.source}"
                logger.debug(
                    "Gas gwei: Using historical gas price %.1f gwei (source: %s) at %s",
                    gas_price_gwei,
                    historical_gas.source,
                    timestamp.isoformat(),
                )
            except Exception as e:
                logger.warning(
                    "Failed to get historical gas price, falling back to market_state/config: %s",
                    str(e),
                )
                # Fall through to next priority
                if market_state.gas_price_gwei is not None:
                    gas_price_gwei = market_state.gas_price_gwei
                    gas_gwei_source = "market_state"
                # else keep config.gas_price_gwei default

        elif config.use_historical_gas_gwei and self.gas_provider is None:
            # Historical gas requested but no provider - try market_state
            if market_state.gas_price_gwei is not None:
                gas_price_gwei = market_state.gas_price_gwei
                gas_gwei_source = "market_state"
            else:
                logger.warning(
                    "use_historical_gas_gwei=True but no gas_provider and "
                    "no gas_price_gwei in market_state, using config default %.1f gwei",
                    config.gas_price_gwei,
                )

        elif market_state.gas_price_gwei is not None:
            # Priority 2: Use market state gas price if available
            gas_price_gwei = market_state.gas_price_gwei
            gas_gwei_source = "market_state"

        # VIB-5088: institutional mode never fabricates a gas price. The
        # chain-aware default is an honest guess, but a guess nonetheless --
        # match the strict_price_mode / data-coverage-gate precedent and
        # fail loud instead of silently costing trades from a made-up number.
        if gas_gwei_source == "chain_default" and config.institutional_mode:
            raise DataSourceUnavailableError(
                data_type="gas_price",
                identifier=config.chain,
                remediation=(
                    "Set config.gas_price_gwei explicitly, enable "
                    "use_historical_gas_gwei with a gas_provider, or use a "
                    "data provider that populates MarketState.gas_price_gwei."
                ),
                message=(
                    f"Institutional mode: gas price for '{config.chain}' at "
                    f"{timestamp.isoformat()} would fall back to the chain-registry "
                    f"default ({config.gas_price_gwei} gwei) and institutional mode "
                    f"refuses to fabricate execution costs"
                ),
            )

        return gas_price_gwei, gas_gwei_source

    def _get_intent_type(self, intent: Any) -> IntentType:
        """Extract the IntentType from an intent object. Delegates to intent_extraction module."""
        from .intent_extraction import get_intent_type

        return get_intent_type(intent)

    def _get_intent_protocol(self, intent: Any) -> str:
        """Extract the protocol from an intent object. Delegates to intent_extraction module."""
        from .intent_extraction import get_intent_protocol

        return get_intent_protocol(intent)

    def _get_intent_tokens(self, intent: Any) -> list[str]:
        """Extract the tokens involved in an intent. Delegates to intent_extraction module."""
        from .intent_extraction import get_intent_tokens

        return get_intent_tokens(intent)

    def _get_intent_amount_usd(
        self,
        intent: Any,
        market_state: MarketState,
        strict_reproducibility: bool = False,
    ) -> Decimal:
        """Extract or calculate the USD amount for an intent. Delegates to intent_extraction module."""
        from .intent_extraction import get_intent_amount_usd

        return get_intent_amount_usd(
            intent,
            market_state,
            strict_reproducibility=strict_reproducibility,
            track_fallback=self._track_fallback,
        )

    def _estimate_gas_for_intent(self, intent_type: IntentType) -> int:
        """Estimate gas usage for an intent type. Delegates to intent_extraction module."""
        from .intent_extraction import estimate_gas_for_intent

        return estimate_gas_for_intent(intent_type)

    def _get_executed_price(
        self,
        intent: Any,
        market_state: MarketState,
        slippage_pct: Decimal,
        intent_type: IntentType,
    ) -> Decimal:
        """Get the executed price for an intent after slippage. Delegates to intent_extraction module."""
        from .intent_extraction import get_executed_price

        return get_executed_price(intent, market_state, slippage_pct, intent_type)

    def _calculate_token_flows(
        self,
        intent: Any,
        intent_type: IntentType,
        amount_usd: Decimal,
        executed_price: Decimal,
        fee_usd: Decimal,
        slippage_usd: Decimal,
        market_state: MarketState,
    ) -> tuple[dict[str, Decimal], dict[str, Decimal]]:
        """Calculate the token inflows and outflows for an intent.

        Dispatches to per-intent-type helpers in
        :mod:`almanak.framework.backtesting.pnl._engine_helpers` (Phase 6C.3).
        Unmatched intent types (HOLD, PERP, ...) return empty flow dicts --
        their balances are tracked via collateral rather than explicit token
        flows.

        Args:
            intent: Intent object
            intent_type: Type of intent
            amount_usd: USD amount of the trade
            executed_price: Executed price (unused; kept for API stability)
            fee_usd: Fee in USD (only consumed by SWAP)
            slippage_usd: Slippage cost in USD (only consumed by SWAP)
            market_state: Market state for prices

        Returns:
            Tuple of (tokens_in, tokens_out) dicts
        """
        del executed_price  # kept for backwards-compatible signature
        return _engine_helpers.calculate_token_flows(
            intent=intent,
            intent_type=intent_type,
            amount_usd=amount_usd,
            fee_usd=fee_usd,
            slippage_usd=slippage_usd,
            market_state=market_state,
        )

    def _create_position_delta(
        self,
        intent: Any,
        intent_type: IntentType,
        protocol: str,
        tokens: list[str],
        executed_price: Decimal,
        timestamp: datetime,
        market_state: MarketState,
        strict_reproducibility: bool = False,
    ) -> SimulatedPosition | None:
        """Create a position delta for intents that create positions.

        Dispatches to per-intent-type handlers through
        :data:`_POSITION_DELTA_HANDLERS` (mirroring
        :func:`_engine_helpers.calculate_token_flows`). Intent types without
        a handler (SWAP, HOLD, closes, ...) do not create positions and
        return ``None`` without extracting a USD amount.

        Args:
            intent: Intent object
            intent_type: Type of intent
            protocol: Protocol name
            tokens: Tokens involved
            executed_price: Executed price
            timestamp: Time of execution
            market_state: Market state
            strict_reproducibility: If True, raise on missing USD amount

        Returns:
            SimulatedPosition if a position is created, None otherwise
        """
        handler_name = _POSITION_DELTA_HANDLERS.get(intent_type)
        if handler_name is None:
            return None
        handler = getattr(self, handler_name)
        return handler(intent, protocol, tokens, executed_price, timestamp, market_state, strict_reproducibility)

    def _lp_open_delta(
        self,
        intent: Any,
        protocol: str,
        tokens: list[str],
        executed_price: Decimal,
        timestamp: datetime,
        market_state: MarketState,
        strict_reproducibility: bool,
    ) -> SimulatedPosition:
        """Create the simulated LP position for an LP_OPEN intent.

        Stores true Uniswap V3 liquidity (L) derived from the intent's USD
        notional at the entry price ratio: ``_mark_lp_position`` feeds
        ``position.liquidity`` into ``calculate_il_v3``, so any other unit
        in that field mints or burns value at the open tick (blueprint 31
        section 4 conservation; VIB-5096). Entry token amounts come from the
        same V3 math and ``entry_price`` is the token0/token1 ratio, matching
        the ``SimulatedPosition.lp`` contract. Tokens whose price is missing
        from ``market_state`` (or non-positive: bad data) raise in strict
        mode and otherwise fall back to a tracked $1 price, keeping the V3
        math anchored -- the L-units field never holds a USD notional.
        """
        from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
            ImpermanentLossCalculator,
        )
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        from .intent_extraction import get_lp_tick_range

        del executed_price  # the position stores the token0/token1 ratio instead

        token0 = tokens[0] if len(tokens) > 0 else "WETH"
        token1 = tokens[1] if len(tokens) > 1 else "USDC"

        amount_usd = self._get_intent_amount_usd(intent, market_state, strict_reproducibility=strict_reproducibility)

        # Get fee tier
        fee_tier = getattr(intent, "fee_tier", Decimal("0.003"))
        if isinstance(fee_tier, int | float):
            fee_tier = Decimal(str(fee_tier))

        calculator = ImpermanentLossCalculator()
        tick_lower, tick_upper = get_lp_tick_range(intent, calculator.price_to_tick)
        if tick_upper <= tick_lower:
            # Degenerate range: widen by one tick so the position has a valid
            # V3 range and non-zero value (same handling as the adapter lane).
            tick_upper = tick_lower + 1

        def price_or_fallback(token: str) -> Decimal:
            try:
                price: Decimal | None = market_state.get_price(token)
            except KeyError:
                price = None
            if price is not None and price > 0:
                return price
            if strict_reproducibility:
                msg = (
                    f"Cannot determine the LP entry price ratio: no positive price available for '{token}'. "
                    "Set strict_reproducibility=False to fall back to $1."
                )
                raise ValueError(msg)
            self._track_fallback("hardcoded_price")
            return Decimal("1")

        price0 = price_or_fallback(token0)
        price1 = price_or_fallback(token1)

        # Entry price ratio: token0 in terms of token1 -- the unit every LP
        # valuation path compares against (SimulatedPosition.lp contract).
        entry_price_ratio = price0 / price1

        liquidity = calculator.liquidity_for_target_value(
            value_token1=amount_usd / price1,
            price=entry_price_ratio,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )
        _, amount0, amount1 = calculator.calculate_il_v3(
            entry_price=entry_price_ratio,
            current_price=entry_price_ratio,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=liquidity,
        )

        position = SimulatedPosition.lp(
            token0=token0,
            token1=token1,
            amount0=amount0,
            amount1=amount1,
            liquidity=liquidity,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            fee_tier=fee_tier,
            entry_price=entry_price_ratio,
            entry_time=timestamp,
            protocol=protocol,
        )
        # Entry amounts anchor the IL hold-value baseline (same contract as
        # the adapter lane's _execute_lp_open).
        position.metadata["entry_amounts"] = {
            token0: str(amount0),
            token1: str(amount1),
        }
        position.metadata["entry_price_ratio"] = str(entry_price_ratio)
        return position

    def _supply_delta(
        self,
        intent: Any,
        protocol: str,
        tokens: list[str],
        executed_price: Decimal,
        timestamp: datetime,
        market_state: MarketState,
        strict_reproducibility: bool,
    ) -> SimulatedPosition:
        """Create the simulated lending position for a SUPPLY intent."""
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        token = tokens[0] if tokens else "WETH"
        amount_usd = self._get_intent_amount_usd(intent, market_state, strict_reproducibility=strict_reproducibility)

        try:
            price = market_state.get_price(token)
            amount = amount_usd / price
        except KeyError:
            amount = amount_usd

        # Get APY if available
        apy = getattr(intent, "apy", Decimal("0.05"))
        if isinstance(apy, int | float):
            apy = Decimal(str(apy))

        return SimulatedPosition.supply(
            token=token,
            amount=amount,
            apy=apy,
            entry_price=executed_price,
            entry_time=timestamp,
            protocol=protocol,
        )

    def _vault_deposit_delta(
        self,
        intent: Any,
        protocol: str,
        tokens: list[str],
        executed_price: Decimal,
        timestamp: datetime,
        market_state: MarketState,
        strict_reproducibility: bool,
    ) -> SimulatedPosition:
        """Create the simulated vault supply position for a VAULT_DEPOSIT intent."""
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        deposit_tok = getattr(intent, "deposit_token", None)
        if deposit_tok:
            token = str(deposit_tok)
        else:
            token = tokens[0] if tokens else "USDC"
            logger.warning(
                "Vault deposit missing deposit_token, defaulting to %s — set deposit_token for accurate backtesting",
                token,
            )
        if isinstance(token, str):
            token = token.upper()
        amount_usd = self._get_intent_amount_usd(intent, market_state, strict_reproducibility=strict_reproducibility)

        try:
            price = market_state.get_price(token)
            amount = amount_usd / price if price > 0 else amount_usd
        except KeyError:
            amount = amount_usd

        # ERC-4626 vault yield: pending PPFS-curve replay via gateway
        # MarketService.GetSharePriceHistory (VIB-3367). Until that ships,
        # honour the strategy-supplied `apy` field on the intent and fall
        # back to a neutral 5% surrogate so existing backtests keep running.
        apy = getattr(intent, "apy", Decimal("0.05"))
        if isinstance(apy, int | float):
            apy = Decimal(str(apy))

        return SimulatedPosition.supply(
            token=token,
            amount=amount,
            apy=apy,
            entry_price=executed_price,
            entry_time=timestamp,
            protocol=protocol,
        )

    def _borrow_delta(
        self,
        intent: Any,
        protocol: str,
        tokens: list[str],
        executed_price: Decimal,
        timestamp: datetime,
        market_state: MarketState,
        strict_reproducibility: bool,
    ) -> SimulatedPosition:
        """Create the simulated borrow position for a BORROW intent."""
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        token = tokens[0] if tokens else "USDC"
        amount_usd = self._get_intent_amount_usd(intent, market_state, strict_reproducibility=strict_reproducibility)

        try:
            price = market_state.get_price(token)
            amount = amount_usd / price
        except KeyError:
            amount = amount_usd

        # Get APY if available
        apy = getattr(intent, "apy", getattr(intent, "borrow_apy", Decimal("0.08")))
        if isinstance(apy, int | float):
            apy = Decimal(str(apy))

        return SimulatedPosition.borrow(
            token=token,
            amount=amount,
            apy=apy,
            entry_price=executed_price,
            entry_time=timestamp,
            protocol=protocol,
        )

    def _perp_open_delta(
        self,
        intent: Any,
        protocol: str,
        tokens: list[str],
        executed_price: Decimal,
        timestamp: datetime,
        market_state: MarketState,
        strict_reproducibility: bool,
    ) -> SimulatedPosition:
        """Create the simulated position for a PERP_OPEN intent.

        The position's collateral comes from the intent's declared collateral
        (or size_usd / leverage), NOT from amount_usd — amount_usd is the
        notional and is only the legacy fallback for duck-typed intents
        without perp fields.
        """
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        from .intent_extraction import get_perp_open_params, intent_is_long

        token = tokens[0] if tokens else "WETH"
        amount_usd = self._get_intent_amount_usd(intent, market_state, strict_reproducibility=strict_reproducibility)
        collateral_usd, leverage = get_perp_open_params(
            intent,
            market_state,
            fallback_amount_usd=amount_usd,
            strict_reproducibility=strict_reproducibility,
            track_fallback=self._track_fallback,
        )

        factory = SimulatedPosition.perp_long if intent_is_long(intent) else SimulatedPosition.perp_short
        return factory(
            token=token,
            collateral_usd=collateral_usd,
            leverage=leverage,
            entry_price=executed_price,
            entry_time=timestamp,
            protocol=protocol,
        )

    def _resolve_position_close(
        self,
        intent: Any,
        intent_type: IntentType,
        portfolio: SimulatedPortfolio,
        amount_usd: Decimal,
        market_state: MarketState,
    ) -> _CloseResolution:
        """Resolve the position a closing intent targets and its notional.

        Perp closes and lending withdraws match against the portfolio's
        open positions (venue position ids never equal simulated ids);
        every other intent type keeps the attribute-based id lookup and
        its extracted notional.
        """
        if intent_type == IntentType.PERP_CLOSE:
            position_close_id, amount_usd = self._resolve_perp_close(intent, portfolio, amount_usd)
            return _CloseResolution(amount_usd=amount_usd, position_close_id=position_close_id)
        if intent_type == IntentType.WITHDRAW:
            return self._resolve_withdraw_close(intent, portfolio, amount_usd, market_state)
        if intent_type == IntentType.REPAY:
            return self._resolve_repay_close(intent, portfolio, amount_usd, market_state)
        return _CloseResolution(amount_usd=amount_usd, position_close_id=self._get_position_close_id(intent))

    def _resolve_withdraw_close(
        self,
        intent: Any,
        portfolio: SimulatedPortfolio,
        amount_usd: Decimal,
        market_state: MarketState,
    ) -> _CloseResolution:
        """Resolve the SUPPLY position a WITHDRAW intent closes or reduces.

        The withdrawn principal must come OUT of the matched supply
        position (VIB-5097): without this linkage the inflow double-counts
        against the still-open position. Matching follows the perp-close
        pattern (exact-id precedence, then FIFO by (token, protocol) via
        :func:`find_lending_close_position_id`).

        Semantics:

        - No open supply position matches -> the fill is rejected
          (``failure_reason``), zero state mutation.
        - ``withdraw_all`` / unresolvable amount / amount >= principal ->
          full close: notional = principal value + accrued interest, the
          interest is realized PnL (mirroring the perp close credit).
        - amount < principal -> partial: the position's principal is
          reduced by the withdrawn token amount; accrued interest stays on
          the position and is realized when it eventually closes in full.
        """
        from .intent_extraction import find_lending_close_position_id

        position_close_id = find_lending_close_position_id(intent, portfolio.positions)
        position = portfolio.get_position(position_close_id) if position_close_id else None
        if position is None:
            return _CloseResolution(
                amount_usd=amount_usd,
                failure_reason="WITHDRAW matched no open supply position to withdraw from",
            )

        # Price the principal the way every valuation path does
        # (entry-price fallback when the market price is unavailable).
        try:
            price = market_state.get_price(position.primary_token)
        except KeyError:
            price = position.entry_price
        if price <= Decimal("0"):
            price = position.entry_price
        principal_value = position.total_amount * price
        total_supply = principal_value + position.interest_accrued

        # Relative dust tolerance: an amount within Decimal round-trip error of
        # the FULL balance (principal + accrued interest) is a full close, not a
        # partial that strands dust principal. Thresholding on total_supply (not
        # principal alone) mirrors the REPAY fix (VIB-5098 / CodeRabbit PR
        # #2777): a withdraw that covers principal but only part of the earned
        # interest must NOT full-close and over-credit the remaining interest.
        full_close_floor = total_supply * (Decimal("1") - Decimal("1E-9"))
        withdraw_all = bool(getattr(intent, "withdraw_all", False))

        # FULL CLOSE: withdraw_all, an unresolvable / non-positive amount, or an
        # amount that covers principal + all earned interest within dust.
        # Notional = principal + interest; the interest realizes as POSITIVE PnL.
        # NOTE: a zero-principal position is NOT force-closed here -- a withdraw
        # >= its earned interest hits this branch via full_close_floor (= the
        # interest), while a smaller withdraw is rejected below rather than
        # over-crediting the whole interest (CodeRabbit PR #2777 round 2; mirror
        # of the REPAY interest-only fix).
        if withdraw_all or amount_usd <= Decimal("0") or amount_usd >= full_close_floor:
            if amount_usd > total_supply:
                logger.warning(
                    "WITHDRAW amount $%s exceeds matched supply $%s (principal $%s + "
                    "accrued interest $%s); capping to the full balance",
                    amount_usd,
                    total_supply,
                    principal_value,
                    position.interest_accrued,
                )
            return _CloseResolution(
                amount_usd=total_supply,
                position_close_id=position.position_id,
                interest_usd=position.interest_accrued,
            )

        # SUB-PRINCIPAL PARTIAL (amount <= principal): the withdrawn inflow
        # comes out of principal only; accrued interest stays on the position
        # and realizes when it eventually closes in full.
        if amount_usd <= principal_value:
            return _CloseResolution(amount_usd=amount_usd, position_reduce_id=position.position_id)

        # BOUNDARY PARTIAL (principal < amount < total_supply): the withdraw
        # takes ALL principal plus PART of the earned interest. Remove the
        # principal in full and realize the withdrawn interest as POSITIVE PnL;
        # the position stays open carrying the unwithdrawn interest remainder.
        principal_tokens = {token: amt for token, amt in position.amounts.items() if amt > Decimal("0")}
        if not principal_tokens:
            # Interest-only position (0 principal): no principal token to reduce,
            # so a withdraw that does not cover the full earned interest cannot
            # be partially settled. Reject (zero state mutation) rather than
            # full-closing it, which would credit the entire interest, not the
            # requested amount.
            return _CloseResolution(
                amount_usd=amount_usd,
                failure_reason=(
                    f"WITHDRAW ${amount_usd} cannot partially settle an interest-only supply position "
                    f"(0 principal, ${position.interest_accrued} accrued interest); "
                    f"withdraw >= the accrued interest to close it"
                ),
            )
        interest_paid = amount_usd - principal_value
        return _CloseResolution(
            amount_usd=amount_usd,
            position_reduce_id=position.position_id,
            interest_usd=interest_paid,
            reduce_amounts=principal_tokens,
        )

    def _resolve_repay_close(
        self,
        intent: Any,
        portfolio: SimulatedPortfolio,
        amount_usd: Decimal,
        market_state: MarketState,
    ) -> _CloseResolution:
        """Resolve the BORROW position a REPAY intent closes or reduces.

        Debt-side mirror of :meth:`_resolve_withdraw_close` (VIB-5098): the
        repaid principal must come OUT of the matched BORROW position --
        without this linkage the outflow debits cash while the debt keeps
        counting against equity (a $2,000 repay burned ~$2,000). Matching
        targets BORROW positions only via
        :func:`find_borrow_close_position_id` (exact-id precedence, then
        FIFO by (token, protocol), fail closed).

        Semantics:

        - No open borrow position matches -> the fill is rejected
          (``failure_reason``), zero state mutation.
        - ``repay_full`` / unresolvable amount / amount >= debt principal ->
          full close: notional = debt principal value + accrued borrow
          interest, the interest realizing as NEGATIVE PnL (the cost of
          having borrowed -- sign-mirrored from the withdraw credit).
        - amount < debt principal -> partial: the position's principal is
          reduced by the repaid token amount; accrued borrow interest stays
          on the position and is realized when it eventually closes in full.
        """
        from .intent_extraction import find_borrow_close_position_id

        position_close_id = find_borrow_close_position_id(intent, portfolio.positions)
        position = portfolio.get_position(position_close_id) if position_close_id else None
        if position is None:
            return _CloseResolution(
                amount_usd=amount_usd,
                failure_reason="REPAY matched no open borrow position to repay",
            )

        # Price the debt principal the way every valuation path does
        # (entry-price fallback when the market price is unavailable).
        try:
            price = market_state.get_price(position.primary_token)
        except KeyError:
            price = position.entry_price
        if price <= Decimal("0"):
            price = position.entry_price
        debt_value = position.total_amount * price
        total_debt = debt_value + position.interest_accrued

        # Relative dust tolerance: an amount within Decimal round-trip error of
        # the FULL debt (principal + accrued interest) is a full close, not a
        # partial that strands dust debt. Thresholding on total_debt (not
        # principal alone) is the VIB-5098 fix: a repay that covers principal
        # but only part of the interest must NOT full-close and overspend the
        # remaining interest (CodeRabbit, PR #2777).
        full_close_floor = total_debt * (Decimal("1") - Decimal("1E-9"))
        repay_full = bool(getattr(intent, "repay_full", False)) or bool(getattr(intent, "repay_all", False))

        # FULL CLOSE: repay_full, an unresolvable / non-positive amount, or an
        # amount that covers principal + all accrued interest within dust.
        # Notional = principal + interest; the interest realizes as NEGATIVE PnL
        # (the cost of having borrowed). NOTE: a zero-principal position is NOT
        # force-closed here -- a repay >= its accrued interest hits this branch
        # via full_close_floor (= the interest), while a smaller repay is
        # rejected below rather than over-paying the whole interest (CodeRabbit
        # PR #2777 round 2; the same over-spend, applied to an interest-only
        # remainder left by a prior boundary partial).
        if repay_full or amount_usd <= Decimal("0") or amount_usd >= full_close_floor:
            if amount_usd > total_debt:
                logger.warning(
                    "REPAY amount $%s exceeds matched debt $%s (principal $%s + "
                    "accrued borrow interest $%s); capping to the full debt",
                    amount_usd,
                    total_debt,
                    debt_value,
                    position.interest_accrued,
                )
            return _CloseResolution(
                amount_usd=total_debt,
                position_close_id=position.position_id,
                interest_usd=-position.interest_accrued,
            )

        # SUB-PRINCIPAL PARTIAL (amount <= principal): the repaid outflow
        # extinguishes principal only; accrued borrow interest stays on the
        # position and realizes when it eventually closes in full.
        if amount_usd <= debt_value:
            return _CloseResolution(amount_usd=amount_usd, position_reduce_id=position.position_id)

        # BOUNDARY PARTIAL (principal < amount < total_debt): the repay covers
        # ALL principal plus PART of the accrued interest. Remove the principal
        # in full and realize the covered interest as NEGATIVE PnL; the position
        # stays open carrying the unpaid interest remainder (VIB-5098).
        principal_tokens = {token: amt for token, amt in position.amounts.items() if amt > Decimal("0")}
        if not principal_tokens:
            # Interest-only position (0 principal): there is no principal token
            # to reduce, so a repay that does not cover the full accrued interest
            # cannot be partially settled through the principal-reduce path.
            # Reject (zero state mutation) rather than full-closing it, which
            # would move the entire interest, not the requested amount.
            return _CloseResolution(
                amount_usd=amount_usd,
                failure_reason=(
                    f"REPAY ${amount_usd} cannot partially settle an interest-only borrow position "
                    f"(0 principal, ${position.interest_accrued} accrued interest); "
                    f"repay >= the accrued interest to close it"
                ),
            )
        interest_paid = amount_usd - debt_value
        return _CloseResolution(
            amount_usd=amount_usd,
            position_reduce_id=position.position_id,
            interest_usd=-interest_paid,
            reduce_amounts=principal_tokens,
        )

    def _resolve_perp_close(
        self,
        intent: Any,
        portfolio: SimulatedPortfolio,
        amount_usd: Decimal,
    ) -> tuple[str | None, Decimal]:
        """Resolve the simulated position a PERP_CLOSE targets and its notional.

        The simulated close machinery is all-or-nothing: a matched position is
        closed in full. A partial ``size_usd`` is honoured as the fee notional
        but logged, since the position itself still closes entirely.

        Args:
            intent: PERP_CLOSE intent object
            portfolio: Portfolio whose open positions are matched against
            amount_usd: Notional extracted from the intent (0 for full closes)

        Returns:
            Tuple of (matched position id or None, effective close notional)
        """
        from .intent_extraction import find_perp_close_position_id

        position_close_id = find_perp_close_position_id(intent, portfolio.positions)
        if position_close_id is None:
            return None, amount_usd
        position = portfolio.get_position(position_close_id)
        if position is None:
            return position_close_id, amount_usd
        if amount_usd <= 0:
            amount_usd = position.notional_usd
        elif amount_usd < position.notional_usd:
            logger.warning(
                "PERP_CLOSE size_usd=%s is below position notional %s; the simulated close "
                "is all-or-nothing and closes the full position",
                amount_usd,
                position.notional_usd,
            )
        elif amount_usd > position.notional_usd:
            # Fees/slippage cannot be charged on notional that does not exist.
            logger.warning(
                "PERP_CLOSE size_usd=%s exceeds matched position notional %s; "
                "capping the close notional to the position",
                amount_usd,
                position.notional_usd,
            )
            amount_usd = position.notional_usd
        return position_close_id, amount_usd

    def _get_position_close_id(self, intent: Any) -> str | None:
        """Get the position ID to close for closing intents.

        Args:
            intent: Intent object

        Returns:
            Position ID string if closing a position, None otherwise
        """
        # Check for position_id attribute
        for attr in ["position_id", "position_to_close", "close_position_id"]:
            if hasattr(intent, attr):
                value = getattr(intent, attr)
                if value and isinstance(value, str):
                    position_id: str = value
                    return position_id

        return None

    def _calculate_metrics(
        self,
        portfolio: SimulatedPortfolio,
        trades: list[TradeRecord],
        config: PnLBacktestConfig,
    ) -> BacktestMetrics:
        """Calculate comprehensive backtest metrics. Delegates to metrics_calculator module."""
        from .metrics_calculator import calculate_metrics

        return calculate_metrics(portfolio, trades, config)

    def _calculate_returns(self, values: list[Decimal]) -> list[Decimal]:
        """Calculate period-over-period returns. Delegates to metrics_calculator module."""
        from .metrics_calculator import calculate_returns

        return calculate_returns(values)

    def _create_gas_price_summary(
        self,
        trades: list[TradeRecord],
    ) -> GasPriceSummary | None:
        """Create gas price summary from trade records. Delegates to metrics_calculator module."""
        from .metrics_calculator import create_gas_price_summary

        return create_gas_price_summary(trades)

    def _calculate_volatility(
        self,
        returns: list[Decimal],
        trading_days: Decimal,
    ) -> Decimal:
        """Calculate annualized volatility. Delegates to metrics_calculator module."""
        from .metrics_calculator import calculate_volatility

        return calculate_volatility(returns, trading_days)

    def _calculate_sharpe_ratio(
        self,
        returns: list[Decimal],
        volatility: Decimal,
        risk_free_rate: Decimal,
        trading_days: Decimal,
    ) -> Decimal:
        """Calculate the Sharpe ratio. Delegates to metrics_calculator module."""
        from .metrics_calculator import calculate_sharpe_ratio

        return calculate_sharpe_ratio(returns, volatility, risk_free_rate, trading_days)

    def _calculate_sortino_ratio(
        self,
        returns: list[Decimal],
        risk_free_rate: Decimal,
        trading_days: Decimal,
    ) -> Decimal:
        """Calculate the Sortino ratio. Delegates to metrics_calculator module."""
        from .metrics_calculator import calculate_sortino_ratio

        return calculate_sortino_ratio(returns, risk_free_rate, trading_days)

    def _calculate_max_drawdown(self, values: list[Decimal]) -> Decimal:
        """Calculate maximum drawdown. Delegates to metrics_calculator module."""
        from .metrics_calculator import calculate_max_drawdown

        return calculate_max_drawdown(values)

    def _decimal_sqrt(self, n: Decimal) -> Decimal:
        """Calculate square root of a Decimal. Delegates to metrics_calculator module."""
        from .metrics_calculator import decimal_sqrt

        return decimal_sqrt(n)


# Dispatch table for PnLBacktester._create_position_delta — per-intent-type
# position factories, mirroring _engine_helpers._SIMPLE_FLOW_HANDLERS
# (Phase 6C.3). Module-level rather than a class attribute: PnLBacktester is
# a @dataclass, where an annotated class-level dict would become a field with
# a mutable default. Values are method NAMES resolved via ``getattr(self, ...)``
# at dispatch time so subclass overrides are honoured; the characterization
# test ``test_position_delta_handler_table_keys`` asserts every name resolves
# to a PnLBacktester callable. Intent types absent from the table do not
# create positions.
_POSITION_DELTA_HANDLERS: dict[IntentType, str] = {
    IntentType.LP_OPEN: "_lp_open_delta",
    IntentType.SUPPLY: "_supply_delta",
    IntentType.VAULT_DEPOSIT: "_vault_deposit_delta",
    IntentType.BORROW: "_borrow_delta",
    IntentType.PERP_OPEN: "_perp_open_delta",
}


__all__ = [
    "PnLBacktester",
    "BacktestableStrategy",
    "DataQualityTracker",
    "create_market_snapshot_from_state",
]
