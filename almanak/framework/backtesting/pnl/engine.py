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

from almanak.framework.backtesting.adapters.base import StrategyBacktestAdapter

# Import adapter registry for strategy type detection
from almanak.framework.backtesting.adapters.registry import (
    StrategyTypeHint,
    detect_strategy_type,
    get_adapter_for_strategy_with_config,
)
from almanak.framework.backtesting.config import BacktestDataConfig
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
    - strategy_id: Unique identifier for the strategy
    - decide(market): Method that returns an intent based on market data

    The decide method can return:
    - An Intent object (SwapIntent, LPIntent, etc.)
    - None (equivalent to HOLD)
    - A DecideResult (for IntentStrategy compatibility)
    """

    @property
    def strategy_id(self) -> str:
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
    chain: str = "arbitrum",
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
        # market.balance("USDC") get the cash balance instead of ValueError
        stablecoin_aliases = frozenset(["USDC", "USDT", "DAI"])
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
            ("gas_price_gwei", config.gas_price_gwei, default_config.gas_price_gwei),
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
                fallback_chain=["historical_provider", "current_provider", "fallback_3000"],
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
            adapter_info = f"'{adapter.adapter_name}' for strategy '{strategy.strategy_id}'"
            if self.data_config is not None:
                logger.info(f"Loaded adapter {adapter_info} with BacktestDataConfig")
            else:
                logger.info(f"Loaded adapter {adapter_info}")
        else:
            self._adapter = None
            logger.debug(f"No adapter loaded for strategy '{strategy.strategy_id}', using generic backtesting")

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
            f"Starting backtest for {strategy.strategy_id} "
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
            try:
                self._adapter.update_position(position, market_state, elapsed_seconds, timestamp)
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
                    # Notify strategy of successful execution so state machines can advance
                    if strategy is not None and hasattr(strategy, "on_intent_executed"):
                        try:
                            callback_result = self._build_callback_result(intent, trade_record, success=True)
                            strategy.on_intent_executed(intent, True, callback_result)
                        except Exception as notify_err:
                            logger.debug(f"on_intent_executed raised: {notify_err}")
                except Exception as e:
                    # Notify strategy of execution failure
                    if strategy is not None and hasattr(strategy, "on_intent_executed"):
                        try:
                            callback_result = self._build_callback_result(intent, None, success=False, error=str(e))
                            strategy.on_intent_executed(intent, False, callback_result)
                        except Exception as notify_err:
                            logger.debug(f"on_intent_executed (failure) raised: {notify_err}")
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

    async def _execute_intent(  # noqa: C901
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
                # Apply the adapter's fill to the portfolio
                portfolio.apply_fill(adapter_fill)

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
        estimated_mev_cost_usd: Decimal | None = None
        if self._mev_simulator is not None:
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

            # Record MEV cost
            estimated_mev_cost_usd = mev_result.mev_cost_usd

            # Add MEV-induced slippage to base slippage
            if mev_result.is_sandwiched:
                slippage_pct = slippage_pct + mev_result.additional_slippage_pct
                logger.debug(
                    f"MEV simulation: Trade sandwiched, additional slippage "
                    f"{mev_result.additional_slippage_pct * 100:.2f}%, "
                    f"MEV cost ${mev_result.mev_cost_usd:.2f}"
                )

        slippage_usd = amount_usd * slippage_pct

        # Calculate gas cost
        gas_cost_usd = Decimal("0")
        gas_price_gwei: Decimal | None = None
        gas_gwei_source: str | None = None  # Track source for trade metadata
        if config.include_gas_costs:
            # Estimate gas used based on intent type
            gas_used = self._estimate_gas_for_intent(intent_type)

            # Get ETH price for gas calculation with priority order:
            # 1. gas_eth_price_override (takes precedence for reproducibility/testing)
            # 2. Historical ETH price (if use_historical_gas_prices enabled)
            # 3. Current market price (WETH or ETH from market_state)
            # No more silent fallback - fail if price unavailable
            eth_price: Decimal | None = None
            gas_price_source = "unknown"

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

            # Track gas price source in data quality metrics
            if data_quality_tracker is not None:
                data_quality_tracker.record_gas_price_source(gas_price_source)

            # Determine gas price in gwei with priority order:
            # 1. Historical gas price from gas_provider (if use_historical_gas_gwei=True)
            # 2. MarketState.gas_price_gwei (if populated by data provider)
            # 3. config.gas_price_gwei (static default)
            gas_price_gwei = config.gas_price_gwei  # Default
            gas_gwei_source = "config"

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

            # Track gas gwei source in fallback usage
            if gas_gwei_source == "config":
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

        # Get position to close if applicable
        position_close_id = self._get_position_close_id(intent)

        # Create the simulated fill
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
            success=True,
            position_delta=position_delta,
            position_close_id=position_close_id,
            metadata={
                "intent": str(intent),
                "slippage_pct": str(slippage_pct),
                "gas_price_source": gas_gwei_source,
            },
            gas_price_gwei=gas_price_gwei,
            estimated_mev_cost_usd=estimated_mev_cost_usd,
            delayed_at_end=delayed_at_end,
        )

        # Apply fill to portfolio
        portfolio.apply_fill(fill)

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

    def _create_position_delta(  # noqa: C901
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
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        if intent_type == IntentType.LP_OPEN:
            # Create LP position
            token0 = tokens[0] if len(tokens) > 0 else "WETH"
            token1 = tokens[1] if len(tokens) > 1 else "USDC"

            amount_usd = self._get_intent_amount_usd(
                intent, market_state, strict_reproducibility=strict_reproducibility
            )
            half = amount_usd / Decimal("2")

            try:
                price0 = market_state.get_price(token0)
                amount0 = half / price0
            except KeyError:
                amount0 = half

            try:
                price1 = market_state.get_price(token1)
                amount1 = half / price1
            except KeyError:
                amount1 = half

            # Get tick range if available
            tick_lower = getattr(intent, "tick_lower", -887272)
            tick_upper = getattr(intent, "tick_upper", 887272)

            # Get fee tier
            fee_tier = getattr(intent, "fee_tier", Decimal("0.003"))
            if isinstance(fee_tier, int | float):
                fee_tier = Decimal(str(fee_tier))

            # Estimate liquidity (simplified)
            liquidity = Decimal(str(amount_usd))

            return SimulatedPosition.lp(
                token0=token0,
                token1=token1,
                amount0=amount0,
                amount1=amount1,
                liquidity=liquidity,
                tick_lower=int(tick_lower) if tick_lower is not None else -887272,
                tick_upper=int(tick_upper) if tick_upper is not None else 887272,
                fee_tier=fee_tier,
                entry_price=executed_price,
                entry_time=timestamp,
                protocol=protocol,
            )

        elif intent_type == IntentType.SUPPLY:
            # Create supply position
            token = tokens[0] if tokens else "WETH"
            amount_usd = self._get_intent_amount_usd(
                intent, market_state, strict_reproducibility=strict_reproducibility
            )

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

        elif intent_type == IntentType.VAULT_DEPOSIT:
            # Create vault supply position (similar to SUPPLY)
            deposit_tok = getattr(intent, "deposit_token", None)
            if deposit_tok:
                token = str(deposit_tok)
            else:
                token = tokens[0] if tokens else "USDC"
                logger.warning(
                    "Vault deposit missing deposit_token, defaulting to %s"
                    " — set deposit_token for accurate backtesting",
                    token,
                )
            if isinstance(token, str):
                token = token.upper()
            amount_usd = self._get_intent_amount_usd(
                intent, market_state, strict_reproducibility=strict_reproducibility
            )

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

        elif intent_type == IntentType.BORROW:
            # Create borrow position
            token = tokens[0] if tokens else "USDC"
            amount_usd = self._get_intent_amount_usd(
                intent, market_state, strict_reproducibility=strict_reproducibility
            )

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

        elif intent_type == IntentType.PERP_OPEN:
            # Create perp position
            token = tokens[0] if tokens else "WETH"
            amount_usd = self._get_intent_amount_usd(
                intent, market_state, strict_reproducibility=strict_reproducibility
            )

            # Get leverage
            leverage = getattr(intent, "leverage", Decimal("1"))
            if isinstance(leverage, int | float):
                leverage = Decimal(str(leverage))

            # Determine if long or short
            is_long = getattr(intent, "is_long", True)
            side = getattr(intent, "side", "long")
            if isinstance(side, str) and side.lower() == "short":
                is_long = False

            if is_long:
                return SimulatedPosition.perp_long(
                    token=token,
                    collateral_usd=amount_usd,
                    leverage=leverage,
                    entry_price=executed_price,
                    entry_time=timestamp,
                    protocol=protocol,
                )
            else:
                return SimulatedPosition.perp_short(
                    token=token,
                    collateral_usd=amount_usd,
                    leverage=leverage,
                    entry_price=executed_price,
                    entry_time=timestamp,
                    protocol=protocol,
                )

        return None

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


__all__ = [
    "PnLBacktester",
    "BacktestableStrategy",
    "DataQualityTracker",
    "create_market_snapshot_from_state",
]
