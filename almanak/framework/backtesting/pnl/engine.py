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
from dataclasses import dataclass, field

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
    BacktestEngine,
    BacktestMetrics,
    BacktestResult,
    DataQualityReport,
    GasPriceRecord,
    GasPriceSummary,
    IntentType,
    ParameterSource,
    ParameterSourceTracker,
    PreflightCheckResult,
    PreflightReport,
    TradeRecord,
)
from almanak.framework.backtesting.pnl.config import PnLBacktestConfig
from almanak.framework.backtesting.pnl.data_provider import (
    HistoricalDataCapability,
    HistoricalDataConfig,
    HistoricalDataProvider,
    MarketState,
)
from almanak.framework.backtesting.pnl.error_handling import (
    BacktestErrorConfig,
    BacktestErrorHandler,
    PreflightValidationError,
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

# Import strategy-related types
from almanak.framework.strategies.intent_strategy import MarketSnapshot

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
        from almanak.framework.strategies.intent_strategy import TokenBalance

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


@dataclass
class DataQualityTracker:
    """Tracks data quality metrics during backtest execution.

    This class accumulates statistics about price lookups and data quality
    throughout the backtest, which are then used to populate the
    DataQualityReport in the BacktestResult.

    Attributes:
        total_price_lookups: Total number of price lookup attempts
        successful_lookups: Number of successful price lookups
        failed_lookups: Number of failed price lookups (KeyError)
        source_counts: Count of price lookups by provider/source name
        stale_data_count: Number of prices marked as stale
        interpolation_count: Number of interpolated/estimated data points
        staleness_threshold_seconds: Threshold for marking data as stale
        unresolved_token_count: Number of token addresses that could not be resolved
        missing_price_count: Number of unique tokens with missing prices during valuation
    """

    total_price_lookups: int = 0
    successful_lookups: int = 0
    failed_lookups: int = 0
    source_counts: dict[str, int] = field(default_factory=dict)
    stale_data_count: int = 0
    interpolation_count: int = 0
    staleness_threshold_seconds: int = 3600
    unresolved_token_count: int = 0
    _unresolved_tokens: set[str] = field(default_factory=set)
    gas_price_source_counts: dict[str, int] = field(default_factory=dict)
    missing_price_count: int = 0
    _missing_price_tokens: set[str] = field(default_factory=set)

    def record_lookup(
        self,
        success: bool,
        source: str = "unknown",
        is_stale: bool = False,
        is_interpolated: bool = False,
    ) -> None:
        """Record a price lookup attempt.

        Args:
            success: Whether the lookup was successful
            source: Name of the data source/provider
            is_stale: Whether the data was older than staleness threshold
            is_interpolated: Whether the data was interpolated/estimated
        """
        self.total_price_lookups += 1

        if success:
            self.successful_lookups += 1
            # Track source breakdown for successful lookups
            self.source_counts[source] = self.source_counts.get(source, 0) + 1

            if is_stale:
                self.stale_data_count += 1

            if is_interpolated:
                self.interpolation_count += 1
        else:
            self.failed_lookups += 1

    def record_successful_tick(self, source: str, token_count: int = 1) -> None:
        """Record a successful tick with prices from a source.

        Args:
            source: Name of the data source/provider
            token_count: Number of tokens with prices in this tick
        """
        for _ in range(token_count):
            self.record_lookup(success=True, source=source)

    def record_failed_tick(self, token_count: int = 1) -> None:
        """Record a failed tick (no price data available).

        Args:
            token_count: Number of tokens expected but missing
        """
        for _ in range(token_count):
            self.record_lookup(success=False)

    def record_unresolved_token(self, token_key: str, chain_id: int | None = None) -> None:
        """Record a token address that could not be resolved to a symbol.

        Tracks unique unresolved tokens to avoid counting the same token multiple times.

        Args:
            token_key: The token address or key that could not be resolved
            chain_id: Optional chain ID for context (included in tracking key)
        """
        # Create a unique key combining chain and token
        tracking_key = f"{chain_id or 'unknown'}:{token_key.lower()}"
        if tracking_key not in self._unresolved_tokens:
            self._unresolved_tokens.add(tracking_key)
            self.unresolved_token_count += 1

    def record_gas_price_source(self, source: str) -> None:
        """Record the source used for gas ETH price in a trade.

        Args:
            source: The gas price source used. Valid values:
                - "override": User-provided gas_eth_price_override value
                - "historical": Historical ETH price from data provider
                - "market": Current market ETH price from market state
        """
        self.gas_price_source_counts[source] = self.gas_price_source_counts.get(source, 0) + 1

    def record_missing_price(
        self,
        token: str,
        timestamp: datetime | None = None,
        chain_id: int | None = None,
    ) -> None:
        """Record a missing price lookup during portfolio valuation.

        Tracks unique (token, chain) pairs to avoid counting the same token multiple times.
        Increments both failed_lookups and the missing_price_tokens set.

        Args:
            token: The token symbol or address for which price was not found
            timestamp: Optional timestamp when the price lookup was attempted
            chain_id: Optional chain ID for context
        """
        # Record as failed lookup
        self.record_lookup(success=False, source="missing")

        # Track unique missing tokens
        tracking_key = f"{chain_id or 'unknown'}:{token.lower()}"
        if tracking_key not in self._missing_price_tokens:
            self._missing_price_tokens.add(tracking_key)
            self.missing_price_count += 1

    @property
    def missing_price_tokens(self) -> list[str]:
        """Get list of unique tokens with missing prices.

        Returns:
            List of unique tokens (as chain_id:token strings) that had missing prices
        """
        return list(self._missing_price_tokens)

    @property
    def coverage_ratio(self) -> Decimal:
        """Calculate the price data coverage ratio.

        Returns:
            Ratio of successful lookups to total lookups (0.0 to 1.0)
        """
        if self.total_price_lookups == 0:
            return Decimal("1.0")  # No lookups means perfect coverage

        return Decimal(str(self.successful_lookups)) / Decimal(str(self.total_price_lookups))

    def to_data_quality_report(self) -> DataQualityReport:
        """Convert tracker statistics to a DataQualityReport.

        Returns:
            DataQualityReport with coverage_ratio, source_breakdown,
            stale_data_count, interpolation_count, unresolved_token_count,
            gas_price_source_counts, missing_price_count, and missing_price_tokens populated
        """
        return DataQualityReport(
            coverage_ratio=self.coverage_ratio,
            source_breakdown=dict(self.source_counts),
            stale_data_count=self.stale_data_count,
            interpolation_count=self.interpolation_count,
            unresolved_token_count=self.unresolved_token_count,
            gas_price_source_counts=dict(self.gas_price_source_counts),
            missing_price_count=self.missing_price_count,
            missing_price_tokens=self.missing_price_tokens,
        )


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

    async def run_preflight_validation(
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
        """Internal backtest implementation. Called by backtest() with guaranteed cleanup."""
        # Initialize parameter_sources early so it's available in error handling
        # It will be populated with proper values in the initialization phase
        parameter_sources: ParameterSourceTracker | None = None

        # Run preflight validation if enabled
        preflight_report: PreflightReport | None = None
        preflight_passed: bool = True  # Default to True if validation is disabled
        if config.preflight_validation:
            with bt_logger.phase("preflight_validation"):
                bt_logger.info("Running preflight validation checks...")
                preflight_report = await self.run_preflight_validation(config)
                preflight_passed = preflight_report.passed

                if preflight_report.passed:
                    bt_logger.info(
                        f"Preflight validation passed: "
                        f"{len(preflight_report.tokens_available)} tokens available, "
                        f"estimated coverage {preflight_report.estimated_coverage:.1%}"
                    )
                else:
                    # Log details about what failed
                    bt_logger.warning(
                        f"Preflight validation issues detected: "
                        f"{preflight_report.error_count} errors, "
                        f"{preflight_report.warning_count} warnings"
                    )
                    for check in preflight_report.failed_checks:
                        bt_logger.warning(f"  - [{check.severity.upper()}] {check.check_name}: {check.message}")

                    # Fail fast if configured to do so
                    if config.fail_on_preflight_error:
                        failed_check_names = [c.check_name for c in preflight_report.failed_checks]
                        raise PreflightValidationError(
                            message=(
                                f"Preflight validation failed with {preflight_report.error_count} errors "
                                f"and {preflight_report.warning_count} warnings. "
                                "Set fail_on_preflight_error=False to continue with degraded mode."
                            ),
                            failed_checks=failed_check_names,
                            recommendations=preflight_report.recommendations,
                            error_count=preflight_report.error_count,
                            warning_count=preflight_report.warning_count,
                        )
                    else:
                        bt_logger.warning(
                            "Continuing in degraded mode (fail_on_preflight_error=False). "
                            "Results may be inaccurate due to data quality issues."
                        )

        # Initialization phase
        with bt_logger.phase("initialization"):
            # Initialize error handler for consistent error classification
            self._error_handler = BacktestErrorHandler(BacktestErrorConfig())
            bt_logger.debug("Initialized BacktestErrorHandler for error classification")

            # Initialize MEV simulator based on config
            self._init_mev_simulator(config)

            # Initialize strategy adapter for strategy-specific backtesting
            self._init_adapter(strategy)

            # Create parameter source tracker for audit trail
            # This must be created after _init_adapter so we can track adapter-specific params
            parameter_sources = self._create_parameter_source_tracker(config)
            bt_logger.debug(
                f"Tracked {len(parameter_sources.records)} parameter sources "
                f"({len(parameter_sources.config_sources)} config, "
                f"{len(parameter_sources.liquidation_sources)} liquidation, "
                f"{len(parameter_sources.apy_funding_sources)} apy/funding)"
            )

            # Initialize portfolio
            portfolio = SimulatedPortfolio(
                initial_capital_usd=config.initial_capital_usd,
            )

            # Create historical data config
            data_config = HistoricalDataConfig(
                start_time=config.start_time,
                end_time=config.end_time,
                interval_seconds=config.interval_seconds,
                tokens=config.tokens,
                chains=[config.chain],
            )

            # Collect data source capabilities and generate warnings
            data_source_capabilities, data_source_warnings = self._collect_data_source_capabilities(bt_logger)

            # Track compliance violations for institutional reporting
            # These indicate potential issues with backtest accuracy/reproducibility
            compliance_violations: list[str] = []

            # Check for CURRENT_ONLY providers which affect historical accuracy
            for provider_name, capability in data_source_capabilities.items():
                if capability == HistoricalDataCapability.CURRENT_ONLY:
                    compliance_violations.append(
                        f"CURRENT_ONLY data provider used: '{provider_name}'. "
                        "Historical prices are not available; backtest uses runtime prices."
                    )

            # Track pending intents for inclusion delay simulation
            # Each entry is (intent, decision_timestamp, blocks_remaining)
            pending_intents: list[tuple[Any, datetime, int]] = []

            # Store the last market state for executing pending intents at simulation end
            # This is needed because we need a valid market state to execute delayed intents
            last_market_state: MarketState | None = None

            # Counter for pending intents executed at simulation end
            execution_delayed_at_end = 0

            # Initialize gas price records tracking (if enabled)
            self._gas_price_records = [] if config.track_gas_prices else None

            # Initialize data quality tracker
            data_quality_tracker = DataQualityTracker(
                staleness_threshold_seconds=config.staleness_threshold_seconds,
            )

            # Initialize indicator engine for populating MarketSnapshot with TA indicators
            # This enables strategies using market.rsi(), market.macd(), market.bollinger_bands()
            # to work identically in live and backtest modes.
            indicator_engine = self._create_indicator_engine(strategy)
            strategy_config = self._get_strategy_config_dict(strategy)

            # Iteration counter for logging
            tick_count = 0
            total_ticks = config.estimated_ticks

        # Simulation phase
        try:
            with bt_logger.phase("simulation"):
                # Iterate through historical data
                async for timestamp, market_state in self.data_provider.iterate(data_config):
                    tick_count += 1

                    # Log progress periodically
                    if tick_count % 100 == 0 or tick_count == 1:
                        bt_logger.info(
                            f"Backtest progress: {tick_count}/{total_ticks} ticks "
                            f"({100 * tick_count / total_ticks:.1f}%)"
                        )

                    # Create market snapshot for strategy
                    snapshot = create_market_snapshot_from_state(
                        market_state=market_state,
                        chain=config.chain,
                        portfolio=portfolio,
                    )

                    # Append prices to indicator engine and populate snapshot
                    tick_tokens: set[str] = set()
                    for token in market_state.available_tokens:
                        try:
                            price = market_state.get_price(token)
                            indicator_engine.append_price(token, price)
                            tick_tokens.add(token)
                        except KeyError:
                            pass
                    indicator_engine.populate_snapshot(snapshot, strategy_config, active_tokens=tick_tokens)

                    # Track data quality: record successful price lookups
                    # Count tokens with available prices in this tick
                    available_tokens = market_state.available_tokens
                    expected_tokens = config.tokens
                    provider_name = getattr(self.data_provider, "provider_name", "unknown")

                    # Record successful lookups for each available token
                    for token in expected_tokens:
                        if token.upper() in [t.upper() for t in available_tokens]:
                            data_quality_tracker.record_lookup(
                                success=True,
                                source=provider_name,
                            )
                        else:
                            data_quality_tracker.record_lookup(success=False)

                    # Execute any pending intents that have waited long enough
                    pending_intents = await self._process_pending_intents(
                        pending_intents=pending_intents,
                        portfolio=portfolio,
                        market_state=market_state,
                        config=config,
                        data_quality_tracker=data_quality_tracker,
                        strategy=strategy,
                    )

                    # Get strategy decision
                    try:
                        decide_result = strategy.decide(snapshot)
                    except Exception as e:
                        # Check if this is an indicator warm-up error (expected during initial ticks).
                        # The indicator engine's is_warming_up() is the authoritative signal:
                        # if the engine hasn't accumulated enough data points AND the strategy
                        # raised a ValueError, it's almost certainly because indicators aren't
                        # ready yet (e.g. "Cannot calculate RSI", "MACD data not available").
                        # We only suppress ValueError to avoid masking real bugs (AttributeError,
                        # KeyError, etc.).
                        is_warmup = isinstance(e, ValueError) and any(
                            indicator_engine.is_warming_up(t, strategy_config) for t in tick_tokens
                        )
                        if is_warmup:
                            # Expected: not enough data points yet for indicators.
                            # Log at debug (not warning) to avoid alarming users.
                            bt_logger.debug(f"Tick {tick_count}: indicator warm-up ({e}) - holding")
                        elif self._error_handler:
                            # Use error handler for consistent classification
                            result = self._error_handler.handle_error(
                                e,
                                context=f"strategy_decide:tick_{tick_count}:{timestamp.isoformat()}",
                            )
                            if result.should_stop:
                                raise RuntimeError(f"Fatal error in strategy.decide() at tick {tick_count}: {e}") from e
                            # Non-fatal: log warning and continue with hold
                            bt_logger.warning(
                                f"Strategy decide() error at tick {tick_count}: {e} - continuing with hold"
                            )
                        else:
                            bt_logger.warning(f"Strategy decide() raised exception at {timestamp}: {e}")
                        decide_result = None

                    # Extract intent from decide result
                    intent = self._extract_intent(decide_result)

                    # Queue intent for execution (with inclusion delay)
                    if intent is not None and not self._is_hold_intent(intent):
                        pending_intents.append((intent, timestamp, config.inclusion_delay_blocks))

                    # Update positions via adapter if available
                    self._update_positions_via_adapter(portfolio, market_state, timestamp)

                    # Mark portfolio to market (uses adapter for valuation if available)
                    portfolio.mark_to_market(market_state, timestamp, adapter=self._adapter)

                    # Store the market state for use after simulation completes
                    last_market_state = market_state  # noqa: F841 (used in US-062b)

                # Execute any remaining pending intents at end of simulation
                # (Use last market state for final execution)
                if pending_intents and last_market_state is not None:
                    bt_logger.warning(
                        f"Executing {len(pending_intents)} pending intent(s) at simulation end "
                        f"(delayed execution using last market state from {last_market_state.timestamp})"
                    )
                    for intent, decision_time, _ in pending_intents:
                        try:
                            trade_record = await self._execute_intent(
                                intent=intent,
                                portfolio=portfolio,
                                market_state=last_market_state,
                                timestamp=last_market_state.timestamp,
                                config=config,
                                delayed_at_end=True,
                                data_quality_tracker=data_quality_tracker,
                            )
                            execution_delayed_at_end += 1
                            # Record successful execution in error handler
                            if self._error_handler:
                                self._error_handler.record_success()
                            bt_logger.debug(
                                f"Executed pending intent at simulation end "
                                f"(decided at {decision_time}): "
                                f"type={trade_record.intent_type.value}, "
                                f"amount=${trade_record.amount_usd:,.2f}"
                            )
                            # Notify strategy of successful execution
                            if hasattr(strategy, "on_intent_executed"):
                                try:
                                    strategy.on_intent_executed(intent, True, trade_record)
                                except Exception as notify_err:
                                    bt_logger.debug(f"on_intent_executed raised: {notify_err}")
                        except Exception as e:
                            # Notify strategy of execution failure
                            if hasattr(strategy, "on_intent_executed"):
                                try:
                                    strategy.on_intent_executed(intent, False, str(e))
                                except Exception as notify_err:
                                    bt_logger.debug(f"on_intent_executed (failure) raised: {notify_err}")
                            # Use error handler for intent execution errors
                            if self._error_handler:
                                result = self._error_handler.handle_error(
                                    e,
                                    context=f"execute_pending_intent:end:{type(intent).__name__}",
                                )
                                if result.should_stop:
                                    bt_logger.error(f"Fatal error executing pending intent at simulation end: {e}")
                                    raise
                                # Non-fatal: log warning and skip this intent
                                bt_logger.warning(f"Failed to execute pending intent at simulation end: {e} - skipping")
                            else:
                                bt_logger.warning(f"Failed to execute pending intent at simulation end: {e}")
                elif pending_intents:
                    bt_logger.warning(
                        f"Cannot execute {len(pending_intents)} remaining pending intents: "
                        "no valid market state available"
                    )

        except Exception as e:
            # Use error handler for consistent classification and tracking
            error_summary: dict[str, Any] = {}
            if self._error_handler:
                result = self._error_handler.handle_error(
                    e,
                    context="simulation_phase:main_loop",
                )
                error_summary = self._error_handler.get_error_summary()
                bt_logger.error(
                    f"Backtest failed with {result.error_record.classification.error_type.value if result.error_record else 'unknown'} error: {e}"
                )
            else:
                bt_logger.error(f"Backtest failed with error: {e}")

            run_ended_at = datetime.now(UTC)
            # On error, compliance is False and we add the error as a violation
            error_compliance_violations = compliance_violations + [f"Backtest failed with error: {e}"]
            error_fallback_usage = self._fallback_usage.copy() if self._fallback_usage else {}
            return BacktestResult(
                engine=BacktestEngine.PNL,
                strategy_id=strategy.strategy_id,
                start_time=config.start_time,
                end_time=config.end_time,
                metrics=BacktestMetrics(),
                initial_capital_usd=config.initial_capital_usd,
                final_capital_usd=config.initial_capital_usd,
                chain=config.chain,
                run_started_at=run_started_at,
                run_ended_at=run_ended_at,
                run_duration_seconds=(run_ended_at - run_started_at).total_seconds(),
                config=config.to_dict_with_metadata(data_provider_info=self._get_data_provider_info()),
                error=str(e),
                backtest_id=backtest_id,
                phase_timings=[t.to_dict() for t in bt_logger.phase_timings],
                config_hash=config.calculate_config_hash(),
                errors=self._error_handler.get_errors_as_dicts() if self._error_handler else [],
                data_source_capabilities=data_source_capabilities,
                data_source_warnings=data_source_warnings,
                data_quality=data_quality_tracker.to_data_quality_report(),
                institutional_compliance=False,
                compliance_violations=error_compliance_violations,
                fallback_usage=error_fallback_usage,
                preflight_report=preflight_report,
                preflight_passed=preflight_passed,
                gas_prices_used=self._gas_price_records or [],
                gas_price_summary=None,  # No trades on error
                parameter_sources=parameter_sources,
            )

        # Data quality gate enforcement - check coverage ratio after simulation
        coverage_ratio = data_quality_tracker.coverage_ratio
        if coverage_ratio < config.min_data_coverage:
            # Track as compliance violation regardless of institutional_mode
            compliance_violations.append(
                f"Data coverage below minimum threshold: {coverage_ratio:.2%} < {config.min_data_coverage:.2%} "
                f"({data_quality_tracker.successful_lookups}/{data_quality_tracker.total_price_lookups} "
                f"successful price lookups)"
            )

            if config.institutional_mode:
                error_msg = (
                    f"Data quality gate failed in institutional mode: "
                    f"coverage ratio {coverage_ratio:.2%} is below minimum threshold "
                    f"{config.min_data_coverage:.2%}. "
                    f"({data_quality_tracker.successful_lookups}/{data_quality_tracker.total_price_lookups} "
                    f"successful price lookups)"
                )
                bt_logger.error(error_msg)
                raise ValueError(error_msg)
            else:
                # Not in institutional mode - log warning only
                bt_logger.warning(
                    f"Data coverage below threshold: {coverage_ratio:.2%} < {config.min_data_coverage:.2%}. "
                    f"({data_quality_tracker.successful_lookups}/{data_quality_tracker.total_price_lookups} "
                    f"successful price lookups). "
                    f"Enable institutional_mode=True to enforce data quality requirements."
                )
        elif config.institutional_mode:
            bt_logger.info(
                f"Data quality gate passed in institutional mode: "
                f"coverage ratio {coverage_ratio:.2%} >= {config.min_data_coverage:.2%}"
            )

        # Metrics calculation phase
        with bt_logger.phase("metrics_calculation"):
            metrics = self._calculate_metrics(portfolio, portfolio.trades, config)

            # Get final portfolio value
            final_value = portfolio.equity_curve[-1].value_usd if portfolio.equity_curve else config.initial_capital_usd

        run_ended_at = datetime.now(UTC)

        bt_logger.info(
            f"Backtest completed for {strategy.strategy_id}: "
            f"PnL=${metrics.net_pnl_usd:,.2f}, "
            f"Return={metrics.total_return_pct * 100:.2f}%, "
            f"Sharpe={metrics.sharpe_ratio:.3f}"
        )

        # Log phase summary
        phase_summary = bt_logger.get_phase_summary()
        bt_logger.info(f"Phase timing summary - Total: {phase_summary['total_duration_seconds']:.2f}s")

        # Log error summary if any non-fatal errors occurred
        if self._error_handler and self._error_handler.error_count > 0:
            error_summary = self._error_handler.get_error_summary()
            bt_logger.info(
                f"Error summary: {error_summary['total_errors']} total "
                f"({error_summary['non_critical_errors']} non-critical, "
                f"{error_summary['recoverable_errors']} recoverable)"
            )

        # Get fallback usage and add compliance violations for any fallbacks used
        fallback_usage = self._fallback_usage.copy() if self._fallback_usage else {}

        if fallback_usage.get("hardcoded_price", 0) > 0:
            count = fallback_usage["hardcoded_price"]
            compliance_violations.append(
                f"Hardcoded price fallback used {count} time(s). "
                "Set strict_reproducibility=True for institutional-grade backtests."
            )
        if fallback_usage.get("default_gas_price", 0) > 0:
            count = fallback_usage["default_gas_price"]
            compliance_violations.append(f"Default gas price fallback used {count} time(s).")
        if fallback_usage.get("default_usd_amount", 0) > 0:
            count = fallback_usage["default_usd_amount"]
            compliance_violations.append(
                f"Default USD amount fallback used {count} time(s). "
                "Set strict_reproducibility=True for institutional-grade backtests."
            )

        # Determine institutional compliance status
        # Compliance is True only if there are no violations
        institutional_compliance = len(compliance_violations) == 0

        return BacktestResult(
            engine=BacktestEngine.PNL,
            strategy_id=strategy.strategy_id,
            start_time=config.start_time,
            end_time=config.end_time,
            metrics=metrics,
            trades=portfolio.trades,
            equity_curve=portfolio.equity_curve,
            initial_capital_usd=config.initial_capital_usd,
            final_capital_usd=final_value,
            chain=config.chain,
            run_started_at=run_started_at,
            run_ended_at=run_ended_at,
            run_duration_seconds=(run_ended_at - run_started_at).total_seconds(),
            config=config.to_dict_with_metadata(data_provider_info=self._get_data_provider_info()),
            backtest_id=backtest_id,
            phase_timings=[t.to_dict() for t in bt_logger.phase_timings],
            config_hash=config.calculate_config_hash(),
            errors=self._error_handler.get_errors_as_dicts() if self._error_handler else [],
            execution_delayed_at_end=execution_delayed_at_end,
            data_source_capabilities=data_source_capabilities,
            data_source_warnings=data_source_warnings,
            data_quality=data_quality_tracker.to_data_quality_report(),
            institutional_compliance=institutional_compliance,
            compliance_violations=compliance_violations,
            fallback_usage=fallback_usage,
            preflight_report=preflight_report,
            preflight_passed=preflight_passed,
            gas_prices_used=self._gas_price_records or [],
            gas_price_summary=self._create_gas_price_summary(portfolio.trades),
            parameter_sources=parameter_sources,
            data_coverage_metrics=portfolio.calculate_data_coverage_metrics(),
        )

    def _extract_intent(self, decide_result: Any) -> Any:
        """Extract the intent from a decide() result.

        The decide() method can return various types:
        - An Intent object directly
        - None (equivalent to HOLD)
        - A DecideResult with .intent attribute
        - A HoldIntent

        Args:
            decide_result: Raw result from strategy.decide()

        Returns:
            The intent object, or None if no action
        """
        if decide_result is None:
            return None

        # Check if it's a DecideResult with an intent attribute
        if hasattr(decide_result, "intent"):
            return decide_result.intent

        # Check if it's a DecideResult tuple-like (intent, context)
        if isinstance(decide_result, tuple) and len(decide_result) >= 1:
            return decide_result[0]

        # Otherwise, assume it's an intent directly
        return decide_result

    def _is_hold_intent(self, intent: Any) -> bool:
        """Check if an intent is a HOLD intent.

        Args:
            intent: Intent to check

        Returns:
            True if this is a hold/no-action intent
        """
        if intent is None:
            return True

        # Check intent_type attribute
        if hasattr(intent, "intent_type"):
            intent_type = intent.intent_type
            if hasattr(intent_type, "value"):
                is_hold: bool = intent_type.value == "HOLD"
                return is_hold
            is_hold_str: bool = str(intent_type) == "HOLD"
            return is_hold_str

        # Check if it's a HoldIntent class
        if hasattr(intent, "__class__"):
            class_name: str = intent.__class__.__name__
            if class_name == "HoldIntent":
                return True

        return False

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
                            strategy.on_intent_executed(intent, True, trade_record)
                        except Exception as notify_err:
                            logger.debug(f"on_intent_executed raised: {notify_err}")
                except Exception as e:
                    # Notify strategy of execution failure
                    if strategy is not None and hasattr(strategy, "on_intent_executed"):
                        try:
                            strategy.on_intent_executed(intent, False, str(e))
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
        """Extract the IntentType from an intent object.

        Args:
            intent: Intent object

        Returns:
            IntentType enum value
        """
        # Check for intent_type attribute
        if hasattr(intent, "intent_type"):
            intent_type_value = intent.intent_type
            # If it's already an IntentType, return it
            if isinstance(intent_type_value, IntentType):
                return intent_type_value
            # If it has a value attribute (enum from another module)
            if hasattr(intent_type_value, "value"):
                try:
                    return IntentType(intent_type_value.value)
                except ValueError:
                    pass
            # Try direct conversion
            try:
                return IntentType(str(intent_type_value))
            except ValueError:
                pass

        # Check class name for common intent types
        class_name = intent.__class__.__name__.upper()
        if "SWAP" in class_name:
            return IntentType.SWAP
        if "LP_OPEN" in class_name or "LPOPEN" in class_name:
            return IntentType.LP_OPEN
        if "LP_CLOSE" in class_name or "LPCLOSE" in class_name:
            return IntentType.LP_CLOSE
        if "PERP_OPEN" in class_name or "PERPOPEN" in class_name:
            return IntentType.PERP_OPEN
        if "PERP_CLOSE" in class_name or "PERPCLOSE" in class_name:
            return IntentType.PERP_CLOSE
        if "SUPPLY" in class_name:
            return IntentType.SUPPLY
        if "WITHDRAW" in class_name:
            return IntentType.WITHDRAW
        if "BORROW" in class_name:
            return IntentType.BORROW
        if "REPAY" in class_name:
            return IntentType.REPAY
        if "BRIDGE" in class_name:
            return IntentType.BRIDGE
        if "VAULTDEPOSIT" in class_name or "VAULT_DEPOSIT" in class_name:
            return IntentType.VAULT_DEPOSIT
        if "VAULTREDEEM" in class_name or "VAULT_REDEEM" in class_name:
            return IntentType.VAULT_REDEEM
        if "HOLD" in class_name:
            return IntentType.HOLD

        return IntentType.UNKNOWN

    def _get_intent_protocol(self, intent: Any) -> str:
        """Extract the protocol from an intent object.

        Args:
            intent: Intent object

        Returns:
            Protocol name string
        """
        # Common attribute names for protocol
        for attr in ["protocol", "protocol_name", "connector", "adapter"]:
            if hasattr(intent, attr):
                value = getattr(intent, attr)
                if value and isinstance(value, str):
                    protocol_str: str = value.lower()
                    return protocol_str

        # Infer from class name
        class_name = intent.__class__.__name__.lower()
        if "uniswap" in class_name:
            return "uniswap_v3"
        if "gmx" in class_name:
            return "gmx"
        if "aave" in class_name:
            return "aave_v3"
        if "hyperliquid" in class_name:
            return "hyperliquid"
        if "across" in class_name or "stargate" in class_name:
            return "bridge"

        return "default"

    def _get_intent_tokens(self, intent: Any) -> list[str]:
        """Extract the tokens involved in an intent.

        Args:
            intent: Intent object

        Returns:
            List of token symbols
        """
        tokens: list[str] = []

        # Common attribute names for tokens
        for attr in [
            "token",
            "from_token",
            "to_token",
            "token0",
            "token1",
            "asset",
            "collateral",
            "borrow_token",
            "supply_token",
            "deposit_token",
        ]:
            if hasattr(intent, attr):
                value = getattr(intent, attr)
                if value and isinstance(value, str) and value not in tokens:
                    tokens.append(value.upper())

        # Check for tokens list attribute
        if hasattr(intent, "tokens"):
            intent_tokens = intent.tokens
            if isinstance(intent_tokens, list):
                for t in intent_tokens:
                    if isinstance(t, str) and t.upper() not in tokens:
                        tokens.append(t.upper())

        return tokens if tokens else ["UNKNOWN"]

    def _get_intent_amount_usd(
        self,
        intent: Any,
        market_state: MarketState,
        strict_reproducibility: bool = False,
    ) -> Decimal:
        """Extract or calculate the USD amount for an intent.

        Args:
            intent: Intent object
            market_state: Market state for price lookups
            strict_reproducibility: If True, raise ValueError when USD amount cannot
                be determined. If False, log warning and return raw amount or zero.

        Returns:
            Amount in USD

        Raises:
            ValueError: If strict_reproducibility is True and USD amount cannot be
                determined (no USD field, no price available, or no amount field).
        """
        # Check for direct USD amount
        for attr in ["amount_usd", "notional_usd", "value_usd", "collateral_usd"]:
            if hasattr(intent, attr):
                value = getattr(intent, attr)
                if value is not None:
                    return Decimal(str(value))

        # Check for amount + token (need to convert to USD)
        amount: Decimal | None = None
        token: str | None = None

        for amount_attr in ["amount", "amount_in", "amount_out", "collateral", "size", "shares"]:
            if hasattr(intent, amount_attr):
                value = getattr(intent, amount_attr)
                if value is not None:
                    str_value = str(value)
                    if str_value.lower() == "all":
                        continue
                    try:
                        amount = Decimal(str_value)
                    except Exception:
                        continue
                    break

        for token_attr in ["token", "from_token", "asset", "collateral_token", "deposit_token"]:
            if hasattr(intent, token_attr):
                value = getattr(intent, token_attr)
                if value and isinstance(value, str):
                    token = value.upper()
                    break

        if amount is not None and token:
            try:
                price = market_state.get_price(token)
                return amount * price
            except KeyError as err:
                # Can't convert to USD without price - handle based on strict mode
                if strict_reproducibility:
                    msg = (
                        f"Cannot determine USD amount for intent: found amount={amount} for token '{token}' "
                        "but no price available. Set strict_reproducibility=False to use zero as fallback."
                    )
                    raise ValueError(msg) from err
                logger.warning(
                    f"No price available for token '{token}' to convert amount {amount} to USD. "
                    "Using zero as fallback to avoid misinterpreting token amount as USD."
                )
                self._track_fallback("default_usd_amount")
                return Decimal("0")

        # Could not determine USD amount - handle based on strict mode
        if amount is not None:
            # Have raw amount but no token for price lookup
            if strict_reproducibility:
                msg = (
                    f"Cannot determine USD amount for intent: found amount={amount} but no token "
                    "for price lookup. Set strict_reproducibility=False to use zero as fallback."
                )
                raise ValueError(msg)
            logger.warning(
                f"Intent has amount={amount} but no token for USD conversion. "
                "Using zero as fallback to avoid misinterpreting token amount as USD."
            )
            self._track_fallback("default_usd_amount")
            return Decimal("0")

        # No amount found at all
        if strict_reproducibility:
            msg = (
                "Cannot determine USD amount for intent: no USD amount field and no "
                "token amount found. Set strict_reproducibility=False to use zero as fallback."
            )
            raise ValueError(msg)
        logger.warning(
            "Intent has no USD amount or token amount field. Using zero as fallback to avoid arbitrary values."
        )
        self._track_fallback("default_usd_amount")
        return Decimal("0")

    def _estimate_gas_for_intent(self, intent_type: IntentType) -> int:
        """Estimate gas usage for an intent type.

        Args:
            intent_type: Type of intent

        Returns:
            Estimated gas units
        """
        # Gas estimates based on typical transaction costs across protocols.
        # These are conservative estimates for gas cost calculations in backtests.
        # Actual gas usage varies by protocol, chain, and execution conditions.
        #
        # Uniswap V3 swaps: ~130k-180k (depends on pools in route)
        # Aave V3 supply/withdraw: ~200k-250k
        # GMX V2 market orders: ~300k-500k
        # LP operations: ~250k-400k
        gas_estimates: dict[IntentType, int] = {
            IntentType.SWAP: 180000,  # Conservative for multi-hop swaps
            IntentType.LP_OPEN: 400000,  # NFT mint + liquidity add
            IntentType.LP_CLOSE: 300000,  # NFT burn + liquidity remove
            IntentType.SUPPLY: 220000,  # Aave/Compound supply
            IntentType.WITHDRAW: 220000,  # Aave/Compound withdraw
            IntentType.BORROW: 280000,  # Includes collateral checks
            IntentType.REPAY: 220000,  # Aave/Compound repay
            IntentType.PERP_OPEN: 450000,  # GMX V2 market increase
            IntentType.PERP_CLOSE: 350000,  # GMX V2 market decrease
            IntentType.BRIDGE: 200000,  # Cross-chain bridge
            IntentType.VAULT_DEPOSIT: 250000,  # ERC-4626 deposit (approve + deposit)
            IntentType.VAULT_REDEEM: 200000,  # ERC-4626 redeem
            IntentType.HOLD: 0,  # No execution
            IntentType.UNKNOWN: 200000,  # Conservative default
        }
        return gas_estimates.get(intent_type, 200000)

    def _get_executed_price(
        self,
        intent: Any,
        market_state: MarketState,
        slippage_pct: Decimal,
        intent_type: IntentType,
    ) -> Decimal:
        """Get the executed price for an intent after applying slippage.

        For swaps and perps, the executed price is the market price adjusted
        for slippage. For other intent types, we use the market price directly.

        Args:
            intent: Intent object
            market_state: Market state for price lookups
            slippage_pct: Slippage percentage as decimal
            intent_type: Type of intent

        Returns:
            Executed price after slippage
        """
        # Get the primary token for price lookup
        tokens = self._get_intent_tokens(intent)
        primary_token = tokens[0] if tokens else "WETH"

        # Get market price
        try:
            market_price = market_state.get_price(primary_token)
        except KeyError:
            market_price = Decimal("1")

        # Apply slippage for market orders
        if intent_type in (IntentType.SWAP, IntentType.PERP_OPEN, IntentType.PERP_CLOSE):
            # Slippage is adverse: buying gets a higher price, selling gets a lower price.
            # primary_token = tokens[0], which for swaps is from_token (the token being sold).
            # Determine direction by checking to_token: if the intent has a to_token that
            # matches primary_token, we're BUYING it (pay more). Otherwise we're selling it
            # (receive less).
            to_token = getattr(intent, "to_token", None)
            if to_token and to_token.upper() == primary_token.upper():
                # Buying primary_token: adverse slippage means higher price
                return market_price * (Decimal("1") + slippage_pct)
            # Selling primary_token (or no to_token): adverse slippage means lower price
            return market_price * (Decimal("1") - slippage_pct)

        return market_price

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

        Args:
            intent: Intent object
            intent_type: Type of intent
            amount_usd: USD amount of the trade
            executed_price: Executed price
            fee_usd: Fee in USD
            slippage_usd: Slippage cost in USD
            market_state: Market state for prices

        Returns:
            Tuple of (tokens_in, tokens_out) dicts
        """
        tokens_in: dict[str, Decimal] = {}
        tokens_out: dict[str, Decimal] = {}

        if intent_type == IntentType.SWAP:
            # For swaps, we send one token and receive another
            from_token = getattr(intent, "from_token", "USDC")
            to_token = getattr(intent, "to_token", "WETH")

            if isinstance(from_token, str):
                from_token = from_token.upper()
            if isinstance(to_token, str):
                to_token = to_token.upper()

            # Amount out is the trade amount
            amount_out = amount_usd
            try:
                from_price = market_state.get_price(from_token)
                if from_price > 0:
                    tokens_out[from_token] = amount_out / from_price
            except KeyError:
                tokens_out[from_token] = amount_out  # Assume $1 price

            # Amount in is after fees and slippage
            amount_in_usd = amount_usd - fee_usd - slippage_usd
            try:
                to_price = market_state.get_price(to_token)
                if to_price > 0:
                    tokens_in[to_token] = amount_in_usd / to_price
            except KeyError:
                tokens_in[to_token] = amount_in_usd  # Assume $1 price

        elif intent_type == IntentType.SUPPLY:
            # Supply: we send tokens to the protocol
            token = getattr(intent, "token", getattr(intent, "asset", "WETH"))
            if isinstance(token, str):
                token = token.upper()

            try:
                price = market_state.get_price(token)
                if price > 0:
                    tokens_out[token] = amount_usd / price
            except KeyError:
                tokens_out[token] = amount_usd

        elif intent_type == IntentType.WITHDRAW:
            # Withdraw: we receive tokens from the protocol
            token = getattr(intent, "token", getattr(intent, "asset", "WETH"))
            if isinstance(token, str):
                token = token.upper()

            try:
                price = market_state.get_price(token)
                if price > 0:
                    tokens_in[token] = amount_usd / price
            except KeyError:
                tokens_in[token] = amount_usd

        elif intent_type == IntentType.BORROW:
            # Borrow: we receive borrowed tokens
            token = getattr(intent, "token", getattr(intent, "asset", "USDC"))
            if isinstance(token, str):
                token = token.upper()

            try:
                price = market_state.get_price(token)
                if price > 0:
                    tokens_in[token] = amount_usd / price
            except KeyError:
                tokens_in[token] = amount_usd

        elif intent_type == IntentType.REPAY:
            # Repay: we send tokens to pay back debt
            token = getattr(intent, "token", getattr(intent, "asset", "USDC"))
            if isinstance(token, str):
                token = token.upper()

            try:
                price = market_state.get_price(token)
                if price > 0:
                    tokens_out[token] = amount_usd / price
            except KeyError:
                tokens_out[token] = amount_usd

        elif intent_type == IntentType.LP_OPEN:
            # LP Open: we send both tokens to the pool
            token0 = getattr(intent, "token0", getattr(intent, "token_a", "WETH"))
            token1 = getattr(intent, "token1", getattr(intent, "token_b", "USDC"))

            if isinstance(token0, str):
                token0 = token0.upper()
            if isinstance(token1, str):
                token1 = token1.upper()

            # Split the USD amount roughly 50/50
            half_amount = amount_usd / Decimal("2")

            try:
                price0 = market_state.get_price(token0)
                tokens_out[token0] = half_amount / price0
            except KeyError:
                tokens_out[token0] = half_amount

            try:
                price1 = market_state.get_price(token1)
                tokens_out[token1] = half_amount / price1
            except KeyError:
                tokens_out[token1] = half_amount

        elif intent_type == IntentType.LP_CLOSE:
            # LP Close: we receive tokens back from the pool
            token0 = getattr(intent, "token0", getattr(intent, "token_a", "WETH"))
            token1 = getattr(intent, "token1", getattr(intent, "token_b", "USDC"))

            if isinstance(token0, str):
                token0 = token0.upper()
            if isinstance(token1, str):
                token1 = token1.upper()

            # Approximate tokens received (actual depends on IL)
            half_amount = amount_usd / Decimal("2")

            try:
                price0 = market_state.get_price(token0)
                tokens_in[token0] = half_amount / price0
            except KeyError:
                tokens_in[token0] = half_amount

            try:
                price1 = market_state.get_price(token1)
                tokens_in[token1] = half_amount / price1
            except KeyError:
                tokens_in[token1] = half_amount

        elif intent_type in {IntentType.VAULT_DEPOSIT, IntentType.VAULT_REDEEM}:
            # Vault deposit/redeem: underlying asset flows to/from vault
            token = getattr(intent, "deposit_token", None)
            if not token:
                token = "USDC"
                logger.warning(
                    "Vault intent missing deposit_token, defaulting to USDC"
                    " — set deposit_token for accurate backtesting"
                )
            if isinstance(token, str):
                token = token.upper()

            try:
                price = market_state.get_price(token)
                amount = amount_usd / price if price > 0 else amount_usd
            except KeyError:
                amount = amount_usd

            if intent_type == IntentType.VAULT_DEPOSIT:
                tokens_out[token] = amount
            else:
                tokens_in[token] = amount

        # For PERP and other types, token flows are handled via collateral

        return tokens_in, tokens_out

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

            # MetaMorpho vaults typically yield ~3-8% APY
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
        """Calculate comprehensive backtest metrics from portfolio and trades.

        This method consolidates metric calculations from the portfolio's equity
        curve and trade records, applying configuration settings such as:
        - Risk-free rate for Sharpe ratio calculation
        - Trading days per year for annualization

        The metrics calculated include:
        - PnL metrics: total_pnl_usd, net_pnl_usd, total_return_pct, annualized_return_pct
        - Risk metrics: sharpe_ratio, sortino_ratio, max_drawdown_pct, volatility, calmar_ratio
        - Trade metrics: win_rate, profit_factor, total_trades, winning_trades, losing_trades
        - Cost metrics: total_fees_usd, total_slippage_usd, total_gas_usd
        - Trade stats: avg_trade_pnl_usd, largest_win_usd, largest_loss_usd, avg_win_usd, avg_loss_usd

        Args:
            portfolio: SimulatedPortfolio with equity curve and trades
            trades: List of TradeRecord from the backtest
            config: PnLBacktestConfig with risk_free_rate and trading_days_per_year

        Returns:
            BacktestMetrics with all calculated performance metrics
        """
        if not portfolio.equity_curve:
            return BacktestMetrics()

        # Extract values for calculations
        equity_values = [p.value_usd for p in portfolio.equity_curve]
        timestamps = [p.timestamp for p in portfolio.equity_curve]

        # Initial and final values
        initial_value = equity_values[0] if equity_values else config.initial_capital_usd
        final_value = equity_values[-1] if equity_values else config.initial_capital_usd

        # Total PnL (before costs - costs are tracked separately)
        total_pnl = final_value - initial_value

        # Execution costs from trades
        total_fees = sum((t.fee_usd for t in trades), Decimal("0"))
        total_slippage = sum((t.slippage_usd for t in trades), Decimal("0"))
        total_gas = sum((t.gas_cost_usd for t in trades), Decimal("0"))

        # MEV costs from trades (only non-None values)
        total_mev = sum(
            (t.estimated_mev_cost_usd for t in trades if t.estimated_mev_cost_usd is not None),
            Decimal("0"),
        )

        # Gas price statistics from trades
        gas_prices = [t.gas_price_gwei for t in trades if t.gas_price_gwei is not None]
        avg_gas_price = Decimal("0")
        max_gas_price = Decimal("0")
        if gas_prices:
            avg_gas_price = sum(gas_prices, Decimal("0")) / Decimal(str(len(gas_prices)))
            max_gas_price = max(gas_prices)

        # Net PnL (same as total since costs are already reflected in equity)
        # The equity curve already accounts for costs deducted during execution
        net_pnl = total_pnl

        # Total return percentage
        total_return = Decimal("0")
        if initial_value > Decimal("0"):
            total_return = (final_value - initial_value) / initial_value

        # Calculate annualized return
        annualized_return = Decimal("0")
        if len(timestamps) >= 2:
            duration_days = (timestamps[-1] - timestamps[0]).total_seconds() / (24 * 3600)
            if duration_days > 0:
                years = Decimal(str(duration_days)) / Decimal("365")
                if years > 0:
                    # Compound annual growth rate (CAGR)
                    # (1 + total_return) ^ (1/years) - 1
                    if total_return <= Decimal("-1"):
                        # Portfolio lost >= 100% (e.g. gas costs exceed principal).
                        # The base (1 + total_return) is <= 0, so exponentiation is
                        # undefined for non-integer exponents. Cap at -100%.
                        annualized_return = Decimal("-1")
                    else:
                        annualized_return = (Decimal("1") + total_return) ** (Decimal("1") / years) - Decimal("1")

        # Calculate returns series for risk metrics
        returns = self._calculate_returns(equity_values)

        # Trading days per year from config (crypto = 365, stocks = 252)
        trading_days = Decimal(str(config.trading_days_per_year))

        # Volatility (annualized standard deviation of returns)
        volatility = self._calculate_volatility(returns, trading_days)

        # Sharpe ratio with risk-free rate from config
        sharpe = self._calculate_sharpe_ratio(
            returns=returns,
            volatility=volatility,
            risk_free_rate=config.risk_free_rate,
            trading_days=trading_days,
        )

        # Sortino ratio (downside risk-adjusted return)
        sortino = self._calculate_sortino_ratio(
            returns=returns,
            risk_free_rate=config.risk_free_rate,
            trading_days=trading_days,
        )

        # Maximum drawdown
        max_drawdown = self._calculate_max_drawdown(equity_values)

        # Calmar ratio (annualized return / max drawdown)
        calmar = Decimal("0")
        if max_drawdown > Decimal("0"):
            calmar = annualized_return / max_drawdown

        # Trade statistics
        winning_trades = [t for t in trades if t.net_pnl_usd > Decimal("0")]
        losing_trades = [t for t in trades if t.net_pnl_usd <= Decimal("0")]

        # Win rate
        win_rate = Decimal("0")
        if trades:
            win_rate = Decimal(str(len(winning_trades))) / Decimal(str(len(trades)))

        # Profit factor (gross profit / gross loss)
        gross_profit = sum((t.net_pnl_usd for t in winning_trades), Decimal("0"))
        gross_loss_sum = sum((t.net_pnl_usd for t in losing_trades), Decimal("0"))
        gross_loss = abs(gross_loss_sum)
        profit_factor = Decimal("0")
        if gross_loss > Decimal("0"):
            profit_factor = gross_profit / gross_loss

        # Average trade PnL
        avg_trade_pnl = Decimal("0")
        if trades:
            total_trade_pnl = sum((t.net_pnl_usd for t in trades), Decimal("0"))
            avg_trade_pnl = total_trade_pnl / Decimal(str(len(trades)))

        # Largest win and loss
        trade_pnls = [t.net_pnl_usd for t in trades]
        largest_win = max(trade_pnls, default=Decimal("0"))
        largest_loss = min(trade_pnls, default=Decimal("0"))

        # Average win and loss
        avg_win = Decimal("0")
        if winning_trades:
            winning_pnl_sum = sum((t.net_pnl_usd for t in winning_trades), Decimal("0"))
            avg_win = winning_pnl_sum / Decimal(str(len(winning_trades)))

        avg_loss = Decimal("0")
        if losing_trades:
            losing_pnl_sum = sum((t.net_pnl_usd for t in losing_trades), Decimal("0"))
            avg_loss = losing_pnl_sum / Decimal(str(len(losing_trades)))

        return BacktestMetrics(
            total_pnl_usd=total_pnl,
            net_pnl_usd=net_pnl,
            sharpe_ratio=sharpe,
            max_drawdown_pct=max_drawdown,
            win_rate=win_rate,
            total_trades=len(trades),
            profit_factor=profit_factor,
            total_return_pct=total_return,
            annualized_return_pct=annualized_return,
            total_fees_usd=total_fees,
            total_slippage_usd=total_slippage,
            total_gas_usd=total_gas,
            winning_trades=len(winning_trades),
            losing_trades=len(losing_trades),
            avg_trade_pnl_usd=avg_trade_pnl,
            largest_win_usd=largest_win,
            largest_loss_usd=largest_loss,
            avg_win_usd=avg_win,
            avg_loss_usd=avg_loss,
            volatility=volatility,
            sortino_ratio=sortino,
            calmar_ratio=calmar,
            avg_gas_price_gwei=avg_gas_price,
            max_gas_price_gwei=max_gas_price,
            total_gas_cost_usd=total_gas,
            total_mev_cost_usd=total_mev,
        )

    def _calculate_returns(self, values: list[Decimal]) -> list[Decimal]:
        """Calculate period-over-period returns from equity values.

        Args:
            values: List of equity values over time

        Returns:
            List of returns where returns[i] = (values[i+1] - values[i]) / values[i]
        """
        if len(values) < 2:
            return []

        returns: list[Decimal] = []
        for i in range(1, len(values)):
            if values[i - 1] > Decimal("0"):
                ret = (values[i] - values[i - 1]) / values[i - 1]
                returns.append(ret)
        return returns

    def _create_gas_price_summary(
        self,
        trades: list[TradeRecord],
    ) -> GasPriceSummary | None:
        """Create gas price summary from trade records.

        Calculates summary statistics for gas prices used during the backtest.
        This method uses the gas_price_gwei values from trades, which are
        always populated regardless of the track_gas_prices config setting.

        Args:
            trades: List of trade records from the backtest

        Returns:
            GasPriceSummary with min, max, mean, std of gas prices, or None if no trades
        """
        gas_prices = [t.gas_price_gwei for t in trades if t.gas_price_gwei is not None]
        if not gas_prices:
            return None

        # Calculate statistics
        min_gwei = min(gas_prices)
        max_gwei = max(gas_prices)
        mean_gwei = sum(gas_prices, Decimal("0")) / Decimal(len(gas_prices))

        # Calculate standard deviation
        if len(gas_prices) > 1:
            variance = sum((g - mean_gwei) ** 2 for g in gas_prices) / Decimal(len(gas_prices))
            std_gwei = self._decimal_sqrt(variance)
        else:
            std_gwei = Decimal("0")

        # Build source breakdown from trade metadata
        source_counts: dict[str, int] = {}
        for t in trades:
            if t.gas_price_gwei is not None:
                # Get source from metadata if available
                source = t.metadata.get("gas_price_source", "unknown") if t.metadata else "unknown"
                source_counts[source] = source_counts.get(source, 0) + 1

        return GasPriceSummary(
            min_gwei=min_gwei,
            max_gwei=max_gwei,
            mean_gwei=mean_gwei,
            std_gwei=std_gwei,
            source_breakdown=source_counts,
            total_records=len(gas_prices),
        )

    def _calculate_volatility(
        self,
        returns: list[Decimal],
        trading_days: Decimal,
    ) -> Decimal:
        """Calculate annualized volatility from returns.

        Volatility is the annualized standard deviation of returns:
        volatility = std_dev(returns) * sqrt(trading_days)

        Args:
            returns: List of period returns
            trading_days: Number of trading days per year (365 for crypto, 252 for stocks)

        Returns:
            Annualized volatility as a decimal (0.2 = 20%)
        """
        if len(returns) < 2:
            return Decimal("0")

        # Calculate mean
        n = Decimal(str(len(returns)))
        mean = sum(returns, Decimal("0")) / n

        # Calculate variance (sample variance with n-1)
        squared_diffs = sum((r - mean) ** 2 for r in returns)
        variance = squared_diffs / (n - Decimal("1"))

        # Standard deviation
        std_dev = self._decimal_sqrt(variance)

        # Annualize
        return std_dev * self._decimal_sqrt(trading_days)

    def _calculate_sharpe_ratio(
        self,
        returns: list[Decimal],
        volatility: Decimal,
        risk_free_rate: Decimal,
        trading_days: Decimal,
    ) -> Decimal:
        """Calculate the Sharpe ratio.

        Sharpe ratio = (annualized_return - risk_free_rate) / volatility

        Args:
            returns: List of period returns
            volatility: Annualized volatility
            risk_free_rate: Annual risk-free rate from config
            trading_days: Number of trading days per year

        Returns:
            Sharpe ratio (risk-adjusted return)
        """
        if volatility == Decimal("0") or not returns:
            return Decimal("0")

        # Calculate annualized mean return
        n = Decimal(str(len(returns)))
        mean_return = sum(returns, Decimal("0")) / n
        annualized_return = mean_return * trading_days

        # Sharpe = (return - risk_free_rate) / volatility
        return (annualized_return - risk_free_rate) / volatility

    def _calculate_sortino_ratio(
        self,
        returns: list[Decimal],
        risk_free_rate: Decimal,
        trading_days: Decimal,
    ) -> Decimal:
        """Calculate the Sortino ratio (downside deviation based).

        Sortino ratio uses only negative returns for the denominator,
        penalizing only downside volatility rather than all volatility.

        Sortino = (annualized_return - risk_free_rate) / downside_deviation

        Args:
            returns: List of period returns
            risk_free_rate: Annual risk-free rate
            trading_days: Number of trading days per year

        Returns:
            Sortino ratio
        """
        if len(returns) < 2:
            return Decimal("0")

        # Get negative returns for downside deviation
        negative_returns = [r for r in returns if r < Decimal("0")]
        if not negative_returns:
            # No negative returns means infinite Sortino (capped at 0 for safety)
            return Decimal("0")

        # Calculate downside deviation
        # Using the semi-deviation: sqrt(sum(min(r, 0)^2) / n)
        n = Decimal(str(len(returns)))
        downside_variance = sum(r**2 for r in negative_returns) / n
        downside_dev = self._decimal_sqrt(downside_variance)

        if downside_dev == Decimal("0"):
            return Decimal("0")

        # Annualize
        annualized_downside = downside_dev * self._decimal_sqrt(trading_days)

        # Calculate annualized return
        mean_return = sum(returns, Decimal("0")) / n
        annualized_return = mean_return * trading_days

        return (annualized_return - risk_free_rate) / annualized_downside

    def _calculate_max_drawdown(self, values: list[Decimal]) -> Decimal:
        """Calculate maximum drawdown from an equity curve.

        Maximum drawdown is the largest peak-to-trough decline:
        max_dd = max((peak - trough) / peak) for all peaks and subsequent troughs

        Args:
            values: List of equity values over time

        Returns:
            Maximum drawdown as a decimal (0.1 = 10% drawdown)
        """
        if len(values) < 2:
            return Decimal("0")

        max_drawdown = Decimal("0")
        peak = values[0]

        for value in values:
            if value > peak:
                peak = value
            elif peak > Decimal("0"):
                drawdown = (peak - value) / peak
                if drawdown > max_drawdown:
                    max_drawdown = drawdown

        return max_drawdown

    def _decimal_sqrt(self, n: Decimal) -> Decimal:
        """Calculate square root of a Decimal using Newton's method.

        Standard library math.sqrt doesn't support Decimal, so we use
        Newton's iterative method for arbitrary precision.

        Args:
            n: Non-negative Decimal value

        Returns:
            Square root approximation

        Raises:
            ValueError: If n is negative
        """
        if n < Decimal("0"):
            raise ValueError("Cannot compute sqrt of negative number")
        if n == Decimal("0"):
            return Decimal("0")

        # Initial guess
        x = n
        # Newton's method: x_new = (x + n/x) / 2
        for _ in range(50):  # Max iterations
            x_new = (x + n / x) / Decimal("2")
            if abs(x_new - x) < Decimal("1e-28"):
                break
            x = x_new
        return x


__all__ = [
    "PnLBacktester",
    "BacktestableStrategy",
    "create_market_snapshot_from_state",
]
