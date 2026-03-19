"""Lending backtest adapter for supply and borrow positions.

This module provides the backtest adapter for lending protocol strategies,
handling Aave, Compound, Morpho, and similar lending positions. It manages:

- Interest accrual for supply and borrow positions
- Health factor tracking and monitoring
- Liquidation detection and simulation
- Position valuation with accrued interest

Key Features:
    - Configurable interest accrual method (simple vs compound)
    - Protocol-specific liquidation thresholds
    - Health factor warnings before liquidation
    - Accurate interest tracking for both supply and borrow
    - Historical APY integration via BacktestDataConfig
    - Support for AaveV3, CompoundV3, MorphoBlue, and Spark APY providers

Example:
    from almanak.framework.backtesting.adapters.lending_adapter import (
        LendingBacktestAdapter,
        LendingBacktestConfig,
    )
    from almanak.framework.backtesting.config import BacktestDataConfig

    # Create config for lending backtesting with historical APY rates
    config = LendingBacktestConfig(
        strategy_type="lending",
        interest_accrual_method="compound",
        health_factor_tracking_enabled=True,
    )
    data_config = BacktestDataConfig(
        use_historical_apy=True,
        supply_apy_fallback=Decimal("0.03"),
        borrow_apy_fallback=Decimal("0.05"),
    )

    # Get adapter instance with data config
    adapter = LendingBacktestAdapter(config, data_config=data_config)

    # Use in backtesting
    fill = adapter.execute_intent(intent, portfolio, market_state)
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal

from almanak.framework.backtesting.adapters.base import (
    StrategyBacktestAdapter,
    StrategyBacktestConfig,
    register_adapter,
)
from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError
from almanak.framework.backtesting.models import LendingLiquidationEvent
from almanak.framework.backtesting.pnl.calculators.health_factor import (
    HealthFactorCalculator,
)
from almanak.framework.backtesting.pnl.calculators.interest import (
    InterestCalculator,
    InterestRateSource,
)
from almanak.framework.backtesting.pnl.portfolio import PositionType
from almanak.framework.backtesting.pnl.types import DataConfidence

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig
    from almanak.framework.backtesting.pnl.data_provider import MarketState
    from almanak.framework.backtesting.pnl.portfolio import (
        SimulatedFill,
        SimulatedPortfolio,
        SimulatedPosition,
    )
    from almanak.framework.backtesting.pnl.providers.lending.aave_v3_apy import AaveV3APYProvider
    from almanak.framework.backtesting.pnl.providers.lending.compound_v3_apy import CompoundV3APYProvider
    from almanak.framework.backtesting.pnl.providers.lending.morpho_apy import MorphoBlueAPYProvider
    from almanak.framework.backtesting.pnl.providers.lending.spark_apy import SparkAPYProvider
    from almanak.framework.intents.vocabulary import Intent

logger = logging.getLogger(__name__)


@dataclass
class LendingBacktestConfig(StrategyBacktestConfig):
    """Configuration for lending-specific backtesting.

    This config extends the base StrategyBacktestConfig with lending-specific
    options for controlling interest accrual, health factor tracking, and
    liquidation simulation.

    Attributes:
        strategy_type: Must be "lending" for lending adapter (inherited)
        fee_tracking_enabled: Whether to track protocol fees (inherited)
        position_tracking_enabled: Whether to track positions in detail (inherited)
        reconcile_on_tick: Whether to reconcile position state each tick (inherited)
        extra_params: Additional parameters (inherited)
        interest_accrual_method: How to calculate interest:
            - "compound": Use compound interest (daily compounding, most accurate)
            - "simple": Use simple interest (faster but less accurate)
        health_factor_tracking_enabled: Whether to track health factor for borrow
            positions. When True, health factor is calculated each tick and warnings
            are emitted when it falls below thresholds.
        liquidation_threshold: Default liquidation threshold as a decimal (e.g., 0.825
            means 82.5% LTV at liquidation). Protocol-specific thresholds override this.
        health_factor_warning_threshold: Health factor below which to emit warnings.
            Default 1.2 means warnings when HF drops below 1.2.
        health_factor_critical_threshold: Health factor below which to emit critical
            warnings. Default 1.05 means critical warnings when HF drops below 1.05.
        liquidation_model_enabled: Whether to simulate liquidations when health
            factor falls below 1.0. When True, positions are partially liquidated.
        liquidation_penalty: Penalty applied during liquidation as a decimal.
            Default 0.05 (5%) means liquidator receives 5% bonus on seized collateral.
        liquidation_close_factor: Maximum percentage of debt that can be repaid in
            a single liquidation. Default 0.5 (50%) per Aave V3 rules.
        default_supply_apy: Default APY for supply positions when not provided.
            Default 0.03 (3%).
        default_borrow_apy: Default APY for borrow positions when not provided.
            Default 0.05 (5%).
        interest_rate_source: Source for interest rate data:
            - "fixed": Use default APYs for all calculations
            - "historical": Use historical APYs from data provider
            - "protocol": Use protocol-specific rates
        protocol: Default protocol for rate/threshold lookups (e.g., "aave_v3", "compound_v3")

    Example:
        config = LendingBacktestConfig(
            strategy_type="lending",
            interest_accrual_method="compound",
            health_factor_tracking_enabled=True,
            liquidation_model_enabled=True,
            default_supply_apy=Decimal("0.04"),  # 4% supply APY
            default_borrow_apy=Decimal("0.06"),  # 6% borrow APY
        )
    """

    interest_accrual_method: Literal["compound", "simple"] = "compound"
    """How to calculate interest (compound or simple)."""

    health_factor_tracking_enabled: bool = True
    """Whether to track health factor for borrow positions."""

    liquidation_threshold: Decimal = Decimal("0.825")
    """Default liquidation threshold (0.825 = 82.5% LTV)."""

    health_factor_warning_threshold: Decimal = Decimal("1.2")
    """Health factor below which to emit warnings."""

    health_factor_critical_threshold: Decimal = Decimal("1.05")
    """Health factor below which to emit critical warnings."""

    liquidation_model_enabled: bool = True
    """Whether to simulate liquidations when HF < 1.0."""

    liquidation_penalty: Decimal = Decimal("0.05")
    """Liquidation penalty (0.05 = 5%)."""

    liquidation_close_factor: Decimal = Decimal("0.5")
    """Maximum debt repayment per liquidation (0.5 = 50%)."""

    default_supply_apy: Decimal = Decimal("0.03")
    """Default supply APY (3%)."""

    default_borrow_apy: Decimal = Decimal("0.05")
    """Default borrow APY (5%)."""

    interest_rate_source: Literal["fixed", "historical", "protocol"] = "fixed"
    """Source for interest rate data."""

    protocol: str = "aave_v3"
    """Default protocol for rate/threshold lookups."""

    def __post_init__(self) -> None:
        """Validate lending-specific configuration.

        Raises:
            ValueError: If strategy_type is not "lending" or invalid parameters.
        """
        # Call parent validation
        super().__post_init__()

        # Validate strategy_type for lending
        if self.strategy_type.lower() != "lending":
            msg = f"LendingBacktestConfig requires strategy_type='lending', got '{self.strategy_type}'"
            raise ValueError(msg)

        # Validate interest_accrual_method
        valid_methods = {"compound", "simple"}
        if self.interest_accrual_method not in valid_methods:
            msg = f"interest_accrual_method must be one of {valid_methods}, got '{self.interest_accrual_method}'"
            raise ValueError(msg)

        # Validate interest_rate_source
        valid_sources = {"fixed", "historical", "protocol"}
        if self.interest_rate_source not in valid_sources:
            msg = f"interest_rate_source must be one of {valid_sources}, got '{self.interest_rate_source}'"
            raise ValueError(msg)

        # Validate threshold values
        if self.liquidation_threshold <= Decimal("0") or self.liquidation_threshold > Decimal("1"):
            msg = f"liquidation_threshold must be in (0, 1], got {self.liquidation_threshold}"
            raise ValueError(msg)
        if self.health_factor_warning_threshold <= Decimal("1"):
            msg = f"health_factor_warning_threshold must be > 1, got {self.health_factor_warning_threshold}"
            raise ValueError(msg)
        if self.health_factor_critical_threshold <= Decimal("1"):
            msg = f"health_factor_critical_threshold must be > 1, got {self.health_factor_critical_threshold}"
            raise ValueError(msg)
        if self.health_factor_critical_threshold >= self.health_factor_warning_threshold:
            msg = f"health_factor_critical_threshold ({self.health_factor_critical_threshold}) must be < health_factor_warning_threshold ({self.health_factor_warning_threshold})"
            raise ValueError(msg)

        # Validate penalty and close factor
        if self.liquidation_penalty < Decimal("0") or self.liquidation_penalty > Decimal("1"):
            msg = f"liquidation_penalty must be in [0, 1], got {self.liquidation_penalty}"
            raise ValueError(msg)
        if self.liquidation_close_factor <= Decimal("0") or self.liquidation_close_factor > Decimal("1"):
            msg = f"liquidation_close_factor must be in (0, 1], got {self.liquidation_close_factor}"
            raise ValueError(msg)

    def to_dict(self) -> dict[str, Any]:
        """Serialize configuration to a dictionary.

        Returns:
            Dictionary representation of the configuration.
        """
        base = super().to_dict()
        base.update(
            {
                "interest_accrual_method": self.interest_accrual_method,
                "health_factor_tracking_enabled": self.health_factor_tracking_enabled,
                "liquidation_threshold": str(self.liquidation_threshold),
                "health_factor_warning_threshold": str(self.health_factor_warning_threshold),
                "health_factor_critical_threshold": str(self.health_factor_critical_threshold),
                "liquidation_model_enabled": self.liquidation_model_enabled,
                "liquidation_penalty": str(self.liquidation_penalty),
                "liquidation_close_factor": str(self.liquidation_close_factor),
                "default_supply_apy": str(self.default_supply_apy),
                "default_borrow_apy": str(self.default_borrow_apy),
                "interest_rate_source": self.interest_rate_source,
                "protocol": self.protocol,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LendingBacktestConfig":
        """Create configuration from a dictionary.

        Args:
            data: Dictionary with configuration values.

        Returns:
            New LendingBacktestConfig instance.
        """
        return cls(
            strategy_type=data.get("strategy_type", "lending"),
            fee_tracking_enabled=data.get("fee_tracking_enabled", True),
            position_tracking_enabled=data.get("position_tracking_enabled", True),
            reconcile_on_tick=data.get("reconcile_on_tick", False),
            extra_params=data.get("extra_params", {}),
            strict_reproducibility=data.get("strict_reproducibility", False),
            interest_accrual_method=data.get("interest_accrual_method", "compound"),
            health_factor_tracking_enabled=data.get("health_factor_tracking_enabled", True),
            liquidation_threshold=Decimal(str(data.get("liquidation_threshold", "0.825"))),
            health_factor_warning_threshold=Decimal(str(data.get("health_factor_warning_threshold", "1.2"))),
            health_factor_critical_threshold=Decimal(str(data.get("health_factor_critical_threshold", "1.05"))),
            liquidation_model_enabled=data.get("liquidation_model_enabled", True),
            liquidation_penalty=Decimal(str(data.get("liquidation_penalty", "0.05"))),
            liquidation_close_factor=Decimal(str(data.get("liquidation_close_factor", "0.5"))),
            default_supply_apy=Decimal(str(data.get("default_supply_apy", "0.03"))),
            default_borrow_apy=Decimal(str(data.get("default_borrow_apy", "0.05"))),
            interest_rate_source=data.get("interest_rate_source", "fixed"),
            protocol=data.get("protocol", "aave_v3"),
        )


@register_adapter(
    "lending",
    description="Adapter for lending protocol strategies with interest accrual, health factor tracking, and liquidation support",
    aliases=["lend", "borrow", "supply", "aave", "aave_v3", "compound", "compound_v3", "morpho", "money_market"],
)
class LendingBacktestAdapter(StrategyBacktestAdapter):
    """Backtest adapter for Lending protocol strategies.

    This adapter handles the simulation of lending protocol positions during
    backtesting. It provides:

    - Interest accrual for supply positions (earning) and borrow positions (paying)
    - Health factor tracking and monitoring for borrow positions
    - Liquidation simulation when health factor falls below 1.0
    - Position valuation with principal plus accrued interest
    - Historical APY integration via AaveV3APYProvider, CompoundV3APYProvider,
      MorphoBlueAPYProvider, and SparkAPYProvider

    The adapter can be used with or without explicit configuration.
    When used without config, it uses sensible defaults.

    When BacktestDataConfig is provided, the adapter uses protocol-specific APY providers
    to fetch historical supply and borrow APY rates from subgraphs for accurate
    interest calculations.

    Attributes:
        config: Lending-specific configuration (optional)
        data_config: BacktestDataConfig for historical data provider settings (optional)

    Example:
        # With config and data_config
        from almanak.framework.backtesting.config import BacktestDataConfig

        config = LendingBacktestConfig(
            strategy_type="lending",
            health_factor_tracking_enabled=True,
        )
        data_config = BacktestDataConfig(
            use_historical_apy=True,
            supply_apy_fallback=Decimal("0.03"),
            borrow_apy_fallback=Decimal("0.05"),
        )
        adapter = LendingBacktestAdapter(config, data_config=data_config)

        # Without config (uses defaults)
        adapter = LendingBacktestAdapter()

        # In backtesting loop
        adapter.update_position(position, market_state, elapsed_seconds)

        # Value position
        value = adapter.value_position(position, market_state)

        # Check if rebalance needed (e.g., approaching liquidation)
        if adapter.should_rebalance(position, market_state):
            # Strategy should consider adjusting position
            pass
    """

    def __init__(
        self,
        config: LendingBacktestConfig | None = None,
        data_config: "BacktestDataConfig | None" = None,
        aave_v3_provider: "AaveV3APYProvider | None" = None,
        compound_v3_provider: "CompoundV3APYProvider | None" = None,
        morpho_provider: "MorphoBlueAPYProvider | None" = None,
        spark_provider: "SparkAPYProvider | None" = None,
    ) -> None:
        """Initialize the lending backtest adapter.

        Args:
            config: Lending-specific configuration. If None, uses default
                LendingBacktestConfig with strategy_type="lending".
            data_config: BacktestDataConfig for controlling historical data provider
                behavior. When provided, overrides config.interest_rate_source behavior
                with data_config.use_historical_apy setting.
            aave_v3_provider: Optional pre-configured AaveV3APYProvider.
                If None and use_historical_apy=True, will create one lazily.
            compound_v3_provider: Optional pre-configured CompoundV3APYProvider.
                If None and use_historical_apy=True, will create one lazily.
            morpho_provider: Optional pre-configured MorphoBlueAPYProvider.
                If None and use_historical_apy=True, will create one lazily.
            spark_provider: Optional pre-configured SparkAPYProvider.
                If None and use_historical_apy=True, will create one lazily.
        """
        self._config = config or LendingBacktestConfig(strategy_type="lending")
        self._data_config = data_config

        # Initialize calculators
        self._interest_calculator = InterestCalculator(
            interest_rate_source=(
                InterestRateSource.FIXED
                if self._config.interest_rate_source == "fixed"
                else InterestRateSource.HISTORICAL
                if self._config.interest_rate_source == "historical"
                else InterestRateSource.PROTOCOL
            ),
            default_supply_apy=self._config.default_supply_apy,
            default_borrow_apy=self._config.default_borrow_apy,
        )
        self._health_factor_calculator = HealthFactorCalculator(
            warning_threshold=self._config.health_factor_warning_threshold,
            critical_threshold=self._config.health_factor_critical_threshold,
        )

        # Track collateral and debt for health factor calculations
        self._position_collateral: dict[str, Decimal] = {}
        self._position_debt: dict[str, Decimal] = {}

        # Historical APY providers (lazy initialized)
        self._aave_v3_provider: AaveV3APYProvider | None = aave_v3_provider
        self._aave_v3_provider_initialized = aave_v3_provider is not None
        self._compound_v3_provider: CompoundV3APYProvider | None = compound_v3_provider
        self._compound_v3_provider_initialized = compound_v3_provider is not None
        self._morpho_provider: MorphoBlueAPYProvider | None = morpho_provider
        self._morpho_provider_initialized = morpho_provider is not None
        self._spark_provider: SparkAPYProvider | None = spark_provider
        self._spark_provider_initialized = spark_provider is not None

        # Cache for APY data to avoid repeated queries
        # Key: (protocol, market, timestamp_day) -> (supply_apy, borrow_apy, confidence, source)
        self._apy_cache: dict[tuple[str, str, datetime], tuple[Decimal, Decimal, str, str]] = {}

    @property
    def adapter_name(self) -> str:
        """Return the unique name of this adapter.

        Returns:
            Strategy type identifier "lending"
        """
        return "lending"

    @property
    def config(self) -> LendingBacktestConfig:
        """Get the adapter configuration.

        Returns:
            Lending backtest configuration
        """
        return self._config

    def _use_historical_apy(self) -> bool:
        """Check if historical APY data should be used via BacktestDataConfig providers.

        This returns True ONLY when BacktestDataConfig is provided with
        use_historical_apy=True. When no data_config is provided, returns False
        to allow the legacy InterestCalculator-based historical APY to be used.

        Returns:
            True if historical APY rates should be fetched from BacktestDataConfig providers.
        """
        if self._data_config is not None:
            return self._data_config.use_historical_apy
        # Without data_config, don't use new provider system
        # (let legacy InterestCalculator handle historical APY if configured)
        return False

    def _get_supply_apy_fallback(self) -> Decimal:
        """Get the supply APY fallback rate to use.

        Uses BacktestDataConfig.supply_apy_fallback if data_config provided,
        otherwise falls back to LendingBacktestConfig.default_supply_apy.

        Returns:
            Supply APY for fallback when historical data unavailable.
        """
        if self._data_config is not None:
            return self._data_config.supply_apy_fallback
        return self._config.default_supply_apy

    def _get_borrow_apy_fallback(self) -> Decimal:
        """Get the borrow APY fallback rate to use.

        Uses BacktestDataConfig.borrow_apy_fallback if data_config provided,
        otherwise falls back to LendingBacktestConfig.default_borrow_apy.

        Returns:
            Borrow APY for fallback when historical data unavailable.
        """
        if self._data_config is not None:
            return self._data_config.borrow_apy_fallback
        return self._config.default_borrow_apy

    def _is_strict_historical_mode(self) -> bool:
        """Check if strict historical mode is enabled.

        When strict mode is enabled, the adapter will raise HistoricalDataUnavailableError
        instead of using fallback values when historical data is unavailable.

        Returns:
            True if strict historical mode is enabled via BacktestDataConfig.
        """
        if self._data_config is not None:
            return self._data_config.strict_historical_mode
        return False

    def _ensure_aave_v3_provider(self) -> "AaveV3APYProvider | None":
        """Lazily initialize the Aave V3 APY provider if needed.

        Creates an AaveV3APYProvider instance for fetching historical APY
        data from the Aave V3 subgraph.

        Returns:
            AaveV3APYProvider instance or None if disabled/failed
        """
        if not self._use_historical_apy():
            return None

        if self._aave_v3_provider_initialized:
            return self._aave_v3_provider

        try:
            from almanak.framework.backtesting.pnl.providers.lending.aave_v3_apy import (
                AaveV3APYProvider,
                AaveV3ClientConfig,
            )

            config = AaveV3ClientConfig(
                supply_apy_fallback=self._get_supply_apy_fallback(),
                borrow_apy_fallback=self._get_borrow_apy_fallback(),
            )
            self._aave_v3_provider = AaveV3APYProvider(config=config)
            self._aave_v3_provider_initialized = True
            logger.debug(
                "Initialized AaveV3APYProvider: supply_fallback=%s, borrow_fallback=%s",
                self._get_supply_apy_fallback(),
                self._get_borrow_apy_fallback(),
            )
            return self._aave_v3_provider
        except Exception as e:
            logger.warning(
                "Failed to initialize AaveV3APYProvider: %s. Will use fallback rate.",
                e,
            )
            self._aave_v3_provider_initialized = True  # Don't retry
            self._aave_v3_provider = None
            return None

    def _ensure_compound_v3_provider(self) -> "CompoundV3APYProvider | None":
        """Lazily initialize the Compound V3 APY provider if needed.

        Creates a CompoundV3APYProvider instance for fetching historical APY
        data from the Compound V3 subgraph.

        Returns:
            CompoundV3APYProvider instance or None if disabled/failed
        """
        if not self._use_historical_apy():
            return None

        if self._compound_v3_provider_initialized:
            return self._compound_v3_provider

        try:
            from almanak.framework.backtesting.pnl.providers.lending.compound_v3_apy import (
                CompoundV3APYProvider,
                CompoundV3ClientConfig,
            )

            config = CompoundV3ClientConfig(
                supply_apy_fallback=self._get_supply_apy_fallback(),
                borrow_apy_fallback=self._get_borrow_apy_fallback(),
            )
            self._compound_v3_provider = CompoundV3APYProvider(config=config)
            self._compound_v3_provider_initialized = True
            logger.debug(
                "Initialized CompoundV3APYProvider: supply_fallback=%s, borrow_fallback=%s",
                self._get_supply_apy_fallback(),
                self._get_borrow_apy_fallback(),
            )
            return self._compound_v3_provider
        except Exception as e:
            logger.warning(
                "Failed to initialize CompoundV3APYProvider: %s. Will use fallback rate.",
                e,
            )
            self._compound_v3_provider_initialized = True  # Don't retry
            self._compound_v3_provider = None
            return None

    def _ensure_morpho_provider(self) -> "MorphoBlueAPYProvider | None":
        """Lazily initialize the Morpho Blue APY provider if needed.

        Creates a MorphoBlueAPYProvider instance for fetching historical APY
        data from the Morpho Blue subgraph.

        Returns:
            MorphoBlueAPYProvider instance or None if disabled/failed
        """
        if not self._use_historical_apy():
            return None

        if self._morpho_provider_initialized:
            return self._morpho_provider

        try:
            from almanak.framework.backtesting.pnl.providers.lending.morpho_apy import (
                MorphoBlueAPYProvider,
                MorphoBlueClientConfig,
            )

            config = MorphoBlueClientConfig(
                supply_apy_fallback=self._get_supply_apy_fallback(),
                borrow_apy_fallback=self._get_borrow_apy_fallback(),
            )
            self._morpho_provider = MorphoBlueAPYProvider(config=config)
            self._morpho_provider_initialized = True
            logger.debug(
                "Initialized MorphoBlueAPYProvider: supply_fallback=%s, borrow_fallback=%s",
                self._get_supply_apy_fallback(),
                self._get_borrow_apy_fallback(),
            )
            return self._morpho_provider
        except Exception as e:
            logger.warning(
                "Failed to initialize MorphoBlueAPYProvider: %s. Will use fallback rate.",
                e,
            )
            self._morpho_provider_initialized = True  # Don't retry
            self._morpho_provider = None
            return None

    def _ensure_spark_provider(self) -> "SparkAPYProvider | None":
        """Lazily initialize the Spark APY provider if needed.

        Creates a SparkAPYProvider instance for fetching historical APY
        data from the Spark subgraph.

        Returns:
            SparkAPYProvider instance or None if disabled/failed
        """
        if not self._use_historical_apy():
            return None

        if self._spark_provider_initialized:
            return self._spark_provider

        try:
            from almanak.framework.backtesting.pnl.providers.lending.spark_apy import (
                SparkAPYProvider,
                SparkClientConfig,
            )

            config = SparkClientConfig(
                supply_apy_fallback=self._get_supply_apy_fallback(),
                borrow_apy_fallback=self._get_borrow_apy_fallback(),
            )
            self._spark_provider = SparkAPYProvider(config=config)
            self._spark_provider_initialized = True
            logger.debug(
                "Initialized SparkAPYProvider: supply_fallback=%s, borrow_fallback=%s",
                self._get_supply_apy_fallback(),
                self._get_borrow_apy_fallback(),
            )
            return self._spark_provider
        except Exception as e:
            logger.warning(
                "Failed to initialize SparkAPYProvider: %s. Will use fallback rate.",
                e,
            )
            self._spark_provider_initialized = True  # Don't retry
            self._spark_provider = None
            return None

    def _get_provider_for_protocol(
        self,
        protocol: str,
    ) -> "AaveV3APYProvider | CompoundV3APYProvider | MorphoBlueAPYProvider | SparkAPYProvider | None":
        """Get the appropriate APY provider for a given protocol.

        Routes to the correct APY provider based on the lending protocol.

        Args:
            protocol: Protocol name (e.g., "aave_v3", "compound_v3", "morpho_blue", "spark")

        Returns:
            The appropriate provider or None if not available
        """
        protocol_lower = protocol.lower()

        if protocol_lower in ("aave_v3", "aave", "aavev3"):
            return self._ensure_aave_v3_provider()
        elif protocol_lower in ("compound_v3", "compound", "compoundv3"):
            return self._ensure_compound_v3_provider()
        elif protocol_lower in ("morpho_blue", "morpho", "morphoblue"):
            return self._ensure_morpho_provider()
        elif protocol_lower in ("spark", "spark_lend", "sparklend"):
            return self._ensure_spark_provider()
        else:
            logger.debug(
                "No historical APY provider for protocol '%s', will use fallback",
                protocol,
            )
            return None

    def _normalize_timestamp_to_day(self, timestamp: datetime) -> datetime:
        """Normalize a timestamp to the start of its day.

        Used for cache key generation since APY data is typically daily.

        Args:
            timestamp: The timestamp to normalize

        Returns:
            Timestamp normalized to midnight UTC
        """
        return datetime(
            timestamp.year,
            timestamp.month,
            timestamp.day,
            tzinfo=UTC,
        )

    async def _get_historical_apy_async(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> tuple[Decimal, Decimal, str, str]:
        """Fetch historical APY data asynchronously.

        Args:
            protocol: Lending protocol name
            market: Market/token symbol (e.g., "USDC", "WETH")
            timestamp: Timestamp to get APY for

        Returns:
            Tuple of (supply_apy, borrow_apy, confidence, data_source)

        Raises:
            HistoricalDataUnavailableError: If strict_historical_mode is True and
                historical APY data cannot be fetched.
        """
        # Check cache first
        cache_key = (protocol.lower(), market.upper(), self._normalize_timestamp_to_day(timestamp))
        if cache_key in self._apy_cache:
            return self._apy_cache[cache_key]

        # Get provider for protocol
        provider = self._get_provider_for_protocol(protocol)
        if provider is None:
            if self._is_strict_historical_mode():
                raise HistoricalDataUnavailableError(
                    data_type="apy",
                    identifier=market,
                    timestamp=timestamp,
                    message=f"No APY provider available for protocol '{protocol}'",
                    protocol=protocol,
                )
            # No provider available, use fallback
            result = (
                self._get_supply_apy_fallback(),
                self._get_borrow_apy_fallback(),
                DataConfidence.LOW.value,
                "fallback:default_rate",
            )
            self._apy_cache[cache_key] = result
            logger.warning(
                "Using fallback APY for %s/%s: supply=%.4f, borrow=%.4f",
                protocol,
                market,
                float(result[0]),
                float(result[1]),
            )
            return result

        try:
            # Fetch APY from provider
            start_date = timestamp
            end_date = timestamp + timedelta(hours=1)

            apy_results = await provider.get_apy(
                protocol=protocol,
                market=market,
                start_date=start_date,
                end_date=end_date,
            )

            if apy_results:
                apy_result = apy_results[-1]  # Use most recent
                supply_apy = apy_result.supply_apy
                borrow_apy = apy_result.borrow_apy
                confidence = apy_result.source_info.confidence.value
                source = apy_result.source_info.source

                result = (supply_apy, borrow_apy, confidence, source)
                self._apy_cache[cache_key] = result
                logger.debug(
                    "Historical APY for %s/%s at %s: supply=%.4f, borrow=%.4f (confidence=%s)",
                    protocol,
                    market,
                    timestamp,
                    float(supply_apy),
                    float(borrow_apy),
                    confidence,
                )
                return result
            else:
                # No data returned
                if self._is_strict_historical_mode():
                    raise HistoricalDataUnavailableError(
                        data_type="apy",
                        identifier=market,
                        timestamp=timestamp,
                        message="No historical APY data returned from subgraph",
                        protocol=protocol,
                    )
                result = (
                    self._get_supply_apy_fallback(),
                    self._get_borrow_apy_fallback(),
                    DataConfidence.LOW.value,
                    "fallback:no_data",
                )
                self._apy_cache[cache_key] = result
                logger.warning(
                    "No APY data for %s/%s at %s, using fallback: supply=%.4f, borrow=%.4f",
                    protocol,
                    market,
                    timestamp,
                    float(result[0]),
                    float(result[1]),
                )
                return result

        except HistoricalDataUnavailableError:
            # Re-raise if it's already our exception
            raise
        except Exception as e:
            if self._is_strict_historical_mode():
                raise HistoricalDataUnavailableError(
                    data_type="apy",
                    identifier=market,
                    timestamp=timestamp,
                    message=f"Failed to fetch historical APY: {e}",
                    protocol=protocol,
                ) from e
            logger.warning(
                "Error fetching historical APY for %s/%s: %s. Using fallback.",
                protocol,
                market,
                e,
            )
            result = (
                self._get_supply_apy_fallback(),
                self._get_borrow_apy_fallback(),
                DataConfidence.LOW.value,
                f"fallback:error:{type(e).__name__}",
            )
            self._apy_cache[cache_key] = result
            return result

    def _get_historical_apy(
        self,
        protocol: str,
        market: str,
        timestamp: datetime,
    ) -> tuple[Decimal, Decimal, str, str]:
        """Fetch historical APY data (sync wrapper for async method).

        Args:
            protocol: Lending protocol name
            market: Market/token symbol (e.g., "USDC", "WETH")
            timestamp: Timestamp to get APY for

        Returns:
            Tuple of (supply_apy, borrow_apy, confidence, data_source)

        Raises:
            HistoricalDataUnavailableError: If strict_historical_mode is True and
                historical APY data cannot be fetched.
        """
        # Check cache first (avoid async if possible)
        cache_key = (protocol.lower(), market.upper(), self._normalize_timestamp_to_day(timestamp))
        if cache_key in self._apy_cache:
            return self._apy_cache[cache_key]

        # If not using historical APY, return fallback immediately
        if not self._use_historical_apy():
            if self._is_strict_historical_mode():
                raise HistoricalDataUnavailableError(
                    data_type="apy",
                    identifier=market,
                    timestamp=timestamp,
                    message="Historical APY is disabled (use_historical_apy=False)",
                    protocol=protocol,
                )
            result = (
                self._get_supply_apy_fallback(),
                self._get_borrow_apy_fallback(),
                DataConfidence.LOW.value,
                "fallback:historical_disabled",
            )
            self._apy_cache[cache_key] = result
            return result

        # Try to run async method
        try:
            asyncio.get_running_loop()
            # We're in an async context, use run_coroutine_threadsafe

            loop = asyncio.new_event_loop()
            try:
                future = loop.run_until_complete(self._get_historical_apy_async(protocol, market, timestamp))
                return future
            finally:
                loop.close()
        except RuntimeError:
            # No running loop, create one
            return asyncio.run(self._get_historical_apy_async(protocol, market, timestamp))

    def execute_intent(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill | None":
        """Simulate execution of a lending-related intent.

        This method handles SUPPLY, WITHDRAW, BORROW, and REPAY intents.
        For BORROW, it validates health factor requirements before allowing
        the position to be opened.

        For SUPPLY:
        - Creates a supply position tracking the deposited tokens
        - Records APY at time of entry for interest projection

        For BORROW:
        - Validates that health factor will remain above threshold
        - Creates a borrow position tracking the debt
        - Sets up health factor monitoring

        Args:
            intent: The intent to execute (SupplyIntent, BorrowIntent, etc.)
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            SimulatedFill if validation fails (with success=False),
            or None to use default execution logic if validation passes.
        """
        from almanak.framework.intents.vocabulary import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent

        # Handle SUPPLY intent
        if isinstance(intent, SupplyIntent):
            return self._execute_supply(intent, portfolio, market_state)

        # Handle WITHDRAW intent
        if isinstance(intent, WithdrawIntent):
            return self._execute_withdraw(intent, portfolio, market_state)

        # Handle BORROW intent with health factor validation
        if isinstance(intent, BorrowIntent):
            return self._execute_borrow(intent, portfolio, market_state)

        # Handle REPAY intent
        if isinstance(intent, RepayIntent):
            return self._execute_repay(intent, portfolio, market_state)

        # Not a lending intent, let default execution handle it
        return None

    def _execute_supply(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill | None":
        """Execute a SUPPLY intent.

        Args:
            intent: SupplyIntent to execute
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            None to proceed with default execution.
        """
        from almanak.framework.intents.vocabulary import SupplyIntent

        # Type narrowing for mypy
        if not isinstance(intent, SupplyIntent):
            return None

        # Log the supply intent details
        logger.debug(
            "SUPPLY intent: token=%s, amount=%s, protocol=%s",
            intent.token,
            intent.amount,
            intent.protocol,
        )

        # Get APY for logging
        protocol = intent.protocol or self._config.protocol
        apy = self._interest_calculator.get_supply_apy_for_protocol(protocol)
        logger.info(
            "SUPPLY position will earn %.2f%% APY on %s via %s",
            float(apy * 100),
            intent.token,
            protocol,
        )

        # Let default execution handle the position creation
        return None

    def _execute_withdraw(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill | None":
        """Execute a WITHDRAW intent.

        Args:
            intent: WithdrawIntent to execute
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            None to proceed with default execution.
        """
        from almanak.framework.intents.vocabulary import WithdrawIntent

        # Type narrowing for mypy
        if not isinstance(intent, WithdrawIntent):
            return None

        # Log the withdraw intent details
        logger.debug(
            "WITHDRAW intent: token=%s, amount=%s",
            intent.token,
            intent.amount,
        )

        # Let default execution handle the withdrawal
        return None

    def _execute_borrow(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill | None":
        """Execute a BORROW intent with health factor validation.

        Args:
            intent: BorrowIntent to execute
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            SimulatedFill with success=False if health factor validation fails,
            or None to proceed with default execution.
        """
        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl.portfolio import SimulatedFill
        from almanak.framework.intents.vocabulary import BorrowIntent

        # Type narrowing for mypy
        if not isinstance(intent, BorrowIntent):
            return None

        # Get borrow amount and token (BorrowIntent uses borrow_token/borrow_amount)
        borrow_token = intent.borrow_token
        borrow_amount = Decimal(str(intent.borrow_amount))
        protocol = intent.protocol or self._config.protocol

        # Get token price to convert to USD
        try:
            borrow_price = market_state.get_price(borrow_token)
        except KeyError:
            borrow_price = Decimal("1")  # Assume stablecoin if not found

        if borrow_price is None or borrow_price <= 0:
            borrow_price = Decimal("1")

        borrow_usd = borrow_amount * borrow_price

        # Calculate current collateral value from supply positions
        collateral_value = self._get_total_collateral_value(portfolio, market_state)

        # Calculate current debt value from borrow positions
        current_debt = self._get_total_debt_value(portfolio, market_state)
        new_debt = current_debt + borrow_usd

        # Get liquidation threshold for protocol
        liq_threshold = self._health_factor_calculator.get_liquidation_threshold_for_protocol(protocol)

        # Calculate health factor after borrow
        health_result = self._health_factor_calculator.calculate_health_factor(
            collateral_value_usd=collateral_value,
            debt_value_usd=new_debt,
            liquidation_threshold=liq_threshold,
        )

        # Check if health factor would be below warning threshold
        if health_result.health_factor < self._config.health_factor_warning_threshold:
            logger.warning(
                "BORROW would result in low health factor: HF=%.4f (threshold=%.2f). "
                "Collateral=$%.2f, Current debt=$%.2f, New borrow=$%.2f",
                float(health_result.health_factor),
                float(self._config.health_factor_warning_threshold),
                float(collateral_value),
                float(current_debt),
                float(borrow_usd),
            )

            # If health factor would be below 1.0, reject the borrow
            if health_result.health_factor < Decimal("1.0"):
                return SimulatedFill(
                    timestamp=market_state.timestamp,
                    intent_type=IntentType.BORROW,
                    protocol=protocol,
                    tokens=[borrow_token],
                    executed_price=borrow_price,
                    amount_usd=borrow_usd,
                    fee_usd=Decimal("0"),
                    slippage_usd=Decimal("0"),
                    gas_cost_usd=Decimal("0"),
                    tokens_in={},
                    tokens_out={borrow_token: borrow_amount},
                    success=False,
                    metadata={
                        "failure_reason": "Health factor would be below 1.0",
                        "validation_type": "health_factor",
                        "projected_health_factor": str(health_result.health_factor),
                        "collateral_usd": str(collateral_value),
                        "total_debt_usd": str(new_debt),
                        "liquidation_threshold": str(liq_threshold),
                    },
                )

        # Log successful validation
        apy = self._interest_calculator.get_borrow_apy_for_protocol(protocol)
        logger.info(
            "BORROW validated: amount=$%.2f, HF=%.4f, APY=%.2f%%",
            float(borrow_usd),
            float(health_result.health_factor),
            float(apy * 100),
        )

        # Let default execution handle the borrow
        return None

    def _execute_repay(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill | None":
        """Execute a REPAY intent.

        Args:
            intent: RepayIntent to execute
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            None to proceed with default execution.
        """
        from almanak.framework.intents.vocabulary import RepayIntent

        # Type narrowing for mypy
        if not isinstance(intent, RepayIntent):
            return None

        # Log the repay intent details
        logger.debug(
            "REPAY intent: token=%s, amount=%s",
            intent.token,
            intent.amount,
        )

        # Let default execution handle the repayment
        return None

    def _get_total_collateral_value(
        self,
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> Decimal:
        """Calculate total collateral value from supply positions.

        Args:
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            Total collateral value in USD
        """
        total = Decimal("0")
        for position in portfolio.positions:
            if position.position_type == PositionType.SUPPLY:
                value = self.value_position(position, market_state)
                total += value
        return total

    def _get_total_debt_value(
        self,
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> Decimal:
        """Calculate total debt value from borrow positions.

        Args:
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            Total debt value in USD
        """
        total = Decimal("0")
        for position in portfolio.positions:
            if position.position_type == PositionType.BORROW:
                value = self.value_position(position, market_state)
                total += value
        return total

    def update_position(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        elapsed_seconds: float,
        timestamp: datetime | None = None,
    ) -> None:
        """Update lending position state based on time passage.

        This method handles lending-specific position updates:
        - Accrues interest for supply positions (increases value)
        - Accrues interest for borrow positions (increases debt)
        - Updates health factor for borrow positions
        - Emits warnings when health factor is low

        Args:
            position: The lending position to update (modified in-place)
            market_state: Current market prices and data
            elapsed_seconds: Time elapsed since last update in seconds
            timestamp: Simulation timestamp for deterministic updates. If None,
                uses market_state.timestamp for reproducible backtests.

        Note:
            This method only updates lending positions (SUPPLY or BORROW).
            Non-lending positions are ignored.

            For liquidation detection, call check_and_simulate_liquidation()
            separately. This method does not trigger liquidation, only emits
            proximity warnings.
        """
        # Only process lending positions
        if position.position_type not in (PositionType.SUPPLY, PositionType.BORROW):
            return

        # Skip if already liquidated
        if position.is_liquidated:
            return

        if elapsed_seconds <= 0:
            return

        # Convert elapsed seconds to days for interest calculation
        elapsed_days = Decimal(str(elapsed_seconds)) / Decimal("86400")

        # Get APY for this position
        protocol = position.protocol or self._config.protocol
        primary_token = position.primary_token

        # Determine effective timestamp for APY lookup
        if timestamp is not None:
            apy_timestamp = timestamp
        elif hasattr(market_state, "timestamp") and market_state.timestamp is not None:
            apy_timestamp = market_state.timestamp
        else:
            if self._config.strict_reproducibility:
                msg = (
                    f"No simulation timestamp available for lending position {position.position_id} "
                    "APY lookup. In strict reproducibility mode, timestamp must be provided. "
                    "Either pass timestamp parameter or ensure market_state.timestamp is set."
                )
                raise ValueError(msg)
            logger.warning(
                "No simulation timestamp available for lending position %s APY lookup, "
                "falling back to datetime.now(). This breaks backtest reproducibility.",
                position.position_id,
            )
            apy_timestamp = datetime.now()

        # Get APY based on position type and rate source
        apy_confidence = DataConfidence.LOW.value  # Default
        apy_data_source = "fallback:default_rate"  # Default

        if position.apy_at_entry:
            # Use position's entry APY if available
            apy = position.apy_at_entry
            apy_data_source = "position_entry"
            apy_confidence = DataConfidence.MEDIUM.value
        elif self._use_historical_apy():
            # Use historical APY from BacktestDataConfig providers
            supply_apy, borrow_apy, confidence, source = self._get_historical_apy(
                protocol=protocol,
                market=primary_token,
                timestamp=apy_timestamp,
            )
            if position.position_type == PositionType.SUPPLY:
                apy = supply_apy
            else:
                apy = borrow_apy
            apy_confidence = confidence
            apy_data_source = source
        elif self._config.interest_rate_source == "historical":
            # Use legacy historical APY from InterestCalculator
            if position.position_type == PositionType.SUPPLY:
                apy = self._interest_calculator.get_historical_supply_apy_sync(
                    protocol=protocol,
                    market=primary_token,
                    timestamp=apy_timestamp,
                )
            else:
                apy = self._interest_calculator.get_historical_borrow_apy_sync(
                    protocol=protocol,
                    market=primary_token,
                    timestamp=apy_timestamp,
                )
            apy_data_source = "legacy_historical"
            apy_confidence = DataConfidence.MEDIUM.value
        else:
            # Use fixed/protocol default APY
            if position.position_type == PositionType.SUPPLY:
                apy = self._interest_calculator.get_supply_apy_for_protocol(protocol)
            else:
                apy = self._interest_calculator.get_borrow_apy_for_protocol(protocol)
            apy_data_source = f"fixed:{self._config.interest_rate_source}"
            apy_confidence = DataConfidence.LOW.value

        # Update position confidence tracking
        position.apy_confidence = apy_confidence
        position.apy_data_source = apy_data_source

        # Calculate principal (current value without interest)
        principal_amount = position.total_amount

        try:
            token_price = market_state.get_price(primary_token)
        except KeyError:
            token_price = position.entry_price

        if token_price is None or token_price <= 0:
            token_price = position.entry_price

        principal_usd = principal_amount * token_price

        # Calculate interest
        use_compound = self._config.interest_accrual_method == "compound"
        interest_result = self._interest_calculator.calculate_interest(
            principal=principal_usd,
            apy=apy,
            time_delta=elapsed_days,
            compound=use_compound,
        )

        # Update position interest
        position.interest_accrued += interest_result.interest

        # For borrow positions, update health factor
        if position.position_type == PositionType.BORROW and self._config.health_factor_tracking_enabled:
            # Get collateral from tracked state or estimate
            collateral_key = f"collateral_{position.position_id}"
            collateral_usd = self._position_collateral.get(collateral_key, Decimal("0"))

            # Calculate debt including accrued interest
            debt_usd = principal_usd + position.interest_accrued

            # Get liquidation threshold
            liq_threshold = self._health_factor_calculator.get_liquidation_threshold_for_protocol(protocol)

            # Calculate health factor
            health_result = self._health_factor_calculator.calculate_health_factor(
                collateral_value_usd=collateral_usd,
                debt_value_usd=debt_usd,
                liquidation_threshold=liq_threshold,
            )

            # Update position health factor
            position.health_factor = health_result.health_factor

            # Check for warnings
            self._health_factor_calculator.check_health_factor_warning(
                health_factor=health_result.health_factor,
                position_id=position.position_id,
                emit_warning=True,
            )

        # Update timestamp using simulation time for reproducibility
        # Prefer explicit timestamp > market_state.timestamp > datetime.now() (with warning)
        if timestamp is not None:
            update_time = timestamp
        elif hasattr(market_state, "timestamp") and market_state.timestamp is not None:
            update_time = market_state.timestamp
        else:
            if self._config.strict_reproducibility:
                msg = (
                    f"No simulation timestamp available for lending position {position.position_id}. "
                    "In strict reproducibility mode, timestamp must be provided. "
                    "Either pass timestamp parameter or ensure market_state.timestamp is set."
                )
                raise ValueError(msg)
            logger.warning(
                "No simulation timestamp available for lending position %s, "
                "falling back to datetime.now(). This breaks backtest reproducibility.",
                position.position_id,
            )
            update_time = datetime.now()
        position.last_updated = update_time

        # Log interest accrual at INFO level for visibility
        if interest_result.interest > Decimal("0.01"):  # Only log meaningful interest
            logger.info(
                "Interest accrued: position=%s, type=%s, principal=$%.2f, "
                "interest=$%.4f, total_accrued=$%.4f, APY=%.2f%% (source: %s, confidence: %s)",
                position.position_id,
                position.position_type.value,
                float(principal_usd),
                float(interest_result.interest),
                float(position.interest_accrued),
                float(apy * 100),
                apy_data_source,
                apy_confidence,
            )
        else:
            logger.debug(
                "Lending position update: position=%s, type=%s, principal=$%.2f, "
                "interest=$%.4f, total_accrued=$%.4f, HF=%s, APY_source=%s, confidence=%s",
                position.position_id,
                position.position_type.value,
                float(principal_usd),
                float(interest_result.interest),
                float(position.interest_accrued),
                f"{float(position.health_factor):.4f}" if position.health_factor else "N/A",
                apy_data_source,
                apy_confidence,
            )

    def update_position_with_portfolio(
        self,
        position: "SimulatedPosition",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
        elapsed_seconds: float,
        timestamp: datetime | None = None,
    ) -> None:
        """Update lending position with automatic collateral discovery from portfolio.

        This method extends update_position() by automatically calculating
        total collateral value from all SUPPLY positions in the portfolio,
        enabling accurate health factor tracking for BORROW positions.

        This is the preferred method when you have access to the portfolio,
        as it ensures health factor is always calculated with current
        collateral values rather than relying on manually set values.

        Args:
            position: The lending position to update (modified in-place)
            portfolio: The portfolio containing supply positions as collateral
            market_state: Current market prices and data
            elapsed_seconds: Time elapsed since last update in seconds
            timestamp: Simulation timestamp for deterministic updates. If None,
                uses market_state.timestamp for reproducible backtests.

        Example:
            # In backtesting loop with portfolio access
            for position in portfolio.positions:
                if position.position_type == PositionType.BORROW:
                    adapter.update_position_with_portfolio(
                        position, portfolio, market_state, elapsed_seconds,
                        timestamp=market_state.timestamp
                    )
        """
        # For borrow positions, auto-sync collateral from portfolio before update
        if position.position_type == PositionType.BORROW:
            total_collateral = self._get_total_collateral_value(portfolio, market_state)
            self.set_position_collateral(position.position_id, total_collateral)

        # Delegate to standard update_position
        self.update_position(position, market_state, elapsed_seconds, timestamp)

    def sync_collateral_from_portfolio(
        self,
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> dict[str, Decimal]:
        """Sync collateral values for all borrow positions from portfolio.

        This method calculates total collateral from all SUPPLY positions
        and associates it with each BORROW position in the portfolio.
        Call this before update_position() when you don't have portfolio
        access in that method.

        Args:
            portfolio: The portfolio containing positions
            market_state: Current market prices and data

        Returns:
            Dictionary mapping borrow position IDs to their collateral values

        Example:
            # Sync before a batch update
            collateral_map = adapter.sync_collateral_from_portfolio(portfolio, market_state)

            # Then update each position
            for position in portfolio.positions:
                adapter.update_position(position, market_state, elapsed_seconds)
        """
        total_collateral = self._get_total_collateral_value(portfolio, market_state)
        result: dict[str, Decimal] = {}

        for position in portfolio.positions:
            if position.position_type == PositionType.BORROW:
                self.set_position_collateral(position.position_id, total_collateral)
                result[position.position_id] = total_collateral

        return result

    def value_position(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        timestamp: datetime | None = None,
    ) -> Decimal:
        """Calculate the current USD value of a lending position.

        This method computes lending position value as:
            SUPPLY: principal_value + accrued_interest (assets)
            BORROW: principal_value + accrued_interest (debt)

        Note that for BORROW positions, this returns the debt value
        (what is owed), not the negative of that value. The portfolio
        handles subtracting borrows from total value.

        Args:
            position: The lending position to value
            market_state: Current market prices and data
            timestamp: Simulation timestamp for deterministic valuation. If None,
                uses market_state.timestamp. Currently unused in lending valuation
                but accepted for interface consistency.

        Returns:
            Total position value in USD (principal + accrued interest)
        """
        # Note: timestamp parameter accepted for interface consistency
        # Lending valuation is based on current market prices, not time-dependent
        _ = timestamp
        # Get primary token and amount
        primary_token = position.primary_token
        amount = position.total_amount

        # Get current price
        try:
            current_price = market_state.get_price(primary_token)
        except KeyError:
            current_price = position.entry_price

        if current_price is None or current_price <= 0:
            current_price = position.entry_price

        # Calculate principal value
        principal_value = amount * current_price

        # Add accrued interest
        total_value = principal_value + position.interest_accrued

        logger.debug(
            "Lending position value: position=%s, type=%s, amount=%.4f, "
            "price=%.2f, principal=$%.2f, interest=$%.4f, total=$%.2f",
            position.position_id,
            position.position_type.value,
            float(amount),
            float(current_price),
            float(principal_value),
            float(position.interest_accrued),
            float(total_value),
        )

        return total_value

    def should_rebalance(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
    ) -> bool:
        """Determine if a lending position should be rebalanced.

        For lending positions, rebalancing is triggered when:
        - BORROW: Health factor is below warning threshold
        - SUPPLY: (No automatic rebalancing trigger for supply)

        The method checks health factor for borrow positions and returns
        True if the position is within the configured warning threshold.

        Args:
            position: The lending position to check
            market_state: Current market prices and data

        Returns:
            True if the position should be rebalanced (add collateral or repay debt),
            False otherwise

        Note:
            This method does NOT trigger on liquidation itself - only when
            approaching liquidation, giving the strategy a chance to adjust.
        """
        # Only process lending positions
        if position.position_type not in (PositionType.SUPPLY, PositionType.BORROW):
            return False

        # Supply positions don't have automatic rebalance triggers
        if position.position_type == PositionType.SUPPLY:
            return False

        # For borrow positions, check health factor
        if not self._config.health_factor_tracking_enabled:
            return False

        # Get health factor from position
        health_factor = position.health_factor
        if health_factor is None:
            return False

        # Check if below warning threshold
        if health_factor < self._config.health_factor_warning_threshold:
            logger.info(
                "Lending position %s should rebalance: HF=%.4f (warning threshold=%.2f)",
                position.position_id,
                float(health_factor),
                float(self._config.health_factor_warning_threshold),
            )
            return True

        return False

    def check_and_simulate_liquidation(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        timestamp: datetime,
    ) -> LendingLiquidationEvent | None:
        """Check if a position should be liquidated and simulate the liquidation.

        This method checks if the health factor has fallen below 1.0 for a
        borrow position. If liquidation is triggered:
        - A portion of the debt is repaid (close factor)
        - Collateral is seized plus liquidation penalty
        - The position is updated accordingly
        - A LendingLiquidationEvent is returned for recording

        Args:
            position: The lending position to check (modified in-place if liquidated)
            market_state: Current market prices and data
            timestamp: Current simulation timestamp

        Returns:
            LendingLiquidationEvent if liquidation occurred, None otherwise

        Note:
            This method only processes BORROW positions.
            SUPPLY positions cannot be liquidated.
        """
        # Only process borrow positions
        if position.position_type != PositionType.BORROW:
            return None

        # Skip if already liquidated
        if position.is_liquidated:
            return None

        # Skip if liquidation model is disabled
        if not self._config.liquidation_model_enabled:
            return None

        # Get health factor
        health_factor = position.health_factor
        if health_factor is None or health_factor >= Decimal("1.0"):
            return None

        # Position is undercollateralized - simulate liquidation

        # Get current debt value
        debt_value = self.value_position(position, market_state)

        # Calculate debt to repay (close factor)
        debt_to_repay = debt_value * self._config.liquidation_close_factor

        # Calculate collateral to seize (debt + penalty)
        collateral_to_seize = debt_to_repay * (Decimal("1") + self._config.liquidation_penalty)

        # Get tracked collateral for this position
        collateral_key = f"collateral_{position.position_id}"
        total_collateral = self._position_collateral.get(collateral_key, Decimal("0"))

        # Cap collateral seized at available collateral
        if collateral_to_seize > total_collateral:
            collateral_to_seize = total_collateral

        # Update position debt
        primary_token = position.primary_token
        try:
            token_price = market_state.get_price(primary_token)
        except KeyError:
            token_price = position.entry_price
        if token_price is None or token_price <= 0:
            token_price = position.entry_price

        # Reduce debt by repaid amount (in token terms)
        debt_reduction_tokens = debt_to_repay / token_price if token_price > 0 else Decimal("0")
        current_amount = position.amounts.get(primary_token, Decimal("0"))
        new_amount = max(Decimal("0"), current_amount - debt_reduction_tokens)

        # Proportionally reduce accrued interest when debt is reduced
        if current_amount > 0 and hasattr(position, "interest_accrued"):
            reduction_ratio = new_amount / current_amount
            position.interest_accrued = position.interest_accrued * reduction_ratio

        position.amounts[primary_token] = new_amount

        # Update collateral tracking
        self._position_collateral[collateral_key] = total_collateral - collateral_to_seize

        # Check if position is fully liquidated
        if new_amount <= Decimal("0"):
            position.is_liquidated = True

        # Update position metadata
        position.last_updated = timestamp
        position.metadata["liquidation_timestamp"] = timestamp.isoformat()
        position.metadata["liquidation_health_factor"] = str(health_factor)
        position.metadata["debt_repaid"] = str(debt_to_repay)
        position.metadata["collateral_seized"] = str(collateral_to_seize)

        # Create liquidation event
        event = LendingLiquidationEvent(
            timestamp=timestamp,
            position_id=position.position_id,
            health_factor=health_factor,
            collateral_seized=collateral_to_seize,
            debt_repaid=debt_to_repay,
            penalty=self._config.liquidation_penalty,
        )

        logger.warning(
            "LIQUIDATION: Position %s liquidated at HF=%.4f. "
            "Debt repaid: $%.2f, Collateral seized: $%.2f (penalty=%.1f%%)",
            position.position_id,
            float(health_factor),
            float(debt_to_repay),
            float(collateral_to_seize),
            float(self._config.liquidation_penalty * 100),
        )

        return event

    def set_position_collateral(
        self,
        position_id: str,
        collateral_usd: Decimal,
    ) -> None:
        """Set the collateral value for a borrow position.

        This method is used to track collateral associated with a borrow
        position for health factor calculations.

        Args:
            position_id: ID of the borrow position
            collateral_usd: Total collateral value in USD
        """
        collateral_key = f"collateral_{position_id}"
        self._position_collateral[collateral_key] = collateral_usd

    def get_health_factor(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
    ) -> Decimal | None:
        """Calculate current health factor for a borrow position.

        Args:
            position: The borrow position
            market_state: Current market prices and data

        Returns:
            Health factor or None if not applicable
        """
        if position.position_type != PositionType.BORROW:
            return None

        # Get collateral
        collateral_key = f"collateral_{position.position_id}"
        collateral_usd = self._position_collateral.get(collateral_key, Decimal("0"))

        # Get debt value
        debt_usd = self.value_position(position, market_state)

        # Get liquidation threshold
        protocol = position.protocol or self._config.protocol
        liq_threshold = self._health_factor_calculator.get_liquidation_threshold_for_protocol(protocol)

        # Calculate health factor
        result = self._health_factor_calculator.calculate_health_factor(
            collateral_value_usd=collateral_usd,
            debt_value_usd=debt_usd,
            liquidation_threshold=liq_threshold,
        )

        return result.health_factor

    def to_dict(self) -> dict[str, Any]:
        """Serialize the adapter configuration to a dictionary.

        Returns:
            Dictionary with adapter configuration
        """
        return {
            "adapter_name": self.adapter_name,
            "config": self._config.to_dict(),
        }


__all__ = [
    "LendingBacktestAdapter",
    "LendingBacktestConfig",
]
