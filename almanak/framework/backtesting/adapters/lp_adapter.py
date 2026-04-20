"""LP (Liquidity Provider) backtest adapter for concentrated liquidity positions.

This module provides the backtest adapter for LP strategies, handling
Uniswap V3 style concentrated liquidity positions. It manages:

- Fee accrual based on volume and liquidity share
- Impermanent loss calculation
- Out-of-range detection for rebalancing triggers
- Position valuation combining token values, fees, and IL

Key Features:
    - Configurable IL calculation method (standard, concentrated, simplified)
    - Fee tracking with token-level breakdown
    - Automatic out-of-range detection for rebalance triggers
    - Tick-based price range handling
    - Historical volume integration for realistic fee estimates via MultiDEXVolumeProvider
    - BacktestDataConfig integration for centralized data provider configuration

Example:
    from almanak.framework.backtesting.adapters.lp_adapter import (
        LPBacktestAdapter,
        LPBacktestConfig,
    )
    from almanak.framework.backtesting.config import BacktestDataConfig

    # Create config for LP backtesting with data provider configuration
    config = LPBacktestConfig(
        strategy_type="lp",
        il_calculation_method="concentrated",
        rebalance_on_out_of_range=True,
    )
    data_config = BacktestDataConfig(
        use_historical_volume=True,
        volume_fallback_multiplier=Decimal("10"),
    )

    # Get adapter instance with data config
    adapter = LPBacktestAdapter(config, data_config=data_config)

    # Use in backtesting
    fill = adapter.execute_intent(intent, portfolio, market_state)
"""

import asyncio
import logging
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

from almanak.core.enums import Chain
from almanak.framework.backtesting.adapters.base import (
    StrategyBacktestAdapter,
    StrategyBacktestConfig,
    register_adapter,
)
from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError
from almanak.framework.backtesting.models import FeeAccrualResult
from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
    ImpermanentLossCalculator,
)
from almanak.framework.backtesting.pnl.types import DataConfidence

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig
    from almanak.framework.backtesting.pnl.data_provider import MarketState
    from almanak.framework.backtesting.pnl.fee_models.slippage_guard import (
        HistoricalSlippageModel,
        HistoricalSlippageResult,
    )
    from almanak.framework.backtesting.pnl.portfolio import (
        SimulatedFill,
        SimulatedPortfolio,
        SimulatedPosition,
    )
    from almanak.framework.backtesting.pnl.providers.liquidity_depth import (
        LiquidityDepthProvider,
    )
    from almanak.framework.backtesting.pnl.providers.multi_dex_volume import (
        MultiDEXVolumeProvider,
    )
    from almanak.framework.backtesting.pnl.types import LiquidityResult
    from almanak.framework.intents.vocabulary import Intent

logger = logging.getLogger(__name__)

# Uniswap V3 tick constants
TICK_BASE = Decimal("1.0001")


class RangeStatus(StrEnum):
    """Status of price relative to LP position tick range.

    Attributes:
        IN_RANGE: Current price is within the position's tick range.
            Position is actively providing liquidity.
        BELOW_RANGE: Current price is below the position's lower tick.
            Position is 100% in token0 (no liquidity provision).
        ABOVE_RANGE: Current price is above the position's upper tick.
            Position is 100% in token1 (no liquidity provision).
        PARTIAL_BELOW: Price is approaching lower boundary (within margin).
            May want to rebalance soon.
        PARTIAL_ABOVE: Price is approaching upper boundary (within margin).
            May want to rebalance soon.
    """

    IN_RANGE = "IN_RANGE"
    BELOW_RANGE = "BELOW_RANGE"
    ABOVE_RANGE = "ABOVE_RANGE"
    PARTIAL_BELOW = "PARTIAL_BELOW"
    PARTIAL_ABOVE = "PARTIAL_ABOVE"


@dataclass
class RangeStatusResult:
    """Result of checking an LP position's range status.

    Attributes:
        status: The range status enum value.
        current_price_ratio: Current price of token0 in terms of token1.
        price_lower: Price at the lower tick boundary.
        price_upper: Price at the upper tick boundary.
        distance_to_lower_pct: Distance to lower boundary as percentage
            (negative if below, positive if above).
        distance_to_upper_pct: Distance to upper boundary as percentage
            (negative if above, positive if below).
        is_out_of_range: True if position is not providing liquidity.
        is_approaching_boundary: True if price is within margin of boundary.
    """

    status: RangeStatus
    current_price_ratio: Decimal
    price_lower: Decimal
    price_upper: Decimal
    distance_to_lower_pct: Decimal
    distance_to_upper_pct: Decimal

    @property
    def is_out_of_range(self) -> bool:
        """Check if position is fully out of range (not providing liquidity)."""
        return self.status in (RangeStatus.BELOW_RANGE, RangeStatus.ABOVE_RANGE)

    @property
    def is_approaching_boundary(self) -> bool:
        """Check if price is approaching a boundary."""
        return self.status in (RangeStatus.PARTIAL_BELOW, RangeStatus.PARTIAL_ABOVE)

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "status": self.status.value,
            "current_price_ratio": str(self.current_price_ratio),
            "price_lower": str(self.price_lower),
            "price_upper": str(self.price_upper),
            "distance_to_lower_pct": str(self.distance_to_lower_pct),
            "distance_to_upper_pct": str(self.distance_to_upper_pct),
            "is_out_of_range": self.is_out_of_range,
            "is_approaching_boundary": self.is_approaching_boundary,
        }


@dataclass
class LPBacktestConfig(StrategyBacktestConfig):
    """Configuration for LP-specific backtesting.

    This config extends the base StrategyBacktestConfig with LP-specific
    options for controlling fee tracking, impermanent loss calculation,
    and rebalancing behavior.

    Attributes:
        strategy_type: Must be "lp" for LP adapter (inherited)
        fee_tracking_enabled: Whether to track and accrue LP fees (inherited)
        position_tracking_enabled: Whether to track positions in detail (inherited)
        reconcile_on_tick: Whether to reconcile position state each tick (inherited)
        extra_params: Additional parameters (inherited)
        il_calculation_method: Method for calculating impermanent loss:
            - "standard": Full V3 concentrated liquidity IL calculation
            - "concentrated": Same as standard (V3-native)
            - "simplified": Simplified IL approximation for faster backtests
        rebalance_on_out_of_range: Whether to signal rebalance when price
            moves outside the position's tick range. When True, should_rebalance()
            returns True if current price is outside [tick_lower, tick_upper].
        volume_multiplier: Multiplier for estimating trading volume in fee
            calculations. Higher values simulate more active pools. Default 10.
        base_liquidity: Base pool liquidity for calculating liquidity share.
            Position's share = min(1, position.liquidity / base_liquidity).
            Default 1_000_000.

    Example:
        config = LPBacktestConfig(
            strategy_type="lp",
            fee_tracking_enabled=True,
            il_calculation_method="concentrated",
            rebalance_on_out_of_range=True,
            volume_multiplier=20,  # More active pool
        )
    """

    il_calculation_method: Literal["standard", "concentrated", "simplified"] = "standard"
    """Method for calculating impermanent loss."""

    rebalance_on_out_of_range: bool = True
    """Whether to signal rebalance when price is outside tick range."""

    rebalance_on_partial_exit: bool = False
    """Whether to signal rebalance when price approaches boundary (within margin)."""

    boundary_margin_pct: Decimal = Decimal("5")
    """Percentage margin from boundary to consider 'approaching'.
    Default 5% means if price is within 5% of lower or upper tick,
    the position is considered PARTIAL_BELOW or PARTIAL_ABOVE."""

    volume_multiplier: Decimal = Decimal("10")
    """Multiplier for estimating trading volume in fee calculations."""

    base_liquidity: Decimal = Decimal("1000000")
    """Base pool liquidity for calculating liquidity share."""

    use_historical_volume: bool = True
    """Whether to attempt fetching historical volume from subgraph.
    If True, will try to use actual pool volume for fee calculation.
    Falls back to volume_multiplier heuristic if data unavailable."""

    chain: str = "arbitrum"
    """Chain for subgraph queries. Options: ethereum, arbitrum, base, optimism, polygon."""

    subgraph_api_key: str | None = None
    """API key for The Graph Gateway (recommended for production).
    If not provided, will attempt to use hosted service (may be rate limited)."""

    def __post_init__(self) -> None:
        """Validate LP-specific configuration.

        Raises:
            ValueError: If strategy_type is not "lp" or il_calculation_method
                is invalid.
        """
        # Call parent validation
        super().__post_init__()

        # Validate strategy_type for LP
        if self.strategy_type.lower() != "lp":
            msg = f"LPBacktestConfig requires strategy_type='lp', got '{self.strategy_type}'"
            raise ValueError(msg)

        # Validate il_calculation_method
        valid_methods = {"standard", "concentrated", "simplified"}
        if self.il_calculation_method not in valid_methods:
            msg = f"il_calculation_method must be one of {valid_methods}, got '{self.il_calculation_method}'"
            raise ValueError(msg)

    def to_dict(self) -> dict[str, Any]:
        """Serialize configuration to a dictionary.

        Returns:
            Dictionary representation of the configuration.
        """
        base = super().to_dict()
        base.update(
            {
                "il_calculation_method": self.il_calculation_method,
                "rebalance_on_out_of_range": self.rebalance_on_out_of_range,
                "rebalance_on_partial_exit": self.rebalance_on_partial_exit,
                "boundary_margin_pct": str(self.boundary_margin_pct),
                "volume_multiplier": str(self.volume_multiplier),
                "base_liquidity": str(self.base_liquidity),
                "use_historical_volume": self.use_historical_volume,
                "chain": self.chain,
                "subgraph_api_key": self.subgraph_api_key,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LPBacktestConfig":
        """Create configuration from a dictionary.

        Args:
            data: Dictionary with configuration values.

        Returns:
            New LPBacktestConfig instance.
        """
        return cls(
            strategy_type=data.get("strategy_type", "lp"),
            fee_tracking_enabled=data.get("fee_tracking_enabled", True),
            position_tracking_enabled=data.get("position_tracking_enabled", True),
            reconcile_on_tick=data.get("reconcile_on_tick", False),
            extra_params=data.get("extra_params", {}),
            strict_reproducibility=data.get("strict_reproducibility", False),
            il_calculation_method=data.get("il_calculation_method", "standard"),
            rebalance_on_out_of_range=data.get("rebalance_on_out_of_range", True),
            rebalance_on_partial_exit=data.get("rebalance_on_partial_exit", False),
            boundary_margin_pct=Decimal(str(data.get("boundary_margin_pct", "5"))),
            volume_multiplier=Decimal(str(data.get("volume_multiplier", "10"))),
            base_liquidity=Decimal(str(data.get("base_liquidity", "1000000"))),
            use_historical_volume=data.get("use_historical_volume", True),
            chain=data.get("chain", "arbitrum"),
            subgraph_api_key=data.get("subgraph_api_key"),
        )


@register_adapter(
    "lp",
    description="Adapter for LP/AMM strategies with concentrated liquidity support",
    aliases=["liquidity_provider", "amm", "uniswap_v3"],
)
class LPBacktestAdapter(StrategyBacktestAdapter):
    """Backtest adapter for LP (Liquidity Provider) strategies.

    This adapter handles the simulation of Uniswap V3 style concentrated
    liquidity positions during backtesting. It provides:

    - Fee accrual based on volume, fee tier, and liquidity share
    - Impermanent loss calculation using V3 math
    - Out-of-range detection for triggering rebalances
    - Position valuation combining tokens + fees - IL

    The adapter can be used with or without explicit configuration.
    When used without config, it uses sensible defaults.

    When BacktestDataConfig is provided, the adapter uses MultiDEXVolumeProvider
    to fetch historical volume data from multiple DEX subgraphs (Uniswap V3,
    SushiSwap V3, PancakeSwap V3, Aerodrome, TraderJoe V2, Curve, Balancer).

    Attributes:
        config: LP-specific configuration (optional)
        data_config: BacktestDataConfig for historical data provider settings (optional)

    Example:
        # With config and data_config
        from almanak.framework.backtesting.config import BacktestDataConfig

        config = LPBacktestConfig(
            strategy_type="lp",
            il_calculation_method="concentrated",
        )
        data_config = BacktestDataConfig(
            use_historical_volume=True,
            volume_fallback_multiplier=Decimal("10"),
        )
        adapter = LPBacktestAdapter(config, data_config=data_config)

        # Without config (uses defaults)
        adapter = LPBacktestAdapter()

        # In backtesting loop
        if adapter.should_rebalance(position, market_state):
            # Strategy should consider rebalancing
            pass

        # Value position
        value = adapter.value_position(position, market_state)
    """

    def __init__(
        self,
        config: LPBacktestConfig | None = None,
        data_config: "BacktestDataConfig | None" = None,
        volume_provider: "MultiDEXVolumeProvider | None" = None,
        liquidity_provider: "LiquidityDepthProvider | None" = None,
        slippage_model: "HistoricalSlippageModel | None" = None,
    ) -> None:
        """Initialize the LP backtest adapter.

        Args:
            config: LP-specific configuration. If None, uses default
                LPBacktestConfig with strategy_type="lp".
            data_config: BacktestDataConfig for controlling historical data provider
                behavior. When provided, overrides config.use_historical_volume and
                config.volume_multiplier with data_config values.
            volume_provider: Optional pre-configured MultiDEXVolumeProvider.
                If None and use_historical_volume=True, will create one lazily.
            liquidity_provider: Optional pre-configured LiquidityDepthProvider.
                If None and use_historical_liquidity=True, will create one lazily.
            slippage_model: Optional pre-configured HistoricalSlippageModel.
                If None, will create one with default config when needed.
        """
        self._config = config or LPBacktestConfig(strategy_type="lp")
        self._data_config = data_config
        self._il_calculator = ImpermanentLossCalculator()
        self._volume_provider: MultiDEXVolumeProvider | None = volume_provider
        self._volume_provider_initialized = volume_provider is not None

        # Liquidity depth provider for historical slippage calculations
        self._liquidity_provider: LiquidityDepthProvider | None = liquidity_provider
        self._liquidity_provider_initialized = liquidity_provider is not None

        # Slippage model for calculating slippage from liquidity depth
        self._slippage_model: HistoricalSlippageModel | None = slippage_model
        self._slippage_model_initialized = slippage_model is not None

        # Cache for volume data to avoid repeated queries
        # Key: (pool_address, date) -> (volume_usd, confidence)
        self._volume_cache: dict[tuple[str, date], tuple[Decimal | None, DataConfidence]] = {}

        # Cache for liquidity depth data to avoid repeated queries
        # Key: (pool_address, date) -> LiquidityResult
        self._liquidity_cache: dict[tuple[str, date], LiquidityResult] = {}

    @property
    def adapter_name(self) -> str:
        """Return the unique name of this adapter.

        Returns:
            Strategy type identifier "lp"
        """
        return "lp"

    @property
    def config(self) -> LPBacktestConfig:
        """Get the adapter configuration.

        Returns:
            LP backtest configuration
        """
        return self._config

    def _use_historical_volume(self) -> bool:
        """Check if historical volume data should be used.

        Uses BacktestDataConfig.use_historical_volume if data_config provided,
        otherwise falls back to LPBacktestConfig.use_historical_volume.

        Returns:
            True if historical volume should be fetched from subgraph.
        """
        if self._data_config is not None:
            return self._data_config.use_historical_volume
        return self._config.use_historical_volume

    def _get_volume_fallback_multiplier(self) -> Decimal:
        """Get the volume fallback multiplier to use.

        Uses BacktestDataConfig.volume_fallback_multiplier if data_config provided,
        otherwise falls back to LPBacktestConfig.volume_multiplier.

        Returns:
            Volume multiplier for fallback fee estimation.
        """
        if self._data_config is not None:
            return self._data_config.volume_fallback_multiplier
        return self._config.volume_multiplier

    def _use_historical_liquidity(self) -> bool:
        """Check if historical liquidity depth should be used.

        Uses BacktestDataConfig.use_historical_liquidity if data_config provided,
        otherwise defaults to True (enabled by default for accurate slippage).

        Returns:
            True if historical liquidity should be fetched from subgraph.
        """
        if self._data_config is not None:
            return self._data_config.use_historical_liquidity
        return True  # Default to enabled for accurate slippage modeling

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

    # Known stablecoins for which a $1 fallback is reasonable
    _STABLECOIN_SYMBOLS: frozenset[str] = frozenset(
        {"USDC", "USDT", "DAI", "FRAX", "LUSD", "USDC.E", "USDT.E", "BUSD", "TUSD", "USDBC"}
    )

    def _price_fallback(self, token: str, fallback: Decimal, context: str) -> Decimal:
        """Return *fallback* price for *token* while logging a warning.

        In ``strict_reproducibility`` mode an error is raised instead.
        When the fallback is $1, a stronger warning is emitted for non-stablecoin tokens.
        """
        if self._config.strict_reproducibility:
            raise HistoricalDataUnavailableError(
                data_type="price",
                identifier=token,
                timestamp=datetime.now(),
                message=f"Price unavailable for {token} in {context} and strict_reproducibility=True",
            )
        is_stablecoin = token.upper() in self._STABLECOIN_SYMBOLS
        if fallback == Decimal("1") and not is_stablecoin:
            logger.warning(
                "Price unavailable for NON-STABLECOIN %s in %s, falling back to $1 assumption -- "
                "this may produce inaccurate results. Consider enabling strict_reproducibility.",
                token,
                context,
            )
        else:
            logger.warning(
                "Price unavailable for %s in %s, falling back to $%s assumption",
                token,
                context,
                fallback,
            )
        return fallback

    def _ensure_liquidity_provider(self) -> "LiquidityDepthProvider | None":
        """Lazily initialize the liquidity depth provider if needed.

        Creates a LiquidityDepthProvider instance for fetching historical liquidity
        depth data from multiple DEX subgraphs. Uses rate limiting from BacktestDataConfig
        if provided.

        Returns:
            LiquidityDepthProvider instance or None if disabled/failed
        """
        if not self._use_historical_liquidity():
            return None

        if self._liquidity_provider_initialized:
            return self._liquidity_provider

        try:
            from almanak.framework.backtesting.pnl.providers.liquidity_depth import (
                LiquidityDepthProvider,
            )

            # Use rate limit from data_config if available
            requests_per_minute = 100
            if self._data_config is not None:
                requests_per_minute = self._data_config.subgraph_rate_limit_per_minute

            self._liquidity_provider = LiquidityDepthProvider(
                fallback_depth=Decimal("0"),
                use_twap=True,  # Use TWAP for more stable slippage estimates
                requests_per_minute=requests_per_minute,
            )
            self._liquidity_provider_initialized = True
            logger.debug(
                "Initialized LiquidityDepthProvider with rate_limit=%s/min, use_twap=True",
                requests_per_minute,
            )
            return self._liquidity_provider
        except Exception as e:
            logger.warning(
                "Failed to initialize LiquidityDepthProvider: %s. Will use fallback slippage.",
                e,
            )
            self._liquidity_provider_initialized = True  # Don't retry
            self._liquidity_provider = None
            return None

    def _ensure_slippage_model(self) -> "HistoricalSlippageModel":
        """Lazily initialize the slippage model if needed.

        Creates a HistoricalSlippageModel instance for calculating slippage
        from historical liquidity depth.

        Returns:
            HistoricalSlippageModel instance
        """
        if self._slippage_model_initialized and self._slippage_model is not None:
            return self._slippage_model

        try:
            from almanak.framework.backtesting.pnl.fee_models.slippage_guard import (
                HistoricalSlippageModel,
                SlippageModelConfig,
            )

            config = SlippageModelConfig(
                use_twap_depth=True,  # Use TWAP depth for more stable estimates
            )
            self._slippage_model = HistoricalSlippageModel(config=config)
            self._slippage_model_initialized = True
            logger.debug("Initialized HistoricalSlippageModel with use_twap_depth=True")
            return self._slippage_model
        except Exception as e:
            logger.warning(
                "Failed to initialize HistoricalSlippageModel: %s. Slippage will not be calculated.",
                e,
            )
            # Create a minimal model instance to avoid repeated failures
            from almanak.framework.backtesting.pnl.fee_models.slippage_guard import (
                HistoricalSlippageModel,
            )

            self._slippage_model = HistoricalSlippageModel()
            self._slippage_model_initialized = True
            return self._slippage_model

    def _get_historical_liquidity(
        self,
        pool_address: str | None,
        timestamp: datetime,
        chain: Chain | None = None,
        protocol: str | None = None,
    ) -> "LiquidityResult | None":
        """Get historical liquidity depth for a pool at a specific timestamp.

        Fetches liquidity depth data from LiquidityDepthProvider which routes to
        the correct DEX-specific subgraph based on protocol or chain detection.

        Args:
            pool_address: The pool contract address (optional)
            timestamp: The timestamp to get liquidity for
            chain: Chain enum for subgraph routing (optional, defaults to config.chain)
            protocol: Protocol identifier (e.g., "uniswap_v3", "aerodrome") (optional)

        Returns:
            LiquidityResult with depth and confidence, or None if unavailable
            (unless strict mode is enabled).

        Raises:
            HistoricalDataUnavailableError: If strict_historical_mode is True and
                historical liquidity data cannot be fetched.
        """
        if not pool_address:
            if self._is_strict_historical_mode():
                raise HistoricalDataUnavailableError(
                    data_type="liquidity",
                    identifier="unknown",
                    timestamp=timestamp,
                    message="Pool address not provided for historical liquidity lookup",
                    chain=self._config.chain,
                    protocol=protocol,
                )
            return None

        provider = self._ensure_liquidity_provider()
        if provider is None:
            if self._is_strict_historical_mode():
                raise HistoricalDataUnavailableError(
                    data_type="liquidity",
                    identifier=pool_address,
                    timestamp=timestamp,
                    message="Liquidity provider not available (historical liquidity disabled or failed to initialize)",
                    chain=self._config.chain,
                    protocol=protocol,
                )
            return None

        # Normalize pool address and get date
        pool_address_lower = pool_address.lower()
        target_date = timestamp.date() if isinstance(timestamp, datetime) else timestamp

        # Check cache first
        cache_key = (pool_address_lower, target_date)
        if cache_key in self._liquidity_cache:
            return self._liquidity_cache[cache_key]

        # Determine chain from config if not provided
        if chain is None:
            chain_str = self._config.chain.upper()
            try:
                chain = Chain[chain_str]
            except KeyError:
                if self._is_strict_historical_mode():
                    raise HistoricalDataUnavailableError(
                        data_type="liquidity",
                        identifier=pool_address_lower,
                        timestamp=timestamp,
                        message=f"Unknown chain '{chain_str}', cannot fetch historical liquidity",
                        chain=chain_str,
                        protocol=protocol,
                    ) from None
                logger.warning("Unknown chain '%s', cannot fetch historical liquidity", chain_str)
                return None

        # Fetch from LiquidityDepthProvider (using asyncio to run async method)
        try:
            # Try to get existing event loop
            try:
                loop = asyncio.get_running_loop()
                loop_is_running = True
            except RuntimeError:
                # No running loop - create one
                loop = asyncio.new_event_loop()
                loop_is_running = False

            # Handle case where we're already in an async context
            if loop_is_running:
                # Avoid blocking the running loop if we're in an async task
                if asyncio.current_task() is not None:
                    if self._is_strict_historical_mode():
                        raise HistoricalDataUnavailableError(
                            data_type="liquidity",
                            identifier=pool_address_lower,
                            timestamp=timestamp,
                            message="Cannot fetch historical liquidity in async context",
                            chain=chain.value if chain else self._config.chain,
                            protocol=protocol,
                        )
                    logger.debug("Historical liquidity fetch skipped in async context; using fallback.")
                    return None
                # Schedule coroutine on the running loop (thread-safe)
                future = asyncio.run_coroutine_threadsafe(
                    provider.get_liquidity_depth(
                        pool_address=pool_address_lower,
                        chain=chain,
                        timestamp=timestamp,
                        protocol=protocol,
                    ),
                    loop,
                )
                try:
                    liquidity_result = future.result(timeout=30)
                except TimeoutError:
                    future.cancel()
                    raise
            else:
                try:
                    liquidity_result = loop.run_until_complete(
                        provider.get_liquidity_depth(
                            pool_address=pool_address_lower,
                            chain=chain,
                            timestamp=timestamp,
                            protocol=protocol,
                        )
                    )
                finally:
                    loop.close()

            # Check if result is valid (has non-zero depth or is from HIGH confidence source)
            if liquidity_result.depth <= 0 and liquidity_result.source_info.confidence == DataConfidence.LOW:
                # Result is a fallback/low-confidence value
                if self._is_strict_historical_mode():
                    raise HistoricalDataUnavailableError(
                        data_type="liquidity",
                        identifier=pool_address_lower,
                        timestamp=timestamp,
                        message="No historical liquidity data available (returned low-confidence fallback)",
                        chain=chain.value if chain else self._config.chain,
                        protocol=protocol,
                    )

            # Cache the result
            self._liquidity_cache[cache_key] = liquidity_result

            logger.debug(
                "Fetched historical liquidity for pool %s... on %s: depth=$%.2f, confidence=%s, source=%s",
                pool_address_lower[:10],
                target_date,
                float(liquidity_result.depth),
                liquidity_result.source_info.confidence.value,
                liquidity_result.source_info.source,
            )
            return liquidity_result

        except HistoricalDataUnavailableError:
            # Re-raise if it's already our exception
            raise
        except Exception as e:
            if self._is_strict_historical_mode():
                raise HistoricalDataUnavailableError(
                    data_type="liquidity",
                    identifier=pool_address_lower,
                    timestamp=timestamp,
                    message=f"Failed to fetch historical liquidity: {e}",
                    chain=chain.value if chain else self._config.chain,
                    protocol=protocol,
                ) from e
            logger.debug(
                "Failed to fetch historical liquidity for pool %s on %s: %s",
                pool_address_lower[:10],
                target_date,
                e,
            )
            return None

    def _calculate_slippage(
        self,
        trade_amount_usd: Decimal,
        pool_address: str | None,
        timestamp: datetime,
        chain: Chain | None = None,
        protocol: str | None = None,
        pool_type: str = "v3",
    ) -> "HistoricalSlippageResult":
        """Calculate slippage using historical liquidity depth.

        Uses the HistoricalSlippageModel with liquidity depth from LiquidityDepthProvider
        to calculate accurate slippage for backtesting. Falls back to constant product
        math when historical data is unavailable.

        Args:
            trade_amount_usd: Trade size in USD.
            pool_address: Pool contract address for liquidity lookup.
            timestamp: Timestamp for historical liquidity query.
            chain: Chain enum for subgraph routing (optional).
            protocol: Protocol identifier (optional).
            pool_type: Type of pool ("v2" or "v3") for slippage calculation.

        Returns:
            HistoricalSlippageResult with slippage, confidence, and source info.
        """
        slippage_model = self._ensure_slippage_model()

        # Get historical liquidity depth if available
        liquidity_result = None
        if pool_address and self._use_historical_liquidity():
            liquidity_result = self._get_historical_liquidity(
                pool_address=pool_address,
                timestamp=timestamp,
                chain=chain,
                protocol=protocol,
            )

        # Calculate slippage
        slippage_result = slippage_model.calculate_slippage(
            trade_amount_usd=trade_amount_usd,
            historical_liquidity=liquidity_result,
            pool_type=pool_type,
        )

        if slippage_result.was_fallback:
            logger.warning(
                "Slippage calculation using fallback: trade=$%.2f, pool=%s, slippage=%.4f%%, confidence=%s",
                float(trade_amount_usd),
                pool_address[:10] if pool_address else "unknown",
                float(slippage_result.slippage_pct),
                slippage_result.confidence.value,
            )
        else:
            logger.debug(
                "Slippage calculated: trade=$%.2f, pool=%s, slippage=%.4f%%, liquidity=$%.2f, confidence=%s",
                float(trade_amount_usd),
                pool_address[:10] if pool_address else "unknown",
                float(slippage_result.slippage_pct),
                float(slippage_result.liquidity_usd),
                slippage_result.confidence.value,
            )

        return slippage_result

    def _ensure_volume_provider(self) -> "MultiDEXVolumeProvider | None":
        """Lazily initialize the volume provider if needed.

        Creates a MultiDEXVolumeProvider instance for fetching historical volume
        data from multiple DEX subgraphs. Uses rate limiting from BacktestDataConfig
        if provided.

        Returns:
            MultiDEXVolumeProvider instance or None if disabled/failed
        """
        if not self._use_historical_volume():
            return None

        if self._volume_provider_initialized:
            return self._volume_provider

        try:
            from almanak.framework.backtesting.pnl.providers.multi_dex_volume import (
                MultiDEXVolumeProvider,
            )

            # Use rate limit from data_config if available
            requests_per_minute = 100
            if self._data_config is not None:
                requests_per_minute = self._data_config.subgraph_rate_limit_per_minute

            self._volume_provider = MultiDEXVolumeProvider(
                fallback_volume=Decimal("0"),
                requests_per_minute=requests_per_minute,
            )
            self._volume_provider_initialized = True
            logger.debug(
                "Initialized MultiDEXVolumeProvider with rate_limit=%s/min",
                requests_per_minute,
            )
            return self._volume_provider
        except Exception as e:
            logger.warning(
                "Failed to initialize MultiDEXVolumeProvider: %s. Will use volume multiplier heuristic.",
                e,
            )
            self._volume_provider_initialized = True  # Don't retry
            self._volume_provider = None
            return None

    def _get_historical_volume(
        self,
        pool_address: str | None,
        timestamp: datetime,
        chain: Chain | None = None,
        protocol: str | None = None,
    ) -> tuple[Decimal | None, DataConfidence]:
        """Get historical pool volume for a specific date.

        Fetches volume data from MultiDEXVolumeProvider which routes to the correct
        DEX-specific subgraph based on protocol or chain detection.

        Args:
            pool_address: The pool contract address (optional)
            timestamp: The timestamp to get volume for
            chain: Chain enum for subgraph routing (optional, defaults to config.chain)
            protocol: Protocol identifier (e.g., "uniswap_v3", "aerodrome") (optional)

        Returns:
            Tuple of (volume in USD, confidence level). Returns (None, LOW) if
            volume data is unavailable (unless strict mode is enabled).

        Raises:
            HistoricalDataUnavailableError: If strict_historical_mode is True and
                historical volume data cannot be fetched.
        """
        if not pool_address:
            if self._is_strict_historical_mode():
                raise HistoricalDataUnavailableError(
                    data_type="volume",
                    identifier="unknown",
                    timestamp=timestamp,
                    message="Pool address not provided for historical volume lookup",
                    chain=self._config.chain,
                    protocol=protocol,
                )
            return None, DataConfidence.LOW

        provider = self._ensure_volume_provider()
        if provider is None:
            if self._is_strict_historical_mode():
                raise HistoricalDataUnavailableError(
                    data_type="volume",
                    identifier=pool_address,
                    timestamp=timestamp,
                    message="Volume provider not available (historical volume disabled or failed to initialize)",
                    chain=self._config.chain,
                    protocol=protocol,
                )
            return None, DataConfidence.LOW

        # Normalize pool address and get date
        pool_address_lower = pool_address.lower()
        target_date = timestamp.date() if isinstance(timestamp, datetime) else timestamp

        # Check cache first
        cache_key = (pool_address_lower, target_date)
        if cache_key in self._volume_cache:
            return self._volume_cache[cache_key]

        # Determine chain from config if not provided
        if chain is None:
            chain_str = self._config.chain.upper()
            try:
                chain = Chain[chain_str]
            except KeyError:
                if self._is_strict_historical_mode():
                    raise HistoricalDataUnavailableError(
                        data_type="volume",
                        identifier=pool_address_lower,
                        timestamp=timestamp,
                        message=f"Unknown chain '{chain_str}', cannot fetch historical volume",
                        chain=chain_str,
                        protocol=protocol,
                    ) from None
                logger.warning("Unknown chain '%s', cannot fetch historical volume", chain_str)
                self._volume_cache[cache_key] = (None, DataConfidence.LOW)
                return None, DataConfidence.LOW

        # Fetch from MultiDEXVolumeProvider (using asyncio to run async method)
        try:
            # Try to get existing event loop
            try:
                loop = asyncio.get_running_loop()
                loop_is_running = True
            except RuntimeError:
                # No running loop - create one
                loop = asyncio.new_event_loop()
                loop_is_running = False

            # Handle case where we're already in an async context
            if loop_is_running:
                # Avoid blocking the running loop if we're in an async task
                if asyncio.current_task() is not None:
                    if self._is_strict_historical_mode():
                        raise HistoricalDataUnavailableError(
                            data_type="volume",
                            identifier=pool_address_lower,
                            timestamp=timestamp,
                            message="Cannot fetch historical volume in async context",
                            chain=chain.value if chain else self._config.chain,
                            protocol=protocol,
                        )
                    logger.debug("Historical volume fetch skipped in async context; using fallback.")
                    self._volume_cache[cache_key] = (None, DataConfidence.LOW)
                    return None, DataConfidence.LOW
                # Schedule coroutine on the running loop (thread-safe)
                future = asyncio.run_coroutine_threadsafe(
                    provider.get_volume(
                        pool_address=pool_address_lower,
                        chain=chain,
                        start_date=target_date,
                        end_date=target_date,
                        protocol=protocol,
                    ),
                    loop,
                )
                try:
                    volume_results = future.result(timeout=30)
                except TimeoutError:
                    future.cancel()
                    raise
            else:
                try:
                    volume_results = loop.run_until_complete(
                        provider.get_volume(
                            pool_address=pool_address_lower,
                            chain=chain,
                            start_date=target_date,
                            end_date=target_date,
                            protocol=protocol,
                        )
                    )
                finally:
                    loop.close()

            if volume_results and len(volume_results) > 0:
                volume_result = volume_results[0]
                volume_usd = volume_result.value
                confidence = volume_result.source_info.confidence
                self._volume_cache[cache_key] = (volume_usd, confidence)

                logger.debug(
                    "Fetched historical volume for pool %s... on %s: volume=$%.2f, confidence=%s, source=%s",
                    pool_address_lower[:10],
                    target_date,
                    float(volume_usd),
                    confidence.value,
                    volume_result.source_info.source,
                )
                return volume_usd, confidence
            else:
                # No data returned from provider
                if self._is_strict_historical_mode():
                    raise HistoricalDataUnavailableError(
                        data_type="volume",
                        identifier=pool_address_lower,
                        timestamp=timestamp,
                        message="No historical volume data returned from subgraph",
                        chain=chain.value if chain else self._config.chain,
                        protocol=protocol,
                    )
                self._volume_cache[cache_key] = (None, DataConfidence.LOW)
                return None, DataConfidence.LOW

        except HistoricalDataUnavailableError:
            # Re-raise if it's already our exception
            raise
        except Exception as e:
            if self._is_strict_historical_mode():
                raise HistoricalDataUnavailableError(
                    data_type="volume",
                    identifier=pool_address_lower,
                    timestamp=timestamp,
                    message=f"Failed to fetch historical volume: {e}",
                    chain=chain.value if chain else self._config.chain,
                    protocol=protocol,
                ) from e
            logger.debug(
                "Failed to fetch historical volume for pool %s on %s: %s",
                pool_address_lower[:10],
                target_date,
                e,
            )
            self._volume_cache[cache_key] = (None, DataConfidence.LOW)
            return None, DataConfidence.LOW

    def execute_intent(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill | None":
        """Simulate execution of an LP-related intent.

        This method handles LP_OPEN, LP_CLOSE, and LP_COLLECT_FEES intents.
        For LP_OPEN, it simulates entering a concentrated liquidity position.
        For LP_CLOSE, it calculates final fees and IL before closing.

        Args:
            intent: The intent to execute (LPOpenIntent, LPCloseIntent, etc.)
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            SimulatedFill describing the execution result, or None to use
            default execution logic.
        """
        from almanak.framework.intents.vocabulary import LPCloseIntent, LPOpenIntent

        # Handle LP_OPEN intent
        if isinstance(intent, LPOpenIntent):
            return self._execute_lp_open(intent, portfolio, market_state)

        # Handle LP_CLOSE intent
        if isinstance(intent, LPCloseIntent):
            return self._execute_lp_close(intent, portfolio, market_state)

        # Not an LP intent we handle, let default execution handle it
        return None

    def _execute_lp_open(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill":
        """Execute an LP_OPEN intent to create a concentrated liquidity position.

        Args:
            intent: LPOpenIntent to execute
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            SimulatedFill with the created LP position
        """
        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl.portfolio import (
            SimulatedFill,
            SimulatedPosition,
        )
        from almanak.framework.intents.vocabulary import LPOpenIntent

        # Type narrowing
        if not isinstance(intent, LPOpenIntent):
            # Return a failed fill if called with wrong intent type
            return SimulatedFill(
                timestamp=market_state.timestamp,
                intent_type=IntentType.LP_OPEN,
                protocol="unknown",
                tokens=[],
                executed_price=Decimal("0"),
                amount_usd=Decimal("0"),
                fee_usd=Decimal("0"),
                slippage_usd=Decimal("0"),
                gas_cost_usd=Decimal("0"),
                tokens_in={},
                tokens_out={},
                success=False,
                metadata={"failure_reason": "Invalid intent type"},
            )

        # Extract pool identifier to get token symbols
        # Pool format can be "TOKEN0/TOKEN1" or just an address
        pool = intent.pool
        if "/" in pool:
            token0, token1 = pool.split("/")[:2]
            token0 = token0.strip().upper()
            token1 = token1.strip().upper()
        else:
            # Default to common pair if pool is an address
            token0 = "WETH"
            token1 = "USDC"

        # Extract amounts from intent
        amount0 = Decimal(str(intent.amount0))
        amount1 = Decimal(str(intent.amount1))

        # Get current prices
        try:
            token0_price = market_state.get_price(token0)
        except KeyError:
            token0_price = None

        try:
            token1_price = market_state.get_price(token1)
        except KeyError:
            token1_price = None

        if token0_price is None or token0_price <= 0:
            token0_price = self._price_fallback(token0, Decimal("1"), "execute_open")
        if token1_price is None or token1_price <= 0:
            token1_price = self._price_fallback(token1, Decimal("1"), "execute_open")

        # Calculate total USD value of position
        amount_usd = amount0 * token0_price + amount1 * token1_price

        # Calculate entry price ratio (token0 in terms of token1)
        entry_price = token0_price / token1_price if token1_price > 0 else token0_price

        # Convert price ranges to ticks
        range_lower = Decimal(str(intent.range_lower))
        range_upper = Decimal(str(intent.range_upper))

        # Convert prices to ticks using Uniswap V3 formula: tick = floor(log(price) / log(1.0001))
        tick_lower = self._price_to_tick_int(range_lower)
        tick_upper = self._price_to_tick_int(range_upper)

        # Get fee tier from protocol (default 0.3% for Uniswap V3)
        # Use explicit suffix matching to avoid false positives (e.g., "uniswap_v1" matching "1")
        protocol = intent.protocol.lower()
        if protocol.endswith("_0.01") or protocol.endswith("_1bps") or "_0.01_" in protocol:
            fee_tier = Decimal("0.0001")  # 0.01% / 1 bps
        elif protocol.endswith("_0.05") or protocol.endswith("_5bps") or "_0.05_" in protocol:
            fee_tier = Decimal("0.0005")  # 0.05% / 5 bps
        elif protocol.endswith("_1") or protocol.endswith("_100bps") or "_1_" in protocol:
            fee_tier = Decimal("0.01")  # 1% / 100 bps
        elif protocol.endswith("_0.3") or protocol.endswith("_30bps") or "_0.3_" in protocol:
            fee_tier = Decimal("0.003")  # 0.3% / 30 bps
        else:
            fee_tier = Decimal("0.003")  # Default 0.3%

        # Estimate liquidity based on amount and price range
        # For V3: L = sqrt(x * y) where x, y are virtual amounts
        # Simplified estimate: use total USD value as proxy
        liquidity = amount_usd

        # Create the LP position
        position = SimulatedPosition.lp(
            token0=token0,
            token1=token1,
            amount0=amount0,
            amount1=amount1,
            liquidity=liquidity,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            fee_tier=fee_tier,
            entry_price=entry_price,
            entry_time=market_state.timestamp,
            protocol=protocol,
        )

        # Store entry amounts in metadata for later IL calculation
        position.metadata["entry_amounts"] = {
            token0: str(amount0),
            token1: str(amount1),
        }
        position.metadata["entry_price_ratio"] = str(entry_price)

        # Store pool address for historical volume lookup in fee accrual
        # The pool parameter can be either "TOKEN0/TOKEN1" format or a contract address
        if "/" not in intent.pool and intent.pool.startswith("0x"):
            position.metadata["pool_address"] = intent.pool.lower()
        else:
            # For token pair format, we can't automatically determine pool address
            # Users should provide the actual pool address for historical volume lookup
            position.metadata["pool_address"] = None

        # Initialize fee accrual tracking
        position.accumulated_fees_usd = Decimal("0")
        position.fees_token0 = Decimal("0")
        position.fees_token1 = Decimal("0")

        # Calculate execution costs (simplified)
        gas_cost_usd = Decimal("20")  # Typical LP open gas cost ~350k gas at ~$0.05 gas

        logger.info(
            "LP_OPEN executed: pool=%s, amount_usd=%.2f, range=[%.6f, %.6f], ticks=[%d, %d], liquidity=%.2f",
            intent.pool,
            float(amount_usd),
            float(range_lower),
            float(range_upper),
            tick_lower,
            tick_upper,
            float(liquidity),
        )

        # Create and return the SimulatedFill
        return SimulatedFill(
            timestamp=market_state.timestamp,
            intent_type=IntentType.LP_OPEN,
            protocol=protocol,
            tokens=[token0, token1],
            executed_price=entry_price,
            amount_usd=amount_usd,
            fee_usd=Decimal("0"),  # No protocol fee for LP open
            slippage_usd=Decimal("0"),  # No slippage for LP open
            gas_cost_usd=gas_cost_usd,
            tokens_in={},  # No tokens received on open
            tokens_out={token0: amount0, token1: amount1},  # Tokens deposited
            success=True,
            position_delta=position,
            metadata={
                "pool": intent.pool,
                "range_lower": str(range_lower),
                "range_upper": str(range_upper),
                "tick_lower": tick_lower,
                "tick_upper": tick_upper,
                "fee_tier": str(fee_tier),
                "liquidity": str(liquidity),
            },
        )

    def _execute_lp_close(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill":
        """Execute an LP_CLOSE intent to close a concentrated liquidity position.

        Calculates final token amounts including accumulated fees and IL,
        then returns a SimulatedFill with the close details.

        Args:
            intent: LPCloseIntent to execute
            portfolio: Current portfolio state (to find the position)
            market_state: Current market prices and data

        Returns:
            SimulatedFill with the closed position details including:
            - Final token amounts received (including fees if collect_fees=True)
            - IL loss calculated from entry to current price
            - PnL breakdown in metadata
        """
        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl.portfolio import SimulatedFill
        from almanak.framework.intents.vocabulary import LPCloseIntent

        # Type narrowing
        if not isinstance(intent, LPCloseIntent):
            # Return a failed fill if called with wrong intent type
            return SimulatedFill(
                timestamp=market_state.timestamp,
                intent_type=IntentType.LP_CLOSE,
                protocol="unknown",
                tokens=[],
                executed_price=Decimal("0"),
                amount_usd=Decimal("0"),
                fee_usd=Decimal("0"),
                slippage_usd=Decimal("0"),
                gas_cost_usd=Decimal("0"),
                tokens_in={},
                tokens_out={},
                success=False,
                metadata={"failure_reason": "Invalid intent type"},
            )

        # Find the position to close in the portfolio
        position = None
        for pos in portfolio.positions:
            if pos.position_id == intent.position_id:
                position = pos
                break

        if position is None:
            logger.warning(
                "LP_CLOSE failed: position %s not found in portfolio",
                intent.position_id,
            )
            return SimulatedFill(
                timestamp=market_state.timestamp,
                intent_type=IntentType.LP_CLOSE,
                protocol=intent.protocol,
                tokens=[],
                executed_price=Decimal("0"),
                amount_usd=Decimal("0"),
                fee_usd=Decimal("0"),
                slippage_usd=Decimal("0"),
                gas_cost_usd=Decimal("0"),
                tokens_in={},
                tokens_out={},
                success=False,
                position_close_id=intent.position_id,
                metadata={"failure_reason": f"Position {intent.position_id} not found"},
            )

        if len(position.tokens) < 2:
            logger.warning(
                "LP_CLOSE failed: position %s has insufficient tokens",
                intent.position_id,
            )
            return SimulatedFill(
                timestamp=market_state.timestamp,
                intent_type=IntentType.LP_CLOSE,
                protocol=intent.protocol,
                tokens=position.tokens,
                executed_price=Decimal("0"),
                amount_usd=Decimal("0"),
                fee_usd=Decimal("0"),
                slippage_usd=Decimal("0"),
                gas_cost_usd=Decimal("0"),
                tokens_in={},
                tokens_out={},
                success=False,
                position_close_id=intent.position_id,
                metadata={"failure_reason": "Position has fewer than 2 tokens"},
            )

        token0 = position.tokens[0]
        token1 = position.tokens[1]

        # Get current prices
        try:
            token0_price = market_state.get_price(token0)
        except KeyError:
            token0_price = position.entry_price

        try:
            token1_price = market_state.get_price(token1)
        except KeyError:
            token1_price = None

        if token0_price is None or token0_price <= 0:
            token0_price = position.entry_price
        if token1_price is None or token1_price <= 0:
            token1_price = self._price_fallback(token1, Decimal("1"), "execute_lp_close")

        # Calculate current price ratio (token0 in terms of token1)
        current_price_ratio = token0_price / token1_price if token1_price > 0 else position.entry_price

        # Get tick bounds (default to full range if not set)
        tick_lower = position.tick_lower if position.tick_lower is not None else -887272
        tick_upper = position.tick_upper if position.tick_upper is not None else 887272

        # Use ImpermanentLossCalculator to get current token amounts and IL
        il_pct, current_token0, current_token1 = self._il_calculator.calculate_il_v3(
            entry_price=position.entry_price,
            current_price=current_price_ratio,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=position.liquidity,
        )

        # Calculate position value from current tokens
        current_value = current_token0 * token0_price + current_token1 * token1_price

        # Get accumulated fees
        fees_earned_usd = position.accumulated_fees_usd
        if fees_earned_usd == Decimal("0"):
            # Fallback to fees_earned if accumulated_fees_usd not set
            fees_earned_usd = position.fees_earned

        # Calculate tokens received including fees if collect_fees=True
        tokens_in = {token0: current_token0, token1: current_token1}

        if intent.collect_fees:
            # Add fee tokens to the received amounts
            # Fee distribution follows position composition
            total_value = current_token0 * token0_price + current_token1 * token1_price
            if total_value > 0:
                token0_ratio = (current_token0 * token0_price) / total_value
            else:
                token0_ratio = Decimal("0.5")

            # Convert fee USD to token amounts
            fee_token0_usd = fees_earned_usd * token0_ratio
            fee_token1_usd = fees_earned_usd * (Decimal("1") - token0_ratio)

            if token0_price > 0:
                tokens_in[token0] += fee_token0_usd / token0_price
            if token1_price > 0:
                tokens_in[token1] += fee_token1_usd / token1_price

            # Total value includes fees when collected
            total_value_received = current_value + fees_earned_usd
        else:
            total_value_received = current_value

        # Calculate IL loss in USD
        # IL is calculated as percentage of hold value (what tokens would be worth if just held)
        # Get entry amounts from metadata or calculate from IL
        entry_amounts = position.metadata.get("entry_amounts", {})
        if entry_amounts:
            entry_token0 = Decimal(str(entry_amounts.get(token0, "0")))
            entry_token1 = Decimal(str(entry_amounts.get(token1, "0")))
        else:
            # Use IL calculator to get entry amounts based on position
            _, entry_token0, entry_token1 = self._il_calculator.calculate_il_v3(
                entry_price=position.entry_price,
                current_price=position.entry_price,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                liquidity=position.liquidity,
            )

        # Hold value = entry tokens at current prices
        hold_value = entry_token0 * token0_price + entry_token1 * token1_price
        il_loss_usd = il_pct * hold_value

        # Calculate initial value (entry amounts at entry prices)
        # NOTE: This is an approximation using current token1 USD price as a proxy for entry price.
        # For more accurate PnL, entry_token0_price_usd and entry_token1_price_usd should be stored
        # in position metadata during LP_OPEN. The current approach is acceptable for backtesting
        # where token price changes over the position lifetime are typically small relative to IL.
        initial_value = entry_token0 * position.entry_price * token1_price + entry_token1 * token1_price

        # Net LP PnL = (Current Value + Fees) - Initial Value
        net_lp_pnl_usd = (current_value + fees_earned_usd) - initial_value

        # Calculate execution costs (simplified)
        gas_cost_usd = Decimal("15")  # Typical LP close gas cost ~250k gas at ~$0.05 gas

        logger.info(
            "LP_CLOSE executed: position=%s, token0_out=%.6f, token1_out=%.6f, "
            "value_usd=%.2f, fees_usd=%.2f, il_pct=%.4f%%, net_pnl=%.2f",
            intent.position_id,
            float(tokens_in.get(token0, 0)),
            float(tokens_in.get(token1, 0)),
            float(total_value_received),
            float(fees_earned_usd),
            float(il_pct * 100),
            float(net_lp_pnl_usd),
        )

        # Create and return the SimulatedFill
        return SimulatedFill(
            timestamp=market_state.timestamp,
            intent_type=IntentType.LP_CLOSE,
            protocol=intent.protocol,
            tokens=[token0, token1],
            executed_price=current_price_ratio,
            amount_usd=total_value_received,
            fee_usd=Decimal("0"),  # Protocol fee for LP close (typically none)
            slippage_usd=Decimal("0"),  # No slippage for LP close
            gas_cost_usd=gas_cost_usd,
            tokens_in=tokens_in,  # Tokens received from closing
            tokens_out={},  # No tokens sent when closing
            success=True,
            position_close_id=intent.position_id,
            metadata={
                "position_id": intent.position_id,
                "pool": intent.pool or f"{token0}/{token1}",
                "collect_fees": intent.collect_fees,
                "current_price_ratio": str(current_price_ratio),
                "il_percentage": str(il_pct),
                "il_loss_usd": str(il_loss_usd),
                "fees_earned_usd": str(fees_earned_usd),
                "net_lp_pnl_usd": str(net_lp_pnl_usd),
                "initial_value_usd": str(initial_value),
                "current_value_usd": str(current_value),
                "token0_price_usd": str(token0_price),
                "token1_price_usd": str(token1_price),
            },
        )

    def _price_to_tick_int(self, price: Decimal) -> int:
        """Convert a price ratio to a Uniswap V3 tick.

        Uses the formula: tick = floor(log(price) / log(1.0001))

        Args:
            price: Price ratio (token0 in terms of token1)

        Returns:
            Tick value (clamped to V3 min/max tick bounds)
        """
        import math

        MIN_TICK = -887272
        MAX_TICK = 887272

        if price <= 0:
            return MIN_TICK

        try:
            tick = math.floor(math.log(float(price), 1.0001))
            return max(MIN_TICK, min(MAX_TICK, tick))
        except (ValueError, OverflowError):
            return MIN_TICK if float(price) < 1 else MAX_TICK

    def update_position(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        elapsed_seconds: float,
        timestamp: datetime | None = None,
    ) -> None:
        """Update LP position state based on time passage and price changes.

        This method handles LP-specific position updates:
        - Uses ImpermanentLossCalculator to compute current token amounts
        - Updates position amounts based on V3 concentrated liquidity math
        - Accrues trading fees based on volume and liquidity share
        - Tracks fee attribution between token0 and token1

        The fee accrual model uses a hybrid approach:
        1. Volume-based fees: volume * fee_tier * liquidity_share
        2. APR-based fees: position_value * daily_apr
        3. Final fee = average of both approaches

        Args:
            position: The LP position to update (modified in-place)
            market_state: Current market prices and data
            elapsed_seconds: Time elapsed since last update in seconds
            timestamp: Simulation timestamp for deterministic updates. If None,
                uses market_state.timestamp for reproducible backtests.

        Note:
            This method only updates LP positions (position.is_lp == True).
            Non-LP positions are ignored.
        """
        # Only process LP positions
        if not position.is_lp:
            return

        if len(position.tokens) < 2:
            return

        token0 = position.tokens[0]
        token1 = position.tokens[1]

        # Get current prices
        try:
            token0_price = market_state.get_price(token0)
        except KeyError:
            token0_price = position.entry_price

        try:
            token1_price = market_state.get_price(token1)
        except KeyError:
            token1_price = None

        if token0_price is None or token0_price <= 0:
            token0_price = position.entry_price
        if token1_price is None or token1_price <= 0:
            token1_price = self._price_fallback(token1, Decimal("1"), "update_position")

        # Calculate current price ratio (token0 in terms of token1)
        current_price = token0_price / token1_price if token1_price > 0 else position.entry_price

        # Get tick bounds (default to full range if not set)
        tick_lower = position.tick_lower if position.tick_lower is not None else -887272
        tick_upper = position.tick_upper if position.tick_upper is not None else 887272

        # Use ImpermanentLossCalculator to compute IL and current token amounts
        il_pct, current_token0, current_token1 = self._il_calculator.calculate_il_v3(
            entry_price=position.entry_price,
            current_price=current_price,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=position.liquidity,
        )

        # Update position's token amounts based on IL calculation
        position.amounts[token0] = current_token0
        position.amounts[token1] = current_token1

        # Store IL percentage in metadata for informational purposes
        position.metadata["il_percentage"] = float(il_pct)
        position.metadata["current_price_ratio"] = float(current_price)

        # Calculate current position value (before fees)
        position_value = current_token0 * token0_price + current_token1 * token1_price

        # Determine the simulation timestamp for fee accrual and position update
        if timestamp is not None:
            update_time = timestamp
        elif hasattr(market_state, "timestamp") and market_state.timestamp is not None:
            update_time = market_state.timestamp
        else:
            if self._config.strict_reproducibility:
                msg = (
                    f"No simulation timestamp available for LP position {position.position_id}. "
                    "In strict reproducibility mode, timestamp must be provided. "
                    "Either pass timestamp parameter or ensure market_state.timestamp is set."
                )
                raise ValueError(msg)
            logger.warning(
                "No simulation timestamp available for LP position %s, "
                "falling back to datetime.now(). This breaks backtest reproducibility.",
                position.position_id,
            )
            update_time = datetime.now()

        # Accrue fees if enabled
        if self._config.fee_tracking_enabled and elapsed_seconds > 0:
            # Get pool address from position metadata for historical volume lookup
            pool_address = position.metadata.get("pool_address")

            fee_result = self._calculate_fee_accrual(
                position=position,
                position_value_usd=position_value,
                elapsed_seconds=elapsed_seconds,
                token0=token0,
                token1=token1,
                token0_price=token0_price,
                token1_price=token1_price,
                timestamp=update_time,
                pool_address=pool_address,
            )
            position.fees_earned += fee_result.fees_usd

            # Update fee confidence on position
            # If already set, only downgrade confidence (never upgrade during a backtest)
            if position.fee_confidence is None:
                position.fee_confidence = fee_result.fee_confidence
            elif fee_result.fee_confidence == "low" and position.fee_confidence != "low":
                # Downgrade to low if any fee calculation used low confidence
                position.fee_confidence = "low"
            elif fee_result.fee_confidence == "medium" and position.fee_confidence == "high":
                # Downgrade from high to medium
                position.fee_confidence = "medium"

            # Update slippage confidence on position (same downgrade-only logic)
            if fee_result.slippage_confidence is not None:
                if position.slippage_confidence is None:
                    position.slippage_confidence = fee_result.slippage_confidence
                elif fee_result.slippage_confidence == "low" and position.slippage_confidence != "low":
                    position.slippage_confidence = "low"
                elif fee_result.slippage_confidence == "medium" and position.slippage_confidence == "high":
                    position.slippage_confidence = "medium"

        # Update position timestamp
        position.last_updated = update_time

    def _calculate_fee_accrual(
        self,
        position: "SimulatedPosition",
        position_value_usd: Decimal,
        elapsed_seconds: float,
        token0: str,
        token1: str,
        token0_price: Decimal,
        token1_price: Decimal,
        timestamp: datetime | None = None,
        pool_address: str | None = None,
        protocol: str | None = None,
    ) -> FeeAccrualResult:
        """Calculate fee accrual for an LP position.

        Uses historical volume data from MultiDEXVolumeProvider when available,
        falling back to a hybrid model combining volume-based and APR-based fee estimates.

        The confidence level is determined by the data source:
        - HIGH: Historical volume from subgraph
        - MEDIUM: Interpolated or estimated data
        - LOW: Fallback using volume_fallback_multiplier

        Args:
            position: The LP position (updated in place with fee tracking)
            position_value_usd: Current position value in USD
            elapsed_seconds: Time elapsed since last update in seconds
            token0: Token0 symbol
            token1: Token1 symbol
            token0_price: Current price of token0 in USD
            token1_price: Current price of token1 in USD
            timestamp: Simulation timestamp for historical data lookup
            pool_address: Pool contract address for subgraph queries
            protocol: Protocol identifier for routing to correct subgraph

        Returns:
            FeeAccrualResult with fees earned, confidence level, and data source
        """
        if position_value_usd <= 0 or elapsed_seconds <= 0:
            return FeeAccrualResult(
                fees_usd=Decimal("0"),
                fee_confidence="low",
                data_source="none:no_value_or_time",
                fees_token0=Decimal("0"),
                fees_token1=Decimal("0"),
                volume_usd=None,
                pool_address=pool_address,
                timestamp=timestamp,
            )

        # Convert seconds to days
        days_elapsed = Decimal(str(elapsed_seconds)) / Decimal("86400")

        # Calculate liquidity share factor
        # liquidity_share = min(1, liquidity / base_liquidity)
        base_liquidity = self._config.base_liquidity
        if position.liquidity > 0 and base_liquidity > 0:
            liquidity_share = min(Decimal("1"), position.liquidity / base_liquidity)
        else:
            liquidity_share = Decimal("0.5")
        # Ensure minimum share of 10% for small positions
        liquidity_share = max(Decimal("0.1"), liquidity_share)

        # Determine base APR based on fee tier (used as fallback)
        fee_tier_pct = position.fee_tier * Decimal("100")  # Convert to percentage

        if fee_tier_pct <= Decimal("0.01"):
            # Stablecoin pools: high volume, low APR
            base_apr = Decimal("0.10")  # 10% APR
        elif fee_tier_pct <= Decimal("0.05"):
            # Blue chip pairs: medium-high volume
            base_apr = Decimal("0.20")  # 20% APR
        elif fee_tier_pct <= Decimal("0.30"):
            # Volatile pairs: medium volume
            base_apr = Decimal("0.25")  # 25% APR
        else:
            # Exotic pairs: low volume
            base_apr = Decimal("0.10")  # 10% APR

        # Try to get actual historical volume from MultiDEXVolumeProvider
        actual_volume: Decimal | None = None
        volume_confidence = DataConfidence.LOW
        volume_source = "estimated"

        if timestamp is not None and pool_address and self._use_historical_volume():
            actual_volume, volume_confidence = self._get_historical_volume(
                pool_address=pool_address,
                timestamp=timestamp,
                protocol=protocol,
            )

        # Get the volume fallback multiplier from config
        volume_fallback_multiplier = self._get_volume_fallback_multiplier()

        if actual_volume is not None and actual_volume > 0 and volume_confidence != DataConfidence.LOW:
            # Use actual historical volume from subgraph
            # Fee calculation: volume * fee_tier * liquidity_share (prorated for elapsed time)
            volume_based_fees = actual_volume * position.fee_tier * liquidity_share * days_elapsed
            volume_source = "historical"
            logger.info(
                "LP fee accrual using historical volume: pool=%s..., date=%s, "
                "volume_usd=$%.2f, fees_usd=%.4f, confidence=%s (source: %s)",
                pool_address[:10] if pool_address else "unknown",
                timestamp.date() if timestamp else "unknown",
                float(actual_volume),
                float(volume_based_fees),
                volume_confidence.value,
                volume_source,
            )
        else:
            # Fallback to estimated volume using multiplier
            estimated_daily_volume = position_value_usd * volume_fallback_multiplier
            volume_based_fees = estimated_daily_volume * position.fee_tier * liquidity_share * days_elapsed
            volume_source = "fallback"
            volume_confidence = DataConfidence.LOW
            logger.warning(
                "LP fee accrual using fallback volume multiplier: position=%s, "
                "multiplier=%.1fx, estimated_volume=$%.2f, fees_usd=%.4f (historical volume unavailable)",
                position.position_id,
                float(volume_fallback_multiplier),
                float(estimated_daily_volume),
                float(volume_based_fees),
            )

        # APR-based fee calculation (fallback/comparison)
        daily_fee_rate = base_apr / Decimal("365")
        apr_based_fees = position_value_usd * daily_fee_rate * days_elapsed

        # Determine fee confidence and data source based on how fees were calculated
        if volume_source == "historical":
            # Historical volume from subgraph - use the confidence from the provider
            fees_usd = volume_based_fees
            # Map DataConfidence enum to string for FeeAccrualResult
            fee_confidence: Literal["high", "medium", "low"] = volume_confidence.value
            data_source = f"multi_dex:{self._config.chain}"
            volume_for_result = actual_volume
        else:
            # Use average of both approaches for balanced estimate
            fees_usd = (volume_based_fees + apr_based_fees) / Decimal("2")
            # Multiplier heuristic - lowest confidence
            fee_confidence = "low"
            data_source = f"fallback_multiplier:{volume_fallback_multiplier}x"
            volume_for_result = position_value_usd * volume_fallback_multiplier

        # Update detailed fee tracking fields on position
        position.accumulated_fees_usd += fees_usd

        # Calculate fee attribution between tokens based on position composition
        total_value = (
            position.amounts.get(token0, Decimal("0")) * token0_price
            + position.amounts.get(token1, Decimal("0")) * token1_price
        )

        if total_value > 0:
            token0_value = position.amounts.get(token0, Decimal("0")) * token0_price
            token0_ratio = token0_value / total_value
            token1_ratio = Decimal("1") - token0_ratio
        else:
            token0_ratio = Decimal("0.5")
            token1_ratio = Decimal("0.5")

        # Attribute fees proportionally to each token
        fees_token0_usd = fees_usd * token0_ratio
        fees_token1_usd = fees_usd * token1_ratio

        # Convert USD fees to token amounts
        fees_token0_amount = Decimal("0")
        fees_token1_amount = Decimal("0")
        if token0_price > 0:
            fees_token0_amount = fees_token0_usd / token0_price
            position.fees_token0 += fees_token0_amount
        if token1_price > 0:
            fees_token1_amount = fees_token1_usd / token1_price
            position.fees_token1 += fees_token1_amount

        # Calculate slippage using historical liquidity depth if available
        slippage_confidence: Literal["high", "medium", "low"] | None = None
        slippage_pct: Decimal | None = None
        liquidity_usd: Decimal | None = None

        if timestamp is not None and pool_address and self._use_historical_liquidity():
            # Use a representative trade size for slippage calculation
            # This could be the average trade size based on volume, or a fraction of position value
            representative_trade_usd = fees_usd * Decimal("10") if fees_usd > 0 else position_value_usd * Decimal("0.1")

            if representative_trade_usd > 0:
                slippage_result = self._calculate_slippage(
                    trade_amount_usd=representative_trade_usd,
                    pool_address=pool_address,
                    timestamp=timestamp,
                    protocol=protocol,
                    pool_type="v3",  # LP positions are typically V3-style
                )

                # Map DataConfidence enum to string
                slippage_confidence = slippage_result.confidence.value
                slippage_pct = slippage_result.slippage
                liquidity_usd = slippage_result.liquidity_usd

        logger.debug(
            "LP fee accrual: position=%s, fees_usd=%.4f, token0_fees=%.6f, token1_fees=%.6f, "
            "fee_confidence=%s, slippage_confidence=%s, slippage_pct=%s",
            position.position_id,
            float(fees_usd),
            float(position.fees_token0),
            float(position.fees_token1),
            fee_confidence,
            slippage_confidence,
            f"{float(slippage_pct):.4f}" if slippage_pct else "N/A",
        )

        return FeeAccrualResult(
            fees_usd=fees_usd,
            fee_confidence=fee_confidence,
            data_source=data_source,
            fees_token0=fees_token0_amount,
            fees_token1=fees_token1_amount,
            volume_usd=volume_for_result,
            pool_address=pool_address,
            timestamp=timestamp,
            slippage_confidence=slippage_confidence,
            slippage_pct=slippage_pct,
            liquidity_usd=liquidity_usd,
        )

    def value_position(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        timestamp: datetime | None = None,
    ) -> Decimal:
        """Calculate the current USD value of an LP position.

        This method computes LP position value using the Uniswap V3 math:

        1. Get current prices for both tokens
        2. Calculate current token amounts using ImpermanentLossCalculator
        3. Value = token0_amount * token0_price + token1_amount * token1_price
        4. Add accumulated fees (if fee_tracking_enabled)

        The IL is inherently reflected in the token amounts returned by
        calculate_il_v3() - positions that have experienced IL will have
        fewer tokens in value terms than if they had been held.

        Args:
            position: The LP position to value
            market_state: Current market prices and data
            timestamp: Simulation timestamp for deterministic valuation. If None,
                uses market_state.timestamp. Currently unused in LP valuation
                but accepted for interface consistency.

        Returns:
            Total position value in USD as a Decimal (tokens + fees)
        """
        # Note: timestamp parameter accepted for interface consistency
        # LP valuation is based on current market prices, not time-dependent
        _ = timestamp
        if len(position.tokens) < 2:
            # Fall back to simple valuation for malformed positions
            total_value = Decimal("0")
            for token, amount in position.amounts.items():
                price = market_state.get_price(token)
                if price:
                    total_value += amount * price
            return total_value

        token0 = position.tokens[0]
        token1 = position.tokens[1]

        # Get current prices
        try:
            token0_price = market_state.get_price(token0)
        except KeyError:
            token0_price = position.entry_price

        try:
            token1_price = market_state.get_price(token1)
        except KeyError:
            token1_price = None

        if token0_price is None or token0_price <= 0:
            token0_price = position.entry_price
        if token1_price is None or token1_price <= 0:
            token1_price = self._price_fallback(token1, Decimal("1"), "value_position")

        # Calculate current price ratio (token0 in terms of token1)
        current_price = token0_price / token1_price if token1_price > 0 else position.entry_price

        # Get tick bounds
        tick_lower = position.tick_lower if position.tick_lower is not None else -887272
        tick_upper = position.tick_upper if position.tick_upper is not None else 887272

        # Use ImpermanentLossCalculator to get current token amounts
        # This properly accounts for concentrated liquidity and IL
        il_pct, current_token0, current_token1 = self._il_calculator.calculate_il_v3(
            entry_price=position.entry_price,
            current_price=current_price,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=position.liquidity,
        )

        # Calculate position value from current token amounts
        token_value = current_token0 * token0_price + current_token1 * token1_price

        # Add accumulated fees if tracking is enabled
        if self._config.fee_tracking_enabled:
            total_value = token_value + position.accumulated_fees_usd
        else:
            total_value = token_value

        logger.debug(
            "LP position value: position=%s, token0=%.6f, token1=%.6f, "
            "token_value=%.2f, fees=%.2f, total=%.2f, il_pct=%.4f%%",
            position.position_id,
            float(current_token0),
            float(current_token1),
            float(token_value),
            float(position.accumulated_fees_usd),
            float(total_value),
            float(il_pct * 100),
        )

        return total_value

    def should_rebalance(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
    ) -> bool:
        """Determine if an LP position should be rebalanced.

        For LP positions, rebalancing is typically triggered when the
        current price moves outside the position's tick range. This means
        the position is no longer actively providing liquidity.

        The method respects two config settings:
        - rebalance_on_out_of_range: Triggers on fully out of range
        - rebalance_on_partial_exit: Triggers when approaching boundary

        Args:
            position: The LP position to check
            market_state: Current market prices and data

        Returns:
            True if the position should be rebalanced, False otherwise

        Note:
            This uses the price ratio (token0/token1) for range checks,
            not the USD price of individual tokens.
        """
        # Check if any rebalance trigger is enabled
        if not self._config.rebalance_on_out_of_range and not self._config.rebalance_on_partial_exit:
            return False

        # Get range status
        range_result = self.get_range_status(position, market_state)
        if range_result is None:
            return False

        # Check for full out-of-range condition
        if self._config.rebalance_on_out_of_range and range_result.is_out_of_range:
            logger.info(
                "LP position %s is out of range (status=%s, price=%.6f, range=[%.6f, %.6f])",
                position.position_id,
                range_result.status.value,
                float(range_result.current_price_ratio),
                float(range_result.price_lower),
                float(range_result.price_upper),
            )
            return True

        # Check for partial exit condition (approaching boundary)
        if self._config.rebalance_on_partial_exit and range_result.is_approaching_boundary:
            logger.info(
                "LP position %s is approaching boundary (status=%s, price=%.6f, range=[%.6f, %.6f])",
                position.position_id,
                range_result.status.value,
                float(range_result.current_price_ratio),
                float(range_result.price_lower),
                float(range_result.price_upper),
            )
            return True

        return False

    def get_range_status(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
    ) -> RangeStatusResult | None:
        """Get the current range status for an LP position.

        This method calculates whether the current price is:
        - Within the position's tick range (IN_RANGE)
        - Below the range (BELOW_RANGE)
        - Above the range (ABOVE_RANGE)
        - Approaching the lower boundary (PARTIAL_BELOW)
        - Approaching the upper boundary (PARTIAL_ABOVE)

        The check uses the price ratio (token0/token1), not USD prices,
        since Uniswap V3 ticks represent price ratios.

        Args:
            position: The LP position to check
            market_state: Current market prices and data

        Returns:
            RangeStatusResult with detailed status information,
            or None if the position cannot be evaluated (not LP, missing data).
        """
        # Only process LP positions
        if not position.is_lp:
            return None

        if position.tick_lower is None or position.tick_upper is None:
            return None

        if len(position.tokens) < 2:
            return None

        token0 = position.tokens[0]
        token1 = position.tokens[1]

        # Get current prices for both tokens
        try:
            token0_price = market_state.get_price(token0)
        except KeyError:
            token0_price = None

        try:
            token1_price = market_state.get_price(token1)
        except KeyError:
            token1_price = None

        # Need both prices to calculate ratio
        if token0_price is None or token0_price <= 0:
            return None
        if token1_price is None or token1_price <= 0:
            token1_price = self._price_fallback(token1, Decimal("1"), "get_range_status")

        # Calculate current price ratio (token0 in terms of token1)
        # This is what Uniswap V3 ticks represent
        current_price_ratio = token0_price / token1_price

        # Calculate tick range prices (these are also ratios)
        price_lower = self._tick_to_price(position.tick_lower)
        price_upper = self._tick_to_price(position.tick_upper)

        # Calculate distances as percentages
        # distance_to_lower: positive if above lower, negative if below
        if price_lower > 0:
            distance_to_lower_pct = ((current_price_ratio - price_lower) / price_lower) * Decimal("100")
        else:
            distance_to_lower_pct = Decimal("0")

        # distance_to_upper: positive if below upper, negative if above
        if price_upper > 0:
            distance_to_upper_pct = ((price_upper - current_price_ratio) / price_upper) * Decimal("100")
        else:
            distance_to_upper_pct = Decimal("0")

        # Determine range status
        margin = self._config.boundary_margin_pct

        if current_price_ratio < price_lower:
            # Price is below the range - position is 100% token0
            status = RangeStatus.BELOW_RANGE
        elif current_price_ratio > price_upper:
            # Price is above the range - position is 100% token1
            status = RangeStatus.ABOVE_RANGE
        elif distance_to_lower_pct >= 0 and distance_to_lower_pct < margin:
            # Price is within range but approaching lower boundary
            status = RangeStatus.PARTIAL_BELOW
        elif distance_to_upper_pct >= 0 and distance_to_upper_pct < margin:
            # Price is within range but approaching upper boundary
            status = RangeStatus.PARTIAL_ABOVE
        else:
            # Price is comfortably within range
            status = RangeStatus.IN_RANGE

        return RangeStatusResult(
            status=status,
            current_price_ratio=current_price_ratio,
            price_lower=price_lower,
            price_upper=price_upper,
            distance_to_lower_pct=distance_to_lower_pct,
            distance_to_upper_pct=distance_to_upper_pct,
        )

    def _tick_to_price(self, tick: int) -> Decimal:
        """Convert a tick value to price ratio.

        Uses the Uniswap V3 formula: price = 1.0001^tick

        This returns the price of token0 in terms of token1.
        For example, if tick=0, price=1 (1 token0 = 1 token1).
        If tick>0, price>1 (token0 is more valuable than token1).
        If tick<0, price<1 (token0 is less valuable than token1).

        Args:
            tick: Tick value

        Returns:
            Price ratio corresponding to the tick
        """
        # Use Python's built-in decimal power for precision
        return TICK_BASE**tick

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
    "LPBacktestAdapter",
    "LPBacktestConfig",
    "RangeStatus",
    "RangeStatusResult",
]
