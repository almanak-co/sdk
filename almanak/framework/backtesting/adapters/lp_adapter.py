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

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Final, Literal, NoReturn, cast

from almanak.core.chains import DEFAULT_CHAIN, LEGACY_SERIALIZED_CHAIN, ChainRegistry
from almanak.core.constants import STABLECOINS
from almanak.framework.backtesting.adapters._sync_bridge import (
    in_running_event_loop_task,
    run_coroutine_blocking,
)
from almanak.framework.backtesting.adapters.base import (
    StrategyBacktestAdapter,
    StrategyBacktestConfig,
    register_adapter,
)
from almanak.framework.backtesting.exceptions import (
    DataSourceUnavailableError,
    HistoricalDataUnavailableError,
)
from almanak.framework.backtesting.models import FeeAccrualResult
from almanak.framework.backtesting.pnl.calculators.impermanent_loss import (
    ImpermanentLossCalculator,
)
from almanak.framework.backtesting.pnl.data_provider import TokenRef, token_ref_display
from almanak.framework.backtesting.pnl.types import DataConfidence

FeeConfidence = Literal["high", "medium", "low"]

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
    from almanak.framework.backtesting.pnl.types import LiquidityResult, VolumeResult
    from almanak.framework.intents.vocabulary import Intent, LPOpenIntent

logger = logging.getLogger(__name__)

# Uniswap V3 tick constants
TICK_BASE = Decimal("1.0001")

_RAISE_PLAIN: Final = object()
"""Sentinel for ``_volume_data_unavailable``: raise without a ``from`` clause."""


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
        volume_multiplier: Multiplier for the OPT-IN volume heuristic
            (``position_value_usd * volume_multiplier``). Only applied when
            ``allow_volume_fallback=True``. Higher values simulate more active
            pools. Default 10.
        base_liquidity: Placeholder pool TVL (USD) for the liquidity-share
            denominator when ``explicit_pool_liquidity_usd`` is not set.
            Position's share = min(1, position_value_usd / base_liquidity).
            Default 1_000_000.
        explicit_pool_volume_usd_daily: Caller-provided daily pool volume (USD).
            When set, used directly with HIGH confidence (no lookup, no fabrication).
        explicit_pool_liquidity_usd: Caller-provided pool TVL (USD) used as the
            liquidity-share denominator instead of ``base_liquidity``.
        allow_volume_fallback: Opt-in to the ``volume_multiplier`` heuristic when no
            real volume is available. Defaults to False -> the adapter raises
            ``DataSourceUnavailableError`` rather than silently fabricating a number
            (VIB-4849).

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
    """Whether to attempt fetching historical volume via the gateway DEX-volume
    lane (``GetDexVolumeHistory``). If True, will try to use actual pool volume
    for fee calculation. When the lookup fails (gateway unreachable / no data)
    the adapter does NOT silently fabricate a number: it either uses explicit
    inputs, the opt-in heuristic (``allow_volume_fallback``), or raises
    ``DataSourceUnavailableError``."""

    explicit_pool_volume_usd_daily: Decimal | None = None
    """Caller-provided daily pool volume in USD. When set, fee accrual uses this
    value directly instead of fetching historical volume or fabricating one.
    Use this when you know the pool's volume and do not want a historical-data
    lookup."""

    explicit_pool_liquidity_usd: Decimal | None = None
    """Caller-provided pool TVL/liquidity in USD for the liquidity-share
    calculation. When set, overrides ``base_liquidity`` as the denominator so the
    position's share is grounded in a real number rather than the 1,000,000
    placeholder. Pair with ``explicit_pool_volume_usd_daily`` for a fully
    user-specified (no-lookup, non-fabricated) fee estimate."""

    allow_volume_fallback: bool = False
    """Explicit opt-in to the ``volume_multiplier`` heuristic when no real volume
    data is available. Defaults to False: the adapter fails loud
    (``DataSourceUnavailableError``) rather than silently fabricating
    ``position_value_usd * volume_multiplier``. Set True only when you knowingly
    accept a rough, order-of-magnitude-uncertain fee estimate (e.g. quick
    parameter sweeps), and understand the result is LOW confidence."""

    chain: str = DEFAULT_CHAIN
    """Chain for historical data routing. Options: ethereum, arbitrum, base, optimism, polygon."""

    def __post_init__(self) -> None:
        """Validate LP-specific configuration.

        Raises:
            ValueError: If strategy_type is not "lp", il_calculation_method
                is invalid, or explicit_pool_liquidity_usd is non-positive.
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

        # A non-positive TVL is a nonsensical liquidity-share denominator; fail
        # at construction rather than silently degrading to the 0.5-share
        # fallback in fee accrual. Mirrors BacktestDataConfig's validation.
        if self.explicit_pool_liquidity_usd is not None and self.explicit_pool_liquidity_usd <= 0:
            msg = f"explicit_pool_liquidity_usd must be positive when provided, got {self.explicit_pool_liquidity_usd}"
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
                "explicit_pool_volume_usd_daily": (
                    str(self.explicit_pool_volume_usd_daily)
                    if self.explicit_pool_volume_usd_daily is not None
                    else None
                ),
                "explicit_pool_liquidity_usd": (
                    str(self.explicit_pool_liquidity_usd) if self.explicit_pool_liquidity_usd is not None else None
                ),
                "allow_volume_fallback": self.allow_volume_fallback,
                "chain": self.chain,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LPBacktestConfig":
        """Create configuration from a dictionary.

        Unknown keys are ignored, so configs serialized by older SDK versions
        (e.g. carrying the removed, never-consumed ``subgraph_api_key`` field)
        still deserialize cleanly.

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
            explicit_pool_volume_usd_daily=(
                Decimal(str(data["explicit_pool_volume_usd_daily"]))
                if data.get("explicit_pool_volume_usd_daily") is not None
                else None
            ),
            explicit_pool_liquidity_usd=(
                Decimal(str(data["explicit_pool_liquidity_usd"]))
                if data.get("explicit_pool_liquidity_usd") is not None
                else None
            ),
            allow_volume_fallback=data.get("allow_volume_fallback", False),
            chain=data.get("chain", LEGACY_SERIALIZED_CHAIN),
        )


@dataclass
class _VolumeResolution:
    """Outcome of resolving the daily pool volume used for LP fee accrual.

    Attributes:
        volume_usd: The daily pool volume in USD selected for the fee calc.
        source: One of "explicit", "historical", or "fallback".
        confidence: Data confidence for the selected source.
    """

    volume_usd: Decimal
    source: Literal["explicit", "historical", "fallback"]
    confidence: DataConfidence


@dataclass(frozen=True)
class _LPUpdatePrices:
    token0: TokenRef
    token1: TokenRef
    token0_price: Decimal
    token1_price: Decimal
    current_price: Decimal


@dataclass(frozen=True)
class _RangeStatusInputs:
    prices: _LPUpdatePrices
    tick_lower: int
    tick_upper: int


@dataclass(frozen=True)
class _RangeDistances:
    lower_pct: Decimal
    upper_pct: Decimal


@dataclass(frozen=True)
class _LPUpdateAmounts:
    il_pct: Decimal
    token0_amount: Decimal
    token1_amount: Decimal
    position_value_usd: Decimal


@dataclass(frozen=True)
class _LPUpdatePlan:
    prices: _LPUpdatePrices
    amounts: _LPUpdateAmounts
    update_time: datetime
    fee_result: FeeAccrualResult | None


@dataclass(frozen=True)
class _LPCloseResult:
    tokens_in: dict[TokenRef, Decimal]
    total_value_received: Decimal
    fees_earned_usd: Decimal
    il_pct: Decimal
    il_loss_usd: Decimal
    initial_value_usd: Decimal
    net_lp_pnl_usd: Decimal


@dataclass(frozen=True)
class _LPOpenPlan:
    token0: str
    token1: str
    amount0: Decimal
    amount1: Decimal
    token0_price: Decimal
    token1_price: Decimal
    amount_usd: Decimal
    entry_price: Decimal
    range_lower: Decimal
    range_upper: Decimal
    tick_lower: int
    tick_upper: int
    protocol: str
    fee_tier: Decimal
    liquidity: Decimal


@dataclass(frozen=True)
class _FeeFormulaContext:
    days_elapsed: Decimal
    liquidity_share: Decimal
    base_apr: Decimal
    resolution: _VolumeResolution


@dataclass(frozen=True)
class _FeeAmountResult:
    fees_usd: Decimal
    fee_confidence: FeeConfidence
    data_source: str
    volume_usd: Decimal


@dataclass(frozen=True)
class _FeeTokenAttribution:
    token0_amount: Decimal
    token1_amount: Decimal


@dataclass(frozen=True)
class _FeeSlippageResult:
    confidence: FeeConfidence | None = None
    pct: Decimal | None = None
    liquidity_usd: Decimal | None = None


@dataclass
class HeuristicValidationSample:
    """A single ground-truth observation for validating the fee heuristic.

    The caller supplies real on-chain (or trusted) data so ``validate_heuristics``
    can compare the adapter's heuristic output against it WITHOUT performing any
    network egress from the strategy container.

    Attributes:
        position_value_usd: Position value in USD over the sample window
            (also the numerator of the liquidity-share heuristic).
        liquidity: Position liquidity in V3 L-units. Informational only since
            VIB-5096 — the share heuristic uses ``position_value_usd``.
        fee_tier: Pool fee tier as a fraction (e.g. Decimal("0.0005") for 0.05%).
        elapsed_seconds: Duration of the sample window in seconds.
        observed_fees_usd: The real fees earned over the window (ground truth).
        label: Optional human-readable label for diagnostics (e.g. pool name + date).
    """

    position_value_usd: Decimal
    liquidity: Decimal
    fee_tier: Decimal
    elapsed_seconds: float
    observed_fees_usd: Decimal
    label: str = ""


@dataclass
class HeuristicValidationResult:
    """Per-sample result of comparing heuristic fees to observed fees.

    Attributes:
        label: Sample label (echoed from the input).
        estimated_fees_usd: Fees the heuristic produced for the sample.
        observed_fees_usd: Real fees supplied by the caller.
        error_pct: Relative error as a fraction (e.g. 0.5 == 50% off).
        exceeds_threshold: True if ``error_pct`` exceeds the warn threshold.
    """

    label: str
    estimated_fees_usd: Decimal
    observed_fees_usd: Decimal
    error_pct: Decimal
    exceeds_threshold: bool


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
    to fetch historical volume data via the gateway DEX-volume lane
    (``GetDexVolumeHistory``); routing to the right DEX is declared by each
    connector's manifest.

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
            True if historical volume should be fetched via the gateway
            DEX-volume lane.
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

    def _explicit_pool_volume_usd_daily(self) -> Decimal | None:
        """Get the caller-provided daily pool volume, if any.

        BacktestDataConfig.explicit_pool_volume_usd_daily takes precedence when
        set; otherwise falls back to LPBacktestConfig.explicit_pool_volume_usd_daily.
        Empty != Zero: ``None`` means "not provided" on either surface and falls
        through; ``Decimal("0")`` is a measured zero and is returned as-is.

        Returns:
            Daily pool volume in USD, or None when neither config provides one.
        """
        if self._data_config is not None and self._data_config.explicit_pool_volume_usd_daily is not None:
            return self._data_config.explicit_pool_volume_usd_daily
        return self._config.explicit_pool_volume_usd_daily

    def _explicit_pool_liquidity_usd(self) -> Decimal | None:
        """Get the caller-provided pool TVL for the liquidity-share denominator.

        BacktestDataConfig.explicit_pool_liquidity_usd takes precedence when set;
        otherwise falls back to LPBacktestConfig.explicit_pool_liquidity_usd.

        Returns:
            Pool TVL in USD, or None when neither config provides one.
        """
        if self._data_config is not None and self._data_config.explicit_pool_liquidity_usd is not None:
            return self._data_config.explicit_pool_liquidity_usd
        return self._config.explicit_pool_liquidity_usd

    def _allow_volume_fallback(self) -> bool:
        """Check whether the caller opted in to the volume_multiplier heuristic.

        The opt-in is OR-ed across both config surfaces: either
        BacktestDataConfig.allow_volume_fallback or
        LPBacktestConfig.allow_volume_fallback enables it. A data_config left at
        its default (False) never silently revokes an opt-in made on the adapter
        config -- the opt-in can only be granted, not implicitly withdrawn.

        Returns:
            True when the LOW-confidence heuristic fallback is permitted.
        """
        if self._data_config is not None and self._data_config.allow_volume_fallback:
            return True
        return self._config.allow_volume_fallback

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

    _STABLECOIN_SYMBOLS: frozenset[str] = STABLECOINS

    def _price_fallback(self, token: TokenRef, fallback: Decimal, context: str) -> Decimal:
        """Return *fallback* price for *token* while logging a warning.

        In ``strict_reproducibility`` mode an error is raised instead.
        When the fallback is $1, a stronger warning is emitted for non-stablecoin tokens.
        """
        token_label = token_ref_display(token)
        if self._config.strict_reproducibility:
            raise HistoricalDataUnavailableError(
                data_type="price",
                identifier=token_label,
                timestamp=datetime.now(),
                message=f"Price unavailable for {token_label} in {context} and strict_reproducibility=True",
            )
        is_stablecoin = token_label.upper() in self._STABLECOIN_SYMBOLS
        if fallback == Decimal("1") and not is_stablecoin:
            logger.warning(
                "Price unavailable for NON-STABLECOIN %s in %s, falling back to $1 assumption -- "
                "this may produce inaccurate results. Consider enabling strict_reproducibility.",
                token_label,
                context,
            )
        else:
            logger.warning(
                "Price unavailable for %s in %s, falling back to $%s assumption",
                token_label,
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

    def _liquidity_data_unavailable(
        self,
        *,
        identifier: str,
        timestamp: datetime,
        message: str,
        chain: str | None,
        protocol: str | None,
        cause: BaseException | None | object = _RAISE_PLAIN,
        on_fallback: Callable[[], None] | None = None,
    ) -> None:
        """Apply the fidelity contract for a failed historical liquidity lookup.

        Strict historical mode raises HistoricalDataUnavailableError -- chained
        from ``cause`` unless it is the ``_RAISE_PLAIN`` sentinel -- without
        logging. Non-strict mode runs ``on_fallback`` (the per-site log call)
        and degrades to None.

        Unlike the volume lane, failed liquidity lookups are never cached:
        ``_liquidity_cache`` only ever holds provider results, so there is
        deliberately no ``cache_key`` parameter. The low-confidence-depth call
        site invokes this helper purely for its strict-mode raise and falls
        through to the success path in non-strict mode.
        """
        if self._is_strict_historical_mode():
            error = HistoricalDataUnavailableError(
                data_type="liquidity",
                identifier=identifier,
                timestamp=timestamp,
                message=message,
                chain=chain,
                protocol=protocol,
            )
            if cause is _RAISE_PLAIN:
                raise error
            raise error from cast("BaseException | None", cause)
        if on_fallback is not None:
            on_fallback()

    def _resolve_liquidity_chain(
        self,
        timestamp: datetime,
        protocol: str | None,
        pool_address_lower: str,
    ) -> str | None:
        """Resolve the config chain string to a canonical chain name for liquidity lookups.

        Returns None when the chain is unknown and the non-strict fallback has
        already been applied (warning logged, nothing cached); strict mode
        raises instead.
        """
        chain_str = self._config.chain
        descriptor = ChainRegistry.try_resolve(chain_str)
        if descriptor is not None:
            return descriptor.name
        self._liquidity_data_unavailable(
            identifier=pool_address_lower,
            timestamp=timestamp,
            message=f"Unknown chain '{chain_str}', cannot fetch historical liquidity",
            chain=chain_str,
            protocol=protocol,
            cause=None,
            on_fallback=lambda: logger.warning("Unknown chain '%s', cannot fetch historical liquidity", chain_str),
        )
        return None

    def _cache_liquidity_success(
        self,
        cache_key: tuple[str, date],
        result: "LiquidityResult",
    ) -> "LiquidityResult":
        """Cache and return a successful liquidity lookup, logging its source."""
        self._liquidity_cache[cache_key] = result
        logger.debug(
            "Fetched historical liquidity for pool %s... on %s: depth=$%.2f, confidence=%s, source=%s",
            cache_key[0][:10],
            cache_key[1],
            float(result.depth),
            result.source_info.confidence.value,
            result.source_info.source,
        )
        return result

    def _fetch_and_cache_liquidity(
        self,
        provider: "LiquidityDepthProvider",
        pool_address_lower: str,
        timestamp: datetime,
        target_date: date,
        chain: str,
        protocol: str | None,
        cache_key: tuple[str, date],
    ) -> "LiquidityResult | None":
        """Fetch liquidity depth from the provider and cache successful results.

        Refuses to block when called from inside a running event-loop task:
        strict mode raises, non-strict mode degrades to None without caching.
        Only provider results are cached -- never failure markers.
        """
        chain_label = chain if chain else self._config.chain
        try:
            if in_running_event_loop_task():
                self._liquidity_data_unavailable(
                    identifier=pool_address_lower,
                    timestamp=timestamp,
                    message="Cannot fetch historical liquidity in async context",
                    chain=chain_label,
                    protocol=protocol,
                    on_fallback=lambda: logger.debug(
                        "Historical liquidity fetch skipped in async context; using fallback."
                    ),
                )
                return None
            liquidity_result = run_coroutine_blocking(
                lambda: provider.get_liquidity_depth(
                    pool_address=pool_address_lower,
                    chain=chain,
                    timestamp=timestamp,
                    protocol=protocol,
                ),
                timeout=30,
            )
            if liquidity_result.depth <= 0 and liquidity_result.source_info.confidence == DataConfidence.LOW:
                # Strict mode raises; non-strict falls through so the
                # low-confidence result is cached and returned like a success.
                self._liquidity_data_unavailable(
                    identifier=pool_address_lower,
                    timestamp=timestamp,
                    message="No historical liquidity data available (returned low-confidence fallback)",
                    chain=chain_label,
                    protocol=protocol,
                )
            return self._cache_liquidity_success(cache_key, liquidity_result)
        except HistoricalDataUnavailableError:
            raise
        except Exception as e:
            fetch_error = e
            self._liquidity_data_unavailable(
                identifier=pool_address_lower,
                timestamp=timestamp,
                message=f"Failed to fetch historical liquidity: {fetch_error}",
                chain=chain_label,
                protocol=protocol,
                cause=fetch_error,
                on_fallback=lambda: logger.debug(
                    "Failed to fetch historical liquidity for pool %s on %s: %s",
                    pool_address_lower[:10],
                    target_date,
                    fetch_error,
                ),
            )
            return None

    def _get_historical_liquidity(
        self,
        pool_address: str | None,
        timestamp: datetime,
        chain: str | None = None,
        protocol: str | None = None,
    ) -> "LiquidityResult | None":
        """Get historical liquidity depth for a pool at a specific timestamp.

        Fetches liquidity depth data from LiquidityDepthProvider which routes to
        the correct DEX-specific subgraph based on protocol or chain detection.

        Args:
            pool_address: The pool contract address (optional)
            timestamp: The timestamp to get liquidity for
            chain: Canonical chain name for subgraph routing (optional, defaults to config.chain)
            protocol: Protocol identifier (e.g., "uniswap_v3", "aerodrome") (optional)

        Returns:
            LiquidityResult with depth and confidence, or None if unavailable
            (unless strict mode is enabled).

        Raises:
            HistoricalDataUnavailableError: If strict_historical_mode is True and
                historical liquidity data cannot be fetched.
        """
        if not pool_address:
            self._liquidity_data_unavailable(
                identifier="unknown",
                timestamp=timestamp,
                message="Pool address not provided for historical liquidity lookup",
                chain=self._config.chain,
                protocol=protocol,
            )
            return None

        provider = self._ensure_liquidity_provider()
        if provider is None:
            self._liquidity_data_unavailable(
                identifier=pool_address,
                timestamp=timestamp,
                message="Liquidity provider not available (historical liquidity disabled or failed to initialize)",
                chain=self._config.chain,
                protocol=protocol,
            )
            return None

        pool_address_lower = pool_address.lower()
        target_date = timestamp.date() if isinstance(timestamp, datetime) else timestamp

        cache_key = (pool_address_lower, target_date)
        if cache_key in self._liquidity_cache:
            return self._liquidity_cache[cache_key]

        if chain is None:
            chain = self._resolve_liquidity_chain(timestamp, protocol, pool_address_lower)
            if chain is None:
                return None

        return self._fetch_and_cache_liquidity(
            provider,
            pool_address_lower,
            timestamp,
            target_date,
            chain,
            protocol,
            cache_key,
        )

    def _calculate_slippage(
        self,
        trade_amount_usd: Decimal,
        pool_address: str | None,
        timestamp: datetime,
        chain: str | None = None,
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
            chain: Canonical chain name for subgraph routing (optional).
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
        data via the gateway DEX-volume lane. Uses rate limiting from
        BacktestDataConfig if provided.

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

    def _volume_data_unavailable(
        self,
        *,
        identifier: str,
        timestamp: datetime,
        message: str,
        chain: str | None,
        protocol: str | None,
        cache_key: tuple[str, date] | None = None,
        cause: BaseException | None | object = _RAISE_PLAIN,
        on_fallback: Callable[[], None] | None = None,
    ) -> tuple[None, DataConfidence]:
        """Apply the fidelity contract for a failed historical volume lookup.

        Strict historical mode raises HistoricalDataUnavailableError -- chained
        from ``cause`` unless it is the ``_RAISE_PLAIN`` sentinel -- without
        logging or caching. Non-strict mode runs ``on_fallback`` (the per-site
        log call), caches ``(None, LOW)`` under ``cache_key`` when one is
        given, and returns the degraded result.
        """
        if self._is_strict_historical_mode():
            error = HistoricalDataUnavailableError(
                data_type="volume",
                identifier=identifier,
                timestamp=timestamp,
                message=message,
                chain=chain,
                protocol=protocol,
            )
            if cause is _RAISE_PLAIN:
                raise error
            raise error from cast("BaseException | None", cause)
        if on_fallback is not None:
            on_fallback()
        if cache_key is not None:
            self._volume_cache[cache_key] = (None, DataConfidence.LOW)
        return None, DataConfidence.LOW

    def _resolve_volume_chain(
        self,
        timestamp: datetime,
        protocol: str | None,
        cache_key: tuple[str, date],
    ) -> str | None:
        """Resolve the config chain string to a canonical chain name for volume lookups.

        Returns None when the chain is unknown and the non-strict fallback has
        already been applied (warning logged, ``(None, LOW)`` cached); strict
        mode raises instead.
        """
        chain_str = self._config.chain
        descriptor = ChainRegistry.try_resolve(chain_str)
        if descriptor is not None:
            return descriptor.name
        self._volume_data_unavailable(
            identifier=cache_key[0],
            timestamp=timestamp,
            message=f"Unknown chain '{chain_str}', cannot fetch historical volume",
            chain=chain_str,
            protocol=protocol,
            cache_key=cache_key,
            cause=None,
            on_fallback=lambda: logger.warning("Unknown chain '%s', cannot fetch historical volume", chain_str),
        )
        return None

    def _cache_volume_success(
        self,
        cache_key: tuple[str, date],
        result: "VolumeResult",
    ) -> tuple[Decimal, DataConfidence]:
        """Cache and return a successful volume lookup, logging its source."""
        volume_usd = result.value
        confidence = result.source_info.confidence
        self._volume_cache[cache_key] = (volume_usd, confidence)
        logger.debug(
            "Fetched historical volume for pool %s... on %s: volume=$%.2f, confidence=%s, source=%s",
            cache_key[0][:10],
            cache_key[1],
            float(volume_usd),
            confidence.value,
            result.source_info.source,
        )
        return volume_usd, confidence

    def _fetch_and_cache_volume(
        self,
        provider: "MultiDEXVolumeProvider",
        pool_address_lower: str,
        timestamp: datetime,
        target_date: date,
        chain: str,
        protocol: str | None,
        cache_key: tuple[str, date],
    ) -> tuple[Decimal | None, DataConfidence]:
        """Fetch volume from the provider and cache the outcome.

        Refuses to block when called from inside a running event-loop task:
        strict mode raises, non-strict mode caches the degraded fallback.
        """
        chain_label = chain if chain else self._config.chain
        try:
            if in_running_event_loop_task():
                return self._volume_data_unavailable(
                    identifier=pool_address_lower,
                    timestamp=timestamp,
                    message="Cannot fetch historical volume in async context",
                    chain=chain_label,
                    protocol=protocol,
                    cache_key=cache_key,
                    on_fallback=lambda: logger.debug(
                        "Historical volume fetch skipped in async context; using fallback."
                    ),
                )
            volume_results = run_coroutine_blocking(
                lambda: provider.get_volume(
                    pool_address=pool_address_lower,
                    chain=chain,
                    start_date=target_date,
                    end_date=target_date,
                    protocol=protocol,
                ),
                timeout=30,
            )
            if volume_results and len(volume_results) > 0:
                return self._cache_volume_success(cache_key, volume_results[0])
            return self._volume_data_unavailable(
                identifier=pool_address_lower,
                timestamp=timestamp,
                message="No historical volume data returned from the gateway DEX-volume lane (GetDexVolumeHistory)",
                chain=chain_label,
                protocol=protocol,
                cache_key=cache_key,
            )
        except HistoricalDataUnavailableError:
            raise
        except Exception as e:
            fetch_error = e
            return self._volume_data_unavailable(
                identifier=pool_address_lower,
                timestamp=timestamp,
                message=f"Failed to fetch historical volume: {fetch_error}",
                chain=chain_label,
                protocol=protocol,
                cache_key=cache_key,
                cause=fetch_error,
                on_fallback=lambda: logger.debug(
                    "Failed to fetch historical volume for pool %s on %s: %s",
                    pool_address_lower[:10],
                    target_date,
                    fetch_error,
                ),
            )

    def _get_historical_volume(
        self,
        pool_address: str | None,
        timestamp: datetime,
        chain: str | None = None,
        protocol: str | None = None,
    ) -> tuple[Decimal | None, DataConfidence]:
        """Get historical pool volume for a specific date.

        Fetches volume data from MultiDEXVolumeProvider, which routes to the
        gateway DEX-volume lane (GetDexVolumeHistory) based on protocol or
        chain detection.

        Args:
            pool_address: The pool contract address (optional)
            timestamp: The timestamp to get volume for
            chain: Canonical chain name for gateway DEX-volume lane routing (optional,
                defaults to config.chain)
            protocol: Protocol identifier (e.g., "uniswap_v3", "aerodrome") (optional)

        Returns:
            Tuple of (volume in USD, confidence level). Returns (None, LOW) if
            volume data is unavailable (unless strict mode is enabled).

        Raises:
            HistoricalDataUnavailableError: If strict_historical_mode is True and
                historical volume data cannot be fetched.
        """
        if not pool_address:
            return self._volume_data_unavailable(
                identifier="unknown",
                timestamp=timestamp,
                message="Pool address not provided for historical volume lookup",
                chain=self._config.chain,
                protocol=protocol,
            )

        provider = self._ensure_volume_provider()
        if provider is None:
            return self._volume_data_unavailable(
                identifier=pool_address,
                timestamp=timestamp,
                message="Volume provider not available (historical volume disabled or failed to initialize)",
                chain=self._config.chain,
                protocol=protocol,
            )

        pool_address_lower = pool_address.lower()
        target_date = timestamp.date() if isinstance(timestamp, datetime) else timestamp

        cache_key = (pool_address_lower, target_date)
        if cache_key in self._volume_cache:
            return self._volume_cache[cache_key]

        if chain is None:
            chain = self._resolve_volume_chain(timestamp, protocol, cache_key)
            if chain is None:
                return None, DataConfidence.LOW

        return self._fetch_and_cache_volume(
            provider,
            pool_address_lower,
            timestamp,
            target_date,
            chain,
            protocol,
            cache_key,
        )

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
        from almanak.framework.intents.vocabulary import LPOpenIntent

        if not isinstance(intent, LPOpenIntent):
            return self._failed_lp_open_fill(
                market_state=market_state,
                protocol="unknown",
                reason="Invalid intent type",
            )

        plan = self._build_lp_open_plan(intent, market_state)
        position = self._lp_open_position(plan, market_state)
        self._annotate_lp_open_position(position, intent.pool, plan)
        self._log_lp_open(intent.pool, plan)
        return self._lp_open_success_fill(intent.pool, market_state, position, plan)

    def _failed_lp_open_fill(
        self,
        market_state: "MarketState",
        protocol: str,
        reason: str,
    ) -> "SimulatedFill":
        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl.portfolio import SimulatedFill

        return SimulatedFill(
            timestamp=market_state.timestamp,
            intent_type=IntentType.LP_OPEN,
            protocol=protocol,
            tokens=[],
            executed_price=Decimal("0"),
            amount_usd=Decimal("0"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={},
            tokens_out={},
            success=False,
            metadata={"failure_reason": reason},
        )

    def _build_lp_open_plan(self, intent: "LPOpenIntent", market_state: "MarketState") -> _LPOpenPlan:
        token0, token1 = self._lp_open_tokens(intent.pool)
        amount0 = Decimal(str(intent.amount0))
        amount1 = Decimal(str(intent.amount1))
        token0_price, token1_price = self._lp_open_prices(token0, token1, market_state)
        amount_usd = amount0 * token0_price + amount1 * token1_price
        entry_price = token0_price / token1_price if token1_price > 0 else token0_price
        range_lower, range_upper, tick_lower, tick_upper = self._lp_open_range(intent)
        protocol = intent.protocol.lower()
        fee_tier = self._lp_fee_tier_from_protocol(protocol)
        liquidity = self._lp_open_liquidity(
            amount_usd=amount_usd,
            token1_price=token1_price,
            entry_price=entry_price,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )
        return _LPOpenPlan(
            token0=token0,
            token1=token1,
            amount0=amount0,
            amount1=amount1,
            token0_price=token0_price,
            token1_price=token1_price,
            amount_usd=amount_usd,
            entry_price=entry_price,
            range_lower=range_lower,
            range_upper=range_upper,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            protocol=protocol,
            fee_tier=fee_tier,
            liquidity=liquidity,
        )

    @staticmethod
    def _lp_open_tokens(pool: str) -> tuple[str, str]:
        if "/" in pool:
            token0, token1 = pool.split("/")[:2]
            return token0.strip().upper(), token1.strip().upper()
        return "WETH", "USDC"

    def _lp_open_prices(
        self,
        token0: str,
        token1: str,
        market_state: "MarketState",
    ) -> tuple[Decimal, Decimal]:
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
        return token0_price, token1_price

    def _lp_open_range(self, intent: "LPOpenIntent") -> tuple[Decimal, Decimal, int, int]:
        range_lower = Decimal(str(intent.range_lower))
        range_upper = Decimal(str(intent.range_upper))
        tick_lower = self._price_to_tick_int(range_lower)
        tick_upper = self._price_to_tick_int(range_upper)
        if tick_upper <= tick_lower:
            # Degenerate range (both bounds floor to the same tick): widen by
            # one tick so the position has a valid V3 range and non-zero value.
            tick_upper = tick_lower + 1
        return range_lower, range_upper, tick_lower, tick_upper

    @staticmethod
    def _lp_fee_tier_from_protocol(protocol: str) -> Decimal:
        if protocol.endswith("_0.01") or protocol.endswith("_1bps") or "_0.01_" in protocol:
            return Decimal("0.0001")
        if protocol.endswith("_0.05") or protocol.endswith("_5bps") or "_0.05_" in protocol:
            return Decimal("0.0005")
        if protocol.endswith("_1") or protocol.endswith("_100bps") or "_1_" in protocol:
            return Decimal("0.01")
        return Decimal("0.003")

    def _lp_open_liquidity(
        self,
        amount_usd: Decimal,
        token1_price: Decimal,
        entry_price: Decimal,
        tick_lower: int,
        tick_upper: int,
    ) -> Decimal:
        value_token1 = amount_usd / token1_price if token1_price > 0 else amount_usd
        return self._il_calculator.liquidity_for_target_value(
            value_token1=value_token1,
            price=entry_price,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
        )

    @staticmethod
    def _lp_open_pool_address(pool: str) -> str | None:
        pool_lower = pool.lower()
        if "/" not in pool and pool_lower.startswith("0x"):
            return pool_lower
        return None

    def _lp_open_position(self, plan: _LPOpenPlan, market_state: "MarketState") -> "SimulatedPosition":
        from almanak.framework.backtesting.pnl.portfolio import SimulatedPosition

        position = SimulatedPosition.lp(
            token0=plan.token0,
            token1=plan.token1,
            amount0=plan.amount0,
            amount1=plan.amount1,
            liquidity=plan.liquidity,
            tick_lower=plan.tick_lower,
            tick_upper=plan.tick_upper,
            fee_tier=plan.fee_tier,
            entry_price=plan.entry_price,
            entry_time=market_state.timestamp,
            protocol=plan.protocol,
        )
        position.accumulated_fees_usd = Decimal("0")
        position.fees_token0 = Decimal("0")
        position.fees_token1 = Decimal("0")
        return position

    def _annotate_lp_open_position(self, position: "SimulatedPosition", pool: str, plan: _LPOpenPlan) -> None:
        position.metadata["entry_amounts"] = {
            plan.token0: str(plan.amount0),
            plan.token1: str(plan.amount1),
        }
        position.metadata["entry_price_ratio"] = str(plan.entry_price)
        position.metadata["pool_address"] = self._lp_open_pool_address(pool)

    @staticmethod
    def _log_lp_open(pool: str, plan: _LPOpenPlan) -> None:
        logger.info(
            "LP_OPEN executed: pool=%s, amount_usd=%.2f, range=[%.6f, %.6f], ticks=[%d, %d], liquidity=%.2f",
            pool,
            float(plan.amount_usd),
            float(plan.range_lower),
            float(plan.range_upper),
            plan.tick_lower,
            plan.tick_upper,
            float(plan.liquidity),
        )

    def _lp_open_success_fill(
        self,
        pool: str,
        market_state: "MarketState",
        position: "SimulatedPosition",
        plan: _LPOpenPlan,
    ) -> "SimulatedFill":
        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl.portfolio import SimulatedFill

        return SimulatedFill(
            timestamp=market_state.timestamp,
            intent_type=IntentType.LP_OPEN,
            protocol=plan.protocol,
            tokens=[plan.token0, plan.token1],
            executed_price=plan.entry_price,
            amount_usd=plan.amount_usd,
            fee_usd=Decimal("0"),  # No protocol fee for LP open
            slippage_usd=Decimal("0"),  # No slippage for LP open
            # Gas is engine-owned: PnLBacktester._execute_intent stamps the
            # chain-aware resolved cost onto successful adapter fills.
            gas_cost_usd=Decimal("0"),
            tokens_in={},  # No tokens received on open
            tokens_out={plan.token0: plan.amount0, plan.token1: plan.amount1},  # Tokens deposited
            success=True,
            position_delta=position,
            metadata={
                "pool": pool,
                "range_lower": str(plan.range_lower),
                "range_upper": str(plan.range_upper),
                "tick_lower": plan.tick_lower,
                "tick_upper": plan.tick_upper,
                "fee_tier": str(plan.fee_tier),
                "liquidity": str(plan.liquidity),
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
        from almanak.framework.intents.vocabulary import LPCloseIntent

        if not isinstance(intent, LPCloseIntent):
            return self._failed_lp_close_fill(
                market_state=market_state,
                protocol="unknown",
                tokens=[],
                reason="Invalid intent type",
            )

        position = self._find_lp_close_position(intent, portfolio)
        if position is None:
            logger.warning(
                "LP_CLOSE failed: no open LP position matched %s in portfolio",
                intent.position_id,
            )
            return self._failed_lp_close_fill(
                market_state=market_state,
                protocol=intent.protocol,
                tokens=[],
                reason=f"Position {intent.position_id} not found",
                position_close_id=intent.position_id,
            )

        if len(position.tokens) < 2:
            logger.warning(
                "LP_CLOSE failed: position %s has insufficient tokens",
                intent.position_id,
            )
            return self._failed_lp_close_fill(
                market_state=market_state,
                protocol=intent.protocol,
                tokens=position.tokens,
                reason="Position has fewer than 2 tokens",
                position_close_id=position.position_id,
            )

        prices = self._resolve_lp_position_prices(position, market_state, "execute_lp_close")
        amounts = self._calculate_lp_update_amounts(position, prices)
        close_result = self._build_lp_close_result(intent, position, prices, amounts)
        self._log_lp_close(intent, prices, close_result)
        return self._lp_close_success_fill(intent, market_state, position, prices, amounts, close_result)

    def _failed_lp_close_fill(
        self,
        market_state: "MarketState",
        protocol: str,
        tokens: list[TokenRef],
        reason: str,
        position_close_id: str | None = None,
    ) -> "SimulatedFill":
        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl.portfolio import SimulatedFill

        return SimulatedFill(
            timestamp=market_state.timestamp,
            intent_type=IntentType.LP_CLOSE,
            protocol=protocol,
            tokens=tokens,
            executed_price=Decimal("0"),
            amount_usd=Decimal("0"),
            fee_usd=Decimal("0"),
            slippage_usd=Decimal("0"),
            gas_cost_usd=Decimal("0"),
            tokens_in={},
            tokens_out={},
            success=False,
            position_close_id=position_close_id,
            metadata={"failure_reason": reason},
        )

    @staticmethod
    def _find_lp_close_position(
        intent: Any,
        portfolio: "SimulatedPortfolio",
    ) -> "SimulatedPosition | None":
        # Fungible-LP protocols (Aerodrome, Uniswap-V2-style) emit LP_CLOSE
        # with a pool-descriptor id that differs from the synthetic open id.
        from almanak.framework.backtesting.pnl.intent_extraction import find_lp_close_position_id

        matched_id = find_lp_close_position_id(intent, portfolio.positions)
        if matched_id is None:
            return None
        return next((pos for pos in portfolio.positions if pos.position_id == matched_id), None)

    @staticmethod
    def _lp_fees_earned(position: "SimulatedPosition") -> Decimal:
        fees_earned_usd = position.accumulated_fees_usd
        return position.fees_earned if fees_earned_usd == Decimal("0") else fees_earned_usd

    def _lp_close_tokens_in(
        self,
        intent: Any,
        prices: _LPUpdatePrices,
        amounts: _LPUpdateAmounts,
        fees_earned_usd: Decimal,
    ) -> tuple[dict[TokenRef, Decimal], Decimal]:
        tokens_in = {
            prices.token0: amounts.token0_amount,
            prices.token1: amounts.token1_amount,
        }
        if intent.collect_fees:
            token0_ratio = self._token0_value_ratio(amounts, prices)
            fee_token0_usd = fees_earned_usd * token0_ratio
            fee_token1_usd = fees_earned_usd * (Decimal("1") - token0_ratio)
            if prices.token0_price > 0:
                tokens_in[prices.token0] += fee_token0_usd / prices.token0_price
            if prices.token1_price > 0:
                tokens_in[prices.token1] += fee_token1_usd / prices.token1_price
            return tokens_in, amounts.position_value_usd + fees_earned_usd
        return tokens_in, amounts.position_value_usd

    @staticmethod
    def _token0_value_ratio(amounts: _LPUpdateAmounts, prices: _LPUpdatePrices) -> Decimal:
        if amounts.position_value_usd <= 0:
            return Decimal("0.5")
        token0_value = amounts.token0_amount * prices.token0_price
        return token0_value / amounts.position_value_usd

    def _lp_close_entry_amounts(
        self,
        position: "SimulatedPosition",
        prices: _LPUpdatePrices,
    ) -> tuple[Decimal, Decimal]:
        entry_amounts = position.metadata.get("entry_amounts", {})
        if entry_amounts:
            return (
                Decimal(str(entry_amounts.get(prices.token0, "0"))),
                Decimal(str(entry_amounts.get(prices.token1, "0"))),
            )

        tick_lower, tick_upper = self._lp_tick_bounds_or_full_range(position)
        _, entry_token0, entry_token1 = self._il_calculator.calculate_il_v3(
            entry_price=position.entry_price,
            current_price=position.entry_price,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=position.liquidity,
        )
        return entry_token0, entry_token1

    def _build_lp_close_result(
        self,
        intent: Any,
        position: "SimulatedPosition",
        prices: _LPUpdatePrices,
        amounts: _LPUpdateAmounts,
    ) -> _LPCloseResult:
        fees_earned_usd = self._lp_fees_earned(position)
        tokens_in, total_value_received = self._lp_close_tokens_in(intent, prices, amounts, fees_earned_usd)
        entry_token0, entry_token1 = self._lp_close_entry_amounts(position, prices)
        hold_value = entry_token0 * prices.token0_price + entry_token1 * prices.token1_price
        il_loss_usd = amounts.il_pct * hold_value
        # NOTE: This is an approximation using current token1 USD price as a proxy for entry price.
        # For more accurate PnL, entry_token0_price_usd and entry_token1_price_usd should be stored
        # in position metadata during LP_OPEN. The current approach is acceptable for backtesting
        # where token price changes over the position lifetime are typically small relative to IL.
        initial_value = entry_token0 * position.entry_price * prices.token1_price + entry_token1 * prices.token1_price
        net_lp_pnl_usd = total_value_received - initial_value
        return _LPCloseResult(
            tokens_in=tokens_in,
            total_value_received=total_value_received,
            fees_earned_usd=fees_earned_usd,
            il_pct=amounts.il_pct,
            il_loss_usd=il_loss_usd,
            initial_value_usd=initial_value,
            net_lp_pnl_usd=net_lp_pnl_usd,
        )

    @staticmethod
    def _log_lp_close(intent: Any, prices: _LPUpdatePrices, close_result: _LPCloseResult) -> None:
        logger.info(
            "LP_CLOSE executed: position=%s, token0_out=%.6f, token1_out=%.6f, "
            "value_usd=%.2f, fees_usd=%.2f, il_pct=%.4f%%, net_pnl=%.2f",
            intent.position_id,
            float(close_result.tokens_in.get(prices.token0, Decimal("0"))),
            float(close_result.tokens_in.get(prices.token1, Decimal("0"))),
            float(close_result.total_value_received),
            float(close_result.fees_earned_usd),
            float(close_result.il_pct * 100),
            float(close_result.net_lp_pnl_usd),
        )

    def _lp_close_success_fill(
        self,
        intent: Any,
        market_state: "MarketState",
        position: "SimulatedPosition",
        prices: _LPUpdatePrices,
        amounts: _LPUpdateAmounts,
        close_result: _LPCloseResult,
    ) -> "SimulatedFill":
        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl.portfolio import SimulatedFill

        return SimulatedFill(
            timestamp=market_state.timestamp,
            intent_type=IntentType.LP_CLOSE,
            protocol=intent.protocol,
            tokens=[prices.token0, prices.token1],
            executed_price=prices.current_price,
            amount_usd=close_result.total_value_received,
            fee_usd=Decimal("0"),  # Protocol fee for LP close (typically none)
            slippage_usd=Decimal("0"),  # No slippage for LP close
            # Gas is engine-owned: PnLBacktester._execute_intent stamps the
            # chain-aware resolved cost onto successful adapter fills.
            gas_cost_usd=Decimal("0"),
            tokens_in=close_result.tokens_in,  # Tokens received from closing
            tokens_out={},  # No tokens sent when closing
            success=True,
            # The matched simulated position id drives apply_fill's
            # _close_position; for fungible LP it differs from the requested
            # pool-descriptor id (kept in metadata["position_id"] below).
            position_close_id=position.position_id,
            metadata={
                "position_id": intent.position_id,
                "pool": intent.pool or f"{token_ref_display(prices.token0)}/{token_ref_display(prices.token1)}",
                "collect_fees": intent.collect_fees,
                "current_price_ratio": str(prices.current_price),
                "il_percentage": str(amounts.il_pct),
                "il_loss_usd": str(close_result.il_loss_usd),
                "fees_earned_usd": str(close_result.fees_earned_usd),
                "net_lp_pnl_usd": str(close_result.net_lp_pnl_usd),
                "initial_value_usd": str(close_result.initial_value_usd),
                "current_value_usd": str(amounts.position_value_usd),
                "token0_price_usd": str(prices.token0_price),
                "token1_price_usd": str(prices.token1_price),
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
        if not position.is_lp or len(position.tokens) < 2:
            return

        plan = self._build_lp_update_plan(position, market_state, elapsed_seconds, timestamp)
        self._commit_lp_update(position, plan)

    def _build_lp_update_plan(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        elapsed_seconds: float,
        timestamp: datetime | None,
    ) -> _LPUpdatePlan:
        prices = self._resolve_lp_update_prices(position, market_state)
        amounts = self._calculate_lp_update_amounts(position, prices)
        update_time = self._resolve_lp_update_time(position, market_state, timestamp)
        fee_result = self._maybe_calculate_lp_fee_accrual(
            position=position,
            prices=prices,
            amounts=amounts,
            elapsed_seconds=elapsed_seconds,
            update_time=update_time,
        )
        return _LPUpdatePlan(
            prices=prices,
            amounts=amounts,
            update_time=update_time,
            fee_result=fee_result,
        )

    def _resolve_lp_update_prices(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
    ) -> _LPUpdatePrices:
        return self._resolve_lp_position_prices(position, market_state, "update_position")

    def _resolve_lp_position_prices(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        context: str,
    ) -> _LPUpdatePrices:
        token0 = position.tokens[0]
        token1 = position.tokens[1]

        try:
            token0_price = market_state.get_price(token0)
        except KeyError:
            token0_price = None

        try:
            token1_price = market_state.get_price(token1)
        except KeyError:
            token1_price = None

        if token0_price is None or token0_price <= 0:
            token0_price = self._price_fallback(token0, position.entry_price, context)
        if token1_price is None or token1_price <= 0:
            token1_price = self._price_fallback(token1, Decimal("1"), context)

        current_price = token0_price / token1_price if token1_price > 0 else position.entry_price
        return _LPUpdatePrices(
            token0=token0,
            token1=token1,
            token0_price=token0_price,
            token1_price=token1_price,
            current_price=current_price,
        )

    def _calculate_lp_update_amounts(
        self,
        position: "SimulatedPosition",
        prices: _LPUpdatePrices,
    ) -> _LPUpdateAmounts:
        tick_lower = position.tick_lower if position.tick_lower is not None else -887272
        tick_upper = position.tick_upper if position.tick_upper is not None else 887272

        il_pct, current_token0, current_token1 = self._il_calculator.calculate_il_v3(
            entry_price=position.entry_price,
            current_price=prices.current_price,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=position.liquidity,
        )

        position_value = current_token0 * prices.token0_price + current_token1 * prices.token1_price
        return _LPUpdateAmounts(
            il_pct=il_pct,
            token0_amount=current_token0,
            token1_amount=current_token1,
            position_value_usd=position_value,
        )

    def _resolve_lp_update_time(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        timestamp: datetime | None,
    ) -> datetime:
        if timestamp is not None:
            return timestamp
        if hasattr(market_state, "timestamp") and market_state.timestamp is not None:
            return market_state.timestamp
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
        return datetime.now()

    def _maybe_calculate_lp_fee_accrual(
        self,
        position: "SimulatedPosition",
        prices: _LPUpdatePrices,
        amounts: _LPUpdateAmounts,
        elapsed_seconds: float,
        update_time: datetime,
    ) -> FeeAccrualResult | None:
        if not self._config.fee_tracking_enabled or elapsed_seconds <= 0:
            return None

        return self._calculate_fee_accrual(
            position=position,
            position_value_usd=amounts.position_value_usd,
            elapsed_seconds=elapsed_seconds,
            token0=prices.token0,
            token1=prices.token1,
            token0_price=prices.token0_price,
            token1_price=prices.token1_price,
            timestamp=update_time,
            pool_address=position.metadata.get("pool_address"),
            amounts={
                prices.token0: amounts.token0_amount,
                prices.token1: amounts.token1_amount,
            },
        )

    @staticmethod
    def _downgrade_confidence(
        current: Literal["high", "medium", "low"] | str | None,
        new: Literal["high", "medium", "low"] | str | None,
    ) -> Literal["high", "medium", "low"] | str | None:
        if new is None:
            return current
        if current is None:
            return new
        if new == "low" and current != "low":
            return "low"
        if new == "medium" and current == "high":
            return "medium"
        return current

    def _commit_lp_update(self, position: "SimulatedPosition", plan: _LPUpdatePlan) -> None:
        prices = plan.prices
        amounts = plan.amounts
        position.amounts[prices.token0] = amounts.token0_amount
        position.amounts[prices.token1] = amounts.token1_amount
        position.metadata["il_percentage"] = float(amounts.il_pct)
        position.metadata["current_price_ratio"] = float(prices.current_price)

        if plan.fee_result is not None:
            fee_result = plan.fee_result
            position.fees_earned += fee_result.fees_usd
            position.accumulated_fees_usd += fee_result.fees_usd
            position.fees_token0 += fee_result.fees_token0
            position.fees_token1 += fee_result.fees_token1
            position.fee_confidence = self._downgrade_confidence(
                position.fee_confidence,
                fee_result.fee_confidence,
            )
            position.slippage_confidence = self._downgrade_confidence(
                position.slippage_confidence,
                fee_result.slippage_confidence,
            )

        position.last_updated = plan.update_time

    def _resolve_pool_volume(
        self,
        position: "SimulatedPosition",
        position_value_usd: Decimal,
        timestamp: datetime | None,
        pool_address: str | None,
        protocol: str | None,
    ) -> _VolumeResolution:
        """Resolve the daily pool volume used for fee accrual, or fail loud.

        Source precedence (highest-trust first):

        1. **Explicit** -- ``explicit_pool_volume_usd_daily`` provided by the
           caller (``BacktestDataConfig`` takes precedence over
           ``LPBacktestConfig``). Used directly; HIGH confidence.
        2. **Historical** -- fetched via the gateway DEX-volume lane
           (``GetDexVolumeHistory``) through ``_get_historical_volume`` when
           ``use_historical_volume`` is enabled and a pool address + timestamp
           are available. Uses the provider's confidence.
        3. **Fallback heuristic** -- ``position_value_usd * volume_multiplier`` --
           ONLY when the caller explicitly opted in via
           ``allow_volume_fallback=True`` on either config surface. LOW confidence.

        When none of the above yields a usable number, this raises
        :class:`DataSourceUnavailableError` rather than silently fabricating a value
        (VIB-4849). The error message tells the caller exactly what to provide.

        Args:
            position: The LP position (used for diagnostics in the error).
            position_value_usd: Current position value in USD (heuristic basis).
            timestamp: Simulation timestamp for the historical lookup.
            pool_address: Pool contract address for the historical volume lookup.
            protocol: Protocol identifier for volume-lane routing.

        Returns:
            A :class:`_VolumeResolution` describing the chosen volume and its source.

        Raises:
            DataSourceUnavailableError: If no acceptable, non-fabricated volume
                source is available and the heuristic fallback is not opted into.
        """
        explicit_resolution = self._explicit_pool_volume_resolution()
        if explicit_resolution is not None:
            return explicit_resolution

        historical_resolution = self._historical_pool_volume_resolution(timestamp, pool_address, protocol)
        if historical_resolution is not None:
            return historical_resolution

        fallback_resolution = self._fallback_pool_volume_resolution(position, position_value_usd)
        if fallback_resolution is not None:
            return fallback_resolution

        self._raise_pool_volume_unavailable(position, pool_address)

    def _explicit_pool_volume_resolution(self) -> _VolumeResolution | None:
        explicit_volume = self._explicit_pool_volume_usd_daily()
        if explicit_volume is None:
            return None
        if explicit_volume < 0:
            msg = (
                "explicit_pool_volume_usd_daily must be >= 0 (a measured zero is "
                f"valid, a negative value is not), got {explicit_volume}"
            )
            raise ValueError(msg)
        return _VolumeResolution(
            volume_usd=explicit_volume,
            source="explicit",
            confidence=DataConfidence.HIGH,
        )

    def _historical_pool_volume_resolution(
        self,
        timestamp: datetime | None,
        pool_address: str | None,
        protocol: str | None,
    ) -> _VolumeResolution | None:
        if timestamp is None or not pool_address or not self._use_historical_volume():
            return None

        actual_volume, volume_confidence = self._get_historical_volume(
            pool_address=pool_address,
            timestamp=timestamp,
            protocol=protocol,
        )
        if actual_volume is None or actual_volume < 0 or volume_confidence == DataConfidence.LOW:
            return None
        return _VolumeResolution(
            volume_usd=actual_volume,
            source="historical",
            confidence=volume_confidence,
        )

    def _fallback_pool_volume_resolution(
        self,
        position: "SimulatedPosition",
        position_value_usd: Decimal,
    ) -> _VolumeResolution | None:
        if not self._allow_volume_fallback():
            return None

        multiplier = self._get_volume_fallback_multiplier()
        if multiplier <= Decimal("0"):
            logger.warning(
                "LP fee accrual fallback volume multiplier must be positive; got %s for position=%s",
                multiplier,
                position.position_id,
            )
            return None
        estimated_daily_volume = position_value_usd * multiplier
        logger.warning(
            "LP fee accrual using OPT-IN fallback volume multiplier (LOW confidence): "
            "position=%s, multiplier=%.1fx, estimated_volume=$%.2f. "
            "This is a rough estimate that can be off by an order of magnitude.",
            position.position_id,
            float(multiplier),
            float(estimated_daily_volume),
        )
        return _VolumeResolution(
            volume_usd=estimated_daily_volume,
            source="fallback",
            confidence=DataConfidence.LOW,
        )

    @staticmethod
    def _raise_pool_volume_unavailable(position: "SimulatedPosition", pool_address: str | None) -> NoReturn:
        raise DataSourceUnavailableError(
            data_type="volume",
            identifier=pool_address or f"position:{position.position_id}",
            remediation=(
                "set use_historical_volume=True with a pool address on the position "
                "and a reachable gateway DEX-volume lane (GetDexVolumeHistory); "
                "OR provide explicit_pool_volume_usd_daily "
                "(and ideally explicit_pool_liquidity_usd); "
                "OR set allow_volume_fallback=True to accept the LOW-confidence "
                "volume_multiplier heuristic."
            ),
        )

    def _calculate_fee_accrual(
        self,
        position: "SimulatedPosition",
        position_value_usd: Decimal,
        elapsed_seconds: float,
        token0: TokenRef,
        token1: TokenRef,
        token0_price: Decimal,
        token1_price: Decimal,
        timestamp: datetime | None = None,
        pool_address: str | None = None,
        protocol: str | None = None,
        amounts: dict[TokenRef, Decimal] | None = None,
    ) -> FeeAccrualResult:
        """Calculate fee accrual for an LP position.

        Uses historical volume data from MultiDEXVolumeProvider when available,
        falling back to a hybrid model combining volume-based and APR-based fee estimates.

        The confidence level is determined by the data source:
        - HIGH: Historical volume via the gateway DEX-volume lane
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
            pool_address: Pool contract address for the historical volume lookup
            protocol: Protocol identifier for routing to the correct DEX volume lane
            amounts: Token amounts used for fee attribution. Defaults to the
                position's current amounts.

        Returns:
            FeeAccrualResult with fees earned, confidence level, and data source
        """
        if position_value_usd <= 0 or elapsed_seconds <= 0:
            return self._empty_fee_accrual_result(pool_address, timestamp)

        context = self._fee_formula_context(
            position=position,
            position_value_usd=position_value_usd,
            elapsed_seconds=elapsed_seconds,
            timestamp=timestamp,
            pool_address=pool_address,
            protocol=protocol,
        )
        fee_amount = self._fee_amount_from_resolution(position, position_value_usd, context, pool_address, timestamp)
        attribution = self._attribute_lp_fees(
            fees_usd=fee_amount.fees_usd,
            amounts=amounts if amounts is not None else position.amounts,
            token0=token0,
            token1=token1,
            token0_price=token0_price,
            token1_price=token1_price,
        )
        slippage = self._fee_slippage_result(
            fees_usd=fee_amount.fees_usd,
            position_value_usd=position_value_usd,
            timestamp=timestamp,
            pool_address=pool_address,
            protocol=protocol,
        )
        self._log_fee_accrual(position, fee_amount, attribution, slippage)

        return FeeAccrualResult(
            fees_usd=fee_amount.fees_usd,
            fee_confidence=fee_amount.fee_confidence,
            data_source=fee_amount.data_source,
            fees_token0=attribution.token0_amount,
            fees_token1=attribution.token1_amount,
            volume_usd=fee_amount.volume_usd,
            pool_address=pool_address,
            timestamp=timestamp,
            slippage_confidence=slippage.confidence,
            slippage_pct=slippage.pct,
            liquidity_usd=slippage.liquidity_usd,
        )

    @staticmethod
    def _empty_fee_accrual_result(pool_address: str | None, timestamp: datetime | None) -> FeeAccrualResult:
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

    def _fee_formula_context(
        self,
        position: "SimulatedPosition",
        position_value_usd: Decimal,
        elapsed_seconds: float,
        timestamp: datetime | None,
        pool_address: str | None,
        protocol: str | None,
    ) -> _FeeFormulaContext:
        resolution = self._resolve_pool_volume(
            position=position,
            position_value_usd=position_value_usd,
            timestamp=timestamp,
            pool_address=pool_address,
            protocol=protocol,
        )
        return _FeeFormulaContext(
            days_elapsed=Decimal(str(elapsed_seconds)) / Decimal("86400"),
            liquidity_share=self._lp_liquidity_share(position_value_usd),
            base_apr=self._base_apr_for_fee_tier(position.fee_tier),
            resolution=resolution,
        )

    def _lp_liquidity_share(self, position_value_usd: Decimal) -> Decimal:
        explicit_pool_liquidity = self._explicit_pool_liquidity_usd()
        pool_liquidity = explicit_pool_liquidity if explicit_pool_liquidity is not None else self._config.base_liquidity
        if pool_liquidity > 0:
            # Real share of pool TVL. There is deliberately NO floor here: a
            # position that is 0.1% of the pool earns 0.1% of pool fees, not
            # 10%. The removed ``max(Decimal("0.1"), liquidity_share)`` floor
            # was a value-minting / conservation-class defect (epic VIB-5079;
            # blocks the v1 flag removal VIB-5130): it credited ANY sub-10%
            # position -- i.e. essentially every realistic position -- with
            # 10% of the ENTIRE pool's fee revenue, making LP fee backtests
            # optimistic by one to three orders of magnitude. Conservation
            # spec: blueprint 31 §4.3 (fee attribution must scale with the
            # real liquidity share); guarded by the lp:fee_share_scaling
            # Trust Matrix cell.
            return min(Decimal("1"), position_value_usd / pool_liquidity)
        # Pool TVL genuinely unknown (non-positive denominator, only reachable
        # through a misconfigured base_liquidity). Fall back to a neutral 0.5
        # share rather than fabricating a precise number.
        return Decimal("0.5")

    def _fee_amount_from_resolution(
        self,
        position: "SimulatedPosition",
        position_value_usd: Decimal,
        context: _FeeFormulaContext,
        pool_address: str | None,
        timestamp: datetime | None,
    ) -> _FeeAmountResult:
        resolution = context.resolution
        volume_based_fees = resolution.volume_usd * position.fee_tier * context.liquidity_share * context.days_elapsed
        apr_based_fees = position_value_usd * (context.base_apr / Decimal("365")) * context.days_elapsed

        if resolution.source == "fallback":
            return _FeeAmountResult(
                fees_usd=(volume_based_fees + apr_based_fees) / Decimal("2"),
                fee_confidence="low",
                data_source=f"fallback_multiplier:{self._get_volume_fallback_multiplier()}x",
                volume_usd=resolution.volume_usd,
            )

        self._log_real_volume_fee_source(resolution, volume_based_fees, pool_address, timestamp)
        data_source = "explicit_volume" if resolution.source == "explicit" else f"multi_dex:{self._config.chain}"
        return _FeeAmountResult(
            fees_usd=volume_based_fees,
            fee_confidence=resolution.confidence.value,
            data_source=data_source,
            volume_usd=resolution.volume_usd,
        )

    @staticmethod
    def _log_real_volume_fee_source(
        resolution: _VolumeResolution,
        volume_based_fees: Decimal,
        pool_address: str | None,
        timestamp: datetime | None,
    ) -> None:
        logger.info(
            "LP fee accrual using %s volume: pool=%s..., date=%s, volume_usd=$%.2f, fees_usd=%.4f, confidence=%s",
            resolution.source,
            pool_address[:10] if pool_address else "unknown",
            timestamp.date() if timestamp else "unknown",
            float(resolution.volume_usd),
            float(volume_based_fees),
            resolution.confidence.value,
        )

    @staticmethod
    def _attribute_lp_fees(
        fees_usd: Decimal,
        amounts: dict[TokenRef, Decimal],
        token0: TokenRef,
        token1: TokenRef,
        token0_price: Decimal,
        token1_price: Decimal,
    ) -> _FeeTokenAttribution:
        total_value = (
            amounts.get(token0, Decimal("0")) * token0_price + amounts.get(token1, Decimal("0")) * token1_price
        )

        if total_value > 0:
            token0_value = amounts.get(token0, Decimal("0")) * token0_price
            token0_ratio = token0_value / total_value
            token1_ratio = Decimal("1") - token0_ratio
        else:
            token0_ratio = Decimal("0.5")
            token1_ratio = Decimal("0.5")

        fees_token0_usd = fees_usd * token0_ratio
        fees_token1_usd = fees_usd * token1_ratio
        fees_token0_amount = Decimal("0")
        fees_token1_amount = Decimal("0")
        if token0_price > 0:
            fees_token0_amount = fees_token0_usd / token0_price
        if token1_price > 0:
            fees_token1_amount = fees_token1_usd / token1_price
        return _FeeTokenAttribution(fees_token0_amount, fees_token1_amount)

    def _fee_slippage_result(
        self,
        fees_usd: Decimal,
        position_value_usd: Decimal,
        timestamp: datetime | None,
        pool_address: str | None,
        protocol: str | None,
    ) -> _FeeSlippageResult:
        if timestamp is None or not pool_address or not self._use_historical_liquidity():
            return _FeeSlippageResult()

        trade_amount_usd = self._representative_fee_slippage_trade(fees_usd, position_value_usd)
        if trade_amount_usd <= 0:
            return _FeeSlippageResult()

        slippage_result = self._calculate_slippage(
            trade_amount_usd=trade_amount_usd,
            pool_address=pool_address,
            timestamp=timestamp,
            protocol=protocol,
            pool_type="v3",
        )
        return _FeeSlippageResult(
            confidence=slippage_result.confidence.value,
            pct=slippage_result.slippage,
            liquidity_usd=slippage_result.liquidity_usd,
        )

    @staticmethod
    def _representative_fee_slippage_trade(fees_usd: Decimal, position_value_usd: Decimal) -> Decimal:
        return fees_usd * Decimal("10") if fees_usd > 0 else position_value_usd * Decimal("0.1")

    @staticmethod
    def _log_fee_accrual(
        position: "SimulatedPosition",
        fee_amount: _FeeAmountResult,
        attribution: _FeeTokenAttribution,
        slippage: _FeeSlippageResult,
    ) -> None:
        logger.debug(
            "LP fee accrual: position=%s, fees_usd=%.4f, token0_fees=%.6f, token1_fees=%.6f, "
            "fee_confidence=%s, slippage_confidence=%s, slippage_pct=%s",
            position.position_id,
            float(fee_amount.fees_usd),
            float(attribution.token0_amount),
            float(attribution.token1_amount),
            fee_amount.fee_confidence,
            slippage.confidence,
            f"{float(slippage.pct):.4f}" if slippage.pct else "N/A",
        )

    def _base_apr_for_fee_tier(self, fee_tier: Decimal) -> Decimal:
        """Return the heuristic base APR for a given fee tier.

        Mirrors the tier->APR mapping used in ``_calculate_fee_accrual`` so the
        validation path estimates fees with the same model the backtest uses.
        """
        fee_tier_pct = fee_tier * Decimal("100")
        if fee_tier_pct <= Decimal("0.01"):
            return Decimal("0.10")  # Stablecoin pools
        if fee_tier_pct <= Decimal("0.05"):
            return Decimal("0.20")  # Blue chip pairs
        if fee_tier_pct <= Decimal("0.30"):
            return Decimal("0.25")  # Volatile pairs
        return Decimal("0.10")  # Exotic pairs

    def _estimate_heuristic_fees(self, sample: "HeuristicValidationSample") -> Decimal:
        """Compute heuristic fees for a sample using the fallback-multiplier model.

        This reproduces the ``allow_volume_fallback`` arm of ``_calculate_fee_accrual``
        (average of volume-multiplier-based and APR-based fees) so the result can be
        compared against an observed ground-truth value. No position state is mutated
        and no network egress occurs.
        """
        if sample.position_value_usd <= 0 or sample.elapsed_seconds <= 0:
            return Decimal("0")

        days_elapsed = Decimal(str(sample.elapsed_seconds)) / Decimal("86400")

        # Liquidity share, mirroring _calculate_fee_accrual (USD value over
        # USD pool TVL — VIB-5096). Use the same _explicit_pool_liquidity_usd()
        # accessor as the runtime accrual path so data_config precedence is
        # honored: otherwise validate_heuristics would score against a
        # different share model than the fees the engine actually accrues.
        explicit_pool_liquidity = self._explicit_pool_liquidity_usd()
        pool_liquidity = explicit_pool_liquidity if explicit_pool_liquidity is not None else self._config.base_liquidity
        if pool_liquidity > 0:
            # Real share, no floor -- mirrors _calculate_fee_accrual. The 10%
            # floor removed here was the same value-minting defect (epic
            # VIB-5079; blocks VIB-5130): it over-credited small positions in
            # the heuristic-validation path too, so validate_heuristics would
            # have masked the accrual bug instead of flagging it.
            liquidity_share = min(Decimal("1"), sample.position_value_usd / pool_liquidity)
        else:
            liquidity_share = Decimal("0.5")

        multiplier = self._get_volume_fallback_multiplier()
        estimated_daily_volume = sample.position_value_usd * multiplier
        volume_based_fees = estimated_daily_volume * sample.fee_tier * liquidity_share * days_elapsed

        base_apr = self._base_apr_for_fee_tier(sample.fee_tier)
        apr_based_fees = sample.position_value_usd * (base_apr / Decimal("365")) * days_elapsed

        return (volume_based_fees + apr_based_fees) / Decimal("2")

    def validate_heuristics(
        self,
        samples: "list[HeuristicValidationSample]",
        warn_threshold_pct: Decimal = Decimal("0.5"),
    ) -> "list[HeuristicValidationResult]":
        """Compare the fee heuristic against caller-provided ground-truth samples.

        For each sample, computes the heuristic fee estimate and the relative error
        versus the observed (real, on-chain) fees, then logs a WARNING for any sample
        whose error exceeds ``warn_threshold_pct`` (default 50%). This surfaces when
        the order-of-magnitude-uncertain ``volume_multiplier`` heuristic is materially
        wrong for the pools being backtested.

        This method does NOT fetch on-chain data itself -- doing so from the strategy
        container would violate the gateway boundary. The caller is responsible for
        supplying observed fees (e.g. derived from Swap events via the gateway's
        MarketService, or from a prior historical-volume run).

        Args:
            samples: Ground-truth observations to validate against. Empty list returns
                an empty result and logs nothing.
            warn_threshold_pct: Relative-error threshold (as a fraction) above which a
                WARNING is logged. Default Decimal("0.5") == 50%.

        Returns:
            One HeuristicValidationResult per input sample, in input order.
        """
        results: list[HeuristicValidationResult] = []
        for sample in samples:
            estimated = self._estimate_heuristic_fees(sample)
            observed = sample.observed_fees_usd

            if observed != 0:
                error_pct = abs(estimated - observed) / abs(observed)
            elif estimated == 0:
                error_pct = Decimal("0")
            else:
                # Observed zero but heuristic non-zero -- treat as fully off.
                error_pct = Decimal("1")

            exceeds = error_pct > warn_threshold_pct
            label = sample.label or "<unlabeled sample>"
            if exceeds:
                logger.warning(
                    "LP fee heuristic validation FAILED for %s: estimated=$%.4f, "
                    "observed=$%.4f, error=%.1f%% (threshold=%.1f%%). The "
                    "volume_multiplier heuristic is unreliable for this pool; prefer "
                    "historical or explicit volume.",
                    label,
                    float(estimated),
                    float(observed),
                    float(error_pct * 100),
                    float(warn_threshold_pct * 100),
                )
            else:
                logger.debug(
                    "LP fee heuristic validation OK for %s: estimated=$%.4f, observed=$%.4f, error=%.1f%%",
                    label,
                    float(estimated),
                    float(observed),
                    float(error_pct * 100),
                )

            results.append(
                HeuristicValidationResult(
                    label=label,
                    estimated_fees_usd=estimated,
                    observed_fees_usd=observed,
                    error_pct=error_pct,
                    exceeds_threshold=exceeds,
                )
            )

        return results

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
        tokens = self._lp_position_tokens(position)
        if tokens is None:
            return self._simple_position_value(position, market_state)

        prices = self._lp_valuation_prices(position, market_state, tokens)
        amounts = self._calculate_lp_value_amounts(position, prices)
        total_value = self._lp_value_with_fees(position, amounts.position_value_usd)
        self._log_lp_position_value(position, amounts, total_value)
        return total_value

    @staticmethod
    def _lp_position_tokens(position: "SimulatedPosition") -> tuple[TokenRef, TokenRef] | None:
        if len(position.tokens) < 2:
            return None
        return position.tokens[0], position.tokens[1]

    def _simple_position_value(self, position: "SimulatedPosition", market_state: "MarketState") -> Decimal:
        total_value = Decimal("0")
        for token, amount in position.amounts.items():
            price = self._market_price_or_none(market_state, token)
            if price is None or price <= 0:
                if self._config.strict_reproducibility:
                    self._price_fallback(token, Decimal("1"), "value_position")
                continue
            total_value += amount * price
        return total_value

    @staticmethod
    def _market_price_or_none(market_state: "MarketState", token: TokenRef) -> Decimal | None:
        try:
            return market_state.get_price(token)
        except KeyError:
            return None

    def _price_or_fallback(
        self,
        token: TokenRef,
        price: Decimal | None,
        fallback: Decimal,
        context: str,
    ) -> Decimal:
        if price is None or price <= 0:
            return self._price_fallback(token, fallback, context)
        return price

    def _lp_valuation_prices(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        tokens: tuple[TokenRef, TokenRef],
    ) -> _LPUpdatePrices:
        token0, token1 = tokens
        token0_price = self._price_or_fallback(
            token=token0,
            price=self._market_price_or_none(market_state, token0),
            fallback=position.entry_price,
            context="value_position",
        )
        token1_price = self._price_or_fallback(
            token=token1,
            price=self._market_price_or_none(market_state, token1),
            fallback=Decimal("1"),
            context="value_position",
        )
        current_price = token0_price / token1_price if token1_price > 0 else position.entry_price
        return _LPUpdatePrices(
            token0=token0,
            token1=token1,
            token0_price=token0_price,
            token1_price=token1_price,
            current_price=current_price,
        )

    @staticmethod
    def _lp_tick_bounds_or_full_range(position: "SimulatedPosition") -> tuple[int, int]:
        tick_lower = position.tick_lower if position.tick_lower is not None else -887272
        tick_upper = position.tick_upper if position.tick_upper is not None else 887272
        return tick_lower, tick_upper

    def _calculate_lp_value_amounts(
        self,
        position: "SimulatedPosition",
        prices: _LPUpdatePrices,
    ) -> _LPUpdateAmounts:
        tick_lower, tick_upper = self._lp_tick_bounds_or_full_range(position)
        il_pct, current_token0, current_token1 = self._il_calculator.calculate_il_v3(
            entry_price=position.entry_price,
            current_price=prices.current_price,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            liquidity=position.liquidity,
        )
        token_value = current_token0 * prices.token0_price + current_token1 * prices.token1_price
        return _LPUpdateAmounts(
            il_pct=il_pct,
            token0_amount=current_token0,
            token1_amount=current_token1,
            position_value_usd=token_value,
        )

    def _lp_value_with_fees(self, position: "SimulatedPosition", token_value: Decimal) -> Decimal:
        if self._config.fee_tracking_enabled:
            return token_value + position.accumulated_fees_usd
        return token_value

    @staticmethod
    def _log_lp_position_value(
        position: "SimulatedPosition",
        amounts: _LPUpdateAmounts,
        total_value: Decimal,
    ) -> None:
        logger.debug(
            "LP position value: position=%s, token0=%.6f, token1=%.6f, "
            "token_value=%.2f, fees=%.2f, total=%.2f, il_pct=%.4f%%",
            position.position_id,
            float(amounts.token0_amount),
            float(amounts.token1_amount),
            float(amounts.position_value_usd),
            float(position.accumulated_fees_usd),
            float(total_value),
            float(amounts.il_pct * 100),
        )

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
        inputs = self._range_status_inputs(position, market_state)
        if inputs is None:
            return None

        current_price_ratio = inputs.prices.current_price
        price_lower = self._tick_to_price(inputs.tick_lower)
        price_upper = self._tick_to_price(inputs.tick_upper)
        distances = self._range_distances(current_price_ratio, price_lower, price_upper)
        status = self._classify_range_status(
            current_price_ratio=current_price_ratio,
            price_lower=price_lower,
            price_upper=price_upper,
            distances=distances,
        )
        return RangeStatusResult(
            status=status,
            current_price_ratio=current_price_ratio,
            price_lower=price_lower,
            price_upper=price_upper,
            distance_to_lower_pct=distances.lower_pct,
            distance_to_upper_pct=distances.upper_pct,
        )

    def _range_status_inputs(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
    ) -> _RangeStatusInputs | None:
        if not position.is_lp:
            return None

        tick_bounds = self._range_tick_bounds(position)
        tokens = self._lp_position_tokens(position)
        if tick_bounds is None or tokens is None:
            return None

        token0, token1 = tokens
        token0_price = self._range_token0_price(position, market_state, token0)
        if token0_price is None:
            return None

        token1_price = self._price_or_fallback(
            token=token1,
            price=self._market_price_or_none(market_state, token1),
            fallback=Decimal("1"),
            context="get_range_status",
        )
        return _RangeStatusInputs(
            prices=_LPUpdatePrices(
                token0=token0,
                token1=token1,
                token0_price=token0_price,
                token1_price=token1_price,
                current_price=token0_price / token1_price,
            ),
            tick_lower=tick_bounds[0],
            tick_upper=tick_bounds[1],
        )

    @staticmethod
    def _range_tick_bounds(position: "SimulatedPosition") -> tuple[int, int] | None:
        if position.tick_lower is None or position.tick_upper is None:
            return None
        return position.tick_lower, position.tick_upper

    def _range_token0_price(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        token0: TokenRef,
    ) -> Decimal | None:
        price = self._market_price_or_none(market_state, token0)
        if price is not None and price > 0:
            return price
        if self._config.strict_reproducibility:
            self._price_fallback(token0, position.entry_price, "get_range_status")
        return None

    @staticmethod
    def _range_distances(
        current_price_ratio: Decimal,
        price_lower: Decimal,
        price_upper: Decimal,
    ) -> _RangeDistances:
        lower_pct = (
            ((current_price_ratio - price_lower) / price_lower) * Decimal("100") if price_lower > 0 else Decimal("0")
        )
        upper_pct = (
            ((price_upper - current_price_ratio) / price_upper) * Decimal("100") if price_upper > 0 else Decimal("0")
        )
        return _RangeDistances(lower_pct=lower_pct, upper_pct=upper_pct)

    def _classify_range_status(
        self,
        current_price_ratio: Decimal,
        price_lower: Decimal,
        price_upper: Decimal,
        distances: _RangeDistances,
    ) -> RangeStatus:
        margin = self._config.boundary_margin_pct
        if current_price_ratio < price_lower:
            return RangeStatus.BELOW_RANGE
        if current_price_ratio > price_upper:
            return RangeStatus.ABOVE_RANGE
        if Decimal("0") <= distances.lower_pct < margin:
            return RangeStatus.PARTIAL_BELOW
        if Decimal("0") <= distances.upper_pct < margin:
            return RangeStatus.PARTIAL_ABOVE
        return RangeStatus.IN_RANGE

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
