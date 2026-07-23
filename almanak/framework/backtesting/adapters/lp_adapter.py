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
import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
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
    HistoricalDataUnavailableError,
    NoAcceptableDataSourceError,
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
    from almanak.framework.backtesting.pnl.providers.pool_history_fallback import (
        DailyPoolHistory,
    )
    from almanak.framework.backtesting.pnl.types import LiquidityResult, VolumeResult
    from almanak.framework.intents.vocabulary import Intent, LPOpenIntent

logger = logging.getLogger(__name__)

# Sentinel expiry for permanently-memoized resolution failures.
_PERMANENT_MEMO = float("inf")

# Uniswap V3 tick constants
TICK_BASE = Decimal("1.0001")

_RAISE_PLAIN: Final = object()
"""Sentinel for ``_volume_data_unavailable``: raise without a ``from`` clause."""

# Keep primary volume prewarm requests bounded while avoiding one gateway RPC
# per pool-day. A failed multi-day request falls back to the per-day recovery
# path, so this bound affects healthy-path scalability without weakening the
# two-consecutive-error policy.
_VOLUME_PREWARM_CHUNK_DAYS = 30


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
            ``NoAcceptableDataSourceError`` rather than silently fabricating a number
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
    ``NoAcceptableDataSourceError``."""

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
    (``NoAcceptableDataSourceError``) rather than silently fabricating
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
    data_source_label: str | None = None
    """Durable provenance override for the fee result. Set when the volume was
    served by the gateway pool-history ladder (``gateway_pool_history:<provider>``)
    so result metadata names the real source instead of the generic
    ``multi_dex:<chain>``; ``None`` keeps the legacy label."""


@dataclass(frozen=True)
class _LadderVolumeOutcome:
    value: tuple[Decimal, DataConfidence] | None
    cacheable: bool


@dataclass(frozen=True)
class _LPUpdatePrices:
    token0: TokenRef
    token1: TokenRef
    token0_price: Decimal
    token1_price: Decimal
    current_price: Decimal
    fallback_used: bool = False
    """True when either token price came from ``_price_fallback`` rather than
    the market — downstream range verdicts must not claim measured-price
    confidence on a fabricated ratio."""


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


class LPPoolResolutionError(ValueError):
    """An LP intent's ``pool`` string cannot be resolved to a token pair.

    Raised instead of fabricating a default pair: the historical
    ``("WETH", "USDC")`` fallback gave every unparseable pool WETH/USDC
    semantics, so an address-style USDC/WETH pool had its ``amount0``
    priced as WETH — a ~1000x notional error on rejects and wrong fill
    legs on fills. Callers convert this into a rejected fill whose
    reason names the pool.
    """


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
        # Log-once bookkeeping for the guessed-fee-tier confidence cap.
        self._guessed_tier_warned_positions: set[str] = set()
        # Log-once bookkeeping for the per-tick fallback-volume warning.
        self._fallback_volume_warned_positions: set[str] = set()
        # Log-once bookkeeping for the per-tick slippage-fallback warning.
        self._slippage_fallback_warned_pools: set[str] = set()
        # Log-once bookkeeping for the unknown-LP-family confidence degrade.
        self._unknown_family_warned_protocols: set[str] = set()
        # Log-once bookkeeping for failed product-ambiguous pool resolution.
        self._ambiguous_resolution_warned: set[tuple[str, frozenset[str]]] = set()
        # Negative memo for failed product-ambiguous resolutions: key ->
        # _PERMANENT_MEMO (semantic no-match, final for the run) or a
        # monotonic expiry (transient transport/rate-limit failure).
        self._ambiguous_resolution_failed: dict[tuple[str, frozenset[str]], float] = {}
        # (protocol, frozenset(pair), fee_units|None) -> resolved pool address.
        # Filled by prewarm_history for symbolic pools (generated strategies
        # pass "WETH/USDC"; the volume lane needs an address). The declared
        # fee/step segment ("WETH/USDC/3000" -> 3000) is part of the identity:
        # a fee-blind key made every tier of a pair share one resolved address
        # (ALM-2949). Keyed without chain: one adapter instance serves one
        # backtest chain, and config.chain may sit on its DEFAULT while intents
        # carry the real one.
        self._resolved_pool_addresses: dict[tuple[str, frozenset[str], int | None], str] = {}
        # resolved address -> how it was chosen (result-doc provenance).
        self._resolved_pool_provenance: dict[str, str] = {}
        # pool address -> real fee tier (fraction) fetched from the v3-family
        # subgraph at prewarm; corrects slug-guessed position tiers.
        self._resolved_fee_tiers: dict[str, Decimal] = {}
        # Whether the most recent _lp_open_fee_tier call used an explicit
        # caller override (single-threaded per-fill; read when annotating).
        self._last_fee_tier_explicit = False
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
        # Per-pool-day provenance for volume days the pool-history ladder served
        # (ALM-2940). Written only by _apply_ladder_volume; keys mirror
        # _volume_cache so the fee source can label ladder-served accrual.
        self._volume_source_labels: dict[tuple[str, date], str] = {}

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
            # Log once per pool, not per tick — an unavailable liquidity
            # source otherwise repeats an identical line every tick.
            warn_key = (pool_address or "unknown").lower()
            if warn_key not in self._slippage_fallback_warned_pools:
                self._slippage_fallback_warned_pools.add(warn_key)
                logger.warning(
                    "Slippage calculation using fallback: trade=$%.2f, pool=%s, slippage=%.4f%%, confidence=%s "
                    "(logged once per pool)",
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

    async def prewarm_history(
        self,
        intent: "LPOpenIntent",
        chain: str,
        start_time: datetime,
        end_time: datetime,
    ) -> None:
        """Pre-fetch the backtest window's daily volume + liquidity into the caches.

        The sync accrual path cannot fetch mid-sim (``in_running_event_loop_task``
        guard refuses to block on the async providers), so without a warm cache
        historical volume ALWAYS degrades to fallback or refusal (ALM-2930 #4).
        The engine awaits this hook right after an LP_OPEN fill — a legal await
        point — so subsequent per-tick sync lookups hit ``_volume_cache`` /
        ``_liquidity_cache`` instead of the guard.

        Best-effort by design: symbolic pools (no address) and provider failures
        log once and return; accrual then proceeds with the existing
        fallback/refusal semantics.
        """
        protocol = intent.protocol.lower()
        pool_address = self._lp_open_pool_address(intent.pool)
        if pool_address is None:
            # Symbolic pool ("WETH/USDC") — resolve pair -> address here, at
            # the async prewarm point, so the sync volume/liquidity lanes have
            # an address to key on.
            pool_address = await self._resolve_symbolic_pool_address(intent.pool, protocol, chain)
            if pool_address is None:
                logger.warning(
                    "prewarm_history: could not resolve symbolic pool %r (%s/%s) to an address; "
                    "volume/liquidity stay on fallback semantics",
                    intent.pool,
                    protocol,
                    chain,
                )
                return
        pool_lower = pool_address.lower()
        days = [start_time.date() + timedelta(days=i) for i in range((end_time.date() - start_time.date()).days + 1)]

        await self._prewarm_fee_tier(pool_lower, protocol, chain)
        await self._prewarm_volume_lane(pool_lower, protocol, chain, days)
        await self._prewarm_liquidity_lane(pool_lower, protocol, chain, days)

    @staticmethod
    def _prewarm_lane_aborts(pool_lower: str, lane: str, streak: int) -> bool:
        """Whether a prewarm lane should stop after ``streak`` consecutive fails.

        Two consecutive failures = a sick deployment / transient outage that
        won't recover within the window — stop dialing it once per day. Shared
        by the volume and liquidity lanes so their resilience policy cannot drift.
        """
        if streak >= 2:
            logger.warning(
                "%s prewarm aborted for pool %s after %d consecutive failed days; "
                "lane appears unavailable for the window",
                lane,
                pool_lower[:10],
                streak,
            )
            return True
        return False

    async def _prewarm_volume_lane(self, pool_lower: str, protocol: str, chain: str, days: list[date]) -> None:
        """Warm ``_volume_cache`` for the window: primary DEX-volume lane, then
        the measured pool-history ladder per day (ALM-2940).

        This prewarm is the hook that actually feeds accrual: the sync per-tick
        path refuses to fetch inside the event loop, so an unwarmed day is a
        guaranteed accrual gap. The primary gateway DEX-volume lane is fetched
        in bounded contiguous ranges and its rows are applied per day. If a
        range request fails, that range retries per day until TWO CONSECUTIVE
        errors mark the primary sick (a single transient blip must not silence
        the window — ALM-2953). Every primary miss still tries the independent
        measured ladder (MEDIUM confidence).
        """
        if not self._use_historical_volume():
            return
        # The pool-history ladder is INDEPENDENT of the primary DEX-volume
        # provider — when the primary can't be constructed we must still iterate
        # so the ladder warms the window (CodeRabbit #3283). A missing provider
        # simply starts the primary lane already aborted.
        volume_provider = self._ensure_volume_provider()
        pending_days = [day for day in days if (pool_lower, day) not in self._volume_cache]
        attempted = len(pending_days)
        warmed = 0
        ladder_warmed = 0
        failed_days = 0
        primary_aborted = volume_provider is None
        last_primary_error: Exception | None = None

        for chunk in self._volume_prewarm_chunks(pending_days):
            rows_by_day: dict[date, VolumeResult] = {}
            batch_failed = False
            if volume_provider is not None and not primary_aborted and len(chunk) > 1:
                try:
                    batch_results = await volume_provider.get_volume(
                        pool_address=pool_lower,
                        chain=chain,
                        start_date=chunk[0],
                        end_date=chunk[-1],
                        protocol=protocol,
                    )
                except Exception as exc:  # noqa: BLE001 — retry this range per day below
                    batch_failed = True
                    last_primary_error = exc
                    logger.warning(
                        "Volume history range prewarm failed for pool %s on %s..%s; retrying per day: %s",
                        pool_lower[:10],
                        chunk[0],
                        chunk[-1],
                        exc,
                    )
                else:
                    failed_days = 0
                    rows_by_day = self._volume_rows_by_day(batch_results, chunk)

            for day in chunk:
                row = rows_by_day.get(day)
                if volume_provider is not None and not primary_aborted and (len(chunk) == 1 or batch_failed):
                    try:
                        day_results = await volume_provider.get_volume(
                            pool_address=pool_lower,
                            chain=chain,
                            start_date=day,
                            end_date=day,
                            protocol=protocol,
                        )
                    except Exception as exc:  # noqa: BLE001 — best-effort, per-day
                        failed_days += 1
                        last_primary_error = exc
                        logger.warning(
                            "Volume history prewarm failed for pool %s on %s (%d consecutive): %s",
                            pool_lower[:10],
                            day,
                            failed_days,
                            exc,
                        )
                        if self._prewarm_lane_aborts(pool_lower, "Volume", failed_days):
                            primary_aborted = True
                    else:
                        failed_days = 0
                        row = self._volume_rows_by_day(day_results, [day]).get(day)

                outcome = await self._prewarm_volume_day(
                    pool_lower=pool_lower,
                    protocol=protocol,
                    chain=chain,
                    day=day,
                    primary_row=row,
                )
                if outcome == "primary":
                    warmed += 1
                elif outcome == "ladder":
                    ladder_warmed += 1
        missing = attempted - warmed - ladder_warmed
        if missing > 0:
            logger.warning(
                "Volume history prewarm left %d/%d days unwarmed for pool %s (%s/%s) — accrual will use "
                "fallback/refusal semantics for those days%s",
                missing,
                attempted,
                pool_lower[:10],
                protocol,
                chain,
                f" (primary lane error: {last_primary_error})" if last_primary_error is not None else "",
            )
        elif ladder_warmed:
            logger.info(
                "Volume prewarm: primary DEX-volume lane %s for pool %s (%s/%s); %d/%d days served by "
                "the gateway pool-history ladder (MEDIUM confidence)",
                "failed" if last_primary_error is not None else "returned no data",
                pool_lower[:10],
                protocol,
                chain,
                ladder_warmed,
                attempted,
            )
        else:
            logger.info(
                "Prewarmed %d/%d days of volume history for pool %s (%s/%s)",
                warmed,
                len(days),
                pool_lower[:10],
                protocol,
                chain,
            )

    @staticmethod
    def _volume_prewarm_chunks(days: list[date]) -> list[list[date]]:
        """Group ordered days into bounded contiguous ranges."""
        chunks: list[list[date]] = []
        for day in days:
            if not chunks or len(chunks[-1]) >= _VOLUME_PREWARM_CHUNK_DAYS or day != chunks[-1][-1] + timedelta(days=1):
                chunks.append([day])
            else:
                chunks[-1].append(day)
        return chunks

    @staticmethod
    def _volume_rows_by_day(results: list["VolumeResult"], requested_days: list[date]) -> dict[date, "VolumeResult"]:
        """Index returned range rows by UTC date, ignoring out-of-range rows."""
        requested = set(requested_days)
        rows: dict[date, VolumeResult] = {}
        for result in results:
            result_day = result.source_info.timestamp.date()
            if result_day in requested and result_day not in rows:
                rows[result_day] = result
        return rows

    async def _prewarm_volume_day(
        self,
        *,
        pool_lower: str,
        protocol: str,
        chain: str,
        day: date,
        primary_row: "VolumeResult | None",
    ) -> Literal["primary", "ladder", "missing"]:
        """Apply one primary row or rescue its miss through the ladder."""
        cache_key = (pool_lower, day)
        if primary_row is not None and primary_row.source_info.confidence != DataConfidence.LOW:
            self._cache_volume_success(cache_key, primary_row)
            return "primary"

        ladder = await self._pool_history_ladder_volume_outcome_async(pool_lower, chain, protocol, day, cache_key)
        if ladder.value is not None:
            return "ladder"
        if primary_row is not None and ladder.cacheable:
            # A LOW routing-mismatch row is definitive, but only memoize it
            # after the measured ladder also returned a definitive miss.
            self._cache_volume_success(cache_key, primary_row)
            return "primary"
        return "missing"

    async def _prewarm_liquidity_lane(self, pool_lower: str, protocol: str, chain: str, days: list[date]) -> None:
        liquidity_provider = self._ensure_liquidity_provider()
        if liquidity_provider is None:
            return
        warmed = 0
        failed_days = 0
        for day in days:
            cache_key = (pool_lower, day)
            if cache_key in self._liquidity_cache:
                continue
            try:
                result = await liquidity_provider.get_liquidity_depth(
                    pool_address=pool_lower,
                    chain=chain,
                    timestamp=datetime.combine(day, datetime.min.time(), tzinfo=UTC),
                    protocol=protocol,
                )
            except Exception as exc:  # noqa: BLE001 — best-effort, per-day
                # A transient per-day fetch error must NOT abandon the rest of
                # the window (mirrors the volume lane): later days still prewarm.
                failed_days += 1
                logger.warning(
                    "Liquidity prewarm failed for pool %s on %s (%d consecutive): %s",
                    pool_lower[:10],
                    day,
                    failed_days,
                    exc,
                )
                if self._prewarm_lane_aborts(pool_lower, "Liquidity", failed_days):
                    break
                continue
            if result.source_info.confidence == DataConfidence.LOW and result.depth <= 0:
                # An empty LOW result shares the same sticky-abort streak.
                failed_days += 1
                if self._prewarm_lane_aborts(pool_lower, "Liquidity", failed_days):
                    break
                continue
            failed_days = 0
            self._cache_liquidity_success(cache_key, result)
            warmed += 1
        logger.info(
            "Prewarmed %d/%d days of liquidity history for pool %s (%s/%s)",
            warmed,
            len(days),
            pool_lower[:10],
            protocol,
            chain,
        )

    async def _prewarm_fee_tier(self, pool_lower: str, protocol: str, chain: str) -> None:
        """Fetch the pool's real ``feeTier`` from its v3-family subgraph.

        Uniswap-v3-family fees are immutable per pool; the slug-derived guess
        (0.30% default) overstates fees 6x on 0.05% pools. Best-effort: only
        v3-family subgraph schemas expose ``feeTier``, and failures leave the
        guessed tier in place.
        """
        if pool_lower in self._resolved_fee_tiers:
            return
        from almanak.connectors._strategy_base.dex_volume_registry import DexVolumeRegistry

        entry = DexVolumeRegistry.entry_for(DexVolumeRegistry.canonical(protocol) or protocol)
        # SCHEMA gate, not economics: the query below (`pool { feeTier }`) is the
        # uniswap-v3 subgraph shape. Messari-standard deployments (sushiswap,
        # pancakeswap/ethereum+bsc, uniswap/optimism) are v3-concentrated pools
        # but expose no `pool.feeTier` entity, so the query would 404 there.
        # Gating on liquidity_family_for (the subgraph schema) — NOT
        # lp_economic_family — is deliberate: it only dials the query where it
        # works. Those chains fall through to the guessed tier, which is HONESTLY
        # marked (fee confidence capped to medium + `guessed_fee_tier` provenance
        # + warn-once, see _fee_amount_from_resolution). A schema-aware feeTier
        # source for messari deployments is tracked (real-fee-tier data broker,
        # ALM-2943 / BUGLIST "Real fee-tier source for Messari-schema chains").
        if entry is None or entry.liquidity_family_for(chain) != "v3_concentrated":
            return
        subgraph_id = (entry.liquidity_subgraph_ids or {}).get(chain.lower())
        if subgraph_id is None:
            return
        provider = self._ensure_liquidity_provider()
        client = getattr(provider, "_client", None)
        if client is None:
            return
        try:
            data = await client.query(
                subgraph_id=subgraph_id,
                query="query GetPoolFee($poolAddress: ID!) { pool(id: $poolAddress) { feeTier } }",
                variables={"poolAddress": pool_lower},
            )
            raw = ((data or {}).get("pool") or {}).get("feeTier")
            if raw is None:
                return
            # Subgraph feeTier is in hundredths of a bip (500 = 0.05%).
            tier = Decimal(str(raw)) / Decimal("1000000")
            # Guard the money-math input: only a finite tier in (0, 1] (0%–100%)
            # may replace the guess. A malformed/Infinity/absurd subgraph row
            # would otherwise install an unbounded multiplier into
            # volume * fee_tier * share and mint arbitrary simulated fees.
            if tier.is_finite() and Decimal("0") < tier <= Decimal("1"):
                self._resolved_fee_tiers[pool_lower] = tier
                logger.info("Resolved pool %s real feeTier: %s", pool_lower[:10], tier)
            else:
                logger.warning(
                    "Ignoring out-of-range feeTier %s for pool %s (raw=%r); keeping prior tier",
                    tier,
                    pool_lower[:10],
                    raw,
                )
        except Exception as exc:  # noqa: BLE001 — best-effort
            logger.debug("feeTier fetch failed for pool %s: %s", pool_lower[:10], exc)

    # DexScreener dexId roots per protocol slug (their ids don't carry version
    # suffixes: uniswap_v3 pools report dexId="uniswap" with labels=["v3"]).
    _DEXSCREENER_DEX_ROOTS = {
        "uniswap_v3": "uniswap",
        "pancakeswap_v3": "pancakeswap",
        "sushiswap_v3": "sushiswap",
        "aerodrome": "aerodrome",
        "aerodrome_slipstream": "aerodrome",
        "balancer": "balancer",
        "curve": "curve",
        "traderjoe_v2": "traderjoe",
    }

    # DexScreener version label a protocol's pools must carry when the venue
    # labels versions at all (dexId is shared across versions: a deep
    # labels=["v2"] pool would otherwise win a v3 resolution). Candidates with
    # NO version labels are accepted — several venues/chains omit them.
    # Keys cover both canonical names AND the bare aliases strategies use
    # (registry detection maps "uniswap"/"pancakeswap"/"sushiswap" to the v3
    # connectors, so a bare-alias resolution demands the same label).
    # Aerodrome/slipstream is absent on purpose: DexScreener returns NO
    # labels on aerodrome pools (verified live 2026-07-14), so the two are
    # indistinguishable here — a documented resolution gap, not a policy one.
    _DEXSCREENER_VERSION_LABELS = {
        "uniswap_v3": "v3",
        "uniswap": "v3",
        "uniswap_v4": "v4",
        "uniswap_v2": "v2",
        "pancakeswap_v3": "v3",
        "pancakeswap": "v3",
        "sushiswap_v3": "v3",
        "sushiswap": "v3",
    }

    # Labels that count as VERSION CLAIMS. DexScreener labels carry other
    # vocabularies too (solidly forks tag "stable"/"volatile"); those say
    # nothing about the protocol version and must not exclude a candidate.
    _DEXSCREENER_VERSION_VOCABULARY = frozenset({"v1", "v2", "v3", "v4"})

    @classmethod
    def _pick_deepest_pair_candidate(
        cls,
        candidates: Any,
        *,
        chain: str,
        dex_root: str,
        wanted_addresses: set[str],
        wanted_symbols: set[str],
        required_version_label: str | None = None,
    ) -> tuple[Any, str]:
        """Pick the highest-liquidity candidate and its match kind.

        Address-exact matches win over symbol matches. Symbol fallback exists
        for bridged variants (USDC.e et al): they trade under the same display
        symbol but a different address than the registry's canonical token —
        a symbolic-pool strategy means "the deep pool for this pair".
        """
        # Slot order encodes trust: address-exact token identity dominates —
        # DexScreener's version labels are aggregator metadata, so a
        # symbol-only match may never override ANY address-exact match no
        # matter how its labels compare. Within each identity tier, an
        # explicit correct version label beats an unlabeled candidate
        # regardless of depth (a $50m unlabeled pool may be the wrong
        # version).
        slots: dict[tuple[str, bool], tuple[float, Any]] = {}

        def _offer(key: tuple[str, bool], candidate: Any, liquidity: float) -> None:
            current = slots.get(key)
            if current is None or liquidity > current[0]:
                slots[key] = (liquidity, candidate)

        for candidate in candidates:
            if not candidate.pair_address:
                continue
            if candidate.chain_id.lower() != chain.lower():
                continue
            if not candidate.dex_id.lower().startswith(dex_root):
                continue
            labels = {str(label).lower() for label in (getattr(candidate, "labels", None) or [])}
            version_labels = labels & cls._DEXSCREENER_VERSION_VOCABULARY
            if len(version_labels) >= 2:
                # Contradictory version claims: the metadata cannot be trusted
                # either way, so the candidate is excluded outright.
                continue
            if required_version_label is not None and version_labels and version_labels != {required_version_label}:
                continue
            version_confirmed = required_version_label is None or required_version_label in version_labels
            # No dedup set: the two token windows repeat pools, but _offer is
            # max-idempotent per slot, so duplicates are harmless — and any
            # order-dependent skip lets one copy shadow another.
            # Liquidity must be finite and non-negative BEFORE ranking:
            # aggregator payloads parse "NaN"/"Infinity"/negatives as floats,
            # and NaN in particular poisons every later comparison (a
            # NaN-first candidate could never be displaced — response order
            # would pick the pool).
            raw_liquidity = candidate.liquidity.usd
            liquidity = 0.0 if raw_liquidity is None else float(raw_liquidity)
            if not math.isfinite(liquidity) or liquidity < 0:
                continue
            candidate_addresses = {
                candidate.base_token.address.lower(),
                candidate.quote_token.address.lower(),
            }
            if candidate_addresses == wanted_addresses:
                _offer(("address", version_confirmed), candidate, liquidity)
                continue
            candidate_symbols = {candidate.base_token.symbol.upper(), candidate.quote_token.symbol.upper()}
            if candidate_symbols == wanted_symbols:
                _offer(("symbol", version_confirmed), candidate, liquidity)

        # Provenance honesty: a version claim in the match kind is only
        # meaningful when a version label was actually REQUIRED — with no
        # requirement every candidate is trivially "confirmed", and stamping
        # "version-labeled" on it would be a lie in the result record.
        if required_version_label is None:
            suffixes = {True: "", False: ""}
        else:
            suffixes = {True: ", version-labeled", False: ", unlabeled version"}
        for match, version_confirmed, base_kind in (
            ("address", True, "address-exact"),
            ("address", False, "address-exact"),
            ("symbol", True, "symbol-match"),
            ("symbol", False, "symbol-match"),
        ):
            entry = slots.get((match, version_confirmed))
            if entry is not None:
                return entry[1], f"{base_kind}{suffixes[version_confirmed]}"
        return None, "no-match"

    async def _resolve_symbolic_pool_address(self, pool: str, protocol: str, chain: str) -> str | None:
        """Resolve a symbolic "TOKEN0/TOKEN1" pool to its deepest on-chain address.

        Token symbols resolve to addresses via the offline token registry, then
        DexScreener's chain-keyed token-pairs endpoint (plain HTTPS, no gateway)
        lists that token's pools; filter to the protocol family + the exact
        address pair and take the highest-liquidity match — the pool a strategy
        means when it names a pair without an address. Memoized per
        (protocol, pair, fee_units). Free-text search is NOT used: it is
        cross-chain fuzzy and misses the majors.

        A declared fee segment ("WETH/USDC/3000") on a V3-factory-family
        protocol resolves FEE-EXACT via the shared ``validate_v3_pool`` factory
        lane instead of depth-ranking — the deepest pool for a pair is
        routinely a DIFFERENT tier than the one the strategy names, and live
        execution honors the declared tier via factory ``getPool`` (ALM-2949).
        """
        from almanak.framework.backtesting.pnl.intent_extraction import lp_pool_fee_units, lp_pool_tokens

        pair = lp_pool_tokens(pool)
        if pair is None:
            return None
        fee_units = lp_pool_fee_units(pool)
        key = (protocol, frozenset(pair), fee_units)
        cached = self._resolved_pool_addresses.get(key)
        if cached is not None:
            return cached

        from almanak.framework.data.dexscreener.client import DexScreenerClient
        from almanak.framework.data.tokens import get_token_resolver

        resolver = get_token_resolver()
        addresses: dict[str, str] = {}
        for symbol in pair:
            info = resolver.resolve(symbol, chain, log_errors=False, skip_gateway=True)
            address = getattr(info, "address", None) if info is not None else None
            if not address:
                logger.warning("Pool resolution: token registry cannot resolve %s on %s", symbol, chain)
                return None
            addresses[symbol] = address.lower()
        token0_address, token1_address = (addresses[symbol] for symbol in pair)

        if protocol in self._PRODUCT_AMBIGUOUS_LP_PROTOCOLS:
            # Classic and Slipstream pools share one DexScreener dexId with no
            # labels (verified live 2026-07-14) — depth-ranking them silently
            # bets the product. Resolution rides the gateway's ListTokenPools
            # (CoinGecko Onchain), whose dex ids ARE product-distinct; when
            # that lane is unavailable the family fails closed, never guesses.
            resolved = await self._resolve_product_ambiguous_pool(
                pool, protocol, chain, pair, token0_address, token1_address
            )
            if resolved is not None:
                self._resolved_pool_addresses[key] = resolved
                self._resolved_pool_provenance[resolved] = "gateway_onchain:product-exact-ranked"
            return resolved

        if fee_units is not None and self._is_v3_factory_family(protocol):
            # Declared tier -> factory getPool is the ground truth (the same
            # lane live compilation uses). Depth-ranking is NOT a fallback
            # here: it silently picks whatever tier is deepest, which is the
            # exact wrong-pool bug this lane exists to close (ALM-2949).
            if not self._valid_v3_fee_units(fee_units):
                # A numeric-but-impossible declaration ("WETH/USDC/0") is a
                # MALFORMED tier, not an undeclared one — silently depth-ranking
                # it would resurrect the wrong-pool bug (CodeRabbit #3308).
                logger.warning(
                    "Symbolic pool %s (%s/%s) NOT resolved: declared fee tier %d is outside the "
                    "V3 domain (0, 1_000_000). Failing closed — fix the pool id or pass an address.",
                    pool,
                    protocol,
                    chain,
                    fee_units,
                )
                return None
            resolved = await self._resolve_fee_exact_pool(
                pool, protocol, chain, token0_address, token1_address, fee_units
            )
            if resolved is not None:
                self._resolved_pool_addresses[key] = resolved
                self._resolved_pool_provenance[resolved] = "factory:fee-exact"
            return resolved

        dex_root = self._DEXSCREENER_DEX_ROOTS.get(protocol, protocol.split("_")[0])
        try:
            async with DexScreenerClient() as client:
                # Union BOTH tokens' pair lists: the endpoint caps at ~30 pairs
                # per token, and the deepest pool for a pair is routinely absent
                # from one side's window.
                candidates = await client.get_token_pairs(chain.lower(), token0_address)
                candidates += await client.get_token_pairs(chain.lower(), token1_address)
        except Exception as exc:  # noqa: BLE001 — resolution is best-effort
            logger.warning("DexScreener pool resolution failed for %s on %s: %s", pool, chain, exc)
            return None

        best, match_kind = self._pick_deepest_pair_candidate(
            candidates,
            chain=chain,
            dex_root=dex_root,
            wanted_addresses={token0_address, token1_address},
            wanted_symbols={symbol.upper() for symbol in pair},
            required_version_label=self._DEXSCREENER_VERSION_LABELS.get(protocol),
        )
        if best is None or not best.pair_address:
            return None
        # DexScreener multi-coin venues (curve) return a COMPOSITE id
        # ("pool-token0-token1"); every downstream lane keys on the plain pool
        # address, so keep only the first segment — and only if it actually is
        # an EVM address.
        first_segment = best.pair_address.lower().split("-")[0]
        if not re.fullmatch(r"0x[0-9a-f]{40}", first_segment):
            logger.warning(
                "Resolved DexScreener id %r for %s is not a plain pool address; skipping",
                best.pair_address,
                pool,
            )
            return None
        resolved = first_segment
        self._resolved_pool_addresses[key] = resolved
        self._resolved_pool_provenance[resolved] = f"dexscreener:{match_kind}"
        logger.info(
            "Resolved symbolic pool %s (%s/%s) -> %s ($%.0f liquidity via DexScreener, %s)",
            pool,
            protocol,
            chain,
            resolved,
            float(best.liquidity.usd or 0),
            match_kind,
        )
        return resolved

    # Venue families whose products (classic vs Slipstream) share one
    # aggregator dex root with no distinguishing labels. Their SYMBOLIC
    # resolution must use product-distinct evidence or fail closed.
    _PRODUCT_AMBIGUOUS_LP_PROTOCOLS = frozenset(
        {"aerodrome", "aerodrome_slipstream", "velodrome", "velodrome_slipstream"}
    )

    # Transient resolution failures (gateway down, rate limit, incomplete
    # snapshot) retry after this many seconds; semantic no-match is final.
    _TRANSIENT_RESOLUTION_RETRY_SECONDS = 60.0

    # ANCHORED CoinGecko Onchain dex-id patterns per product. An id that does
    # not match a known pattern is EXCLUDED — an unknown namespace (vendor
    # rename, new venue product, near-prefix lookalike like "aerodrome-fork")
    # must fail closed, never be classified by substring heuristics. Numeric
    # suffixes cover factory generations ("aerodrome-slipstream-3",
    # "velodrome-slipstream-v2-optimism": all observed live 2026-07-14).
    _PRODUCT_DEX_ID_PATTERNS: dict[str, re.Pattern[str]] = {
        "aerodrome": re.compile(r"^aerodrome-base$"),
        "aerodrome_slipstream": re.compile(r"^aerodrome-slipstream(-\d+)?$"),
        # Velodrome ids are ENUMERATED exactly (observed live 2026-07-14):
        # open version wildcards would classify a future incompatible product
        # (e.g. a hypothetical velodrome-v3 CL) as classic. Extend this list
        # deliberately when Velodrome ships something new.
        "velodrome": re.compile(r"^(velodrome|velodrome-finance-v2)$"),
        "velodrome_slipstream": re.compile(r"^(velodrome-finance-slipstream|velodrome-slipstream-v2-optimism)$"),
    }

    @classmethod
    def _product_dex_id_matches(cls, protocol: str, dex_id: str) -> bool:
        """True when a CoinGecko Onchain dex id names ``protocol``'s product,
        by ANCHORED pattern — unknown namespaces never classify."""
        pattern = cls._PRODUCT_DEX_ID_PATTERNS.get(protocol)
        return bool(pattern is not None and pattern.match(dex_id))

    def _warn_once_ambiguous_resolution(self, protocol: str, pair: tuple[str, str], reason: str) -> None:
        warn_key = (protocol, frozenset(pair))
        if warn_key in self._ambiguous_resolution_warned:
            return
        self._ambiguous_resolution_warned.add(warn_key)
        logger.warning(
            "Symbolic %s pool %s/%s NOT resolved (fail closed — classic and Slipstream pools are "
            "indistinguishable without product-exact dex ids): %s. Pass an explicit pool address, "
            "or ensure the gateway is reachable.",
            protocol,
            pair[0],
            pair[1],
            reason,
        )

    async def _fetch_token_pool_rows(
        self,
        client: Any,
        gateway_pb2: Any,
        chain: str,
        token_addresses: tuple[str, str],
    ) -> list[list[Any]]:
        """Fetch both tokens' RANKED pool windows via ListTokenPools (atomic).

        page=0 asks the GATEWAY to page the upstream itself on raw row counts
        and return one consistent snapshot per token, preserving upstream
        rank order — client-side pagination inferred completeness from
        FILTERED counts (a sanitized junk row truncated the search) and could
        mix cache ages across pages. Major tokens (WETH) have hundreds of
        pools, so full completeness is unattainable; selection therefore uses
        RANK-ORDER semantics (see the caller), never a deepest-of-all claim.
        """
        from almanak.framework.backtesting.pnl.providers.perp._gateway_history import run_sync_gateway_call

        windows: list[list[Any]] = []
        for token_address in token_addresses:
            request = gateway_pb2.TokenPoolsRequest(chain=chain, token_address=token_address, page=0)
            response = await run_sync_gateway_call(client.pool_analytics.ListTokenPools, request, timeout=30.0)
            if not response.success:
                raise RuntimeError(response.error or "token-pools returned success=False")
            windows.append(list(response.pools))
        return windows

    async def _resolve_product_ambiguous_pool(
        self,
        pool: str,
        protocol: str,
        chain: str,
        pair: tuple[str, str],
        token0_address: str,
        token1_address: str,
    ) -> str | None:
        """Resolve an aerodrome/velodrome-family symbolic pool product-exactly.

        Unions both tokens' pool windows from the gateway's ListTokenPools
        (CoinGecko Onchain), keeps only pools whose dex id names THIS
        protocol's product and whose token pair matches address-exactly, and
        picks the deepest finite, non-negative reserve. Any failure returns
        None (fail closed) — DexScreener is deliberately NOT a fallback here.
        """
        # Negative memo with failure-kind semantics: a SEMANTIC no-match is
        # final for the run (the pool set will not change mid-backtest), but
        # transport/rate-limit/incomplete failures are TRANSIENT — memoizing
        # them permanently would let one rate-limited burst blank the family
        # for the whole run. Transient entries expire and retry.
        import time as _time

        from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
            get_connected_gateway_client,
        )

        memo_key = (protocol, frozenset(pair))
        memo = self._ambiguous_resolution_failed.get(memo_key)
        if memo is not None and (memo == _PERMANENT_MEMO or _time.monotonic() < memo):
            return None

        def _fail_permanent(reason: str) -> None:
            self._ambiguous_resolution_failed[memo_key] = _PERMANENT_MEMO
            self._warn_once_ambiguous_resolution(protocol, pair, reason)
            return None

        def _fail_transient(reason: str) -> None:
            self._ambiguous_resolution_failed[memo_key] = _time.monotonic() + self._TRANSIENT_RESOLUTION_RETRY_SECONDS
            self._warn_once_ambiguous_resolution(protocol, pair, reason)
            return None

        try:
            client, gateway_pb2 = get_connected_gateway_client()
        except Exception as exc:  # noqa: BLE001 — fail closed, never guess the product
            _fail_transient(f"gateway unavailable: {exc}")
            return None

        try:
            windows = await self._fetch_token_pool_rows(client, gateway_pb2, chain, (token0_address, token1_address))
        except Exception as exc:  # noqa: BLE001 — fail closed, never guess the product
            _fail_transient(f"token-pools lookup failed: {exc}")
            return None

        # RANK-ORDER selection: the FIRST exact-pair, product-exact match in
        # each token's upstream-ranked window (combined liquidity/volume
        # relevance) is the pool the strategy means — the canonical, active
        # pool for the pair. Reserve is the tie-break between the two token
        # windows' firsts, never a deepest-of-all claim: major tokens carry
        # hundreds of pools, so an exhaustive reserve scan is unattainable and
        # a bounded one would be a truncation lie (round-7). Windows are
        # bounded (gateway page bound) — a pair whose pools all rank below the
        # window fails closed.
        wanted = {token0_address, token1_address}
        candidates_seen = 0
        firsts: list[tuple[float, Any]] = []
        for window in windows:
            for row in window:
                candidates_seen += 1
                if not self._product_dex_id_matches(protocol, row.dex_id):
                    continue
                if {row.base_token_address, row.quote_token_address} != wanted:
                    continue
                try:
                    reserve = float(row.reserve_usd) if row.reserve_usd else 0.0
                except ValueError:
                    continue
                if not math.isfinite(reserve) or reserve < 0:
                    continue
                firsts.append((reserve, row))
                break
        if not firsts:
            _fail_permanent(f"no product-exact pool within the ranked windows ({candidates_seen} candidates)")
            return None
        best_reserve, best = max(firsts, key=lambda entry: entry[0])
        logger.info(
            "Resolved symbolic pool %s (%s/%s) -> %s ($%.0f reserve via gateway CoinGecko Onchain, "
            "product-exact dex id %r, ranked-window selection)",
            pool,
            protocol,
            chain,
            best.pool_address,
            best_reserve,
            best.dex_id,
        )
        return best.pool_address

    @staticmethod
    def _valid_v3_fee_units(fee_units: int) -> bool:
        """Whether raw units are a possible V3 fee (uint24 fraction of 1e6)."""
        return 0 < fee_units < 1_000_000

    @staticmethod
    def _is_v3_factory_family(protocol: str) -> bool:
        """Whether ``protocol`` exposes the shared V3 ``getPool(a,b,fee)`` factory.

        Gates the fee-exact resolution lane and the pool-string fee-segment
        read: bin venues (traderjoe_v2) put a BIN STEP in the same segment,
        which is not a V3 fee and must not be priced or resolved as one.
        """
        from almanak.connectors._strategy_base.address_registry import AbiFamily, AddressRegistry

        return AddressRegistry.has_abi(protocol, AbiFamily.V3_FACTORY)

    async def _resolve_fee_exact_pool(
        self,
        pool: str,
        protocol: str,
        chain: str,
        token0_address: str,
        token1_address: str,
        fee_units: int,
    ) -> str | None:
        """Resolve a fee-declared symbolic pool via the factory ``getPool`` lane.

        Uses the shared :func:`validate_v3_pool` (the lane live compilation
        honors the declared tier through), gateway-routed — available in the
        runner since the ALM-2940 sidecar. Fails closed on any non-confirmation:
        a gateway outage or a zero-address factory answer must never degrade to
        depth-ranking, which is tier-blind (ALM-2949).
        """
        import asyncio

        from almanak.connectors._strategy_base.v3_pool_validation import validate_v3_pool
        from almanak.framework.backtesting.pnl.providers.perp._gateway_history import (
            get_connected_gateway_client,
        )

        try:
            client, _ = get_connected_gateway_client()
        except Exception as exc:  # noqa: BLE001 — fail closed, never depth-rank a declared tier
            logger.warning(
                "Fee-exact resolution for %s (%s/%s, tier %d) unavailable — gateway: %s. "
                "Failing closed; pass an explicit pool address or ensure the gateway is reachable.",
                pool,
                protocol,
                chain,
                fee_units,
                exc,
            )
            return None

        # validate_v3_pool is a sync eth_call; run it off the event loop. Any
        # escape hatch (a malformed RPC response at decode, a registry edge)
        # must still fail CLOSED, not abort the prewarm (review round, #3308).
        try:
            result = await asyncio.to_thread(
                validate_v3_pool, chain, protocol, token0_address, token1_address, fee_units, None, client
            )
        except Exception as exc:  # noqa: BLE001 — fail closed, never depth-rank a declared tier
            logger.warning(
                "Fee-exact factory lookup for %s (%s/%s, tier %d) raised: %s. Failing closed.",
                pool,
                protocol,
                chain,
                fee_units,
                exc,
            )
            return None
        if result.exists and result.pool_address:
            resolved = result.pool_address.lower()
            logger.info(
                "Resolved symbolic pool %s (%s/%s) -> %s (factory getPool, fee-exact tier %d)",
                pool,
                protocol,
                chain,
                resolved,
                fee_units,
            )
            return resolved
        if result.exists is False:
            logger.warning(
                "Symbolic pool %s (%s/%s) NOT resolved: no pool exists at declared fee tier %d "
                "(factory getPool returned the zero address). The strategy names a tier the venue "
                "does not have on %s — failing closed, never substituting another tier's pool.",
                pool,
                protocol,
                chain,
                fee_units,
                chain,
            )
        else:
            logger.warning(
                "Symbolic pool %s (%s/%s) NOT resolved: fee-exact factory lookup for tier %d "
                "could not verify (%s). Failing closed; volume/liquidity stay on fallback semantics.",
                pool,
                protocol,
                chain,
                fee_units,
                result.warning or result.reason,
            )
        return None

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

        Both branches stamp the run manifest (ALM-2943): a strict refusal or a
        non-strict degrade is exactly the "why did this run degrade" record.
        """
        from almanak.framework.backtesting.pnl.data_broker import record_data_serve
        from almanak.framework.backtesting.pnl.data_manifest import (
            LANE_POOL_VOLUME,
            OUTCOME_DEGRADED,
            OUTCOME_REFUSED,
        )

        strict = self._is_strict_historical_mode()
        record_data_serve(
            lane=LANE_POOL_VOLUME,
            key=f"{chain or ''}:{identifier}",
            source="",
            outcome=OUTCOME_REFUSED if strict else OUTCOME_DEGRADED,
            at=timestamp,
            detail=message[:200],
        )
        if strict:
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
                row = volume_results[0]
                if row.source_info.confidence != DataConfidence.LOW:
                    return self._cache_volume_success(cache_key, row)
                # A LOW row is the provider's routing-mismatch placeholder
                # (unknown protocol / unsupported chain), which the accrual
                # resolution treats as a miss — try the measured ladder
                # before caching it (ALM-2940).
                ladder = self._pool_history_ladder_volume_outcome(
                    pool_address_lower, chain, protocol, target_date, cache_key
                )
                if ladder.value is not None:
                    return ladder.value
                if ladder.cacheable:
                    return self._cache_volume_success(cache_key, row)
                return None, DataConfidence.LOW
            ladder = self._pool_history_ladder_volume_outcome(
                pool_address_lower, chain, protocol, target_date, cache_key
            )
            if ladder.value is not None:
                return ladder.value
            return self._volume_data_unavailable(
                identifier=pool_address_lower,
                timestamp=timestamp,
                message="No historical volume data returned from the gateway DEX-volume lane (GetDexVolumeHistory)",
                chain=chain_label,
                protocol=protocol,
                cache_key=cache_key if ladder.cacheable else None,
            )
        except HistoricalDataUnavailableError:
            raise
        except Exception as e:
            fetch_error = e
            ladder = self._pool_history_ladder_volume_outcome(
                pool_address_lower, chain, protocol, target_date, cache_key
            )
            if ladder.value is not None:
                return ladder.value
            return self._volume_data_unavailable(
                identifier=pool_address_lower,
                timestamp=timestamp,
                message=f"Failed to fetch historical volume: {fetch_error}",
                chain=chain_label,
                protocol=protocol,
                # The primary exception is retryable regardless of whether the
                # ladder miss was definitive, so never poison the adapter cache.
                cache_key=None,
                cause=fetch_error,
                on_fallback=lambda: logger.debug(
                    "Failed to fetch historical volume for pool %s on %s: %s",
                    pool_address_lower[:10],
                    target_date,
                    fetch_error,
                ),
            )

    def _pool_history_ladder_volume(
        self,
        pool_address_lower: str,
        chain: str,
        protocol: str | None,
        target_date: date,
        cache_key: tuple[str, date],
    ) -> tuple[Decimal, DataConfidence] | None:
        """Measured rescue after the DEX-volume lane missed (ALM-2940).

        Consults the gateway pool-history ladder (TheGraph -> DefiLlama ->
        CoinGecko Onchain) for the pool-day's real traded volume. Runs
        BEFORE ``_volume_data_unavailable`` on both the empty-result and
        exception paths — including strict historical mode, where measured
        ladder data legitimately satisfies the no-fabricated-data contract
        (it is fetched history, just from a secondary source, hence MEDIUM
        confidence). Returns ``None`` on any miss so the caller's existing
        degradation path runs unchanged.
        """
        return self._pool_history_ladder_volume_outcome(
            pool_address_lower,
            chain,
            protocol,
            target_date,
            cache_key,
        ).value

    def _pool_history_ladder_volume_outcome(
        self,
        pool_address_lower: str,
        chain: str,
        protocol: str | None,
        target_date: date,
        cache_key: tuple[str, date],
    ) -> _LadderVolumeOutcome:
        """Measured rescue retaining whether a miss is safe to memoize."""
        if not protocol:
            return _LadderVolumeOutcome(value=None, cacheable=True)
        try:
            # Broker seam (ALM-2943): routed through the run's data broker when
            # one is active; the process-wide singleton serves outside a run.
            from almanak.framework.backtesting.pnl.data_broker import pool_history_provider

            outcome = pool_history_provider().daily_history_outcome(
                pool_address=pool_address_lower,
                chain=chain,
                protocol=protocol,
                day=target_date,
            )
        except Exception as e:  # noqa: BLE001 — the rescue path must never out-fail the failed primary lane
            logger.debug("Pool-history ladder volume lookup failed for %s: %s", pool_address_lower[:10], e)
            self._record_ladder_volume_serve(
                None, pool_address_lower, chain, target_date, detail="ladder lookup failed"
            )
            return _LadderVolumeOutcome(value=None, cacheable=False)
        self._record_ladder_volume_serve(outcome.history, pool_address_lower, chain, target_date)
        return _LadderVolumeOutcome(
            value=self._apply_ladder_volume(outcome.history, pool_address_lower, target_date, cache_key),
            cacheable=outcome.cacheable,
        )

    async def _pool_history_ladder_volume_async(
        self,
        pool_address_lower: str,
        chain: str,
        protocol: str | None,
        target_date: date,
        cache_key: tuple[str, date],
    ) -> tuple[Decimal, DataConfidence] | None:
        """Async form of :meth:`_pool_history_ladder_volume` for the prewarm hook."""
        return (
            await self._pool_history_ladder_volume_outcome_async(
                pool_address_lower,
                chain,
                protocol,
                target_date,
                cache_key,
            )
        ).value

    async def _pool_history_ladder_volume_outcome_async(
        self,
        pool_address_lower: str,
        chain: str,
        protocol: str | None,
        target_date: date,
        cache_key: tuple[str, date],
    ) -> _LadderVolumeOutcome:
        """Async measured rescue retaining definitive-vs-retryable state."""
        if not protocol:
            return _LadderVolumeOutcome(value=None, cacheable=True)
        try:
            # Broker seam (ALM-2943): same routing as the sync path.
            from almanak.framework.backtesting.pnl.data_broker import pool_history_provider

            outcome = await pool_history_provider().daily_history_outcome_async(
                pool_address=pool_address_lower,
                chain=chain,
                protocol=protocol,
                day=target_date,
            )
        except Exception as e:  # noqa: BLE001 — the rescue path must never out-fail the failed primary lane
            logger.debug("Pool-history ladder volume lookup failed for %s: %s", pool_address_lower[:10], e)
            self._record_ladder_volume_serve(
                None, pool_address_lower, chain, target_date, detail="ladder lookup failed"
            )
            return _LadderVolumeOutcome(value=None, cacheable=False)
        self._record_ladder_volume_serve(outcome.history, pool_address_lower, chain, target_date)
        return _LadderVolumeOutcome(
            value=self._apply_ladder_volume(outcome.history, pool_address_lower, target_date, cache_key),
            cacheable=outcome.cacheable,
        )

    @staticmethod
    def _record_ladder_volume_serve(
        history: "DailyPoolHistory | None",
        pool_address_lower: str,
        chain: str,
        target_date: date,
        detail: str = "",
    ) -> None:
        """Stamp the run manifest with this pool-day's volume-ladder outcome (ALM-2943)."""
        from almanak.framework.backtesting.pnl.data_broker import record_data_serve
        from almanak.framework.backtesting.pnl.data_manifest import (
            LANE_POOL_VOLUME,
            OUTCOME_DEGRADED,
            OUTCOME_SERVED,
        )
        from almanak.framework.backtesting.pnl.providers.pool_history_fallback import (
            POOL_HISTORY_SOURCE_PREFIX,
        )

        served = history is not None and history.volume_24h is not None
        record_data_serve(
            lane=LANE_POOL_VOLUME,
            key=f"{chain}:{pool_address_lower}",
            source=f"{POOL_HISTORY_SOURCE_PREFIX}:{history.volume_source}" if served and history is not None else "",
            outcome=OUTCOME_SERVED if served else OUTCOME_DEGRADED,
            at=target_date,
            detail=detail if detail else ("" if served else "pool-history ladder miss"),
        )

    def _apply_ladder_volume(
        self,
        history: "DailyPoolHistory | None",
        pool_address_lower: str,
        target_date: date,
        cache_key: tuple[str, date],
    ) -> tuple[Decimal, DataConfidence] | None:
        from almanak.framework.backtesting.pnl.providers.pool_history_fallback import (
            POOL_HISTORY_SOURCE_PREFIX,
        )

        if history is None or history.volume_24h is None:
            return None
        result = (history.volume_24h, DataConfidence.MEDIUM)
        self._volume_cache[cache_key] = result
        self._volume_source_labels[cache_key] = f"{POOL_HISTORY_SOURCE_PREFIX}:{history.volume_source}"
        logger.info(
            "Historical volume for pool %s on %s served by the gateway pool-history ladder "
            "(source=%s:%s, MEDIUM confidence)",
            pool_address_lower[:10],
            target_date,
            POOL_HISTORY_SOURCE_PREFIX,
            history.volume_source,
        )
        return result

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

        pool_address_lower = pool_address.lower()
        target_date = timestamp.date() if isinstance(timestamp, datetime) else timestamp
        cache_key = (pool_address_lower, target_date)

        # Cache check FIRST — before the provider check: a ladder-warmed day
        # (from a prewarm where the primary DEX-volume provider was absent) must
        # be consumable even without a provider now (CodeRabbit #3283).
        if cache_key in self._volume_cache:
            return self._volume_cache[cache_key]

        provider = self._ensure_volume_provider()
        if provider is None:
            # Distinguish the two provider-None cases: historical volume ENABLED
            # but the primary provider failed to build (the INDEPENDENT ladder
            # can still serve — CodeRabbit #3283) vs historical volume DISABLED
            # (no measured lane at all; the ladder is historical data too, so
            # don't dial it, and don't cache).
            if self._use_historical_volume():
                # The direct ladder call below (_pool_history_ladder_volume ->
                # daily_history) is a BLOCKING gateway read. Refuse to block when
                # called from inside the engine's async iteration task (mirrors
                # _fetch_and_cache_volume) — the async prewarm already warms the
                # cache via the ASYNC ladder, so accrual reads a hit there
                # (CodeRabbit #3283).
                if in_running_event_loop_task():
                    return self._volume_data_unavailable(
                        identifier=pool_address,
                        timestamp=timestamp,
                        message="Cannot fetch historical volume in async context (no primary provider)",
                        chain=self._config.chain,
                        protocol=protocol,
                        cache_key=cache_key,
                        on_fallback=lambda: logger.debug(
                            "Ladder volume fetch skipped in async context (no primary provider); using fallback."
                        ),
                    )
                resolved_chain = (
                    chain if chain is not None else self._resolve_volume_chain(timestamp, protocol, cache_key)
                )
                ladder = _LadderVolumeOutcome(value=None, cacheable=True)
                if resolved_chain is not None:
                    ladder = self._pool_history_ladder_volume_outcome(
                        pool_address_lower, resolved_chain, protocol, target_date, cache_key
                    )
                    if ladder.value is not None:
                        return ladder.value
                # Memoize only a definitive ladder miss; transport failures and
                # provisional days must remain recoverable on the next lookup.
                return self._volume_data_unavailable(
                    identifier=pool_address,
                    timestamp=timestamp,
                    message="Primary volume provider unavailable and pool-history ladder missed",
                    chain=self._config.chain,
                    protocol=protocol,
                    cache_key=cache_key if ladder.cacheable else None,
                )
            return self._volume_data_unavailable(
                identifier=pool_address,
                timestamp=timestamp,
                message="Volume provider not available (historical volume disabled)",
                chain=self._config.chain,
                protocol=protocol,
            )

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

        try:
            plan = self._build_lp_open_plan(intent, market_state)
        except LPPoolResolutionError as exc:
            # Fail honest: an unresolvable pool must reject with a named
            # reason, never open a fabricated-default (WETH/USDC) position.
            return self._failed_lp_open_fill(
                market_state=market_state,
                protocol=str(getattr(intent, "protocol", "") or "unknown").lower(),
                reason=str(exc),
            )
        coin_amounts = getattr(intent, "coin_amounts", None) or []
        has_coin_vector = any(amount and Decimal(str(amount)) > 0 for amount in coin_amounts)
        # NB: coin_amounts and amount0/amount1 are mutually exclusive — the
        # LPOpenIntent validator rejects both together ("Cannot provide both
        # coin_amounts and amount0/amount1"), so a coin vector alongside a
        # positive amount0/amount1 notional is unconstructable (CodeRabbit
        # #3271 flagged a silent-drop there; it is not reachable).
        if plan.amount_usd <= 0 and not has_coin_vector:
            # Nothing to deposit: on-chain a zero-amount mint reverts, and a
            # zero-liquidity position would still accrue fee ticks.
            return self._failed_lp_open_fill(
                market_state=market_state,
                protocol=plan.protocol,
                reason="zero-notional LP_OPEN: no deposit amounts resolved",
            )
        if plan.amount_usd <= 0 and has_coin_vector:
            # Multi-coin allocation vectors are not modeled: proceeding would
            # record a phantom success ($0 notional, zero flows, missing
            # legs) indistinguishable from a real position in the result.
            # Fail closed with a machine-visible reason (ALM-2943 tracks the
            # stable-swap position family that values these).
            return self._failed_lp_open_fill(
                market_state=market_state,
                protocol=plan.protocol,
                reason=(
                    "unsupported: multi-coin allocation vector (coin_amounts) is not yet modeled by "
                    "the backtest LP adapter — use amount0/amount1 pairs, or forward-test"
                ),
            )
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
        token0, token1 = self._lp_open_tokens(intent)
        amount0 = Decimal(str(intent.amount0))
        amount1 = Decimal(str(intent.amount1))
        token0_price, token1_price = self._lp_open_prices(token0, token1, market_state)
        amount_usd = amount0 * token0_price + amount1 * token1_price
        entry_price = token0_price / token1_price if token1_price > 0 else token0_price
        range_lower, range_upper, tick_lower, tick_upper = self._lp_open_range(intent)
        protocol = intent.protocol.lower()
        fee_tier = self._lp_open_fee_tier(intent, protocol)
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

    def _lp_open_tokens(self, intent: "LPOpenIntent") -> tuple[str, str]:
        """Resolve the ``(token0, token1)`` symbols an LP intent's pool declares.

        ``"TOKEN0/TOKEN1[/fee]"`` parses positionally (unchanged). An
        address-style pool resolves through the offline pool registry
        (``known_pool_pair``) to the pool's REAL pair in registry
        orientation, then to symbols via the token registry — mirroring
        ``engine.get_pool_ohlcv``. Registry-unknown addresses and other
        unparseable names raise :class:`LPPoolResolutionError` so the open
        is rejected with a named reason instead of inheriting the old
        fabricated ``("WETH", "USDC")`` default.
        """
        from almanak.framework.backtesting.pnl.data_provider import is_address_like
        from almanak.framework.data.pools.reader import known_pool_pair
        from almanak.framework.data.tokens import TokenResolutionError, get_token_resolver

        pool = str(getattr(intent, "pool", "") or "").strip()
        if "/" in pool:
            token0, token1 = pool.split("/")[:2]
            return token0.strip().upper(), token1.strip().upper()
        chain = str(getattr(intent, "chain", None) or self._config.chain)
        if is_address_like(pool):
            pair = known_pool_pair(chain, pool)
            if pair is None:
                raise LPPoolResolutionError(
                    f"pool {pool!r} is not registry-known on {chain!r}; "
                    "pass a TOKEN0/TOKEN1 pool or a registry-known pool address"
                )
            if len(pair) != 2:
                raise LPPoolResolutionError(
                    f"pool {pool!r} resolved to {len(pair)} tokens on {chain!r}; a token pair is required"
                )
            symbols: list[str] = []
            for token_address in pair:
                try:
                    resolved = get_token_resolver().resolve(token_address, chain, log_errors=False, skip_gateway=True)
                except TokenResolutionError:
                    resolved = None
                if resolved is None or not getattr(resolved, "symbol", None):
                    raise LPPoolResolutionError(
                        f"pool {pool!r} token {token_address!r} is not registry-resolvable on {chain!r}; "
                        "pass a TOKEN0/TOKEN1 pool instead"
                    )
                symbols.append(str(resolved.symbol).upper())
            return symbols[0], symbols[1]
        raise LPPoolResolutionError(
            f"pool {pool!r} does not declare a token pair: pass a TOKEN0/TOKEN1 pool "
            "(e.g. 'WETH/USDC') or a registry-known pool address"
        )

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
        # Resolve through the shared extractor — tick/price discrimination and
        # decimals-aware raw-tick conversion (ALM-2948) — so the routed lane
        # can never re-interpret a range differently from the generic lane.
        from almanak.framework.backtesting.pnl.engine import _lp_pair_decimals
        from almanak.framework.backtesting.pnl.intent_extraction import get_lp_tick_range
        from almanak.framework.intents.vocabulary import lp_range_bounds

        token0, token1 = self._lp_open_tokens(intent)
        chain = str(getattr(intent, "chain", None) or self._config.chain)
        tick_lower, tick_upper = get_lp_tick_range(
            intent, self._price_to_tick_int, decimals=_lp_pair_decimals(token0, token1, chain)
        )
        if tick_upper <= tick_lower:
            # Degenerate range (both bounds floor to the same tick): widen by
            # one tick so the position has a valid V3 range and non-zero value.
            tick_upper = tick_lower + 1
        bounds = lp_range_bounds(intent)
        if bounds is not None:
            range_lower, range_upper = bounds
        else:
            range_lower = Decimal(str(intent.range_lower))
            range_upper = Decimal(str(intent.range_upper))
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

    def _lp_open_fee_tier(self, intent: "LPOpenIntent", protocol: str) -> Decimal:
        """Resolve the LP fee tier, honoring a caller-declared tier.

        Declaration order: an explicit ``protocol_params["fee_tier"]`` fraction
        wins; else a V3-family pool id's fee segment ("WETH/USDC/3000" — raw
        factory units, ALM-2949) is the declared tier. Only then fall back to
        ``_lp_fee_tier_from_protocol``, which defaults to 0.30% for any pool
        whose fee tier is not encoded in its protocol slug (e.g.
        ``aerodrome_slipstream``), overstating fees ~60x for low-fee pools
        (ALM-2930). Guessed tiers (``_last_fee_tier_explicit`` False) are
        corrected from the pool's real subgraph ``feeTier`` at prewarm;
        declared tiers never are.
        """
        from almanak.framework.backtesting.pnl.intent_extraction import lp_pool_fee_units

        self._last_fee_tier_explicit = False
        pool = getattr(intent, "pool", None)
        fee_units = lp_pool_fee_units(pool)
        declared = (
            Decimal(fee_units) / Decimal("1000000")
            if fee_units is not None and self._is_v3_factory_family(protocol) and self._valid_v3_fee_units(fee_units)
            else None
        )
        params = getattr(intent, "protocol_params", None) or {}
        override = params.get("fee_tier")
        if override is not None:
            try:
                tier = Decimal(str(override))
            except (TypeError, ValueError, ArithmeticError):
                tier = Decimal("-1")
            if tier > 0:
                if declared is not None and tier != declared:
                    # Both knobs set and disagreeing: the pool id names WHICH
                    # pool (identity — resolution and volume/liquidity follow
                    # it, like live getPool); the params override sets the fee
                    # ECONOMICS (e.g. dynamic-fee venues). Legitimate, but
                    # never silent (Codex review, #3308).
                    logger.warning(
                        "protocol_params['fee_tier']=%s differs from the tier declared in pool id %r "
                        "(%s): the position PRICES fees at the override while volume/liquidity come "
                        "from the declared pool.",
                        tier,
                        pool,
                        declared,
                    )
                self._last_fee_tier_explicit = True
                return tier
            logger.warning("Ignoring invalid protocol_params['fee_tier']=%r for %s", override, protocol)
        if declared is not None:
            self._last_fee_tier_explicit = True
            return declared
        if fee_units is not None and self._is_v3_factory_family(protocol) and not self._valid_v3_fee_units(fee_units):
            # Malformed declaration ("WETH/USDC/0"): resolution already fails
            # closed on it; pricing falls to the slug guess, loudly.
            logger.warning(
                "Declared fee tier %d in pool id %r is outside the V3 domain; falling back to the "
                "slug-guessed tier for pricing",
                fee_units,
                pool,
            )
        return self._lp_fee_tier_from_protocol(protocol)

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
        # Guessed tiers may be corrected from the pool's real subgraph feeTier
        # at prewarm (the 0.30% slug default overstates fees 6x on a real
        # 0.05% pool); declared tiers (params override or pool-id fee segment)
        # never are.
        position.metadata["fee_tier_source"] = "explicit" if self._last_fee_tier_explicit else "slug_guess"
        if position.metadata["pool_address"] is None:
            # Symbolic pool: carry the declared fee segment so the accrual-time
            # address backfill reconstructs the SAME memo key the resolver
            # wrote — the key is fee-aware now (ALM-2949).
            from almanak.framework.backtesting.pnl.intent_extraction import lp_pool_fee_units

            position.metadata["declared_fee_units"] = lp_pool_fee_units(pool)

    @staticmethod
    def _log_lp_open(pool: str, plan: _LPOpenPlan) -> None:
        # The adapter cannot see portfolio acceptance: the fill it builds may
        # still be rejected by apply_fill (insufficient cash). Never claim
        # "executed" here — the authoritative executed line is the engine's
        # post-acceptance log (_log_pending_trade_outcome).
        logger.info(
            "LP_OPEN fill simulated (pending portfolio acceptance): pool=%s, amount_usd=%.2f, "
            "range=[%.6f, %.6f], ticks=[%d, %d], liquidity=%.2f",
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

        if getattr(intent, "coin_index", None) is not None or getattr(intent, "imbalanced_amounts", None) is not None:
            # Pool-coin exit selectors reshape the close's token flows
            # (single-sided or exact-amounts withdrawal); executing them as a
            # standard proportional close records flows the venue would never
            # pay out. Fail closed before any position mutation.
            selector = "coin_index" if getattr(intent, "coin_index", None) is not None else "imbalanced_amounts"
            return self._failed_lp_close_fill(
                market_state=market_state,
                protocol=intent.protocol,
                tokens=[],
                reason=(
                    f"unsupported: pool-coin exit selector ({selector}) is not yet modeled by the "
                    "backtest LP adapter — close proportionally, or forward-test"
                ),
                position_close_id=intent.position_id,
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
        # Same contract as _log_lp_open: acceptance happens later in
        # apply_fill, so this line must not claim execution.
        logger.info(
            "LP_CLOSE fill simulated (pending portfolio acceptance): position=%s, token0_out=%.6f, token1_out=%.6f, "
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

    @staticmethod
    def _is_concentrated_position(position: "SimulatedPosition") -> bool:
        """True when the position's protocol family has range semantics.

        Resolved from connector-owned `lp_economic_family` declarations —
        position economics are the connector's to declare, not derivable
        from data-source registries. Unknown venues default to fungible
        (no gating): wrongly zeroing an earning position is worse than
        skipping the range refinement, and "slipstream" forks without a
        declaration are the one recognized concentrated shape. Positions
        that hit this default while carrying real tick bounds get their fee
        confidence degraded (see `_unknown_family_with_tick_bounds`) — the
        fungible treatment is a guess there, not a declared fact.
        """
        from almanak.framework.backtesting.adapters.registry import lp_economic_family_for

        protocol = str(getattr(position, "protocol", "") or "").lower()
        family = lp_economic_family_for(protocol)
        if family is not None:
            return family in ("concentrated", "bin")
        return "slipstream" in protocol

    @staticmethod
    def _unknown_family_with_tick_bounds(position: "SimulatedPosition") -> bool:
        """True when a venue with NO declared LP family carries tick bounds.

        Such a position is treated as fungible (never range-gated), but the
        tick bounds suggest concentrated economics — the accrual may be
        crediting fees an out-of-range position would not earn, so the
        result must say so instead of passing as a declared-family number.
        """
        from almanak.framework.backtesting.adapters.registry import lp_economic_family_for

        protocol = str(getattr(position, "protocol", "") or "").lower()
        if lp_economic_family_for(protocol) is not None or "slipstream" in protocol:
            return False
        return getattr(position, "tick_lower", None) is not None and getattr(position, "tick_upper", None) is not None

    def _warn_once_unknown_family(self, protocol: str) -> None:
        if protocol in self._unknown_family_warned_protocols:
            return
        self._unknown_family_warned_protocols.add(protocol)
        logger.warning(
            "Protocol %r declares no lp_economic_family but its positions carry tick bounds: "
            "treating as fungible (no out-of-range gating) with LOW fee confidence. "
            "Declare the family on the connector's backtest_strategy_type to remove this degrade.",
            protocol,
        )

    def _position_out_of_range(
        self, position: "SimulatedPosition", token0_price: Decimal, token1_price: Decimal
    ) -> bool:
        """True when the current price sits outside the position's tick range.

        Full-range / non-CL positions (no tick bounds) are never out of range.
        Unpriceable inputs conservatively count as in-range so a data gap can't
        zero out fee accrual (ALM-2930).
        """
        tick_lower = getattr(position, "tick_lower", None)
        tick_upper = getattr(position, "tick_upper", None)
        if tick_lower is None or tick_upper is None:
            return False
        if token0_price <= 0 or token1_price <= 0:
            return False
        current_tick = self._price_to_tick_int(token0_price / token1_price)
        return current_tick < int(tick_lower) or current_tick >= int(tick_upper)

    def _price_to_tick_int(self, price: Decimal) -> int:
        """Convert a price ratio to a Uniswap V3 tick (clamped to V3 bounds).

        Delegates to the shared CL kernel — Decimal logs, same floor/clamp
        semantics, no float boundary drift near tick edges.
        """
        from almanak.connectors._strategy_base.concentrated_liquidity_math import price_to_tick

        return price_to_tick(price, non_positive="min_tick")

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

        fallback_used = False
        if token0_price is None or token0_price <= 0:
            token0_price = self._price_fallback(token0, position.entry_price, context)
            fallback_used = True
        if token1_price is None or token1_price <= 0:
            token1_price = self._price_fallback(token1, Decimal("1"), context)
            fallback_used = True

        current_price = token0_price / token1_price if token1_price > 0 else position.entry_price
        return _LPUpdatePrices(
            token0=token0,
            token1=token1,
            token0_price=token0_price,
            token1_price=token1_price,
            current_price=current_price,
            fallback_used=fallback_used,
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

        # Backfill a symbolic-pool position with the prewarm-resolved address
        # so the volume/liquidity lanes can key on it. The resolution
        # provenance travels with the position into the result doc.
        if position.metadata.get("pool_address") is None:
            resolved = self._resolved_pool_addresses.get(
                (
                    position.protocol.lower(),
                    frozenset(token_ref_display(t).upper() for t in (prices.token0, prices.token1)),
                    position.metadata.get("declared_fee_units"),
                )
            )
            if resolved is not None:
                position.metadata["pool_address"] = resolved
                provenance = self._resolved_pool_provenance.get(resolved)
                if provenance:
                    position.metadata["pool_resolution"] = provenance

        # Correct a slug-guessed fee tier with the pool's real subgraph
        # feeTier (never an explicit caller override).
        pool_address = position.metadata.get("pool_address")
        if pool_address and position.metadata.get("fee_tier_source") == "slug_guess":
            real_tier = self._resolved_fee_tiers.get(str(pool_address).lower())
            if real_tier is not None:
                # A verified tier is "subgraph" even when it equals the guess:
                # fee-confidence handling keys on whether the tier is verified,
                # not on whether the guess happened to be right.
                if real_tier != position.fee_tier:
                    logger.info(
                        "Correcting %s fee tier %s -> %s (pool's real feeTier)",
                        position.position_id,
                        position.fee_tier,
                        real_tier,
                    )
                    position.fee_tier = real_tier
                position.metadata["fee_tier_source"] = "subgraph"

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
            # Route the volume/liquidity lanes to the position's real protocol.
            # Without it, an accrual-tick cache MISS (prewarm didn't cover the
            # day / never ran) re-resolves with protocol=None, which the
            # MultiDEXVolumeProvider maps to the chain-DEFAULT DEX — silently
            # sending curve/balancer/sushiswap pools to uniswap_v3's subgraph
            # and undoing the schema-family + fee-share fixes on the miss path.
            protocol=position.protocol,
            amounts={
                prices.token0: amounts.token0_amount,
                prices.token1: amounts.token1_amount,
            },
            prices_are_fallback=prices.fallback_used,
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
            # Durable result provenance: the metrics aggregator reads
            # metadata["data_source"] — logs alone are not result-visible.
            # "data_source" holds the LATEST source (back-compat);
            # "data_sources" accumulates every distinct source the position
            # touched, so a run that degraded mid-way cannot present its
            # final tick's source as the whole story.
            if fee_result.data_source:
                sources = position.metadata.get("data_sources")
                if not isinstance(sources, list):
                    sources = []
                    position.metadata["data_sources"] = sources
                # Seed the previous singular source whenever it is absent —
                # not only when the list is missing: a partially-migrated
                # position ({data_source: "x", data_sources: []}) would
                # otherwise lose its first source on the next overwrite.
                previous = position.metadata.get("data_source")
                if isinstance(previous, str) and previous and previous not in sources:
                    sources.append(previous)
                position.metadata["data_source"] = fee_result.data_source
                if fee_result.data_source not in sources:
                    sources.append(fee_result.data_source)
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
        :class:`NoAcceptableDataSourceError` rather than silently fabricating a value
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
            NoAcceptableDataSourceError: If no acceptable, non-fabricated volume
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
        target_date = timestamp.date() if isinstance(timestamp, datetime) else timestamp
        return _VolumeResolution(
            volume_usd=actual_volume,
            source="historical",
            confidence=volume_confidence,
            # Ladder-served days carry a per-provider provenance label so the
            # fee result names the real source (ALM-2940).
            data_source_label=self._volume_source_labels.get((pool_address.lower(), target_date)),
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
        # Warn once per position, not once per tick: a 6-month hourly backtest
        # otherwise emits thousands of identical lines and drowns real signals
        # in Cloud Run logs (ALM-2936 follow-up). The LOW confidence is also
        # stamped on the position and the per-fill data_source.
        if position.position_id not in self._fallback_volume_warned_positions:
            self._fallback_volume_warned_positions.add(position.position_id)
            logger.warning(
                "LP fee accrual using OPT-IN fallback volume multiplier (LOW confidence): "
                "position=%s, multiplier=%.1fx, estimated_volume=$%.2f. "
                "This is a rough estimate that can be off by an order of magnitude. "
                "(Logged once per position; applies to every accrual tick.)",
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
        raise NoAcceptableDataSourceError(
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
        prices_are_fallback: bool = False,
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

        # Out-of-range CL positions provide no active liquidity and earn no
        # fees on-chain (ALM-2930). Decided from prices alone, BEFORE any
        # historical-data resolution (the formula context resolves volume and
        # can raise in strict mode): a measured zero must not fail over data
        # it does not need. Fungible families (stableswap, weighted, solidly)
        # have no range — their tick fields are vocabulary defaults with no
        # financial meaning — so they are never gated.
        if self._is_concentrated_position(position) and self._position_out_of_range(
            position, token0_price, token1_price
        ):
            # A complete deterministic result: no attribution, no slippage
            # resolution — zero fees produce zero flows, and the slippage
            # lane would otherwise query historical liquidity for a
            # synthetic trade this position never makes.
            # A range verdict computed from FALLBACK prices (entry price /
            # $1 substitutes) is not a measured verdict: the zero fee stands
            # (accruing on the same fabricated ratio would be no better) but
            # it must not read as high confidence (CodeRabbit find, #3271).
            return FeeAccrualResult(
                fees_usd=Decimal("0"),
                fee_confidence="high" if not prices_are_fallback else "low",
                data_source="out_of_range" if not prices_are_fallback else "out_of_range:fallback_price",
                fees_token0=Decimal("0"),
                fees_token1=Decimal("0"),
                # None, not $0: no volume was measured — the zero fee is a
                # range verdict, and a "measured zero volume" reading would
                # misattribute it to a dead pool.
                volume_usd=None,
                pool_address=pool_address,
                timestamp=timestamp,
                slippage_confidence=None,
                slippage_pct=None,
                liquidity_usd=None,
            )
        context = self._fee_formula_context(
            position=position,
            position_value_usd=position_value_usd,
            elapsed_seconds=elapsed_seconds,
            timestamp=timestamp,
            pool_address=pool_address,
            protocol=protocol,
        )
        fee_amount = self._fee_amount_from_resolution(position, position_value_usd, context, pool_address, timestamp)
        degrade_unknown_family = self._unknown_family_with_tick_bounds(position)
        if degrade_unknown_family:
            self._warn_once_unknown_family(str(getattr(position, "protocol", "") or "").lower())
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
            fee_confidence="low" if degrade_unknown_family else fee_amount.fee_confidence,
            data_source=(
                f"{fee_amount.data_source}:unknown_lp_family" if degrade_unknown_family else fee_amount.data_source
            ),
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
        # Real pool depth for the share denominator: the historical liquidity
        # lane was previously consumed ONLY by slippage, so fee shares divided
        # by the static base_liquidity placeholder ($1M) — inflating fees ~100x
        # on a deep pool (ALM-2930). Caller override still wins; placeholder is
        # the last resort.
        historical_depth: Decimal | None = None
        if pool_address and timestamp is not None and self._use_historical_liquidity():
            liquidity_result = self._get_historical_liquidity(
                pool_address=pool_address,
                timestamp=timestamp,
                protocol=protocol,
            )
            if liquidity_result is not None and liquidity_result.depth > 0:
                historical_depth = liquidity_result.depth

        return _FeeFormulaContext(
            days_elapsed=Decimal(str(elapsed_seconds)) / Decimal("86400"),
            liquidity_share=self._lp_liquidity_share(position_value_usd, historical_depth),
            base_apr=self._base_apr_for_fee_tier(position.fee_tier),
            resolution=resolution,
        )

    def _lp_liquidity_share(self, position_value_usd: Decimal, historical_depth: Decimal | None = None) -> Decimal:
        explicit_pool_liquidity = self._explicit_pool_liquidity_usd()
        if explicit_pool_liquidity is not None:
            pool_liquidity = explicit_pool_liquidity
        elif historical_depth is not None:
            pool_liquidity = historical_depth
        else:
            pool_liquidity = self._config.base_liquidity
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
        tier_is_guess = position.metadata.get("fee_tier_source") == "slug_guess"
        if tier_is_guess:
            self._warn_once_guessed_tier(position)

        if resolution.source == "fallback":
            data_source = f"fallback_multiplier:{self._get_volume_fallback_multiplier()}x"
            if tier_is_guess:
                data_source = f"{data_source}:guessed_fee_tier"
            return _FeeAmountResult(
                fees_usd=(volume_based_fees + apr_based_fees) / Decimal("2"),
                fee_confidence="low",
                data_source=data_source,
                volume_usd=resolution.volume_usd,
            )

        self._log_real_volume_fee_source(resolution, volume_based_fees, pool_address, timestamp)
        if resolution.source == "explicit":
            data_source = "explicit_volume"
        else:
            # Ladder-served days carry their own per-provider label
            # ("gateway_pool_history:<provider>"); everything else keeps the
            # legacy DEX-volume-lane label (ALM-2940).
            data_source = resolution.data_source_label or f"multi_dex:{self._config.chain}"
        fee_confidence: FeeConfidence = resolution.confidence.value
        if tier_is_guess:
            # The tier is an unverified guess (no v3-schema subgraph to read
            # feeTier from) and can be 6x off — the provenance marks EVERY
            # confidence level, and high volume confidence must not read as
            # high FEE confidence.
            data_source = f"{data_source}:guessed_fee_tier"
            if fee_confidence == "high":
                fee_confidence = "medium"
        return _FeeAmountResult(
            fees_usd=volume_based_fees,
            fee_confidence=fee_confidence,
            data_source=data_source,
            volume_usd=resolution.volume_usd,
        )

    def _warn_once_guessed_tier(self, position: "SimulatedPosition") -> None:
        if position.position_id in self._guessed_tier_warned_positions:
            return
        self._guessed_tier_warned_positions.add(position.position_id)
        logger.warning(
            "LP fee tier for %s %s is a slug guess (%s) with no verifiable subgraph source — "
            "fee confidence capped at medium; pass protocol_params['fee_tier'] for exact fees",
            position.protocol,
            position.position_id,
            position.fee_tier,
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
