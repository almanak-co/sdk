"""Legacy ``almanak.framework.data.market_snapshot`` module.

The ``MarketSnapshot`` class moved to ``almanak.framework.market.snapshot``
in VIB-4062 and is no longer importable from this module — see
``docs/migration/vib-4062-marketsnapshot.md`` for the migration table.

What still lives here:

* ``StablecoinConfig`` / ``StablecoinMode`` / ``DEFAULT_STABLECOINS`` —
  stablecoin pricing-mode configuration consumed by ``MarketSnapshot``.
* ``FreshnessConfig`` — data-staleness thresholds (warn / error).
* ``RSICalculator`` Protocol.
* Re-exports of every typed error from ``almanak.framework.market.errors``
  so legacy ``from almanak.framework.data.market_snapshot import <Error>``
  imports keep working and ``isinstance`` checks against the canonical
  class continue to succeed.

For the canonical strategy-facing API, import from the canonical home::

    from almanak.framework.market import MarketSnapshot
"""

import inspect
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    # Pandas is heavy (~83 MB resident with pyarrow/numpy). Imported only for
    # static type-checker visibility on the two ``pd.DataFrame`` return
    # annotations below; the actual ``pd.DataFrame(...)`` constructions and
    # ``df.attrs / df["col"]`` operations live inside ``_ohlcv_via_router``
    # which does its own function-local ``import pandas as pd``.
    pass


# =============================================================================
# Stablecoin Configuration
# =============================================================================

# Type for stablecoin pricing mode
StablecoinMode = Literal["market", "pegged", "hybrid"]

# Default stablecoins that are commonly used in DeFi
DEFAULT_STABLECOINS: frozenset[str] = frozenset({"USDC", "USDT", "DAI"})


# =============================================================================
# Per-protocol token variant registry (VIB-3138 → VIB-4989)
# =============================================================================
#
# The former ``PROTOCOL_TOKEN_VARIANTS`` dict was removed in VIB-4989:
# settlement-token variants (e.g. Polymarket's USDC -> PUSD on Polygon) are now
# connector capabilities, declared in each connector's ``capabilities.py`` under
# ``settlement_token_variants`` and read by ``MarketSnapshot.balance`` via
# ``CapabilitiesRegistry`` (see ``framework/market/snapshot.py``
# ``_resolve_protocol_variant``). Framework code no longer names protocols here.


@dataclass
class StablecoinConfig:
    """Configuration for stablecoin pricing behavior.

    This configuration controls how stablecoin prices are returned by MarketSnapshot.
    By default, market prices are used for safety (to detect depeg events), but
    strategies can opt into pegged pricing for simplicity or hybrid mode for
    a balance of safety and convenience.

    Attributes:
        mode: Pricing mode for stablecoins:
            - 'market' (default): Always use actual market price (safest for detecting depegs)
            - 'pegged': Always return Decimal('1.00') for stablecoins (simplest)
            - 'hybrid': Use peg if within tolerance, else fall back to market price
        depeg_tolerance_bps: Tolerance for hybrid mode in basis points (default 100 = 1%).
            If market price is within this tolerance of $1.00, the peg is used.
            Otherwise, the actual market price is returned.
        stablecoins: Set of token symbols to treat as stablecoins.
            Default: {'USDC', 'USDT', 'DAI'}

    Example:
        # Default: use market prices (safest)
        config = StablecoinConfig()

        # Always use $1.00 for stablecoins (simplest)
        config = StablecoinConfig(mode='pegged')

        # Hybrid: use peg if within 0.5%, else market price
        config = StablecoinConfig(mode='hybrid', depeg_tolerance_bps=50)

        # Add custom stablecoins
        config = StablecoinConfig(stablecoins={'USDC', 'USDT', 'DAI', 'FRAX', 'LUSD'})
    """

    mode: StablecoinMode = "market"
    depeg_tolerance_bps: int = 100  # 100 bps = 1%
    stablecoins: set[str] = field(default_factory=lambda: set(DEFAULT_STABLECOINS))

    def __post_init__(self) -> None:
        """Validate configuration values."""
        valid_modes: tuple[str, ...] = ("market", "pegged", "hybrid")
        if self.mode not in valid_modes:
            raise ValueError(f"Invalid stablecoin mode: {self.mode!r}. Must be one of {valid_modes}")
        if self.depeg_tolerance_bps < 0:
            raise ValueError(f"depeg_tolerance_bps must be non-negative, got {self.depeg_tolerance_bps}")
        # Normalize stablecoin symbols to uppercase
        self.stablecoins = {s.upper() for s in self.stablecoins}

    def is_stablecoin(self, token: str) -> bool:
        """Check if a token is configured as a stablecoin.

        Args:
            token: Token symbol (e.g., 'USDC', 'USDT')

        Returns:
            True if token is in the stablecoins set
        """
        return token.upper() in self.stablecoins

    def should_use_peg(self, token: str, market_price: Decimal) -> bool:
        """Determine if the pegged price ($1.00) should be used.

        Args:
            token: Token symbol
            market_price: Actual market price of the token

        Returns:
            True if the pegged price should be used based on mode and tolerance
        """
        if not self.is_stablecoin(token):
            return False

        if self.mode == "market":
            return False
        elif self.mode == "pegged":
            return True
        else:  # hybrid
            # Check if market price is within tolerance of $1.00
            peg = Decimal("1.00")
            tolerance = Decimal(self.depeg_tolerance_bps) / Decimal("10000")
            lower_bound = peg - tolerance
            upper_bound = peg + tolerance
            return lower_bound <= market_price <= upper_bound

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "mode": self.mode,
            "depeg_tolerance_bps": self.depeg_tolerance_bps,
            "stablecoins": list(self.stablecoins),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StablecoinConfig":
        """Create from dictionary."""
        return cls(
            mode=data.get("mode", "market"),
            depeg_tolerance_bps=data.get("depeg_tolerance_bps", 100),
            stablecoins=set(data.get("stablecoins", DEFAULT_STABLECOINS)),
        )


# =============================================================================
# Freshness Configuration
# =============================================================================


@dataclass
class FreshnessConfig:
    """Configuration for data freshness thresholds.

    This configuration controls when MarketSnapshot should warn about or reject
    stale data. Different data types can have different thresholds based on
    their expected update frequency and criticality.

    Thresholds:
        - warn thresholds: Triggers a StaleDataWarning (logged but not raised)
        - error thresholds: Raises a StaleDataError (blocks execution)

    Attributes:
        price_warn_sec: Seconds before warning about stale price data (default 30)
        price_error_sec: Seconds before rejecting stale price data (default 300)
        gas_warn_sec: Seconds before warning about stale gas data (default 30)
        gas_error_sec: Seconds before rejecting stale gas data (default 300)
        pool_warn_sec: Seconds before warning about stale pool data (default 30)
        pool_error_sec: Seconds before rejecting stale pool data (default 300)
        enabled: Whether freshness checking is enabled (default True)

    Example:
        # Default configuration
        config = FreshnessConfig()

        # Stricter thresholds for high-frequency trading
        config = FreshnessConfig(
            price_warn_sec=10,
            price_error_sec=60,
            gas_warn_sec=5,
            gas_error_sec=30,
        )

        # Disable freshness checking (not recommended for production)
        config = FreshnessConfig(enabled=False)
    """

    price_warn_sec: float = 30.0
    price_error_sec: float = 300.0
    gas_warn_sec: float = 30.0
    gas_error_sec: float = 300.0
    pool_warn_sec: float = 30.0
    pool_error_sec: float = 300.0
    enabled: bool = True

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.price_warn_sec < 0:
            raise ValueError("price_warn_sec must be non-negative")
        if self.price_error_sec < 0:
            raise ValueError("price_error_sec must be non-negative")
        if self.gas_warn_sec < 0:
            raise ValueError("gas_warn_sec must be non-negative")
        if self.gas_error_sec < 0:
            raise ValueError("gas_error_sec must be non-negative")
        if self.pool_warn_sec < 0:
            raise ValueError("pool_warn_sec must be non-negative")
        if self.pool_error_sec < 0:
            raise ValueError("pool_error_sec must be non-negative")

        # Warn threshold should be less than error threshold
        if self.price_warn_sec > self.price_error_sec:
            raise ValueError("price_warn_sec must be <= price_error_sec")
        if self.gas_warn_sec > self.gas_error_sec:
            raise ValueError("gas_warn_sec must be <= gas_error_sec")
        if self.pool_warn_sec > self.pool_error_sec:
            raise ValueError("pool_warn_sec must be <= pool_error_sec")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "price_warn_sec": self.price_warn_sec,
            "price_error_sec": self.price_error_sec,
            "gas_warn_sec": self.gas_warn_sec,
            "gas_error_sec": self.gas_error_sec,
            "pool_warn_sec": self.pool_warn_sec,
            "pool_error_sec": self.pool_error_sec,
            "enabled": self.enabled,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FreshnessConfig":
        """Create from dictionary."""
        return cls(
            price_warn_sec=data.get("price_warn_sec", 30.0),
            price_error_sec=data.get("price_error_sec", 300.0),
            gas_warn_sec=data.get("gas_warn_sec", 30.0),
            gas_error_sec=data.get("gas_error_sec", 300.0),
            pool_warn_sec=data.get("pool_warn_sec", 30.0),
            pool_error_sec=data.get("pool_error_sec", 300.0),
            enabled=data.get("enabled", True),
        )


if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


def _price_oracle_supports_chain_kwarg(get_aggregated_price: Any) -> bool:
    """Return True when ``get_aggregated_price(..., chain=...)`` is supported."""
    try:
        parameters = inspect.signature(get_aggregated_price).parameters.values()
    except (TypeError, ValueError):
        return True

    for parameter in parameters:
        if parameter.kind == inspect.Parameter.VAR_KEYWORD:
            return True
        if parameter.name == "chain":
            return parameter.kind != inspect.Parameter.POSITIONAL_ONLY

    return False


# =============================================================================
# Exceptions
# =============================================================================


# Errors are defined canonically in ``almanak.framework.market.errors``;
# we re-export them here so legacy ``from almanak.framework.data.market_snapshot
# import <Error>`` imports keep working AND ``isinstance`` checks against the
# canonical class succeed (both paths now alias the same class object).
# See VIB-4062.
from ..market.errors import (
    AmbiguousChainError,  # noqa: F401
    BalanceUnavailableError,  # noqa: F401
    ChainNotConfiguredError,  # noqa: F401
    DexQuoteUnavailableError,  # noqa: F401
    FundingRateHistoryUnavailableError,  # noqa: F401
    FundingRateUnavailableError,  # noqa: F401
    GasUnavailableError,  # noqa: F401
    HealthUnavailableError,  # noqa: F401
    ILExposureUnavailableError,  # noqa: F401
    IndicatorUnavailableError,  # noqa: F401
    LendingRateHistoryUnavailableError,  # noqa: F401
    LendingRateUnavailableError,  # noqa: F401
    LiquidityDepthUnavailableError,  # noqa: F401
    LSTDataUnavailableError,  # noqa: F401
    MarketSnapshotError,
    OHLCVUnavailableError,  # noqa: F401
    PoolAnalyticsUnavailableError,  # noqa: F401
    PoolHistoryUnavailableError,  # noqa: F401
    PoolPriceUnavailableError,  # noqa: F401
    PoolReservesUnavailableError,  # noqa: F401
    PortfolioRiskUnavailableError,  # noqa: F401
    PredictionMarketNotFoundError,  # noqa: F401
    PredictionUnavailableError,  # noqa: F401
    PriceUnavailableError,  # noqa: F401
    RollingSharpeUnavailableError,  # noqa: F401
    RSIUnavailableError,  # noqa: F401
    SlippageEstimateUnavailableError,  # noqa: F401
    StaleDataError,  # noqa: F401
    VolatilityUnavailableError,  # noqa: F401
    VolConeUnavailableError,  # noqa: F401
    YieldOpportunitiesUnavailableError,  # noqa: F401
)

# =============================================================================
# RSI Calculator Protocol
# =============================================================================


@runtime_checkable
class RSICalculator(Protocol):
    """Protocol for RSI calculators.

    RSI calculators provide the Relative Strength Index indicator
    for a given token and period.
    """

    async def calculate_rsi(self, token: str, period: int = 14, timeframe: str = "4h") -> float:
        """Calculate RSI for a token.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            period: RSI calculation period (default 14)

        Returns:
            RSI value from 0 to 100

        Raises:
            InsufficientDataError: If not enough historical data
            DataSourceError: If data cannot be fetched
        """
        ...


# =============================================================================
# MarketSnapshot Class
# =============================================================================


# =============================================================================
# VIB-4062 — MarketSnapshot class DELETED from this module.
#
# The class definition lives at ``almanak.framework.market.snapshot``. This
# module retains the typed errors (``MarketSnapshotError`` and subclasses)
# and the config dataclasses (``StablecoinConfig``, ``FreshnessConfig``)
# during the cross-codebase deprecation. Importing ``MarketSnapshot`` from
# this module raises ``ImportError`` — see PRD §3.
# =============================================================================


# =============================================================================
# Exports — see PRD §3 for the full breakage table.
# =============================================================================


__all__ = [
    "RSICalculator",
    # Stablecoin config
    "StablecoinConfig",
    "StablecoinMode",
    "DEFAULT_STABLECOINS",
    # Freshness config
    "FreshnessConfig",
    # Exceptions
    "MarketSnapshotError",
    "PriceUnavailableError",
    "BalanceUnavailableError",
    "RSIUnavailableError",
    "OHLCVUnavailableError",
    "GasUnavailableError",
    "PoolPriceUnavailableError",
    "PoolReservesUnavailableError",
    "HealthUnavailableError",
    "LendingRateUnavailableError",
    "FundingRateUnavailableError",
    "DexQuoteUnavailableError",
    "ILExposureUnavailableError",
    "PredictionUnavailableError",
    "LiquidityDepthUnavailableError",
    "SlippageEstimateUnavailableError",
    "VolatilityUnavailableError",
    "VolConeUnavailableError",
    "PoolAnalyticsUnavailableError",
    "PortfolioRiskUnavailableError",
    "RollingSharpeUnavailableError",
    "YieldOpportunitiesUnavailableError",
    "LSTDataUnavailableError",
]
