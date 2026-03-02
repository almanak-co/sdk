"""MarketSnapshot - Unified market data interface for strategies.

This module provides a clean interface for strategies to access market data
without knowing about underlying data providers, caching, or aggregation logic.

The MarketSnapshot wraps PriceOracle, BalanceProvider, and RSI calculators
to provide simple, type-safe methods for strategy decision-making.

Key Features:
    - Simplified synchronous interface (async handled internally)
    - Clear exceptions on failure (not silent defaults)
    - Type-safe Decimal returns for monetary values
    - Integration with existing IntentStrategy.decide() signature

Example:
    from almanak.framework.data.market_snapshot import MarketSnapshot
    from almanak.framework.data.price import PriceAggregator, CoinGeckoPriceSource

    # Create providers
    price_oracle = PriceAggregator(sources=[CoinGeckoPriceSource()])
    balance_provider = Web3BalanceProvider(web3, wallet_address)

    # Create snapshot
    snapshot = MarketSnapshot(
        chain="arbitrum",
        wallet_address="0x...",
        price_oracle=price_oracle,
        balance_provider=balance_provider,
    )

    # Use in strategy
    def decide(self, market: MarketSnapshot) -> Intent:
        eth_price = market.price("WETH")  # Returns Decimal
        usdc_balance = market.balance("USDC")  # Returns Decimal
        rsi = market.rsi("WETH", period=14)  # Returns float 0-100

        if rsi < 30 and usdc_balance > Decimal("100"):
            return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        return Intent.hold()
"""

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal, Optional, Protocol, runtime_checkable

import pandas as pd

from .interfaces import (
    AllDataSourcesFailed,
    BalanceProvider,
    BalanceResult,
    DataSourceError,
    DataSourceUnavailable,
    InsufficientDataError,
    PriceOracle,
    PriceResult,
    StaleDataError,
)

# =============================================================================
# Stablecoin Configuration
# =============================================================================

# Type for stablecoin pricing mode
StablecoinMode = Literal["market", "pegged", "hybrid"]

# Default stablecoins that are commonly used in DeFi
DEFAULT_STABLECOINS: frozenset[str] = frozenset({"USDC", "USDT", "DAI"})


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
    from almanak.framework.gateway_client import GatewayClient

    from .defi.gas import GasOracle, GasPrice
    from .defi.pools import PoolReserves, UniswapV3PoolReader
    from .funding import FundingRate, FundingRateProvider, FundingRateSpread
    from .health import HealthReport
    from .indicators.atr import ATRCalculator
    from .indicators.base import BollingerBandsResult, MACDResult, StochasticResult
    from .indicators.bollinger_bands import BollingerBandsCalculator
    from .indicators.macd import MACDCalculator
    from .indicators.moving_averages import MovingAverageCalculator
    from .indicators.stochastic import StochasticCalculator
    from .lp import ILCalculator, ILExposure, ProjectedILResult
    from .models import DataEnvelope, Instrument
    from .ohlcv.module import GapStrategy, OHLCVModule
    from .ohlcv.ohlcv_router import OHLCVRouter
    from .pools.aggregation import AggregatedPrice, PriceAggregator
    from .pools.analytics import PoolAnalytics, PoolAnalyticsReader, PoolAnalyticsResult
    from .pools.history import PoolHistoryReader, PoolSnapshot
    from .pools.liquidity import LiquidityDepth, LiquidityDepthReader, SlippageEstimate, SlippageEstimator
    from .pools.reader import PoolPrice, PoolReaderRegistry
    from .position_health import PositionHealth, PTPositionHealth
    from .prediction_provider import (
        PredictionMarket,
        PredictionMarketDataProvider,
        PredictionOrder,
        PredictionPosition,
    )
    from .price.multi_dex import (
        BestDexResult,
        MultiDexPriceResult,
        MultiDexPriceService,
    )
    from .rates import BestRateResult, LendingRate, RateMonitor
    from .rates.history import FundingRateSnapshot, LendingRateSnapshot, RateHistoryReader
    from .risk.metrics import PortfolioRisk, PortfolioRiskCalculator, RollingSharpeResult
    from .routing.router import DataRouter
    from .volatility.realized import RealizedVolatilityCalculator, VolatilityResult, VolConeResult
    from .wallet_activity import WalletActivityProvider
    from .yields.aggregator import YieldAggregator, YieldOpportunity

logger = logging.getLogger(__name__)


# =============================================================================
# Exceptions
# =============================================================================


class MarketSnapshotError(Exception):
    """Base exception for MarketSnapshot errors."""

    pass


class PriceUnavailableError(MarketSnapshotError):
    """Raised when price data cannot be retrieved for a token."""

    def __init__(self, token: str, reason: str) -> None:
        self.token = token
        self.reason = reason
        super().__init__(f"Price unavailable for {token}: {reason}")


class BalanceUnavailableError(MarketSnapshotError):
    """Raised when balance data cannot be retrieved for a token."""

    def __init__(self, token: str, reason: str) -> None:
        self.token = token
        self.reason = reason
        super().__init__(f"Balance unavailable for {token}: {reason}")


class RSIUnavailableError(MarketSnapshotError):
    """Raised when RSI cannot be calculated for a token."""

    def __init__(self, token: str, reason: str) -> None:
        self.token = token
        self.reason = reason
        super().__init__(f"RSI unavailable for {token}: {reason}")


class OHLCVUnavailableError(MarketSnapshotError):
    """Raised when OHLCV data cannot be retrieved for a token."""

    def __init__(self, token: str, reason: str) -> None:
        self.token = token
        self.reason = reason
        super().__init__(f"OHLCV unavailable for {token}: {reason}")


class GasUnavailableError(MarketSnapshotError):
    """Raised when gas price cannot be retrieved for a chain."""

    def __init__(self, chain: str, reason: str) -> None:
        self.chain = chain
        self.reason = reason
        super().__init__(f"Gas price unavailable for {chain}: {reason}")


class PoolPriceUnavailableError(MarketSnapshotError):
    """Raised when on-chain pool price cannot be retrieved."""

    def __init__(self, identifier: str, reason: str) -> None:
        self.identifier = identifier
        self.reason = reason
        super().__init__(f"Pool price unavailable for {identifier}: {reason}")


class PoolReservesUnavailableError(MarketSnapshotError):
    """Raised when pool reserves cannot be retrieved."""

    def __init__(self, pool_address: str, reason: str) -> None:
        self.pool_address = pool_address
        self.reason = reason
        super().__init__(f"Pool reserves unavailable for {pool_address}: {reason}")


class PoolHistoryUnavailableError(MarketSnapshotError):
    """Raised when historical pool data cannot be retrieved."""

    def __init__(self, pool_address: str, reason: str) -> None:
        self.pool_address = pool_address
        self.reason = reason
        super().__init__(f"Pool history unavailable for {pool_address}: {reason}")


class HealthUnavailableError(MarketSnapshotError):
    """Raised when health report cannot be generated."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"Health report unavailable: {reason}")


class LendingRateUnavailableError(MarketSnapshotError):
    """Raised when lending rate cannot be retrieved."""

    def __init__(self, protocol: str, token: str, side: str, reason: str) -> None:
        self.protocol = protocol
        self.token = token
        self.side = side
        self.reason = reason
        super().__init__(f"Lending rate unavailable for {protocol}/{token}/{side}: {reason}")


class FundingRateUnavailableError(MarketSnapshotError):
    """Raised when funding rate cannot be retrieved."""

    def __init__(self, venue: str, market: str, reason: str) -> None:
        self.venue = venue
        self.market = market
        self.reason = reason
        super().__init__(f"Funding rate unavailable for {venue}/{market}: {reason}")


class DexQuoteUnavailableError(MarketSnapshotError):
    """Raised when DEX quote cannot be retrieved."""

    def __init__(self, token_in: str, token_out: str, reason: str) -> None:
        self.token_in = token_in
        self.token_out = token_out
        self.reason = reason
        super().__init__(f"DEX quote unavailable for {token_in}->{token_out}: {reason}")


class ILExposureUnavailableError(MarketSnapshotError):
    """Raised when IL exposure cannot be calculated for a position."""

    def __init__(self, position_id: str, reason: str) -> None:
        self.position_id = position_id
        self.reason = reason
        super().__init__(f"IL exposure unavailable for {position_id}: {reason}")


class PredictionUnavailableError(MarketSnapshotError):
    """Raised when prediction market data cannot be retrieved."""

    def __init__(self, market_id: str, reason: str) -> None:
        self.market_id = market_id
        self.reason = reason
        super().__init__(f"Prediction market data unavailable for {market_id}: {reason}")


class LendingRateHistoryUnavailableError(MarketSnapshotError):
    """Raised when historical lending rate data cannot be retrieved."""

    def __init__(self, protocol: str, token: str, reason: str) -> None:
        self.protocol = protocol
        self.token = token
        self.reason = reason
        super().__init__(f"Lending rate history unavailable for {protocol}/{token}: {reason}")


class FundingRateHistoryUnavailableError(MarketSnapshotError):
    """Raised when historical funding rate data cannot be retrieved."""

    def __init__(self, venue: str, market: str, reason: str) -> None:
        self.venue = venue
        self.market = market
        self.reason = reason
        super().__init__(f"Funding rate history unavailable for {venue}/{market}: {reason}")


class LiquidityDepthUnavailableError(MarketSnapshotError):
    """Raised when liquidity depth data cannot be retrieved."""

    def __init__(self, identifier: str, reason: str) -> None:
        self.identifier = identifier
        self.reason = reason
        super().__init__(f"Liquidity depth unavailable for {identifier}: {reason}")


class SlippageEstimateUnavailableError(MarketSnapshotError):
    """Raised when slippage estimation cannot be performed."""

    def __init__(self, pair: str, reason: str) -> None:
        self.pair = pair
        self.reason = reason
        super().__init__(f"Slippage estimate unavailable for {pair}: {reason}")


class VolatilityUnavailableError(MarketSnapshotError):
    """Raised when realized volatility cannot be calculated."""

    def __init__(self, token: str, reason: str) -> None:
        self.token = token
        self.reason = reason
        super().__init__(f"Volatility unavailable for {token}: {reason}")


class VolConeUnavailableError(MarketSnapshotError):
    """Raised when volatility cone cannot be calculated."""

    def __init__(self, token: str, reason: str) -> None:
        self.token = token
        self.reason = reason
        super().__init__(f"Vol cone unavailable for {token}: {reason}")


class PortfolioRiskUnavailableError(MarketSnapshotError):
    """Raised when portfolio risk metrics cannot be calculated."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"Portfolio risk unavailable: {reason}")


class RollingSharpeUnavailableError(MarketSnapshotError):
    """Raised when rolling Sharpe ratio cannot be calculated."""

    def __init__(self, reason: str) -> None:
        self.reason = reason
        super().__init__(f"Rolling Sharpe unavailable: {reason}")


class PoolAnalyticsUnavailableError(MarketSnapshotError):
    """Raised when pool analytics cannot be retrieved."""

    def __init__(self, identifier: str, reason: str) -> None:
        self.identifier = identifier
        self.reason = reason
        super().__init__(f"Pool analytics unavailable for {identifier}: {reason}")


class YieldOpportunitiesUnavailableError(MarketSnapshotError):
    """Raised when yield opportunities cannot be retrieved."""

    def __init__(self, token: str, reason: str) -> None:
        self.token = token
        self.reason = reason
        super().__init__(f"Yield opportunities unavailable for {token}: {reason}")


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


class MarketSnapshot:
    """Unified market data interface for strategy decision-making.

    MarketSnapshot provides a clean, synchronous interface for strategies to
    access market data. It wraps underlying async data providers and handles
    event loop management internally.

    All methods raise clear exceptions on failure - there are no silent defaults
    or fallback values. Strategies should handle exceptions appropriately.

    Attributes:
        chain: Blockchain network (e.g., "arbitrum", "ethereum")
        wallet_address: Address of the wallet for balance queries
        timestamp: When the snapshot was created

    Example:
        # Create snapshot with all providers
        snapshot = MarketSnapshot(
            chain="arbitrum",
            wallet_address="0x...",
            price_oracle=price_aggregator,
            balance_provider=web3_balance_provider,
            rsi_calculator=rsi_calculator,
        )

        # Get aggregated price
        eth_price = snapshot.price("WETH")  # Decimal("2500.50")

        # Get wallet balance
        usdc_balance = snapshot.balance("USDC")  # Decimal("1000.00")

        # Get RSI indicator
        rsi = snapshot.rsi("WETH", period=14)  # 45.5

        # Get balance in USD terms
        usdc_value = snapshot.balance_usd("USDC")  # Decimal("1000.00")

        # Get total portfolio value
        total = snapshot.total_portfolio_usd(["WETH", "USDC", "ARB"])
    """

    def __init__(
        self,
        chain: str,
        wallet_address: str,
        price_oracle: PriceOracle | None = None,
        balance_provider: BalanceProvider | None = None,
        rsi_calculator: RSICalculator | None = None,
        ohlcv_module: Optional["OHLCVModule"] = None,
        gas_oracle: Optional["GasOracle"] = None,
        pool_reader: Optional["UniswapV3PoolReader"] = None,
        rate_monitor: Optional["RateMonitor"] = None,
        funding_rate_provider: Optional["FundingRateProvider"] = None,
        multi_dex_service: Optional["MultiDexPriceService"] = None,
        il_calculator: Optional["ILCalculator"] = None,
        prediction_provider: Optional["PredictionMarketDataProvider"] = None,
        stablecoin_config: StablecoinConfig | None = None,
        freshness_config: FreshnessConfig | None = None,
        timestamp: datetime | None = None,
        pool_reader_registry: Optional["PoolReaderRegistry"] = None,
        price_aggregator: Optional["PriceAggregator"] = None,
        data_router: Optional["DataRouter"] = None,
        ohlcv_router: Optional["OHLCVRouter"] = None,
        pool_history_reader: Optional["PoolHistoryReader"] = None,
        rate_history_reader: Optional["RateHistoryReader"] = None,
        liquidity_depth_reader: Optional["LiquidityDepthReader"] = None,
        slippage_estimator: Optional["SlippageEstimator"] = None,
        volatility_calculator: Optional["RealizedVolatilityCalculator"] = None,
        risk_calculator: Optional["PortfolioRiskCalculator"] = None,
        pool_analytics_reader: Optional["PoolAnalyticsReader"] = None,
        yield_aggregator: Optional["YieldAggregator"] = None,
        wallet_activity_provider: Optional["WalletActivityProvider"] = None,
        gateway_client: Optional["GatewayClient"] = None,
    ) -> None:
        """Initialize the MarketSnapshot.

        Args:
            chain: Blockchain network name (e.g., "arbitrum", "ethereum")
            wallet_address: Wallet address for balance queries
            price_oracle: PriceOracle implementation for price data
            balance_provider: BalanceProvider implementation for balance data
            rsi_calculator: RSICalculator implementation for RSI indicator
            ohlcv_module: OHLCVModule for historical candlestick data
            gas_oracle: GasOracle implementation for gas price data
            pool_reader: UniswapV3PoolReader for DEX pool data
            rate_monitor: RateMonitor for lending protocol rates
            funding_rate_provider: FundingRateProvider for perpetual funding rates
            multi_dex_service: MultiDexPriceService for cross-DEX price comparison
            il_calculator: ILCalculator for impermanent loss calculations
            prediction_provider: PredictionMarketDataProvider for prediction market data
            stablecoin_config: Configuration for stablecoin pricing behavior.
                Default is StablecoinConfig(mode='market') which uses actual market prices.
            freshness_config: Configuration for data freshness thresholds.
                Default is FreshnessConfig(price_warn_sec=30, price_error_sec=300).
            timestamp: Optional snapshot timestamp (defaults to now)
            pool_reader_registry: PoolReaderRegistry for on-chain pool price reads
            price_aggregator: PriceAggregator for TWAP/LWAP aggregation
            data_router: DataRouter for provider selection and failover
            ohlcv_router: OHLCVRouter for multi-provider OHLCV with CEX/DEX awareness
            pool_history_reader: PoolHistoryReader for historical pool state data
            rate_history_reader: RateHistoryReader for historical lending/funding rate data
            liquidity_depth_reader: LiquidityDepthReader for tick-level liquidity reads
            slippage_estimator: SlippageEstimator for swap slippage estimation
            volatility_calculator: RealizedVolatilityCalculator for vol metrics
            risk_calculator: PortfolioRiskCalculator for portfolio risk metrics
            pool_analytics_reader: PoolAnalyticsReader for pool TVL, volume, fee APR
            yield_aggregator: YieldAggregator for cross-protocol yield comparison
        """
        self._chain = chain
        self._wallet_address = wallet_address
        self._price_oracle = price_oracle
        self._balance_provider = balance_provider
        self._rsi_calculator = rsi_calculator
        self._ohlcv_module = ohlcv_module
        self._gas_oracle = gas_oracle
        self._pool_reader = pool_reader
        self._rate_monitor = rate_monitor
        self._funding_rate_provider = funding_rate_provider
        self._multi_dex_service = multi_dex_service
        self._il_calculator = il_calculator
        self._prediction_provider = prediction_provider
        self._stablecoin_config = stablecoin_config or StablecoinConfig()
        self._freshness_config = freshness_config or FreshnessConfig()
        self._timestamp = timestamp or datetime.now(UTC)
        self._pool_reader_registry = pool_reader_registry
        self._price_aggregator = price_aggregator
        self._data_router = data_router
        self._ohlcv_router = ohlcv_router
        self._pool_history_reader = pool_history_reader
        self._rate_history_reader = rate_history_reader
        self._liquidity_depth_reader = liquidity_depth_reader
        self._slippage_estimator = slippage_estimator
        self._volatility_calculator = volatility_calculator
        self._risk_calculator = risk_calculator
        self._pool_analytics_reader = pool_analytics_reader
        self._yield_aggregator = yield_aggregator
        self._wallet_activity_provider = wallet_activity_provider
        self._gateway_client = gateway_client

        # Internal caches to avoid redundant async calls within same snapshot
        self._price_cache: dict[str, Decimal] = {}
        self._balance_cache: dict[str, Decimal] = {}
        self._rsi_cache: dict[str, float] = {}
        self._sma_cache: dict[str, float] = {}
        self._ema_cache: dict[str, float] = {}
        self._bollinger_cache: dict[str, BollingerBandsResult] = {}
        self._macd_cache: dict[str, MACDResult] = {}
        self._stochastic_cache: dict[str, StochasticResult] = {}
        self._atr_cache: dict[str, float] = {}

        # Lazy-initialized indicator calculators
        self._ma_calculator: MovingAverageCalculator | None = None
        self._bollinger_calculator: BollingerBandsCalculator | None = None
        self._macd_calculator: MACDCalculator | None = None
        self._stochastic_calculator: StochasticCalculator | None = None
        self._atr_calculator: ATRCalculator | None = None

        # Gas price cache with timestamp for TTL (12 seconds = 1 block)
        self._gas_cache: dict[str, tuple[GasPrice, datetime]] = {}
        self._gas_cache_ttl_seconds: float = 12.0

        # Pool reserves cache with timestamp for TTL (12 seconds = 1 block)
        self._pool_cache: dict[str, tuple[PoolReserves, datetime]] = {}
        self._pool_cache_ttl_seconds: float = 12.0

        # Funding rate cache with timestamp for TTL (60 seconds)
        self._funding_rate_cache: dict[str, tuple[FundingRate, datetime]] = {}
        self._funding_rate_cache_ttl_seconds: float = 60.0

        logger.debug(
            "Created MarketSnapshot for chain=%s, wallet=%s...",
            chain,
            wallet_address[:10] if wallet_address else "None",
        )

    @property
    def chain(self) -> str:
        """Get the blockchain network name."""
        return self._chain

    @property
    def wallet_address(self) -> str:
        """Get the wallet address."""
        return self._wallet_address

    @property
    def timestamp(self) -> datetime:
        """Get the snapshot creation timestamp."""
        return self._timestamp

    def wallet_activity(
        self,
        leader_address: str | None = None,
        action_types: list[str] | None = None,
        min_usd_value: Decimal | None = None,
        protocols: list[str] | None = None,
    ) -> list:
        """Get leader wallet activity signals for copy trading.

        Returns filtered signals from the WalletActivityProvider. If no
        provider is configured, returns an empty list (graceful degradation).

        Args:
            leader_address: Filter by specific leader wallet address
            action_types: Filter by action types (e.g., ["SWAP"])
            min_usd_value: Minimum USD value filter
            protocols: Filter by protocol names (e.g., ["uniswap_v3"])

        Returns:
            List of CopySignal objects matching the filters
        """
        if self._wallet_activity_provider is None:
            return []
        return self._wallet_activity_provider.get_signals(
            action_types=action_types,
            protocols=protocols,
            min_usd_value=min_usd_value,
            leader_address=leader_address,
        )

    def _run_async(self, coro: Any) -> Any:
        """Run an async coroutine synchronously.

        Handles event loop creation/reuse for async-to-sync bridge.

        Args:
            coro: Coroutine to run

        Returns:
            Result from the coroutine
        """
        try:
            # Check if there's an existing event loop
            loop = asyncio.get_running_loop()
        except RuntimeError:
            # No running loop, create new one
            loop = None

        if loop is not None:
            # We're inside an async context - need to create a nested loop
            # This handles the case where MarketSnapshot is used inside async code
            import nest_asyncio

            try:
                nest_asyncio.apply()
                return asyncio.get_event_loop().run_until_complete(coro)
            except ImportError:
                # nest_asyncio not available, try using asyncio.run in new thread
                import concurrent.futures

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(asyncio.run, coro)
                    return future.result()
        else:
            # No running loop - simple case
            return asyncio.run(coro)

    def _check_freshness(
        self,
        age_seconds: float,
        source: str,
        data_type: str,
        warn_threshold: float,
        error_threshold: float,
    ) -> None:
        """Check data freshness and handle warnings/errors.

        This method checks the age of data against configured thresholds and
        either logs a warning or raises an error based on staleness.

        Args:
            age_seconds: How old the data is in seconds
            source: Name of the data source for error messages
            data_type: Type of data (e.g., "price", "gas", "pool")
            warn_threshold: Seconds threshold for warning
            error_threshold: Seconds threshold for error

        Raises:
            StaleDataError: If age exceeds the error threshold
        """
        import warnings

        if not self._freshness_config.enabled:
            return

        if age_seconds >= error_threshold:
            raise StaleDataError(
                source=source,
                age_seconds=age_seconds,
                threshold_seconds=error_threshold,
                data_type=data_type,
            )

        if age_seconds >= warn_threshold:
            # Log the warning but don't raise
            warning_msg = f"{data_type.capitalize()} from '{source}' is stale: {age_seconds:.1f}s old (warn threshold: {warn_threshold:.1f}s)"
            # Issue as Python warning so it can be caught by warning filters
            warnings.warn(warning_msg, UserWarning, stacklevel=2)
            logger.warning(warning_msg)

    def price(self, token: str, quote: str = "USD") -> Decimal:
        """Get the aggregated price for a token.

        Fetches the price from the configured PriceOracle, which may
        aggregate prices from multiple sources.

        For stablecoins, the behavior depends on the configured StablecoinConfig:
        - mode='market' (default): Returns actual market price
        - mode='pegged': Returns Decimal('1.00') for configured stablecoins
        - mode='hybrid': Returns $1.00 if within tolerance, else market price

        Args:
            token: Token symbol (e.g., "WETH", "ETH", "USDC")
            quote: Quote currency (default "USD")

        Returns:
            Price as a Decimal for precision

        Raises:
            PriceUnavailableError: If price cannot be determined
            ValueError: If no price oracle is configured

        Example:
            eth_price = snapshot.price("WETH")
            # Returns: Decimal("2500.50")

            # With mode='pegged'
            usdc_price = snapshot.price("USDC")
            # Returns: Decimal("1.00")
        """
        cache_key = f"{token}/{quote}"

        # Return cached value if available
        if cache_key in self._price_cache:
            return self._price_cache[cache_key]

        # Handle pegged mode for stablecoins (no oracle call needed)
        if quote == "USD" and self._stablecoin_config.mode == "pegged" and self._stablecoin_config.is_stablecoin(token):
            pegged_price = Decimal("1.00")
            self._price_cache[cache_key] = pegged_price
            return pegged_price

        if self._price_oracle is None:
            raise ValueError("No price oracle configured for MarketSnapshot")

        try:
            result: PriceResult = self._run_async(self._price_oracle.get_aggregated_price(token, quote))
            market_price = result.price

            # Apply stablecoin config logic for hybrid mode
            if quote == "USD" and self._stablecoin_config.should_use_peg(token, market_price):
                final_price = Decimal("1.00")
            else:
                final_price = market_price

            self._price_cache[cache_key] = final_price
            return final_price
        except AllDataSourcesFailed as e:
            raise PriceUnavailableError(token, f"All data sources failed: {e.errors}") from e
        except DataSourceUnavailable as e:
            raise PriceUnavailableError(token, e.reason) from e
        except DataSourceError as e:
            raise PriceUnavailableError(token, str(e)) from e
        except Exception as e:
            raise PriceUnavailableError(token, f"Unexpected error: {e}") from e

    def balance(self, token: str) -> Decimal:
        """Get the wallet balance for a token.

        Queries the balance from the configured BalanceProvider.

        Args:
            token: Token symbol (e.g., "WETH", "USDC") or "ETH" for native

        Returns:
            Balance as a Decimal in human-readable units (not wei)

        Raises:
            BalanceUnavailableError: If balance cannot be determined
            ValueError: If no balance provider is configured

        Example:
            usdc_balance = snapshot.balance("USDC")
            # Returns: Decimal("1000.50")
        """
        # Return cached value if available
        if token in self._balance_cache:
            return self._balance_cache[token]

        if self._balance_provider is None:
            raise ValueError("No balance provider configured for MarketSnapshot")

        try:
            result: BalanceResult = self._run_async(self._balance_provider.get_balance(token))
            self._balance_cache[token] = result.balance
            return result.balance
        except DataSourceError as e:
            raise BalanceUnavailableError(token, str(e)) from e
        except Exception as e:
            raise BalanceUnavailableError(token, f"Unexpected error: {e}") from e

    def rsi(self, token: str, period: int = 14, timeframe: str = "4h") -> float:
        """Get the RSI (Relative Strength Index) for a token.

        Calculates RSI using the configured RSI calculator with
        historical price data.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            period: RSI calculation period (default 14)
            timeframe: OHLCV candle timeframe (default "4h")
                Supported: "1m", "5m", "15m", "1h", "4h", "1d"
                Note: 1m/5m/15m may return 30-min candles (CoinGecko limitation)

        Returns:
            RSI value from 0 to 100 as a float

        Raises:
            RSIUnavailableError: If RSI cannot be calculated
            ValueError: If no RSI calculator is configured

        Example:
            # Default 4-hour candles
            rsi = snapshot.rsi("WETH", period=14)

            # 1-hour candles for shorter-term analysis
            rsi_1h = snapshot.rsi("WETH", period=14, timeframe="1h")

            # Daily candles for longer-term analysis
            rsi_1d = snapshot.rsi("WETH", period=14, timeframe="1d")

            # Multi-timeframe analysis
            if snapshot.rsi("WETH", timeframe="1h") < 30 and snapshot.rsi("WETH", timeframe="1d") < 50:
                # Short-term oversold, long-term not overbought
                return SwapIntent(...)
        """
        cache_key = f"{token}:{period}:{timeframe}"

        # Return cached value if available
        if cache_key in self._rsi_cache:
            return self._rsi_cache[cache_key]

        if self._rsi_calculator is None:
            raise ValueError("No RSI calculator configured for MarketSnapshot")

        try:
            rsi_value = self._run_async(self._rsi_calculator.calculate_rsi(token, period, timeframe))
            self._rsi_cache[cache_key] = rsi_value
            return rsi_value
        except InsufficientDataError as e:
            raise RSIUnavailableError(
                token, f"Insufficient historical data: need {e.required}, have {e.available}"
            ) from e
        except DataSourceError as e:
            raise RSIUnavailableError(token, str(e)) from e
        except Exception as e:
            raise RSIUnavailableError(token, f"Unexpected error: {e}") from e

    def _get_ohlcv_provider(self) -> Any:
        """Get OHLCV provider from RSI calculator for indicator calculations."""
        if self._rsi_calculator is not None and hasattr(self._rsi_calculator, "_ohlcv_provider"):
            return self._rsi_calculator._ohlcv_provider
        raise ValueError("No OHLCV provider available. Configure an RSI calculator with an OHLCV provider.")

    def sma(self, token: str, period: int = 20, timeframe: str = "1h") -> float:
        """Get the Simple Moving Average (SMA) for a token.

        SMA is the unweighted mean of the last N closing prices.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            period: Number of periods for the average (default 20)
            timeframe: OHLCV candle timeframe (default "1h")

        Returns:
            SMA value as float

        Example:
            sma_20 = snapshot.sma("WETH", period=20, timeframe="1h")
            sma_200 = snapshot.sma("WETH", period=200, timeframe="1d")

            # Trading logic
            if current_price > sma_20:
                print("Price above 20-period SMA - bullish trend")
        """
        cache_key = f"{token}:{period}:{timeframe}"
        if cache_key in self._sma_cache:
            return self._sma_cache[cache_key]

        if self._ma_calculator is None:
            from .indicators.moving_averages import MovingAverageCalculator

            self._ma_calculator = MovingAverageCalculator(self._get_ohlcv_provider())

        try:
            sma_value = self._run_async(self._ma_calculator.sma(token, period, timeframe))
            self._sma_cache[cache_key] = sma_value
            return sma_value
        except InsufficientDataError as e:
            raise RSIUnavailableError(
                token, f"Insufficient historical data for SMA: need {e.required}, have {e.available}"
            ) from e
        except Exception as e:
            raise RSIUnavailableError(token, f"SMA calculation error: {e}") from e

    def ema(self, token: str, period: int = 12, timeframe: str = "1h", smoothing: float = 2.0) -> float:
        """Get the Exponential Moving Average (EMA) for a token.

        EMA gives more weight to recent prices using exponential decay.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            period: Number of periods (default 12)
            timeframe: OHLCV candle timeframe (default "1h")
            smoothing: Smoothing factor (default 2.0)

        Returns:
            EMA value as float

        Example:
            ema_12 = snapshot.ema("WETH", period=12, timeframe="1h")
            ema_26 = snapshot.ema("WETH", period=26, timeframe="1h")

            # Golden cross check
            if ema_12 > ema_26:
                print("Golden cross - bullish signal")
        """
        cache_key = f"{token}:{period}:{timeframe}:{smoothing}"
        if cache_key in self._ema_cache:
            return self._ema_cache[cache_key]

        if self._ma_calculator is None:
            from .indicators.moving_averages import MovingAverageCalculator

            self._ma_calculator = MovingAverageCalculator(self._get_ohlcv_provider())

        try:
            ema_value = self._run_async(self._ma_calculator.ema(token, period, timeframe, smoothing))
            self._ema_cache[cache_key] = ema_value
            return ema_value
        except InsufficientDataError as e:
            raise RSIUnavailableError(
                token, f"Insufficient historical data for EMA: need {e.required}, have {e.available}"
            ) from e
        except Exception as e:
            raise RSIUnavailableError(token, f"EMA calculation error: {e}") from e

    def bollinger_bands(
        self, token: str, period: int = 20, std_dev: float = 2.0, timeframe: str = "1h"
    ) -> "BollingerBandsResult":
        """Get Bollinger Bands for a token.

        Bollinger Bands consist of a middle band (SMA) with upper and lower bands
        at a specified number of standard deviations away.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            period: SMA period (default 20)
            std_dev: Standard deviation multiplier (default 2.0)
            timeframe: OHLCV candle timeframe (default "1h")

        Returns:
            BollingerBandsResult with:
                - upper_band: Upper band value
                - middle_band: Middle band (SMA) value
                - lower_band: Lower band value
                - bandwidth: Band width as percentage
                - percent_b: Price position (0=lower, 1=upper)

        Example:
            bb = snapshot.bollinger_bands("WETH", period=20, std_dev=2.0, timeframe="1h")

            if bb.percent_b < 0:
                print("Price below lower band - oversold!")
            elif bb.percent_b > 1:
                print("Price above upper band - overbought!")

            if bb.bandwidth < 0.05:
                print("Low volatility - squeeze detected")
        """
        cache_key = f"{token}:{period}:{std_dev}:{timeframe}"
        if cache_key in self._bollinger_cache:
            return self._bollinger_cache[cache_key]

        if self._bollinger_calculator is None:
            from .indicators.bollinger_bands import BollingerBandsCalculator

            self._bollinger_calculator = BollingerBandsCalculator(self._get_ohlcv_provider())

        try:
            bb = self._run_async(
                self._bollinger_calculator.calculate_bollinger_bands(token, period, std_dev, timeframe)
            )
            self._bollinger_cache[cache_key] = bb
            return bb
        except InsufficientDataError as e:
            raise RSIUnavailableError(
                token, f"Insufficient historical data for Bollinger Bands: need {e.required}, have {e.available}"
            ) from e
        except Exception as e:
            raise RSIUnavailableError(token, f"Bollinger Bands calculation error: {e}") from e

    def macd(
        self,
        token: str,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
        timeframe: str = "1h",
    ) -> "MACDResult":
        """Get MACD (Moving Average Convergence Divergence) for a token.

        MACD is a trend-following momentum indicator showing the relationship
        between two exponential moving averages.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            fast_period: Fast EMA period (default 12)
            slow_period: Slow EMA period (default 26)
            signal_period: Signal line EMA period (default 9)
            timeframe: OHLCV candle timeframe (default "1h")

        Returns:
            MACDResult with:
                - macd_line: MACD line (fast EMA - slow EMA)
                - signal_line: Signal line (EMA of MACD line)
                - histogram: MACD histogram (macd_line - signal_line)

        Example:
            macd = snapshot.macd("WETH", timeframe="4h")

            if macd.histogram > 0:
                print("MACD above signal - bullish momentum")
            elif macd.histogram < 0:
                print("MACD below signal - bearish momentum")
        """
        cache_key = f"{token}:{fast_period}:{slow_period}:{signal_period}:{timeframe}"
        if cache_key in self._macd_cache:
            return self._macd_cache[cache_key]

        if self._macd_calculator is None:
            from .indicators.macd import MACDCalculator

            self._macd_calculator = MACDCalculator(self._get_ohlcv_provider())

        try:
            macd_result = self._run_async(
                self._macd_calculator.calculate_macd(token, fast_period, slow_period, signal_period, timeframe)
            )
            self._macd_cache[cache_key] = macd_result
            return macd_result
        except InsufficientDataError as e:
            raise RSIUnavailableError(
                token, f"Insufficient historical data for MACD: need {e.required}, have {e.available}"
            ) from e
        except Exception as e:
            raise RSIUnavailableError(token, f"MACD calculation error: {e}") from e

    def stochastic(
        self,
        token: str,
        k_period: int = 14,
        d_period: int = 3,
        timeframe: str = "1h",
    ) -> "StochasticResult":
        """Get Stochastic Oscillator for a token.

        The Stochastic Oscillator is a momentum indicator comparing a token's
        closing price to its price range over a given period.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            k_period: Lookback period for %K (default 14)
            d_period: SMA period for %D (default 3)
            timeframe: OHLCV candle timeframe (default "1h")

        Returns:
            StochasticResult with:
                - k_value: %K (fast stochastic, 0-100 scale)
                - d_value: %D (slow stochastic, 0-100 scale)

        Example:
            stoch = snapshot.stochastic("WETH", k_period=14, d_period=3)

            if stoch.k_value < 20:
                print("Oversold territory")
            elif stoch.k_value > 80:
                print("Overbought territory")

            # Crossover signals
            if stoch.k_value > stoch.d_value:
                print("Bullish - %K crossed above %D")
        """
        cache_key = f"{token}:{k_period}:{d_period}:{timeframe}"
        if cache_key in self._stochastic_cache:
            return self._stochastic_cache[cache_key]

        if self._stochastic_calculator is None:
            from .indicators.stochastic import StochasticCalculator

            self._stochastic_calculator = StochasticCalculator(self._get_ohlcv_provider())

        try:
            stoch = self._run_async(
                self._stochastic_calculator.calculate_stochastic(token, k_period, d_period, timeframe)
            )
            self._stochastic_cache[cache_key] = stoch
            return stoch
        except InsufficientDataError as e:
            raise RSIUnavailableError(
                token, f"Insufficient historical data for Stochastic: need {e.required}, have {e.available}"
            ) from e
        except Exception as e:
            raise RSIUnavailableError(token, f"Stochastic calculation error: {e}") from e

    def atr(self, token: str, period: int = 14, timeframe: str = "1h") -> float:
        """Get Average True Range (ATR) for a token.

        ATR is a volatility indicator showing how much an asset moves on average.

        Args:
            token: Token symbol (e.g., "WETH", "ETH")
            period: ATR period (default 14)
            timeframe: OHLCV candle timeframe (default "1h")

        Returns:
            ATR value (in the same units as the token price)

        Example:
            atr = snapshot.atr("WETH", period=14, timeframe="4h")
            current_price = float(snapshot.price("WETH"))

            # Stop-loss placement
            stop_loss = current_price - (2 * atr)
            print(f"Stop loss at ${stop_loss:.2f} (2 ATR below)")

            # Position sizing with 1% risk
            risk_amount = 10000 * 0.01  # $100
            position_size = risk_amount / atr
            print(f"Position size: {position_size:.4f} units")
        """
        cache_key = f"{token}:{period}:{timeframe}"
        if cache_key in self._atr_cache:
            return self._atr_cache[cache_key]

        if self._atr_calculator is None:
            from .indicators.atr import ATRCalculator

            self._atr_calculator = ATRCalculator(self._get_ohlcv_provider())

        try:
            atr_value = self._run_async(self._atr_calculator.calculate_atr(token, period, timeframe))
            self._atr_cache[cache_key] = atr_value
            return atr_value
        except InsufficientDataError as e:
            raise RSIUnavailableError(
                token, f"Insufficient historical data for ATR: need {e.required}, have {e.available}"
            ) from e
        except Exception as e:
            raise RSIUnavailableError(token, f"ATR calculation error: {e}") from e

    def ohlcv(
        self,
        token: "str | Instrument",
        timeframe: str = "1h",
        limit: int = 100,
        quote: str = "USD",
        gap_strategy: "GapStrategy" = "nan",
        *,
        pool_address: str | None = None,
    ) -> pd.DataFrame:
        """Get OHLCV (candlestick) data for a token.

        Fetches historical candlestick data from the configured OHLCV providers.
        When an OHLCVRouter is configured, automatically classifies instruments
        as CEX-primary or DeFi-primary and routes to the appropriate provider.

        Accepts plain token symbols (e.g. "WETH"), pair strings (e.g.
        "WETH/USDC"), or Instrument objects.

        Args:
            token: Token symbol, "BASE/QUOTE" string, or Instrument.
            timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d). Default "1h".
            limit: Maximum number of candles to return. Default 100.
            quote: Quote currency (default "USD")
            gap_strategy: How to handle gaps in data:
                - 'nan': Fill gaps with NaN values (default)
                - 'ffill': Forward-fill gaps with last known values
                - 'drop': Remove gaps (returns only continuous data)
            pool_address: Explicit pool address for DEX providers (optional).

        Returns:
            pandas DataFrame with columns:
                - timestamp: datetime
                - open: float64
                - high: float64
                - low: float64
                - close: float64
                - volume: float64 (may contain NaN if unavailable)

            DataFrame.attrs includes metadata:
                - base: Token symbol
                - quote: Quote currency
                - timeframe: Candle timeframe
                - source: Provider source name
                - chain: Chain identifier
                - fetched_at: When the data was fetched
                - confidence: Data confidence (0.0-1.0)

            Returns empty DataFrame with correct schema if no data available.

        Raises:
            OHLCVUnavailableError: If OHLCV data cannot be retrieved
            ValueError: If no OHLCV module/router is configured or invalid timeframe

        Example:
            # Get 1-hour candles for WETH
            df = snapshot.ohlcv("WETH", timeframe="1h", limit=100)
            print(df.columns)  # timestamp, open, high, low, close, volume

            # Use Instrument for explicit routing
            from almanak.framework.data.models import Instrument
            inst = Instrument(base="WETH", quote="USDC", chain="arbitrum")
            df = snapshot.ohlcv(inst, timeframe="1h")

            # Use with pandas-ta for indicators
            import pandas_ta as ta
            df['rsi'] = ta.rsi(df['close'], length=14)
            df['macd'] = ta.macd(df['close'])['MACD_12_26_9']

            # Handle gaps with forward-fill
            df = snapshot.ohlcv("WETH", gap_strategy="ffill")
        """
        # Route through OHLCVRouter when available
        if self._ohlcv_router is not None:
            return self._ohlcv_via_router(
                token=token,
                timeframe=timeframe,
                limit=limit,
                quote=quote,
                gap_strategy=gap_strategy,
                pool_address=pool_address,
            )

        # Fall back to legacy OHLCVModule path
        if self._ohlcv_module is None:
            raise ValueError("No OHLCV module or router configured for MarketSnapshot")

        # Legacy path only supports string tokens
        token_str = token if isinstance(token, str) else token.base

        try:
            # OHLCVModule.get_ohlcv is already sync (handles async internally)
            df = self._ohlcv_module.get_ohlcv(
                token=token_str,
                timeframe=timeframe,
                limit=limit,
                quote=quote,
                gap_strategy=gap_strategy,
            )
            return df
        except ValueError:
            # Re-raise ValueError for invalid timeframe
            raise
        except DataSourceError as e:
            raise OHLCVUnavailableError(token_str, str(e)) from e
        except Exception as e:
            raise OHLCVUnavailableError(token_str, f"Unexpected error: {e}") from e

    def _ohlcv_via_router(
        self,
        token: "str | Instrument",
        timeframe: str,
        limit: int,
        quote: str,
        gap_strategy: "GapStrategy",
        pool_address: str | None,
    ) -> pd.DataFrame:
        """Fetch OHLCV via OHLCVRouter and convert to DataFrame.

        Internal helper that routes through the multi-provider OHLCVRouter,
        then converts the candle list to a pandas DataFrame matching the
        existing ohlcv() return format.
        """
        from .models import Instrument as InstrumentCls

        token_str = token if isinstance(token, str) else token.pair

        try:
            if self._ohlcv_router is None:
                raise OHLCVUnavailableError(token_str, "No OHLCV router configured")
            envelope = self._ohlcv_router.get_ohlcv(
                token=token,
                chain=self._chain,
                timeframe=timeframe,
                limit=limit,
                pool_address=pool_address,
                quote=quote,
            )
        except DataSourceError as e:
            raise OHLCVUnavailableError(token_str, str(e)) from e
        except Exception as e:
            raise OHLCVUnavailableError(token_str, f"Unexpected error: {e}") from e

        candles = envelope.value
        if not candles:
            # Return empty DataFrame with correct schema
            df = pd.DataFrame(columns=["timestamp", "open", "high", "low", "close", "volume"])
            df.attrs = {
                "base": token_str,
                "quote": quote,
                "timeframe": timeframe,
                "source": envelope.meta.source,
                "chain": self._chain,
                "fetched_at": datetime.now(UTC).isoformat(),
                "confidence": envelope.meta.confidence,
            }
            return df

        # Convert candles to DataFrame
        rows = []
        for c in candles:
            rows.append(
                {
                    "timestamp": c.timestamp,
                    "open": float(c.open),
                    "high": float(c.high),
                    "low": float(c.low),
                    "close": float(c.close),
                    "volume": float(c.volume) if c.volume is not None else float("nan"),
                }
            )
        df = pd.DataFrame(rows)

        # Apply gap strategy
        if gap_strategy == "ffill":
            df = df.ffill()
        elif gap_strategy == "drop":
            df = df.dropna()

        # Resolve base symbol for metadata
        if isinstance(token, InstrumentCls):
            base_sym = token.base
        elif isinstance(token, str) and "/" in token:
            base_sym = token.split("/")[0].strip()
        else:
            base_sym = str(token)

        df.attrs = {
            "base": base_sym,
            "quote": quote,
            "timeframe": timeframe,
            "source": envelope.meta.source,
            "chain": self._chain,
            "fetched_at": datetime.now(UTC).isoformat(),
            "confidence": envelope.meta.confidence,
        }
        return df

    def gas_price(self, chain: str | None = None) -> "GasPrice":
        """Get current gas price for a chain.

        Fetches the current gas price data from the configured GasOracle.
        Results are cached for 12 seconds (approximately 1 block) to avoid
        excessive RPC calls.

        Args:
            chain: Chain identifier (e.g., "ethereum", "arbitrum", "optimism").
                   If not specified, uses the strategy's primary chain from
                   the MarketSnapshot.

        Returns:
            GasPrice dataclass with:
                - chain: Chain identifier
                - base_fee_gwei: Network base fee in gwei
                - priority_fee_gwei: Priority/tip fee in gwei
                - max_fee_gwei: Maximum fee (base + priority) in gwei
                - l1_base_fee_gwei: L1 base fee for L2 chains (optional)
                - l1_data_cost_gwei: L1 data cost for L2 chains (optional)
                - estimated_cost_usd: Estimated cost in USD for 21000 gas
                - timestamp: When the gas price was observed

        Raises:
            GasUnavailableError: If gas price cannot be retrieved
            ValueError: If no gas oracle is configured

        Example:
            # Get gas for strategy's primary chain
            gas = snapshot.gas_price()
            print(f"Base fee: {gas.base_fee_gwei} gwei")
            print(f"Estimated cost: ${gas.estimated_cost_usd}")

            # Get gas for specific chain
            arb_gas = snapshot.gas_price("arbitrum")
            if arb_gas.is_l2:
                print(f"L1 data cost: {arb_gas.l1_data_cost_gwei} gwei")
        """
        if self._gas_oracle is None:
            raise ValueError("No gas oracle configured for MarketSnapshot")

        # Use strategy's primary chain if not specified
        target_chain = chain or self._chain

        # Check cache
        if target_chain in self._gas_cache:
            cached_gas, cached_time = self._gas_cache[target_chain]
            age = (datetime.now(UTC) - cached_time).total_seconds()
            if age < self._gas_cache_ttl_seconds:
                logger.debug(
                    "Using cached gas price for %s (age: %.1fs)",
                    target_chain,
                    age,
                )
                return cached_gas

        try:
            gas_price_result: GasPrice = self._run_async(self._gas_oracle.get_gas_price(target_chain))

            # Cache the result with current timestamp
            self._gas_cache[target_chain] = (gas_price_result, datetime.now(UTC))

            return gas_price_result
        except DataSourceUnavailable as e:
            raise GasUnavailableError(target_chain, e.reason) from e
        except DataSourceError as e:
            raise GasUnavailableError(target_chain, str(e)) from e
        except Exception as e:
            raise GasUnavailableError(target_chain, f"Unexpected error: {e}") from e

    # =========================================================================
    # On-chain Pool Price Methods (Quant Data Layer)
    # =========================================================================

    def pool_price(
        self,
        pool_address: str,
        chain: str | None = None,
    ) -> "DataEnvelope[PoolPrice]":
        """Get the live price from an on-chain DEX pool.

        Reads slot0() from the pool contract and decodes sqrtPriceX96 into a
        human-readable price using token decimals.

        Returns a DataEnvelope[PoolPrice] with EXECUTION_GRADE classification
        (fail-closed: raises on any error, no off-chain fallback).

        Args:
            pool_address: Pool contract address.
            chain: Chain name (e.g. "arbitrum", "base"). Defaults to
                the snapshot's primary chain.

        Returns:
            DataEnvelope[PoolPrice] with provenance metadata.

        Raises:
            PoolPriceUnavailableError: If pool price cannot be retrieved.
            ValueError: If no pool reader registry is configured.
        """
        if self._pool_reader_registry is None:
            raise ValueError("No pool reader registry configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()

        try:
            # Try to get a reader for any supported protocol on this chain
            protocols = self._pool_reader_registry.protocols_for_chain(target_chain)
            if not protocols:
                raise PoolPriceUnavailableError(
                    pool_address,
                    f"No pool reader protocols registered for chain '{target_chain}'",
                )

            # Try each protocol until one succeeds
            last_error: Exception | None = None
            for protocol in protocols:
                try:
                    reader = self._pool_reader_registry.get_reader(target_chain, protocol)
                    return reader.read_pool_price(pool_address, target_chain)
                except Exception as e:
                    last_error = e
                    continue

            raise PoolPriceUnavailableError(
                pool_address,
                f"All protocols failed for pool {pool_address} on {target_chain}: {last_error}",
            )
        except PoolPriceUnavailableError:
            raise
        except Exception as e:
            raise PoolPriceUnavailableError(pool_address, f"Unexpected error: {e}") from e

    def pool_price_by_pair(
        self,
        token_a: str,
        token_b: str,
        chain: str | None = None,
        protocol: str | None = None,
        fee_tier: int = 3000,
    ) -> "DataEnvelope[PoolPrice]":
        """Get the live pool price for a token pair.

        Resolves the pool address for the given pair and reads the price.
        This is a convenience method that wraps pool address resolution and
        price reading.

        Args:
            token_a: Token A symbol or address.
            token_b: Token B symbol or address.
            chain: Chain name. Defaults to the snapshot's primary chain.
            protocol: Protocol name (e.g. "uniswap_v3"). If None, tries all
                registered protocols.
            fee_tier: Fee tier in basis points (default 3000 = 0.3%).

        Returns:
            DataEnvelope[PoolPrice] with provenance metadata.

        Raises:
            PoolPriceUnavailableError: If pool cannot be found or price cannot be read.
            ValueError: If no pool reader registry is configured.
        """
        if self._pool_reader_registry is None:
            raise ValueError("No pool reader registry configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()
        pair_str = f"{token_a}/{token_b}"

        protocols = [protocol] if protocol else self._pool_reader_registry.protocols_for_chain(target_chain)
        if not protocols:
            raise PoolPriceUnavailableError(
                pair_str,
                f"No pool reader protocols registered for chain '{target_chain}'",
            )

        last_error: Exception | None = None
        for proto in protocols:
            try:
                reader = self._pool_reader_registry.get_reader(target_chain, proto)
                pool_addr = reader.resolve_pool_address(token_a, token_b, target_chain, fee_tier)
                if pool_addr is None:
                    continue
                return reader.read_pool_price(pool_addr, target_chain)
            except Exception as e:
                last_error = e
                continue

        raise PoolPriceUnavailableError(
            pair_str,
            f"No pool found for {pair_str} (fee_tier={fee_tier}) on {target_chain}: {last_error}",
        )

    def twap(
        self,
        token_pair: "str | Instrument",
        chain: str | None = None,
        window_seconds: int = 300,
        pool_address: str | None = None,
        protocol: str = "uniswap_v3",
    ) -> "DataEnvelope[AggregatedPrice]":
        """Get the time-weighted average price (TWAP) for a token pair.

        Uses the Uniswap V3 oracle's observe() function to compute the TWAP
        over the specified time window.

        Classification: EXECUTION_GRADE (fail-closed, no off-chain fallback).

        Args:
            token_pair: Token pair as "BASE/QUOTE" string (e.g. "WETH/USDC")
                or an Instrument instance.
            chain: Chain name. Defaults to the snapshot's primary chain.
            window_seconds: Time window in seconds (default 300 = 5 min).
            pool_address: Explicit pool address. If None, resolves from pair.
            protocol: Protocol to use (default "uniswap_v3").

        Returns:
            DataEnvelope[AggregatedPrice] with TWAP price and provenance.

        Raises:
            PoolPriceUnavailableError: If TWAP cannot be calculated.
            ValueError: If no price aggregator is configured.
        """
        if self._price_aggregator is None:
            raise ValueError("No price aggregator configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()

        # Resolve instrument for pair info
        from .models import resolve_instrument

        inst = resolve_instrument(token_pair, target_chain)
        pair_str = inst.pair

        try:
            # Resolve pool address if not provided
            if pool_address is None:
                if self._pool_reader_registry is None:
                    raise ValueError("No pool reader registry configured; provide pool_address explicitly")
                reader = self._pool_reader_registry.get_reader(target_chain, protocol)
                pool_address = reader.resolve_pool_address(inst.base, inst.quote, target_chain)
                if pool_address is None:
                    raise PoolPriceUnavailableError(
                        pair_str,
                        f"Cannot resolve pool for {pair_str} on {target_chain} (protocol={protocol})",
                    )
                # Get token decimals from the reader
                token0_decimals, token1_decimals, _ = reader._get_pool_metadata(pool_address, target_chain)
            else:
                # Defaults; the aggregator will use these
                token0_decimals = 18
                token1_decimals = 6

            return self._price_aggregator.twap(
                pool_address=pool_address,
                chain=target_chain,
                window_seconds=window_seconds,
                token0_decimals=token0_decimals,
                token1_decimals=token1_decimals,
                protocol=protocol,
            )
        except PoolPriceUnavailableError:
            raise
        except Exception as e:
            raise PoolPriceUnavailableError(
                pair_str,
                f"TWAP calculation failed for {pair_str} on {target_chain}: {e}",
            ) from e

    def lwap(
        self,
        token_pair: "str | Instrument",
        chain: str | None = None,
        fee_tiers: list[int] | None = None,
        protocols: list[str] | None = None,
    ) -> "DataEnvelope[AggregatedPrice]":
        """Get the liquidity-weighted average price (LWAP) for a token pair.

        Reads live prices from all known pools for the pair, filters by
        minimum liquidity, and computes a liquidity-weighted average.

        Classification: EXECUTION_GRADE (fail-closed, no off-chain fallback).

        Args:
            token_pair: Token pair as "BASE/QUOTE" string (e.g. "WETH/USDC")
                or an Instrument instance.
            chain: Chain name. Defaults to the snapshot's primary chain.
            fee_tiers: Fee tiers to search (default: [100, 500, 3000, 10000]).
            protocols: Protocols to search (default: all registered for chain).

        Returns:
            DataEnvelope[AggregatedPrice] with LWAP price and provenance.

        Raises:
            PoolPriceUnavailableError: If LWAP cannot be calculated.
            ValueError: If no price aggregator is configured.
        """
        if self._price_aggregator is None:
            raise ValueError("No price aggregator configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()

        # Resolve instrument for pair info
        from .models import resolve_instrument

        inst = resolve_instrument(token_pair, target_chain)
        pair_str = inst.pair

        try:
            return self._price_aggregator.lwap(
                token_a=inst.base,
                token_b=inst.quote,
                chain=target_chain,
                fee_tiers=fee_tiers,
                protocols=protocols,
            )
        except Exception as e:
            raise PoolPriceUnavailableError(
                pair_str,
                f"LWAP calculation failed for {pair_str} on {target_chain}: {e}",
            ) from e

    def pool_history(
        self,
        pool_address: str,
        chain: str | None = None,
        start_date: "datetime | None" = None,
        end_date: "datetime | None" = None,
        resolution: str = "1h",
    ) -> "DataEnvelope[list[PoolSnapshot]]":
        """Get historical pool state snapshots for backtesting and analytics.

        Fetches TVL, volume, fee revenue, and reserve data from The Graph,
        DeFi Llama, or GeckoTerminal with graceful fallback between providers.
        Results are cached in VersionedDataCache for deterministic replay.

        Args:
            pool_address: Pool contract address.
            chain: Chain name (e.g. "arbitrum", "ethereum"). Defaults to strategy chain.
            start_date: Start of the history window (UTC). Defaults to 90 days ago.
            end_date: End of the history window (UTC). Defaults to now.
            resolution: Data resolution: "1h", "4h", or "1d". Default "1h".

        Returns:
            DataEnvelope[list[PoolSnapshot]] with INFORMATIONAL classification.

        Raises:
            PoolHistoryUnavailableError: If historical data cannot be retrieved.
            ValueError: If no pool history reader is configured.
        """
        if self._pool_history_reader is None:
            raise ValueError("No pool history reader configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()

        if start_date is None:
            start_date = datetime.now(UTC) - timedelta(days=90)
        if end_date is None:
            end_date = datetime.now(UTC)

        try:
            return self._pool_history_reader.get_pool_history(
                pool_address=pool_address,
                chain=target_chain,
                start_date=start_date,
                end_date=end_date,
                resolution=resolution,
            )
        except Exception as e:
            raise PoolHistoryUnavailableError(
                pool_address,
                f"Failed to fetch pool history for {pool_address} on {target_chain}: {e}",
            ) from e

    def liquidity_depth(
        self,
        pool_address: str,
        chain: str | None = None,
    ) -> "DataEnvelope[LiquidityDepth]":
        """Get tick-level liquidity depth for a concentrated-liquidity pool.

        Reads the tick bitmap and individual tick liquidity values from the
        pool contract to build a picture of liquidity distribution around the
        current price. Essential for slippage estimation and position sizing.

        Classification: EXECUTION_GRADE (fails closed, no off-chain fallback).

        Args:
            pool_address: Pool contract address.
            chain: Chain name (e.g. "arbitrum", "ethereum"). Defaults to strategy chain.

        Returns:
            DataEnvelope[LiquidityDepth] with tick-level liquidity data.

        Raises:
            LiquidityDepthUnavailableError: If liquidity data cannot be read.
            ValueError: If no liquidity depth reader is configured.
        """
        if self._liquidity_depth_reader is None:
            raise ValueError("No liquidity depth reader configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()

        try:
            return self._liquidity_depth_reader.read_liquidity_depth(
                pool_address=pool_address,
                chain=target_chain,
            )
        except LiquidityDepthUnavailableError:
            raise
        except Exception as e:
            raise LiquidityDepthUnavailableError(
                pool_address,
                f"Failed to read liquidity depth for {pool_address} on {target_chain}: {e}",
            ) from e

    def estimate_slippage(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
        chain: str | None = None,
        protocol: str | None = None,
    ) -> "DataEnvelope[SlippageEstimate]":
        """Estimate price impact and slippage for a potential swap.

        Simulates the swap through tick ranges using actual on-chain liquidity
        data to compute the expected execution price and slippage. Logs a
        warning if estimated slippage exceeds the configured threshold (default 1%).

        Classification: EXECUTION_GRADE (fails closed, no off-chain fallback).

        Args:
            token_in: Input token symbol or address.
            token_out: Output token symbol or address.
            amount: Amount of token_in to swap (human-readable units).
            chain: Chain name. Defaults to strategy chain.
            protocol: Protocol name (e.g. "uniswap_v3"). Auto-detected if None.

        Returns:
            DataEnvelope[SlippageEstimate] with price impact data.

        Raises:
            SlippageEstimateUnavailableError: If slippage cannot be estimated.
            ValueError: If no slippage estimator is configured.
        """
        if self._slippage_estimator is None:
            raise ValueError("No slippage estimator configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()

        try:
            return self._slippage_estimator.estimate_slippage(
                token_in=token_in,
                token_out=token_out,
                amount=amount,
                chain=target_chain,
                protocol=protocol,
            )
        except SlippageEstimateUnavailableError:
            raise
        except Exception as e:
            raise SlippageEstimateUnavailableError(
                f"{token_in}/{token_out}",
                f"Slippage estimation failed: {e}",
            ) from e

    def pool_reserves(self, pool_address: str, chain: str | None = None) -> "PoolReserves":
        """Get DEX pool reserves and state.

        Fetches the current state of a DEX liquidity pool from the blockchain.
        Auto-detects the pool type (Uniswap V2 vs V3) by checking the contract
        interface.

        Results are cached for 12 seconds (approximately 1 block) to avoid
        excessive RPC calls.

        Args:
            pool_address: Pool contract address
            chain: Chain identifier (e.g., "ethereum", "arbitrum", "optimism").
                   If not specified, uses the strategy's primary chain from
                   the MarketSnapshot.

        Returns:
            PoolReserves dataclass with:
                - pool_address: Pool contract address
                - dex: DEX type ('uniswap_v2', 'uniswap_v3', 'sushiswap')
                - token0: First token in the pair (ChainToken)
                - token1: Second token in the pair (ChainToken)
                - reserve0: Reserve of token0 (human-readable Decimal)
                - reserve1: Reserve of token1 (human-readable Decimal)
                - fee_tier: Pool fee in basis points
                - sqrt_price_x96: V3 sqrt price (None for V2)
                - tick: V3 current tick (None for V2)
                - liquidity: V3 in-range liquidity (None for V2)
                - tvl_usd: Total value locked in USD
                - last_updated: When the data was fetched

        Raises:
            PoolReservesUnavailableError: If pool data cannot be retrieved
            ValueError: If no pool reader is configured

        Example:
            # Get pool reserves for USDC/WETH pool
            pool = snapshot.pool_reserves(
                "0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640"
            )
            print(f"Reserve0: {pool.reserve0} {pool.token0.symbol}")
            print(f"Reserve1: {pool.reserve1} {pool.token1.symbol}")
            print(f"TVL: ${pool.tvl_usd}")

            # Check if V3 pool
            if pool.is_v3:
                print(f"Current tick: {pool.tick}")
        """
        if self._pool_reader is None:
            raise ValueError("No pool reader configured for MarketSnapshot")

        # Use strategy's primary chain if not specified
        target_chain = chain or self._chain

        # Normalize pool address for cache key
        pool_address_lower = pool_address.lower()
        cache_key = f"{target_chain}:{pool_address_lower}"

        # Check cache
        if cache_key in self._pool_cache:
            cached_pool, cached_time = self._pool_cache[cache_key]
            age = (datetime.now(UTC) - cached_time).total_seconds()
            if age < self._pool_cache_ttl_seconds:
                logger.debug(
                    "Using cached pool reserves for %s on %s (age: %.1fs)",
                    pool_address,
                    target_chain,
                    age,
                )
                return cached_pool

        try:
            # Auto-detect pool type by trying V3 first
            # V3 pools have slot0() function, V2 pools have getReserves()
            # Currently only V3 is supported via UniswapV3PoolReader
            pool_result: PoolReserves = self._run_async(self._detect_and_fetch_pool(pool_address, target_chain))

            # Cache the result with current timestamp
            self._pool_cache[cache_key] = (pool_result, datetime.now(UTC))

            return pool_result
        except DataSourceUnavailable as e:
            raise PoolReservesUnavailableError(pool_address, e.reason) from e
        except DataSourceError as e:
            raise PoolReservesUnavailableError(pool_address, str(e)) from e
        except Exception as e:
            raise PoolReservesUnavailableError(pool_address, f"Unexpected error: {e}") from e

    async def _detect_and_fetch_pool(self, pool_address: str, chain: str) -> "PoolReserves":
        """Auto-detect pool type and fetch reserves.

        Attempts to detect the pool type by checking for V3-specific functions.
        V3 pools have slot0(), while V2 pools have getReserves().

        Args:
            pool_address: Pool contract address
            chain: Chain identifier

        Returns:
            PoolReserves for the detected pool type

        Raises:
            DataSourceError: If pool type cannot be detected or data cannot be fetched
        """
        # Try V3 first (UniswapV3PoolReader handles the slot0 call)
        # If it fails with a specific error indicating not a V3 pool, we could
        # fall back to V2, but currently only V3 is implemented
        try:
            # UniswapV3PoolReader is the only reader we have
            # It will fail with an error if the pool is not V3
            # Note: pool_reader is guaranteed non-None by caller (pool_reserves method)
            assert self._pool_reader is not None
            return await self._pool_reader.get_pool_reserves(pool_address, chain)
        except DataSourceError:
            # Re-raise DataSourceError as-is (could be V2 pool or other issue)
            # In the future, we could try V2 reader here
            raise
        except Exception as e:
            # Convert other exceptions to DataSourceError
            raise DataSourceError(f"Failed to fetch pool reserves: {e}") from e

    def prices(self, tokens: list[str], quote: str = "USD") -> dict[str, Decimal]:
        """Get prices for multiple tokens in a single batch call.

        Fetches prices for all specified tokens in parallel using asyncio.gather.
        Returns partial results if some tokens fail (errors are logged).

        Args:
            tokens: List of token symbols (e.g., ["WETH", "USDC", "ARB"])
            quote: Quote currency (default "USD")

        Returns:
            Dictionary mapping token symbols to their prices as Decimal.
            Only includes tokens that were successfully fetched.

        Raises:
            ValueError: If no price oracle is configured

        Example:
            prices = snapshot.prices(["WETH", "USDC", "ARB"])
            # Returns: {"WETH": Decimal("2500.50"), "USDC": Decimal("1.00"), "ARB": Decimal("0.85")}

            # If ARB fails to fetch:
            # Returns: {"WETH": Decimal("2500.50"), "USDC": Decimal("1.00")}
            # (Error is logged)
        """
        if self._price_oracle is None:
            raise ValueError("No price oracle configured for MarketSnapshot")

        if not tokens:
            return {}

        # Check cache first for all tokens
        results: dict[str, Decimal] = {}
        tokens_to_fetch: list[str] = []

        for token in tokens:
            cache_key = f"{token}/{quote}"
            if cache_key in self._price_cache:
                results[token] = self._price_cache[cache_key]
            else:
                tokens_to_fetch.append(token)

        # If all tokens were cached, return early
        if not tokens_to_fetch:
            return results

        # Fetch remaining tokens in parallel
        async def fetch_prices() -> dict[str, Decimal]:
            async def fetch_single(token: str) -> tuple[str, Decimal | None, str | None]:
                """Fetch a single token price, returning (token, price, error)."""
                try:
                    result: PriceResult = await self._price_oracle.get_aggregated_price(  # type: ignore[union-attr]
                        token, quote
                    )
                    return (token, result.price, None)
                except AllDataSourcesFailed as e:
                    return (token, None, f"All data sources failed: {e.errors}")
                except DataSourceUnavailable as e:
                    return (token, None, e.reason)
                except DataSourceError as e:
                    return (token, None, str(e))
                except Exception as e:
                    return (token, None, f"Unexpected error: {e}")

            # Gather all fetch operations
            fetch_results = await asyncio.gather(
                *[fetch_single(t) for t in tokens_to_fetch],
                return_exceptions=False,
            )

            fetched: dict[str, Decimal] = {}
            for token, price, error in fetch_results:
                if price is not None:
                    fetched[token] = price
                    # Update cache
                    cache_key = f"{token}/{quote}"
                    self._price_cache[cache_key] = price
                else:
                    logger.warning(
                        "Failed to fetch price for %s: %s",
                        token,
                        error,
                    )
            return fetched

        # Run async fetch
        fetched_prices = self._run_async(fetch_prices())
        results.update(fetched_prices)
        return results

    def balances(self, tokens: list[str]) -> dict[str, Decimal]:
        """Get balances for multiple tokens in a single batch call.

        Fetches balances for all specified tokens in parallel using asyncio.gather.
        Returns partial results if some tokens fail (errors are logged).

        Args:
            tokens: List of token symbols (e.g., ["WETH", "USDC", "ARB"])

        Returns:
            Dictionary mapping token symbols to their balances as Decimal.
            Only includes tokens that were successfully fetched.

        Raises:
            ValueError: If no balance provider is configured

        Example:
            balances = snapshot.balances(["WETH", "USDC", "ARB"])
            # Returns: {"WETH": Decimal("1.5"), "USDC": Decimal("1000.00"), "ARB": Decimal("500.00")}

            # If ARB fails to fetch:
            # Returns: {"WETH": Decimal("1.5"), "USDC": Decimal("1000.00")}
            # (Error is logged)
        """
        if self._balance_provider is None:
            raise ValueError("No balance provider configured for MarketSnapshot")

        if not tokens:
            return {}

        # Check cache first for all tokens
        results: dict[str, Decimal] = {}
        tokens_to_fetch: list[str] = []

        for token in tokens:
            if token in self._balance_cache:
                results[token] = self._balance_cache[token]
            else:
                tokens_to_fetch.append(token)

        # If all tokens were cached, return early
        if not tokens_to_fetch:
            return results

        # Fetch remaining tokens in parallel
        async def fetch_balances() -> dict[str, Decimal]:
            async def fetch_single(token: str) -> tuple[str, Decimal | None, str | None]:
                """Fetch a single token balance, returning (token, balance, error)."""
                try:
                    result: BalanceResult = await self._balance_provider.get_balance(  # type: ignore[union-attr]
                        token
                    )
                    return (token, result.balance, None)
                except DataSourceError as e:
                    return (token, None, str(e))
                except Exception as e:
                    return (token, None, f"Unexpected error: {e}")

            # Gather all fetch operations
            fetch_results = await asyncio.gather(
                *[fetch_single(t) for t in tokens_to_fetch],
                return_exceptions=False,
            )

            fetched: dict[str, Decimal] = {}
            for token, balance, error in fetch_results:
                if balance is not None:
                    fetched[token] = balance
                    # Update cache
                    self._balance_cache[token] = balance
                else:
                    logger.warning(
                        "Failed to fetch balance for %s: %s",
                        token,
                        error,
                    )
            return fetched

        # Run async fetch
        fetched_balances = self._run_async(fetch_balances())
        results.update(fetched_balances)
        return results

    def health(self) -> "HealthReport":
        """Get a health report for all registered data providers.

        Aggregates health metrics from all configured providers including
        price oracle, balance provider, OHLCV module, gas oracle, and pool reader.

        The health report includes:
        - Individual source health (success rate, latency, errors)
        - Cache statistics (hits, misses, hit rate)
        - Overall system status (healthy, degraded, unhealthy)

        Returns:
            HealthReport dataclass with:
                - timestamp: When the report was generated
                - sources: Dictionary mapping source names to SourceHealth
                - cache_stats: CacheStats with cache performance metrics
                - overall_status: "healthy", "degraded", or "unhealthy"

        Example:
            report = snapshot.health()
            print(f"Overall status: {report.overall_status}")
            print(f"Sources: {list(report.sources.keys())}")

            for name, health in report.sources.items():
                print(f"  {name}: {health.success_rate:.1%} success rate")

            if report.failing_sources:
                print(f"Warning: failing sources: {report.failing_sources}")
        """
        from .health import CacheStats, HealthReport, SourceHealth

        sources: dict[str, SourceHealth] = {}

        # Collect health from price oracle
        if self._price_oracle is not None:
            # Try to get source health from the oracle if it supports it
            try:
                oracle_health = self._price_oracle.get_source_health("aggregator")
                if oracle_health is not None:
                    sources["price_oracle"] = SourceHealth(
                        name="price_oracle",
                        success_rate=oracle_health.get("success_rate", 1.0),
                        latency_p50_ms=oracle_health.get("latency_p50_ms", 0.0),
                        latency_p95_ms=oracle_health.get("latency_p95_ms", 0.0),
                        error_count=oracle_health.get("error_count", 0),
                        last_success=oracle_health.get("last_success"),
                        last_error=oracle_health.get("last_error"),
                        last_error_message=oracle_health.get("last_error_message"),
                        total_requests=oracle_health.get("total_requests", 0),
                    )
                else:
                    # Oracle exists but no health info - assume healthy
                    sources["price_oracle"] = SourceHealth(
                        name="price_oracle",
                        success_rate=1.0,
                        latency_p50_ms=0.0,
                        latency_p95_ms=0.0,
                        error_count=0,
                        last_success=datetime.now(UTC),
                    )
            except Exception:
                # Oracle doesn't support health check - assume healthy
                sources["price_oracle"] = SourceHealth(
                    name="price_oracle",
                    success_rate=1.0,
                    latency_p50_ms=0.0,
                    latency_p95_ms=0.0,
                    error_count=0,
                    last_success=datetime.now(UTC),
                )

        # Collect health from balance provider
        if self._balance_provider is not None:
            sources["balance_provider"] = SourceHealth(
                name="balance_provider",
                success_rate=1.0,
                latency_p50_ms=0.0,
                latency_p95_ms=0.0,
                error_count=0,
                last_success=datetime.now(UTC),
            )

        # Collect health from OHLCV module
        if self._ohlcv_module is not None:
            sources["ohlcv_module"] = SourceHealth(
                name="ohlcv_module",
                success_rate=1.0,
                latency_p50_ms=0.0,
                latency_p95_ms=0.0,
                error_count=0,
                last_success=datetime.now(UTC),
            )

        # Collect health from gas oracle
        if self._gas_oracle is not None:
            sources["gas_oracle"] = SourceHealth(
                name="gas_oracle",
                success_rate=1.0,
                latency_p50_ms=0.0,
                latency_p95_ms=0.0,
                error_count=0,
                last_success=datetime.now(UTC),
            )

        # Collect health from pool reader
        if self._pool_reader is not None:
            sources["pool_reader"] = SourceHealth(
                name="pool_reader",
                success_rate=1.0,
                latency_p50_ms=0.0,
                latency_p95_ms=0.0,
                error_count=0,
                last_success=datetime.now(UTC),
            )

        # Calculate cache stats from internal caches
        price_cache_size = len(self._price_cache)
        balance_cache_size = len(self._balance_cache)
        rsi_cache_size = len(self._rsi_cache)
        gas_cache_size = len(self._gas_cache)
        pool_cache_size = len(self._pool_cache)

        total_cache_size = price_cache_size + balance_cache_size + rsi_cache_size + gas_cache_size + pool_cache_size

        cache_stats = CacheStats(
            hits=0,  # We don't track hits/misses currently
            misses=0,
            size=total_cache_size,
            max_size=None,  # No max size configured
        )

        # Calculate overall status
        overall_status = HealthReport.calculate_overall_status(sources)

        return HealthReport(
            timestamp=datetime.now(UTC),
            sources=sources,
            cache_stats=cache_stats,
            overall_status=overall_status,
        )

    def balance_usd(self, token: str) -> Decimal:
        """Get the wallet balance value in USD terms.

        Calculates the USD value by multiplying the token balance
        by its current price.

        Args:
            token: Token symbol (e.g., "WETH", "USDC")

        Returns:
            Balance value in USD as a Decimal

        Raises:
            PriceUnavailableError: If price cannot be determined
            BalanceUnavailableError: If balance cannot be determined

        Example:
            eth_value = snapshot.balance_usd("WETH")
            # If balance is 2 WETH at $2500, returns: Decimal("5000.00")
        """
        token_balance = self.balance(token)
        token_price = self.price(token)
        return token_balance * token_price

    def total_portfolio_usd(self, tokens: list[str] | None = None) -> Decimal:
        """Get the total portfolio value in USD.

        When called with a token list, sums the USD value of those tokens.
        When called without arguments, sums all cached balance values (tokens
        that have been queried via balance() or balances() in this snapshot).

        Args:
            tokens: List of token symbols to include. If None, uses all
                cached balances.

        Returns:
            Total portfolio value in USD as a Decimal

        Raises:
            PriceUnavailableError: If any price cannot be determined
            BalanceUnavailableError: If any balance cannot be determined

        Example:
            # Explicit token list
            total = snapshot.total_portfolio_usd(["WETH", "USDC", "ARB"])

            # All cached balances
            total = snapshot.total_portfolio_usd()
        """
        if tokens is not None:
            if not tokens:
                return Decimal("0")
            total = Decimal("0")
            for token in tokens:
                total += self.balance_usd(token)
            return total

        # No tokens specified: sum all cached balances
        total = Decimal("0")
        for token, balance in self._balance_cache.items():
            try:
                price = self.price(token)
                total += balance * price
            except Exception:
                logger.debug("Skipping %s in portfolio total: price unavailable", token)
        return total

    def lending_rate(
        self,
        protocol: str,
        token: str,
        side: str = "supply",
    ) -> "LendingRate":
        """Get the lending rate for a specific protocol and token.

        Fetches the current supply or borrow APY from the specified lending
        protocol. Rates are cached for efficiency (typically 12s = ~1 block).

        Args:
            protocol: Protocol identifier (aave_v3, morpho_blue, compound_v3)
            token: Token symbol (e.g., "USDC", "WETH")
            side: Rate side - "supply" or "borrow" (default "supply")

        Returns:
            LendingRate dataclass with:
                - protocol: Protocol identifier
                - token: Token symbol
                - side: Rate side
                - apy_ray: APY in ray units (1e27 scale)
                - apy_percent: APY as percentage (e.g., 5.25 for 5.25%)
                - utilization_percent: Pool utilization percentage
                - timestamp: When rate was fetched
                - chain: Blockchain network
                - market_id: Market identifier (optional)

        Raises:
            LendingRateUnavailableError: If rate cannot be retrieved
            ValueError: If no rate monitor is configured

        Example:
            # Get Aave USDC supply rate
            rate = snapshot.lending_rate("aave_v3", "USDC", "supply")
            print(f"Aave USDC Supply APY: {rate.apy_percent:.2f}%")

            # Get Morpho WETH borrow rate
            rate = snapshot.lending_rate("morpho_blue", "WETH", "borrow")
            print(f"Morpho WETH Borrow APY: {rate.apy_percent:.2f}%")
        """
        if self._rate_monitor is None:
            raise ValueError("No rate monitor configured for MarketSnapshot")

        # Import RateSide enum for type conversion
        from .rates import ProtocolNotSupportedError, RateSide, RateUnavailableError, TokenNotSupportedError

        try:
            rate_side = RateSide(side)
            result = self._run_async(self._rate_monitor.get_lending_rate(protocol, token, rate_side))
            return result
        except RateUnavailableError as e:
            raise LendingRateUnavailableError(protocol, token, side, e.reason) from e
        except ProtocolNotSupportedError as e:
            raise LendingRateUnavailableError(protocol, token, side, str(e)) from e
        except TokenNotSupportedError as e:
            raise LendingRateUnavailableError(protocol, token, side, str(e)) from e
        except Exception as e:
            raise LendingRateUnavailableError(protocol, token, side, f"Unexpected error: {e}") from e

    def best_lending_rate(
        self,
        token: str,
        side: str = "supply",
        protocols: list[str] | None = None,
    ) -> "BestRateResult":
        """Get the best lending rate for a token across protocols.

        Compares rates from all available lending protocols and returns the
        optimal one. For supply rates, returns highest APY. For borrow rates,
        returns lowest APY.

        Args:
            token: Token symbol (e.g., "USDC", "WETH")
            side: Rate side - "supply" or "borrow" (default "supply")
            protocols: Protocols to compare (default: all available on chain)

        Returns:
            BestRateResult dataclass with:
                - token: Token symbol
                - side: Rate side
                - best_rate: The best LendingRate found (or None if all failed)
                - all_rates: List of all rates from different protocols
                - timestamp: When comparison was made

        Raises:
            ValueError: If no rate monitor is configured

        Example:
            # Find best USDC supply rate
            result = snapshot.best_lending_rate("USDC", "supply")
            if result.best_rate:
                print(f"Best rate: {result.best_rate.protocol} at {result.best_rate.apy_percent:.2f}%")

                # Compare all protocols
                for rate in result.all_rates:
                    print(f"  {rate.protocol}: {rate.apy_percent:.2f}%")
        """
        if self._rate_monitor is None:
            raise ValueError("No rate monitor configured for MarketSnapshot")

        from .rates import RateSide

        try:
            rate_side = RateSide(side)
            result = self._run_async(self._rate_monitor.get_best_lending_rate(token, rate_side, protocols))
            return result
        except Exception as e:
            # Return empty result on error instead of raising
            logger.warning(f"Failed to get best lending rate for {token}/{side}: {e}")
            from .rates import BestRateResult

            return BestRateResult(
                token=token,
                side=side,
                best_rate=None,
                all_rates=[],
            )

    def funding_rate(
        self,
        venue: str,
        market: str,
    ) -> "FundingRate":
        """Get the funding rate for a perpetual venue and market.

        Fetches the current funding rate from the specified venue.
        Funding rates indicate the cost of holding perpetual positions:
        - Positive rate: Longs pay shorts (bullish market)
        - Negative rate: Shorts pay longs (bearish market)

        Args:
            venue: Venue identifier (gmx_v2, hyperliquid)
            market: Market symbol (e.g., "ETH-USD", "BTC-USD")

        Returns:
            FundingRate dataclass with:
                - venue: Venue identifier
                - market: Market symbol
                - rate_hourly: Hourly funding rate
                - rate_8h: 8-hour funding rate (typical display)
                - rate_annualized: Annualized rate for comparison
                - next_funding_time: Next settlement time
                - open_interest_long: Total long OI in USD
                - open_interest_short: Total short OI in USD
                - mark_price: Current mark price
                - index_price: Current index price

        Raises:
            FundingRateUnavailableError: If rate cannot be retrieved
            ValueError: If no funding rate provider is configured

        Example:
            # Get GMX V2 ETH funding rate
            rate = snapshot.funding_rate("gmx_v2", "ETH-USD")
            print(f"8h rate: {rate.rate_percent_8h:.4f}%")
            print(f"Annualized: {rate.rate_percent_annualized:.2f}%")

            # Check if longs are paying
            if rate.is_positive:
                print("Longs pay shorts (bullish sentiment)")
        """
        if self._funding_rate_provider is None:
            raise ValueError("No funding rate provider configured for MarketSnapshot")

        # Check cache
        cache_key = f"{venue}:{market}"
        if cache_key in self._funding_rate_cache:
            cached_rate, cached_time = self._funding_rate_cache[cache_key]
            age = (datetime.now(UTC) - cached_time).total_seconds()
            if age < self._funding_rate_cache_ttl_seconds:
                logger.debug(
                    "Using cached funding rate for %s/%s (age: %.1fs)",
                    venue,
                    market,
                    age,
                )
                return cached_rate

        from .funding import FundingRateUnavailableError as ProviderFundingRateError
        from .funding import MarketNotSupportedError, Venue, VenueNotSupportedError

        try:
            venue_enum = Venue(venue)
            result = self._run_async(self._funding_rate_provider.get_funding_rate(venue_enum, market))

            # Cache the result
            self._funding_rate_cache[cache_key] = (result, datetime.now(UTC))

            return result
        except ProviderFundingRateError as e:
            raise FundingRateUnavailableError(venue, market, e.reason) from e
        except VenueNotSupportedError as e:
            raise FundingRateUnavailableError(venue, market, str(e)) from e
        except MarketNotSupportedError as e:
            raise FundingRateUnavailableError(venue, market, str(e)) from e
        except Exception as e:
            raise FundingRateUnavailableError(venue, market, f"Unexpected error: {e}") from e

    def funding_rate_spread(
        self,
        market: str,
        venue_a: str,
        venue_b: str,
    ) -> "FundingRateSpread":
        """Get the funding rate spread between two venues.

        Compares funding rates from two venues to identify arbitrage
        opportunities. A positive spread means venue_a has higher funding
        than venue_b.

        The spread can be used for funding rate arbitrage:
        - If spread > 0: Short venue_a, long venue_b
        - If spread < 0: Short venue_b, long venue_a

        Args:
            market: Market symbol (e.g., "ETH-USD")
            venue_a: First venue identifier
            venue_b: Second venue identifier

        Returns:
            FundingRateSpread dataclass with:
                - market: Market symbol
                - venue_a: First venue
                - venue_b: Second venue
                - rate_a: Funding rate at venue_a
                - rate_b: Funding rate at venue_b
                - spread_8h: 8-hour spread (rate_a - rate_b)
                - spread_annualized: Annualized spread
                - is_profitable: True if spread exceeds threshold
                - recommended_direction: Trade direction for arb

        Raises:
            FundingRateUnavailableError: If either rate cannot be retrieved
            ValueError: If no funding rate provider is configured

        Example:
            # Compare GMX V2 vs Hyperliquid for ETH
            spread = snapshot.funding_rate_spread(
                "ETH-USD", "gmx_v2", "hyperliquid"
            )
            print(f"Spread: {spread.spread_percent_8h:.4f}% (8h)")

            if spread.is_profitable:
                print(f"Arbitrage opportunity: {spread.recommended_direction}")
        """
        if self._funding_rate_provider is None:
            raise ValueError("No funding rate provider configured for MarketSnapshot")

        from .funding import FundingRateUnavailableError as ProviderFundingRateError
        from .funding import MarketNotSupportedError, Venue, VenueNotSupportedError

        try:
            venue_a_enum = Venue(venue_a)
            venue_b_enum = Venue(venue_b)
            result = self._run_async(
                self._funding_rate_provider.get_funding_rate_spread(market, venue_a_enum, venue_b_enum)
            )
            return result
        except ProviderFundingRateError as e:
            raise FundingRateUnavailableError(f"{venue_a}/{venue_b}", market, e.reason) from e
        except VenueNotSupportedError as e:
            raise FundingRateUnavailableError(f"{venue_a}/{venue_b}", market, str(e)) from e
        except MarketNotSupportedError as e:
            raise FundingRateUnavailableError(f"{venue_a}/{venue_b}", market, str(e)) from e
        except Exception as e:
            raise FundingRateUnavailableError(f"{venue_a}/{venue_b}", market, f"Unexpected error: {e}") from e

    def lending_rate_history(
        self,
        protocol: str,
        token: str,
        chain: str | None = None,
        days: int = 90,
    ) -> "DataEnvelope[list[LendingRateSnapshot]]":
        """Get historical lending rate snapshots for backtesting.

        Fetches supply/borrow APY history from The Graph or DeFi Llama
        with graceful fallback between providers.  Results are cached in
        VersionedDataCache for deterministic replay.

        Args:
            protocol: Lending protocol (e.g. "aave_v3", "morpho_blue", "compound_v3").
            token: Token symbol (e.g. "USDC", "WETH").
            chain: Chain name. Defaults to strategy chain.
            days: Number of days of history. Default 90.

        Returns:
            DataEnvelope[list[LendingRateSnapshot]] with INFORMATIONAL classification.
            Snapshots are sorted ascending by timestamp.

        Raises:
            LendingRateHistoryUnavailableError: If historical data cannot be retrieved.
            ValueError: If no rate history reader is configured.

        Example:
            envelope = snapshot.lending_rate_history("aave_v3", "USDC", days=90)
            for snap in envelope.value:
                print(f"Supply: {snap.supply_apy}%, Borrow: {snap.borrow_apy}%")
        """
        if self._rate_history_reader is None:
            raise ValueError("No rate history reader configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()

        try:
            return self._rate_history_reader.get_lending_rate_history(
                protocol=protocol,
                token=token,
                chain=target_chain,
                days=days,
            )
        except Exception as e:
            raise LendingRateHistoryUnavailableError(
                protocol,
                token,
                f"Failed to fetch lending rate history: {e}",
            ) from e

    def funding_rate_history(
        self,
        venue: str,
        market_symbol: str,
        hours: int = 168,
    ) -> "DataEnvelope[list[FundingRateSnapshot]]":
        """Get historical funding rate snapshots for backtesting.

        Fetches funding rate history from Hyperliquid API or DeFi Llama
        with graceful fallback.  Results are cached in VersionedDataCache
        for deterministic replay.

        Args:
            venue: Perps venue (e.g. "hyperliquid", "gmx_v2").
            market_symbol: Market symbol (e.g. "ETH-USD", "BTC-USD").
            hours: Number of hours of history. Default 168 (7 days).

        Returns:
            DataEnvelope[list[FundingRateSnapshot]] with INFORMATIONAL classification.
            Snapshots are sorted ascending by timestamp.

        Raises:
            FundingRateHistoryUnavailableError: If historical data cannot be retrieved.
            ValueError: If no rate history reader is configured.

        Example:
            envelope = snapshot.funding_rate_history("hyperliquid", "ETH-USD", hours=168)
            for snap in envelope.value:
                print(f"Rate: {snap.rate}, Annualized: {snap.annualized_rate}")
        """
        if self._rate_history_reader is None:
            raise ValueError("No rate history reader configured for MarketSnapshot")

        try:
            return self._rate_history_reader.get_funding_rate_history(
                venue=venue,
                market_symbol=market_symbol,
                hours=hours,
            )
        except Exception as e:
            raise FundingRateHistoryUnavailableError(
                venue,
                market_symbol,
                f"Failed to fetch funding rate history: {e}",
            ) from e

    def price_across_dexs(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
        dexs: list[str] | None = None,
    ) -> "MultiDexPriceResult":
        """Get prices from multiple DEXs for comparison.

        Fetches quotes from all configured DEXs (Uniswap V3, Curve, Enso) and
        returns a comparison of prices and execution details. Use this to
        identify the best execution venue for a swap.

        Args:
            token_in: Input token symbol (e.g., "USDC", "WETH")
            token_out: Output token symbol (e.g., "WETH", "USDC")
            amount: Input amount (human-readable, e.g., Decimal("10000") for 10k)
            dexs: DEXs to query (default: all available on chain)

        Returns:
            MultiDexPriceResult dataclass with:
                - token_in: Input token symbol
                - token_out: Output token symbol
                - amount_in: Input amount
                - quotes: Dictionary mapping DEX name to DexQuote
                - best_quote: Quote with highest output amount
                - price_spread_bps: Spread between best and worst in bps

        Raises:
            DexQuoteUnavailableError: If no quotes can be fetched
            ValueError: If no multi-DEX service is configured

        Example:
            # Compare prices for 10k USDC -> WETH
            result = snapshot.price_across_dexs(
                "USDC", "WETH", Decimal("10000")
            )

            for dex, quote in result.quotes.items():
                print(f"{dex}: {quote.amount_out} WETH")
                print(f"  Price impact: {quote.price_impact_bps} bps")
                print(f"  Slippage estimate: {quote.slippage_estimate_bps} bps")

            print(f"Best venue: {result.best_quote.dex}")
            print(f"Price spread: {result.price_spread_bps} bps")
        """
        if self._multi_dex_service is None:
            raise ValueError("No multi-DEX service configured for MarketSnapshot")

        from .price.multi_dex import DexNotSupportedError, QuoteUnavailableError

        try:
            result = self._run_async(self._multi_dex_service.get_prices_across_dexs(token_in, token_out, amount, dexs))
            return result
        except (QuoteUnavailableError, DexNotSupportedError) as e:
            raise DexQuoteUnavailableError(token_in, token_out, str(e)) from e
        except Exception as e:
            raise DexQuoteUnavailableError(token_in, token_out, f"Unexpected error: {e}") from e

    def best_dex_price(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
        dexs: list[str] | None = None,
    ) -> "BestDexResult":
        """Get the best DEX for a trade.

        Compares prices from all configured DEXs and returns the one with
        the highest output amount (best execution). This is useful for
        routing trades to the optimal venue.

        Args:
            token_in: Input token symbol (e.g., "USDC", "WETH")
            token_out: Output token symbol (e.g., "WETH", "USDC")
            amount: Input amount (human-readable, e.g., Decimal("10000") for 10k)
            dexs: DEXs to compare (default: all available on chain)

        Returns:
            BestDexResult dataclass with:
                - token_in: Input token symbol
                - token_out: Output token symbol
                - amount_in: Input amount
                - best_dex: Best DEX for the trade (e.g., "uniswap_v3")
                - best_quote: DexQuote from the best DEX
                - all_quotes: List of quotes from all DEXs
                - savings_vs_worst_bps: Savings vs worst venue in bps

        Raises:
            ValueError: If no multi-DEX service is configured

        Example:
            # Find best venue for USDC -> WETH swap
            result = snapshot.best_dex_price(
                "USDC", "WETH", Decimal("10000")
            )

            if result.best_quote:
                print(f"Best DEX: {result.best_dex}")
                print(f"Output: {result.best_quote.amount_out} WETH")
                print(f"Savings vs worst: {result.savings_vs_worst_bps} bps")
            else:
                print("No quotes available")
        """
        if self._multi_dex_service is None:
            raise ValueError("No multi-DEX service configured for MarketSnapshot")

        from .price.multi_dex import BestDexResult

        try:
            result = self._run_async(self._multi_dex_service.get_best_dex_price(token_in, token_out, amount, dexs))
            return result
        except Exception as e:
            # Return empty result on error instead of raising
            import logging

            logging.getLogger(__name__).warning(f"Failed to get best DEX price for {token_in}->{token_out}: {e}")
            return BestDexResult(
                token_in=token_in,
                token_out=token_out,
                amount_in=amount,
                best_dex=None,
                best_quote=None,
                all_quotes=[],
                savings_vs_worst_bps=0,
            )

    def il_exposure(
        self,
        position_id: str,
        fees_earned: Decimal = Decimal("0"),
    ) -> "ILExposure":
        """Get the impermanent loss exposure for a tracked LP position.

        Calculates the current IL for a tracked LP position using the
        position's entry prices and current market prices. This method
        requires an ILCalculator with the position already registered.

        The ILCalculator should be configured with the position via
        add_position() before calling this method.

        Args:
            position_id: Unique identifier for the LP position
            fees_earned: Optional fees earned by the position (for net PnL calc)

        Returns:
            ILExposure dataclass with:
                - position_id: Position identifier
                - position: LPPosition details
                - current_il: ILResult with IL metrics
                - entry_value: Original position value
                - current_value: Current position value
                - fees_earned: Fees earned (if provided)
                - net_pnl: Net profit/loss including fees

        Raises:
            ILExposureUnavailableError: If exposure cannot be calculated
            ValueError: If no IL calculator is configured

        Example:
            # Get IL exposure for a position
            exposure = snapshot.il_exposure("my-lp-position-123")
            print(f"Current IL: {exposure.current_il.il_percent:.2f}%")
            print(f"Entry value: ${exposure.entry_value}")
            print(f"Current value: ${exposure.current_value}")

            if exposure.il_offset_by_fees:
                print("Fees offset the IL - net positive!")
        """
        if self._il_calculator is None:
            raise ValueError("No IL calculator configured for MarketSnapshot")

        from .lp import ILExposureUnavailableError as CalcILExposureError
        from .lp import PositionNotFoundError

        try:
            # Get current prices from price oracle if available
            position = self._il_calculator.get_position(position_id)

            current_price_a: Decimal | None = None
            current_price_b: Decimal | None = None

            if self._price_oracle is not None:
                try:
                    current_price_a = self.price(position.token_a)
                except PriceUnavailableError:
                    pass
                try:
                    current_price_b = self.price(position.token_b)
                except PriceUnavailableError:
                    pass

            result = self._il_calculator.calculate_il_exposure(
                position_id=position_id,
                current_price_a=current_price_a,
                current_price_b=current_price_b,
                fees_earned=fees_earned,
            )
            return result
        except PositionNotFoundError as e:
            raise ILExposureUnavailableError(position_id, f"Position not found: {e}") from e
        except CalcILExposureError as e:
            raise ILExposureUnavailableError(position_id, e.reason) from e
        except Exception as e:
            raise ILExposureUnavailableError(position_id, f"Unexpected error: {e}") from e

    def projected_il(
        self,
        token_a: str,
        token_b: str,
        price_change_pct: Decimal,
        weight_a: Decimal = Decimal("0.5"),
        weight_b: Decimal = Decimal("0.5"),
    ) -> "ProjectedILResult":
        """Project impermanent loss for a hypothetical price change.

        This method simulates what IL would be if token A's price changed
        by the specified percentage relative to token B. This is useful for
        understanding IL risk before entering a position.

        Args:
            token_a: Symbol of token A (the volatile token)
            token_b: Symbol of token B (often a stablecoin)
            price_change_pct: Price change percentage (e.g., 50 for +50%, -30 for -30%)
            weight_a: Weight of token A in the pool (default 0.5)
            weight_b: Weight of token B in the pool (default 0.5)

        Returns:
            ProjectedILResult dataclass with:
                - price_change_pct: The input price change
                - il_ratio: Projected IL as decimal (e.g., -0.0057 for 0.57% loss)
                - il_percent: Projected IL as percentage (e.g., -0.57)
                - il_bps: Projected IL in basis points (e.g., -57)
                - pool_type: Type of pool (default: constant_product)
                - weight_a: Weight of token A
                - weight_b: Weight of token B

        Raises:
            ValueError: If no IL calculator is configured or invalid parameters

        Example:
            # What would IL be if ETH goes up 50%?
            proj = snapshot.projected_il("WETH", "USDC", Decimal("50"))
            print(f"If ETH +50%: IL = {proj.il_percent:.2f}%")

            # What if ETH drops 30%?
            proj = snapshot.projected_il("WETH", "USDC", Decimal("-30"))
            print(f"If ETH -30%: IL = {proj.il_percent:.2f}%")

            # Weighted pool (80/20 ETH/USDC)
            proj = snapshot.projected_il(
                "WETH", "USDC",
                price_change_pct=Decimal("100"),
                weight_a=Decimal("0.8"),
                weight_b=Decimal("0.2"),
            )
            print(f"80/20 pool, ETH +100%: IL = {proj.il_percent:.2f}%")
        """
        if self._il_calculator is None:
            raise ValueError("No IL calculator configured for MarketSnapshot")

        from .lp import InvalidPriceError, InvalidWeightError

        try:
            result = self._il_calculator.project_il(
                price_change_pct=price_change_pct,
                weight_a=weight_a,
                weight_b=weight_b,
            )
            return result
        except InvalidPriceError as e:
            raise ValueError(f"Invalid price change: {e.reason}") from e
        except InvalidWeightError as e:
            raise ValueError(f"Invalid weights: {e.reason}") from e
        except Exception as e:
            raise ValueError(f"Failed to project IL: {e}") from e

    # =========================================================================
    # Prediction Market Methods
    # =========================================================================

    def prediction(self, market_id: str) -> "PredictionMarket":
        """Get prediction market data.

        Fetches full market details for a prediction market by ID or slug.
        Uses lazy loading - only fetches prediction data when accessed.

        Args:
            market_id: Prediction market ID or URL slug
                Examples: "12345", "will-bitcoin-exceed-100k-2025"

        Returns:
            PredictionMarket with:
                - market_id: Internal market ID
                - condition_id: CTF condition ID (0x...)
                - question: Market question text
                - slug: URL slug
                - yes_price: Current YES outcome price (0-1)
                - no_price: Current NO outcome price (0-1)
                - spread: Bid-ask spread
                - volume_24h: 24-hour trading volume in USDC
                - liquidity: Current liquidity
                - end_date: Resolution deadline
                - is_active: Whether market is accepting orders
                - is_resolved: Whether market has been resolved

        Raises:
            PredictionUnavailableError: If market data cannot be retrieved
            ValueError: If no prediction provider is configured

        Example:
            # Get market by ID
            market = snapshot.prediction("12345")
            print(f"YES: {market.yes_price}, NO: {market.no_price}")

            # Get market by slug
            market = snapshot.prediction("will-btc-hit-100k")
            print(f"Question: {market.question}")
            print(f"24h Volume: ${market.volume_24h:,.2f}")

            # Check implied probability
            yes_prob = market.yes_price * 100
            print(f"Implied probability: {yes_prob:.1f}%")
        """
        if self._prediction_provider is None:
            raise ValueError("No prediction provider configured for MarketSnapshot")

        try:
            return self._prediction_provider.get_market(market_id)
        except Exception as e:
            raise PredictionUnavailableError(market_id, str(e)) from e

    def prediction_positions(
        self,
        market_id: str | None = None,
    ) -> list["PredictionPosition"]:
        """Get all open prediction market positions.

        Fetches positions from the prediction market provider. Can optionally
        filter by market ID.

        Args:
            market_id: Optional market ID or slug to filter by

        Returns:
            List of PredictionPosition objects with:
                - market_id: Market ID
                - condition_id: CTF condition ID
                - token_id: CLOB token ID
                - outcome: Position outcome (YES or NO)
                - size: Number of shares held
                - avg_price: Average entry price
                - current_price: Current market price
                - unrealized_pnl: Unrealized profit/loss
                - realized_pnl: Realized profit/loss
                - value: Current position value (size * current_price)

        Raises:
            PredictionUnavailableError: If positions cannot be retrieved
            ValueError: If no prediction provider is configured

        Example:
            # Get all positions
            positions = snapshot.prediction_positions()
            total_value = sum(p.value for p in positions)
            print(f"Total position value: ${total_value:,.2f}")

            # Get positions for a specific market
            positions = snapshot.prediction_positions("btc-100k")
            for pos in positions:
                print(f"{pos.outcome}: {pos.size} shares @ {pos.avg_price}")
                print(f"  Unrealized PnL: ${pos.unrealized_pnl:,.2f}")
        """
        if self._prediction_provider is None:
            raise ValueError("No prediction provider configured for MarketSnapshot")

        try:
            # Get positions filtered by market if specified
            if market_id:
                market = self._prediction_provider.get_market(market_id)
                return self._prediction_provider.get_positions(
                    wallet=self._wallet_address,
                    market_id=market.market_id,
                )
            return self._prediction_provider.get_positions(wallet=self._wallet_address)
        except Exception as e:
            raise PredictionUnavailableError(market_id or "all", f"Failed to get positions: {e}") from e

    def prediction_orders(
        self,
        market_id: str | None = None,
    ) -> list["PredictionOrder"]:
        """Get all open prediction market orders.

        Fetches open orders from the prediction market provider. Can optionally
        filter by market ID.

        Args:
            market_id: Optional market ID or slug to filter by

        Returns:
            List of PredictionOrder objects with:
                - order_id: Order ID
                - market_id: Market ID (token ID)
                - outcome: Order outcome (YES or NO)
                - side: Order side (BUY or SELL)
                - price: Order price
                - size: Order size in shares
                - filled_size: Filled amount
                - remaining_size: Remaining unfilled size
                - created_at: Order creation timestamp

        Raises:
            PredictionUnavailableError: If orders cannot be retrieved
            ValueError: If no prediction provider is configured

        Example:
            # Get all open orders
            orders = snapshot.prediction_orders()
            for order in orders:
                print(f"{order.side} {order.remaining_size} @ {order.price}")

            # Get orders for a specific market
            orders = snapshot.prediction_orders("btc-100k")
            buy_orders = [o for o in orders if o.side == "BUY"]
            print(f"Open buy orders: {len(buy_orders)}")
        """
        if self._prediction_provider is None:
            raise ValueError("No prediction provider configured for MarketSnapshot")

        try:
            return self._prediction_provider.get_open_orders(market_id)
        except Exception as e:
            raise PredictionUnavailableError(market_id or "all", f"Failed to get orders: {e}") from e

    def prediction_price(
        self,
        market_id: str,
        outcome: str = "YES",
    ) -> Decimal:
        """Get the current price of a prediction market outcome.

        Convenience method that fetches the market and returns the price
        for the specified outcome. This is equivalent to calling
        prediction(market_id).yes_price or .no_price.

        Args:
            market_id: Prediction market ID or URL slug
            outcome: Outcome to get price for - "YES" or "NO" (default: "YES")

        Returns:
            Price as Decimal (0-1 range, representing implied probability)

        Raises:
            PredictionUnavailableError: If market data cannot be retrieved
            ValueError: If no prediction provider is configured or invalid outcome

        Example:
            yes_price = snapshot.prediction_price("will-btc-hit-100k")
            print(f"YES price: {yes_price:.4f}")

            no_price = snapshot.prediction_price("will-btc-hit-100k", "NO")
            print(f"NO price: {no_price:.4f}")
        """
        market = self.prediction(market_id)
        outcome_upper = outcome.upper()
        if outcome_upper == "YES":
            return Decimal(str(market.yes_price))
        elif outcome_upper == "NO":
            return Decimal(str(market.no_price))
        else:
            raise ValueError(f"Invalid outcome '{outcome}'. Must be 'YES' or 'NO'.")

    # =========================================================================
    # Volatility Methods
    # =========================================================================

    def realized_vol(
        self,
        token: str,
        window_days: int = 30,
        timeframe: str = "1h",
        estimator: str = "close_to_close",
        *,
        ohlcv_limit: int | None = None,
    ) -> "DataEnvelope[VolatilityResult]":
        """Calculate realized volatility for a token.

        Fetches OHLCV candles via the configured providers and computes
        realized volatility using the specified estimator. Requires either
        an OHLCVRouter or an OHLCVModule for data, and a
        RealizedVolatilityCalculator for the computation.

        Args:
            token: Token symbol (e.g. "WETH", "ETH").
            window_days: Lookback window in calendar days. Default 30.
            timeframe: Candle timeframe (1m, 5m, 15m, 1h, 4h, 1d). Default "1h".
            estimator: "close_to_close" (default) or "parkinson".
            ohlcv_limit: Override for number of candles to fetch. If None,
                auto-calculated from window_days and timeframe.

        Returns:
            DataEnvelope[VolatilityResult] with INFORMATIONAL classification.

        Raises:
            VolatilityUnavailableError: If volatility cannot be calculated.
            ValueError: If no volatility calculator is configured.

        Example:
            result = snapshot.realized_vol("WETH", window_days=30, timeframe="1h")
            print(f"Annualized vol: {result.value.annualized_vol:.2%}")
            print(f"Daily vol: {result.value.daily_vol:.2%}")
        """
        if self._volatility_calculator is None:
            raise ValueError("No volatility calculator configured for MarketSnapshot")

        try:
            candles = self._fetch_candles_for_vol(token, window_days, timeframe, ohlcv_limit)
            result = self._volatility_calculator.realized_vol(
                candles=candles,
                window_days=window_days,
                timeframe=timeframe,
                estimator=estimator,
            )
            from .models import DataClassification, DataEnvelope, DataMeta

            meta = DataMeta(
                source="computed",
                observed_at=self._timestamp,
                finality="off_chain",
                confidence=1.0,
                cache_hit=False,
            )
            return DataEnvelope(value=result, meta=meta, classification=DataClassification.INFORMATIONAL)
        except VolatilityUnavailableError:
            raise
        except Exception as e:
            raise VolatilityUnavailableError(token, str(e)) from e

    def vol_cone(
        self,
        token: str,
        windows: list[int] | None = None,
        timeframe: str = "1h",
        estimator: str = "close_to_close",
        *,
        ohlcv_limit: int | None = None,
    ) -> "DataEnvelope[VolConeResult]":
        """Compute volatility cone: current vol vs historical percentile.

        For each window, calculates the current realized vol and compares
        it to the historical distribution of rolling vols over the full
        candle history.

        Args:
            token: Token symbol (e.g. "WETH").
            windows: Lookback windows in days. Default [7, 14, 30, 90].
            timeframe: Candle timeframe. Default "1h".
            estimator: "close_to_close" or "parkinson".
            ohlcv_limit: Override for number of candles to fetch. If None,
                auto-calculated for the largest window.

        Returns:
            DataEnvelope[VolConeResult] with INFORMATIONAL classification.

        Raises:
            VolConeUnavailableError: If vol cone cannot be calculated.
            ValueError: If no volatility calculator is configured.

        Example:
            cone = snapshot.vol_cone("WETH", windows=[7, 14, 30, 90])
            for entry in cone.value.entries:
                print(f"{entry.window_days}d: {entry.current_vol:.2%} (p{entry.percentile:.0f})")
        """
        if self._volatility_calculator is None:
            raise ValueError("No volatility calculator configured for MarketSnapshot")

        if windows is None:
            windows = [7, 14, 30, 90]

        try:
            max_window = max(windows)
            # Fetch enough candles for the largest window plus extra for rolling history.
            candles = self._fetch_candles_for_vol(token, max_window * 3, timeframe, ohlcv_limit)
            result = self._volatility_calculator.vol_cone(
                candles=candles,
                windows=windows,
                timeframe=timeframe,
                estimator=estimator,
                token=token,
            )
            from .models import DataClassification, DataEnvelope, DataMeta

            meta = DataMeta(
                source="computed",
                observed_at=self._timestamp,
                finality="off_chain",
                confidence=1.0,
                cache_hit=False,
            )
            return DataEnvelope(value=result, meta=meta, classification=DataClassification.INFORMATIONAL)
        except VolConeUnavailableError:
            raise
        except Exception as e:
            raise VolConeUnavailableError(token, str(e)) from e

    def portfolio_risk(
        self,
        pnl_series: list[float],
        total_value_usd: Decimal | None = None,
        return_interval: str = "1d",
        risk_free_rate: Decimal = Decimal("0"),
        var_method: str = "parametric",
        timestamps: list[datetime] | None = None,
        benchmark_eth_returns: list[float] | None = None,
        benchmark_btc_returns: list[float] | None = None,
    ) -> "DataEnvelope[PortfolioRisk]":
        """Calculate portfolio risk metrics from a PnL return series.

        Computes Sharpe ratio, Sortino ratio, VaR, CVaR, and drawdown
        with explicit conventions for unambiguous results.

        Args:
            pnl_series: List of periodic returns as fractions (0.01 = 1% gain).
            total_value_usd: Current portfolio value in USD. Defaults to Decimal("0").
            return_interval: Periodicity of returns (1d, 1h, etc.).
            risk_free_rate: Risk-free rate per period as a decimal.
            var_method: VaR method: "parametric", "historical", or "cornish_fisher".
            timestamps: Optional timestamps for each return.
            benchmark_eth_returns: Optional ETH returns for beta calculation.
            benchmark_btc_returns: Optional BTC returns for beta calculation.

        Returns:
            DataEnvelope[PortfolioRisk] with INFORMATIONAL classification.

        Raises:
            PortfolioRiskUnavailableError: If risk metrics cannot be calculated.
            ValueError: If no risk calculator is configured.

        Example:
            risk = snapshot.portfolio_risk(pnl_series, total_value_usd=Decimal("100000"))
            print(f"Sharpe: {risk.value.sharpe_ratio:.2f}")
            print(f"VaR 95%: ${risk.value.var_95}")
        """
        if self._risk_calculator is None:
            raise ValueError("No risk calculator configured for MarketSnapshot")

        try:
            from .risk.metrics import VaRMethod

            method_map = {
                "parametric": VaRMethod.PARAMETRIC,
                "historical": VaRMethod.HISTORICAL,
                "cornish_fisher": VaRMethod.CORNISH_FISHER,
            }
            vm = method_map.get(var_method)
            if vm is None:
                raise ValueError(f"Unknown var_method '{var_method}'. Use: {list(method_map.keys())}")

            result = self._risk_calculator.portfolio_risk(
                pnl_series=pnl_series,
                total_value_usd=total_value_usd or Decimal("0"),
                return_interval=return_interval,
                risk_free_rate=risk_free_rate,
                var_method=vm,
                timestamps=timestamps,
                benchmark_eth_returns=benchmark_eth_returns,
                benchmark_btc_returns=benchmark_btc_returns,
            )
            from .models import DataClassification, DataEnvelope, DataMeta

            meta = DataMeta(
                source="computed",
                observed_at=self._timestamp,
                finality="off_chain",
                confidence=1.0,
                cache_hit=False,
            )
            return DataEnvelope(value=result, meta=meta, classification=DataClassification.INFORMATIONAL)
        except PortfolioRiskUnavailableError:
            raise
        except Exception as e:
            raise PortfolioRiskUnavailableError(str(e)) from e

    def rolling_sharpe(
        self,
        pnl_series: list[float],
        window_days: int = 30,
        return_interval: str = "1d",
        risk_free_rate: Decimal = Decimal("0"),
        timestamps: list[datetime] | None = None,
    ) -> "DataEnvelope[RollingSharpeResult]":
        """Compute rolling Sharpe ratio over a PnL series.

        Args:
            pnl_series: List of periodic returns as fractions.
            window_days: Rolling window in days. Default 30.
            return_interval: Periodicity of returns (1d, 1h, etc.).
            risk_free_rate: Risk-free rate per period.
            timestamps: Optional timestamps aligned with pnl_series.

        Returns:
            DataEnvelope[RollingSharpeResult] with INFORMATIONAL classification.

        Raises:
            RollingSharpeUnavailableError: If rolling Sharpe cannot be computed.
            ValueError: If no risk calculator is configured.

        Example:
            result = snapshot.rolling_sharpe(pnl_series, window_days=30)
            for entry in result.value.entries:
                print(f"{entry.timestamp}: Sharpe={entry.sharpe:.2f}")
        """
        if self._risk_calculator is None:
            raise ValueError("No risk calculator configured for MarketSnapshot")

        try:
            result = self._risk_calculator.rolling_sharpe(
                pnl_series=pnl_series,
                window_days=window_days,
                return_interval=return_interval,
                risk_free_rate=risk_free_rate,
                timestamps=timestamps,
            )
            from .models import DataClassification, DataEnvelope, DataMeta

            meta = DataMeta(
                source="computed",
                observed_at=self._timestamp,
                finality="off_chain",
                confidence=1.0,
                cache_hit=False,
            )
            return DataEnvelope(value=result, meta=meta, classification=DataClassification.INFORMATIONAL)
        except RollingSharpeUnavailableError:
            raise
        except Exception as e:
            raise RollingSharpeUnavailableError(str(e)) from e

    # -------------------------------------------------------------------------
    # Pool Analytics & Yield Methods
    # -------------------------------------------------------------------------

    def pool_analytics(
        self,
        pool_address: str,
        chain: str | None = None,
        protocol: str | None = None,
    ) -> "DataEnvelope[PoolAnalytics]":
        """Get real-time analytics for a pool (TVL, volume, fee APR/APY).

        Fetches from DeFi Llama (primary) or GeckoTerminal (fallback).
        Cache TTL: 5 minutes.

        Args:
            pool_address: Pool contract address.
            chain: Chain name. Defaults to strategy chain.
            protocol: Optional protocol hint (e.g. "uniswap_v3").

        Returns:
            DataEnvelope[PoolAnalytics] with INFORMATIONAL classification.

        Raises:
            PoolAnalyticsUnavailableError: If analytics cannot be retrieved.
            ValueError: If no pool analytics reader is configured.
        """
        if self._pool_analytics_reader is None:
            raise ValueError("No pool analytics reader configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()

        try:
            return self._pool_analytics_reader.get_pool_analytics(
                pool_address=pool_address,
                chain=target_chain,
                protocol=protocol,
            )
        except Exception as e:
            raise PoolAnalyticsUnavailableError(
                pool_address,
                f"Failed to fetch analytics for {pool_address} on {target_chain}: {e}",
            ) from e

    def best_pool(
        self,
        token_a: str,
        token_b: str,
        chain: str | None = None,
        metric: str = "fee_apr",
        protocols: list[str] | None = None,
    ) -> "DataEnvelope[PoolAnalyticsResult]":
        """Find the best pool for a token pair based on a metric.

        Searches DeFi Llama for matching pools and ranks by metric.

        Args:
            token_a: First token symbol (e.g. "WETH").
            token_b: Second token symbol (e.g. "USDC").
            chain: Chain name. Defaults to strategy chain.
            metric: Sorting metric: "fee_apr", "fee_apy", "tvl_usd", "volume_24h_usd".
            protocols: Optional list of protocols to filter by.

        Returns:
            DataEnvelope[PoolAnalyticsResult] with the best pool.

        Raises:
            PoolAnalyticsUnavailableError: If no pools found or all providers fail.
            ValueError: If no pool analytics reader is configured.
        """
        if self._pool_analytics_reader is None:
            raise ValueError("No pool analytics reader configured for MarketSnapshot")

        target_chain = (chain or self._chain).lower()

        try:
            return self._pool_analytics_reader.best_pool(
                token_a=token_a,
                token_b=token_b,
                chain=target_chain,
                metric=metric,
                protocols=protocols,
            )
        except Exception as e:
            raise PoolAnalyticsUnavailableError(
                f"{token_a}/{token_b}",
                f"Failed to find best pool on {target_chain}: {e}",
            ) from e

    def yield_opportunities(
        self,
        token: str,
        chains: list[str] | None = None,
        min_tvl: float = 100_000,
        sort_by: str = "apy",
    ) -> "DataEnvelope[list[YieldOpportunity]]":
        """Find yield opportunities for a token across protocols and chains.

        Searches DeFi Llama yields API for matching pools, sorted by the
        chosen metric. Cache TTL: 15 minutes.

        Args:
            token: Token symbol (e.g. "USDC", "WETH").
            chains: Optional list of chains to filter. None means all.
            min_tvl: Minimum TVL in USD. Default $100k.
            sort_by: Sort field: "apy", "tvl", "risk_score". Default "apy".

        Returns:
            DataEnvelope[list[YieldOpportunity]] sorted by chosen metric.

        Raises:
            YieldOpportunitiesUnavailableError: If data cannot be retrieved.
            ValueError: If no yield aggregator is configured.
        """
        if self._yield_aggregator is None:
            raise ValueError("No yield aggregator configured for MarketSnapshot")

        try:
            return self._yield_aggregator.get_yield_opportunities(
                token=token,
                chains=chains,
                min_tvl=min_tvl,
                sort_by=sort_by,
            )
        except Exception as e:
            raise YieldOpportunitiesUnavailableError(
                token,
                f"Failed to fetch yield opportunities: {e}",
            ) from e

    def _fetch_candles_for_vol(
        self,
        token: str,
        window_days: int,
        timeframe: str,
        ohlcv_limit: int | None,
    ) -> list:
        """Fetch OHLCV candles for volatility calculations.

        Uses OHLCVRouter (preferred) or legacy OHLCVModule, then converts
        the DataFrame result to a list of OHLCVCandle objects.
        """
        from .interfaces import OHLCVCandle

        # Compute candle count from window.
        hours_per_candle = {"1m": 1 / 60, "5m": 5 / 60, "15m": 0.25, "1h": 1.0, "4h": 4.0, "1d": 24.0}
        if timeframe not in hours_per_candle:
            raise ValueError(f"Unsupported timeframe '{timeframe}'")
        limit = ohlcv_limit or max(int(window_days * 24 / hours_per_candle[timeframe]), 100)

        df = self.ohlcv(token, timeframe=timeframe, limit=limit)

        if df.empty:
            raise VolatilityUnavailableError(token, "No OHLCV data available")

        candles = []
        for _, row in df.iterrows():
            candles.append(
                OHLCVCandle(
                    timestamp=row["timestamp"] if hasattr(row["timestamp"], "tzinfo") else row["timestamp"],
                    open=Decimal(str(row["open"])),
                    high=Decimal(str(row["high"])),
                    low=Decimal(str(row["low"])),
                    close=Decimal(str(row["close"])),
                    volume=Decimal(str(row["volume"]))
                    if not (hasattr(row["volume"], "__float__") and str(row["volume"]) == "nan")
                    else None,
                )
            )
        return candles

    # =========================================================================
    # Position Health Monitoring
    # =========================================================================

    def position_health(
        self,
        protocol: str,
        market_id: str,
        rpc_url: str | None = None,
        collateral_price_usd: Decimal | None = None,
        debt_price_usd: Decimal | None = None,
    ) -> "PositionHealth":
        """Get health factor for a lending position.

        Reads on-chain position data and computes health factor for
        Morpho Blue or Aave V3 positions.

        Args:
            protocol: "morpho_blue" or "aave_v3"
            market_id: Protocol-specific market identifier
            rpc_url: RPC endpoint (uses default if not provided)
            collateral_price_usd: Optional override for collateral price
            debt_price_usd: Optional override for debt token price

        Returns:
            PositionHealth with computed health factor

        Raises:
            MarketSnapshotError: If health data cannot be retrieved
        """
        from .position_health import PositionHealthProvider

        try:
            url = rpc_url or self._get_rpc_url()
            provider = PositionHealthProvider(
                rpc_url=url,
                chain=self._chain,
                price_oracle=self._price_oracle,
                gateway_client=self._gateway_client,
            )
            return provider.get_health(
                protocol=protocol,
                market_id=market_id,
                user_address=self._wallet_address,
                collateral_price_usd=collateral_price_usd,
                debt_price_usd=debt_price_usd,
            )
        except Exception as e:
            raise HealthUnavailableError(f"Position health unavailable: {e}") from e

    def pt_position_health(
        self,
        morpho_market_id: str,
        pendle_market_address: str,
        rpc_url: str | None = None,
        collateral_price_usd: Decimal | None = None,
        debt_price_usd: Decimal | None = None,
    ) -> "PTPositionHealth":
        """Get extended health data for a PT-collateral position.

        Combines Morpho Blue position data with Pendle market metrics
        (implied APY, maturity risk) for comprehensive risk assessment.

        Args:
            morpho_market_id: Morpho Blue market ID
            pendle_market_address: Pendle market address for the PT
            rpc_url: RPC endpoint (uses default if not provided)
            collateral_price_usd: Override for PT collateral price
            debt_price_usd: Override for debt token price

        Returns:
            PTPositionHealth with Morpho + Pendle risk metrics

        Raises:
            MarketSnapshotError: If health data cannot be retrieved
        """
        from .position_health import PositionHealthProvider

        try:
            url = rpc_url or self._get_rpc_url()
            provider = PositionHealthProvider(
                rpc_url=url,
                chain=self._chain,
                price_oracle=self._price_oracle,
                gateway_client=self._gateway_client,
            )
            return provider.get_pt_position_health(
                morpho_market_id=morpho_market_id,
                pendle_market_address=pendle_market_address,
                user_address=self._wallet_address,
                collateral_price_usd=collateral_price_usd,
                debt_price_usd=debt_price_usd,
            )
        except Exception as e:
            raise HealthUnavailableError(f"PT position health unavailable: {e}") from e

    def _get_rpc_url(self) -> str:
        """Get RPC URL from chain configuration via centralized resolver."""
        from almanak.gateway.utils.rpc_provider import get_rpc_url

        return get_rpc_url(self._chain)

    def to_dict(self) -> dict[str, Any]:
        """Convert snapshot state to dictionary for serialization.

        Returns:
            Dictionary with snapshot metadata and cached values
        """
        return {
            "chain": self._chain,
            "wallet_address": self._wallet_address,
            "timestamp": self._timestamp.isoformat(),
            "cached_prices": {k: str(v) for k, v in self._price_cache.items()},
            "cached_balances": {k: str(v) for k, v in self._balance_cache.items()},
            "cached_rsi": self._rsi_cache.copy(),
        }


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "MarketSnapshot",
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
]
