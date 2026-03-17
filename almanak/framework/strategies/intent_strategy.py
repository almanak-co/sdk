"""IntentStrategy Base Class for simplified strategy authoring.

This module provides the IntentStrategy base class that allows developers to write
strategies using the high-level Intent pattern. Strategies only need to implement
a decide() method that returns an Intent, and the framework handles:

1. Auto-compiling intents to ActionBundles
2. Auto-generating state machines for execution
3. Managing hot-reloadable configuration
4. Providing market data through MarketSnapshot helper

Example:
    from almanak.framework.strategies.intent_strategy import IntentStrategy, MarketSnapshot
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
import concurrent.futures
import logging
from abc import abstractmethod
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal, Optional, TypeVar

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

from ..intents import (
    CompilationStatus,
    DecideResult,
    HoldIntent,
    Intent,
    IntentCompiler,
    IntentSequence,
    IntentStateMachine,
    StateMachineConfig,
    StepResult,
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

logger = logging.getLogger(__name__)


# =============================================================================
# Market Snapshot Helper
# =============================================================================


@dataclass
class TokenBalance:
    """Balance information for a single token.

    Supports numeric comparisons so strategy authors can write:
        if market.balance("ETH") > Decimal("1.0"): ...
        amount = min(trade_size, market.balance("USDC"))

    Comparisons delegate to the ``balance`` field (native units).

    Attributes:
        symbol: Token symbol (e.g., "ETH", "USDC")
        balance: Token balance in native units
        balance_usd: Token balance in USD terms
        address: Token contract address
    """

    symbol: str
    balance: Decimal
    balance_usd: Decimal
    address: str = ""

    # -- numeric protocol --------------------------------------------------

    def _to_decimal(self, other: object) -> Decimal | None:
        """Coerce other to Decimal for comparison, or return None."""
        if isinstance(other, TokenBalance):
            return other.balance
        if isinstance(other, Decimal):
            return other
        if isinstance(other, int | float):
            return Decimal(str(other))
        return None

    def __eq__(self, other: object) -> bool:
        if isinstance(other, TokenBalance):
            return self.symbol == other.symbol and self.balance == other.balance and self.address == other.address
        val = self._to_decimal(other)
        if val is None:
            return NotImplemented
        return self.balance == val

    def __lt__(self, other: object) -> bool:
        val = self._to_decimal(other)
        if val is None:
            return NotImplemented
        return self.balance < val

    def __le__(self, other: object) -> bool:
        val = self._to_decimal(other)
        if val is None:
            return NotImplemented
        return self.balance <= val

    def __gt__(self, other: object) -> bool:
        val = self._to_decimal(other)
        if val is None:
            return NotImplemented
        return self.balance > val

    def __ge__(self, other: object) -> bool:
        val = self._to_decimal(other)
        if val is None:
            return NotImplemented
        return self.balance >= val

    def __hash__(self) -> int:
        # Hash only on balance to satisfy the Python invariant: a == b => hash(a) == hash(b).
        # Since __eq__ returns True for `tb == Decimal(100)`, hash must match hash(Decimal(100)).
        # Different tokens with the same balance will hash-collide but __eq__ distinguishes them.
        return hash(self.balance)

    def __float__(self) -> float:
        return float(self.balance)

    def __int__(self) -> int:
        return int(self.balance)

    def __format__(self, format_spec: str) -> str:
        if format_spec:
            return format(self.balance, format_spec)
        return str(self.balance)

    def __repr__(self) -> str:
        return f"TokenBalance(symbol={self.symbol!r}, balance={self.balance}, balance_usd={self.balance_usd})"


@dataclass
class PriceData:
    """Price data for a token.

    Attributes:
        price: Current price in USD
        price_24h_ago: Price 24 hours ago in USD
        change_24h_pct: 24-hour price change percentage
        high_24h: 24-hour high
        low_24h: 24-hour low
        timestamp: When the price was fetched
    """

    price: Decimal
    price_24h_ago: Decimal = Decimal("0")
    change_24h_pct: Decimal = Decimal("0")
    high_24h: Decimal = Decimal("0")
    low_24h: Decimal = Decimal("0")
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class RSIData:
    """RSI (Relative Strength Index) data for a token.

    Supports numeric operations so strategy authors can write:
        rsi = market.rsi("ETH")
        if rsi > 70: ...          # comparison against int/float
        round(rsi, 2)             # rounding
        f"{rsi:.2f}"              # f-string formatting
        float(rsi)                # explicit float conversion

    Attributes:
        value: Current RSI value (0-100)
        period: RSI period used (e.g., 14)
        overbought: Overbought threshold (default 70)
        oversold: Oversold threshold (default 30)
        signal: Signal based on RSI (BUY, SELL, or HOLD)
    """

    value: Decimal
    period: int = 14
    overbought: Decimal = Decimal("70")
    oversold: Decimal = Decimal("30")

    def __float__(self) -> float:
        return float(self.value)

    def __round__(self, ndigits: int | None = None) -> int | float:
        if ndigits is None:
            return round(float(self.value))
        return round(float(self.value), ndigits)

    def __format__(self, format_spec: str) -> str:
        return format(float(self.value), format_spec) if format_spec else str(self.value)

    def __gt__(self, other: object) -> bool:
        if isinstance(other, int | float | Decimal):
            return self.value > Decimal(str(other))
        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, int | float | Decimal):
            return self.value < Decimal(str(other))
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, int | float | Decimal):
            return self.value >= Decimal(str(other))
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, int | float | Decimal):
            return self.value <= Decimal(str(other))
        return NotImplemented

    def __eq__(self, other: object) -> bool:
        if isinstance(other, RSIData):
            return self.value == other.value
        if isinstance(other, int | float | Decimal):
            return self.value == Decimal(str(other))
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value)

    @property
    def signal(self) -> str:
        """Get signal based on RSI value."""
        if self.value <= self.oversold:
            return "BUY"
        elif self.value >= self.overbought:
            return "SELL"
        return "HOLD"

    @property
    def is_oversold(self) -> bool:
        """Check if RSI indicates oversold condition."""
        return self.value <= self.oversold

    @property
    def is_overbought(self) -> bool:
        """Check if RSI indicates overbought condition."""
        return self.value >= self.overbought


@dataclass
class MACDData:
    """MACD (Moving Average Convergence Divergence) data for a token.

    Attributes:
        macd_line: MACD line value (fast EMA - slow EMA)
        signal_line: Signal line value (EMA of MACD line)
        histogram: MACD histogram (MACD line - signal line)
        fast_period: Fast EMA period (default 12)
        slow_period: Slow EMA period (default 26)
        signal_period: Signal EMA period (default 9)
    """

    macd_line: Decimal
    signal_line: Decimal
    histogram: Decimal
    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9

    @property
    def is_bullish_crossover(self) -> bool:
        """Check if MACD line is above signal line (bullish)."""
        return self.histogram > 0

    @property
    def is_bearish_crossover(self) -> bool:
        """Check if MACD line is below signal line (bearish)."""
        return self.histogram < 0

    @property
    def signal(self) -> str:
        """Get signal based on MACD histogram."""
        if self.histogram > 0:
            return "BUY"
        elif self.histogram < 0:
            return "SELL"
        return "HOLD"


@dataclass
class BollingerBandsData:
    """Bollinger Bands data for a token.

    Numeric operations use percent_b (price position within bands, 0-1):
        bb = market.bollinger_bands("ETH")
        f"{bb:.2f}"               # formats percent_b
        float(bb)                 # returns percent_b as float

    Attributes:
        upper_band: Upper band value (middle + std_dev * multiplier)
        middle_band: Middle band value (SMA)
        lower_band: Lower band value (middle - std_dev * multiplier)
        bandwidth: Band width as percentage ((upper - lower) / middle)
        percent_b: Price position relative to bands (0 = lower, 1 = upper)
        period: SMA period (default 20)
        std_dev: Standard deviation multiplier (default 2.0)
    """

    upper_band: Decimal
    middle_band: Decimal
    lower_band: Decimal
    bandwidth: Decimal = Decimal("0")
    percent_b: Decimal = Decimal("0.5")
    period: int = 20
    std_dev: float = 2.0

    def __float__(self) -> float:
        return float(self.percent_b)

    def __format__(self, format_spec: str) -> str:
        return format(float(self.percent_b), format_spec) if format_spec else str(self.percent_b)

    @property
    def is_oversold(self) -> bool:
        """Check if price is below lower band (oversold)."""
        return self.percent_b < Decimal("0")

    @property
    def is_overbought(self) -> bool:
        """Check if price is above upper band (overbought)."""
        return self.percent_b > Decimal("1")

    @property
    def is_squeeze(self) -> bool:
        """Check if bands are tight (low volatility squeeze)."""
        return self.bandwidth < Decimal("0.05")

    @property
    def signal(self) -> str:
        """Get signal based on Bollinger Bands position."""
        if self.is_oversold:
            return "BUY"
        elif self.is_overbought:
            return "SELL"
        return "HOLD"


@dataclass
class StochasticData:
    """Stochastic Oscillator data for a token.

    Attributes:
        k_value: %K value (fast stochastic, 0-100)
        d_value: %D value (slow stochastic, SMA of %K, 0-100)
        k_period: %K period (default 14)
        d_period: %D period (default 3)
        overbought: Overbought threshold (default 80)
        oversold: Oversold threshold (default 20)
    """

    k_value: Decimal
    d_value: Decimal
    k_period: int = 14
    d_period: int = 3
    overbought: Decimal = Decimal("80")
    oversold: Decimal = Decimal("20")

    @property
    def is_oversold(self) -> bool:
        """Check if stochastic indicates oversold condition."""
        return self.k_value <= self.oversold

    @property
    def is_overbought(self) -> bool:
        """Check if stochastic indicates overbought condition."""
        return self.k_value >= self.overbought

    @property
    def is_bullish_crossover(self) -> bool:
        """Check if %K crossed above %D (bullish signal)."""
        return self.k_value > self.d_value and self.is_oversold

    @property
    def is_bearish_crossover(self) -> bool:
        """Check if %K crossed below %D (bearish signal)."""
        return self.k_value < self.d_value and self.is_overbought

    @property
    def signal(self) -> str:
        """Get signal based on Stochastic values."""
        if self.is_bullish_crossover:
            return "BUY"
        elif self.is_bearish_crossover:
            return "SELL"
        return "HOLD"


@dataclass
class ATRData:
    """ATR (Average True Range) data for a token.

    Numeric operations use value (ATR in price units):
        atr = market.atr("ETH")
        f"{atr:.2f}"              # formats ATR value
        float(atr)                # returns ATR value as float

    Attributes:
        value: ATR value in price units
        value_percent: ATR as percentage points of current price (e.g., 2.62 means 2.62%, not 0.0262)
        period: ATR period (default 14)
        volatility_threshold: Max volatility threshold in percentage points (e.g., 5.0 means 5.0%)
    """

    value: Decimal
    value_percent: Decimal = Decimal("0")
    period: int = 14
    volatility_threshold: Decimal = Decimal("5.0")

    def __float__(self) -> float:
        return float(self.value)

    def __format__(self, format_spec: str) -> str:
        return format(float(self.value), format_spec) if format_spec else str(self.value)

    @property
    def is_high_volatility(self) -> bool:
        """Check if volatility is above threshold (risky to trade)."""
        return self.value_percent > self.volatility_threshold

    @property
    def is_low_volatility(self) -> bool:
        """Check if volatility is below threshold (safe to trade)."""
        return self.value_percent <= self.volatility_threshold

    @property
    def signal(self) -> str:
        """Get signal based on ATR volatility gate."""
        if self.is_low_volatility:
            return "TRADE"  # Safe to trade
        return "WAIT"  # High volatility, wait


@dataclass
class MAData:
    """Moving Average data for a token.

    Attributes:
        value: Moving average value
        ma_type: Type of moving average ("SMA", "EMA", "WMA")
        period: MA period
        current_price: Current price for comparison
    """

    value: Decimal
    ma_type: str = "SMA"
    period: int = 20
    current_price: Decimal = Decimal("0")

    @property
    def is_price_above(self) -> bool:
        """Check if current price is above the MA."""
        return self.current_price > self.value

    @property
    def is_price_below(self) -> bool:
        """Check if current price is below the MA."""
        return self.current_price < self.value

    @property
    def signal(self) -> str:
        """Get signal based on price vs MA."""
        if self.is_price_above:
            return "BULLISH"
        elif self.is_price_below:
            return "BEARISH"
        return "NEUTRAL"


@dataclass
class ADXData:
    """ADX (Average Directional Index) data for a token.

    Attributes:
        adx: ADX value (0-100, measures trend strength)
        plus_di: +DI value (positive directional indicator)
        minus_di: -DI value (negative directional indicator)
        period: ADX period (default 14)
        trend_threshold: Threshold for strong trend (default 25)
    """

    adx: Decimal
    plus_di: Decimal
    minus_di: Decimal
    period: int = 14
    trend_threshold: Decimal = Decimal("25")

    @property
    def is_strong_trend(self) -> bool:
        """Check if ADX indicates a strong trend."""
        return self.adx >= self.trend_threshold

    @property
    def is_uptrend(self) -> bool:
        """Check if in uptrend (+DI > -DI with strong trend)."""
        return self.is_strong_trend and self.plus_di > self.minus_di

    @property
    def is_downtrend(self) -> bool:
        """Check if in downtrend (-DI > +DI with strong trend)."""
        return self.is_strong_trend and self.minus_di > self.plus_di

    @property
    def signal(self) -> str:
        """Get signal based on ADX trend analysis."""
        if self.is_uptrend:
            return "BUY"
        elif self.is_downtrend:
            return "SELL"
        return "HOLD"


@dataclass
class OBVData:
    """OBV (On-Balance Volume) data for a token.

    Attributes:
        obv: Current OBV value
        signal_line: Signal line (SMA of OBV)
        signal_period: Signal period (default 21)
    """

    obv: Decimal
    signal_line: Decimal
    signal_period: int = 21

    @property
    def is_bullish(self) -> bool:
        """Check if OBV is above signal (buying pressure)."""
        return self.obv > self.signal_line

    @property
    def is_bearish(self) -> bool:
        """Check if OBV is below signal (selling pressure)."""
        return self.obv < self.signal_line

    @property
    def signal(self) -> str:
        """Get signal based on OBV analysis."""
        if self.is_bullish:
            return "BUY"
        elif self.is_bearish:
            return "SELL"
        return "HOLD"


@dataclass
class CCIData:
    """CCI (Commodity Channel Index) data for a token.

    Attributes:
        value: CCI value
        period: CCI period (default 20)
        upper_level: Overbought level (default 100)
        lower_level: Oversold level (default -100)
    """

    value: Decimal
    period: int = 20
    upper_level: Decimal = Decimal("100")
    lower_level: Decimal = Decimal("-100")

    @property
    def is_oversold(self) -> bool:
        """Check if CCI indicates oversold condition."""
        return self.value <= self.lower_level

    @property
    def is_overbought(self) -> bool:
        """Check if CCI indicates overbought condition."""
        return self.value >= self.upper_level

    @property
    def signal(self) -> str:
        """Get signal based on CCI."""
        if self.is_oversold:
            return "BUY"
        elif self.is_overbought:
            return "SELL"
        return "HOLD"


@dataclass
class IchimokuData:
    """Ichimoku Cloud data for a token.

    Attributes:
        tenkan_sen: Conversion line (9-period midpoint)
        kijun_sen: Base line (26-period midpoint)
        senkou_span_a: Leading span A
        senkou_span_b: Leading span B
        current_price: Current price for cloud position check
        tenkan_period: Tenkan-sen period (default 9)
        kijun_period: Kijun-sen period (default 26)
        senkou_b_period: Senkou Span B period (default 52)
    """

    tenkan_sen: Decimal
    kijun_sen: Decimal
    senkou_span_a: Decimal
    senkou_span_b: Decimal
    current_price: Decimal = Decimal("0")
    tenkan_period: int = 9
    kijun_period: int = 26
    senkou_b_period: int = 52

    @property
    def cloud_top(self) -> Decimal:
        """Get the top of the cloud."""
        return max(self.senkou_span_a, self.senkou_span_b)

    @property
    def cloud_bottom(self) -> Decimal:
        """Get the bottom of the cloud."""
        return min(self.senkou_span_a, self.senkou_span_b)

    @property
    def is_bullish_crossover(self) -> bool:
        """Check if Tenkan crossed above Kijun (bullish)."""
        return self.tenkan_sen > self.kijun_sen

    @property
    def is_bearish_crossover(self) -> bool:
        """Check if Tenkan crossed below Kijun (bearish)."""
        return self.tenkan_sen < self.kijun_sen

    @property
    def is_above_cloud(self) -> bool:
        """Check if price is above the cloud."""
        return self.current_price > self.cloud_top

    @property
    def is_below_cloud(self) -> bool:
        """Check if price is below the cloud."""
        return self.current_price < self.cloud_bottom

    @property
    def signal(self) -> str:
        """Get signal based on Ichimoku analysis."""
        if self.is_bullish_crossover:
            return "BUY"
        elif self.is_bearish_crossover:
            return "SELL"
        return "HOLD"


# Type for price oracle function
PriceOracle = Callable[[str, str], Decimal]

# Type for RSI provider function (accepts optional timeframe kwarg for backward compat)
RSIProvider = Callable[..., RSIData]

# Type for balance provider function
BalanceProvider = Callable[[str], TokenBalance]


class IndicatorProvider:
    """Provider that wraps all indicator calculators for synchronous access.

    Each method corresponds to a MarketSnapshot accessor. The runner creates
    this once and injects it into the strategy, so all indicators work
    out of the box without strategy authors needing to manage calculators.
    """

    def __init__(
        self,
        macd: Callable[..., MACDData] | None = None,
        bollinger: Callable[..., BollingerBandsData] | None = None,
        stochastic: Callable[..., StochasticData] | None = None,
        atr: Callable[..., ATRData] | None = None,
        sma: Callable[..., MAData] | None = None,
        ema: Callable[..., MAData] | None = None,
        adx: Callable[..., ADXData] | None = None,
        obv: Callable[..., OBVData] | None = None,
        cci: Callable[..., CCIData] | None = None,
        ichimoku: Callable[..., IchimokuData] | None = None,
    ):
        self.macd = macd
        self.bollinger = bollinger
        self.stochastic = stochastic
        self.atr = atr
        self.sma = sma
        self.ema = ema
        self.adx = adx
        self.obv = obv
        self.cci = cci
        self.ichimoku = ichimoku


class MarketSnapshot:
    """Helper class providing market data access for strategy decisions.

    MarketSnapshot provides a simple interface for strategies to access:
    - Token prices
    - RSI values
    - Wallet balances
    - Position information

    The snapshot is populated with data at the start of each iteration,
    allowing strategies to make decisions based on current market conditions.

    Example:
        def decide(self, market: MarketSnapshot) -> Optional[Intent]:
            # Get ETH price
            eth_price = market.price("ETH")

            # Get RSI
            rsi = market.rsi("ETH", period=14)
            if rsi.is_oversold:
                return Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))

            # Check balance
            balance = market.balance("USDC")
            if balance.balance_usd < Decimal("100"):
                return Intent.hold(reason="Insufficient balance")

            return Intent.hold()
    """

    def __init__(
        self,
        chain: str,
        wallet_address: str,
        price_oracle: PriceOracle | None = None,
        rsi_provider: RSIProvider | None = None,
        balance_provider: BalanceProvider | None = None,
        timestamp: datetime | None = None,
        wallet_activity_provider: "WalletActivityProvider | None" = None,
        prediction_provider: Any | None = None,
        indicator_provider: IndicatorProvider | None = None,
        multi_dex_service: Any | None = None,
        rate_monitor: Any | None = None,
    ) -> None:
        """Initialize market snapshot.

        Args:
            chain: Chain name (e.g., "arbitrum", "ethereum")
            wallet_address: Wallet address for balance queries
            price_oracle: Function to fetch prices (token, quote) -> price
            rsi_provider: Function to calculate RSI (token, period[, timeframe=]) -> RSIData
            balance_provider: Function to fetch balances (token) -> TokenBalance
            timestamp: Snapshot timestamp (defaults to now)
            wallet_activity_provider: Provider for leader wallet activity signals
            prediction_provider: PredictionMarketDataProvider for prediction market data
            indicator_provider: IndicatorProvider for calculator-backed TA indicators
            multi_dex_service: MultiDexService for cross-DEX price comparison
            rate_monitor: RateMonitor instance for lending rate queries
        """
        self._chain = chain
        self._wallet_address = wallet_address
        self._price_oracle = price_oracle
        self._rsi_provider = rsi_provider
        self._balance_provider = balance_provider
        self._timestamp = timestamp or datetime.now(UTC)
        self._wallet_activity_provider = wallet_activity_provider
        self._prediction_provider = prediction_provider
        self._indicator_provider = indicator_provider
        self._multi_dex_service = multi_dex_service
        self._rate_monitor = rate_monitor

        # Cache for fetched data
        self._price_cache: dict[str, PriceData] = {}
        self._rsi_cache: dict[tuple[str, str, int], RSIData] = {}
        self._balance_cache: dict[str, TokenBalance] = {}

        # Per-indicator caches (tuple keys for timeframe-aware caching)
        self._macd_cache: dict[tuple[str, str, int, int, int], MACDData] = {}
        self._bollinger_cache: dict[tuple[str, str, int, float], BollingerBandsData] = {}
        self._stochastic_cache: dict[tuple[str, str, int, int], StochasticData] = {}
        self._atr_cache: dict[tuple[str, str, int], ATRData] = {}
        self._ma_cache: dict[tuple[str, str, str, int], MAData] = {}

        # Lending rate cache (populated by lending_rate() or set_lending_rate())
        self._lending_rate_cache: dict[str, Any] = {}

        # Pre-populated data (can be set directly)
        self._prices: dict[str, Decimal] = {}
        self._balances: dict[str, TokenBalance] = {}
        self._rsi_values: dict[str, tuple[RSIData, str | None]] = {}

        # Pre-populated indicator data (for all TA indicators)
        # Stored as (data, timeframe) tuples; timeframe=None matches any query
        self._macd_values: dict[str, tuple[MACDData, str | None]] = {}
        self._bollinger_values: dict[str, tuple[BollingerBandsData, str | None]] = {}
        self._stochastic_values: dict[str, tuple[StochasticData, str | None]] = {}
        self._atr_values: dict[str, tuple[ATRData, str | None]] = {}
        self._ma_values: dict[str, tuple[MAData, str | None]] = {}
        self._adx_cache: dict[tuple[str, str, int], ADXData] = {}
        self._obv_cache: dict[tuple[str, str, int], OBVData] = {}
        self._cci_cache: dict[tuple[str, str, int], CCIData] = {}
        self._ichimoku_cache: dict[tuple[str, str, int, int, int], IchimokuData] = {}
        self._adx_values: dict[str, tuple[ADXData, str | None]] = {}
        self._obv_values: dict[str, tuple[OBVData, str | None]] = {}
        self._cci_values: dict[str, tuple[CCIData, str | None]] = {}
        self._ichimoku_values: dict[str, tuple[IchimokuData, str | None]] = {}

    @property
    def chain(self) -> str:
        """Get the chain name."""
        return self._chain

    @property
    def wallet_address(self) -> str:
        """Get the wallet address."""
        return self._wallet_address

    @property
    def timestamp(self) -> datetime:
        """Get the snapshot timestamp."""
        return self._timestamp

    def price(self, token: str, quote: str = "USD") -> Decimal:
        """Get the price of a token.

        Args:
            token: Token symbol (e.g., "ETH", "WBTC")
            quote: Quote currency (default "USD")

        Returns:
            Token price in quote currency

        Raises:
            ValueError: If price cannot be determined
        """
        cache_key = f"{token}/{quote}"

        # Check pre-populated prices first
        if token in self._prices:
            return self._prices[token]

        # Check cache
        if cache_key in self._price_cache:
            return self._price_cache[cache_key].price

        # Use oracle if available
        if self._price_oracle:
            try:
                price_value = self._price_oracle(token, quote)
                self._price_cache[cache_key] = PriceData(price=price_value)
                return price_value
            except Exception as e:
                logger.warning(f"Price oracle failed for {cache_key}: {e}")

        raise ValueError(f"Cannot determine price for {token}/{quote}")

    def price_data(self, token: str, quote: str = "USD") -> PriceData:
        """Get full price data for a token.

        Args:
            token: Token symbol
            quote: Quote currency (default "USD")

        Returns:
            PriceData with current price and historical data
        """
        cache_key = f"{token}/{quote}"

        if cache_key in self._price_cache:
            return self._price_cache[cache_key]

        # Get basic price and create PriceData
        current_price = self.price(token, quote)
        return self._price_cache.get(cache_key, PriceData(price=current_price))

    def rsi(self, token: str, period: int = 14, timeframe: str = "4h") -> RSIData:
        """Get RSI (Relative Strength Index) for a token.

        Args:
            token: Token symbol
            period: RSI calculation period (default 14)
            timeframe: OHLCV candle timeframe (default "4h")

        Returns:
            RSIData with current RSI value and signal

        Raises:
            ValueError: If RSI cannot be calculated
        """
        cache_key = (token, timeframe, period)

        # Check pre-populated RSI first (validate period and timeframe match)
        if token in self._rsi_values:
            pre, stored_tf = self._rsi_values[token]
            if pre.period == period and (stored_tf is None or stored_tf == timeframe):
                return pre
            logger.debug(
                "Pre-populated RSI for %s (period=%d, tf=%s) doesn't match requested (period=%d, tf=%s), skipping",
                token,
                pre.period,
                stored_tf,
                period,
                timeframe,
            )

        # Check cache
        if cache_key in self._rsi_cache:
            return self._rsi_cache[cache_key]

        # Use provider if available
        if self._rsi_provider:
            try:
                rsi_data = self._rsi_provider(token, period, timeframe=timeframe)
                self._rsi_cache[cache_key] = rsi_data
                return rsi_data
            except TypeError:
                # Backward compat: older RSI providers only accept (token, period)
                rsi_data = self._rsi_provider(token, period)
                self._rsi_cache[cache_key] = rsi_data
                return rsi_data
            except Exception as e:
                logger.warning(f"RSI provider failed for {cache_key}: {e}")

        raise ValueError(f"Cannot calculate RSI for {token} with period {period}")

    def price_across_dexs(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
        dexs: list[str] | None = None,
    ) -> Any:
        """Get prices from multiple DEXs for comparison.

        Fetches quotes from all configured DEXs and returns a comparison
        of prices and execution details.

        Args:
            token_in: Input token symbol (e.g., "USDC", "WETH")
            token_out: Output token symbol (e.g., "WETH", "USDC")
            amount: Input amount (human-readable)
            dexs: DEXs to query (default: all available on chain)

        Returns:
            MultiDexPriceResult with quotes from each DEX

        Raises:
            NotImplementedError: If multi-DEX service is not configured
        """
        if self._multi_dex_service is None:
            raise NotImplementedError(
                "Multi-DEX price comparison is not available. "
                "The MultiDexService must be configured by the strategy runner."
            )
        import asyncio
        import concurrent.futures

        service = self._multi_dex_service

        async def _run() -> Any:
            return await service.get_prices_across_dexs(token_in, token_out, amount, dexs)

        # If there is already a running event loop (e.g., inside asyncio.run()),
        # run_until_complete() would crash. Use a thread pool to bridge safely.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, _run()).result()
        else:
            return asyncio.run(_run())

    def best_dex_price(
        self,
        token_in: str,
        token_out: str,
        amount: Decimal,
        dexs: list[str] | None = None,
    ) -> Any:
        """Get the best DEX for a trade.

        Compares prices from all configured DEXs and returns the one with
        the highest output amount (best execution).

        Args:
            token_in: Input token symbol (e.g., "USDC", "WETH")
            token_out: Output token symbol (e.g., "WETH", "USDC")
            amount: Input amount (human-readable)
            dexs: DEXs to compare (default: all available on chain)

        Returns:
            BestDexResult with the best DEX and quote

        Raises:
            NotImplementedError: If multi-DEX service is not configured
        """
        if self._multi_dex_service is None:
            raise NotImplementedError(
                "Multi-DEX price comparison is not available. "
                "The MultiDexService must be configured by the strategy runner."
            )
        import asyncio
        import concurrent.futures

        service = self._multi_dex_service

        async def _run() -> Any:
            return await service.get_best_dex_price(token_in, token_out, amount, dexs)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None and loop.is_running():
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                return pool.submit(asyncio.run, _run()).result()
        else:
            return asyncio.run(_run())

    def macd(
        self,
        token: str,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
        timeframe: str = "4h",
    ) -> MACDData:
        """Get MACD (Moving Average Convergence Divergence) for a token.

        Args:
            token: Token symbol
            fast_period: Fast EMA period (default 12)
            slow_period: Slow EMA period (default 26)
            signal_period: Signal EMA period (default 9)
            timeframe: OHLCV candle timeframe (default "4h")

        Returns:
            MACDData with MACD line, signal line, and histogram

        Raises:
            ValueError: If MACD data is not available

        Example:
            macd = market.macd("WETH")
            if macd.is_bullish_crossover:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        cache_key = (token, timeframe, fast_period, slow_period, signal_period)

        # Check pre-populated values first (validate params and timeframe)
        if token in self._macd_values:
            pre, stored_tf = self._macd_values[token]
            if (
                pre.fast_period == fast_period
                and pre.slow_period == slow_period
                and pre.signal_period == signal_period
                and (stored_tf is None or stored_tf == timeframe)
            ):
                return pre
            logger.debug(
                "Pre-populated MACD for %s (periods=(%d,%d,%d), tf=%s) doesn't match requested, skipping",
                token,
                pre.fast_period,
                pre.slow_period,
                pre.signal_period,
                stored_tf,
            )

        # Check cache
        if cache_key in self._macd_cache:
            return self._macd_cache[cache_key]

        # Use provider if available
        if self._indicator_provider and self._indicator_provider.macd:
            try:
                macd_data = self._indicator_provider.macd(
                    token,
                    fast_period,
                    slow_period,
                    signal_period,
                    timeframe=timeframe,
                )
                self._macd_cache[cache_key] = macd_data
                return macd_data
            except Exception as e:  # noqa: BLE001
                logger.warning(f"MACD provider failed for {cache_key}: {e}")

        raise ValueError(f"MACD data not available for {token}")

    def bollinger_bands(
        self, token: str, period: int = 20, std_dev: float = 2.0, timeframe: str = "4h"
    ) -> BollingerBandsData:
        """Get Bollinger Bands for a token.

        Args:
            token: Token symbol
            period: SMA period (default 20)
            std_dev: Standard deviation multiplier (default 2.0)
            timeframe: OHLCV candle timeframe (default "4h")

        Returns:
            BollingerBandsData with upper, middle, lower bands and position metrics

        Raises:
            ValueError: If Bollinger Bands data is not available

        Example:
            bb = market.bollinger_bands("WETH")
            if bb.is_oversold:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        cache_key = (token, timeframe, period, std_dev)

        # Check pre-populated values first (validate params and timeframe)
        if token in self._bollinger_values:
            pre, stored_tf = self._bollinger_values[token]
            if pre.period == period and pre.std_dev == std_dev and (stored_tf is None or stored_tf == timeframe):
                return pre
            logger.debug(
                "Pre-populated Bollinger for %s (period=%d, std_dev=%.1f, tf=%s) doesn't match requested, skipping",
                token,
                pre.period,
                pre.std_dev,
                stored_tf,
            )

        # Check cache
        if cache_key in self._bollinger_cache:
            return self._bollinger_cache[cache_key]

        # Use provider if available
        if self._indicator_provider and self._indicator_provider.bollinger:
            try:
                bb_data = self._indicator_provider.bollinger(
                    token,
                    period,
                    std_dev,
                    timeframe=timeframe,
                )
                self._bollinger_cache[cache_key] = bb_data
                return bb_data
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Bollinger provider failed for {cache_key}: {e}")

        raise ValueError(f"Bollinger Bands data not available for {token}")

    def stochastic(self, token: str, k_period: int = 14, d_period: int = 3, timeframe: str = "4h") -> StochasticData:
        """Get Stochastic Oscillator for a token.

        Args:
            token: Token symbol
            k_period: %K period (default 14)
            d_period: %D period (default 3)
            timeframe: OHLCV candle timeframe (default "4h")

        Returns:
            StochasticData with %K and %D values

        Raises:
            ValueError: If Stochastic data is not available

        Example:
            stoch = market.stochastic("WETH")
            if stoch.is_oversold and stoch.k_value > stoch.d_value:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        cache_key = (token, timeframe, k_period, d_period)

        # Check pre-populated values first (validate params and timeframe)
        if token in self._stochastic_values:
            pre, stored_tf = self._stochastic_values[token]
            if pre.k_period == k_period and pre.d_period == d_period and (stored_tf is None or stored_tf == timeframe):
                return pre
            logger.debug(
                "Pre-populated Stochastic for %s (periods=(%d,%d), tf=%s) doesn't match requested, skipping",
                token,
                pre.k_period,
                pre.d_period,
                stored_tf,
            )

        # Check cache
        if cache_key in self._stochastic_cache:
            return self._stochastic_cache[cache_key]

        # Use provider if available
        if self._indicator_provider and self._indicator_provider.stochastic:
            try:
                stoch_data = self._indicator_provider.stochastic(
                    token,
                    k_period,
                    d_period,
                    timeframe=timeframe,
                )
                self._stochastic_cache[cache_key] = stoch_data
                return stoch_data
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Stochastic provider failed for {cache_key}: {e}")

        raise ValueError(f"Stochastic data not available for {token}")

    def atr(self, token: str, period: int = 14, timeframe: str = "4h") -> ATRData:
        """Get ATR (Average True Range) for a token.

        Args:
            token: Token symbol
            period: ATR period (default 14)
            timeframe: OHLCV candle timeframe (default "4h")

        Returns:
            ATRData with ATR value and volatility assessment

        Raises:
            ValueError: If ATR data is not available

        Example:
            atr = market.atr("WETH")
            if atr.is_low_volatility:
                # Safe to trade
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        cache_key = (token, timeframe, period)

        # Check pre-populated values first (validate period and timeframe)
        if token in self._atr_values:
            pre, stored_tf = self._atr_values[token]
            if pre.period == period and (stored_tf is None or stored_tf == timeframe):
                return pre
            logger.debug(
                "Pre-populated ATR for %s (period=%d, tf=%s) doesn't match requested (period=%d, tf=%s), skipping",
                token,
                pre.period,
                stored_tf,
                period,
                timeframe,
            )

        # Check cache
        if cache_key in self._atr_cache:
            return self._atr_cache[cache_key]

        # Use provider if available
        if self._indicator_provider and self._indicator_provider.atr:
            try:
                atr_data = self._indicator_provider.atr(
                    token,
                    period,
                    timeframe=timeframe,
                )
                self._atr_cache[cache_key] = atr_data
                return atr_data
            except Exception as e:  # noqa: BLE001
                logger.warning(f"ATR provider failed for {cache_key}: {e}")

        raise ValueError(f"ATR data not available for {token}")

    def sma(self, token: str, period: int = 20, timeframe: str = "4h") -> MAData:
        """Get Simple Moving Average for a token.

        Args:
            token: Token symbol
            period: SMA period (default 20)
            timeframe: OHLCV candle timeframe (default "4h")

        Returns:
            MAData with SMA value

        Raises:
            ValueError: If SMA data is not available

        Example:
            sma = market.sma("WETH", period=50)
            if sma.is_price_above:
                print("Bullish - price above 50 SMA")
        """
        cache_key = (token, timeframe, "SMA", period)

        # Check pre-populated values first (validate params and timeframe)
        for ma_key in (f"{token}:SMA:{period}", token):
            if ma_key in self._ma_values:
                pre, stored_tf = self._ma_values[ma_key]
                if pre.ma_type == "SMA" and pre.period == period and (stored_tf is None or stored_tf == timeframe):
                    return pre

        # Check cache
        if cache_key in self._ma_cache:
            return self._ma_cache[cache_key]

        # Use provider if available
        if self._indicator_provider and self._indicator_provider.sma:
            try:
                sma_data = self._indicator_provider.sma(
                    token,
                    period,
                    timeframe=timeframe,
                )
                self._ma_cache[cache_key] = sma_data
                return sma_data
            except Exception as e:  # noqa: BLE001
                logger.warning(f"SMA provider failed for {cache_key}: {e}")

        raise ValueError(f"SMA data not available for {token} with period {period}")

    def ema(self, token: str, period: int = 12, timeframe: str = "4h") -> MAData:
        """Get Exponential Moving Average for a token.

        Args:
            token: Token symbol
            period: EMA period (default 12)
            timeframe: OHLCV candle timeframe (default "4h")

        Returns:
            MAData with EMA value

        Raises:
            ValueError: If EMA data is not available

        Example:
            ema_12 = market.ema("WETH", period=12)
            ema_26 = market.ema("WETH", period=26)
            if ema_12.value > ema_26.value:
                print("Golden cross - bullish")
        """
        cache_key = (token, timeframe, "EMA", period)

        # Check pre-populated values first (validate params and timeframe)
        str_cache_key = f"{token}:EMA:{period}"
        if str_cache_key in self._ma_values:
            pre, stored_tf = self._ma_values[str_cache_key]
            if pre.ma_type == "EMA" and pre.period == period and (stored_tf is None or stored_tf == timeframe):
                return pre

        # Check cache
        if cache_key in self._ma_cache:
            return self._ma_cache[cache_key]

        # Use provider if available
        if self._indicator_provider and self._indicator_provider.ema:
            try:
                ema_data = self._indicator_provider.ema(
                    token,
                    period,
                    timeframe=timeframe,
                )
                self._ma_cache[cache_key] = ema_data
                return ema_data
            except Exception as e:  # noqa: BLE001
                logger.warning(f"EMA provider failed for {cache_key}: {e}")

        raise ValueError(f"EMA data not available for {token} with period {period}")

    def adx(self, token: str, period: int = 14, timeframe: str = "4h") -> ADXData:
        """Get ADX (Average Directional Index) for a token.

        Args:
            token: Token symbol
            period: ADX period (default 14)
            timeframe: OHLCV candle timeframe (default "4h")

        Returns:
            ADXData with ADX, +DI, and -DI values

        Raises:
            ValueError: If ADX data is not available

        Example:
            adx = market.adx("WETH")
            if adx.is_uptrend:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        cache_key = (token, timeframe, period)

        if token in self._adx_values:
            pre, stored_tf = self._adx_values[token]
            if pre.period == period and (stored_tf is None or stored_tf == timeframe):
                return pre

        if cache_key in self._adx_cache:
            return self._adx_cache[cache_key]

        if self._indicator_provider and self._indicator_provider.adx:
            try:
                adx_data = self._indicator_provider.adx(
                    token,
                    period=period,
                    timeframe=timeframe,
                )
                self._adx_cache[cache_key] = adx_data
                return adx_data
            except Exception as e:  # noqa: BLE001
                logger.warning(f"ADX provider failed for {cache_key}: {e}")

        raise ValueError(f"ADX data not available for {token}")

    def obv(self, token: str, signal_period: int = 21, timeframe: str = "4h") -> OBVData:
        """Get OBV (On-Balance Volume) for a token.

        Args:
            token: Token symbol
            signal_period: OBV signal line period (default 21)
            timeframe: OHLCV candle timeframe (default "4h")

        Returns:
            OBVData with OBV and signal line values

        Raises:
            ValueError: If OBV data is not available

        Example:
            obv = market.obv("WETH")
            if obv.is_bullish:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        cache_key = (token, timeframe, signal_period)

        if token in self._obv_values:
            pre, stored_tf = self._obv_values[token]
            if pre.signal_period == signal_period and (stored_tf is None or stored_tf == timeframe):
                return pre

        if cache_key in self._obv_cache:
            return self._obv_cache[cache_key]

        if self._indicator_provider and self._indicator_provider.obv:
            try:
                obv_data = self._indicator_provider.obv(
                    token,
                    signal_period=signal_period,
                    timeframe=timeframe,
                )
                self._obv_cache[cache_key] = obv_data
                return obv_data
            except Exception as e:  # noqa: BLE001
                logger.warning(f"OBV provider failed for {cache_key}: {e}")

        raise ValueError(f"OBV data not available for {token}")

    def cci(self, token: str, period: int = 20, timeframe: str = "4h") -> CCIData:
        """Get CCI (Commodity Channel Index) for a token.

        Args:
            token: Token symbol
            period: CCI period (default 20)
            timeframe: OHLCV candle timeframe (default "4h")

        Returns:
            CCIData with CCI value and overbought/oversold status

        Raises:
            ValueError: If CCI data is not available

        Example:
            cci = market.cci("WETH")
            if cci.is_oversold:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        cache_key = (token, timeframe, period)

        if token in self._cci_values:
            pre, stored_tf = self._cci_values[token]
            if pre.period == period and (stored_tf is None or stored_tf == timeframe):
                return pre

        if cache_key in self._cci_cache:
            return self._cci_cache[cache_key]

        if self._indicator_provider and self._indicator_provider.cci:
            try:
                cci_data = self._indicator_provider.cci(
                    token,
                    period=period,
                    timeframe=timeframe,
                )
                self._cci_cache[cache_key] = cci_data
                return cci_data
            except Exception as e:  # noqa: BLE001
                logger.warning(f"CCI provider failed for {cache_key}: {e}")

        raise ValueError(f"CCI data not available for {token}")

    def ichimoku(
        self,
        token: str,
        tenkan_period: int = 9,
        kijun_period: int = 26,
        senkou_b_period: int = 52,
        timeframe: str = "4h",
    ) -> IchimokuData:
        """Get Ichimoku Cloud data for a token.

        Args:
            token: Token symbol
            tenkan_period: Conversion line period (default 9)
            kijun_period: Base line period (default 26)
            senkou_b_period: Leading span B period (default 52)
            timeframe: OHLCV candle timeframe (default "4h")

        Returns:
            IchimokuData with all Ichimoku components

        Raises:
            ValueError: If Ichimoku data is not available

        Example:
            ich = market.ichimoku("WETH")
            if ich.is_bullish_crossover and ich.is_above_cloud:
                return Intent.swap("USDC", "WETH", amount_usd=Decimal("100"))
        """
        cache_key = (token, timeframe, tenkan_period, kijun_period, senkou_b_period)

        if token in self._ichimoku_values:
            pre, stored_tf = self._ichimoku_values[token]
            if (
                pre.tenkan_period == tenkan_period
                and pre.kijun_period == kijun_period
                and pre.senkou_b_period == senkou_b_period
                and (stored_tf is None or stored_tf == timeframe)
            ):
                return pre

        if cache_key in self._ichimoku_cache:
            return self._ichimoku_cache[cache_key]

        if self._indicator_provider and self._indicator_provider.ichimoku:
            try:
                ichimoku_data = self._indicator_provider.ichimoku(
                    token,
                    tenkan_period=tenkan_period,
                    kijun_period=kijun_period,
                    senkou_b_period=senkou_b_period,
                    timeframe=timeframe,
                )
                self._ichimoku_cache[cache_key] = ichimoku_data
                return ichimoku_data
            except Exception as e:  # noqa: BLE001
                logger.warning(f"Ichimoku provider failed for {cache_key}: {e}")

        raise ValueError(f"Ichimoku data not available for {token}")

    def balance(self, token: str) -> TokenBalance:
        """Get wallet balance for a token.

        Args:
            token: Token symbol

        Returns:
            TokenBalance with current balance

        Raises:
            ValueError: If balance cannot be determined
        """
        # Check pre-populated balances first
        if token in self._balances:
            return self._balances[token]

        # Check cache
        if token in self._balance_cache:
            return self._balance_cache[token]

        # Use provider if available
        if self._balance_provider:
            try:
                balance_data = self._balance_provider(token)
                self._balance_cache[token] = balance_data
                return balance_data
            except Exception as e:
                logger.warning(f"Balance provider failed for {token}: {e}")

        raise ValueError(f"Cannot determine balance for {token}")

    def balance_usd(self, token: str) -> Decimal:
        """Get wallet balance in USD terms.

        Args:
            token: Token symbol

        Returns:
            Balance in USD
        """
        return self.balance(token).balance_usd

    def collateral_value_usd(self, token: str, amount: Decimal) -> Decimal:
        """Get the USD value of a given amount of collateral.

        Convenience helper for perp position sizing. Multiplies the given
        amount by the token's current price.

        Args:
            token: Token symbol (e.g., "WETH", "USDC", "WBTC")
            amount: Token amount in human-readable units (not wei)

        Returns:
            USD value as a Decimal
        """
        token_price = self.price(token)
        return amount * token_price

    def total_portfolio_usd(self) -> Decimal:
        """Calculate total portfolio value in USD across all known balances.

        Sums balance_usd for all tokens in pre-populated balances and
        cached balances (tokens queried via balance() in this snapshot).

        Returns:
            Total portfolio value in USD
        """
        total = Decimal("0")
        seen: set[str] = set()

        for token, balance in self._balances.items():
            total += balance.balance_usd
            seen.add(token)

        for token, balance in self._balance_cache.items():
            if token not in seen:
                total += balance.balance_usd

        return total

    def set_price(self, token: str, price_value: Decimal) -> None:
        """Pre-populate price for a token.

        Args:
            token: Token symbol
            price_value: Price value in USD
        """
        self._prices[token] = price_value

    def set_price_data(self, token: str, price_data: PriceData, quote: str = "USD") -> None:
        """Pre-populate enriched price data for a token (useful for testing).

        Unlike set_price() which only sets a scalar price, this sets the full
        PriceData object including change_24h_pct, high_24h, low_24h, etc.

        Args:
            token: Token symbol
            price_data: PriceData with price, change_24h_pct, etc.
            quote: Quote currency (default "USD")
        """
        cache_key = f"{token}/{quote}"
        self._price_cache[cache_key] = price_data

    def set_balance(self, token: str, balance_data: TokenBalance) -> None:
        """Pre-populate balance for a token.

        Args:
            token: Token symbol
            balance_data: Balance data
        """
        self._balances[token] = balance_data

    def set_rsi(self, token: str, rsi_data: RSIData, timeframe: str | None = None) -> None:
        """Pre-populate RSI for a token.

        Args:
            token: Token symbol
            rsi_data: RSI data
            timeframe: OHLCV timeframe this data was computed from (None matches any)
        """
        self._rsi_values[token] = (rsi_data, timeframe)

    def set_macd(self, token: str, macd_data: MACDData, timeframe: str | None = None) -> None:
        """Pre-populate MACD data for a token.

        Args:
            token: Token symbol
            macd_data: MACDData instance
            timeframe: OHLCV timeframe this data was computed from (None matches any)

        Example:
            market.set_macd("WETH", MACDData(
                macd_line=Decimal("0.5"),
                signal_line=Decimal("0.3"),
                histogram=Decimal("0.2"),
            ))
        """
        self._macd_values[token] = (macd_data, timeframe)

    def set_bollinger_bands(self, token: str, bb_data: BollingerBandsData, timeframe: str | None = None) -> None:
        """Pre-populate Bollinger Bands data for a token.

        Args:
            token: Token symbol
            bb_data: BollingerBandsData instance
            timeframe: OHLCV timeframe this data was computed from (None matches any)

        Example:
            market.set_bollinger_bands("WETH", BollingerBandsData(
                upper_band=Decimal("3100"),
                middle_band=Decimal("3000"),
                lower_band=Decimal("2900"),
                percent_b=Decimal("0.5"),
            ))
        """
        self._bollinger_values[token] = (bb_data, timeframe)

    def set_stochastic(self, token: str, stoch_data: StochasticData, timeframe: str | None = None) -> None:
        """Pre-populate Stochastic data for a token.

        Args:
            token: Token symbol
            stoch_data: StochasticData instance
            timeframe: OHLCV timeframe this data was computed from (None matches any)

        Example:
            market.set_stochastic("WETH", StochasticData(
                k_value=Decimal("25"),
                d_value=Decimal("30"),
            ))
        """
        self._stochastic_values[token] = (stoch_data, timeframe)

    def set_atr(self, token: str, atr_data: ATRData, timeframe: str | None = None) -> None:
        """Pre-populate ATR data for a token.

        Args:
            token: Token symbol
            atr_data: ATRData instance
            timeframe: OHLCV timeframe this data was computed from (None matches any)

        Example:
            market.set_atr("WETH", ATRData(
                value=Decimal("50"),
                value_percent=Decimal("2.5"),
            ))
        """
        self._atr_values[token] = (atr_data, timeframe)

    def set_ma(
        self, token: str, ma_data: MAData, ma_type: str = "SMA", period: int = 20, timeframe: str | None = None
    ) -> None:
        """Pre-populate Moving Average data for a token.

        Args:
            token: Token symbol
            ma_data: MAData instance
            ma_type: Type of MA ("SMA" or "EMA")
            period: MA period
            timeframe: OHLCV timeframe this data was computed from (None matches any)

        Example:
            market.set_ma("WETH", MAData(
                value=Decimal("3000"),
                ma_type="SMA",
                period=20,
                current_price=Decimal("3050"),
            ), ma_type="SMA", period=20)
        """
        cache_key = f"{token}:{ma_type}:{period}"
        entry = (ma_data, timeframe)
        self._ma_values[cache_key] = entry
        # Also store under simple token key for convenience
        self._ma_values[token] = entry

    def set_adx(self, token: str, adx_data: ADXData, timeframe: str | None = None) -> None:
        """Pre-populate ADX data for a token.

        Args:
            token: Token symbol
            adx_data: ADXData instance
            timeframe: Optional timeframe (None matches any query)

        Example:
            market.set_adx("WETH", ADXData(
                adx=Decimal("30"),
                plus_di=Decimal("25"),
                minus_di=Decimal("15"),
            ))
        """
        self._adx_values[token] = (adx_data, timeframe)

    def set_obv(self, token: str, obv_data: OBVData, timeframe: str | None = None) -> None:
        """Pre-populate OBV data for a token.

        Args:
            token: Token symbol
            obv_data: OBVData instance
            timeframe: Optional timeframe (None matches any query)

        Example:
            market.set_obv("WETH", OBVData(
                obv=Decimal("1000000"),
                signal_line=Decimal("950000"),
            ))
        """
        self._obv_values[token] = (obv_data, timeframe)

    def set_cci(self, token: str, cci_data: CCIData, timeframe: str | None = None) -> None:
        """Pre-populate CCI data for a token.

        Args:
            token: Token symbol
            cci_data: CCIData instance
            timeframe: Optional timeframe (None matches any query)

        Example:
            market.set_cci("WETH", CCIData(
                value=Decimal("-120"),
            ))
        """
        self._cci_values[token] = (cci_data, timeframe)

    def set_ichimoku(self, token: str, ichimoku_data: IchimokuData, timeframe: str | None = None) -> None:
        """Pre-populate Ichimoku data for a token.

        Args:
            token: Token symbol
            ichimoku_data: IchimokuData instance
            timeframe: Optional timeframe (None matches any query)

        Example:
            market.set_ichimoku("WETH", IchimokuData(
                tenkan_sen=Decimal("3050"),
                kijun_sen=Decimal("3000"),
                senkou_span_a=Decimal("3025"),
                senkou_span_b=Decimal("2950"),
                current_price=Decimal("3100"),
            ))
        """
        self._ichimoku_values[token] = (ichimoku_data, timeframe)

    @staticmethod
    def _lending_cache_key(protocol: str, token: str, side: str) -> str:
        """Normalize lending rate cache key to avoid case-sensitive misses."""
        return f"{protocol.strip().lower()}/{token.strip().upper()}/{side.strip().lower()}"

    def _run_async_bridged(self, coro: Any) -> Any:
        """Bridge an async coroutine to sync, handling running event loops."""
        import asyncio
        import concurrent.futures

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            future = executor.submit(asyncio.run, coro)
            try:
                return future.result(timeout=10)
            finally:
                executor.shutdown(wait=False, cancel_futures=True)
        else:
            return asyncio.run(coro)

    def lending_rate(
        self,
        protocol: str,
        token: str,
        side: str = "supply",
    ) -> Any:
        """Get the lending rate for a specific protocol and token.

        Fetches the current supply or borrow APY from the specified lending
        protocol. Rates are cached for efficiency (typically 12s = ~1 block).

        Args:
            protocol: Protocol identifier (aave_v3, morpho_blue, compound_v3)
            token: Token symbol (e.g., "USDC", "WETH")
            side: Rate side - "supply" or "borrow" (default "supply")

        Returns:
            LendingRate dataclass with apy_percent, apy_ray, utilization_percent, etc.

        Raises:
            ValueError: If no rate monitor is configured

        Example:
            rate = market.lending_rate("aave_v3", "USDC", "supply")
            print(f"Aave USDC Supply APY: {rate.apy_percent:.2f}%")
        """
        # Check pre-populated rates first
        cache_key = self._lending_cache_key(protocol, token, side)
        if cache_key in self._lending_rate_cache:
            return self._lending_rate_cache[cache_key]

        if self._rate_monitor is None:
            raise ValueError(
                "No rate monitor configured for MarketSnapshot. "
                "Pass rate_monitor= to MarketSnapshot() or use set_lending_rate() to pre-populate rates."
            )

        from almanak.framework.data.rates import RateSide

        try:
            rate_side = RateSide(side)
            result = self._run_async_bridged(self._rate_monitor.get_lending_rate(protocol, token, rate_side))
            self._lending_rate_cache[cache_key] = result
            return result
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Failed to get lending rate for {protocol}/{token}/{side}: {e}") from e

    def best_lending_rate(
        self,
        token: str,
        side: str = "supply",
        protocols: list[str] | None = None,
    ) -> Any:
        """Get the best lending rate for a token across protocols.

        For supply rates, returns highest APY. For borrow rates, returns lowest APY.

        Args:
            token: Token symbol (e.g., "USDC", "WETH")
            side: Rate side - "supply" or "borrow" (default "supply")
            protocols: Protocols to compare (default: all available on chain)

        Returns:
            BestRateResult with best_rate, all_rates, etc.

        Raises:
            ValueError: If no rate monitor is configured

        Example:
            result = market.best_lending_rate("USDC", "supply")
            if result.best_rate:
                print(f"Best: {result.best_rate.protocol} at {result.best_rate.apy_percent:.2f}%")
        """
        if self._rate_monitor is None:
            raise ValueError(
                "No rate monitor configured for MarketSnapshot. "
                "Pass rate_monitor= to MarketSnapshot() or use set_lending_rate() to pre-populate rates."
            )

        from almanak.framework.data.rates import RateSide

        try:
            rate_side = RateSide(side)
            result = self._run_async_bridged(self._rate_monitor.get_best_lending_rate(token, rate_side, protocols))
            return result
        except ValueError:
            raise
        except Exception as e:
            raise ValueError(f"Failed to get best lending rate for {token}/{side}: {e}") from e

    def set_lending_rate(self, protocol: str, token: str, side: str, rate: Any) -> None:
        """Pre-populate a lending rate for a protocol/token/side.

        Useful for backtesting and testing where you want to inject known rates
        without needing a live RateMonitor.

        Args:
            protocol: Protocol identifier (e.g., "aave_v3")
            token: Token symbol (e.g., "USDC")
            side: Rate side ("supply" or "borrow")
            rate: LendingRate dataclass instance

        Example:
            from almanak.framework.data.rates import LendingRate
            market.set_lending_rate("aave_v3", "USDC", "supply", LendingRate(
                protocol="aave_v3", token="USDC", side="supply",
                apy_ray=Decimal("0"), apy_percent=Decimal("4.25"),
            ))
        """
        cache_key = self._lending_cache_key(protocol, token, side)
        self._lending_rate_cache[cache_key] = rate

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

    def prediction_price(
        self,
        market_id: str,
        outcome: str,
    ) -> Decimal | None:
        """Get current price for a prediction market outcome.

        Convenience method that extracts the YES or NO price from a market.

        Args:
            market_id: Prediction market ID or URL slug
            outcome: "YES" or "NO"

        Returns:
            Current price as Decimal (0.01 to 0.99), or None if unavailable

        Example:
            yes_price = market.prediction_price("btc-100k", "YES")
            if yes_price is not None and yes_price < Decimal("0.3"):
                return BuyIntent(...)
        """
        if self._prediction_provider is None:
            return None

        try:
            return self._prediction_provider.get_price(market_id, outcome)
        except Exception:
            logger.debug(f"Failed to get prediction price for {market_id}/{outcome}")
            return None

    def get_price_oracle_dict(self) -> dict[str, Decimal]:
        """Get all prices as a dict suitable for IntentCompiler.

        Combines pre-populated prices and cached prices from oracle calls.
        This is used to pass real prices to the IntentCompiler for accurate
        slippage calculations.

        Returns:
            Dict mapping token symbols to USD prices
        """
        prices: dict[str, Decimal] = {}

        # Add pre-populated prices
        prices.update(self._prices)

        # Add cached prices from oracle calls (key format: "TOKEN/USD")
        for cache_key, price_data in self._price_cache.items():
            # Extract token symbol from cache key (e.g., "ETH/USD" -> "ETH")
            if "/" in cache_key:
                token = cache_key.split("/")[0]
                prices[token] = price_data.price

        return prices

    def to_dict(self) -> dict[str, Any]:
        """Convert snapshot to dictionary."""
        return {
            "chain": self._chain,
            "wallet_address": self._wallet_address,
            "timestamp": self._timestamp.isoformat(),
            "prices": {k: str(v) for k, v in self._prices.items()},
            "balances": {
                k: {
                    "symbol": v.symbol,
                    "balance": str(v.balance),
                    "balance_usd": str(v.balance_usd),
                }
                for k, v in self._balances.items()
            },
            "rsi_values": {
                k: {"value": str(data.value), "period": data.period, "timeframe": tf}
                for k, (data, tf) in self._rsi_values.items()
            },
        }


# =============================================================================
# Multi-Chain Market Snapshot Exceptions
# =============================================================================


class ChainNotConfiguredError(Exception):
    """Raised when accessing data for a chain not configured for the strategy.

    Attributes:
        chain: The chain that was requested
        configured_chains: List of chains that are configured
    """

    def __init__(self, chain: str, configured_chains: list[str]) -> None:
        self.chain = chain
        self.configured_chains = configured_chains
        super().__init__(f"Chain '{chain}' is not configured for this strategy. Configured chains: {configured_chains}")


# =============================================================================
# Multi-Chain Market Snapshot Types
# =============================================================================

# Type for chain-aware price oracle function
# (token, quote, chain) -> price
MultiChainPriceOracle = Callable[[str, str, str], Decimal]

# Type for chain-aware balance provider function
# (token, chain) -> TokenBalance
MultiChainBalanceProvider = Callable[[str, str], TokenBalance]


# =============================================================================
# Chain Health Status
# =============================================================================


class ChainHealthStatus(Enum):
    """Status of a chain's data health.

    Attributes:
        HEALTHY: Chain data is fresh and available
        DEGRADED: Chain data is stale but still usable (between threshold and 2x threshold)
        UNAVAILABLE: Chain data could not be fetched
        STALE: Chain data is too old to be trusted (beyond staleness threshold)
    """

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"
    STALE = "stale"


@dataclass
class ChainHealth:
    """Health status and staleness information for a single chain.

    This dataclass provides detailed information about the health of market data
    for a specific chain, including when data was last fetched, staleness metrics,
    and any error information.

    Attributes:
        chain: Chain name (e.g., "arbitrum", "optimism")
        status: Current health status of the chain
        last_updated: When the chain's data was last successfully fetched
        staleness_seconds: How old the data is in seconds (None if unavailable)
        stale_threshold_seconds: The threshold used to determine staleness
        error: Error message if data fetch failed
        is_stale: Whether the data is considered stale
        is_available: Whether the data is available for use

    Example:
        health = ChainHealth(
            chain="arbitrum",
            status=ChainHealthStatus.HEALTHY,
            last_updated=datetime.now(timezone.utc),
            staleness_seconds=5.2,
            stale_threshold_seconds=30.0,
        )

        if health.is_stale:
            logger.warning(f"Chain {health.chain} data is stale")
    """

    chain: str
    status: ChainHealthStatus
    last_updated: datetime | None = None
    staleness_seconds: float | None = None
    stale_threshold_seconds: float = 30.0
    error: str | None = None

    @property
    def is_stale(self) -> bool:
        """Check if the chain data is stale.

        Returns:
            True if staleness exceeds threshold or data is unavailable
        """
        if self.status == ChainHealthStatus.UNAVAILABLE:
            return True
        if self.status == ChainHealthStatus.STALE:
            return True
        if self.staleness_seconds is not None:
            return self.staleness_seconds > self.stale_threshold_seconds
        return False

    @property
    def is_available(self) -> bool:
        """Check if the chain data is available for use.

        Returns:
            True if data is healthy or degraded (but still usable)
        """
        return self.status in (ChainHealthStatus.HEALTHY, ChainHealthStatus.DEGRADED)

    @property
    def is_healthy(self) -> bool:
        """Check if the chain data is fully healthy.

        Returns:
            True if status is HEALTHY
        """
        return self.status == ChainHealthStatus.HEALTHY

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation.

        Returns:
            Dictionary with health information
        """
        return {
            "chain": self.chain,
            "status": self.status.value,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
            "staleness_seconds": self.staleness_seconds,
            "stale_threshold_seconds": self.stale_threshold_seconds,
            "error": self.error,
            "is_stale": self.is_stale,
            "is_available": self.is_available,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChainHealth":
        """Create ChainHealth from dictionary.

        Args:
            data: Dictionary with health information

        Returns:
            ChainHealth instance
        """
        last_updated = None
        if data.get("last_updated"):
            last_updated = datetime.fromisoformat(data["last_updated"])

        return cls(
            chain=data["chain"],
            status=ChainHealthStatus(data["status"]),
            last_updated=last_updated,
            staleness_seconds=data.get("staleness_seconds"),
            stale_threshold_seconds=data.get("stale_threshold_seconds", 30.0),
            error=data.get("error"),
        )


class StaleDataError(Exception):
    """Raised when market data is stale and fail_closed policy is active.

    This exception is raised during snapshot validation when one or more chains
    have stale or unavailable data and the data freshness policy is set to
    'fail_closed'.

    Attributes:
        stale_chains: List of chains with stale data
        chain_health: Dictionary mapping chain names to their health status
    """

    def __init__(
        self,
        stale_chains: list[str],
        chain_health: dict[str, "ChainHealth"],
    ) -> None:
        self.stale_chains = stale_chains
        self.chain_health = chain_health

        chain_details = []
        for chain in stale_chains:
            health = chain_health.get(chain)
            if health:
                if health.error:
                    chain_details.append(f"{chain}: {health.error}")
                elif health.staleness_seconds is not None:
                    chain_details.append(
                        f"{chain}: {health.staleness_seconds:.1f}s old (threshold: {health.stale_threshold_seconds}s)"
                    )
                else:
                    chain_details.append(f"{chain}: unavailable")
            else:
                chain_details.append(f"{chain}: unknown")

        super().__init__(f"Data is stale for chains: {', '.join(stale_chains)}. Details: {'; '.join(chain_details)}")


# Type alias for data freshness policy
DataFreshnessPolicy = Literal["fail_closed", "fail_open"]


# Type for Aave health factor provider function
# (chain) -> health_factor or None if no position
AaveHealthFactorProvider = Callable[[str], Decimal | None]

# Type for Aave available borrow provider function
# (token, chain) -> available_borrow_amount or None
AaveAvailableBorrowProvider = Callable[[str, str], Decimal | None]

# Type for GMX available liquidity provider function
# (market, chain) -> available_liquidity or None
GmxAvailableLiquidityProvider = Callable[[str, str], Decimal | None]

# Type for GMX funding rate provider function
# (market, chain) -> funding_rate or None
GmxFundingRateProvider = Callable[[str, str], Decimal | None]


# =============================================================================
# Multi-Chain Market Snapshot
# =============================================================================


class MultiChainMarketSnapshot:
    """Multi-chain market data access for cross-chain strategy decisions.

    MultiChainMarketSnapshot extends MarketSnapshot to provide data from multiple
    blockchain networks for cross-chain strategy decision-making. It supports:

    - Chain-specific prices via market.price('ETH', chain='arbitrum')
    - Chain-specific balances via market.balance('USDC', chain='optimism')
    - Cross-chain portfolio aggregation via market.total_portfolio_usd()
    - Parallel data fetching from all chains for performance

    The snapshot validates that all chain requests are for configured chains,
    raising ChainNotConfiguredError for invalid chain requests.

    Example:
        def decide(self, market: MultiChainMarketSnapshot) -> Optional[Intent]:
            # Get chain-specific prices
            arb_eth_price = market.price("ETH", chain="arbitrum")
            opt_eth_price = market.price("ETH", chain="optimism")

            # Get chain-specific balances
            usdc_on_optimism = market.balance("USDC", chain="optimism")

            # Get all configured chains
            for chain in market.chains:
                print(f"Balance on {chain}: {market.balance('ETH', chain=chain)}")

            # Aggregate portfolio value
            total_value = market.total_portfolio_usd()

            return Intent.hold()
    """

    # Default timeout for parallel fetching (2 seconds as per spec)
    DEFAULT_FETCH_TIMEOUT: float = 2.0

    def __init__(
        self,
        chains: list[str],
        wallet_address: str,
        price_oracle: MultiChainPriceOracle | None = None,
        balance_provider: MultiChainBalanceProvider | None = None,
        timestamp: datetime | None = None,
        fetch_timeout: float | None = None,
        # Protocol health metric providers
        aave_health_factor_provider: AaveHealthFactorProvider | None = None,
        aave_available_borrow_provider: AaveAvailableBorrowProvider | None = None,
        gmx_available_liquidity_provider: GmxAvailableLiquidityProvider | None = None,
        gmx_funding_rate_provider: GmxFundingRateProvider | None = None,
        # Data freshness settings
        data_freshness_policy: DataFreshnessPolicy = "fail_closed",
        stale_data_threshold_seconds: float = 30.0,
    ) -> None:
        """Initialize multi-chain market snapshot.

        Args:
            chains: List of configured chain names (e.g., ["arbitrum", "optimism", "base"])
            wallet_address: Wallet address for balance queries (same across EVM chains)
            price_oracle: Function to fetch prices (token, quote, chain) -> price
            balance_provider: Function to fetch balances (token, chain) -> TokenBalance
            timestamp: Snapshot timestamp (defaults to now)
            fetch_timeout: Timeout for parallel fetching in seconds (default 2.0)
            aave_health_factor_provider: Function to fetch Aave health factor (chain) -> factor
            aave_available_borrow_provider: Function to fetch Aave available borrow (token, chain) -> amount
            gmx_available_liquidity_provider: Function to fetch GMX liquidity (market, chain) -> amount
            gmx_funding_rate_provider: Function to fetch GMX funding rate (market, chain) -> rate
            data_freshness_policy: How to handle stale data - 'fail_closed' (default) errors on stale,
                'fail_open' excludes stale chains and continues
            stale_data_threshold_seconds: Data older than this is considered stale (default 30s)
        """
        if not chains:
            raise ValueError("At least one chain must be configured")

        self._chains = [c.lower() for c in chains]
        self._wallet_address = wallet_address
        self._price_oracle = price_oracle
        self._balance_provider = balance_provider
        self._timestamp = timestamp or datetime.now(UTC)
        self._fetch_timeout = fetch_timeout or self.DEFAULT_FETCH_TIMEOUT

        # Data freshness settings
        self._data_freshness_policy: DataFreshnessPolicy = data_freshness_policy
        self._stale_data_threshold_seconds = stale_data_threshold_seconds

        # Protocol health metric providers
        self._aave_health_factor_provider = aave_health_factor_provider
        self._aave_available_borrow_provider = aave_available_borrow_provider
        self._gmx_available_liquidity_provider = gmx_available_liquidity_provider
        self._gmx_funding_rate_provider = gmx_funding_rate_provider

        # Per-chain caches: {chain: {token: data}}
        self._price_cache: dict[str, dict[str, PriceData]] = {c: {} for c in self._chains}
        self._balance_cache: dict[str, dict[str, TokenBalance]] = {c: {} for c in self._chains}

        # Per-chain protocol metrics caches: {chain: data}
        self._aave_health_factor_cache: dict[str, Decimal | None] = {}
        self._aave_available_borrow_cache: dict[str, dict[str, Decimal | None]] = {c: {} for c in self._chains}
        self._gmx_available_liquidity_cache: dict[str, dict[str, Decimal | None]] = {c: {} for c in self._chains}
        self._gmx_funding_rate_cache: dict[str, dict[str, Decimal | None]] = {c: {} for c in self._chains}

        # Pre-populated data (can be set directly): {chain: {token: data}}
        self._prices: dict[str, dict[str, Decimal]] = {c: {} for c in self._chains}
        self._balances: dict[str, dict[str, TokenBalance]] = {c: {} for c in self._chains}

        # Chain health tracking: {chain: ChainHealth}
        self._chain_health: dict[str, ChainHealth] = {}
        self._chain_last_updated: dict[str, datetime] = {}

        # Initialize chain health for all chains as unknown
        for chain in self._chains:
            self._chain_health[chain] = ChainHealth(
                chain=chain,
                status=ChainHealthStatus.HEALTHY,  # Start healthy, update on fetch
                stale_threshold_seconds=stale_data_threshold_seconds,
            )

    @property
    def chains(self) -> list[str]:
        """Get the list of configured chains.

        Returns:
            List of chain names configured for this snapshot
        """
        return list(self._chains)

    @property
    def wallet_address(self) -> str:
        """Get the wallet address."""
        return self._wallet_address

    @property
    def timestamp(self) -> datetime:
        """Get the snapshot timestamp."""
        return self._timestamp

    @property
    def chain_health(self) -> dict[str, ChainHealth]:
        """Get health status for all configured chains.

        Returns:
            Dictionary mapping chain names to their ChainHealth status.
            Use this to check staleness and availability before making decisions.

        Example:
            health = market.chain_health
            for chain, status in health.items():
                if status.is_stale:
                    logger.warning(f"Chain {chain} has stale data")
        """
        return dict(self._chain_health)

    @property
    def data_freshness_policy(self) -> DataFreshnessPolicy:
        """Get the data freshness policy for this snapshot.

        Returns:
            'fail_closed' or 'fail_open'
        """
        return self._data_freshness_policy

    @property
    def stale_data_threshold_seconds(self) -> float:
        """Get the staleness threshold in seconds.

        Returns:
            Number of seconds after which data is considered stale
        """
        return self._stale_data_threshold_seconds

    @property
    def healthy_chains(self) -> list[str]:
        """Get list of chains with healthy (non-stale) data.

        This is useful when using fail_open policy to know which chains
        have usable data.

        Returns:
            List of chain names with healthy data
        """
        return [chain for chain, health in self._chain_health.items() if health.is_available]

    @property
    def stale_chains(self) -> list[str]:
        """Get list of chains with stale or unavailable data.

        Returns:
            List of chain names with stale data
        """
        return [chain for chain, health in self._chain_health.items() if health.is_stale]

    @property
    def all_chains_healthy(self) -> bool:
        """Check if all chains have healthy data.

        Returns:
            True if no chains have stale data
        """
        return len(self.stale_chains) == 0

    def _validate_chain(self, chain: str) -> str:
        """Validate that a chain is configured and return normalized name.

        Args:
            chain: Chain name to validate

        Returns:
            Normalized (lowercase) chain name

        Raises:
            ChainNotConfiguredError: If chain is not configured
        """
        chain_lower = chain.lower()
        if chain_lower not in self._chains:
            raise ChainNotConfiguredError(chain, self._chains)
        return chain_lower

    def _update_chain_health(
        self,
        chain: str,
        success: bool,
        error: str | None = None,
    ) -> None:
        """Update health status for a chain after a data fetch.

        This method should be called after fetching data from a chain to
        update the chain's health status and last updated timestamp.

        Args:
            chain: Chain name
            success: Whether the fetch was successful
            error: Error message if fetch failed
        """
        chain_lower = chain.lower()
        now = datetime.now(UTC)

        if success:
            self._chain_last_updated[chain_lower] = now
            staleness = 0.0

            # Determine status based on staleness
            status = ChainHealthStatus.HEALTHY

            self._chain_health[chain_lower] = ChainHealth(
                chain=chain_lower,
                status=status,
                last_updated=now,
                staleness_seconds=staleness,
                stale_threshold_seconds=self._stale_data_threshold_seconds,
                error=None,
            )
        else:
            # Fetch failed - mark as unavailable
            self._chain_health[chain_lower] = ChainHealth(
                chain=chain_lower,
                status=ChainHealthStatus.UNAVAILABLE,
                last_updated=self._chain_last_updated.get(chain_lower),
                staleness_seconds=None,
                stale_threshold_seconds=self._stale_data_threshold_seconds,
                error=error,
            )
            logger.warning(f"Chain {chain_lower} data fetch failed: {error}")

    def _recalculate_chain_staleness(self) -> None:
        """Recalculate staleness for all chains based on current time.

        This method updates the staleness_seconds and status for each chain
        based on when data was last successfully fetched.
        """
        now = datetime.now(UTC)

        for chain in self._chains:
            if chain in self._chain_last_updated:
                last_updated = self._chain_last_updated[chain]
                staleness = (now - last_updated).total_seconds()

                # Determine status based on staleness
                if staleness <= self._stale_data_threshold_seconds:
                    status = ChainHealthStatus.HEALTHY
                elif staleness <= self._stale_data_threshold_seconds * 2:
                    status = ChainHealthStatus.DEGRADED
                    logger.warning(
                        f"Chain {chain} data is degraded "
                        f"({staleness:.1f}s old, threshold: {self._stale_data_threshold_seconds}s)"
                    )
                else:
                    status = ChainHealthStatus.STALE
                    logger.warning(
                        f"Chain {chain} data is stale "
                        f"({staleness:.1f}s old, threshold: {self._stale_data_threshold_seconds}s)"
                    )

                self._chain_health[chain] = ChainHealth(
                    chain=chain,
                    status=status,
                    last_updated=last_updated,
                    staleness_seconds=staleness,
                    stale_threshold_seconds=self._stale_data_threshold_seconds,
                    error=self._chain_health[chain].error if chain in self._chain_health else None,
                )

    def validate_freshness(self) -> None:
        """Validate data freshness according to the configured policy.

        This method should be called before using the snapshot for decisions.
        It recalculates staleness and applies the freshness policy:

        - fail_closed: Raises StaleDataError if ANY chain has stale data
        - fail_open: Logs warnings for stale chains but doesn't raise

        Raises:
            StaleDataError: If data is stale and policy is 'fail_closed'

        Example:
            try:
                market.validate_freshness()
                # Safe to proceed with decision
                intent = strategy.decide(market)
            except StaleDataError as e:
                logger.error(f"Cannot proceed: {e}")
                # Handle stale data situation
        """
        # Update staleness calculations
        self._recalculate_chain_staleness()

        stale = self.stale_chains

        if not stale:
            logger.debug("All chain data is fresh")
            return

        if self._data_freshness_policy == "fail_closed":
            # Log which chains are stale before raising
            for chain in stale:
                health = self._chain_health.get(chain)
                if health:
                    logger.error(
                        f"Chain {chain} data is stale/unavailable - "
                        f"status: {health.status.value}, "
                        f"staleness: {health.staleness_seconds}s, "
                        f"error: {health.error}"
                    )
            raise StaleDataError(stale, self._chain_health)

        else:  # fail_open
            # Log warnings but don't raise
            for chain in stale:
                health = self._chain_health.get(chain)
                if health:
                    logger.warning(
                        f"Chain {chain} excluded due to stale/unavailable data - "
                        f"status: {health.status.value}, "
                        f"staleness: {health.staleness_seconds}s, "
                        f"error: {health.error}"
                    )
            logger.info(
                f"Proceeding with fail_open policy. Healthy chains: {self.healthy_chains}, Stale chains: {stale}"
            )

    def set_chain_health(self, chain: str, health: ChainHealth) -> None:
        """Manually set health status for a chain.

        This is useful for pre-populating health information when creating
        snapshots programmatically.

        Args:
            chain: Chain name
            health: ChainHealth instance to set

        Raises:
            ChainNotConfiguredError: If chain is not configured
        """
        chain_lower = self._validate_chain(chain)
        self._chain_health[chain_lower] = health
        if health.last_updated:
            self._chain_last_updated[chain_lower] = health.last_updated

    def price(self, token: str, chain: str, quote: str = "USD") -> Decimal:
        """Get the price of a token on a specific chain.

        Args:
            token: Token symbol (e.g., "ETH", "WBTC")
            chain: Chain name (e.g., "arbitrum", "optimism")
            quote: Quote currency (default "USD")

        Returns:
            Token price in quote currency

        Raises:
            ChainNotConfiguredError: If chain is not configured
            ValueError: If price cannot be determined
        """
        chain_lower = self._validate_chain(chain)
        cache_key = f"{token}/{quote}"

        # Check pre-populated prices first
        if token in self._prices.get(chain_lower, {}):
            return self._prices[chain_lower][token]

        # Check cache
        chain_cache = self._price_cache.get(chain_lower, {})
        if cache_key in chain_cache:
            return chain_cache[cache_key].price

        # Use oracle if available
        if self._price_oracle:
            try:
                price_value = self._price_oracle(token, quote, chain_lower)
                self._price_cache[chain_lower][cache_key] = PriceData(price=price_value)
                return price_value
            except Exception as e:
                logger.warning(f"Price oracle failed for {token}/{quote} on {chain_lower}: {e}")

        raise ValueError(f"Cannot determine price for {token}/{quote} on {chain}")

    def balance(self, token: str, chain: str) -> TokenBalance:
        """Get wallet balance for a token on a specific chain.

        Args:
            token: Token symbol
            chain: Chain name (e.g., "arbitrum", "optimism")

        Returns:
            TokenBalance with current balance on the specified chain

        Raises:
            ChainNotConfiguredError: If chain is not configured
            ValueError: If balance cannot be determined
        """
        chain_lower = self._validate_chain(chain)

        # Check pre-populated balances first
        if token in self._balances.get(chain_lower, {}):
            return self._balances[chain_lower][token]

        # Check cache
        chain_cache = self._balance_cache.get(chain_lower, {})
        if token in chain_cache:
            return chain_cache[token]

        # Use provider if available
        if self._balance_provider:
            try:
                balance_data = self._balance_provider(token, chain_lower)
                self._balance_cache[chain_lower][token] = balance_data
                return balance_data
            except Exception as e:
                logger.warning(f"Balance provider failed for {token} on {chain_lower}: {e}")

        raise ValueError(f"Cannot determine balance for {token} on {chain}")

    def balance_usd(self, token: str, chain: str) -> Decimal:
        """Get wallet balance in USD terms for a token on a specific chain.

        Args:
            token: Token symbol
            chain: Chain name

        Returns:
            Balance in USD on the specified chain
        """
        return self.balance(token, chain).balance_usd

    def price_difference(
        self,
        token: str,
        chain_a: str,
        chain_b: str,
        quote: str = "USD",
    ) -> Decimal | None:
        """Calculate price difference (spread) between two chains.

        This method is useful for cross-chain arbitrage detection by comparing
        the price of a token on two different chains.

        Args:
            token: Token symbol (e.g., "ETH", "WBTC")
            chain_a: First chain name (e.g., "arbitrum")
            chain_b: Second chain name (e.g., "optimism")
            quote: Quote currency (default "USD")

        Returns:
            Price spread as a decimal representing the percentage difference.
            Positive value means chain_a price is higher than chain_b.
            For example: 0.005 means chain_a is 0.5% higher than chain_b.
            Returns None if price is unavailable on either chain.

        Raises:
            ChainNotConfiguredError: If either chain is not configured

        Example:
            # Check for arbitrage opportunity
            spread = market.price_difference("ETH", chain_a="arbitrum", chain_b="optimism")
            if spread is not None and spread > Decimal("0.005"):  # 0.5% spread
                # Arbitrage: buy on optimism, sell on arbitrum
                pass
        """
        # Validate both chains are configured (raises ChainNotConfiguredError if not)
        self._validate_chain(chain_a)
        self._validate_chain(chain_b)

        try:
            price_a = self.price(token, chain_a, quote)
        except ValueError:
            logger.debug(f"Price unavailable for {token} on {chain_a}")
            return None

        try:
            price_b = self.price(token, chain_b, quote)
        except ValueError:
            logger.debug(f"Price unavailable for {token} on {chain_b}")
            return None

        # Avoid division by zero
        if price_b == Decimal("0"):
            logger.warning(f"Price for {token} on {chain_b} is zero, cannot calculate spread")
            return None

        # Calculate spread: (price_a - price_b) / price_b
        # Positive means chain_a is more expensive
        spread = (price_a - price_b) / price_b
        return spread

    # =========================================================================
    # Protocol Health Metrics - Aave
    # =========================================================================

    def aave_health_factor(self, chain: str) -> Decimal | None:
        """Get Aave health factor for the wallet on a specific chain.

        The health factor represents the safety of the user's position in Aave.
        - Health factor > 1: Position is safe
        - Health factor <= 1: Position can be liquidated

        Args:
            chain: Chain name (e.g., "arbitrum", "optimism")

        Returns:
            Aave health factor as Decimal, or None if:
            - No Aave position exists on this chain
            - Health factor cannot be determined
            - Provider returned None (graceful handling)

        Raises:
            ChainNotConfiguredError: If chain is not configured

        Example:
            health = market.aave_health_factor(chain='arbitrum')
            if health is not None and health < Decimal('1.5'):
                # Position is getting risky, consider repaying
                pass
        """
        chain_lower = self._validate_chain(chain)

        # Check cache
        if chain_lower in self._aave_health_factor_cache:
            return self._aave_health_factor_cache[chain_lower]

        # Use provider if available
        if self._aave_health_factor_provider:
            try:
                health_factor = self._aave_health_factor_provider(chain_lower)
                self._aave_health_factor_cache[chain_lower] = health_factor
                return health_factor
            except Exception as e:
                logger.debug(f"Aave health factor provider failed for {chain_lower}: {e}")
                # Cache None to avoid repeated failed calls
                self._aave_health_factor_cache[chain_lower] = None
                return None

        # No provider - return None (missing metrics return None, not error)
        return None

    def aave_available_borrow(self, token: str, chain: str) -> Decimal | None:
        """Get maximum available borrow amount for a token on Aave.

        This returns the maximum amount of the specified token that can be
        borrowed based on the user's collateral and current borrow utilization.

        Args:
            token: Token symbol to check borrow capacity for (e.g., "USDC", "ETH")
            chain: Chain name (e.g., "arbitrum", "optimism")

        Returns:
            Maximum borrowable amount as Decimal, or None if:
            - No Aave position exists on this chain
            - Token is not available for borrowing
            - Available borrow cannot be determined
            - Provider returned None (graceful handling)

        Raises:
            ChainNotConfiguredError: If chain is not configured

        Example:
            available = market.aave_available_borrow(token='USDC', chain='arbitrum')
            if available is not None and available > Decimal('1000'):
                # Can borrow more USDC
                return Intent.borrow(token='USDC', amount=Decimal('500'), chain='arbitrum')
        """
        chain_lower = self._validate_chain(chain)
        cache_key = token.upper()

        # Check cache
        chain_cache = self._aave_available_borrow_cache.get(chain_lower, {})
        if cache_key in chain_cache:
            return chain_cache[cache_key]

        # Use provider if available
        if self._aave_available_borrow_provider:
            try:
                available = self._aave_available_borrow_provider(token, chain_lower)
                self._aave_available_borrow_cache[chain_lower][cache_key] = available
                return available
            except Exception as e:
                logger.debug(f"Aave available borrow provider failed for {token} on {chain_lower}: {e}")
                # Cache None to avoid repeated failed calls
                self._aave_available_borrow_cache[chain_lower][cache_key] = None
                return None

        # No provider - return None (missing metrics return None, not error)
        return None

    # =========================================================================
    # Protocol Health Metrics - GMX
    # =========================================================================

    def gmx_available_liquidity(self, market: str, chain: str) -> Decimal | None:
        """Get available liquidity for a GMX market.

        This returns the available liquidity in the GMX market pool that can
        be used for opening new positions. Important for determining if a
        position size can be supported.

        Args:
            market: Market identifier (e.g., "ETH/USD", "BTC/USD")
            chain: Chain name (e.g., "arbitrum")

        Returns:
            Available liquidity in USD as Decimal, or None if:
            - Market does not exist on this chain
            - Liquidity cannot be determined
            - Provider returned None (graceful handling)

        Raises:
            ChainNotConfiguredError: If chain is not configured

        Example:
            liquidity = market.gmx_available_liquidity(market='ETH/USD', chain='arbitrum')
            if liquidity is not None and liquidity > Decimal('100000'):
                # Sufficient liquidity for position
                return Intent.perp_open(
                    market='ETH/USD',
                    direction='long',
                    size_usd=Decimal('10000'),
                    chain='arbitrum'
                )
        """
        chain_lower = self._validate_chain(chain)
        cache_key = market.upper()

        # Check cache
        chain_cache = self._gmx_available_liquidity_cache.get(chain_lower, {})
        if cache_key in chain_cache:
            return chain_cache[cache_key]

        # Use provider if available
        if self._gmx_available_liquidity_provider:
            try:
                liquidity = self._gmx_available_liquidity_provider(market, chain_lower)
                self._gmx_available_liquidity_cache[chain_lower][cache_key] = liquidity
                return liquidity
            except Exception as e:
                logger.debug(f"GMX available liquidity provider failed for {market} on {chain_lower}: {e}")
                # Cache None to avoid repeated failed calls
                self._gmx_available_liquidity_cache[chain_lower][cache_key] = None
                return None

        # No provider - return None (missing metrics return None, not error)
        return None

    def gmx_funding_rate(self, market: str, chain: str) -> Decimal | None:
        """Get current funding rate for a GMX market.

        The funding rate is the periodic payment between long and short
        positions. Positive rate means longs pay shorts, negative means
        shorts pay longs.

        Args:
            market: Market identifier (e.g., "ETH/USD", "BTC/USD")
            chain: Chain name (e.g., "arbitrum")

        Returns:
            Current funding rate as Decimal (per hour), or None if:
            - Market does not exist on this chain
            - Funding rate cannot be determined
            - Provider returned None (graceful handling)

            The rate is expressed as a decimal per hour.
            For example, 0.0001 means 0.01% per hour.

        Raises:
            ChainNotConfiguredError: If chain is not configured

        Example:
            rate = market.gmx_funding_rate(market='ETH/USD', chain='arbitrum')
            if rate is not None and rate > Decimal('0.0005'):
                # High positive funding - expensive to hold longs
                pass
        """
        chain_lower = self._validate_chain(chain)
        cache_key = market.upper()

        # Check cache
        chain_cache = self._gmx_funding_rate_cache.get(chain_lower, {})
        if cache_key in chain_cache:
            return chain_cache[cache_key]

        # Use provider if available
        if self._gmx_funding_rate_provider:
            try:
                rate = self._gmx_funding_rate_provider(market, chain_lower)
                self._gmx_funding_rate_cache[chain_lower][cache_key] = rate
                return rate
            except Exception as e:
                logger.debug(f"GMX funding rate provider failed for {market} on {chain_lower}: {e}")
                # Cache None to avoid repeated failed calls
                self._gmx_funding_rate_cache[chain_lower][cache_key] = None
                return None

        # No provider - return None (missing metrics return None, not error)
        return None

    # =========================================================================
    # Protocol Metrics - Parallel Fetching
    # =========================================================================

    def fetch_all_protocol_metrics_parallel(
        self,
        aave_chains: list[str] | None = None,
        aave_borrow_tokens: list[str] | None = None,
        gmx_markets: list[str] | None = None,
        gmx_chains: list[str] | None = None,
    ) -> dict[str, Any]:
        """Fetch protocol health metrics in parallel with other market data.

        This method fetches Aave and GMX metrics concurrently from all specified
        chains within the configured timeout (default 2 seconds).

        Args:
            aave_chains: Chains to fetch Aave health factors from (default: all chains)
            aave_borrow_tokens: Tokens to fetch Aave available borrow for (default: none)
            gmx_markets: GMX markets to fetch liquidity/funding for (default: none)
            gmx_chains: Chains to fetch GMX metrics from (default: all chains)

        Returns:
            Dictionary with fetched metrics:
            {
                'aave_health_factors': {chain: factor_or_none},
                'aave_available_borrow': {chain: {token: amount_or_none}},
                'gmx_available_liquidity': {chain: {market: amount_or_none}},
                'gmx_funding_rates': {chain: {market: rate_or_none}}
            }

        Example:
            # Fetch all Aave and GMX metrics in parallel
            metrics = market.fetch_all_protocol_metrics_parallel(
                aave_chains=['arbitrum', 'optimism'],
                aave_borrow_tokens=['USDC', 'ETH'],
                gmx_markets=['ETH/USD', 'BTC/USD'],
                gmx_chains=['arbitrum']
            )
        """
        # Default to all chains if not specified
        aave_chains_to_fetch = [c.lower() for c in (aave_chains or self._chains)]
        gmx_chains_to_fetch = [c.lower() for c in (gmx_chains or self._chains)]
        aave_tokens = aave_borrow_tokens or []
        gmx_mkts = gmx_markets or []

        results: dict[str, Any] = {
            "aave_health_factors": {},
            "aave_available_borrow": {c: {} for c in aave_chains_to_fetch},
            "gmx_available_liquidity": {c: {} for c in gmx_chains_to_fetch},
            "gmx_funding_rates": {c: {} for c in gmx_chains_to_fetch},
        }

        # Build task list
        tasks: list[tuple[str, str, str, str]] = []  # (type, chain, key, unused)

        # Aave health factor tasks
        if self._aave_health_factor_provider:
            for chain in aave_chains_to_fetch:
                if chain in self._chains:
                    tasks.append(("aave_health", chain, "", ""))

        # Aave available borrow tasks
        if self._aave_available_borrow_provider:
            for chain in aave_chains_to_fetch:
                if chain in self._chains:
                    for token in aave_tokens:
                        tasks.append(("aave_borrow", chain, token, ""))

        # GMX available liquidity tasks
        if self._gmx_available_liquidity_provider:
            for chain in gmx_chains_to_fetch:
                if chain in self._chains:
                    for market in gmx_mkts:
                        tasks.append(("gmx_liquidity", chain, market, ""))

        # GMX funding rate tasks
        if self._gmx_funding_rate_provider:
            for chain in gmx_chains_to_fetch:
                if chain in self._chains:
                    for market in gmx_mkts:
                        tasks.append(("gmx_funding", chain, market, ""))

        if not tasks:
            logger.debug("No protocol metrics tasks to fetch (no providers or empty task list)")
            return results

        def fetch_metric(task_type: str, chain: str, key: str) -> tuple[str, str, str, Decimal | None]:
            """Fetch a single metric and return (type, chain, key, value)."""
            try:
                if task_type == "aave_health":
                    value = self._aave_health_factor_provider(chain)  # type: ignore
                    return (task_type, chain, "", value)
                elif task_type == "aave_borrow":
                    value = self._aave_available_borrow_provider(key, chain)  # type: ignore
                    return (task_type, chain, key, value)
                elif task_type == "gmx_liquidity":
                    value = self._gmx_available_liquidity_provider(key, chain)  # type: ignore
                    return (task_type, chain, key, value)
                elif task_type == "gmx_funding":
                    value = self._gmx_funding_rate_provider(key, chain)  # type: ignore
                    return (task_type, chain, key, value)
                else:
                    return (task_type, chain, key, None)
            except Exception as e:
                logger.debug(f"Failed to fetch {task_type} for {key or 'health'} on {chain}: {e}")
                return (task_type, chain, key, None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            futures = {executor.submit(fetch_metric, t[0], t[1], t[2]): t for t in tasks}

            # Wait for completion with timeout
            done, not_done = concurrent.futures.wait(
                futures,
                timeout=self._fetch_timeout,
                return_when=concurrent.futures.ALL_COMPLETED,
            )

            # Process completed futures
            for future in done:
                try:
                    task_type, chain, key, value = future.result()
                    if task_type == "aave_health":
                        results["aave_health_factors"][chain] = value
                        self._aave_health_factor_cache[chain] = value
                    elif task_type == "aave_borrow":
                        results["aave_available_borrow"][chain][key] = value
                        self._aave_available_borrow_cache[chain][key.upper()] = value
                    elif task_type == "gmx_liquidity":
                        results["gmx_available_liquidity"][chain][key] = value
                        self._gmx_available_liquidity_cache[chain][key.upper()] = value
                    elif task_type == "gmx_funding":
                        results["gmx_funding_rates"][chain][key] = value
                        self._gmx_funding_rate_cache[chain][key.upper()] = value
                except Exception as e:
                    logger.debug(f"Future result error: {e}")

            # Log if any timed out
            if not_done:
                logger.warning(
                    f"Protocol metrics fetch timed out after {self._fetch_timeout}s, "
                    f"{len(not_done)} requests incomplete"
                )

        return results

    def total_portfolio_usd(self) -> Decimal:
        """Calculate total portfolio value in USD across all chains.

        Aggregates all known balances across all configured chains.

        Returns:
            Total portfolio value in USD
        """
        total = Decimal("0")

        # Sum pre-populated balances
        for _chain, balances in self._balances.items():
            for _token, balance in balances.items():
                total += balance.balance_usd

        # Sum cached balances (only those not already in pre-populated)
        for chain, cache in self._balance_cache.items():
            for token, balance in cache.items():
                # Skip if already counted in pre-populated
                if token not in self._balances.get(chain, {}):
                    total += balance.balance_usd

        return total

    def set_price(self, token: str, chain: str, price_value: Decimal) -> None:
        """Pre-populate price for a token on a specific chain.

        Args:
            token: Token symbol
            chain: Chain name
            price_value: Price value in USD

        Raises:
            ChainNotConfiguredError: If chain is not configured
        """
        chain_lower = self._validate_chain(chain)
        self._prices[chain_lower][token] = price_value

    def set_price_data(self, token: str, chain: str, price_data: "PriceData", quote: str = "USD") -> None:
        """Pre-populate enriched price data for a token on a specific chain.

        Unlike set_price() which only sets a scalar price, this sets the full
        PriceData object including change_24h_pct, high_24h, low_24h, etc.

        Args:
            token: Token symbol
            chain: Chain name
            price_data: PriceData with price, change_24h_pct, etc.
            quote: Quote currency (default "USD")

        Raises:
            ChainNotConfiguredError: If chain is not configured
        """
        chain_lower = self._validate_chain(chain)
        cache_key = f"{token}/{quote}"
        self._price_cache[chain_lower][cache_key] = price_data

    def set_balance(self, token: str, chain: str, balance_data: TokenBalance) -> None:
        """Pre-populate balance for a token on a specific chain.

        Args:
            token: Token symbol
            chain: Chain name
            balance_data: Balance data

        Raises:
            ChainNotConfiguredError: If chain is not configured
        """
        chain_lower = self._validate_chain(chain)
        self._balances[chain_lower][token] = balance_data

    def fetch_all_prices_parallel(
        self,
        tokens: list[str],
        quote: str = "USD",
    ) -> dict[str, dict[str, Decimal]]:
        """Fetch prices for multiple tokens across all chains in parallel.

        Uses concurrent execution to fetch prices from all chains within
        the configured timeout (default 2 seconds).

        Args:
            tokens: List of token symbols to fetch prices for
            quote: Quote currency (default "USD")

        Returns:
            Nested dict: {chain: {token: price}}
            Missing prices are omitted from the result.
        """
        if not self._price_oracle:
            logger.warning("No price oracle configured, cannot fetch prices")
            return {}

        results: dict[str, dict[str, Decimal]] = {c: {} for c in self._chains}

        def fetch_price(chain: str, token: str) -> tuple[str, str, Decimal | None]:
            """Fetch a single price and return (chain, token, price)."""
            try:
                price = self._price_oracle(token, quote, chain)  # type: ignore
                return (chain, token, price)
            except Exception as e:
                logger.debug(f"Failed to fetch {token}/{quote} on {chain}: {e}")
                return (chain, token, None)

        # Create all fetch tasks
        tasks = [(chain, token) for chain in self._chains for token in tokens]

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            futures = {executor.submit(fetch_price, chain, token): (chain, token) for chain, token in tasks}

            # Wait for completion with timeout
            done, not_done = concurrent.futures.wait(
                futures,
                timeout=self._fetch_timeout,
                return_when=concurrent.futures.ALL_COMPLETED,
            )

            # Process completed futures
            for future in done:
                try:
                    chain, token, price = future.result()
                    if price is not None:
                        results[chain][token] = price
                        # Update cache
                        cache_key = f"{token}/{quote}"
                        self._price_cache[chain][cache_key] = PriceData(price=price)
                except Exception as e:
                    logger.debug(f"Future result error: {e}")

            # Log if any timed out
            if not_done:
                logger.warning(
                    f"Price fetch timed out after {self._fetch_timeout}s, {len(not_done)} requests incomplete"
                )

        return results

    def fetch_all_balances_parallel(
        self,
        tokens: list[str],
    ) -> dict[str, dict[str, TokenBalance]]:
        """Fetch balances for multiple tokens across all chains in parallel.

        Uses concurrent execution to fetch balances from all chains within
        the configured timeout (default 2 seconds).

        Args:
            tokens: List of token symbols to fetch balances for

        Returns:
            Nested dict: {chain: {token: TokenBalance}}
            Missing balances are omitted from the result.
        """
        if not self._balance_provider:
            logger.warning("No balance provider configured, cannot fetch balances")
            return {}

        results: dict[str, dict[str, TokenBalance]] = {c: {} for c in self._chains}

        def fetch_balance(chain: str, token: str) -> tuple[str, str, TokenBalance | None]:
            """Fetch a single balance and return (chain, token, balance)."""
            try:
                balance = self._balance_provider(token, chain)  # type: ignore
                return (chain, token, balance)
            except Exception as e:
                logger.debug(f"Failed to fetch {token} balance on {chain}: {e}")
                return (chain, token, None)

        # Create all fetch tasks
        tasks = [(chain, token) for chain in self._chains for token in tokens]

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            futures = {executor.submit(fetch_balance, chain, token): (chain, token) for chain, token in tasks}

            # Wait for completion with timeout
            done, not_done = concurrent.futures.wait(
                futures,
                timeout=self._fetch_timeout,
                return_when=concurrent.futures.ALL_COMPLETED,
            )

            # Process completed futures
            for future in done:
                try:
                    chain, token, balance = future.result()
                    if balance is not None:
                        results[chain][token] = balance
                        # Update cache
                        self._balance_cache[chain][token] = balance
                except Exception as e:
                    logger.debug(f"Future result error: {e}")

            # Log if any timed out
            if not_done:
                logger.warning(
                    f"Balance fetch timed out after {self._fetch_timeout}s, {len(not_done)} requests incomplete"
                )

        return results

    def to_dict(self) -> dict[str, Any]:
        """Convert snapshot to dictionary."""
        return {
            "chains": self._chains,
            "wallet_address": self._wallet_address,
            "timestamp": self._timestamp.isoformat(),
            "data_freshness_policy": self._data_freshness_policy,
            "stale_data_threshold_seconds": self._stale_data_threshold_seconds,
            "chain_health": {chain: health.to_dict() for chain, health in self._chain_health.items()},
            "prices": {chain: {k: str(v) for k, v in prices.items()} for chain, prices in self._prices.items()},
            "balances": {
                chain: {
                    token: {
                        "symbol": balance.symbol,
                        "balance": str(balance.balance),
                        "balance_usd": str(balance.balance_usd),
                    }
                    for token, balance in balances.items()
                }
                for chain, balances in self._balances.items()
            },
        }


# =============================================================================
# Strategy Decorator
# =============================================================================


@dataclass
class StrategyMetadata:
    """Metadata for a strategy.

    Attributes:
        name: Strategy name (e.g., "simple_dca")
        description: Human-readable description
        version: Strategy version (e.g., "1.0.0")
        author: Author name or organization
        tags: List of tags for categorization
        supported_chains: List of supported chains
        supported_protocols: List of supported protocols
        intent_types: List of intent types this strategy may use
        default_chain: Default chain for single-chain execution (falls back to supported_chains[0])
    """

    name: str
    description: str = ""
    version: str = "1.0.0"
    author: str = ""
    tags: list[str] = field(default_factory=list)
    supported_chains: list[str] = field(default_factory=list)
    supported_protocols: list[str] = field(default_factory=list)
    intent_types: list[str] = field(default_factory=list)
    default_chain: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "description": self.description,
            "version": self.version,
            "author": self.author,
            "tags": self.tags,
            "supported_chains": self.supported_chains,
            "supported_protocols": self.supported_protocols,
            "intent_types": self.intent_types,
            "default_chain": self.default_chain,
        }


# Type variable for strategy class
StrategyClassT = TypeVar("StrategyClassT", bound=type)


def almanak_strategy(
    name: str,
    description: str = "",
    version: str = "1.0.0",
    author: str = "",
    tags: list[str] | None = None,
    supported_chains: list[str] | None = None,
    supported_protocols: list[str] | None = None,
    intent_types: list[str] | None = None,
    default_chain: str = "",
) -> Callable[[StrategyClassT], StrategyClassT]:
    """Decorator to add metadata to an IntentStrategy class.

    This decorator attaches metadata to a strategy class, making it
    discoverable and self-documenting. It also registers the strategy
    in STRATEGY_REGISTRY for lookup by name.

    Args:
        name: Strategy name
        description: Human-readable description
        version: Strategy version
        author: Author name
        tags: Categorization tags
        supported_chains: List of supported chains
        supported_protocols: List of supported protocols
        intent_types: List of intent types used
        default_chain: Default chain for single-chain execution (falls back to supported_chains[0])

    Returns:
        Decorated class with STRATEGY_METADATA attribute

    Example:
        @almanak_strategy(
            name="mean_reversion_simple",
            description="RSI-based mean reversion strategy",
            version="1.0.0",
            author="Almanak",
            tags=["trading", "rsi", "mean-reversion"],
            supported_chains=["arbitrum", "ethereum"],
            intent_types=["SWAP"],
            default_chain="arbitrum",
        )
        class MeanReversionStrategy(IntentStrategy):
            pass
    """
    # Import here to avoid circular import
    from . import STRATEGY_REGISTRY

    def decorator(cls: StrategyClassT) -> StrategyClassT:
        resolved_supported_chains = supported_chains or []
        resolved_default_chain = default_chain or (resolved_supported_chains[0] if resolved_supported_chains else "")
        if default_chain and resolved_supported_chains and default_chain not in resolved_supported_chains:
            raise ValueError(
                f"default_chain '{default_chain}' must be one of supported_chains: {resolved_supported_chains}"
            )
        metadata = StrategyMetadata(
            name=name,
            description=description,
            version=version,
            author=author,
            tags=tags or [],
            supported_chains=resolved_supported_chains,
            supported_protocols=supported_protocols or [],
            intent_types=intent_types or [],
            default_chain=resolved_default_chain,
        )

        # Attach metadata to class
        cls.STRATEGY_METADATA = metadata  # type: ignore[attr-defined]
        cls.STRATEGY_NAME = name  # type: ignore[attr-defined]

        # Register in the global registry
        if name not in STRATEGY_REGISTRY:
            STRATEGY_REGISTRY[name] = cls
            logger.info(f"Registered strategy: {name} v{version}")
        else:
            logger.debug(f"Strategy {name} already registered, skipping")

        return cls

    return decorator


# =============================================================================
# Execution Result
# =============================================================================


@dataclass
class ExecutionResult:
    """Result of strategy execution.

    Attributes:
        intent: The intent that was executed (or None if HOLD)
        action_bundle: The compiled action bundle (or None)
        state_machine_result: Final state machine step result
        success: Whether execution was successful
        error: Error message if failed
        execution_time_ms: Time taken for execution in milliseconds
    """

    intent: AnyIntent | None
    action_bundle: ActionBundle | None = None
    state_machine_result: StepResult | None = None
    success: bool = False
    error: str | None = None
    execution_time_ms: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "intent_type": self.intent.intent_type.value if self.intent else None,
            "intent_id": self.intent.intent_id if self.intent else None,
            "action_bundle": self.action_bundle.to_dict() if self.action_bundle else None,
            "success": self.success,
            "error": self.error,
            "execution_time_ms": self.execution_time_ms,
        }


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
        """
        super().__init__(config, risk_guard_config, notification_callback)

        self._chain = chain
        self._wallet_address = wallet_address
        self._rpc_url = rpc_url

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
        self._multi_dex_service: Any | None = None
        self._rate_monitor: Any | None = None

        # Multi-chain providers (set by set_multi_chain_providers)
        self._multi_chain_price_oracle: MultiChainPriceOracle | None = None
        self._multi_chain_balance_provider: MultiChainBalanceProvider | None = None
        self._aave_health_factor_provider: AaveHealthFactorProvider | None = None

        # Current execution state
        self._current_intent: AnyIntent | None = None
        self._current_state_machine: IntentStateMachine | None = None

        # State persistence (set by runner via set_state_manager)
        self._state_manager: Any | None = None
        self._strategy_id: str = ""
        self._state_version: int = 0
        self._pending_save: Any | None = None

        logger.info(f"Initialized IntentStrategy on {chain} with wallet {wallet_address[:10]}...")

    @property
    def chain(self) -> str:
        """Get the chain name."""
        return self._chain

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
    # State Persistence
    # =========================================================================

    def set_state_manager(self, state_manager: Any, strategy_id: str) -> None:
        """Set the state manager for persistence.

        Called by the runner to inject the state manager.

        Args:
            state_manager: StateManager instance
            strategy_id: Unique ID for this strategy instance
        """
        self._state_manager = state_manager
        self._strategy_id = strategy_id

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

    def save_state(self) -> None:
        """Save current strategy state to persistence.

        Called by runner after each iteration.
        """
        if not self._state_manager or not self._strategy_id:
            return

        state = self.get_persistent_state()
        if not state:
            return

        try:
            from ..state.state_manager import StateData

            # Create StateData object with the strategy state
            # Try to get existing version for proper CAS updates
            version = getattr(self, "_state_version", 0) + 1

            state_data = StateData(
                strategy_id=self._strategy_id,
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

            logger.debug(f"Saved state for {self._strategy_id}: {list(state.keys())}")
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

    def load_state(self) -> bool:
        """Load strategy state from persistence.

        Called by runner on startup.

        Returns:
            True if state was found and loaded, False otherwise
        """
        if not self._state_manager or not self._strategy_id:
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
                state_data = asyncio.run(self._state_manager.load_state(self._strategy_id))

            if state_data and state_data.state:
                self.load_persistent_state(state_data.state)
                # Store version for CAS updates
                self._state_version = state_data.version
                logger.info(f"Loaded state for {self._strategy_id}: {list(state_data.state.keys())}")
                return True
            return False
        except Exception as e:
            # StateNotFoundError is expected for fresh starts
            if "not found" in str(e).lower():
                logger.debug(f"No existing state for {self._strategy_id}")
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
        """Check if this strategy is multi-chain.

        Returns:
            True if SUPPORTED_CHAINS has multiple chains
        """
        supported_chains = getattr(self.__class__, "SUPPORTED_CHAINS", None)
        if supported_chains and isinstance(supported_chains, list | tuple):
            return len(supported_chains) > 1
        return False

    def get_supported_chains(self) -> list[str]:
        """Get the chains supported by this strategy.

        Returns:
            List of supported chain names
        """
        return list(getattr(self.__class__, "SUPPORTED_CHAINS", [self._chain]))

    def create_market_snapshot(self) -> MarketSnapshot:
        """Create a market snapshot for the current iteration.

        Automatically creates MultiChainMarketSnapshot for multi-chain strategies
        if multi-chain providers have been set. Otherwise returns single-chain
        MarketSnapshot.

        Override this method to customize how market data is populated.

        Returns:
            MarketSnapshot (or MultiChainMarketSnapshot for multi-chain strategies)
        """
        # Check if this is a multi-chain strategy
        if self.is_multi_chain():
            chains = self.get_supported_chains()
            logger.debug(f"Creating MultiChainMarketSnapshot for chains: {chains}")
            return MultiChainMarketSnapshot(  # type: ignore[return-value]
                chains=chains,
                wallet_address=self._wallet_address,
                price_oracle=self._multi_chain_price_oracle,
                balance_provider=self._multi_chain_balance_provider,
                aave_health_factor_provider=self._aave_health_factor_provider,
            )

        # Single-chain snapshot
        return MarketSnapshot(
            chain=self._chain,
            wallet_address=self._wallet_address,
            price_oracle=self._price_oracle,
            rsi_provider=self._rsi_provider,
            balance_provider=self._balance_provider,
            wallet_activity_provider=self._wallet_activity_provider,
            prediction_provider=self._prediction_provider,
            indicator_provider=self._indicator_provider,
            multi_dex_service=self._multi_dex_service,
            rate_monitor=self._rate_monitor,
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
                    strategy_id=self.strategy_id,
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
                    strategy_id=self._strategy_id or self.STRATEGY_NAME,
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
                            )
                        )
                except Exception as e:  # noqa: BLE001  # Intentional graceful degradation
                    logger.debug(f"Could not get balance/price for {token}: {e}")
                    continue

            return PortfolioSnapshot(
                timestamp=datetime.now(UTC),
                strategy_id=self._strategy_id or self.STRATEGY_NAME,
                total_value_usd=position_value + wallet_value,
                available_cash_usd=wallet_value,
                value_confidence=ValueConfidence.ESTIMATED if positions_unavailable else ValueConfidence.HIGH,
                positions=positions,
                wallet_balances=wallet_balances,
                chain=self._chain,
            )

        except Exception as e:  # noqa: BLE001  # Intentional graceful degradation
            # Graceful degradation - return unavailable instead of $0
            logger.warning(f"Failed to compute portfolio snapshot: {e}")
            return PortfolioSnapshot(
                timestamp=datetime.now(UTC),
                strategy_id=self._strategy_id or self.STRATEGY_NAME,
                total_value_usd=Decimal("0"),
                available_cash_usd=Decimal("0"),
                value_confidence=ValueConfidence.UNAVAILABLE,
                error=str(e),
                chain=self._chain,
            )

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
        (e.g., pool="WETH/USDC/500").

        Returns:
            Deduplicated list of token symbols, or empty list if none found.
        """
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
        # like "WETH/USDC/500" or "WETH/USDC"
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
                # Parse pool-style values: "WETH/USDC/500" -> ["WETH", "USDC"]
                parts = value.split("/")
                for part in parts:
                    # Skip numeric parts (fee tiers like "500", "3000")
                    if part.isdigit():
                        continue
                    symbol = part.strip()
                    if symbol and symbol not in seen:
                        seen.add(symbol)
                        tokens.append(symbol)
            elif key in _TOKEN_FIELDS:
                symbol = value.strip()
                if symbol and symbol not in seen:
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
        class, or return TeardownPositionSummary.empty(self.strategy_id).

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
                    strategy_id=self.STRATEGY_NAME,
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

        Returns:
            TeardownRequest if one exists and is active, None otherwise
        """
        try:
            from almanak.framework.teardown import get_teardown_state_manager

            manager = get_teardown_state_manager()
            strategy_id = self._strategy_id or self.STRATEGY_NAME

            request = manager.get_active_request(strategy_id)
            if request:
                logger.info(
                    f"Found active teardown request for {strategy_id}: "
                    f"mode={request.mode.value}, status={request.status.value}"
                )
            return request

        except Exception as e:
            logger.warning(f"Error checking teardown request: {e}")
            return None

    def acknowledge_teardown_request(self) -> bool:
        """Acknowledge a pending teardown request.

        Called when the strategy picks up a teardown request
        and starts processing it.

        Returns:
            True if request was acknowledged, False otherwise
        """
        try:
            from almanak.framework.teardown import get_teardown_state_manager

            manager = get_teardown_state_manager()
            strategy_id = self._strategy_id or self.STRATEGY_NAME

            request = manager.acknowledge_request(strategy_id)
            return request is not None

        except Exception as e:
            logger.warning(f"Error acknowledging teardown request: {e}")
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
    "StrategyMetadata",
]
