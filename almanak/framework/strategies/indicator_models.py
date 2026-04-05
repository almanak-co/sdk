"""Technical indicator data models for MarketSnapshot.

This module contains all indicator dataclasses (RSI, MACD, Bollinger Bands, etc.)
and the IndicatorProvider class used by MarketSnapshot to provide technical analysis
data to strategies.

These were extracted from intent_strategy.py for maintainability. All symbols
remain importable from almanak.framework.strategies.intent_strategy.
"""

from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal


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


__all__ = [
    "RSIData",
    "MACDData",
    "BollingerBandsData",
    "StochasticData",
    "ATRData",
    "MAData",
    "ADXData",
    "OBVData",
    "CCIData",
    "IchimokuData",
    "IndicatorProvider",
]
