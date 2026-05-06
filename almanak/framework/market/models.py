"""Typed return models for VIB-4062 MarketSnapshot.

Public surface for ``TokenBalance``, ``PriceData``, ``RSIData``, ``MACDData``,
all ``*Data`` indicator types, and config dataclasses.

This module is intentionally **lean** (PRD §4.8) — only stdlib imports at
module level. The legacy locations
``almanak.framework.strategies.strategy_models`` and
``almanak.framework.strategies.indicator_models`` re-export from this module
during the VIB-4062 transition; commit 6 deletes those re-export shims.

If you are adding a new typed return model for ``MarketSnapshot``, put it
HERE — not in strategies/ or data/. The AST uniqueness gate (PRD §5.1)
enforces this.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass


# =============================================================================
# Balance / price DTOs
# =============================================================================


@dataclass
class TokenBalance:
    """Balance information for a single token.

    Supports numeric comparisons so strategy authors can write:
        if market.balance("ETH") > Decimal("1.0"): ...
        amount = min(trade_size, market.balance("USDC"))

    Comparisons delegate to the ``balance`` field (native units).
    """

    symbol: str
    balance: Decimal
    balance_usd: Decimal
    address: str = ""

    def _to_decimal(self, other: object) -> Decimal | None:
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

    Includes ``source`` (the named provider that produced the datum) so
    accounting writers can stamp ``transaction_ledger.price_inputs_json`` with
    the real provider name (VIB-3889).
    """

    price: Decimal
    price_24h_ago: Decimal = Decimal("0")
    change_24h_pct: Decimal = Decimal("0")
    high_24h: Decimal = Decimal("0")
    low_24h: Decimal = Decimal("0")
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    source: str = ""


# =============================================================================
# Indicator DTOs
# =============================================================================


@dataclass
class RSIData:
    """RSI (Relative Strength Index) data for a token.

    Supports numeric operations so strategy authors can write:
        rsi = market.rsi("ETH")
        if rsi > 70: ...
        round(rsi, 2)
        f"{rsi:.2f}"
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
        if self.value <= self.oversold:
            return "BUY"
        elif self.value >= self.overbought:
            return "SELL"
        return "HOLD"

    @property
    def is_oversold(self) -> bool:
        return self.value <= self.oversold

    @property
    def is_overbought(self) -> bool:
        return self.value >= self.overbought


@dataclass
class MACDData:
    """MACD data for a token."""

    macd_line: Decimal
    signal_line: Decimal
    histogram: Decimal
    fast_period: int = 12
    slow_period: int = 26
    signal_period: int = 9

    @property
    def is_bullish_crossover(self) -> bool:
        return self.histogram > 0

    @property
    def is_bearish_crossover(self) -> bool:
        return self.histogram < 0

    @property
    def signal(self) -> str:
        if self.histogram > 0:
            return "BUY"
        elif self.histogram < 0:
            return "SELL"
        return "HOLD"


@dataclass
class BollingerBandsData:
    """Bollinger Bands data for a token."""

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
        return self.percent_b < Decimal("0")

    @property
    def is_overbought(self) -> bool:
        return self.percent_b > Decimal("1")

    @property
    def is_squeeze(self) -> bool:
        return self.bandwidth < Decimal("0.05")

    @property
    def signal(self) -> str:
        if self.is_oversold:
            return "BUY"
        elif self.is_overbought:
            return "SELL"
        return "HOLD"


@dataclass
class StochasticData:
    """Stochastic Oscillator data for a token."""

    k_value: Decimal
    d_value: Decimal
    k_period: int = 14
    d_period: int = 3
    overbought: Decimal = Decimal("80")
    oversold: Decimal = Decimal("20")

    @property
    def is_oversold(self) -> bool:
        return self.k_value <= self.oversold

    @property
    def is_overbought(self) -> bool:
        return self.k_value >= self.overbought

    @property
    def is_bullish_crossover(self) -> bool:
        return self.k_value > self.d_value and self.is_oversold

    @property
    def is_bearish_crossover(self) -> bool:
        return self.k_value < self.d_value and self.is_overbought

    @property
    def signal(self) -> str:
        if self.is_bullish_crossover:
            return "BUY"
        elif self.is_bearish_crossover:
            return "SELL"
        return "HOLD"


@dataclass
class ATRData:
    """ATR (Average True Range) data for a token."""

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
        return self.value_percent > self.volatility_threshold

    @property
    def is_low_volatility(self) -> bool:
        return self.value_percent <= self.volatility_threshold

    @property
    def signal(self) -> str:
        if self.is_low_volatility:
            return "TRADE"
        return "WAIT"


@dataclass
class MAData:
    """Moving Average data for a token."""

    value: Decimal
    ma_type: str = "SMA"
    period: int = 20
    current_price: Decimal = Decimal("0")

    @property
    def is_price_above(self) -> bool:
        return self.current_price > self.value

    @property
    def is_price_below(self) -> bool:
        return self.current_price < self.value

    @property
    def signal(self) -> str:
        if self.is_price_above:
            return "BULLISH"
        elif self.is_price_below:
            return "BEARISH"
        return "NEUTRAL"


@dataclass
class ADXData:
    """ADX (Average Directional Index) data for a token."""

    adx: Decimal
    plus_di: Decimal
    minus_di: Decimal
    period: int = 14
    trend_threshold: Decimal = Decimal("25")

    @property
    def is_strong_trend(self) -> bool:
        return self.adx >= self.trend_threshold

    @property
    def is_uptrend(self) -> bool:
        return self.is_strong_trend and self.plus_di > self.minus_di

    @property
    def is_downtrend(self) -> bool:
        return self.is_strong_trend and self.minus_di > self.plus_di

    @property
    def signal(self) -> str:
        if self.is_uptrend:
            return "BUY"
        elif self.is_downtrend:
            return "SELL"
        return "HOLD"


@dataclass
class OBVData:
    """OBV (On-Balance Volume) data for a token."""

    obv: Decimal
    signal_line: Decimal
    signal_period: int = 21

    @property
    def is_bullish(self) -> bool:
        return self.obv > self.signal_line

    @property
    def is_bearish(self) -> bool:
        return self.obv < self.signal_line

    @property
    def signal(self) -> str:
        if self.is_bullish:
            return "BUY"
        elif self.is_bearish:
            return "SELL"
        return "HOLD"


@dataclass
class CCIData:
    """CCI (Commodity Channel Index) data for a token."""

    value: Decimal
    period: int = 20
    upper_level: Decimal = Decimal("100")
    lower_level: Decimal = Decimal("-100")

    @property
    def is_oversold(self) -> bool:
        return self.value <= self.lower_level

    @property
    def is_overbought(self) -> bool:
        return self.value >= self.upper_level

    @property
    def signal(self) -> str:
        if self.is_oversold:
            return "BUY"
        elif self.is_overbought:
            return "SELL"
        return "HOLD"


@dataclass
class IchimokuData:
    """Ichimoku Cloud data for a token."""

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
        return max(self.senkou_span_a, self.senkou_span_b)

    @property
    def cloud_bottom(self) -> Decimal:
        return min(self.senkou_span_a, self.senkou_span_b)

    @property
    def is_bullish_crossover(self) -> bool:
        return self.tenkan_sen > self.kijun_sen

    @property
    def is_bearish_crossover(self) -> bool:
        return self.tenkan_sen < self.kijun_sen

    @property
    def is_above_cloud(self) -> bool:
        return self.current_price > self.cloud_top

    @property
    def is_below_cloud(self) -> bool:
        return self.current_price < self.cloud_bottom

    @property
    def signal(self) -> str:
        if self.is_bullish_crossover:
            return "BUY"
        elif self.is_bearish_crossover:
            return "SELL"
        return "HOLD"


# =============================================================================
# Provider type aliases (callable Protocols collapsed to Callable[..., …])
# =============================================================================

PriceOracle = Callable[..., Decimal]
RSIProvider = Callable[..., RSIData]
BalanceProvider = Callable[[str], TokenBalance]


class IndicatorProvider:
    """Provider that wraps all indicator calculators for synchronous access.

    Each attribute corresponds to a MarketSnapshot accessor. The runner
    creates this once and injects it into the strategy, so all indicators
    work out of the box without strategy authors managing calculators.
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


# =============================================================================
# Lazy re-exports — config dataclasses (data-layer source until commit 6)
# =============================================================================
#
# StablecoinConfig and FreshnessConfig live in framework/data/market_snapshot.py
# during the transition. We expose them through this module via PEP 562
# __getattr__ so importing them does not transitively pull web3/pandas into
# the gateway sidecar's import graph (PRD §4.8 lean budget).


def __getattr__(name: str):
    if name in {"StablecoinConfig", "FreshnessConfig"}:
        from ..data.market_snapshot import FreshnessConfig, StablecoinConfig

        return {"StablecoinConfig": StablecoinConfig, "FreshnessConfig": FreshnessConfig}[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "TokenBalance",
    "PriceData",
    "PriceOracle",
    "RSIProvider",
    "BalanceProvider",
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
    # Lazy-loaded via __getattr__ to keep this module lean (PRD §4.8).
    "StablecoinConfig",  # noqa: F822
    "FreshnessConfig",  # noqa: F822
]
