"""Canonical OHLCV timeframe vocabulary and provider capability contracts.

This module is intentionally dependency-free beyond the Python standard
library.  Both the untrusted framework container and the trusted gateway
egress layer import it, so putting the vocabulary behind either layer would
invert one side of the dependency graph.

``OHLCVTimeframe`` inherits from :class:`str` through :class:`enum.StrEnum`.
Its values therefore remain byte-for-byte compatible with the historical
public, protobuf, JSON, and SQLite representations (``"1h"`` stays ``"1h"``)
while internal APIs gain a closed, statically-checkable vocabulary.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from types import MappingProxyType
from typing import Generic, TypeVar


class OHLCVTimeframe(StrEnum):
    """Candle intervals supported by the SDK OHLCV contract."""

    ONE_MINUTE = "1m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    ONE_HOUR = "1h"
    FOUR_HOURS = "4h"
    ONE_DAY = "1d"

    @property
    def seconds(self) -> int:
        """Exact duration of one candle in seconds."""
        return _TIMEFRAME_SECONDS[self]


CANONICAL_OHLCV_TIMEFRAMES: tuple[OHLCVTimeframe, ...] = tuple(OHLCVTimeframe)
"""Canonical intervals in finest-to-coarsest order."""

CANONICAL_OHLCV_TIMEFRAME_VALUES: tuple[str, ...] = tuple(timeframe.value for timeframe in CANONICAL_OHLCV_TIMEFRAMES)
"""String projection retained for compatibility and boundary messages."""

_TIMEFRAME_SECONDS: Mapping[OHLCVTimeframe, int] = MappingProxyType(
    {
        OHLCVTimeframe.ONE_MINUTE: 60,
        OHLCVTimeframe.FIVE_MINUTES: 5 * 60,
        OHLCVTimeframe.FIFTEEN_MINUTES: 15 * 60,
        OHLCVTimeframe.ONE_HOUR: 60 * 60,
        OHLCVTimeframe.FOUR_HOURS: 4 * 60 * 60,
        OHLCVTimeframe.ONE_DAY: 24 * 60 * 60,
    }
)


def parse_ohlcv_timeframe(value: object, *, field_name: str = "timeframe") -> OHLCVTimeframe:
    """Parse a public/config/wire value without semantic normalization.

    Existing callers may continue passing the historical strings.  Case,
    whitespace, and aliases such as ``"60m"`` are deliberately *not*
    normalized: doing so could label candles with a different interval than
    the upstream actually returned.

    Args:
        value: An :class:`OHLCVTimeframe` or its exact historical string value.
        field_name: Boundary field name used in actionable error messages.

    Raises:
        TypeError: If ``value`` is not a string-like timeframe.
        ValueError: If the string is outside the canonical vocabulary.
    """
    if isinstance(value, OHLCVTimeframe):
        return value
    if not isinstance(value, str):
        raise TypeError(f"{field_name} must be an OHLCVTimeframe or string; got {type(value).__name__}")
    try:
        return OHLCVTimeframe(value)
    except ValueError as exc:
        supported = ", ".join(CANONICAL_OHLCV_TIMEFRAME_VALUES)
        raise ValueError(
            f"Invalid {field_name} {value!r}. Expected one of: {supported}. "
            "Aliases, case changes, and whitespace are not accepted because "
            "they may represent a different candle interval."
        ) from exc


_ProviderValueT = TypeVar("_ProviderValueT")


@dataclass(frozen=True)
class OHLCVTimeframeCapabilities(Generic[_ProviderValueT]):
    """Exhaustive provider mapping over the canonical timeframe vocabulary.

    Every canonical interval must appear exactly once: either in ``mapping``
    with the provider-native request value/plan, or in ``unsupported``.  This
    turns capability drift into an import/test failure instead of a late
    request failure or silent interval substitution.
    """

    provider: str
    mapping: Mapping[OHLCVTimeframe, _ProviderValueT]
    unsupported: frozenset[OHLCVTimeframe]

    def __post_init__(self) -> None:
        mapping = dict(self.mapping)
        if not all(isinstance(key, OHLCVTimeframe) for key in mapping):
            raise TypeError(f"{self.provider} OHLCV mapping keys must be OHLCVTimeframe values")
        if not all(isinstance(value, OHLCVTimeframe) for value in self.unsupported):
            raise TypeError(f"{self.provider} unsupported values must be OHLCVTimeframe values")

        supported = frozenset(mapping)
        overlap = supported & self.unsupported
        if overlap:
            rendered = ", ".join(timeframe.value for timeframe in self._ordered(overlap))
            raise ValueError(f"{self.provider} marks OHLCV timeframes both supported and unsupported: {rendered}")

        accounted = supported | self.unsupported
        canonical = frozenset(CANONICAL_OHLCV_TIMEFRAMES)
        if accounted != canonical:
            missing = canonical - accounted
            extra = accounted - canonical
            details: list[str] = []
            if missing:
                details.append("missing=" + ",".join(timeframe.value for timeframe in self._ordered(missing)))
            if extra:
                details.append("extra=" + ",".join(str(timeframe) for timeframe in extra))
            raise ValueError(f"{self.provider} OHLCV capabilities are not exhaustive ({'; '.join(details)})")

        object.__setattr__(self, "mapping", MappingProxyType(mapping))

    @staticmethod
    def _ordered(values: frozenset[OHLCVTimeframe]) -> tuple[OHLCVTimeframe, ...]:
        return tuple(timeframe for timeframe in CANONICAL_OHLCV_TIMEFRAMES if timeframe in values)

    @property
    def supported(self) -> tuple[OHLCVTimeframe, ...]:
        """Supported canonical intervals in canonical order."""
        keys = frozenset(self.mapping)
        return self._ordered(keys)

    def resolve(self, timeframe: OHLCVTimeframe) -> _ProviderValueT:
        """Return the provider-native mapping or raise an actionable refusal."""
        try:
            return self.mapping[timeframe]
        except KeyError as exc:
            supported = ", ".join(value.value for value in self.supported)
            raise ValueError(
                f"{self.provider} does not support OHLCV timeframe {timeframe.value!r}; supported: {supported}"
            ) from exc


@dataclass(frozen=True)
class CoinGeckoOHLCVPlan:
    """CoinGecko ``/ohlc`` request and native candle granularity."""

    days: str
    native_stride_seconds: int
    max_candles: int

    def validate_limit(self, timeframe: OHLCVTimeframe, limit: int) -> None:
        """Reject requests larger than this fixed upstream window can supply."""
        if limit > self.max_candles:
            raise ValueError(
                f"CoinGecko OHLC timeframe {timeframe.value!r} supports at most "
                f"{self.max_candles} candles per request; got limit={limit}"
            )


@dataclass(frozen=True)
class CoinGeckoOnchainOHLCVParams:
    """CoinGecko Onchain path segment and aggregate query value."""

    timeframe: str
    aggregate: str


BINANCE_OHLCV_TIMEFRAMES = OHLCVTimeframeCapabilities[str](
    provider="Binance",
    mapping={timeframe: timeframe.value for timeframe in CANONICAL_OHLCV_TIMEFRAMES},
    unsupported=frozenset(),
)

COINGECKO_OHLCV_TIMEFRAMES = OHLCVTimeframeCapabilities[CoinGeckoOHLCVPlan](
    provider="CoinGecko OHLC",
    mapping={
        OHLCVTimeframe.ONE_HOUR: CoinGeckoOHLCVPlan(
            days="1",
            native_stride_seconds=30 * 60,
            max_candles=24,
        ),
        OHLCVTimeframe.FOUR_HOURS: CoinGeckoOHLCVPlan(
            days="30",
            native_stride_seconds=4 * 60 * 60,
            max_candles=180,
        ),
        OHLCVTimeframe.ONE_DAY: CoinGeckoOHLCVPlan(
            days="30",
            native_stride_seconds=4 * 60 * 60,
            max_candles=30,
        ),
    },
    unsupported=frozenset(
        {
            OHLCVTimeframe.ONE_MINUTE,
            OHLCVTimeframe.FIVE_MINUTES,
            OHLCVTimeframe.FIFTEEN_MINUTES,
        }
    ),
)

COINGECKO_ONCHAIN_OHLCV_TIMEFRAMES = OHLCVTimeframeCapabilities[CoinGeckoOnchainOHLCVParams](
    provider="CoinGecko Onchain",
    mapping={
        OHLCVTimeframe.ONE_MINUTE: CoinGeckoOnchainOHLCVParams(timeframe="minute", aggregate="1"),
        OHLCVTimeframe.FIVE_MINUTES: CoinGeckoOnchainOHLCVParams(timeframe="minute", aggregate="5"),
        OHLCVTimeframe.FIFTEEN_MINUTES: CoinGeckoOnchainOHLCVParams(timeframe="minute", aggregate="15"),
        OHLCVTimeframe.ONE_HOUR: CoinGeckoOnchainOHLCVParams(timeframe="hour", aggregate="1"),
        OHLCVTimeframe.FOUR_HOURS: CoinGeckoOnchainOHLCVParams(timeframe="hour", aggregate="4"),
        OHLCVTimeframe.ONE_DAY: CoinGeckoOnchainOHLCVParams(timeframe="day", aggregate="1"),
    },
    unsupported=frozenset(),
)


__all__ = [
    "BINANCE_OHLCV_TIMEFRAMES",
    "CANONICAL_OHLCV_TIMEFRAMES",
    "CANONICAL_OHLCV_TIMEFRAME_VALUES",
    "COINGECKO_OHLCV_TIMEFRAMES",
    "COINGECKO_ONCHAIN_OHLCV_TIMEFRAMES",
    "CoinGeckoOHLCVPlan",
    "CoinGeckoOnchainOHLCVParams",
    "OHLCVTimeframe",
    "OHLCVTimeframeCapabilities",
    "parse_ohlcv_timeframe",
]
