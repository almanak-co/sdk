"""A declared strategy price series as one decision sees it, in every run mode.

A strategy declares each series with a strategy-purpose `PriceBinding` from its
`data_dependencies` hook: one pool, its timeframe and a window. Every decision then
receives, for each binding, the latest `window` points that were available at the
decision's cutoff, oldest first. A point arrives only once its candle boundary has
passed and its availability lag has elapsed; nothing later is visible, nothing
missing is filled, and no other source stands in for a failed one.

Each point prices the declared asset in the pool's other token (`quote`) and is dated
by its boundary. A `PoolPrice` series carries closes only; a `PoolSwapCandles` series
also carries each candle's open time, open, high, low and volume.
"""

from collections.abc import Iterable
from decimal import Decimal
from typing import Annotated, Any, Protocol

from pydantic import Field

from almanak.backtest.pricing import PoolSeriesSource, PoolSwapCandles, Price, PriceBinding
from almanak.backtest.types import UnixTime
from almanak.caip import AssetType
from almanak.models import Model


class PricePoint(Model):
    # The candle boundary the point is dated by.
    source_time: UnixTime
    # The pool's price at the boundary: the candle's close.
    value: Price
    # Swap candle members; absent for a closes-only series.
    opened_at: UnixTime | None = Field(default=None, exclude_if=lambda v: v is None)
    open: Price | None = Field(default=None, exclude_if=lambda v: v is None)
    high: Price | None = Field(default=None, exclude_if=lambda v: v is None)
    low: Price | None = Field(default=None, exclude_if=lambda v: v is None)
    volume: Annotated[Decimal, Field(ge=0, allow_inf_nan=False)] | None = Field(
        default=None, exclude_if=lambda v: v is None
    )


class PriceSeries(Model):
    binding: PriceBinding
    # The token the points are priced in; absent while the series has no point.
    quote: AssetType | None = Field(default=None, exclude_if=lambda v: v is None)
    # Information after this instant is not visible to the decision.
    cutoff: UnixTime
    points: tuple[PricePoint, ...]
    # Digests of the retained on-chain evidence the points came from.
    evidence: tuple[str, ...]

    @property
    def values(self) -> tuple[Decimal, ...]:
        """The closes, for candles."""
        return tuple(point.value for point in self.points)

    @property
    def opens(self) -> tuple[Decimal, ...]:
        return self.candle_member("open")

    @property
    def highs(self) -> tuple[Decimal, ...]:
        return self.candle_member("high")

    @property
    def lows(self) -> tuple[Decimal, ...]:
        return self.candle_member("low")

    @property
    def volumes(self) -> tuple[Decimal, ...]:
        return self.candle_member("volume")

    def candle_member(self, name: str) -> tuple[Decimal, ...]:
        if not isinstance(self.binding.source, PoolSwapCandles):
            raise ValueError(f"{name} requires a pool swap candle series (PoolSwapCandles)")
        return tuple(getattr(point, name) for point in self.points)

    @property
    def latest(self) -> PricePoint | None:
        return self.points[-1] if self.points else None

    @property
    def gaps(self) -> tuple[tuple[UnixTime, UnixTime], ...]:
        """Consecutive points further apart than the timeframe."""
        seconds = interval_seconds(self.binding)
        return tuple(
            (before.source_time, after.source_time)
            for before, after in zip(self.points, self.points[1:], strict=False)
            if after.source_time - before.source_time != seconds
        )

    @property
    def complete(self) -> bool:
        """The full window, evenly spaced, with its latest point within the source age."""
        latest = self.latest
        source = self.binding.source
        return (
            latest is not None
            and len(self.points) == self.binding.window
            and not self.gaps
            and isinstance(source, PoolSeriesSource)
            and self.cutoff - latest.source_time <= source.max_age_seconds
        )


class SourceRow(Protocol):
    """A dated source row: a prepared record or a point a live gateway read."""

    @property
    def source_time(self) -> int: ...

    @property
    def available_at(self) -> int: ...

    @property
    def evidence(self) -> tuple[str, ...]: ...

    @property
    def payload(self) -> Any: ...


def interval_seconds(binding: PriceBinding) -> int:
    if not isinstance(binding.source, PoolSeriesSource):
        raise ValueError("a price series reads a pool (PoolPrice or PoolSwapCandles)")
    return binding.source.seconds


def series_at(binding: PriceBinding, rows: Iterable[SourceRow], cutoff: int) -> PriceSeries:
    """The binding's window as of `cutoff`, from retained source rows.

    Rows available after the cutoff are excluded, so a row and the series built from
    it agree whether the rows come from a prepared dataset or a live read.
    """
    interval_seconds(binding)
    visible = sorted(
        (row for row in rows if row.available_at <= cutoff), key=lambda row: row.source_time
    )
    selected = visible[-binding.window :]
    return PriceSeries(
        binding=binding,
        quote=selected[-1].payload.quote if selected else None,
        cutoff=UnixTime(cutoff),
        points=tuple(point(binding, row) for row in selected),
        evidence=tuple(dict.fromkeys(digest for row in selected for digest in row.evidence)),
    )


def point(binding: PriceBinding, row: SourceRow) -> PricePoint:
    payload = row.payload
    if not isinstance(binding.source, PoolSwapCandles):
        return PricePoint(source_time=UnixTime(row.source_time), value=payload.close)
    return PricePoint(
        source_time=UnixTime(row.source_time),
        value=payload.close,
        opened_at=UnixTime(row.source_time - binding.source.seconds),
        open=payload.open,
        high=payload.high,
        low=payload.low,
        volume=payload.volume,
    )
