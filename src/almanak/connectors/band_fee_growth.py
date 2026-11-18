"""Bracketed historical fee growth for position ranges that history never initialized.

Between two pool samples, the growth accrued while the pool tick was below a tick T is
exact only when T stayed initialized for the whole interval: the pool updates a tick's
outside growth when price crosses it and discards it when the tick clears. A position
boundary x that lies between two such ticks accrued at least the growth below the nearer
one at or under it and at most the growth below the nearer one at or above it; past the
last sampled tick the bounds are zero and the whole global growth. Growth inside [a, b)
is the growth below b minus the growth below a, so every interval yields a lower and an
upper bound, and they coincide when both boundaries are continuously initialized ticks.
Nothing is interpolated between samples or extrapolated past the sampled bitmap words.
"""

from bisect import bisect_left, bisect_right
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from itertools import pairwise
from typing import Protocol

MODULUS = 2**256

Pair = tuple[int, int]


class TickGrowth(Protocol):
    index: int
    gross: int
    outside0: int
    outside1: int


class PoolGrowth(Protocol):
    @property
    def tick(self) -> int: ...


class GrowthSample(Protocol):
    @property
    def pool(self) -> PoolGrowth: ...

    @property
    def ticks(self) -> Sequence[TickGrowth]: ...

    @property
    def global_growth(self) -> Pair: ...


@dataclass(frozen=True)
class Bounds:
    """Cumulative growth inside one range: `lower` is credited, `upper` is only reported."""

    lower: Pair
    upper: Pair


@dataclass(frozen=True)
class Interval:
    ticks: tuple[int, ...]
    below: tuple[Pair, ...]
    total: Pair


class CoverageExceeded(RuntimeError):
    """A band position left the prepared tick coverage, so the run cannot value it.

    Not a ValueError: economic models record those as a refused action and continue,
    which would let a run finish with idle capital instead of naming the missing data.
    """


def validate_envelope(band, lower: int, upper: int, spacing: int) -> None:
    # Any position a strategy may open lies inside the envelope, so the whole envelope
    # must be sampled; a partial envelope would leave position boundaries unmeasured.
    # An unbounded band follows the price, so its coverage is the author's explicit
    # choice of where history is measured and must hold at least one aligned position.
    if band.lower is None or band.upper is None:
        first = -(-lower // spacing) * spacing
        last = upper // spacing * spacing
        if last - first < band.width:
            raise ValueError("prepared tick coverage is narrower than one liquidity band width")
    elif not lower <= band.lower < band.upper <= upper:
        raise ValueError("liquidity band must lie inside explicit prepared tick coverage")


def validate_covered(lower: int, upper: int, selected) -> None:
    """A band position whose boundaries leave the sampled coverage has unmeasured fees."""
    if not lower <= selected.lower < selected.upper <= upper:
        raise CoverageExceeded(
            "liquidity band position leaves the prepared tick coverage; widen data.coverage"
        )


def validate_band_coverage(band, sample, spacing: int) -> None:
    """Coverage checks at book open: the envelope, or the first unbounded position."""
    validate_envelope(band, sample.lower, sample.upper, spacing)
    if not band.bounded:
        validate_covered(sample.lower, sample.upper, band.centered(sample.pool.tick, spacing))


def below(tick: TickGrowth, current: int, total: Pair) -> Pair:
    """Growth below an initialized tick, as V3 and V4 `getFeeGrowthInside` read it."""
    outside = (tick.outside0, tick.outside1)
    if current >= tick.index:
        return outside
    return (total[0] - outside[0]) % MODULUS, (total[1] - outside[1]) % MODULUS


def continuous(
    previous: Sequence[TickGrowth],
    current: Sequence[TickGrowth],
    changes: Iterable[tuple[int, int, int]],
    covered: Callable[[int], bool],
) -> tuple[int, ...]:
    """Ticks initialized at both samples whose gross liquidity never cleared in between.

    `changes` are the canonical (lower, upper, signed liquidity) events after the first
    sample through the second. Every covered tick must reconcile with the second sample,
    so a missing or extra event fails instead of silently widening a bracket.
    """
    gross = {tick.index: tick.gross for tick in previous}
    cleared = set()
    for lower, upper, change in changes:
        for index in (lower, upper):
            if covered(index):
                gross[index] = gross.get(index, 0) + change
                if gross[index] < 0:
                    raise ValueError("liquidity history removes more than a sampled tick holds")
                if not gross[index]:
                    cleared.add(index)
    if {i: v for i, v in gross.items() if v} != {t.index: t.gross for t in current}:
        raise ValueError("liquidity history does not reconcile with the sampled ticks")
    return tuple(tick.index for tick in previous if tick.index not in cleared)


def interval(previous: GrowthSample, current: GrowthSample, indices: Iterable[int]) -> Interval:
    start = {tick.index: tick for tick in previous.ticks}
    end = {tick.index: tick for tick in current.ticks}
    total = delta(previous.global_growth, current.global_growth)
    ticks = tuple(sorted(indices))
    rows = tuple(
        delta(
            below(start[index], previous.pool.tick, previous.global_growth),
            below(end[index], current.pool.tick, current.global_growth),
        )
        for index in ticks
    )
    result = Interval(ticks=ticks, below=rows, total=total)
    validate_order(result)
    return result


def delta(before: Pair, after: Pair) -> Pair:
    return (after[0] - before[0]) % MODULUS, (after[1] - before[1]) % MODULUS


def validate_order(item: Interval) -> None:
    # Growth below a higher tick includes the growth below every lower one and never
    # exceeds the global growth; anything else means the sampled state is inconsistent.
    for token in (0, 1):
        values = (*(row[token] for row in item.below), item.total[token])
        if any(low > high for low, high in pairwise(values)):
            raise ValueError("sampled tick growth is not ordered by tick; history is inconsistent")


def bracket(item: Interval, lower: int, upper: int) -> tuple[Pair, Pair]:
    def least_below(tick):
        position = bisect_right(item.ticks, tick) - 1
        return item.below[position] if position >= 0 else (0, 0)

    def most_below(tick):
        position = bisect_left(item.ticks, tick)
        return item.below[position] if position < len(item.ticks) else item.total

    floor_upper, ceil_lower = least_below(upper), most_below(lower)
    ceil_upper, floor_lower = most_below(upper), least_below(lower)
    least = (max(0, floor_upper[0] - ceil_lower[0]), max(0, floor_upper[1] - ceil_lower[1]))
    most = (ceil_upper[0] - floor_lower[0], ceil_upper[1] - floor_lower[1])
    return least, most


@dataclass
class BandGrowth:
    """Cumulative bracketed growth per concrete range, indexed by prepared sample."""

    intervals: tuple[Interval, ...]
    cache: dict[tuple[int, int], tuple[Bounds, ...]] = field(default_factory=dict)

    def at(self, index: int, lower: int, upper: int) -> Bounds:
        if not 0 <= index <= len(self.intervals):
            raise ValueError("band growth requires a prepared sample index")
        if lower >= upper:
            raise ValueError("band growth requires an increasing tick range")
        key = (lower, upper)
        if key not in self.cache:
            self.cache[key] = self.cumulative(lower, upper)
        return self.cache[key][index]

    def cumulative(self, lower: int, upper: int) -> tuple[Bounds, ...]:
        least, most = (0, 0), (0, 0)
        result = [Bounds(lower=least, upper=most)]
        for item in self.intervals:
            low, high = bracket(item, lower, upper)
            least = ((least[0] + low[0]) % MODULUS, (least[1] + low[1]) % MODULUS)
            most = ((most[0] + high[0]) % MODULUS, (most[1] + high[1]) % MODULUS)
            result.append(Bounds(lower=least, upper=most))
        return tuple(result)


def partition(
    events: Sequence[tuple[int, tuple[int, int, int]]], blocks: Sequence[int]
) -> tuple[tuple[tuple[int, int, int], ...], ...]:
    """Split (block number, change) events into the intervals between sample blocks.

    A sample is end-of-block state, so an event belongs to the interval that ends at
    the first sample block at or after its own block.
    """
    numbers = [number for number, _ in events]
    if numbers != sorted(numbers):
        raise ValueError("liquidity events must retain canonical block order")
    return tuple(
        tuple(
            change
            for _, change in events[bisect_right(numbers, left) : bisect_right(numbers, right)]
        )
        for left, right in pairwise(blocks)
    )


def band_growth(
    samples: Sequence[GrowthSample],
    changes: Sequence[Sequence[tuple[int, int, int]]],
    covered: Callable[[int], bool],
) -> BandGrowth:
    """`changes[k]` holds the liquidity events between samples k and k + 1."""
    if len(changes) != len(samples) - 1:
        raise ValueError("band growth requires the liquidity events of every sample interval")
    intervals = tuple(
        interval(previous, current, continuous(previous.ticks, current.ticks, events, covered))
        for (previous, current), events in zip(pairwise(samples), changes, strict=True)
    )
    return BandGrowth(intervals=intervals)
