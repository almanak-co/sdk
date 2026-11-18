"""Bounded independent trades that exercise an authored V4 LP range."""

from dataclasses import dataclass
from types import SimpleNamespace

from almanak.connectors.uniswap_v4.economic_acquisition import tick_state
from almanak.connectors.uniswap_v4.pool import read_pool
from almanak.connectors.uniswap_v4.swap_math import exact_input

MAX_BITMAP_WORDS = 4
MAX_INITIALIZED_TICKS = 512


@dataclass(frozen=True)
class FeeExercisePlan:
    amount: int
    entry_amount: int
    exercise_amount: int
    maximum_amount: int
    zero_for_one: bool
    start_tick: int
    end_tick: int
    coverage_lower: int
    coverage_upper: int
    initialized_ticks: int


@dataclass(frozen=True)
class _Bound:
    lower: int
    upper: int


@dataclass(frozen=True)
class _Policy:
    bound: _Bound
    max_bitmap_words: int = MAX_BITMAP_WORDS
    max_initialized_ticks: int = MAX_INITIALIZED_TICKS

    def for_target(self, target):
        return self.bound


def observed_plan(reader, target, block, exercise_amount, maximum_amount):
    """Read only the finite tick interval needed by the independent fee trade."""
    if target.range is None:
        raise ValueError("fee exercise requires an authored LP range")
    pool = read_pool(reader, target.pool, block)
    spacing = target.pool.tick_spacing
    bound = _Bound(
        lower=min(target.range.lower, pool.tick - spacing),
        upper=max(target.range.upper, pool.tick + spacing),
    )
    words, ticks = tick_state(reader, target, block, _Policy(bound))
    sample = SimpleNamespace(
        pool=pool, lower=bound.lower, upper=bound.upper, words=words, ticks=ticks, hook=None
    )
    return fee_exercise_plan(sample, target, exercise_amount, maximum_amount)


def fee_exercise_plan(sample, target, exercise_amount, maximum_amount):
    """Find the smallest exact-input trade that enters and then exercises the range."""
    if target.range is None:
        raise ValueError("fee exercise requires an authored LP range")
    if not 0 < exercise_amount < maximum_amount < 2**127:
        raise ValueError("fee exercise requires positive amounts within its finite int128 budget")
    if sample.lower > target.range.lower or sample.upper < target.range.upper:
        raise ValueError("fee exercise sample does not cover the authored range")
    start = sample.pool.tick
    zero_for_one = target.funding_currency == target.assets[0]
    if start >= target.range.upper and not zero_for_one:
        raise ValueError(
            "configured fixture asset cannot move the pool down into the authored range"
        )
    if start < target.range.lower and zero_for_one:
        raise ValueError("configured fixture asset cannot move the pool up into the authored range")
    entry = 0
    if not target.range.lower <= start < target.range.upper:
        entry = minimum_entry_amount(
            sample,
            target.range.lower if start < target.range.lower else target.range.upper,
            zero_for_one,
            maximum_amount - exercise_amount,
        )
    amount = entry + exercise_amount
    try:
        result = exact_input(sample, amount, zero_for_one)
    except ValueError as exc:
        if str(exc) == "swap requires liquidity outside prepared tick coverage":
            raise ValueError(
                "fee exercise sample coverage is insufficient for the bounded trade"
            ) from exc
        raise
    if not target.range.lower <= result.tick < target.range.upper:
        raise ValueError("bounded fee exercise does not finish inside the authored range")
    return FeeExercisePlan(
        amount=amount,
        entry_amount=entry,
        exercise_amount=exercise_amount,
        maximum_amount=maximum_amount,
        zero_for_one=zero_for_one,
        start_tick=start,
        end_tick=result.tick,
        coverage_lower=sample.lower,
        coverage_upper=sample.upper,
        initialized_ticks=len(sample.ticks),
    )


def minimum_entry_amount(sample, boundary, zero_for_one, budget):
    if budget <= 0 or not reaches_boundary(sample, boundary, zero_for_one, budget):
        raise ValueError("fee exercise fixture budget cannot reach the authored range")
    lower, upper = 0, budget
    while upper - lower > 1:
        middle = (lower + upper) // 2
        if reaches_boundary(sample, boundary, zero_for_one, middle):
            upper = middle
        else:
            lower = middle
    return upper


def reaches_boundary(sample, boundary, zero_for_one, amount):
    limited = SimpleNamespace(
        pool=sample.pool,
        lower=boundary if zero_for_one else sample.lower,
        upper=sample.upper if zero_for_one else boundary,
        words=sample.words,
        ticks=sample.ticks,
        hook=getattr(sample, "hook", None),
    )
    try:
        result = exact_input(limited, amount, zero_for_one)
    except ValueError as exc:
        if str(exc) == "swap requires liquidity outside prepared tick coverage":
            return True
        raise
    return result.tick < boundary if zero_for_one else result.tick >= boundary
