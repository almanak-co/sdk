"""Bracketed band growth brackets a known synthetic fee path and is exact at initialized ticks."""

import random
from types import SimpleNamespace

import pytest

from almanak.connectors.band_fee_growth import (
    MODULUS,
    BandGrowth,
    band_growth,
    below,
    bracket,
    continuous,
    interval,
    partition,
    validate_envelope,
)
from almanak.connectors.v3_fee_growth import inside
from tests.band_pool_ledger import Ledger

SPACING = 10


def everywhere(_tick):
    return True


def walk(seed, samples=12, steps=15):
    """A seeded path of accruals, price moves and liquidity changes, sampled at intervals."""
    rng = random.Random(seed)
    ledger = Ledger(tick=5)
    # The widest position never exits: random burns cannot clear its boundary ticks.
    for lower, upper, liquidity in ((-300, 300, 10**30), (-100, 50, 10**18), (0, 200, 10**18)):
        ledger.modify(lower, upper, liquidity)
    taken, marks, changes = [ledger.sample()], [ledger.mark()], []
    for _ in range(samples - 1):
        for _ in range(steps):
            action = rng.random()
            if action < 0.5:
                ledger.accrue(rng.randrange(1, 2**100), rng.randrange(1, 2**100))
            elif action < 0.85:
                ledger.move(max(-395, min(395, ledger.tick + rng.randrange(-60, 61))))
            else:
                liquidity_step(rng, ledger)
        previous = marks[-1][1]
        marks.append(ledger.mark())
        changes.append(tuple(ledger.events[previous:]))
        taken.append(ledger.sample())
    return ledger, taken, marks, changes


def liquidity_step(rng, ledger):
    lower = rng.randrange(-40, 30) * SPACING
    upper = lower + rng.randrange(1, 10) * SPACING
    held = min(ledger.ticks.get(t, [0])[0] for t in (lower, upper))
    if held and rng.random() < 0.6:
        ledger.modify(lower, upper, -rng.randrange(1, held + 1))
    else:
        ledger.modify(lower, upper, rng.randrange(1, 10**18))


@pytest.mark.parametrize("seed", range(8))
def test_every_interval_brackets_the_growth_a_hypothetical_range_actually_earned(seed):
    ledger, samples, marks, changes = walk(seed)
    growth = band_growth(samples, changes, everywhere)
    rng = random.Random(seed + 1000)
    ranges = [(-100, 50), (0, 200)] + [
        (low, low + rng.randrange(1, 30) * SPACING)
        for low in (rng.randrange(-40, 30) * SPACING for _ in range(12))
    ]
    shapes = set()
    for lower, upper in ranges:
        least, most = (0, 0), (0, 0)
        for k, item in enumerate(growth.intervals):
            earned = ledger.earned(lower, upper, marks[k][0], marks[k + 1][0])
            low, high = bracket(item, lower, upper)
            assert all(a <= e <= b for a, e, b in zip(low, earned, high, strict=True))
            if lower in item.ticks and upper in item.ticks:
                assert low == earned == high
            shapes.add("exact" if low == high else "bracketed")
            least = tuple((x + y) % MODULUS for x, y in zip(least, low, strict=True))
            most = tuple((x + y) % MODULUS for x, y in zip(most, high, strict=True))
        final = growth.at(len(growth.intervals), lower, upper)
        assert (final.lower, final.upper) == (least, most)
    # The path exercises both exact and strictly bracketed intervals.
    assert shapes == {"exact", "bracketed"}


def test_continuously_initialized_boundaries_match_the_exact_fixed_range_growth():
    ledger, samples, marks, changes = walk(3)
    growth = band_growth(samples, changes, everywhere)
    for k, (previous, current) in enumerate(zip(samples, samples[1:], strict=False)):

        def exact(sample):
            ticks = {t.index: t for t in sample.ticks}
            return inside(
                sample.pool.tick,
                -300,
                300,
                sample.global_growth,
                (ticks[-300].outside0, ticks[-300].outside1),
                (ticks[300].outside0, ticks[300].outside1),
            )

        fixed = tuple(
            (b - a) % MODULUS for a, b in zip(exact(previous), exact(current), strict=True)
        )
        low, high = bracket(growth.intervals[k], -300, 300)
        assert low == high == fixed == ledger.earned(-300, 300, marks[k][0], marks[k + 1][0])


def test_uninitialized_boundaries_between_sparse_ticks_credit_nothing_but_retain_the_bound():
    ledger = Ledger(tick=0)
    ledger.modify(-1000, 1000, 10**18)
    start = ledger.sample()
    ledger.accrue(7, 11)
    ledger.move(40)
    ledger.accrue(3, 5)
    end = ledger.sample()
    item = interval(start, end, continuous(start.ticks, end.ticks, (), everywhere))
    assert bracket(item, -100, 100) == ((0, 0), (10, 16))
    assert bracket(item, -1000, 1000) == ((10, 16), (10, 16))
    # Past the last sampled tick the growth below is bracketed by the global growth.
    assert bracket(item, 500, 2000) == ((0, 0), (10, 16))
    assert bracket(item, 2000, 3000) == ((0, 0), (0, 0))


def test_a_tick_cleared_and_reinitialized_between_samples_is_not_continuous():
    ledger = Ledger(tick=0)
    ledger.modify(-100, 100, 5)
    ledger.modify(-200, 200, 5)
    start = ledger.sample()
    ledger.modify(-100, 100, -5)
    ledger.accrue(9, 9)
    ledger.modify(-100, 100, 5)
    end = ledger.sample()
    kept = continuous(start.ticks, end.ticks, ledger.events[2:], everywhere)
    assert kept == (-200, 200)
    assert bracket(interval(start, end, kept), -100, 100) == ((0, 0), (9, 9))


def test_unreconciled_or_impossible_liquidity_history_fails():
    ledger = Ledger(tick=0)
    ledger.modify(-100, 100, 5)
    start = ledger.sample()
    ledger.modify(-100, 100, 3)
    end = ledger.sample()
    with pytest.raises(ValueError, match="does not reconcile"):
        continuous(start.ticks, end.ticks, (), everywhere)
    with pytest.raises(ValueError, match="removes more"):
        continuous(start.ticks, end.ticks, ((-100, 100, -6),), everywhere)
    # Uncovered ticks are outside the sampled words and cannot be reconciled.
    assert continuous(start.ticks, start.ticks, ((500, 600, 1),), lambda t: t < 500) == (-100, 100)
    with pytest.raises(ValueError, match="every sample interval"):
        band_growth((start, end), (), everywhere)


def test_growth_below_follows_the_pool_convention_and_refuses_disordered_samples():
    tick = SimpleNamespace(index=10, outside0=3, outside1=4)
    assert below(tick, 10, (9, 9)) == (3, 4)
    assert below(tick, 9, (2, 9)) == ((2 - 3) % MODULUS, 5)
    ledger = Ledger(tick=0)
    ledger.modify(-100, 100, 5)
    start = ledger.sample()
    ledger.accrue(5, 5)
    end = ledger.sample()
    corrupt = SimpleNamespace(
        pool=end.pool,
        global_growth=end.global_growth,
        ticks=(SimpleNamespace(**{**vars(end.ticks[0]), "outside0": 7}), end.ticks[1]),
    )
    with pytest.raises(ValueError, match="not ordered"):
        interval(start, corrupt, (-100, 100))


def test_cumulative_growth_requires_a_sample_index_and_an_ordered_range():
    growth = BandGrowth(intervals=())
    assert growth.at(0, -10, 10).lower == (0, 0)
    with pytest.raises(ValueError, match="sample index"):
        growth.at(1, -10, 10)
    with pytest.raises(ValueError, match="increasing"):
        growth.at(0, 10, 10)


def test_prepared_coverage_must_contain_the_whole_envelope():
    band = SimpleNamespace(lower=-500, upper=500)
    validate_envelope(band, -500, 500, 10)
    with pytest.raises(ValueError, match="inside explicit prepared tick coverage"):
        validate_envelope(band, -490, 600, 10)


def test_events_belong_to_the_interval_ending_at_the_first_sample_at_or_after_them():
    events = ((5, (0, 10, 1)), (10, (0, 10, 2)), (11, (0, 10, 3)), (20, (0, 10, 4)))
    assert partition(events, (5, 10, 20)) == (((0, 10, 2),), ((0, 10, 3), (0, 10, 4)))
    assert partition(events, (5, 5, 20)) == ((), ((0, 10, 2), (0, 10, 3), (0, 10, 4)))
    with pytest.raises(ValueError, match="canonical block order"):
        partition(tuple(reversed(events)), (5, 20))
