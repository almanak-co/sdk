"""The diluted fee estimate scales credited fees by active over active plus own liquidity."""

from datetime import UTC, datetime
from fractions import Fraction
from types import SimpleNamespace

import pytest

from almanak.connectors import v3_economic_reporting as reporting
from almanak.connectors.v3_fee_dilution import (
    METHOD,
    active_liquidity,
    diluted,
    estimate,
    holdings,
    intervals,
)
from almanak.connectors.v3_fee_growth import Q128

START = 1_800_000_000
SECONDS = 60
POOL = "pool"
LOWER, UPPER = -600, 600
ACTIVE = 2**60
# Fee growth per unit of liquidity, per sample interval in range, in both assets.
# Powers of two, so an interval's fees and their halves and quarters are exact.
GROWTH = (Q128 >> 20, Q128 >> 30)


def sample(tick, steps_in_range, liquidity=ACTIVE):
    """A pool sample, at or above the range, whose range earned that many intervals so far."""
    inside = tuple(value * steps_in_range for value in GROWTH)
    # Inside the range both boundary ticks hold no outside growth, so the growth inside
    # is the global growth. Above it, the upper tick's outside growth is what the range
    # earned before the price left, and later global growth accrues elsewhere.
    above = tick >= UPPER
    kept = inside if above else (0, 0)
    return SimpleNamespace(
        pool=SimpleNamespace(source=POOL, tick=tick, liquidity=liquidity),
        ticks=(
            SimpleNamespace(index=LOWER, gross=1, outside0=0, outside1=0),
            SimpleNamespace(index=UPPER, gross=1, outside0=kept[0], outside1=kept[1]),
        ),
        global_growth=tuple(value + GROWTH[0] for value in inside) if above else inside,
    )


class Target(SimpleNamespace):
    """A stand-in target usable as a book key, as the frozen models are."""

    __hash__ = object.__hash__


def fixed_target():
    return Target(
        pool=POOL,
        band=None,
        range=SimpleNamespace(lower=LOWER, upper=UPPER),
        assets=("eip155:1/erc20:0xaa", "eip155:1/erc20:0xbb"),
    )


def book(target, samples):
    times = tuple(START + SECONDS * index for index in range(len(samples)))
    rows = tuple(SimpleNamespace(payload=payload) for payload in samples)
    return SimpleNamespace(timelines={target: SimpleNamespace(times=times, rows=rows)})


def action(target, at, liquidity, *, success=True):
    position = None if liquidity is None else SimpleNamespace(liquidity=liquidity, range=None)
    result = SimpleNamespace(
        success=success,
        intent=SimpleNamespace(target=target),
        effective_at=datetime.fromtimestamp(at, UTC),
        after=SimpleNamespace(position=position),
    )
    return SimpleNamespace(transition=SimpleNamespace(result=result))


def in_range_run(liquidity, steps=5):
    """One position opened on the first sample and held, in range, over `steps` intervals."""
    target = fixed_target()
    samples = tuple(sample(0, index) for index in range(steps + 1))
    state = SimpleNamespace(
        completed=(action(target, START + 1, liquidity),), time=START + SECONDS * steps
    )
    return state, book(target, samples), target


def credited(liquidity, steps):
    return tuple(value * steps * liquidity // Q128 for value in GROWTH)


def test_a_position_as_large_as_the_active_liquidity_keeps_half_its_credited_fees():
    state, prepared, target = in_range_run(ACTIVE)
    result = estimate(state, prepared, target)
    assert result["measurement_status"] == "estimated" and result["method"] == METHOD
    assert result["interval_fees"] == credited(ACTIVE, 5)
    assert result["diluted_fees"] == tuple(value // 2 for value in credited(ACTIVE, 5))
    assert result["max_pool_liquidity_share"] == 0.5
    assert (result["fee_intervals"], result["fee_intervals_without_active_liquidity"]) == (5, 0)


def test_a_tiny_position_keeps_nearly_all_its_credited_fees():
    liquidity = ACTIVE >> 20
    state, prepared, target = in_range_run(liquidity)
    result = estimate(state, prepared, target)
    whole, kept = result["interval_fees"][0], result["diluted_fees"][0]
    assert whole == credited(liquidity, 5)[0] > 0
    # About one part in a million of dilution, and at most one base unit of rounding
    # per interval.
    assert whole * (1 - Fraction(1, 2**20)) - 5 <= kept < whole
    assert result["max_pool_liquidity_share"] == pytest.approx(2**-20, rel=1e-3)


@pytest.mark.parametrize(
    ("liquidity", "kept_share"),
    [(ACTIVE // 9, Fraction(9, 10)), (ACTIVE * 3, Fraction(1, 4)), (ACTIVE * 99, Fraction(1, 100))],
)
def test_the_kept_share_is_active_over_active_plus_own(liquidity, kept_share):
    state, prepared, target = in_range_run(liquidity)
    result = estimate(state, prepared, target)
    whole, kept = result["interval_fees"][0], result["diluted_fees"][0]
    assert abs(kept - whole * kept_share) <= 5
    assert result["max_pool_liquidity_share"] == pytest.approx(float(1 - kept_share), rel=1e-4)


def test_intervals_out_of_range_add_nothing():
    target = fixed_target()
    # In range for two intervals, then above the range for three: growth inside stops.
    samples = (sample(0, 0), sample(0, 1), sample(0, 2), *(sample(UPPER + 10, 2),) * 3)
    state = SimpleNamespace(completed=(action(target, START, ACTIVE),), time=START + 5 * SECONDS)
    result = estimate(state, book(target, samples), target)
    assert result["interval_fees"] == credited(ACTIVE, 2)
    assert result["diluted_fees"] == tuple(value // 2 for value in credited(ACTIVE, 2))
    assert result["fee_intervals"] == 2


def test_each_interval_uses_its_own_active_liquidity_and_the_largest_share_is_reported():
    target = fixed_target()
    samples = (sample(0, 0, ACTIVE), sample(0, 1, ACTIVE * 3), sample(0, 2, ACTIVE))
    state = SimpleNamespace(completed=(action(target, START, ACTIVE),), time=START + 2 * SECONDS)
    result = estimate(state, book(target, samples), target)
    one = credited(ACTIVE, 1)[0]
    assert result["diluted_fees"][0] == one // 2 + one * 3 // 4
    assert result["max_pool_liquidity_share"] == 0.5


def test_fees_earned_between_two_samples_outside_the_range_stay_undiluted_and_are_counted():
    target = fixed_target()
    # The price crossed the range between two samples that both sit above it.
    samples = (sample(UPPER + 10, 0), sample(UPPER + 10, 1))
    state = SimpleNamespace(completed=(action(target, START, ACTIVE),), time=START + SECONDS)
    result = estimate(state, book(target, samples), target)
    assert result["diluted_fees"] == result["interval_fees"] == credited(ACTIVE, 1)
    assert result["fee_intervals_without_active_liquidity"] == 1
    assert result["max_pool_liquidity_share"] == 0.0


def test_an_interval_entered_from_outside_uses_the_sample_inside_the_range():
    selected = SimpleNamespace(lower=LOWER, upper=UPPER)
    outside, inside = sample(UPPER + 10, 0, 7), sample(0, 1, 11)
    assert active_liquidity(selected, outside, inside) == 11
    assert active_liquidity(selected, inside, outside) == 11
    assert active_liquidity(selected, outside, outside) is None
    # The upper tick is outside the range, as in the pool's own accounting.
    assert active_liquidity(selected, sample(UPPER, 0, 5), sample(UPPER - 1, 0, 6)) == 6


def test_positions_are_summed_across_liquidity_changes_and_reopening():
    target = fixed_target()
    samples = tuple(sample(0, index) for index in range(7))
    other = Target(pool="another")
    state = SimpleNamespace(
        completed=(
            action(target, START, ACTIVE),  # mint
            action(other, START + 30, ACTIVE * 50),  # another allocation is ignored
            action(target, START + 2 * SECONDS, ACTIVE * 3),  # increase
            action(target, START + 3 * SECONDS, None, success=False),  # a failure changes nothing
            action(target, START + 4 * SECONDS, None),  # close
            action(target, START + 5 * SECONDS, ACTIVE),  # reopen
        ),
        time=START + 6 * SECONDS,
    )
    spans = [(p.liquidity, start - START, end - START) for p, start, end in holdings(state, target)]
    assert spans == [(ACTIVE, 0, 120), (ACTIVE * 3, 120, 240), (ACTIVE, 300, 360)]
    result = estimate(state, book(target, samples), target)
    one = credited(ACTIVE, 1)[0]
    assert result["interval_fees"][0] == 2 * one + 2 * 3 * one + one
    assert result["diluted_fees"][0] == 2 * (one // 2) + 2 * (3 * one // 4) + one // 2
    assert result["max_pool_liquidity_share"] == 0.75
    assert result["fee_intervals"] == 5


def test_no_position_estimates_no_fees():
    target = fixed_target()
    state = SimpleNamespace(completed=(), time=START + SECONDS)
    result = estimate(state, book(target, (sample(0, 0), sample(0, 1))), target)
    assert result["interval_fees"] == result["diluted_fees"] == (0, 0)
    assert result["max_pool_liquidity_share"] == 0.0 and result["fee_intervals"] == 0


def test_an_unmeasurable_range_reports_the_estimate_as_unavailable():
    target = fixed_target()
    broken = sample(0, 1)
    broken.ticks = broken.ticks[:1]
    state = SimpleNamespace(completed=(action(target, START, ACTIVE),), time=START + SECONDS)
    result = estimate(state, book(target, (sample(0, 0), broken)), target)
    assert result == {
        "measurement_status": "unavailable",
        "reason": "fixed historical LP growth requires both initialized boundary ticks",
    }


def test_a_band_position_uses_the_credited_lower_bracket_of_its_own_range():
    selected = SimpleNamespace(lower=-120, upper=120)
    target = Target(pool=POOL, band=object(), range=None)
    position = SimpleNamespace(liquidity=ACTIVE, range=selected)
    growth = [(0, 0), GROWTH, tuple(2 * value for value in GROWTH)]
    calls = []

    class Bands:
        def at(self, index, lower, upper):
            calls.append((index, lower, upper))
            return SimpleNamespace(lower=growth[index], upper=(Q128, Q128))

    prepared = book(target, (sample(0, 0), sample(0, 0), sample(0, 0)))
    prepared.bands = {target: Bands()}
    measured = list(intervals(prepared, target, position, START, START + 2 * SECONDS))
    assert measured == [(credited(ACTIVE, 1), ACTIVE)] * 2
    assert calls == [(0, -120, 120), (1, -120, 120), (2, -120, 120)]


def test_dilution_totals_skip_intervals_without_fees():
    credited_fees, kept, share, earning, unmeasured = diluted(
        100, [((0, 0), 300), ((40, 8), 300), ((10, 0), None)]
    )
    assert (credited_fees, kept) == ((50, 8), (40, 6))
    assert (share, earning, unmeasured) == (Fraction(1, 4), 2, 1)


def test_position_report_labels_the_estimate_and_leaves_credited_fees_alone():
    state, prepared, target = in_range_run(ACTIVE)
    block = reporting.dilution_report(state, SimpleNamespace(book=prepared), target)
    assert block["measurement_status"] == "estimated"
    assert block["diluted_fees"] == [
        {"asset": asset, "amount": str(amount // 2), "unit": "base_units"}
        for asset, amount in zip(target.assets, credited(ACTIVE, 5), strict=True)
    ]
    assert [row["amount"] for row in block["interval_fees"]] == [
        str(amount) for amount in credited(ACTIVE, 5)
    ]
    broken = SimpleNamespace(book=book(target, (sample(0, 0),) * 6))
    broken.book.timelines[target].rows[3].payload = SimpleNamespace(
        pool=SimpleNamespace(source="another"), ticks=(), global_growth=(0, 0)
    )
    assert reporting.dilution_report(state, broken, target)["measurement_status"] == "unavailable"


def test_report_assumptions_name_the_estimate_and_the_fill_timing():
    config = SimpleNamespace(
        allocations=(SimpleNamespace(name="a"),), target=lambda name: SimpleNamespace(band=None)
    )
    fixed = reporting.fee_assumptions(SimpleNamespace(config=config))
    assert (
        fixed[0] == "LP fees use fixed historical range growth without dilution or own-trade fees"
    )
    assert reporting.DILUTION in fixed and "estimate" in reporting.DILUTION
    config.target = lambda name: SimpleNamespace(band=object())
    assert reporting.DILUTION in reporting.fee_assumptions(SimpleNamespace(config=config))
    assert "fill_on_decision_sample" in reporting.TIMING
