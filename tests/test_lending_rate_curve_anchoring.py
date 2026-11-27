"""The sampled accounting curve starts again from each sample's own on-chain indexes."""

from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import DataRequirement, InputUnavailable, Timeline
from almanak.connectors.aave_v3 import economic_data as aave_data
from almanak.connectors.aave_v3 import math as aave_math
from almanak.connectors.sparklend import economic_data as spark_data
from almanak.connectors.sparklend import math as spark_math
from tests.test_aave_v3_risk import position as aave_position
from tests.test_sparklend_risk import position as spark_position

RAY = 10**27
SAMPLE = 3600
CONNECTORS = {
    "aave_v3": SimpleNamespace(
        data=aave_data, math=aave_math, position=aave_position, unit="aave_v3_reserve_accounting"
    ),
    "sparklend": SimpleNamespace(
        data=spark_data,
        math=spark_math,
        position=spark_position,
        unit="sparklend_reserve_accounting",
    ),
}


@pytest.fixture(params=sorted(CONNECTORS))
def connector(request):
    return CONNECTORS[request.param]


def percent(value):
    return RAY * value // 100


def history(connector, intervals):
    """Samples of a reserve whose rates change at the stated seconds inside each interval.

    `intervals` holds, per sample interval, the `(seconds, supply_rate, borrow_rate)`
    segments the reserve went through. Each segment starts with a reserve update, as an
    interaction causes on chain: the stored indexes accrue at the rates in force and the
    new rates apply from that second. A sample is the stored reserve at its boundary.
    """
    math = connector.math
    reserve = connector.position(0).reserve
    start = reserve.block.timestamp
    supply, borrow, updated = RAY, RAY, start
    rates = intervals[0][0][1:]
    rows = []

    def sample(at):
        block = reserve.block.model_copy(update={"timestamp": at})
        payload = reserve.model_copy(
            update={
                "block": block,
                "oracle": reserve.oracle.model_copy(update={"block": block}),
                "last_update_timestamp": updated,
                "liquidity_index_ray": supply,
                "variable_borrow_index_ray": borrow,
                "liquidity_rate_ray": rates[0],
                "variable_borrow_rate_ray": rates[1],
            }
        )
        return connector.data.RateRecord(
            key=connector.data.key(reserve.source, "accounting"),
            unit=connector.unit,
            source_time=at,
            available_at=at,
            retrieved_at=float(at + 1000),
            evidence=("a" * 64,),
            reference=block,
            payload=payload,
        )

    at = start
    rows.append(sample(at))
    for segments in intervals:
        assert sum(seconds for seconds, _, _ in segments) == SAMPLE
        for seconds, supply_rate, borrow_rate in segments:
            elapsed = at - updated
            supply = math.ray_mul(supply, math.linear_interest(rates[0], elapsed))
            borrow = math.ray_mul(borrow, math.compounded_interest(rates[1], elapsed))
            updated, rates = at, (supply_rate, borrow_rate)
            at += seconds
        rows.append(sample(at))
    requirement = DataRequirement(
        key=rows[0].key,
        start=start,
        end=at,
        semantics="sampled",
        sample_seconds=SAMPLE,
        max_age_seconds=SAMPLE + 10,
        unit=connector.unit,
        visibility="end_of_block",
        payload_revision=connector.data.REVISION,
    )
    return Timeline(tuple(rows), requirement)


def on_chain(connector, row):
    return connector.math.normalized_indices(row.payload, int(row.available_at))


def first_anchor_only(connector, data):
    """The curve before this change: the first sample's indexes, then sampled rates only."""
    math = connector.math
    supply, borrow = on_chain(connector, data.rows[0])
    for prior in data.rows[:-1]:
        supply = math.ray_mul(
            supply, math.linear_interest(prior.payload.liquidity_rate_ray, SAMPLE)
        )
        borrow = math.ray_mul(
            borrow, math.compounded_interest(prior.payload.variable_borrow_rate_ray, SAMPLE)
        )
    return supply, borrow


RISING = [
    [(600, percent(4), percent(5)), (3000, percent(9), percent(11))],
    [(1800, percent(9), percent(11)), (1800, percent(12), percent(15))],
    [(3600, percent(12), percent(15))],
    [(900, percent(12), percent(15)), (2700, percent(20), percent(26))],
]
FALLING = [
    [(600, percent(20), percent(26)), (3000, percent(4), percent(5))],
    *([[(3600, percent(4), percent(5))]] * 5),
]


@pytest.mark.parametrize("intervals", [RISING, FALLING], ids=["rising", "falling"])
def test_curve_equals_the_sampled_on_chain_indexes_at_every_boundary(connector, intervals):
    data = history(connector, intervals)
    curve = connector.data.RateCurve(data)
    for row in data.rows:
        assert curve.at(row.available_at) == on_chain(connector, row)


@pytest.mark.parametrize("intervals", [RISING, FALLING], ids=["rising", "falling"])
def test_curve_integrates_forward_from_the_latest_sample_between_boundaries(connector, intervals):
    data = history(connector, intervals)
    curve = connector.data.RateCurve(data)
    for row in data.rows[:-1]:
        for elapsed in (1, 1234, SAMPLE - 1):
            inside = row.available_at + elapsed
            expected = connector.math.normalized_indices(row.payload, int(inside))
            assert curve.at(inside) == expected
            assert expected[0] > on_chain(connector, row)[0]
            assert expected[1] > on_chain(connector, row)[1]


def test_a_rate_change_inside_an_interval_no_longer_compounds_over_the_window(connector):
    data = history(connector, RISING)
    curve = connector.data.RateCurve(data)
    last = data.rows[-1]
    truth = on_chain(connector, last)
    before = first_anchor_only(connector, data)
    # Rates rose inside three of four intervals, so the left sample understates each.
    assert before[0] < truth[0] and before[1] < truth[1]
    assert curve.at(last.available_at) == truth


def test_curve_steps_down_to_the_on_chain_indexes_when_rates_fell_inside_an_interval(connector):
    data = history(connector, FALLING)
    curve = connector.data.RateCurve(data)
    first, second, *_ = data.rows
    boundary = second.available_at
    accrued = connector.math.normalized_indices(first.payload, int(boundary))
    read = on_chain(connector, second)
    # The first sample's high rates overshoot the indexes read at the next boundary.
    assert accrued[0] > read[0] and accrued[1] > read[1]
    # One second before the boundary the curve already exceeds what the chain reads at
    # it; at the boundary it equals the chain, so both indexes decrease across it.
    just_before = curve.at(boundary - 1)
    assert just_before[0] > read[0] and just_before[1] > read[1]
    assert curve.at(boundary) == read
    # After the step the curve accrues from the new anchor at the new, lower rates.
    assert curve.at(boundary + 600) == connector.math.normalized_indices(
        second.payload, int(boundary + 600)
    )
    # The excess is one interval's and is gone: every later boundary equals the chain, and
    # with unchanged rates inside the later intervals nothing steps down again.
    previous = curve.at(boundary)
    for at in range(boundary, data.requirement.end + 1, 60):
        current = curve.at(at)
        assert current[0] >= previous[0] and current[1] >= previous[1]
        previous = current
    assert curve.at(data.rows[-1].available_at) == on_chain(connector, data.rows[-1])


def test_a_step_down_shrinks_a_balance_and_a_debt_without_failing_their_valuation(connector):
    data = history(connector, FALLING)
    curve = connector.data.RateCurve(data)
    boundary = data.rows[1].available_at
    scaled = 10**24
    before, after = curve.at(boundary - 1), curve.at(boundary)
    supplied = [connector.math.supplied_assets(scaled, index) for index in (before[0], after[0])]
    debt = [connector.math.debt_assets(scaled, index) for index in (before[1], after[1])]
    assert 0 < supplied[1] < supplied[0]
    assert 0 < debt[1] < debt[0]
    # The step is the model's excess of that one interval, a few parts per million here.
    assert (supplied[0] - supplied[1]) * 10**6 < 20 * supplied[0]
    assert (debt[0] - debt[1]) * 10**6 < 25 * debt[0]


def test_no_earlier_sample_changes_the_curve_after_a_boundary(connector):
    data = history(connector, FALLING)
    first = data.rows[0]
    payload = first.payload.model_copy(
        update={"liquidity_rate_ray": percent(90), "variable_borrow_rate_ray": percent(95)}
    )
    other = first.model_copy(update={"payload": payload})
    changed = Timeline((other, *data.rows[1:]), data.requirement)
    curve, altered = connector.data.RateCurve(data), connector.data.RateCurve(changed)
    assert altered.at(first.available_at + 60) != curve.at(first.available_at + 60)
    for at in range(data.rows[1].available_at, data.requirement.end + 1, 300):
        assert altered.at(at) == curve.at(at)


def test_curve_is_a_function_of_time_alone(connector):
    data = history(connector, FALLING)
    curve = connector.data.RateCurve(data)
    at = data.rows[1].available_at + 17
    expected = curve.at(at)
    for second in range(data.requirement.end, data.requirement.start - 1, -300):
        curve.at(second)
    assert curve.at(at) == expected == connector.data.RateCurve(data).at(at)


def test_a_missing_boundary_and_a_time_outside_the_window_are_refused(connector):
    data = history(connector, RISING)
    with pytest.raises(ValueError, match="missing a fixed sample boundary"):
        connector.data.RateCurve(Timeline((data.rows[0], *data.rows[2:]), data.requirement))
    curve = connector.data.RateCurve(data)
    with pytest.raises(InputUnavailable):
        curve.at(data.requirement.start - 1)


def test_model_identifier_names_the_anchored_curve():
    assert aave_data.MODEL == "aave-v3-revision11-economic-indices-v2"
    assert spark_data.MODEL == "sparklend-revision4-economic-indices-v2"
