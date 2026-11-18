"""Synthetic sampled paths test rounding and visibility, not historical fidelity."""

import json
from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import InputUnavailable, Timeline
from almanak.connectors.compound_v3.domain import MarketObservation
from almanak.connectors.compound_v3.economic_data import (
    RateCurve,
    RateRecord,
    RiskSample,
    key,
    requirement,
)
from tests.test_compound_v3_reader import recorded


def market(network="mainnet"):
    _, data = recorded(network, False)
    return MarketObservation.model_validate_json(json.dumps(data["market"]))


def timeline():
    basis = market()
    start = basis.block.timestamp
    settings = SimpleNamespace(
        schedule=SimpleNamespace(start=start, deadline=start + 120),
        data=SimpleNamespace(sample_seconds=60, max_age_seconds=90),
    )
    needed = requirement(basis.source, settings, "accounting")
    rows = []
    for index in range(3):
        timestamp = start + index * 60
        block = basis.block.model_copy(update={"timestamp": timestamp})
        # The later observed indices deliberately jump. A modeled path must integrate
        # sampled rates, without resetting hypothetical accounting to those indices.
        accounting = basis.accounting.model_copy(
            update={
                "supply_index": 10**15 * (index + 1),
                "borrow_index": 10**15 * (index + 1),
                "last_accrual_time": timestamp,
            }
        )
        source = basis.model_copy(
            update={
                "block": block,
                "accounting": accounting,
                "base_price": basis.base_price.model_copy(update={"block": block}),
                "collaterals": tuple(
                    c.model_copy(update={"price": c.price.model_copy(update={"block": block})})
                    for c in basis.collaterals
                ),
                "supply_curve": basis.supply_curve.model_copy(
                    update={
                        "base_per_second": (index + 1) * 10**12,
                        "slope_low_per_second": 0,
                        "slope_high_per_second": 0,
                    }
                ),
                "borrow_curve": basis.borrow_curve.model_copy(
                    update={
                        "base_per_second": (index + 1) * 2 * 10**12,
                        "slope_low_per_second": 0,
                        "slope_high_per_second": 0,
                    }
                ),
            }
        )
        rows.append(
            RateRecord(
                key=needed.key,
                unit=needed.unit,
                source_time=timestamp,
                available_at=timestamp,
                retrieved_at=float(timestamp),
                evidence=("a" * 64,),
                reference=block,
                payload=source,
            )
        )
    return Timeline(tuple(rows), needed)


def test_sampled_indices_integrate_integer_rates_without_reset_or_decision_feedback():
    records = timeline()
    curve = RateCurve(records)
    start = records.times[0]
    assert curve.at(start + 60) == (1000060000000000, 1000120000000000)
    assert curve.at(start + 120) == (1000180007200000, 1000360028800000)
    expected = curve.at(start + 89)
    for time in range(start, start + 90):
        curve.at(time)
    assert curve.at(start + 89) == expected
    assert records.rows[1].payload.accounting.supply_index == 2 * 10**15


def test_accounting_requires_every_boundary_and_pinned_source_identity():
    records = timeline()
    with pytest.raises(ValueError, match="fixed sample"):
        RateCurve(Timeline((records.rows[0], records.rows[2]), records.requirement))
    corrupted = records.rows[1].model_copy(update={"reference": records.rows[0].reference})
    with pytest.raises(ValueError, match="pinned historical"):
        RateCurve(Timeline((records.rows[0], corrupted, records.rows[2]), records.requirement))
    other = market("base").source
    corrupted = records.rows[1].model_copy(
        update={"payload": records.rows[1].payload.model_copy(update={"source": other})}
    )
    with pytest.raises(ValueError, match="mixes configured"):
        RateCurve(Timeline((records.rows[0], corrupted, records.rows[2]), records.requirement))


def test_curve_refuses_uncovered_and_stale_inputs():
    records = timeline()
    curve = RateCurve(records)
    with pytest.raises(InputUnavailable, match="outside prepared"):
        curve.at(records.times[-1] + 1)
    sparse = Timeline(
        (records.rows[0],),
        records.requirement.model_copy(
            update={"end": records.times[0] + 59, "max_age_seconds": 30}
        ),
    )
    with pytest.raises(InputUnavailable, match="source observation age"):
        RateCurve(sparse).at(records.times[0] + 31)


@pytest.mark.parametrize("network", ["mainnet", "base"])
def test_whole_market_risk_requires_adjacent_timestamp_groups(network):
    source = market(network)
    block = source.block
    following = block.model_copy(
        update={"number": block.number + 1, "timestamp": block.timestamp + 1}
    )
    value = RiskSample(market=source, block_group=(block,), following_block=following)
    assert RiskSample.model_validate_json(value.model_dump_json()) == value
    for changed in (following.model_copy(update={"number": block.number + 2}), block):
        with pytest.raises(ValueError, match="following time"):
            RiskSample(market=source, block_group=(block,), following_block=changed)
    with pytest.raises(ValueError, match="incomplete or unordered"):
        RiskSample(market=source, block_group=(block, block), following_block=following)
    assert key(source.source, "risk").chain == source.block.chain
