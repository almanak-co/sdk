"""Sample boundaries control modeled accrual independently from strategy observations."""

import pytest

from almanak.backtest.dataset import DataRequirement, InputUnavailable, Timeline
from almanak.connectors.sparklend.economic_data import RateCurve, RateRecord, key
from almanak.connectors.sparklend.math import RAY, normalized_indices
from tests.test_sparklend_risk import position


def timeline():
    reserve = position(0).reserve
    start = reserve.block.timestamp
    requirement = DataRequirement(
        key=key(reserve.source, "accounting"),
        start=start,
        end=start + 180,
        semantics="sampled",
        sample_seconds=60,
        max_age_seconds=70,
        unit="sparklend_reserve_accounting",
        visibility="end_of_block",
        payload_revision="sparklend-revision4-prepared-reserves-v1",
    )
    records = []
    for i in range(4):
        at = start + 60 * i
        block = reserve.block.model_copy(update={"timestamp": at})
        payload = reserve.model_copy(
            update={
                "block": block,
                "oracle": reserve.oracle.model_copy(update={"block": block}),
                "last_update_timestamp": at,
                "liquidity_rate_ray": RAY // 20 + i,
                "variable_borrow_rate_ray": RAY // 10 + i,
            }
        )
        records.append(
            RateRecord(
                key=requirement.key,
                unit=requirement.unit,
                source_time=at,
                available_at=at,
                retrieved_at=float(at + 1000),
                evidence=("a" * 64,),
                reference=block,
                payload=payload,
            )
        )
    return Timeline(tuple(records), requirement)


def test_arbitrary_observation_cadence_cannot_change_the_sampled_accounting_path():
    data = timeline()
    curve = RateCurve(data)
    at = data.requirement.start
    expected = curve.at(at + 173)
    for second in range(174):
        curve.at(at + second)
    assert curve.at(at + 173) == expected
    assert RateCurve(data).at(at + 173) == expected
    first = data.rows[0]
    assert curve.at(at + 59) == normalized_indices(first.payload, at + 59)
    assert curve.at(at + 59)[0] < curve.at(at + 60)[0] < curve.at(at + 61)[0]
    assert curve.at(at + 59)[1] < curve.at(at + 60)[1] < curve.at(at + 61)[1]


def test_curve_cannot_bridge_a_missing_sample_or_use_expired_inputs():
    data = timeline()
    with pytest.raises(ValueError, match="missing"):
        RateCurve(Timeline((data.rows[0], *data.rows[2:]), data.requirement))
    with pytest.raises(InputUnavailable, match="outside"):
        RateCurve(data).at(data.requirement.start - 1)
    stale = data.requirement.model_copy(update={"max_age_seconds": 30})
    curve = RateCurve(Timeline(data.rows, stale))
    with pytest.raises(InputUnavailable, match="age"):
        curve.at(stale.start + 31)


@pytest.mark.parametrize("fault", ["source", "reference", "time", "unsampled"])
def test_curve_refuses_mixed_sources_or_unverifiable_accounting(fault):
    data = timeline()
    first = data.rows[0]
    requirement = data.requirement
    if fault == "source":
        first = first.model_copy(
            update={
                "payload": first.payload.model_copy(
                    update={
                        "source": first.payload.source.model_copy(
                            update={"pool": "0x" + "12" * 20}
                        ),
                    }
                )
            }
        )
    elif fault == "reference":
        first = first.model_copy(
            update={"reference": first.reference.model_copy(update={"number": 1})}
        )
    elif fault == "time":
        first = first.model_copy(update={"source_time": first.source_time - 1})
    else:
        requirement = requirement.model_copy(
            update={"semantics": "changes", "sample_seconds": None}
        )
    with pytest.raises(ValueError):
        RateCurve(Timeline((first, *data.rows[1:]), requirement))


@pytest.mark.parametrize("fault", ["stable", "unbacked", "isolated", "siloed"])
def test_prepared_rates_refuse_unmodeled_market_accounting(fault):
    data = timeline()
    first = data.rows[0]
    reserve = first.payload
    if fault in ("stable", "unbacked"):
        reserve = reserve.model_copy(
            update={"total_stable_debt" if fault == "stable" else "unbacked": 1}
        )
    else:
        bit = 212 if fault == "isolated" else 62
        reserve = reserve.model_copy(
            update={
                "configuration": reserve.configuration.model_copy(
                    update={"raw": reserve.configuration.raw | (1 << bit)}
                )
            }
        )
    with pytest.raises(ValueError, match="excludes"):
        RateCurve(
            Timeline(
                (first.model_copy(update={"payload": reserve}), *data.rows[1:]), data.requirement
            )
        )


def test_market_converts_only_valid_shared_clock_values():
    from dataclasses import replace

    from almanak.backtest.types import UnixTime
    from tests.test_sparklend_economic_actions import markets

    market = markets()[0]
    assert type(replace(market, time=UnixTime(market.time)).time) is int
    for invalid in (True, float(market.time), -1):
        with pytest.raises(ValueError):
            replace(market, time=invalid)


def test_risk_sample_requires_observed_absent_sentinel():
    from almanak.connectors.sparklend.economic_data import RiskSample
    from tests.test_sparklend_economic_actions import markets

    sample = markets()[0].sample.model_dump()
    sample.pop("oracle_sentinel")
    with pytest.raises(ValueError):
        RiskSample.model_validate(sample)
    sample["oracle_sentinel"] = "0x" + "12" * 20
    with pytest.raises(ValueError, match="observed absent sentinel"):
        RiskSample.model_validate(sample)
