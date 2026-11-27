"""A modeled exchange step reports the age of its pool state; latency below sampling shows."""

from bisect import bisect_right
from types import SimpleNamespace

import pytest

from almanak.connectors import dex_fill_timing
from almanak.connectors.aerodrome import economic_state as aerodrome
from almanak.connectors.aerodrome_slipstream import economic_state as slipstream
from almanak.connectors.curve import authoring
from almanak.connectors.curve import economic_state as curve
from almanak.connectors.dex_economic_model import EconomicModel
from almanak.connectors.dex_fill_timing import OnDecisionSample, StateAge
from almanak.connectors.pancakeswap_v3 import economic_state as pancakeswap
from almanak.connectors.uniswap_v3 import economic_state as uniswap_v3
from almanak.models import Model
from tests.test_curve_economic_model import component, execute
from tests.test_curve_settlement import target as target  # noqa: F401

START = 1_800_000_000
SAMPLE_SECONDS = 60


class Book:
    """Samples every 60 seconds, each read from the block one second before its boundary."""

    def __init__(self, samples=10):
        self.times = tuple(START + SAMPLE_SECONDS * index for index in range(samples))
        self.rows = tuple(
            SimpleNamespace(available_at=at, source_time=at - 1, reference=("block", at - 1))
            for at in self.times
        )

    def at(self, target, time):
        return self.rows[bisect_right(self.times, time) - 1]


def pending(decision_time, latency):
    return SimpleNamespace(decision_time=decision_time, eligible_at=decision_time + latency)


def test_latency_below_the_sampling_interval_fills_on_the_decision_sample():
    decided = START + 2 * SAMPLE_SECONDS
    assert dex_fill_timing.measured(Book(), "pool", pending(decided, 5)) == {
        # The sample was read one second before the decision; five seconds of latency
        # later the step still executes on it.
        "fill_state_age_seconds": 6,
        "fill_on_decision_sample": True,
    }


def test_latency_of_one_sampling_interval_fills_on_the_next_sample():
    decided = START + 2 * SAMPLE_SECONDS
    assert dex_fill_timing.measured(Book(), "pool", pending(decided, SAMPLE_SECONDS)) == {
        "fill_state_age_seconds": 1,
        "fill_on_decision_sample": False,
    }


@pytest.mark.parametrize(
    ("offset", "latency", "on_decision_sample", "age"),
    [(0, 1, True, 2), (30, 29, True, 60), (30, 30, False, 1), (59, 1, False, 1), (0, 59, True, 60)],
)
def test_state_age_runs_from_the_sample_source_time_to_the_eligible_time(
    offset, latency, on_decision_sample, age
):
    timing = dex_fill_timing.measured(Book(), "pool", pending(START + 120 + offset, latency))
    assert timing == {"fill_state_age_seconds": age, "fill_on_decision_sample": on_decision_sample}


def completed(**fields):
    return SimpleNamespace(transition=SimpleNamespace(result=SimpleNamespace(**fields)))


def test_report_block_counts_steps_on_their_decision_sample():
    state = SimpleNamespace(
        schedule=SimpleNamespace(latency_seconds=5),
        completed=(
            completed(fill_state_age_seconds=6, fill_on_decision_sample=True),
            completed(fill_state_age_seconds=41, fill_on_decision_sample=True),
            completed(fill_state_age_seconds=1, fill_on_decision_sample=False),
            # A revocation, a settled exit and a failure record no state age.
            completed(fill_state_age_seconds=None, fill_on_decision_sample=None),
            completed(error="refused"),
        ),
    )
    block = dex_fill_timing.summary(state)
    assert block["latency_seconds"] == 5
    assert (block["steps"], block["steps_on_decision_sample"]) == (3, 2)
    assert block["max_fill_state_age_seconds"] == 41
    assert "decision sample" in block["interpretation"]


def test_report_block_states_no_age_before_any_step():
    state = SimpleNamespace(schedule=SimpleNamespace(latency_seconds=1), completed=())
    block = dex_fill_timing.summary(state)
    assert (block["steps"], block["max_fill_state_age_seconds"]) == (0, None)


def test_a_revocation_reads_no_pool_sample_and_reports_no_age():
    model = SimpleNamespace(book=Book())
    waiting = pending(START + 60, 5)
    assert EconomicModel.fill_timing(model, "pool", waiting, "revoke") == {}
    assert EconomicModel.fill_timing(model, "pool", waiting, "swap") == {
        "fill_state_age_seconds": 6,
        "fill_on_decision_sample": True,
    }


class Sample(Model):
    amount: int
    fill_state_age_seconds: StateAge = None
    fill_on_decision_sample: OnDecisionSample = None


def test_fields_are_additive_and_absent_from_results_retained_without_them():
    retained = Sample.model_validate_json('{"amount": 1}')
    assert retained.fill_state_age_seconds is None and retained.fill_on_decision_sample is None
    assert retained.model_dump(mode="json") == {"amount": 1}
    timed = Sample(amount=1, fill_state_age_seconds=0, fill_on_decision_sample=False)
    assert timed.model_dump(mode="json") == {
        "amount": 1,
        "fill_state_age_seconds": 0,
        "fill_on_decision_sample": False,
    }
    with pytest.raises(ValueError):
        Sample(amount=1, fill_state_age_seconds=-1)


@pytest.mark.parametrize("state_types", [uniswap_v3, pancakeswap, slipstream, aerodrome, curve])
def test_every_exchange_result_carries_the_optional_timing_fields(state_types):
    fields = state_types.EconomicResult.model_fields
    for name in ("fill_state_age_seconds", "fill_on_decision_sample"):
        assert name in fields and fields[name].default is None


def test_shared_execution_records_timing_without_changing_the_fill(target):  # noqa: F811
    model, state = component(target)
    allocation = model.config.allocations[0]
    observed = model.observe(state)[0]
    intent = authoring.swap(allocation, observed, observed.entry_quote)
    after, transition = execute(model, state, intent)
    result = transition.result
    assert result.success
    # The harness holds one sample, read at the window start, and one second of latency.
    assert (result.fill_state_age_seconds, result.fill_on_decision_sample) == (1, True)
    assert result.fill.amount_out == observed.entry_quote.amount_out
    dumped = result.model_dump(mode="json")
    assert dumped["fill_state_age_seconds"] == 1 and dumped["fill_on_decision_sample"] is True
    assert dex_fill_timing.summary(
        SimpleNamespace(
            schedule=model.settings.schedule, completed=(SimpleNamespace(transition=transition),)
        )
    ) == {
        "latency_seconds": 1,
        "steps": 1,
        "steps_on_decision_sample": 1,
        "max_fill_state_age_seconds": 1,
        "interpretation": dex_fill_timing.summary(after)["interpretation"],
    }
