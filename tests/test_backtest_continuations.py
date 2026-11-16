"""Connector continuation clocks preserve initial eligibility and strategy scheduling."""

import pytest

from almanak.backtest.engine import Journal, Simulator, Transition
from almanak.backtest.types import UnixTime
from almanak.effects import Effects
from tests.test_backtest_core import INPUT, OUTPUT, Empty, Receipt, State, SwapModel, initial


class PublishedSettlement(SwapModel):
    def continuation_at(self, state, pending):
        return UnixTime(state.time + 1)

    def execute(self, state, pending):
        if not state.completed:
            return Transition[Empty, Receipt](
                effects=Effects[Empty](), result=Receipt(acquired=0), complete=False
            )
        if pending.eligible_at in (104, 106):
            return super().execute(state, pending)
        return Transition[Empty, Receipt](
            effects=Effects[Empty](), result=Receipt(acquired=0), complete=False
        )


def test_initial_latency_cannot_skip_an_intervening_eligible_publication(tmp_path):
    journal = Journal(tmp_path / "run.sqlite", State)
    journal.create(initial(latency_seconds=5))
    result = Simulator(journal, PublishedSettlement()).run()
    assert [value.pending.eligible_at for value in result.completed] == [105, 106]
    assert [value.transition.result.acquired for value in result.completed] == [0, 6]
    assert result.portfolio.balance(INPUT) == 7
    assert result.portfolio.balance(OUTPUT) == 6
    assert result.evaluations == 3
    assert result.interval.configured.seconds == 10
    assert result.decision_schedule.last_evaluation.started_at.timestamp() == 120


def test_continuation_clock_survives_replacement_after_acceptance_commit(tmp_path):
    baseline = Journal(tmp_path / "baseline.sqlite", State)
    baseline.create(initial(latency_seconds=5))
    expected = Simulator(baseline, PublishedSettlement()).run()
    interrupted = Journal(tmp_path / "interrupted.sqlite", State)
    interrupted.create(initial(latency_seconds=5))

    def fault(stage):
        if stage == "effects_commit":
            raise RuntimeError("simulator lost")

    with pytest.raises(RuntimeError, match="simulator lost"):
        Simulator(interrupted, PublishedSettlement(), fault=fault).run()
    assert interrupted.load().portfolio.balance(INPUT) == 10
    assert Simulator(Journal(interrupted.path, State), PublishedSettlement()).run() == expected


@pytest.mark.parametrize("gap", [0, -1])
def test_continuation_clock_must_advance_before_any_effect(tmp_path, gap):
    class InvalidTiming(PublishedSettlement):
        def continuation_at(self, state, pending):
            return UnixTime(state.time + gap)

    journal = Journal(tmp_path / "invalid.sqlite", State)
    journal.create(initial(latency_seconds=5))
    with pytest.raises(ValueError, match="must follow"):
        Simulator(journal, InvalidTiming()).run()
    assert journal.load().portfolio.balance(INPUT) == 10
