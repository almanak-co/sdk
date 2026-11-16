"""Missing prepared risk history preserves chain-local obligations and ordering."""

import json
from datetime import UTC, datetime

import pytest

from almanak.backtest.config import EconomicBacktestConfig
from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.economic_availability import availability, unavailable
from almanak.backtest.economic_data import reserve_key
from almanak.backtest.economic_events import EconomicDriver
from almanak.backtest.economic_model import initial_state, parameters
from almanak.backtest.economic_state import EconomicState
from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.aave_v4.economic_binding import result_report
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.models import Asset
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers
from tests.test_economic_math import market
from tests.test_economic_math import reserve as reserve
from tests.uniswap_support import ROOT


class GapBook:
    def __init__(self, config, reserve, start, gap):
        self.gap = gap
        self.markets = {}
        for position in config.positions:
            source = position.reserve
            debt = position in config.debts
            metadata = reserve.model_copy(
                update={
                    "source": source,
                    "block": reserve.block.model_copy(
                        update={"chain": source.chain, "timestamp": start}
                    ),
                    "asset": Asset(
                        chain=source.chain, identifier=source.asset, decimals=6 if debt else 18
                    ),
                }
            )
            self.markets[source] = market(metadata, price=(1 if debt else 3000) * 10**8)

    def at(self, reserve, time):
        # A later fresh sample cannot prove that liquidation was absent in the gap.
        if self.gap <= time < self.gap + 2 and str(reserve.chain.chain_id) == "eip155:43114":
            raise InputUnavailable(reserve_key(reserve, "price"), time, "missing risk observation")
        return self.markets[reserve]

    def evidence(self, reserve, time):
        self.at(reserve, time)
        return (self.markets[reserve].risk.reserve.block.timestamp,), ("a" * 64,)

    def next(self, time):
        return next((t for t in (self.gap, self.gap + 2) if t > time), None)


def setup(tmp_path, reserve, gap_seconds=21):
    directory = ROOT / "tests/fixtures/author/aave_v4_multichain_borrowing"
    author = ForkConfig.model_validate_json((directory / "config.json").read_text())
    raw = json.loads((directory / "backtest-economic.json").read_text())
    start = raw["schedule"]["start"]
    raw["schedule"].update(
        end=start + 31,
        cleanup_seconds=15,
        latency_seconds=1,
        decision_interval_override_seconds=None,
    )
    settings = EconomicBacktestConfig.model_validate_json(json.dumps(raw))
    interval = IntervalSelection(configured=author.decision_trigger)
    config = parameters(settings, author, interval)
    initial = initial_state(settings, config, interval)
    journal = Journal(tmp_path / "state.sqlite", EconomicState)
    journal.create(initial)
    model = EconomicDriver(
        config,
        settings,
        StrategyDefinition.read(ROOT / "strategies/aave_v4_multichain_borrowing/strategy.py"),
        GapBook(config, reserve, start, start + gap_seconds),
    )
    return journal, model


def finish_incomplete(journal, model):
    with pytest.raises(ValueError, match="cleanup deadline.*unresolved obligations"):
        Simulator(journal, model).run()
    return journal.load()


def test_unavailable_chain_keeps_debt_and_allows_independent_cleanup(tmp_path, reserve):
    journal, model = setup(tmp_path, reserve)
    with preloaded_workers():
        state = finish_incomplete(journal, model)
    report = result_report(state, model)
    by_name = {p["target"]["allocation"]: p for p in report["positions"]}
    assert not report["closed"] and not report["accounting_complete"]
    assert by_name["ethereum_debt"]["total_debt"] == "0"
    assert by_name["ethereum_collateral"]["shares"] == 0
    assert by_name["avalanche_debt"]["drawn_shares"] > 0
    assert by_name["avalanche_debt"]["total_debt"] is None
    assert by_name["avalanche_collateral"]["shares"] > 0
    assert by_name["avalanche_collateral"]["risk"] is None
    assert len(unavailable(state)) == 1
    assert state.events[0].time == journal.load().schedule.start + 21
    assert state.events[0].transition.result.kind == "input_unavailable"
    assert state.events_delivered == 1
    assert [a.status for a in availability(state, model.observe(state))] == ["ready", "unavailable"]
    assert {str(o.target.fund.chain.chain_id) for o in model.observe(state)} == {"eip155:1"}
    assert [c.pending.intent.kind for c in state.completed] == [
        "supply",
        "supply",
        "borrow",
        "borrow",
        "repay",
        "withdraw",
    ]


@pytest.mark.parametrize("stage", ["event_commit", "event_delivery", "event_delivery_commit"])
def test_missing_history_survives_coordinator_and_book_replacement(tmp_path, reserve, stage):
    journal, model = setup(tmp_path, reserve)
    initial = journal.load()
    expected_journal = Journal(tmp_path / "expected.sqlite", EconomicState)
    expected_journal.create(initial)

    def crash(current):
        if current == stage and journal.load().events:
            raise RuntimeError("lost coordinator after missing history")

    with preloaded_workers():
        expected = finish_incomplete(expected_journal, model)
        with pytest.raises(RuntimeError, match="lost coordinator"):
            Simulator(journal, model, fault=crash).run()
        replacement = EconomicDriver(
            model.config,
            model.settings,
            model.strategy,
            GapBook(model.config, reserve, initial.time, initial.time + 21),
        )
        actual = finish_incomplete(Journal(journal.path, EconomicState), replacement)
    assert actual == expected


def test_admitted_unavailable_action_records_known_no_fill(tmp_path, reserve):
    journal, model = setup(tmp_path, reserve, gap_seconds=6)
    with preloaded_workers():
        state = finish_incomplete(journal, model)
    rejected = [c for c in state.completed if not c.transition.result.success]
    assert len(rejected) == 1
    completed = rejected[0]
    assert completed.pending.intent.target.allocation == "avalanche_collateral"
    assert completed.pending.eligible_at == state.schedule.start + 6
    assert completed.transition.result.effective_at == datetime.fromtimestamp(
        completed.pending.eligible_at, UTC
    )
    assert not completed.transition.effects.assets and not completed.transition.effects.positions
    assert completed.transition.result.assets is None
    assert str(completed.transition.result.unavailable.key.chain.chain_id) == "eip155:43114"
    assert "avalanche_collateral" not in state.memory.values["supplied_at"]
    assert state.completed[-1].pending.intent.target.allocation == "ethereum_collateral"


def test_corrupt_market_input_is_fatal_and_cannot_be_hidden_as_unavailable(tmp_path, reserve):
    journal, model = setup(tmp_path, reserve)

    def corrupt(*args):
        raise ValueError("wrong chain or corrupt implementation evidence")

    model.book.at = corrupt
    with pytest.raises(ValueError, match="corrupt implementation"):
        model.observe(journal.load())


def test_timeline_exposes_first_stale_second_without_using_a_later_sample():
    from almanak.backtest.dataset import Timeline
    from tests.test_prepared_dataset import record, requirement

    required = requirement().model_copy(update={"max_age_seconds": 5})
    timeline = Timeline((record(100), record(110)), required)
    assert timeline.expiry(99) is None
    assert timeline.expiry(100) == 106
    assert timeline.at(105).source_time == 100
    with pytest.raises(InputUnavailable) as error:
        timeline.at(106)
    assert error.value.gap.key == required.key and error.value.gap.time == 106
    assert timeline.expiry(106) is None
    assert timeline.at(110).source_time == 110
    assert timeline.expiry(120) is None
    refreshed = Timeline((record(100), record(105), record(110)), required)
    assert refreshed.expiry(100) is None
