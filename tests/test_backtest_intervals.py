"""Shared scheduling in the simulator and the real isolated strategy interface."""

import json
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
import yaml
from pydantic import ValidationError

from almanak.backtest.cli import create, resume
from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.types import UnixTime
from almanak.connectors.aave_v4.backtest_binding import Manifest
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.gateway.ethereum import DataError
from almanak.strategy_api import Memory, StrategyDecision
from almanak.triggers import DecisionSchedule, IntervalSelection, IntervalTrigger
from tests.backtest_support import iso
from tests.test_backtest_core import State, Swap, SwapModel, initial
from tests.test_backtest_process import archive as archive
from tests.test_backtest_process import run_cli


class TimedModel(SwapModel):
    def __init__(self):
        super().__init__()
        self.events = []
        self.reads = []

    def observe(self, state):
        self.reads.append(state.time)
        return super().observe(state)

    def decide(self, state, observations, trigger):
        self.events.append(trigger)
        return super().decide(state, observations, trigger)


@pytest.mark.parametrize("latency,expected", [(2, [100, 105, 110]), (12, [100, 112, 117])])
def test_simulator_matches_shared_deadlines_after_actions(tmp_path, latency, expected):
    interval = IntervalTrigger(kind="interval", seconds=5)
    before = initial(latency_seconds=latency, end=UnixTime(expected[-1] + 1)).model_copy(
        update={"interval": IntervalSelection(configured=interval)}
    )
    journal = Journal(tmp_path / "state.sqlite", State)
    journal.create(before)
    model = TimedModel()
    result = Simulator(journal, model).run()
    assert [int(e.started_at.timestamp()) for e in model.events] == expected
    assert model.reads == expected
    schedule = DecisionSchedule()
    for time in expected:
        event = schedule.event(datetime.fromtimestamp(time, UTC))
        schedule = schedule.after(interval, event)
    assert result.decision_schedule == schedule
    assert result.evaluations == len(expected)
    assert result.completed[0].pending.trigger == model.events[0]


class UnchangedModel(TimedModel):
    def observe(self, state):
        self.reads.append(state.time)
        return "unchanged"

    def decide(self, state, observations, trigger):
        self.events.append(trigger)
        assert observations == "unchanged"
        return StrategyDecision[Swap](
            memory=Memory(values={"calls": state.memory.values.get("calls", 0) + 1})
        )


@pytest.mark.parametrize("stage,committed", [("decision", False), ("decision_commit", True)])
def test_no_action_commit_recovers_without_skipping_memory(tmp_path, stage, committed):
    journal = Journal(tmp_path / "state.sqlite", State)
    journal.create(initial())
    model = UnchangedModel()

    def crash(current):
        if current == stage:
            raise RuntimeError("lost process or acknowledgement")

    with pytest.raises(RuntimeError, match="acknowledgement"):
        Simulator(journal, model, fault=crash).run()
    retained = journal.load()
    assert bool(retained.memory.values) == committed
    assert (retained.decision_schedule.last_evaluation is not None) == committed
    result = Simulator(journal, model).run()
    assert result.memory.values == {"calls": 3}
    assert result.evaluations == 3


@pytest.mark.parametrize("override", [True, 0, 5, 4, 5.5, "60"])
def test_author_refuses_invalid_backtest_override(override):
    from tests.backtest_support import ROOT

    raw = json.loads((ROOT / "tests/fixtures/author/aave_v4_weth_supply/config.json").read_text())
    with pytest.raises(ValidationError):
        IntervalSelection(
            configured=ForkConfig.model_validate_json(json.dumps(raw)).decision_trigger,
            override_seconds=override,
        )


def arguments(config, operator, env_file, directory):
    from tests.backtest_support import ROOT

    return SimpleNamespace(
        strategy=ROOT / "strategies/aave_v4_weth_supply/strategy.py",
        config=config,
        backtest_config=config.with_name("backtest.yaml"),
        gateway_config=operator,
        env_file=env_file,
        output=directory,
    )


def configure(config, data, override):
    config = config.with_name("backtest.yaml")
    raw = yaml.safe_load(config.read_text())
    schedule = raw["window"]
    schedule["end"] = iso(data.start + 301)
    raw["execution"].pop("decision_interval", None)
    if override is not None:
        raw["execution"]["decision_interval"] = f"{override}s"
    config.write_text(yaml.safe_dump(raw))


@pytest.mark.parametrize("override,fidelity", [(None, "configured"), (60, "reduced")])
def test_backtest_settings_survive_resume_and_offline_reproduction(
    archive, tmp_path, override, fidelity
):
    data, config, operator, env_file = archive
    configure(config, data, override)
    directory = tmp_path / "run"
    report = create(arguments(config, operator, env_file, directory))
    assert report["closed"] and report["completed_steps"] == 12
    assert report["decision_interval"]["cadence_fidelity"] == fidelity
    manifest = Manifest.model_validate_json((directory / "manifest.json").read_text())
    assert manifest.config.decision_trigger.seconds == (override or 5)
    assert manifest.interval.configured.seconds == 5
    assert manifest.cadence_fidelity == fidelity
    assert_continuation(directory)
    provider_calls = len(data.calls)
    config.write_text("{}")
    resumed = resume(SimpleNamespace(run=directory, env_file=None))
    assert resumed["decision_interval"] == report["decision_interval"]
    reproduced = run_cli(["backtest", "reproduce", "--run", str(directory)])
    assert reproduced.returncode == 0, reproduced.stderr
    assert len(data.calls) == provider_calls


def assert_continuation(directory):
    from almanak.connectors.aave_v4.backtest import AaveState

    state = Journal(directory / "state.sqlite", AaveState).load()
    approval, supply = state.completed[:2]
    assert not approval.transition.complete and supply.transition.complete
    assert approval.pending.decision_identity == supply.pending.decision_identity
    assert approval.pending.trigger == supply.pending.trigger
    assert approval.pending.decision_time == supply.pending.decision_time
    assert supply.pending.eligible_at > approval.pending.eligible_at
    assert state.continuation is None


def test_reduced_cadence_cannot_make_future_history_available(archive, tmp_path):
    data, config, operator, env_file = archive
    configure(config, data, 60)
    config = config.with_name("backtest.yaml")
    raw = yaml.safe_load(config.read_text())
    raw["window"]["end"] = iso(data.start + 20000)
    config.write_text(yaml.safe_dump(raw))
    directory = tmp_path / "run"
    with pytest.raises(DataError, match="exceeds pinned history"):
        create(arguments(config.with_name("config.yaml"), operator, env_file, directory))
    assert not (directory / "state.sqlite").exists()
    assert yaml.safe_load(config.read_text())["window"]["end"] == iso(data.start + 20000)
