"""Shared journal and sandbox proofs with a deliberately frozen captured pool path."""

from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.configuration.yaml import load
from almanak.connectors.uniswap_v3.economic_binding import UniswapEconomic
from almanak.connectors.uniswap_v3.economic_model import EconomicModel
from almanak.connectors.uniswap_v3.economic_state import State
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers
from scripts.legacy_configuration.author import normalize
from scripts.legacy_configuration.backtest import read_backtest
from tests.legacy_references import harness_directory
from tests.test_uniswap_v3_economic_simulation import setup

ROOT = Path(__file__).parents[1]


@pytest.fixture
def runner(monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("synthetic shared-runner proof must remain offline")

    monkeypatch.setattr(httpx.Client, "send", forbid)

    def build(path, index, style):
        chain_name = ("ethereum", "base")[index]
        directory = ROOT / f"strategies/uniswap_v3_{chain_name}_{style}"
        frozen = harness_directory(directory.name)
        wire = load(frozen / "config.yaml").value
        wire.pop("execution")
        wire["strategy"]["allocations"]["position"]["hold_seconds"] = "60s"
        author = normalize(wire)
        settings = read_backtest(frozen / "backtest-economic.yaml", author)
        _, record, _ = setup(index)
        at = record.available_at
        settings = settings.model_copy(
            update={
                "schedule": settings.schedule.model_copy(
                    update={
                        "start": at,
                        "end": at + 100,
                        "cleanup_seconds": 100,
                        "latency_seconds": 1,
                    }
                )
            }
        )
        interval = IntervalSelection(configured=author.decision_trigger)
        config = UniswapEconomic.parameters(author, settings, interval)
        strategy = StrategyDefinition(
            filename="strategy.py", source=(directory / "strategy.py").read_text()
        )
        book = SimpleNamespace(at=lambda target, time: record, baselines={})
        model = EconomicModel(config, settings, strategy, book)
        journal = Journal(path, State)
        journal.create(UniswapEconomic.initial(settings, config, interval))
        return journal, model

    return build


def assert_roundtrip(result, model, style, *, close_intent="close_liquidity"):
    assert all(item.transition.result.success for item in result.completed)
    kinds = [item.pending.intent.kind for item in result.completed]
    expected = ["swap_exact_input"]
    if style == "lp":
        expected += [
            "open_liquidity",
            "increase_liquidity",
            "decrease_liquidity",
            "collect_liquidity_fees",
        ]
    if style == "lp":
        expected.append(close_intent)
    expected.append("exit_exchange")
    assert kinds == expected
    assert result.delivered == len(expected)
    report = UniswapEconomic.report(result, model)
    assert report["closed"] and report["missing_inputs"] == []
    assert result.portfolio.positions[0].payload.owned.position is None


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize("style", ["trading", "lp"])
def test_reusable_strategy_roundtrip_through_shared_simulator(runner, tmp_path, index, style):
    journal, model = runner(tmp_path / "state.sqlite", index, style)
    with preloaded_workers():
        result = Simulator(journal, model).run()
    assert_roundtrip(result, model, style)


@pytest.mark.parametrize("stage", ["pending_commit", "effects_commit", "delivery_commit"])
def test_reference_recovers_committed_work_without_duplicate_economic_effects(
    runner, tmp_path, stage
):
    journal, model = runner(tmp_path / "state.sqlite", 0, "lp")

    def crash(at):
        if at == stage:
            raise RuntimeError("interrupted")

    with preloaded_workers():
        with pytest.raises(RuntimeError, match="interrupted"):
            Simulator(journal, model, fault=crash).run()
        result = Simulator(Journal(journal.path, State), model).run()
    assert_roundtrip(result, model, "lp")
    control, uninterrupted_model = runner(tmp_path / "uninterrupted.sqlite", 0, "lp")
    with preloaded_workers():
        uninterrupted = Simulator(control, uninterrupted_model).run()
    assert result.portfolio == uninterrupted.portfolio
    assert result.memory == uninterrupted.memory
