"""Real captured pool timelines drive the reusable LP reference entirely offline."""

import gzip
import json

import httpx
import pytest

from almanak.backtest.engine import Journal, Schedule, Simulator
from almanak.backtest.gateway import GatewayConfig
from almanak.configuration.yaml import load
from almanak.connectors.uniswap_v3.economic_binding import UniswapEconomic
from almanak.connectors.uniswap_v3.economic_book import PoolBook
from almanak.connectors.uniswap_v3.economic_config import DataPolicy
from almanak.connectors.uniswap_v3.economic_model import EconomicModel
from almanak.connectors.uniswap_v3.economic_state import State
from almanak.connectors.uniswap_v3.models import Parameters
from almanak.models import SimulationAccount
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers
from scripts.legacy_configuration.author import normalize
from scripts.legacy_configuration.backtest import read_backtest
from tests.exchange_scope import without_exchange_budgets
from tests.legacy_references import harness_directory
from tests.test_uniswap_v3_economic_runner import assert_roundtrip
from tests.test_uniswap_v3_prepared_acquisition import ROOT, ROWS


def prepared_inputs(directory, index):
    row = ROWS[index]
    captured = Parameters.model_validate_json(json.dumps(without_exchange_budgets(row["config"])))
    reference = ROOT / f"strategies/uniswap_v3_{('ethereum', 'base')[index]}_lp"
    frozen = harness_directory(reference.name)
    wire = load(frozen / "config.yaml").value
    wire.pop("execution")
    wire["strategy"]["allocations"]["position"]["hold_seconds"] = "60s"
    author = normalize(wire)
    settings = read_backtest(frozen / "backtest-economic.yaml", author)
    history = GatewayConfig.model_validate_json(
        gzip.decompress((ROOT / row["files"]["history.json"]).read_bytes())
    )
    profile = SimulationAccount(fund=captured.accounts.bindings[0].profile.fund)
    settings = type(settings).model_validate(
        {
            **settings.model_dump(),
            "schedule": Schedule.model_validate_json(json.dumps(row["schedule"])),
            "data": DataPolicy.model_validate_json(json.dumps(row["data"])),
            "history": history.providers,
            "funds": (settings.funds[0].model_copy(update={"profile": profile}),),
        }
    )
    interval = IntervalSelection(configured=author.decision_trigger)
    config = UniswapEconomic.parameters(author, settings, interval)
    assert config.target("position") == captured.target("position")
    directory.mkdir()
    for name in ("data.sqlite", "dataset.json"):
        (directory / name).write_bytes(gzip.decompress((ROOT / row["files"][name]).read_bytes()))
    return config, settings, interval, StrategyDefinition.read(reference / "strategy.py")


@pytest.mark.parametrize("index", [0, 1])
def test_prepared_pool_samples_execute_reference_with_no_provider_calls(
    tmp_path, monkeypatch, index
):
    def forbid(*args, **kwargs):
        raise AssertionError("prepared execution cannot use network inputs")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    directory = tmp_path / "dataset"
    config, settings, interval, strategy = prepared_inputs(directory, index)
    book = PoolBook(directory, config, settings)
    try:
        model = EconomicModel(config, settings, strategy, book)
        journal = Journal(tmp_path / "state.sqlite", State)
        journal.create(UniswapEconomic.initial(settings, config, interval))
        with preloaded_workers():
            result = Simulator(journal, model).run()
        assert_roundtrip(result, model, "lp")
        assert len(book.timelines[config.target("position")].rows) == 3
    finally:
        book.close()


@pytest.mark.parametrize("index", [0, 1])
@pytest.mark.parametrize("stage", ["pending_commit", "effects_commit", "delivery_commit"])
def test_prepared_lp_reopens_dataset_and_journal_without_duplicate_effects(
    tmp_path, monkeypatch, index, stage
):
    def forbid(*args, **kwargs):
        raise AssertionError("prepared recovery cannot fetch replacement inputs")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    directory = tmp_path / "dataset"
    config, settings, interval, strategy = prepared_inputs(directory, index)
    journal = Journal(tmp_path / "state.sqlite", State)
    journal.create(UniswapEconomic.initial(settings, config, interval))

    def crash(at):
        if at == stage:
            raise RuntimeError("interrupted prepared run")

    book = PoolBook(directory, config, settings)
    try:
        model = EconomicModel(config, settings, strategy, book)
        with preloaded_workers(), pytest.raises(RuntimeError, match="interrupted prepared run"):
            Simulator(journal, model, fault=crash).run()
    finally:
        book.close()
    resumed = run_to_completion(directory, config, settings, strategy, journal.path)
    control = Journal(tmp_path / "control.sqlite", State)
    control.create(UniswapEconomic.initial(settings, config, interval))
    uninterrupted = run_to_completion(directory, config, settings, strategy, control.path)
    assert resumed.portfolio == uninterrupted.portfolio
    assert resumed.memory == uninterrupted.memory
    assert len(resumed.completed) == len(uninterrupted.completed)


def run_to_completion(directory, config, settings, strategy, path):
    book = PoolBook(directory, config, settings)
    try:
        model = EconomicModel(config, settings, strategy, book)
        with preloaded_workers():
            result = Simulator(Journal(path, State), model).run()
        assert_roundtrip(result, model, "lp")
        return result
    finally:
        book.close()
