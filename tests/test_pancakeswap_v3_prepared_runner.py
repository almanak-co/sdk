"""Captured Pancake datasets drive actual references and offline journal recovery."""

import gzip
import json
from pathlib import Path

import httpx
import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.gateway import GatewayConfig
from almanak.connectors.interfaces import Connector
from almanak.connectors.pancakeswap_v3.economic_binding import PancakeEconomic
from almanak.connectors.pancakeswap_v3.economic_book import PoolBook
from almanak.connectors.pancakeswap_v3.economic_config import EconomicConfig
from almanak.connectors.pancakeswap_v3.economic_data import MODEL
from almanak.connectors.pancakeswap_v3.economic_model import EconomicModel
from almanak.connectors.pancakeswap_v3.economic_prepare import (
    acquire,
    authorize_ranges,
    provision,
)
from almanak.connectors.pancakeswap_v3.economic_state import State
from almanak.connectors.pancakeswap_v3.models import Config, Parameters
from almanak.connectors.pancakeswap_v3.support import candidate_support
from almanak.connectors.support import HistoricalSupport, RunMode
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers
from tests.exchange_scope import without_exchange_budgets
from tests.rebuilt_datasets import assert_rebuilt_dataset

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/pancakeswap-v3-bnb-prepared"
ADDITIONAL = (
    ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/pancakeswap-v3-additional-prepared"
)
ROWS = [
    *json.loads((EVIDENCE / "preparation-component.json").read_text()),
    *json.loads((ADDITIONAL / "preparation-component.json").read_text()),
]


def row_id(row):
    return f"{row.get('network', 'bnb')}-{row['kind']}"


def inputs(row):
    config = Parameters.model_validate_json(json.dumps(without_exchange_budgets(row["config"])))
    settings = EconomicConfig.model_validate_json(json.dumps(row["settings"]))
    interval = IntervalSelection(configured=config.decision_trigger)
    strategy = StrategyDefinition.read(
        ROOT / f"strategies/pancakeswap_v3_{row.get('network', 'bnb')}_{row['kind']}/strategy.py"
    )
    return config, settings, interval, strategy


# The trading captures predate 0bf62c6d5 (#115), after which trading targets also
# collect the pool's Mint and Burn events; their observations hold no such reads, so
# they cannot be replayed. The replay test uses recaptures of the same rows, prepared
# on 2026-10-19 by current collection from the same config and settings. Every other
# test keeps the original captures.
RECAPTURED = {
    row_id(row): row
    for name in (
        "pancakeswap-v3-bnb-prepared-2026-10-19",
        "pancakeswap-v3-additional-prepared-2026-10-19",
    )
    for row in json.loads((EVIDENCE.parent / name / "preparation-component.json").read_text())
}
REPLAYED = [RECAPTURED.get(row_id(row), row) for row in ROWS]


@pytest.mark.parametrize("name", sorted(RECAPTURED))
def test_recaptured_rows_keep_their_retained_inputs(name):
    original = next(row for row in ROWS if row_id(row) == name)
    assert inputs(RECAPTURED[name])[:2] == inputs(original)[:2]


def restore(directory, row, files):
    directory.mkdir(exist_ok=True)
    for name in files:
        (directory / name).write_bytes(gzip.decompress((ROOT / row["files"][name]).read_bytes()))


def candidate(monkeypatch):
    supports = {
        declared.chain: declared.model_copy(
            update={
                "modes": (RunMode.BACKTEST,),
                "historical": (
                    HistoricalSupport(model=MODEL, mode="economic", actions=declared.actions),
                ),
            }
        )
        for declared in candidate_support()
    }
    original = Connector.support_for

    def selected(self, chain):
        if self.codec.config is Config and chain in supports:
            return supports[chain]
        return original(self, chain)

    monkeypatch.setattr(Connector, "support_for", selected)


def assert_roundtrip(result, model, kind):
    assert all(c.transition.result.success for c in result.completed)
    expected = ["swap_exact_input"]
    if kind == "lp":
        expected += [
            "open_liquidity",
            "increase_liquidity",
            "decrease_liquidity",
            "collect_liquidity_fees",
        ]
    if kind == "lp":
        expected.append("close_liquidity")
    expected.append("exit_exchange")
    assert [c.pending.intent.kind for c in result.completed] == expected
    assert result.delivered == len(expected)
    report = PancakeEconomic.report(result, model)
    assert report["connector"] == "pancakeswap_v3"
    assert report["closed"] and not report["missing_inputs"]
    assert result.pending is None and result.portfolio.positions[0].payload.owned.position is None
    assert result.interval.effective.seconds == 5


def run(directory, row, path, *, fault=None):
    config, settings, _, strategy = inputs(row)
    book = PoolBook(directory, config, settings)
    try:
        model = EconomicModel(config, settings, strategy, book)
        with preloaded_workers():
            result = Simulator(Journal(path, State), model, fault=fault).run()
        assert_roundtrip(result, model, row["kind"])
        return result
    finally:
        book.close()


@pytest.mark.parametrize("row", REPLAYED, ids=row_id)
def test_replacement_gateways_reproduce_preparation_without_provider_authority(tmp_path, row):
    config, settings, _, _ = inputs(row)
    provision(tmp_path, config, settings)
    restore(tmp_path, row, ("observations.sqlite",))
    boundaries = authorize_ranges(tmp_path, config, settings, None)
    assert GatewayConfig.model_validate_json((tmp_path / "history.json").read_text()) == (
        GatewayConfig.model_validate_json(
            gzip.decompress((ROOT / row["files"]["history.json"]).read_bytes())
        )
    )
    acquire(tmp_path, config, settings, None, boundaries)
    book = PoolBook(tmp_path, config, settings)
    try:
        assert_rebuilt_dataset(
            tmp_path,
            book.manifest.model_dump(mode="json"),
            row["manifest"],
            gzip.decompress((ROOT / row["files"]["data.sqlite"]).read_bytes()),
        )
        assert book.manifest.records == 10
    finally:
        book.close()


@pytest.mark.parametrize("row", ROWS, ids=row_id)
@pytest.mark.parametrize("stage", [None, "pending_commit", "effects_commit", "delivery_commit"])
def test_reference_offline_execution_and_interrupted_commits(tmp_path, monkeypatch, row, stage):
    def forbid(*args, **kwargs):
        raise AssertionError("prepared execution cannot acquire network data")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    candidate(monkeypatch)
    directory = tmp_path / "dataset"
    restore(directory, row, ("data.sqlite", "dataset.json"))
    config, settings, interval, _ = inputs(row)
    journal = Journal(tmp_path / "state.sqlite", State)
    journal.create(PancakeEconomic.initial(settings, config, interval))
    if stage is None:
        run(directory, row, journal.path)
        return

    def crash(at):
        if at == stage:
            raise RuntimeError("interrupted prepared run")

    with pytest.raises(RuntimeError, match="interrupted prepared run"):
        run(directory, row, journal.path, fault=crash)
    resumed = run(directory, row, journal.path)
    control = Journal(tmp_path / "control.sqlite", State)
    control.create(PancakeEconomic.initial(settings, config, interval))
    uninterrupted = run(directory, row, control.path)
    assert resumed.portfolio == uninterrupted.portfolio
    assert resumed.memory == uninterrupted.memory
    assert len(resumed.completed) == len(uninterrupted.completed)


@pytest.mark.parametrize("callback", ["0x0000000000000000000000000000000000000001", "factory"])
def test_prepared_callback_requires_its_retained_revision_binding(tmp_path, callback):
    row = ROWS[0]
    restore(tmp_path, row, ("data.sqlite", "dataset.json"))
    config, settings, _, _ = inputs(row)
    book = PoolBook(tmp_path, config, settings)
    try:
        target = config.target("position")
        record = book.timelines[target].rows[0]
        changed = record.payload.pool.model_copy(
            update={
                "lm_pool": target.pool.factory if callback == "factory" else callback,
            }
        )
        forged = record.model_copy(
            update={
                "payload": record.payload.model_copy(update={"pool": changed}),
            }
        )
        with pytest.raises(ValueError, match="callback lacks retained revision evidence"):
            book.validate_row(target, book.baselines[target], forged)
    finally:
        book.close()
