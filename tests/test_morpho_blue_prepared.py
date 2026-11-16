"""Captured Morpho inputs exercise current references and shared journal recovery offline."""

import gzip
import shutil
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.cli import GatewayProcess
from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.history import HistoricalReader
from almanak.connectors.interfaces import Connector
from almanak.connectors.morpho_blue.economic_binding import MorphoEconomic
from almanak.connectors.morpho_blue.economic_book import Book
from almanak.connectors.morpho_blue.economic_data import MODEL
from almanak.connectors.morpho_blue.economic_events import EconomicDriver
from almanak.connectors.morpho_blue.economic_state import State
from almanak.connectors.morpho_blue.models import Config
from almanak.connectors.morpho_blue.support import candidate_support
from almanak.connectors.support import HistoricalSupport, RunMode
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.legacy_references import harness_directory

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout/morpho-blue-prepared"
AVALANCHE_EVIDENCE = ROOT / (
    "docs/internal/reports/evidence/2026-09-15-enablement/"
    "lending-morpho-avalanche-pinned-standalone-db728/public/cases/711/dataset"
)


def forbidden(*args, **kwargs):
    raise AssertionError("offline Morpho execution attempted historical or HTTP access")


@pytest.fixture(params=["ethereum", "base", "avalanche"])
def prepared(request, tmp_path, monkeypatch):
    name = f"morpho_blue_{request.param}_lending"
    reference = harness_directory(name)
    strategy_path = ROOT / "strategies" / name / "strategy.py"
    supports = {
        row.chain: row.model_copy(
            update={
                "modes": (RunMode.FORK, RunMode.BACKTEST),
                "historical": (
                    HistoricalSupport(model=MODEL, mode="economic", actions=row.actions),
                ),
            }
        )
        for row in candidate_support()
    }
    original = Connector.support_for
    monkeypatch.setattr(
        Connector,
        "support_for",
        lambda self, chain: (
            supports[chain]
            if self.codec.config is Config and chain in supports
            else original(self, chain)
        ),
    )
    monkeypatch.setattr(HistoricalReader, "_rpc", forbidden)
    monkeypatch.setattr(GatewayProcess, "__enter__", forbidden)
    monkeypatch.setattr(httpx.Client, "send", forbidden)
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    for name in ("dataset.json", "data.sqlite"):
        if request.param == "avalanche":
            shutil.copyfile(AVALANCHE_EVIDENCE / name, dataset / name)
        else:
            (dataset / name).write_bytes(
                gzip.decompress((EVIDENCE / request.param / (name + ".gz")).read_bytes())
            )
    author = read_author(reference / "config.yaml")
    settings = read_backtest(reference / "backtest-economic.yaml", author)
    strategy = StrategyDefinition.read(strategy_path)
    interval = IntervalSelection(configured=author.decision_trigger)
    config = MorphoEconomic.parameters(author, settings, interval)
    return SimpleNamespace(
        reference=reference,
        strategy_path=strategy_path,
        author=author,
        settings=settings,
        config=config,
        strategy=strategy,
        interval=interval,
        dataset=dataset,
    )


def test_current_reference_creates_resumes_and_reproduces_without_network(prepared, tmp_path):
    inputs = prepared
    args = SimpleNamespace(
        config=inputs.reference / "config.yaml",
        backtest_config=inputs.reference / "backtest-economic.yaml",
        strategy=inputs.strategy_path,
        dataset=inputs.dataset,
        output=tmp_path / "run",
        _backtest_inputs=(inputs.author, inputs.settings, inputs.strategy),
    )
    report = MorphoEconomic.create(args, inputs.author, inputs.settings)
    assert report["closed"] and report["completed_intents"] == 9
    assert report["provider_requests_during_execution"] == 0
    assert report["gas_payers"][0]["fund_accounts"] == ["borrower", "lender"]
    assert report["gas_payers"][0]["cost"] is None
    before = Journal(args.output / "state.sqlite", State).load()
    assert MorphoEconomic.resume(SimpleNamespace(run=args.output))["closed"]
    assert Journal(args.output / "state.sqlite", State).load() == before
    assert MorphoEconomic.reproduce(SimpleNamespace(run=args.output))["reproduced"]


def test_borrow_commit_recovers_with_replaced_book_and_driver(prepared, tmp_path):
    inputs = prepared
    journal = Journal(tmp_path / "interrupted.sqlite", State)
    initial = MorphoEconomic.initial(inputs.settings, inputs.config, inputs.interval)
    journal.create(initial)

    def crash(stage):
        if (
            stage == "effects_commit"
            and journal.load().completed[-1].pending.intent.kind == "borrow"
        ):
            raise RuntimeError("interrupted after Morpho borrowing")

    book = Book(inputs.dataset, inputs.config, inputs.settings)
    try:
        driver = EconomicDriver(inputs.config, inputs.settings, inputs.strategy, book)
        with preloaded_workers(), pytest.raises(RuntimeError, match="interrupted after Morpho"):
            Simulator(journal, driver, fault=crash).run()
    finally:
        book.close()
    retained = journal.load()
    assert retained.delivered == 2 and len(retained.completed) == 3
    book = Book(inputs.dataset, inputs.config, inputs.settings)
    try:
        driver = EconomicDriver(inputs.config, inputs.settings, inputs.strategy, book)
        with preloaded_workers():
            resumed = Simulator(Journal(journal.path, State), driver).run()
            baseline = Journal(tmp_path / "uninterrupted.sqlite", State)
            baseline.create(initial)
            expected = Simulator(baseline, driver).run()
        assert resumed == expected
        assert MorphoEconomic.report(resumed, driver)["closed"]
    finally:
        book.close()


def test_prepared_dataset_freezes_policy_market_scope_and_coverage(prepared):
    inputs = prepared
    book = Book(inputs.dataset, inputs.config, inputs.settings)
    try:
        source = inputs.config.positions[0].reserve.market
        assert len(book.rates) == len(book.risks) == 1
        for time in (inputs.settings.schedule.start - 1, inputs.settings.schedule.deadline + 1):
            with pytest.raises(InputUnavailable):
                book.at(source, time)
        assert book.evidence(source, inputs.settings.schedule.start)
    finally:
        book.close()
    changed = inputs.settings.model_copy(
        update={"data": inputs.settings.data.model_copy(update={"sample_seconds": 15})}
    )
    with pytest.raises(ValueError, match="frozen model or data policy"):
        Book(inputs.dataset, inputs.config, changed)
