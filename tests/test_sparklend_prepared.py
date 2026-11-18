"""Captured Spark inputs reproduce through the shared runner without a data gateway."""

import gzip
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.backtest.cli import GatewayProcess
from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.engine import Journal, Simulator
from almanak.backtest.history import HistoricalReader
from almanak.connectors.sparklend.economic_binding import SparkEconomic
from almanak.connectors.sparklend.economic_book import Book
from almanak.connectors.sparklend.economic_events import EconomicDriver
from almanak.connectors.sparklend.economic_state import State
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers
from scripts.legacy_configuration.author import read_author
from scripts.legacy_configuration.backtest import read_backtest
from tests.legacy_references import harness_directory
from tests.sparklend_helpers import candidate_spark_support as candidate_spark_support

ROOT = Path(__file__).parents[1]
EVIDENCE = ROOT / "docs/internal/reports/evidence/2026-09-07-rollout"


def forbidden(*args, **kwargs):
    raise AssertionError("offline execution attempted historical data access")


@pytest.fixture
def prepared(tmp_path, monkeypatch, candidate_spark_support):
    name = "sparklend_ethereum_lending"
    reference = harness_directory(name)
    strategy_path = ROOT / "strategies" / name / "strategy.py"
    capture = EVIDENCE / "sparklend-prepared"
    monkeypatch.setattr(HistoricalReader, "_rpc", forbidden)
    monkeypatch.setattr(GatewayProcess, "__enter__", forbidden)
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    (dataset / "dataset.json").write_bytes((capture / "dataset.json").read_bytes())
    (dataset / "data.sqlite").write_bytes(
        gzip.decompress((capture / "data.sqlite.gz").read_bytes())
    )
    author = read_author(reference / "config.yaml")
    settings = read_backtest(reference / "backtest-economic.yaml", author)
    strategy = StrategyDefinition.read(strategy_path)
    interval = IntervalSelection(configured=author.decision_trigger)
    config = SparkEconomic.parameters(author, settings, interval)
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


def test_captured_reference_creates_resumes_and_reproduces_offline(prepared, tmp_path):
    inputs = prepared
    reference = inputs.reference
    args = SimpleNamespace(
        config=reference / "config.yaml",
        backtest_config=reference / "backtest-economic.yaml",
        strategy=inputs.strategy_path,
        dataset=inputs.dataset,
        output=tmp_path / "run",
        _backtest_inputs=(inputs.author, inputs.settings, inputs.strategy),
    )
    report = SparkEconomic.create(args, inputs.author, inputs.settings)
    assert report["closed"] and report["completed_intents"] == 8
    assert all(
        c.transition.result.success
        for c in Journal(args.output / "state.sqlite", State).load().completed
    )
    assert report["provider_requests_during_execution"] == 0
    before = Journal(args.output / "state.sqlite", State).load()
    resumed = SparkEconomic.resume(SimpleNamespace(run=args.output))
    assert resumed["closed"]
    assert Journal(args.output / "state.sqlite", State).load() == before
    assert SparkEconomic.reproduce(SimpleNamespace(run=args.output))["reproduced"]


def test_captured_borrow_commit_recovers_with_a_new_book_and_driver(prepared, tmp_path):
    inputs = prepared
    journal = Journal(tmp_path / "interrupted.sqlite", State)
    initial = SparkEconomic.initial(inputs.settings, inputs.config, inputs.interval)
    journal.create(initial)

    def crash(stage):
        if (
            stage == "effects_commit"
            and journal.load().completed[-1].pending.intent.kind == "borrow"
        ):
            raise RuntimeError("interrupted after borrowing")

    book = Book(inputs.dataset, inputs.config, inputs.settings)
    try:
        driver = EconomicDriver(inputs.config, inputs.settings, inputs.strategy, book)
        with preloaded_workers(), pytest.raises(RuntimeError, match="interrupted after borrowing"):
            Simulator(journal, driver, fault=crash).run()
    finally:
        book.close()
    pending_delivery = journal.load()
    assert pending_delivery.delivered == 3 and len(pending_delivery.completed) == 4
    book = Book(inputs.dataset, inputs.config, inputs.settings)
    try:
        driver = EconomicDriver(inputs.config, inputs.settings, inputs.strategy, book)
        with preloaded_workers():
            resumed = Simulator(Journal(journal.path, State), driver).run()
            baseline = Journal(tmp_path / "uninterrupted.sqlite", State)
            baseline.create(initial)
            expected = Simulator(baseline, driver).run()
        assert resumed == expected
        assert SparkEconomic.report(resumed, driver)["closed"]
    finally:
        book.close()


def test_prepared_data_preserves_coverage_and_rejects_changed_policy(prepared):
    inputs = prepared
    book = Book(inputs.dataset, inputs.config, inputs.settings)
    try:
        source = inputs.config.positions[0].reserve
        with pytest.raises(InputUnavailable):
            book.at(source, inputs.settings.schedule.start - 1)
        with pytest.raises(InputUnavailable):
            book.at(source, inputs.settings.schedule.deadline + 1)
        assert book.evidence(source, inputs.settings.schedule.start)
    finally:
        book.close()
    changed = inputs.settings.model_copy(
        update={
            "data": inputs.settings.data.model_copy(update={"sample_seconds": 30}),
        }
    )
    with pytest.raises(ValueError, match="frozen model or data policy"):
        Book(inputs.dataset, inputs.config, changed)
