"""Captured lending and pool history execute one reference without provider access."""

import gzip
import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.cli import GatewayProcess
from almanak.backtest.economic_cli import create, reproduce, resume
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.connectors.composed.economic_book import Book
from almanak.connectors.interfaces import Connector
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from tests.composed_captures import rebind
from tests.legacy_references import frozen_backtest_inputs
from tests.test_composed_economic_runner import REFERENCE, synthetic_support

CAPTURE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/composed-base-prepared"
)


@pytest.fixture(params=["base", "multichain"])
def prepared(request, tmp_path, monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("captured composed execution cannot access providers")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    monkeypatch.setattr(GatewayProcess, "__enter__", forbid)
    # Candidate-only registration permits testing captured inputs without promotion.
    monkeypatch.setattr(Connector, "support_for", synthetic_support)
    reference = REFERENCE
    capture = CAPTURE
    if request.param == "multichain":
        reference = REFERENCE.with_name("ethereum_base_borrow_and_trade")
        capture = CAPTURE.with_name("composed-multichain-prepared")
    strategy = StrategyDefinition.read(reference / "strategy.py")
    selection = frozen_backtest_inputs(reference.name, strategy)
    author, settings = selection.author, selection.settings
    interval = IntervalSelection(configured=author.decision_trigger)
    config = ComposedEconomic.parameters(author, settings, interval)
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    (dataset / "dataset.json").write_bytes((capture / "dataset.json").read_bytes())
    for connector in ("aave_v3", "uniswap_v3"):
        child = dataset / connector
        child.mkdir()
        (child / "dataset.json").write_bytes((capture / connector / "dataset.json").read_bytes())
        (child / "data.sqlite").write_bytes(
            gzip.decompress((capture / connector / "data.sqlite.gz").read_bytes())
        )
    rebind(dataset, json.loads((capture / "run-manifest.json").read_text()), config, settings)
    args = SimpleNamespace(
        config=reference / "config.yaml",
        strategy=reference / "strategy.py",
        backtest_config=reference / "backtest-economic.yaml",
        dataset=dataset,
        output=tmp_path / "run",
        _backtest_inputs=selection,
    )
    return SimpleNamespace(
        args=args,
        author=author,
        settings=settings,
        config=config,
        strategy=strategy,
        interval=interval,
        dataset=dataset,
    )


def test_prepared_mixed_reference_runs_resumes_and_reproduces_offline(prepared):
    report = create(prepared.args, prepared.author, prepared.settings)
    funds = len(prepared.config.accounts.bindings)
    assert report["closed"] and report["completed_intents"] == 6 * funds
    assert report["delivered_outcomes"] == 6 * funds
    assert report["provider_requests_during_execution"] == 0
    assert len(report["wallets"]) == 3 * funds
    args = SimpleNamespace(run=prepared.args.output)
    assert resume(args)["closed"]
    assert reproduce(args)["reproduced"]


def test_composed_descriptor_refuses_policy_or_native_manifest_substitution(prepared):
    book = Book(prepared.dataset, prepared.config, prepared.settings)
    book.close()
    settings = prepared.settings.model_copy(
        update={
            "schedule": prepared.settings.schedule.model_copy(update={"latency_seconds": 2}),
        }
    )
    with pytest.raises(ValueError, match="frozen configuration"):
        Book(prepared.dataset, prepared.config, settings)
    path = prepared.dataset / "uniswap_v3/dataset.json"
    path.write_text(path.read_text() + "\n")
    with pytest.raises(ValueError, match="manifest differs from the frozen run"):
        Book(prepared.dataset, prepared.config, prepared.settings)


@pytest.mark.parametrize("kind", ["borrow", "swap_exact_input"])
def test_captured_effect_commit_reopens_both_books_without_reapplying_cash(prepared, kind):
    from almanak.backtest.engine import Journal, Simulator
    from almanak.connectors.composed.economic_model import EconomicDriver
    from almanak.connectors.composed.economic_state import State
    from almanak.worker_session import preloaded_workers

    initial = ComposedEconomic.initial(prepared.settings, prepared.config, prepared.interval)
    path = prepared.dataset.parent / "interrupted.sqlite"
    journal = Journal(path, State)
    journal.create(initial)

    def driver():
        book = Book(prepared.dataset, prepared.config, prepared.settings)
        return EconomicDriver(prepared.config, prepared.settings, prepared.strategy, book)

    def crash(stage):
        state = journal.load()
        if stage == "effects_commit" and state.completed[-1].pending.intent.kind == kind:
            raise RuntimeError("committed captured effect")

    model = driver()
    try:
        with preloaded_workers(), pytest.raises(RuntimeError, match="committed captured effect"):
            Simulator(journal, model, fault=crash).run()
    finally:
        model.book.close()
    committed = journal.load()
    assert committed.delivered == len(committed.completed) - 1
    model = driver()
    try:
        with preloaded_workers():
            resumed = Simulator(journal, model)
            resumed.tick()
            assert journal.load().portfolio == committed.portfolio
            recovered = resumed.run()
        baseline = Journal(path.with_name("baseline.sqlite"), State)
        baseline.create(initial)
        with preloaded_workers():
            assert Simulator(baseline, model).run() == recovered
    finally:
        model.book.close()
