"""Captured lending and pool history execute one reference without provider access."""

import gzip
import json
from importlib import import_module
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.cli import GatewayProcess
from almanak.backtest.economic_cli import create, reproduce, resume
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.connectors.composed.economic_book import Book
from almanak.connectors.interfaces import Connector
from almanak.connectors.support import HistoricalSupport, RunMode
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from tests.composed_captures import rebind
from tests.legacy_references import frozen_backtest_inputs

ROOT = Path(__file__).parents[1]
pytestmark = pytest.mark.filterwarnings("error::UserWarning")


def candidate_support(self, chain):
    protocol = self.codec.config.__module__.split(".")[-2]
    if protocol not in ("morpho_blue", "compound_v3", "uniswap_v3"):
        raise ValueError("unexpected candidate protocol")
    support = import_module(f"almanak.connectors.{protocol}.support")
    rows = support.verified_support() if protocol == "uniswap_v3" else support.candidate_support()
    model = import_module(f"almanak.connectors.{protocol}.economic_data").MODEL
    row = next(r for r in rows if r.chain == chain)
    return row.model_copy(
        update={
            "modes": (RunMode.FORK, RunMode.BACKTEST),
            "historical": (HistoricalSupport(model=model, mode="economic", actions=row.actions),),
        }
    )


CAPTURE = (
    Path(__file__).parents[1]
    / "docs/internal/reports/evidence/2026-09-07-rollout/composed-lending-prepared"
)


@pytest.fixture(params=["morpho", "compound"])
def prepared(request, tmp_path, monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("captured composed execution cannot access providers")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    monkeypatch.setattr(GatewayProcess, "__enter__", forbid)
    # Candidate-only registration permits testing captured inputs without promotion.
    monkeypatch.setattr(Connector, "support_for", candidate_support)
    name = f"base_{request.param}_borrow_and_trade"
    reference = ROOT / "strategies" / name
    capture = CAPTURE / request.param / "dataset"
    strategy = StrategyDefinition.read(reference / "strategy.py")
    selection = frozen_backtest_inputs(name, strategy)
    author, settings = selection.author, selection.settings
    interval = IntervalSelection(configured=author.decision_trigger)
    config = ComposedEconomic.parameters(author, settings, interval)
    dataset = tmp_path / "dataset"
    dataset.mkdir()
    for path in capture.rglob("*.gz"):
        relative = path.relative_to(capture).with_suffix("")
        if relative.name not in ("dataset.json", "data.sqlite"):
            continue
        target = dataset / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(gzip.decompress(path.read_bytes()))
    recorded = gzip.decompress((CAPTURE / request.param / "run/manifest.json.gz").read_bytes())
    rebind(dataset, json.loads(recorded), config, settings)
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


@pytest.mark.parametrize("prepared", ["morpho"], indirect=True)
def test_accounting_batch_commit_recovers_native_generation_without_author_event(prepared):
    from almanak.backtest.engine import Journal, Simulator
    from almanak.connectors.composed.economic_model import EconomicDriver, component_state
    from almanak.connectors.composed.economic_state import State
    from almanak.connectors.identifiers import ConnectorId
    from almanak.worker_session import preloaded_workers

    p = prepared
    initial = ComposedEconomic.initial(p.settings, p.config, p.interval)
    journal = Journal(p.dataset.parent / "event.sqlite", State)
    journal.create(initial)

    def crash(stage):
        if stage == "event_commit" and journal.load().events:
            raise RuntimeError("committed composed market event")

    book = Book(p.dataset, p.config, p.settings)
    model = EconomicDriver(p.config, p.settings, p.strategy, book)
    try:
        with preloaded_workers(), pytest.raises(RuntimeError, match="composed market event"):
            Simulator(journal, model, fault=crash).run()
    finally:
        book.close()
    before = journal.load()
    assert before.events_delivered == 0 and len(before.events) == 1
    batch = before.events[0].transition.result
    assert len(batch.components) == 1
    assert batch.components[0].connector == ConnectorId("morpho_blue")
    assert batch.components[0].transition.result.kind == "accounting"
    book = Book(p.dataset, p.config, p.settings)
    model = EconomicDriver(p.config, p.settings, p.strategy, book)
    try:
        with preloaded_workers():
            simulator = Simulator(Journal(journal.path, State), model)
            simulator.tick()
            delivered = journal.load()
            assert delivered.memory == before.memory and delivered.portfolio == before.portfolio
            assert delivered.events_delivered == 1
            native = component_state(delivered, ConnectorId("morpho_blue"))
            assert native.events[0].transition.result == batch.components[0].transition.result
            assert component_state(delivered, ConnectorId("uniswap_v3")).events == ()
            recovered = simulator.run()
            baseline = Journal(p.dataset.parent / "clean-event.sqlite", State)
            baseline.create(initial)
            assert Simulator(baseline, model).run() == recovered
    finally:
        book.close()


def test_unavailable_reverse_trade_keeps_lending_debt_and_collateral_visible(prepared, monkeypatch):
    from almanak.backtest.dataset import InputUnavailable
    from almanak.backtest.engine import Journal, Simulator
    from almanak.connectors.composed.economic_model import EconomicDriver
    from almanak.connectors.composed.economic_state import State
    from almanak.connectors.identifiers import ConnectorId
    from almanak.connectors.uniswap_v3.economic_data import key
    from almanak.worker_session import preloaded_workers

    p = prepared
    book = Book(p.dataset, p.config, p.settings)
    pool = book.components[ConnectorId("uniswap_v3")]
    original = pool.at

    def unavailable(target, time):
        if time > p.settings.schedule.end:
            raise InputUnavailable(
                key(target), time, "synthetic unavailable reverse-trade evidence"
            )
        return original(target, time)

    monkeypatch.setattr(pool, "at", unavailable)
    model = EconomicDriver(p.config, p.settings, p.strategy, book)
    journal = Journal(p.dataset.parent / "failed-reverse.sqlite", State)
    journal.create(ComposedEconomic.initial(p.settings, p.config, p.interval))
    try:
        with preloaded_workers(), pytest.raises(ValueError, match="unresolved obligations"):
            Simulator(journal, model).run()
        state = journal.load()
        native = [
            p.payload
            for p in state.portfolio.positions
            if p.key.connector != ConnectorId("uniswap_v3")
        ]
        assert len(native) == 2 and all(not p.empty for p in native)
        assert state.memory.values["traded"]
        failed = [c for c in state.completed if not c.transition.result.success]
        assert len(failed) == 1 and failed[0].pending.intent.kind == "exit_exchange"
        assert failed[0].transition.result.unavailable is not None
        assert not ComposedEconomic.report(state, model)["closed"]
    finally:
        book.close()
