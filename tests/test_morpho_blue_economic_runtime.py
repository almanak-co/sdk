"""Synthetic time paths exercise the actual reference and shared durable simulator."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import Timeline
from almanak.backtest.engine import Journal, Schedule, Simulator
from almanak.backtest.inventory import Fund
from almanak.backtest.types import UnixTime
from almanak.connectors.interfaces import Connector
from almanak.connectors.morpho_blue.authority import declared
from almanak.connectors.morpho_blue.domain import MarketObservation
from almanak.connectors.morpho_blue.economic_data import MODEL, RateRecord, requirement
from almanak.connectors.morpho_blue.economic_events import EconomicDriver
from almanak.connectors.morpho_blue.economic_parameters import initial
from almanak.connectors.morpho_blue.economic_state import Event, MarketInputGap, State
from almanak.connectors.morpho_blue.models import Config, Parameters
from almanak.connectors.morpho_blue.support import candidate_support
from almanak.connectors.support import HistoricalSupport, RunMode
from almanak.models import AssetAmount, SimulationAccount
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection, IntervalTrigger
from almanak.worker_session import preloaded_workers
from tests.test_morpho_blue_config import parameters as parameters
from tests.test_morpho_blue_reader import recorded


class SyntheticBook:
    def __init__(self, market, settings):
        self.market = market
        self.prices = SimpleNamespace(at=lambda time: ())
        needed = requirement(market.source, settings, "accounting")
        rows = tuple(
            RateRecord(
                key=needed.key,
                unit=needed.unit,
                source_time=market.block.timestamp,
                available_at=at,
                retrieved_at=float(at),
                evidence=("a" * 64,),
                reference=market.block,
                payload=market,
            )
            for at in range(needed.start, needed.end + 1, needed.sample_seconds)
        )
        self.rates = {market.source: Timeline(rows, needed)}

    def at(self, source, time):
        self.rates[source].at(time)
        return self.market

    def evidence(self, source, time):
        return self.rates[source].at(time).evidence

    def next_boundary(self, time, until):
        boundary = self.rates[self.market.source].next(time)
        return boundary if boundary is not None and boundary <= until else None


@pytest.fixture
def runtime(parameters, monkeypatch):
    environment = parameters.allocations[0].chain.environment
    parameters = Parameters.model_validate_json(
        parameters.model_dump_json().replace(environment, "mainnet")
    )
    config = parameters.model_copy(
        update={"decision_trigger": IntervalTrigger(kind="interval", seconds=2)}
    )
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
            supports[chain] if self.codec.config is Config else original(self, chain)
        ),
    )
    market = MarketObservation.model_validate_json(
        json.dumps(recorded(1, True)["market"]).replace(environment, "mainnet")
    )
    start = UnixTime(market.block.timestamp)
    settings = SimpleNamespace(
        schedule=Schedule(
            start=start,
            end=UnixTime(start + 20),
            latency_seconds=1,
            end_policy="teardown",
            cleanup_seconds=20,
        ),
        data=SimpleNamespace(sample_seconds=10, max_age_seconds=100, max_liquidations_per_event=10),
        costs=SimpleNamespace(kind="excluded"),
        funds=tuple(
            Fund(
                name=b.name,
                profile=SimulationAccount(fund=b.profile.fund),
                balances=tuple(
                    AssetAmount(asset=asset, amount=amount)
                    for asset, amount in (
                        (market.source.loan_asset, 2000 * 10**6),
                        (market.source.collateral_asset, 10**18),
                        (config.allocations[0].native_asset, 10**18),
                    )
                ),
            )
            for b in config.accounts.bindings
        ),
        payers=(),
    )
    strategy = StrategyDefinition.read(
        Path(__file__).parents[1] / "strategies/morpho_blue_ethereum_lending/strategy.py"
    )
    interval = IntervalSelection(configured=config.decision_trigger)
    return config, settings, strategy, interval, market


def driver(runtime):
    config, settings, strategy, _, market = runtime
    return EconomicDriver(
        config, settings, strategy, SyntheticBook(market, settings), authority=declared(config)
    )


def test_reference_recovers_across_accounting_event_and_borrow_commit(runtime, tmp_path):
    config, settings, _, interval, _ = runtime
    baseline = initial(settings, config, interval)
    journal = Journal(tmp_path / "interrupted.sqlite", State)
    journal.create(baseline)

    def crash(stage):
        if stage == "event_commit" and journal.load().events:
            raise RuntimeError("interrupted market accounting commit")

    with preloaded_workers(), pytest.raises(RuntimeError, match="market accounting commit"):
        Simulator(journal, driver(runtime), fault=crash).run()
    retained = journal.load()
    assert retained.events_delivered == 0
    assert retained.events[-1].transition.result.kind == "accounting"
    assert retained.completed[-1].pending.intent.kind == "borrow"
    with preloaded_workers():
        resumed = Simulator(Journal(journal.path, State), driver(runtime)).run()
        clean = Journal(tmp_path / "baseline.sqlite", State)
        clean.create(baseline)
        expected = Simulator(clean, driver(runtime)).run()
    assert resumed == expected
    assert len(resumed.completed) == resumed.delivered == 9
    assert all(step.transition.result.success for step in resumed.completed)
    assert driver(runtime).closed(resumed, driver(runtime).observe(resumed))


class StressBook(SyntheticBook):
    def __init__(self, market, settings, scenario):
        super().__init__(market, settings)
        self.scenario, self.start = scenario, settings.schedule.start

    def at(self, source, time):
        market = super().at(source, time)
        if time < self.start + 8:
            return market
        if self.scenario == "missing":
            return market.model_copy(
                update={"price": None, "price_error": "synthetic missing oracle"}
            )
        return market.model_copy(
            update={"price": market.price.model_copy(update={"price": market.price.price // 1000})}
        )

    def evidence(self, source, time):
        return (*super().evidence(source, time), "synthetic-stress:" + self.scenario)


def stress_driver(runtime, scenario):
    config, settings, strategy, _, market = runtime
    return EconomicDriver(
        config,
        settings,
        strategy,
        StressBook(market, settings, scenario),
        authority=declared(config),
    )


def test_missing_borrower_oracle_preserves_independent_lender_exit(runtime, tmp_path):
    config, settings, _, interval, _ = runtime
    journal = Journal(tmp_path / "missing.sqlite", State)
    journal.create(initial(settings, config, interval))
    with preloaded_workers(), pytest.raises(ValueError, match="unresolved obligations"):
        Simulator(journal, stress_driver(runtime, "missing")).run()
    final = journal.load()
    positions = {p.payload.target.allocation: p.payload for p in final.portfolio.positions}
    assert positions["earn"].empty
    assert positions["loan"].borrow_shares > 0
    assert positions["collateral"].collateral > 0
    missing = [gap for e in final.events for gap in e.transition.result.affected_markets]
    assert len(missing) == 1 and missing[0].fund == config.target("loan").fund
    foreign = missing[0].gap.model_copy(
        update={"key": missing[0].gap.key.model_copy(update={"source": "another-market"})}
    )
    with pytest.raises(ValueError, match="affected market"):
        MarketInputGap.model_validate(missing[0].model_dump() | {"gap": foreign})


def test_liquidation_event_recovers_and_lender_loss_survives_next_sample(runtime, tmp_path):
    config, settings, _, interval, _ = runtime
    journal = Journal(tmp_path / "liquidation.sqlite", State)
    journal.create(initial(settings, config, interval))

    def crash(stage):
        if (
            stage == "event_commit"
            and journal.load().events[-1].transition.result.kind == "liquidation"
        ):
            raise RuntimeError("interrupted liquidation commit")

    with preloaded_workers(), pytest.raises(RuntimeError, match="liquidation commit"):
        Simulator(journal, stress_driver(runtime, "loss"), fault=crash).run()
    retained = journal.load()
    assert retained.events_delivered == 0
    event = retained.events[-1].transition.result
    with pytest.raises(ValueError, match="event kind"):
        Event.model_validate(event.model_dump() | {"kind": "accounting"})
    with preloaded_workers():
        final = Simulator(Journal(journal.path, State), stress_driver(runtime, "loss")).run()
    assert all(p.payload.empty for p in final.portfolio.positions)
    losses = [loss for event in final.events for loss in event.transition.result.liquidations]
    assert len(losses) == 1 and losses[0].liquidation.bad_debt_assets > 0
    earn = next(
        p.payload for p in final.portfolio.positions if p.payload.target.allocation == "earn"
    )
    assert earn.withdrawn < earn.supplied
