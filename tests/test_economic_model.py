"""Actual reference strategy through the economic driver, scheduler and sandbox."""

import json

import pytest

from almanak.backtest.config import EconomicBacktestConfig
from almanak.backtest.economic_events import EconomicDriver
from almanak.backtest.economic_model import initial_state, parameters
from almanak.backtest.economic_state import EconomicState
from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.aave_v4.fork import ForkConfig
from almanak.models import Asset
from almanak.triggers import IntervalSelection
from tests.fork.eoa import configuration
from tests.test_economic_math import market
from tests.test_economic_math import reserve as reserve


class FixedBook:
    def __init__(self, config, reserve):
        self.markets = {}
        for allocation in config.positions:
            source = allocation.reserve
            metadata = reserve.model_copy(
                update={
                    "source": source,
                    "asset": Asset(
                        chain=source.chain,
                        identifier=source.asset,
                        decimals=18 if source.reserve_id == 0 else 6,
                    ),
                }
            )
            self.markets[source] = market(
                metadata, price=3000 * 10**8 if source.reserve_id == 0 else 10**8
            )

    def at(self, reserve, time):
        return self.markets[reserve]

    def evidence(self, reserve, time):
        return (self.markets[reserve].risk.reserve.block.timestamp,), ("a" * 64,)

    def next(self, time):
        return None


@pytest.fixture
def simulation(tmp_path, reserve, reference_strategy):
    raw = configuration()
    from tests.backtest_support import ROOT

    raw["backtest"] = json.loads(
        (ROOT / "tests/fixtures/author/aave_v4_weth_supply/backtest.json").read_text()
    )
    raw["backtest"]["funds"] = raw["backtest"]["funds"][:1]
    raw["backtest"].update(
        market_model="economic",
        data={
            "source": "aave_v4_archive",
            "rate_sample_seconds": 300,
            "risk_prices": "spoke_oracle_events",
        },
    )
    start = reserve.block.timestamp
    raw["backtest"]["schedule"].update(start=start, end=start + 21, cleanup_seconds=60)
    settings = EconomicBacktestConfig.model_validate_json(json.dumps(raw.pop("backtest")))
    author = ForkConfig.model_validate_json(json.dumps(raw))
    interval = IntervalSelection(configured=author.decision_trigger)
    config = parameters(settings, author, interval)
    state = initial_state(settings, config, interval)
    journal = Journal(tmp_path / "state.sqlite", EconomicState)
    journal.create(state)
    driver = EconomicDriver(config, settings, reference_strategy, FixedBook(config, reserve))
    return journal, driver


def test_reference_supply_withdraw_runs_with_no_historical_transport(simulation, monkeypatch):
    import httpx

    def forbidden(*args, **kwargs):
        raise AssertionError("economic execution must not call any HTTP transport")

    monkeypatch.setattr(httpx.Client, "send", forbidden)
    journal, model = simulation
    initial = journal.load()
    result = Simulator(journal, model).run()
    assert result.mode == "finished"
    assert len(result.completed) == result.delivered == 4
    assert [c.pending.intent.kind for c in result.completed] == [
        "supply",
        "supply",
        "withdraw",
        "withdraw",
    ]
    assert all(c.pending.eligible_at == c.pending.decision_time + 1 for c in result.completed)
    assert result.portfolio.balances == initial.portfolio.balances
    assert all(p.payload.shares == 0 for p in result.portfolio.positions)
    assert result.evaluations == 8
    assert len(result.memory.values["supplied_at"]) == 2


@pytest.mark.parametrize(
    "stage", ["pending_commit", "effects", "effects_commit", "outcome", "delivery_commit"]
)
def test_economic_crash_resume_does_not_duplicate_effects(simulation, stage):
    journal, model = simulation

    def crash(current):
        if current == stage:
            raise RuntimeError("lost runtime")

    with pytest.raises(RuntimeError, match="lost runtime"):
        Simulator(journal, model, fault=crash).run()
    result = Simulator(Journal(journal.path, EconomicState), model).run()
    assert result.mode == "finished"
    assert len(result.completed) == result.delivered == 4
    assert all(
        p.payload.supplied == p.payload.withdrawn == 10**16 for p in result.portfolio.positions
    )


def borrowing_simulation(tmp_path, reserve, reference_strategy):
    from pathlib import Path

    raw = json.loads(
        (
            Path(__file__).parents[1] / "tests/fixtures/author/aave_v4_weth_supply/config.json"
        ).read_text()
    )
    raw["backtest"] = json.loads(
        (
            Path(__file__).parents[1] / "tests/fixtures/author/aave_v4_weth_supply/backtest.json"
        ).read_text()
    )
    raw["backtest"].update(
        market_model="economic",
        data={
            "source": "aave_v4_archive",
            "rate_sample_seconds": 300,
            "risk_prices": "spoke_oracle_events",
        },
    )
    raw["backtest"]["schedule"].update(
        start=reserve.block.timestamp, end=reserve.block.timestamp + 31, cleanup_seconds=60
    )
    settings = EconomicBacktestConfig.model_validate_json(json.dumps(raw.pop("backtest")))
    author = ForkConfig.model_validate_json(json.dumps(raw))
    interval = IntervalSelection(configured=author.decision_trigger)
    config = parameters(settings, author, interval)
    initial = initial_state(settings, config, interval)
    journal = Journal(tmp_path / "borrow.sqlite", EconomicState)
    journal.create(initial)
    model = EconomicDriver(config, settings, reference_strategy, FixedBook(config, reserve))
    return journal, model


def test_actual_reference_borrowing_lifecycle_needs_no_prefunded_principal(
    tmp_path, reserve, reference_strategy
):
    journal, model = borrowing_simulation(tmp_path, reserve, reference_strategy)
    initial, config = journal.load(), model.config
    result = Simulator(journal, model).run()
    assert result.mode == "finished"
    assert len(result.completed) == result.delivered == 12
    assert result.portfolio.balances == initial.portfolio.balances
    borrowing = [c for c in result.completed if c.pending.intent.kind == "borrow"]
    assert len(borrowing) == 2
    for debt in config.debts:
        supplied = next(
            p.payload
            for p in result.portfolio.positions
            if p.payload.target.allocation == debt.name
        )
        # The debt names no amount; the reference borrows its own BORROW constant.
        assert supplied.borrowed == supplied.repaid == 5 * 10**6
        assert supplied.drawn_shares == supplied.premium_shares == 0
    assert all(not p.payload.collateral for p in result.portfolio.positions)


def test_interest_breach_search_matches_every_whole_second(reserve):
    from almanak.backtest.economic_data import RAY
    from almanak.backtest.economic_events import first_breach
    from almanak.backtest.economic_math import risk_view
    from tests.test_economic_math import debt_reserve, position

    d = debt_reserve(reserve)
    positions = (
        position(reserve, shares=100 * 10**18, collateral=True),
        position(d, drawn_shares=7900 * 10**6),
    )
    for supply_rate, borrow_rate in ((0, 1), (1, 2), (2, 1), (1, 1)):

        def markets(time, supply_rate=supply_rate, borrow_rate=borrow_rate):
            return (
                market(reserve, supply_index=RAY + supply_rate * time * RAY // 1000),
                market(d, price=10**8, borrow_index=RAY + borrow_rate * time * RAY // 1000),
            )

        expected = next(
            (t for t in range(101) if risk_view(positions, markets(t)).health_factor_wad < 10**18),
            None,
        )
        assert first_breach(positions, markets, 0, 100) == expected


def test_unsigned_supply_is_cancelled_at_teardown_boundary(simulation):
    journal, model = simulation
    initial = journal.load()
    schedule = initial.schedule.model_copy(update={"end": initial.time + 1})
    model.settings = model.settings.model_copy(update={"schedule": schedule})
    journal.save(initial, initial.model_copy(update={"schedule": schedule}))
    result = Simulator(journal, model).run()
    assert result.mode == "finished"
    assert len(result.cancelled) == 1
    assert not result.completed
    assert result.portfolio.balances == initial.portfolio.balances


class ShockBook(FixedBook):
    def __init__(self, config, reserve, shock):
        super().__init__(config, reserve)
        self.shock = shock

    def at(self, reserve, time):
        result = super().at(reserve, time)
        if time >= self.shock and reserve.reserve_id == 0:
            return result.model_copy(
                update={"price": result.price.model_copy(update={"price": 600 * 10**8})}
            )
        return result

    def next(self, time):
        return self.shock if time < self.shock else None


@pytest.mark.parametrize(
    "stage", ["event_effects", "event_commit", "event_delivery", "event_delivery_commit"]
)
def test_liquidation_effects_and_delivery_survive_replacement(
    tmp_path, reserve, reference_strategy, stage
):
    from almanak.worker_session import preloaded_workers

    journal, model = borrowing_simulation(tmp_path, reserve, reference_strategy)
    initial = journal.load()
    shock = initial.time + 27
    model.book = ShockBook(model.config, reserve, shock)
    expected_journal = Journal(tmp_path / "expected.sqlite", EconomicState)
    expected_journal.create(initial)
    with preloaded_workers():
        expected = Simulator(expected_journal, model).run()

        def crash(current):
            if current == stage and journal.load().time >= shock - 1:
                raise RuntimeError("lost economic coordinator")

        with pytest.raises(RuntimeError, match="lost economic coordinator"):
            Simulator(journal, model, fault=crash).run()
        replacement = EconomicDriver(
            model.config, model.settings, model.strategy, ShockBook(model.config, reserve, shock)
        )
        actual = Simulator(Journal(journal.path, EconomicState), replacement).run()
    assert actual == expected
    assert actual.mode == "finished"
    assert len(actual.events) == actual.events_delivered == 1
    event = actual.events[0].transition.result
    assert len(event.liquidations) == 2
    assert not event.deficits
