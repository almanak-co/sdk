"""Reusable mixed strategy on synthetic frozen markets, with one durable simulator."""

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.aave_v3.domain import PositionObservation
from almanak.connectors.aave_v3.economic_data import MODEL as LENDING_MODEL
from almanak.connectors.aave_v3.economic_data import Market, RiskSample
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.connectors.composed.economic_model import EconomicDriver, component_state
from almanak.connectors.composed.economic_state import State
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.interfaces import Connector
from almanak.connectors.support import ConnectorChainSupport, HistoricalSupport, RunMode
from almanak.connectors.uniswap_v3.economic_data import MODEL as EXCHANGE_MODEL
from almanak.strategy import StrategyDefinition
from almanak.worker_session import preloaded_workers
from tests.composed_public import economic_inputs
from tests.test_aave_v3_reader import captured
from tests.test_uniswap_v3_economic_simulation import setup

REFERENCE = Path(__file__).parents[1] / "strategies/base_borrow_and_trade"


class FrozenLendingBook:
    def __init__(self, network="base"):
        self.markets = []
        for wire in captured(network)["positions"]:
            reserve = PositionObservation.model_validate_json(json.dumps(wire)).reserve
            sample = RiskSample(
                reserve=reserve,
                liquidation_grace_until=0,
                block_group=(reserve.block,),
                following_block=reserve.block.model_copy(
                    update={
                        "number": reserve.block.number + 1,
                        "timestamp": reserve.block.timestamp + 2,
                    }
                ),
            )
            self.markets.append(
                Market(
                    sample=sample,
                    supply_index=reserve.liquidity_index_ray,
                    debt_index=reserve.variable_borrow_index_ray,
                    time=reserve.block.timestamp,
                )
            )

    def at(self, source, time):
        return replace(next(m for m in self.markets if m.reserve.source == source), time=time)

    def evidence(self, source, time):
        return ("a" * 64,)

    def next_boundary(self, time, until):
        return None


def synthetic_support(self, chain):
    from almanak.connectors.aave_v3.support import verified_support as lending
    from almanak.connectors.uniswap_v3.support import verified_support as exchange

    is_lending = self.codec.config.__module__.endswith("aave_v3.models")
    declaration = next(d for d in (lending() if is_lending else exchange()) if d.chain == chain)
    return ConnectorChainSupport(
        chain=chain,
        modes=(RunMode.BACKTEST,),
        actions=declaration.actions,
        account_profiles=("eoa", "safe_roles"),
        restrictions=("synthetic test only",),
        historical=(
            HistoricalSupport(
                model=LENDING_MODEL if is_lending else EXCHANGE_MODEL,
                mode="economic",
                actions=declaration.actions,
            ),
        ),
    )


@pytest.fixture
def runner(monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("composed synthetic runner must remain offline")

    monkeypatch.setattr(httpx.Client, "send", forbid)
    monkeypatch.setattr(Connector, "support_for", synthetic_support)

    def build(path):
        _, config, settings, interval = economic_inputs(REFERENCE.name)
        _, record, _ = setup(1)
        at = record.available_at
        settings = settings.model_copy(
            update={
                "schedule": settings.schedule.model_copy(
                    update={
                        "start": at,
                        "end": at + 30,
                        "cleanup_seconds": 60,
                    }
                )
            }
        )
        strategy = StrategyDefinition(
            filename="strategy.py", source=(REFERENCE / "strategy.py").read_text()
        )
        book = SimpleNamespace(
            components={
                ConnectorId("aave_v3"): FrozenLendingBook(),
                ConnectorId("uniswap_v3"): SimpleNamespace(
                    at=lambda target, time: record, baselines={}
                ),
            }
        )
        model = EconomicDriver(config, settings, strategy, book)
        journal = Journal(path, State)
        journal.create(ComposedEconomic.initial(settings, config, interval))
        return journal, model

    return build


def test_one_strategy_borrows_trades_and_repays_with_one_cash_ledger(runner, tmp_path):
    journal, model = runner(tmp_path / "state.sqlite")
    with preloaded_workers():
        result = Simulator(journal, model).run()
    kinds = [c.pending.intent.kind for c in result.completed]
    assert kinds[:3] == ["supply", "borrow", "swap_exact_input"]
    assert all(c.transition.result.success for c in result.completed)
    assert "exit_exchange" in kinds and "repay" in kinds and "withdraw" in kinds
    assert result.delivered == len(result.completed)
    assert model.closed(result, model.observe(result))
    assert len(result.portfolio.balances) == 3
    report = ComposedEconomic.report(result, model)
    assert report["closed"]
    assert len(report["wallets"]) == 3
    assert len(report["gas_payers"]) == 1
    assert len(report["gas_payers"][0]["contributions"]) == 2
    assert all("wallets" not in component for component in report["components"].values())


@pytest.mark.parametrize("kind", ["borrow", "swap_exact_input"])
def test_committed_native_effect_survives_driver_replacement_without_duplicate_cash(
    runner, tmp_path, kind
):
    journal, model = runner(tmp_path / "interrupted.sqlite")
    initial = journal.load()

    def crash(stage):
        recorded = journal.load()
        if stage == "effects_commit" and recorded.completed[-1].pending.intent.kind == kind:
            raise RuntimeError("synthetic crash after durable economic effect")

    with preloaded_workers():
        with pytest.raises(RuntimeError, match="synthetic crash"):
            Simulator(journal, model, fault=crash).run()
    recorded = journal.load()
    assert recorded.delivered == len(recorded.completed) - 1
    assert recorded.pending is None
    count = len(recorded.completed)
    for connector in model.components:
        view = component_state(recorded, connector)
        assert State.model_validate_json(view.model_dump_json()) == view
    replacement = EconomicDriver(model.config, model.settings, model.strategy, model.book)
    with preloaded_workers():
        simulator = Simulator(journal, replacement)
        simulator.tick()
        delivered = journal.load()
        assert delivered.portfolio == recorded.portfolio
        assert len(delivered.completed) == count
        assert delivered.delivered == count
        recovered = simulator.run()

    baseline = Journal(tmp_path / "baseline.sqlite", State)
    baseline.create(initial)
    fresh = EconomicDriver(model.config, model.settings, model.strategy, model.book)
    with preloaded_workers():
        expected = Simulator(baseline, fresh).run()
    assert recovered == expected
