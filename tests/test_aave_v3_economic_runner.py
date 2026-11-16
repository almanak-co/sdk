"""Shared journal recovery using the reusable V3 strategy and synthetic markets."""

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.backtest.dataset import InputUnavailable
from almanak.backtest.engine import Journal, Simulator
from almanak.configuration.strategy_parameters import StrategyParameters
from almanak.connectors.aave_v3.economic_binding import AaveEconomic
from almanak.connectors.aave_v3.economic_config import EconomicConfig
from almanak.connectors.aave_v3.economic_data import MODEL, key
from almanak.connectors.aave_v3.economic_events import EconomicDriver
from almanak.connectors.aave_v3.economic_parameters import initial
from almanak.connectors.aave_v3.economic_state import State
from almanak.connectors.aave_v3.models import Config
from almanak.connectors.interfaces import Connector
from almanak.connectors.support import (
    ConnectorChainSupport,
    EconomicAction,
    HistoricalSupport,
    RunMode,
)
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers
from tests.test_aave_v3_config import parameters as parameters
from tests.test_aave_v3_economic_actions import markets

REFERENCE = Path(__file__).parents[1] / "strategies/aave_v3_ethereum_lending/strategy.py"


def reference_source():
    return REFERENCE.read_text()


def with_large_borrow(parameters):
    """A 1,000 USDC borrow, which the synthetic price shock liquidates.

    The reference's `borrow_quantity` is 5 USDC, far too little for a synthetic shock to
    reach; the knob, not the source, changes.
    """
    knobs = StrategyParameters({"borrow_quantity": "1000"})
    return parameters.model_copy(update={"parameters": knobs})


class SyntheticBook:
    def __init__(self, fault=None):
        self.markets = markets()
        self.start = self.markets[0].time
        self.fault = fault
        self.prices = SimpleNamespace(at=lambda time: ())

    def at(self, source, time):
        market = next(m for m in self.markets if m.reserve.source == source)
        if self.fault == "missing" and time >= self.start + 210:
            raise InputUnavailable(key(source, "risk"), time, "synthetic provider gap")
        if self.fault == "liquidation" and time >= self.start + 210 and market == self.markets[0]:
            reserve = market.reserve
            reserve = reserve.model_copy(
                update={"oracle": reserve.oracle.model_copy(update={"price": 900 * 10**8})}
            )
            market = replace(market, sample=market.sample.model_copy(update={"reserve": reserve}))
        return replace(market, time=time)

    def evidence(self, source, time):
        return ("a" * 64,)

    def next_boundary(self, time, until):
        boundary = self.start + 210
        return boundary if self.fault and time < boundary <= until else None


def settings(config, start):
    profile = {
        "kind": "simulation",
        "fund": config.accounts.bindings[0].profile.fund.model_dump(mode="json"),
    }
    wire = {
        "market_model": "economic",
        "schedule": {
            "start": start,
            "end": start + 600,
            "latency_seconds": 1,
            "end_policy": "teardown",
            "cleanup_seconds": 180,
        },
        "funds": [
            {
                "name": "fund",
                "profile": profile,
                "balances": [
                    {"asset": str(config.target("collateral").asset), "amount": 10**18},
                    {"asset": str(config.target("loan").asset), "amount": 100 * 10**6},
                    {"asset": str(config.positions[0].native_asset), "amount": 10**18},
                ],
            }
        ],
        "history": [
            {
                "chain": config.positions[0].chain.model_dump(mode="json"),
                "provider": {"kind": "alchemy", "network": "eth-mainnet", "api_key_env": "UNUSED"},
            }
        ],
        "costs": {"kind": "excluded"},
        "data": {
            "source": "aave_v3_archive",
            "risk_model": "pool_oracle_each_block",
            "sample_seconds": 60,
            "max_age_seconds": 120,
            "max_accounting_samples": 100,
            "max_risk_blocks": 100,
            "max_risk_samples": 200,
            "max_liquidations_per_event": 8,
        },
        "liquidation_model": "immediate_underlying_v3_revision11",
    }
    return EconomicConfig.model_validate_json(json.dumps(wire))


@pytest.fixture
def runner(parameters, monkeypatch):
    parameters = with_large_borrow(parameters)
    # This declaration is confined to component tests. No chain is promoted by
    # synthetic data or by installing a wire binding in production composition.
    actions = tuple(
        EconomicAction(a)
        for a in (
            "supply",
            "borrow",
            "repay_amount",
            "withdraw_amount",
            "repay",
            "withdraw",
            "collateral",
        )
    )
    support = ConnectorChainSupport(
        chain=parameters.positions[0].chain.chain_id,
        modes=(RunMode.BACKTEST,),
        actions=actions,
        account_profiles=("eoa", "safe_roles"),
        restrictions=("synthetic test only",),
        historical=(HistoricalSupport(model=MODEL, mode="economic", actions=actions),),
    )
    original = Connector.support_for
    monkeypatch.setattr(
        Connector,
        "support_for",
        lambda self, chain: (
            support
            if self.codec.config is Config and chain == support.chain
            else original(self, chain)
        ),
    )
    strategy = StrategyDefinition(filename="strategy.py", source=reference_source())

    def build(path, fault=None):
        book = SyntheticBook(fault)
        configured = settings(parameters, book.start)
        interval = IntervalSelection(configured=parameters.decision_trigger)
        config = AaveEconomic.parameters(parameters, configured, interval)
        driver = EconomicDriver(config, configured, strategy, book)
        journal = Journal(path, State)
        journal.create(initial(configured, config, interval))
        return journal, driver

    return build


def test_reference_runs_all_intents_and_holds_without_network(runner, tmp_path):
    journal, model = runner(tmp_path / "state.sqlite")
    with preloaded_workers():
        result = Simulator(journal, model).run()
    assert all(step.transition.result.success for step in result.completed)
    assert [step.pending.intent.kind for step in result.completed] == [
        "supply",
        "collateral",
        "collateral",
        "borrow",
        "repay_amount",
        "withdraw_amount",
        "repay",
        "withdraw",
    ]
    assert result.delivered == 8
    assert (
        result.portfolio.balances
        == initial(model.settings, model.config, result.interval).portfolio.balances
    )
    report = AaveEconomic.report(result, model)
    assert report["closed"]
    assert report["gas_payers"][0]["cost"] is None
    assert report["missing_inputs"] == []


@pytest.mark.parametrize("stage", ["pending_commit", "effects_commit", "delivery_commit"])
def test_reference_resumes_committed_actions_without_duplicate_effects(runner, tmp_path, stage):
    journal, model = runner(tmp_path / "state.sqlite")

    def crash(at):
        if at == stage:
            raise RuntimeError("interrupted")

    with preloaded_workers():
        with pytest.raises(RuntimeError, match="interrupted"):
            Simulator(journal, model, fault=crash).run()
        resumed = Simulator(Journal(journal.path, State), model).run()
    assert len(resumed.completed) == resumed.delivered == 8
    assert AaveEconomic.report(resumed, model)["closed"]


def test_liquidation_event_survives_commit_before_strategy_delivery(runner, tmp_path):
    journal, model = runner(tmp_path / "state.sqlite", "liquidation")

    def crash(stage):
        if stage == "event_commit":
            raise RuntimeError("interrupted")

    with preloaded_workers():
        with pytest.raises(RuntimeError, match="interrupted"):
            Simulator(journal, model, fault=crash).run()
        retained = journal.load()
        assert len(retained.events) == 1 and retained.events_delivered == 0
        result = Simulator(Journal(journal.path, State), model).run()
    assert len(result.events) == result.events_delivered == 1
    assert result.portfolio.positions[1].payload.deficit > 0
    assert all(step.pending.intent.kind != "borrow" for step in result.completed[4:])
    assert AaveEconomic.report(result, model)["closed"]


def test_missing_history_preserves_obligations_and_reports_incomplete_closure(runner, tmp_path):
    journal, model = runner(tmp_path / "state.sqlite", "missing")
    with preloaded_workers():
        with pytest.raises(ValueError, match="unresolved obligations"):
            Simulator(journal, model).run()
    result = journal.load()
    assert len(result.events) == result.events_delivered == 1
    assert result.portfolio.positions[1].payload.debt_scaled > 0
    report = AaveEconomic.report(result, model)
    assert not report["closed"]
    assert report["missing_inputs"]
    assert all(p["measurement_status"] == "unavailable" for p in report["positions"])
