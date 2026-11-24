"""The composed bridge-and-lend reference in an economic backtest, offline.

The bridge is the real model. The lending side is the real Comet economic driver over
one literal market re-pinned to Base, so no retained dataset is read. The hooks are the
reference's own, executed in the sandbox.
"""

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest

from almanak.backtest.engine import Journal, Simulator
from almanak.connectors.across.economic_state import ArrivalEvent, ModeledArrival
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.connectors.composed.economic_config import AcrossPolicy, EconomicConfig
from almanak.connectors.composed.economic_model import (
    EconomicDriver,
    arrivals,
    component_state,
    modeled_transfers,
    ownership,
)
from almanak.connectors.composed.economic_state import State
from almanak.connectors.composed.models import OwnedParameters
from almanak.connectors.compound_v3.domain import Market, MarketObservation, Reserve
from almanak.connectors.compound_v3.economic_data import EconomicMarket, RiskSample
from almanak.connectors.compound_v3.models import Allocation as CompoundAllocation
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import Balance, WalletAsset
from almanak.gateway.evm import native_asset
from almanak.strategy import StrategyDefinition
from almanak.triggers import IntervalSelection
from almanak.worker_session import preloaded_workers
from tests import test_compound_v3_offline as comet
from tests.across_economic_support import ETH, GAS, SENT, START, author, fund
from tests.across_support import ARBITRUM, BASE, BASE_USDC, RECIPIENT, SENDER, USDC

REFERENCE = Path(__file__).parents[1] / "strategies/arbitrum_base_bridge_and_lend"
ACROSS, COMPOUND = ConnectorId("across"), ConnectorId("compound_v3")
BASE_ETH = native_asset(BASE.chain_id)
SOURCE = WalletAsset(account="arb", chain=ARBITRUM, asset=USDC)
DESTINATION = WalletAsset(account="base", chain=BASE, asset=BASE_USDC)
MARKET = Market(
    chain=BASE, comet=comet.COMET, base_asset=BASE_USDC, base_price_feed=comet.BASE_FEED
)
ARRIVED = SENT - SENT * 10 // 10_000


def base_market():
    """The literal Comet market of the offline tests, with Base identity and a later block."""
    wire = comet.observation().model_dump_json()
    for old, new in (
        ("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48", str(BASE_USDC)),
        ("eip155:1/", "eip155:8453/"),
        ('"eip155:1"', '"eip155:8453"'),
        ("1700000000", str(START - 12)),
    ):
        wire = wire.replace(old, new)
    observed = MarketObservation.model_validate_json(wire)
    block = observed.block
    following = block.model_copy(update={"number": block.number + 1, "timestamp": START})
    sample = RiskSample(market=observed, block_group=(block,), following_block=following)
    return EconomicMarket(sample, comet.SUPPLY_INDEX, comet.BORROW_INDEX, block.timestamp)


class FrozenComet:
    """One Comet market held still: no accrual boundary and no price series."""

    def __init__(self):
        self.market = base_market()
        self.prices = SimpleNamespace(at=lambda time: ())

    def at(self, source, time):
        assert source == MARKET
        return replace(self.market, time=time)

    def evidence(self, source, time):
        return ("a" * 64,)

    def next_boundary(self, time, until):
        return None


def parameters():
    bridge = author(interval=10)
    earn = CompoundAllocation(
        name="earn",
        account="base",
        reserve=Reserve(market=MARKET, asset=BASE_USDC, price_feed=comet.BASE_FEED),
        native_asset=BASE_ETH,
        minimum_native_balance=0,
    )
    return OwnedParameters(
        decision_trigger=bridge.decision_trigger,
        asset_metadata=bridge.asset_metadata,
        parameters=bridge.parameters,
        accounts=bridge.accounts,
        allocations=(*bridge.allocations, earn),
        inventory=(Balance(key=SOURCE, amount=SENT), Balance(key=DESTINATION, amount=0)),
    )


def settings(*, latency=20, window=60, end_policy="teardown", cleanup=60):
    def history(chain, network):
        provider = {"kind": "alchemy", "network": network, "api_key_env": "UNUSED"}
        return {"chain": chain.model_dump(mode="json"), "provider": provider}

    wire = {
        "market_model": "economic",
        "schedule": {
            "start": START,
            "end": START + window,
            "latency_seconds": 1,
            "end_policy": end_policy,
            "cleanup_seconds": cleanup,
        },
        "history": [history(ARBITRUM, "arb-mainnet"), history(BASE, "base-mainnet")],
        "funds": [
            fund("arb", ARBITRUM, SENDER, (USDC, SENT), (ETH, GAS)),
            fund("base", BASE, RECIPIENT, (BASE_USDC, 0), (BASE_ETH, GAS)),
        ],
        "models": {
            "across": {
                "data": {"arrival_latency_seconds": latency},
                "costs": {"kind": "excluded"},
                "bridge_model": "full_fee_ceiling_fixed_latency",
            },
            "compound_v3": {
                "data": {
                    "source": "compound_v3_archive",
                    "risk_model": "comet_oracle_each_block",
                    "sample_seconds": 60,
                    "max_age_seconds": 120,
                    "max_accounting_samples": 100,
                    "max_risk_blocks": 100,
                    "max_risk_samples": 100,
                    "max_liquidations_per_event": 8,
                },
                "costs": {"kind": "excluded"},
                "liquidation_model": "immediate_comet_absorb",
            },
        },
    }
    return EconomicConfig.model_validate_json(json.dumps(wire))


@pytest.fixture
def runner(monkeypatch, tmp_path):
    def forbid(*args, **kwargs):
        raise AssertionError("the composed bridge backtest must remain offline")

    monkeypatch.setattr(httpx.Client, "send", forbid)

    def build(name="state.sqlite", **changes):
        configured = settings(**changes)
        authored = parameters()
        interval = IntervalSelection(configured=authored.decision_trigger)
        config = ComposedEconomic.parameters(authored, configured, interval)
        strategy = StrategyDefinition.read(REFERENCE / "strategy.py")
        book = SimpleNamespace(components={ACROSS: None, COMPOUND: FrozenComet()})
        driver = EconomicDriver(config, configured, strategy, book)
        journal = Journal(tmp_path / name, State)
        journal.create(ComposedEconomic.initial(configured, config, interval))
        return journal, driver

    return build


def cash(state):
    return {b.key: b.amount for b in state.portfolio.balances}


def test_the_reference_bridges_then_lends_exactly_the_modeled_arrival(runner):
    journal, driver = runner()
    with preloaded_workers():
        result = Simulator(journal, driver).run()
    kinds = [(c.pending.intent.kind, c.pending.intent.target.account) for c in result.completed]
    assert kinds == [("bridge", "arb"), ("supply", "base"), ("withdraw", "base")]
    assert all(c.transition.result.success for c in result.completed)
    bridged, supplied, _ = result.completed
    (event,) = result.events
    (arrived,) = arrivals(event.transition.result)
    assert isinstance(arrived, ModeledArrival) and arrived.received.amount == ARRIVED
    assert event.time == bridged.pending.eligible_at + 20
    # The destination decision sees the credit only once the arrival is recorded.
    assert supplied.pending.decision_time >= event.time
    assert supplied.pending.intent.amount.amount == ARRIVED
    assert result.memory.values["received"] == ARRIVED
    assert cash(result)[SOURCE] == 0
    # Comet rounds a principal down, so the literal market returns one base unit less.
    assert ARRIVED - 1 <= cash(result)[DESTINATION] <= ARRIVED
    assert driver.closed(result, driver.observe(result))

    report = ComposedEconomic.report(result, driver)
    assert report["closed"] is True
    bridge = report["components"]["across"]
    (transfer,) = bridge["positions"][0]["transfers"]
    assert transfer["state"] == "arrived" and transfer["measurement_status"] == "modeled"
    assert transfer["received"]["amount"] == str(ARRIVED)
    assert bridge["bridge_model"]["arrival_latency_seconds"] == 20
    assert any("full fee ceiling charged" in line for line in bridge["assumptions"])


def test_value_in_flight_is_in_no_balance_and_no_view_can_spend_it(runner):
    journal, driver = runner(latency=300, end_policy="mark_open")
    with preloaded_workers():
        result = Simulator(journal, driver).run()
    assert [c.pending.intent.kind for c in result.completed] == ["bridge"]
    assert not result.events
    assert cash(result)[SOURCE] == 0 and cash(result)[DESTINATION] == 0
    (transfer,) = modeled_transfers(result)
    assert transfer.resolution is None and transfer.sent.amount == SENT
    owned = {o.target.allocation: o for o in ownership(result)}
    assert owned["move"].inventory.amount == 0
    assert [t.reference for t in owned["move"].in_flight] == [transfer.reference]
    assert all(t.resolution is None for t in owned["move"].in_flight)
    assert not driver.closed(result, driver.observe(result))
    report = ComposedEconomic.report(result, driver)
    (reported,) = report["components"]["across"]["positions"][0]["transfers"]
    assert reported["state"] == "in_flight" and reported["received"] is None
    assert report["closed"] is False


def test_an_arrival_is_recorded_once_across_a_replaced_driver(runner):
    journal, driver = runner(name="cut.sqlite")

    def crash(stage):
        if stage == "event_commit":
            raise RuntimeError("synthetic crash after the arrival commit")

    with preloaded_workers():
        with pytest.raises(RuntimeError, match="synthetic crash"):
            Simulator(journal, driver, fault=crash).run()
        recorded = journal.load()
        assert len(recorded.events) == 1 and recorded.events_delivered == 0
        assert cash(recorded)[DESTINATION] == ARRIVED
        view = component_state(recorded, ACROSS)
        assert isinstance(view.events[0].transition.result, ArrivalEvent)
        replacement = EconomicDriver(driver.config, driver.settings, driver.strategy, driver.book)
        result = Simulator(journal, replacement).run()
    assert len(result.events) == 1
    assert [c.pending.intent.kind for c in result.completed] == ["bridge", "supply", "withdraw"]
    assert result.completed[1].pending.intent.amount.amount == ARRIVED


def test_the_bridge_policy_is_decoded_by_its_connector_and_names_its_latency():
    configured = settings(latency=45)
    assert isinstance(configured.models[ACROSS], AcrossPolicy)
    assert configured.component(ACROSS, parameters()).data.arrival_latency_seconds == 45
    wire = json.loads(configured.model_dump_json())
    del wire["models"]["across"]["data"]
    with pytest.raises(ValueError):
        EconomicConfig.model_validate_json(json.dumps(wire))
