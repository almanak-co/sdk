"""The shipped bridge backtest files through the real backtest input path, offline.

Market resolution needs a provider, so the parameters are typed here with the shipped
account names. Everything after resolution is the real path: the file is read, its
settings are built for the connector the parameters name, and the run's initial state
and the bridge's dataset are prepared from them.
"""

import json
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
import yaml

from almanak.backtest.config import HistoryBinding
from almanak.configuration.backtest_inputs import read_backtest_inputs, resolve_backtest
from almanak.configuration.unresolved import read_inputs
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.across.economic_binding import AcrossEconomic
from almanak.connectors.across.economic_prepare import Book as AcrossBook
from almanak.connectors.across.models import Parameters
from almanak.connectors.composed.economic_binding import ComposedEconomic
from almanak.connectors.composed.economic_config import AcrossPolicy
from almanak.connectors.composed.economic_prepare import component_args, native_preparation
from almanak.connectors.composed.models import OwnedParameters
from almanak.connectors.compound_v3.domain import Market, Reserve
from almanak.connectors.compound_v3.models import Allocation as CompoundAllocation
from almanak.connectors.identifiers import ConnectorId
from almanak.effects import Balance, WalletAsset
from almanak.gateway.evm import native_asset
from almanak.models import (
    Account,
    AccountBinding,
    AssetMetadata,
    SimulationAccount,
    StrategyAccounts,
)
from almanak.triggers import IntervalSelection, IntervalTrigger
from tests.across_support import ARBITRUM, BASE, BASE_USDC, RECIPIENT, SENDER, USDC, allocation

ROOT = Path(__file__).parents[1]
STANDALONE = ROOT / "strategies/across_usdc_arbitrum_base_bridge"
COMPOSED = ROOT / "strategies/arbitrum_base_bridge_and_lend"
ACROSS = ConnectorId("across")
SENT = 100_000_000
ETH, BASE_ETH = native_asset(ARBITRUM.chain_id), native_asset(BASE.chain_id)


def simulated(name, chain, address):
    fund = Account(chain=chain, address=address)
    return AccountBinding(name=name, profile=SimulationAccount(fund=fund))


def bridge_parameters():
    """The standalone reference's parameters as resolution would state them."""
    return Parameters(
        decision_trigger=IntervalTrigger(kind="interval", seconds=60),
        asset_metadata={
            USDC: AssetMetadata(decimals=6, symbol="USDC"),
            BASE_USDC: AssetMetadata(decimals=6, symbol="USDC"),
            ETH: AssetMetadata(decimals=18, symbol="ETH"),
            BASE_ETH: AssetMetadata(decimals=18, symbol="ETH"),
        },
        parameters={"bridge_quantity": "100"},
        accounts=StrategyAccounts(
            bindings=(
                simulated("arbitrum_fund", ARBITRUM, SENDER),
                simulated("base_fund", BASE, RECIPIENT),
            )
        ),
        allocations=(
            allocation(
                account="arbitrum_fund",
                destination_account="base_fund",
                capital=SENT,
                minimum_native_balance=10**14,
            ),
        ),
    )


def composed_parameters():
    bridge = bridge_parameters()
    market = Market(
        chain=BASE, comet="0x" + "c0" * 20, base_asset=BASE_USDC, base_price_feed="0x" + "fe" * 20
    )
    earn = CompoundAllocation(
        name="earn",
        account="base_fund",
        reserve=Reserve(market=market, asset=BASE_USDC, price_feed="0x" + "fe" * 20),
        native_asset=BASE_ETH,
        minimum_native_balance=0,
    )

    def cash(account, chain, asset, amount):
        return Balance(key=WalletAsset(account=account, chain=chain, asset=asset), amount=amount)

    return OwnedParameters(
        decision_trigger=bridge.decision_trigger,
        asset_metadata=bridge.asset_metadata,
        parameters=bridge.parameters,
        accounts=bridge.accounts,
        allocations=(*bridge.allocations, earn),
        inventory=(
            cash("arbitrum_fund", ARBITRUM, USDC, SENT),
            cash("base_fund", BASE, BASE_USDC, 0),
        ),
    )


def history():
    def binding(chain, network):
        provider = {"kind": "alchemy", "network": network, "api_key_env": "UNUSED"}
        wire = {"chain": chain.model_dump(mode="json"), "provider": provider}
        return HistoryBinding.model_validate_json(json.dumps(wire))

    return (binding(ARBITRUM, "arb-mainnet"), binding(BASE, "base-mainnet"))


def changed(tmp_path, reference, change):
    raw = yaml.safe_load((reference / "backtest-economic.yaml").read_text())
    change(raw)
    path = tmp_path / "backtest-economic.yaml"
    path.write_text(yaml.safe_dump(raw))
    return read_backtest_inputs(path)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    def forbid(*args, **kwargs):
        raise AssertionError("the backtest files are read offline")

    monkeypatch.setattr(httpx.Client, "send", forbid)


@pytest.mark.parametrize("reference", [STANDALONE, COMPOSED])
def test_the_shipped_file_names_the_authors_accounts_and_anchors_both_chains(reference):
    author = read_inputs(reference / "config.yaml").inputs
    document = read_backtest_inputs(reference / "backtest-economic.yaml")
    document.require_accounts(author)
    anchors = document.inputs.anchors(author)
    assert [a.chain for a in anchors] == [ARBITRUM.chain_id, BASE.chain_id]
    assert {a.at for a in anchors} == {document.inputs.window.start}
    assert document.inputs.window.cleanup > 30


def test_the_standalone_file_resolves_to_the_bridge_settings_and_initial_state():
    parameters = bridge_parameters()
    document = read_backtest_inputs(STANDALONE / "backtest-economic.yaml")
    settings = resolve_backtest(document, parameters, history())
    assert isinstance(settings, AcrossEconomic.settings_type)
    assert settings.data.arrival_latency_seconds == 30
    assert settings.bridge_model == "full_fee_ceiling_fixed_latency"
    assert settings.costs.kind == "excluded"
    assert settings.schedule.cleanup_seconds > settings.data.arrival_latency_seconds
    interval = IntervalSelection(configured=parameters.decision_trigger)
    config = AcrossEconomic.parameters(parameters, settings, interval)
    state = AcrossEconomic.initial(settings, config, interval)
    cash = {(b.key.account, b.key.asset): b.amount for b in state.portfolio.balances}
    assert cash[("arbitrum_fund", USDC)] == SENT and cash[("base_fund", BASE_USDC)] == 0
    assert cash[("arbitrum_fund", ETH)] == 10**18


def test_the_composed_file_resolves_to_one_policy_per_connector_and_initial_state():
    parameters = composed_parameters()
    document = read_backtest_inputs(COMPOSED / "backtest-economic.yaml")
    settings = resolve_backtest(document, parameters, history())
    assert isinstance(settings, ComposedEconomic.settings_type)
    assert set(settings.models) == {ACROSS, ConnectorId("compound_v3")}
    policy = settings.models[ACROSS]
    assert isinstance(policy, AcrossPolicy) and policy.data.arrival_latency_seconds == 30
    interval = IntervalSelection(configured=parameters.decision_trigger)
    config = ComposedEconomic.parameters(parameters, settings, interval)
    state = ComposedEconomic.initial(settings, config, interval)
    assert len(state.portfolio.positions) == 2
    cash = {(b.key.account, b.key.asset): b.amount for b in state.portfolio.balances}
    assert cash[("arbitrum_fund", USDC)] == SENT and cash[("base_fund", BASE_USDC)] == 0


def drop(*path):
    def change(raw):
        node = raw
        for key in path[:-1]:
            node = node[key]
        del node[path[-1]]

    return change


def put(value, *path):
    def change(raw):
        node = raw
        for key in path[:-1]:
            node = node[key]
        node[path[-1]] = value

    return change


@pytest.mark.parametrize(
    ("change", "field", "reason"),
    [
        (drop("data", "arrival_latency_seconds"), "data.arrival_latency_seconds", "required"),
        (drop("data"), "data.arrival_latency_seconds", "required"),
        (put("0s", "data", "arrival_latency_seconds"), "data.arrival_latency_seconds", "greater"),
        (
            put("3600s", "data", "arrival_latency_seconds"),
            "data",
            "below the modeled fill window",
        ),
        (
            drop("execution", "models", "bridge_model"),
            "execution.models.bridge_model",
            "required",
        ),
        (
            put("measured", "execution", "models", "bridge_model"),
            "execution.models.bridge_model",
            "full_fee_ceiling_fixed_latency",
        ),
    ],
)
def test_a_standalone_refusal_names_the_field_as_the_author_wrote_it(
    tmp_path, change, field, reason
):
    document = changed(tmp_path, STANDALONE, change)
    with pytest.raises(ConfigurationError, match=reason) as refused:
        resolve_backtest(document, bridge_parameters(), history())
    assert refused.value.field == field


@pytest.mark.parametrize(
    ("change", "field"),
    [
        (
            drop("data", "connectors", "across", "arrival_latency_seconds"),
            "data.connectors.across.arrival_latency_seconds",
        ),
        (
            drop("execution", "models", "across", "bridge_model"),
            "execution.models.across.bridge_model",
        ),
        (drop("costs", "across", "kind"), "costs.across"),
    ],
)
def test_a_composed_refusal_names_the_connectors_field(tmp_path, change, field):
    document = changed(tmp_path, COMPOSED, change)
    with pytest.raises(ConfigurationError) as refused:
        resolve_backtest(document, composed_parameters(), history())
    assert refused.value.field == field


def test_composed_preparation_prepares_and_opens_the_bridge_dataset_offline(tmp_path):
    parameters = composed_parameters()
    document = read_backtest_inputs(COMPOSED / "backtest-economic.yaml")
    settings = resolve_backtest(document, parameters, history())
    interval = IntervalSelection(configured=parameters.decision_trigger)
    config = ComposedEconomic.parameters(parameters, settings, interval)
    args = SimpleNamespace(
        config=COMPOSED / "config.yaml",
        backtest_config=COMPOSED / "backtest-economic.yaml",
        strategy=COMPOSED / "strategy.py",
        output=tmp_path / "dataset",
    )
    child = component_args(args, config, settings, ACROSS)
    child.output = tmp_path / "dataset" / ACROSS.value
    native, policy, _ = child._backtest_inputs
    preparation = native_preparation(ACROSS)
    preparation.validate_sources(child, native, policy)
    report = preparation.prepare(child)
    assert report["cache_state"] == "modeled_without_acquisition"
    # The composed book opens each component with its own parameters and policy.
    component = config.parameter_components[ACROSS]
    book = AcrossBook(child.output, component, settings.component(ACROSS, component))
    assert book.manifest.inputs == "none"
    book.close()
