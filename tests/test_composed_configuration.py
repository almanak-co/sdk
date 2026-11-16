"""Configured connector targets share one fund without creating duplicate parameters."""

import json
from pathlib import Path

import pytest

from almanak.configuration.composed import Parameters
from almanak.configuration.parameter_codec import parameters_from_wire
from almanak.configuration.yaml import load
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.target_routing import (
    allocation_connector,
    connector_for_target,
    target_connector,
)
from almanak.models import Account, AccountBinding, Eoa, StrategyAccounts
from scripts.legacy_configuration.author import normalize
from tests.legacy_references import harness_directory

ROOT = Path(__file__).parents[1]


def author_wire():
    lending = load(harness_directory("aave_v3_base_lending") / "config.yaml").value
    exchange = load(harness_directory("uniswap_v3_base_trading") / "config.yaml").value
    lending.pop("execution")
    lending["strategy"].pop("connector")
    lending["strategy"]["allocations"].update(exchange["strategy"]["allocations"])
    lending["assets"].update(exchange["assets"])
    lending["markets"] = {
        name: {"connector": "aave_v3", "reserve": reserve}
        for name, reserve in lending["markets"].items()
    }
    lending["markets"].update(
        {
            name: {"connector": "uniswap_v3", **market}
            for name, market in exchange["markets"].items()
        }
    )
    return lending


def parameters():
    author = normalize(author_wire())
    fund = Account(chain=author.author_accounts["fund"].chain, address="0x" + "12" * 20)
    return Parameters(
        decision_trigger=author.decision_trigger,
        asset_metadata=author.asset_metadata,
        allocations=author.allocations,
        accounts=StrategyAccounts(
            bindings=(AccountBinding(name="fund", profile=Eoa(kind="eoa", fund=fund)),)
        ),
    )


def test_one_fund_routes_lending_and_exchange_with_retained_caip_targets():
    config = parameters()
    collateral, loan, trading = (config.target(name) for name in ("collateral", "loan", "position"))
    assert collateral.fund == loan.fund == trading.fund
    assert loan.asset == trading.funding_currency
    assert target_connector(loan) == ConnectorId("aave_v3")
    assert target_connector(trading) == ConnectorId("uniswap_v3")
    assert (
        connector_for_target(loan).codec.parameter_type
        is not connector_for_target(trading).codec.parameter_type
    )
    restored = Parameters.model_validate_json(config.model_dump_json())
    assert restored == config


def test_component_views_share_validated_allocation_instances():
    config = parameters()
    identity = ConnectorId("aave_v3")
    first = config.component(identity)
    assert config.component(identity) is first
    assert first.allocations[0] is config.allocation("collateral")
    assert first.accounts.bindings[0] is config.accounts.bindings[0]
    assert {allocation_connector(a) for a in first.allocations} == {identity}
    with pytest.raises(ValueError, match="validated connector"):
        config.component("aave_v3")
    with pytest.raises(ValueError, match="no configured"):
        config.component(ConnectorId("uniswap_v4"))


def test_author_wire_roundtrip_keeps_all_markets_without_execution_authority():
    author = normalize(author_wire())
    restored = parameters_from_wire(json.loads(author.model_dump_json()))
    assert restored == author
    assert author.connector is None and author.execution == {}


@pytest.mark.parametrize("mutation", ["connector", "chain", "account", "market"])
def test_market_discriminators_and_account_scope_cannot_be_substituted(mutation):
    wire = author_wire()
    if mutation == "connector":
        wire["markets"]["pool"]["connector"] = "aave_v3"
    elif mutation == "chain":
        wire["accounts"]["fund"]["chain"]["chain_id"] = "eip155:1"
    elif mutation == "account":
        wire["strategy"]["allocations"]["position"]["account"] = "unbound"
    else:
        wire["strategy"]["allocations"]["position"]["market"] = "unbound"
    with pytest.raises(ValueError):
        normalize(wire)


def test_untyped_target_cannot_select_an_adapter():
    with pytest.raises(ValueError, match="trusted connector"):
        target_connector({"connector": "aave_v3"})


def test_names_are_unique_across_connectors():
    config = parameters()
    allocations = (
        *config.allocations[:-1],
        config.allocations[-1].model_copy(update={"name": "loan"}),
    )
    with pytest.raises(ValueError, match="unique across connectors"):
        Parameters.model_validate({**config.model_dump(), "allocations": allocations})
