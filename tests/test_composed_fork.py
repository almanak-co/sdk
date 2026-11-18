"""Fixture bindings preserve explicit shared cash independently of trading ceilings."""

import pytest

from almanak.configuration.yaml import load
from almanak.connectors.composed.fork import ComposedFork, ForkConfig
from almanak.connectors.composed.models import inventory_keys
from almanak.effects import Balance
from scripts.legacy_configuration.author import normalize, read_author
from tests.legacy_references import harness_directory
from tests.test_composed_configuration import author_wire, parameters
from tests.test_composed_inventory import config as config


def fixture_config():
    source = read_author(harness_directory("aave_v3_base_lending") / "config.yaml").fork_config()
    params = parameters()
    funds = {a.balance.asset: a.balance.amount for a in source.funds[0].assets}
    return ForkConfig(
        decision_trigger=params.decision_trigger,
        asset_metadata=params.asset_metadata,
        allocations=params.allocations,
        forks=source.forks,
        funds=source.funds,
        operators=source.operators,
        inventory=tuple(
            Balance(key=key, amount=funds.get(key.asset, 0))
            for key in sorted(inventory_keys(params), key=str)
        ),
    )


def test_fork_remaps_environment_and_preserves_owned_cash(config):
    fixture = fixture_config()
    restored = ForkConfig.model_validate_json(fixture.model_dump_json())
    assert restored == fixture
    prepared = ComposedFork.prepare_chains(fixture, config.accounts.bindings, config.fork_bindings)
    assert prepared.allocations == config.allocations
    assert {b.key.asset: b.amount for b in prepared.inventory} == {
        b.key.asset: b.amount for b in fixture.inventory
    }
    assert {b.key.chain for b in prepared.inventory} == {config.fork_bindings[0].chain}
    assert prepared.allocation("position").capital.amount > next(
        b.amount for b in prepared.inventory if b.key.asset == prepared.target("loan").asset
    )


def test_fork_rejects_unfunded_owned_cash(config):
    fixture = fixture_config()
    first, *rest = fixture.inventory
    with pytest.raises(ValueError, match="funding must cover"):
        ForkConfig.model_validate(
            {
                **fixture.model_dump(),
                "inventory": (
                    first.model_copy(update={"amount": first.amount + 1}),
                    *rest,
                ),
            }
        )


def test_fork_rejects_missing_and_duplicate_inventory(config):
    fixture = fixture_config()
    for inventory in (fixture.inventory[:1], (*fixture.inventory, fixture.inventory[0])):
        with pytest.raises(ValueError):
            ForkConfig.model_validate({**fixture.model_dump(), "inventory": inventory})


def test_author_configuration_requires_explicit_inventory_for_execution(config):
    wire = author_wire()
    wire["execution"] = load(harness_directory("aave_v3_base_lending") / "config.yaml").value[
        "execution"
    ]
    with pytest.raises(ValueError, match="inventory must cover"):
        normalize(wire)
    wire["inventory"] = [
        {"account": fund["account"], "balance": asset["balance"]}
        for fund in wire["execution"]["fork"]["funds"]
        for asset in fund["assets"]
    ]
    author = normalize(wire)
    assert author.fork_config() == fixture_config()
    assert sum(b.amount for b in author.inventory) > 0
