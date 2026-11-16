"""Public composed inputs rebuild the frozen shared parameters without provider reads."""

import json
from pathlib import Path

import pytest

from almanak.configuration.parameters import assemble, resolved_inventory
from almanak.configuration.yaml import ConfigurationError
from almanak.connectors.identifiers import ConnectorId
from almanak.connectors.target_routing import allocation_connector
from tests.composed_public import resolved_parameters
from tests.test_reference_conversion import legacy_parameters, legacy_snapshot, reviewed_holds

REFERENCES = (
    "base_borrow_and_trade",
    "arbitrum_aave_borrow_and_trade_gmx",
    "bnb_morpho_curve_borrow_and_trade",
    "ethereum_aave_v4_borrow_and_trade",
    "ethereum_base_borrow_and_trade",
)


def assembled(name: str):
    document, actual = resolved_parameters(name)
    return document, actual, legacy_parameters(legacy_snapshot(name))


@pytest.mark.parametrize("name", REFERENCES)
def test_composed_parameters_preserve_positions_inventory_and_component_identity(name):
    _, actual, expected = assembled(name)
    assert reviewed_holds(name, {item.name: item for item in actual.positions}) == reviewed_holds(
        name, {item.name: item for item in expected.positions}
    )
    assert actual.inventory == expected.inventory
    assert actual.decision_trigger == expected.decision_trigger
    assert actual.accounts == expected.accounts
    assert actual.asset_metadata == expected.asset_metadata
    assert set(actual.parameter_components) == {
        allocation_connector(item) for item in actual.positions
    }
    for identity, component in actual.parameter_components.items():
        assert all(allocation_connector(item) == identity for item in component.positions)


def test_shared_fund_cash_is_not_duplicated_between_lending_and_trading():
    _, actual, _ = assembled("base_borrow_and_trade")
    loan = actual.target("loan")
    trade = actual.target("trade")
    assert loan.account == trade.account
    matching = [
        balance
        for balance in actual.inventory
        if balance.key.account == loan.account and balance.key.asset == loan.asset
    ]
    assert len(matching) == 1


def test_multichain_parameters_keep_accounts_positions_and_cash_chain_scoped():
    _, actual, _ = assembled("ethereum_base_borrow_and_trade")
    chains = {binding.profile.fund.chain.chain_id for binding in actual.accounts.bindings}
    assert chains == {balance.key.chain.chain_id for balance in actual.inventory}
    assert chains == {item.chain.chain_id for item in actual.positions}
    for binding in actual.accounts.bindings:
        chain = binding.profile.fund.chain
        assert all(
            balance.key.chain == chain
            for balance in actual.inventory
            if balance.key.account == binding.name
        )


def test_composed_inventory_remains_exact_and_unique():
    _, actual, _ = assembled("base_borrow_and_trade")
    raw = actual.model_dump(mode="json")
    raw["inventory"] = raw["inventory"][:-1]
    with pytest.raises(ValueError, match="inventory"):
        type(actual).model_validate(raw)
    raw = actual.model_dump(mode="json")
    raw["inventory"].append(raw["inventory"][0])
    with pytest.raises(ValueError, match="inventory"):
        type(actual).model_validate(raw)


def test_aave_v4_borrowing_converts_to_a_debt_position():
    document, actual, _ = assembled("ethereum_aave_v4_borrow_and_trade")
    assert tuple(document.inputs.strategy.debts) == ("loan",)
    assert actual.component(ConnectorId("aave_v4")).allocation("loan") is actual.allocation("loan")
    assert actual.allocation("loan").collateral == ("collateral",)


def test_inventory_precision_failure_points_to_the_authored_inventory_item():
    document, actual, _ = assembled("base_borrow_and_trade")
    item = document.inputs.inventory[0]
    changed = item.model_copy(
        update={"balance": item.balance.model_copy(update={"quantity": "0.0000000000000000001"})}
    )
    inputs = document.inputs.model_copy(
        update={"inventory": (changed, *document.inputs.inventory[1:])}
    )
    source = document.source
    document = type(document)(inputs=inputs, source=source)

    with pytest.raises(ConfigurationError) as caught:
        resolved_inventory(document, actual.accounts, actual.asset_metadata)
    assert caught.value.field == "inventory.0"
    assert "18 decimal places" in str(caught.value)


def test_single_connector_parameters_refuse_unused_shared_inventory():
    document, actual, _ = assembled("base_borrow_and_trade")
    item = actual.positions[0]
    groups = {allocation_connector(item): {"allocations": [item], "debts": []}}

    with pytest.raises(ConfigurationError, match="inventory requires positions") as caught:
        assemble(document, groups, actual.accounts, actual.asset_metadata, ())
    assert caught.value.field == "inventory"


def test_composed_parameters_refuse_duplicate_position_names():
    _, actual, _ = assembled("base_borrow_and_trade")
    raw = actual.model_dump(mode="json")
    raw["allocations"].append(raw["allocations"][0])
    with pytest.raises(ValueError, match="allocation names must be unique"):
        type(actual).model_validate_json(json.dumps(raw))


def test_all_composed_operator_examples_are_fork_only_and_strategy_owned():
    from scripts.ci.check_strategies import COMPOSED_REFERENCES

    root = Path(__file__).parents[1]
    for name in COMPOSED_REFERENCES:
        assert (root / "examples/operators" / f"{name}.fork.yaml").is_file()
        assert not (root / "examples/operators" / f"{name}.production.yaml").exists()
