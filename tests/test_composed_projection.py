"""Shared dashboard cash and report costs are not multiplied by connector count."""

import json

import pytest

from almanak.connectors.composed.authority import declared
from almanak.connectors.composed.dashboard import add_values, project
from almanak.connectors.composed.inventory import initial_owned
from almanak.connectors.composed.models import Config
from almanak.connectors.composed.reporting import report
from almanak.connectors.composed.state import Checkpoint
from almanak.dashboard.models import Context, ExchangePosition
from almanak.models import Block
from almanak.strategy import StrategyDefinition
from almanak.triggers import DecisionSchedule
from tests.composed_public import resolved_parameters
from tests.test_composed_inventory import checkpoint
from tests.test_composed_inventory import config as config


def test_unobserved_report_keeps_initial_cash_and_missing_gas(config):
    state = checkpoint(config)
    result = report(state)
    assert len(result["inventory"]) == 2
    assert len(result["positions"]) == 3
    assert len(result["gas_payers"]) == 1
    assert result["gas_payers"][0]["gas_paid"]["amount"] is None
    assert all(b["evidence"] == "configured" for b in result["inventory"])
    assert all(p["last_observation"] is None for p in result["positions"])


def test_dashboard_exchange_uses_shared_fund_values(config):
    state = checkpoint(config)
    context, snapshot = project(state, captured_at=1788734000)
    restored = Context.model_validate_json(context.model_dump_json())
    assert restored == context
    assert len(context.datasets["fund_inventory"].rows) == 2
    assert len(context.datasets["payer_costs"].rows) == 1
    exchange = next(p for p in context.positions if isinstance(p, ExchangePosition))
    for value in exchange.inventory:
        assert context.values[value.id] == snapshot.values[value.id] == value
        assert value.scope.target is None
        assert value.scope.account == exchange.target.account
        assert value.evidence == "configured"
    assert len([k for k in context.values if k.startswith("fund-cash:")]) == 2


def test_duplicate_dashboard_value_names_cannot_overwrite_evidence():
    with pytest.raises(ValueError, match="collide"):
        add_values({"same": "retained"}, {"same": "replacement"})


def aave_v4_checkpoint(name):
    _, parameters = resolved_parameters(name)
    instance = "0x" + "34" * 32
    wire = json.loads(parameters.model_dump_json())

    def identify_fork(value):
        if isinstance(value, dict):
            if "chain_id" in value:
                value["environment"] = "anvil:" + instance
            for child in value.values():
                identify_fork(child)
        elif isinstance(value, list):
            for child in value:
                identify_fork(child)

    identify_fork(wire)
    scoped = type(parameters).model_validate_json(json.dumps(wire))
    chain = scoped.accounts.bindings[0].profile.fund.chain
    origin = Block(chain=chain, number=1, hash="0x" + "56" * 32, timestamp=1)
    config = Config(
        **{field: getattr(scoped, field) for field in type(scoped).model_fields},
        fork_bindings=({"chain": chain, "origin": origin, "instance": instance},),
    )
    return Checkpoint(
        config=config,
        strategy=StrategyDefinition(filename="strategy.py", source=""),
        capabilities=declared(config),
        permissions=None,
        schedule=DecisionSchedule(),
        deployment_id=name,
        mode="running",
        initialized=False,
        inventory=config.inventory,
        owned=initial_owned(config),
    )


@pytest.mark.parametrize(
    "name", ("ethereum_aave_v4_borrow_and_trade", "avalanche_aave_v4_borrow_and_trade")
)
def test_aave_v4_composed_dashboard_projects_typed_component_scope(name):
    state = Checkpoint.model_validate_json(aave_v4_checkpoint(name).model_dump_json())
    context, snapshot = project(state, captured_at=1)
    assert {position.allocation for position in context.positions} == {
        "collateral",
        "loan",
        "trade",
    }
    for allocation in ("collateral", "loan"):
        suffix = "observed" if allocation == "collateral" else "debt"
        value = context.values[f"{allocation}-{suffix}"]
        target = state.config.target(allocation)
        assert value.amount is None and value.reason
        assert value.asset == target.asset
        assert value.scope.account == target.account
        assert value.scope.fund == target.fund
        assert value.scope.target == target
        assert snapshot.values[value.id] == value
