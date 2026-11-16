"""V3 author inputs and sandbox requests use the trusted composition boundary."""

from datetime import UTC, datetime

import pytest

from almanak.caip import ChainId
from almanak.configuration.parameter_codec import validation_parameters
from almanak.connectors.aave_v3.models import Config, Parameters
from almanak.connectors.aave_v3.state import DecisionInput
from almanak.connectors.composition import connector_for, connector_from_wire
from almanak.connectors.support import RunMode
from almanak.gateway.evm import native_asset
from almanak.runtime import strategy_capabilities, strategy_decision, validate_strategy_config
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import Memory
from almanak.triggers import DecisionSchedule
from scripts.legacy_configuration.author import normalize
from tests.test_aave_v3_config import parameters as parameters


def author_wire(parameters):
    chain = parameters.allocations[0].chain
    assets = {str(native_asset(chain.chain_id)): {"decimals": 18}}
    markets, allocations = {}, {}
    for allocation in parameters.allocations:
        wire = allocation.model_dump(mode="json")
        markets[allocation.name] = wire.pop("reserve")
        decimals = 18 if allocation.name == "collateral" else 6
        assets[str(allocation.reserve.asset)] = {"decimals": decimals}
        wire["capital"] = {"base_units": wire["capital"]}
        wire.update(minimum_health_factor="2", minimum_native_balance="0")
        wire.pop("name")
        allocations[allocation.name] = {"market": allocation.name, **wire}
    return {
        "strategy": {
            "connector": "aave_v3",
            "decision_interval": "1m",
            "allocations": allocations,
        },
        "assets": assets,
        "markets": markets,
        "accounts": {"fund": {"chain": chain.model_dump(mode="json")}},
    }


def test_author_resolves_exact_quantities_and_explicit_sources(parameters):
    wire = author_wire(parameters)
    wire["strategy"]["allocations"]["collateral"].update(capital="1")
    author = normalize(wire)
    assert author.allocations == parameters.allocations
    assert author.decision_trigger == parameters.decision_trigger
    assert validation_parameters(parameters).allocations == author.allocations
    assert connector_for(author).codec.parameter_type == Config | Parameters
    assert author.execution == {}


@pytest.mark.parametrize("fault", ["asset", "pool", "precision", "source"])
def test_author_rejects_missing_identity_precision_and_price_source(parameters, fault):
    wire = author_wire(parameters)
    if fault == "asset":
        wire["assets"].pop(str(parameters.allocations[1].reserve.asset))
    elif fault == "pool":
        wire["markets"]["loan"].pop("pool")
    elif fault == "precision":
        wire["strategy"]["allocations"]["loan"]["capital"] = "1.0000001"
    else:
        wire["strategy"]["allocations"]["loan"].pop("price_source")
    with pytest.raises(ValueError):
        normalize(wire)


def test_registered_wire_binding_exposes_only_verified_modes():
    connector = connector_from_wire({"connector": "aave_v3"})
    anchors = (
        ChainId("eip155:1"),
        ChainId("eip155:8453"),
        ChainId("eip155:42161"),
        ChainId("eip155:10"),
        ChainId("eip155:56"),
        ChainId("eip155:137"),
        ChainId("eip155:43114"),
    )
    assert connector.supported_chains(RunMode.FORK) == anchors
    assert connector.supported_chains(RunMode.BACKTEST) == anchors
    # All seven chains declare production under the scoped-promotion amendment.
    assert connector.supported_chains(RunMode.PRODUCTION) == anchors
    with pytest.raises(ValueError, match="does not support"):
        connector.support_for(ChainId("eip155:31337"))
    assert connector.backtest("economic").state_type is not None


def test_sandbox_binds_v3_authority_and_decision_to_configured_fund(parameters):
    definition = StrategyDefinition(
        filename="strategy.py",
        source=(
            "from almanak.author_capabilities import declared\n"
            "def capabilities(config):\n"
            "    return declared(config)\n"
            "def validate_config(config):\n"
            "    assert config.allocation('loan').borrow\n"
            "def decide(ctx):\n"
            "    return ctx.decision(ctx.actions.supply('collateral', 123))\n"
        ),
    )
    validate_strategy_config(definition, parameters)
    capabilities = strategy_capabilities(definition, parameters)
    context = DecisionInput(
        config=parameters,
        memory=Memory(),
        owned=(),
        observations=(),
        trigger=DecisionSchedule().event(datetime(2026, 9, 7, tzinfo=UTC)),
    )
    decision = strategy_decision(definition, context)
    assert decision.intent == parameters.supply("collateral", 123)
    connector_for(parameters).codec.capability.admit(capabilities, decision.intent)
