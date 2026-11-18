"""V3 dispatch admits only verified chain modes and typed author hooks."""

import pytest

from almanak.caip import ChainId
from almanak.connectors.composition import connector_for, connector_from_wire
from almanak.connectors.support import RunMode
from almanak.connectors.uniswap_v3.state import DecisionInput
from almanak.runtime import strategy_capabilities, strategy_decision, validate_strategy_config
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import Memory
from almanak.triggers import DecisionSchedule
from scripts.legacy_configuration.author import normalize
from tests.test_uniswap_v3_validation import state_for


def author_wire(config, observed):
    allocation = config.allocations[0]
    wire = allocation.model_dump(mode="json")
    market = {"pool": wire.pop("pool"), "contracts": wire.pop("contracts")}
    wire.pop("name")
    capital = wire["capital"]
    wire["capital"] = {"asset": capital["asset"], "quantity": {"base_units": capital["amount"]}}
    wire["minimum_native_balance"] = {"base_units": wire["minimum_native_balance"]}
    assets = {
        str(observed.pool.currency0.identifier): {"decimals": observed.pool.currency0.decimals},
        str(observed.pool.currency1.identifier): {"decimals": observed.pool.currency1.decimals},
        str(allocation.native_asset): {"decimals": 18},
    }
    return {
        "strategy": {
            "connector": "uniswap_v3",
            "decision_interval": f"{config.decision_trigger.seconds}s",
            "allocations": {allocation.name: {"market": "pool", **wire}},
        },
        "assets": assets,
        "markets": {"pool": market},
        "accounts": {allocation.account: {"chain": allocation.chain.model_dump(mode="json")}},
    }


@pytest.mark.parametrize("index", [0, 1])
def test_named_author_inputs_resolve_exact_market_and_amounts(index):
    state, before, _ = state_for(chain=index)
    author = normalize(author_wire(state.config, before))
    assert author.allocations == state.config.allocations
    assert connector_for(author).codec.config.model_fields["connector"].default == "uniswap_v3"


def test_dispatch_exposes_verified_chains_and_priority_production_candidates():
    connector = connector_from_wire({"connector": "uniswap_v3"})
    for mode in (RunMode.FORK, RunMode.PRODUCTION, RunMode.BACKTEST):
        assert connector.supported_chains(mode) == (
            ChainId("eip155:1"),
            ChainId("eip155:8453"),
            ChainId("eip155:42161"),
            ChainId("eip155:10"),
            ChainId("eip155:137"),
            ChainId("eip155:43114"),
            ChainId("eip155:56"),
            ChainId("eip155:4663"),
        )
    assert connector.support_for(ChainId("eip155:56")).modes == (
        RunMode.FORK,
        RunMode.PRODUCTION,
        RunMode.BACKTEST,
    )
    with pytest.raises(ValueError, match="does not support"):
        connector.support_for(ChainId("eip155:999999"))
    assert connector.economic is not None
    assert connector.permissions is not None
    with pytest.raises(ValueError, match="no historical_path"):
        connector.backtest("historical_path")


def test_sandboxed_authority_and_actions_retain_the_observed_v3_target():
    state, before, expected = state_for()
    definition = StrategyDefinition(
        filename="strategy.py",
        source=(
            "from almanak.author_capabilities import declared\n"
            "def capabilities(config):\n"
            "    return declared(config)\n"
            "def validate_config(config):\n"
            "    assert config.connector == 'uniswap_v3'\n"
            "def decide(ctx):\n"
            "    return ctx.decision(ctx.actions.swap(ctx.config.allocations[0].name))\n"
        ),
    )
    validate_strategy_config(definition, state.config)
    capabilities = strategy_capabilities(definition, state.config)
    context = DecisionInput(
        config=state.config,
        memory=Memory(),
        owned=state.owned,
        observations=(before,),
        trigger=DecisionSchedule().event(before.effective_at),
    )
    decision = strategy_decision(definition, context)
    assert decision.intent == expected
    connector_for(state.config).codec.capability.admit(capabilities, decision.intent)
