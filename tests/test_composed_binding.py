"""The real sandbox chooses native actions from one composed parameter instance."""

from almanak.configuration.composed import Parameters
from almanak.connectors.aave_v3.fork import fork_allocation
from almanak.connectors.composed.models import OwnedParameters, inventory_keys
from almanak.connectors.composed.state import DecisionInput
from almanak.connectors.composition import checkpoint_from_json, connector_for
from almanak.connectors.target_routing import target_connector
from almanak.effects import Balance
from almanak.runtime import strategy_capabilities, strategy_decision, validate_strategy_config
from almanak.strategy import StrategyDefinition
from almanak.strategy_api import Memory
from almanak.triggers import DecisionSchedule
from tests.test_composed_configuration import parameters
from tests.test_composed_inventory import checkpoint
from tests.test_composed_inventory import config as config
from tests.test_uniswap_v3_validation import state_for


def test_root_checkpoint_dispatch_keeps_shared_cash(config):
    state = checkpoint(config)
    assert checkpoint_from_json(state.model_dump_json()) == state
    assert connector_for(config).codec.checkpoint is type(state)


def test_one_sandbox_hook_routes_lending_and_exchange_actions():
    exchange, observed, _ = state_for(chain=1)
    source = parameters()
    scoped = Parameters(
        decision_trigger=source.decision_trigger,
        asset_metadata=source.asset_metadata,
        accounts=exchange.config.accounts,
        allocations=(
            *(fork_allocation(a, observed.target.fund.chain) for a in source.allocations[:2]),
            exchange.config.allocations[0],
        ),
    )
    params = OwnedParameters(
        **{name: getattr(scoped, name) for name in Parameters.model_fields},
        inventory=tuple(
            Balance(key=key, amount=0) for key in sorted(inventory_keys(scoped), key=str)
        ),
    )
    definition = StrategyDefinition(
        filename="strategy.py",
        source=(
            "from almanak.author_capabilities import declared\n"
            "def capabilities(config):\n"
            "    return declared(config)\n"
            "def validate_config(config):\n"
            "    assert config.connector is None\n"
            "    assert len(config.allocations) == 3\n"
            "    assert config.accounts.bindings\n"
            "    assert config.inventory and all(b.amount == 0 for b in config.inventory)\n"
            "def decide(ctx):\n"
            "    if ctx.memory.values.get('trade'):\n"
            "        return ctx.decision(ctx.actions.swap('position'))\n"
            "    return ctx.decision(ctx.actions.supply('collateral', 1))\n"
        ),
    )
    validate_strategy_config(definition, params)
    capabilities = strategy_capabilities(definition, params)
    context = DecisionInput(
        config=params,
        memory=Memory(),
        owned=(observed.owned,),
        observations=(observed,),
        trigger=DecisionSchedule().event(observed.effective_at),
    )
    lending = strategy_decision(definition, context)
    trading = strategy_decision(
        definition, context.model_copy(update={"memory": Memory(values={"trade": True})})
    )
    assert str(target_connector(lending.intent.target)) == "aave_v3"
    assert str(target_connector(trading.intent.target)) == "uniswap_v3"
    assert trading.required_observations[0].allocation == "position"
    codec = connector_for(params).codec.capability
    codec.admit(capabilities, lending.intent)
    codec.admit(capabilities, trading.intent)
