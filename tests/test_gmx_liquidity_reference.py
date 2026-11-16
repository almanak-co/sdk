"""The actual reusable GM author hooks wait for keeper delivery and exit their retained shares."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

from almanak.connectors.gmx_v2.api import Actions
from tests.gmx_native_runtime import gm_completed as gm_completed


def reference(chain):
    path = Path(__file__).parents[1] / "strategies" / f"gmx_v2_{chain}_liquidity/strategy.py"
    spec = importlib.util.spec_from_file_location("gmx_liquidity_reference", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_reusable_gm_reference_advances_only_complete_keeper_outcomes(completed):
    chain = (
        "arbitrum"
        if str(completed.config.allocations[0].chain.chain_id) == "eip155:42161"
        else "avalanche"
    )
    strategy = reference(chain)
    context = SimpleNamespace(
        config=completed.config,
        memory=strategy.State(),
        teardown=False,
        require_observation=lambda name: observed,
    )
    context.actions = Actions(context)
    allocation = completed.config.allocations[0]
    observed = completed.baseline[0]
    intent = strategy.allocation_intent(context, allocation, observed)
    assert intent.kind == "open_liquidity" and intent.maximum0 == allocation.entry_amounts.root[0]
    for outcome in completed.outcomes[:3]:
        context.memory = strategy.on_outcome(
            SimpleNamespace(memory=context.memory, result=outcome.strategy_result())
        )
    assert context.memory.settled == {}
    context.memory = strategy.on_outcome(
        SimpleNamespace(memory=context.memory, result=completed.outcomes[3].strategy_result())
    )
    observed = completed.outcomes[3].after
    intent = strategy.allocation_intent(context, allocation, observed)
    assert (
        intent.kind == "increase_liquidity"
        and intent.maximum1 == allocation.additional_amounts.root[1]
    )
    context.memory = strategy.on_outcome(
        SimpleNamespace(memory=context.memory, result=completed.outcomes[5].strategy_result())
    )
    observed = completed.outcomes[5].after
    intent = strategy.allocation_intent(context, allocation, observed)
    assert (
        intent.kind == "decrease_liquidity"
        and intent.liquidity == allocation.partial_withdrawal.amount
    )
    context.memory = strategy.on_outcome(
        SimpleNamespace(memory=context.memory, result=completed.outcomes[8].strategy_result())
    )
    observed = completed.outcomes[8].after
    assert context.memory.held_at == {"Main": observed.block.timestamp}
    assert strategy.allocation_intent(context, allocation, observed) is None
    context.teardown = True
    assert strategy.allocation_intent(context, allocation, observed).kind == "close_liquidity"
    observed = completed.closure[0]
    assert strategy.allocation_intent(context, allocation, observed) is None
