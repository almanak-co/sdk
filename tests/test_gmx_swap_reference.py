"""Reusable swap author logic consumes captured acceptance and keeper-settlement facts."""

import importlib.util
from pathlib import Path
from types import SimpleNamespace

from almanak.connectors.gmx_v2.api import Actions
from tests.test_gmx_swap_replay import completed as completed


def reference(chain):
    path = Path(__file__).parents[1] / "strategies" / f"gmx_v2_{chain}_swap/strategy.py"
    spec = importlib.util.spec_from_file_location("gmx_swap_reference", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_reusable_swap_waits_for_keeper_effect_before_holding_and_returns_owned_output(completed):
    chain = (
        "arbitrum"
        if str(completed.config.allocations[0].chain.chain_id) == "eip155:42161"
        else "avalanche"
    )
    strategy = reference(chain)
    memory = strategy.State()
    accepted = completed.outcomes[1]
    context = SimpleNamespace(memory=memory, result=accepted.strategy_result())
    assert strategy.on_outcome(context) == memory
    context.result = completed.outcomes[2].strategy_result()
    held = strategy.on_outcome(context)
    assert held.held_at == {"Main": completed.outcomes[2].after.block.timestamp}
    allocation = completed.config.allocations[0]
    observed = completed.baseline[0]
    decision = SimpleNamespace(
        config=completed.config,
        memory=memory,
        teardown=False,
        require_observation=lambda name: observed,
    )
    decision.actions = Actions(decision)
    assert (
        strategy.allocation_intent(decision, allocation, observed).amount == allocation.entry_amount
    )
    decision.memory = held
    observed = completed.outcomes[2].after
    assert strategy.allocation_intent(decision, allocation, observed) is None
    decision.teardown = True
    returned = strategy.allocation_intent(decision, allocation, observed)
    assert returned.amount == completed.outcomes[4].operation.amount
    observed = completed.closure[0]
    assert strategy.allocation_intent(decision, allocation, observed) is None
