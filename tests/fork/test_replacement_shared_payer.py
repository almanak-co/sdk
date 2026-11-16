"""A common Safe operator preserves its lifetime exit reserve under a fee spike."""

import pytest

from almanak.runtime import Runtime
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from tests.fork.support import processes
from tests.fork.test_production import STRATEGY, gateway_arguments
from tests.fork.test_transaction_replacement import (
    assert_winner,
    close,
    drive,
    mine_winner,
    prepare_pending,
    retain,
    setup,
)
from tests.fork.test_transaction_replacement import node as node

pytestmark = pytest.mark.fork


def bounded_setup(node, directory):
    config = setup(node, directory, "safe_borrow")
    policy = config.production.gas[0]
    policy = policy.model_copy(
        update={
            "total_budget": policy.total_budget.model_copy(
                update={"amount": policy.exit_reserve.amount + 10**8}
            )
        }
    )
    config = config.model_copy(
        update={"production": config.production.model_copy(update={"gas": (policy,)})}
    )
    (directory / "production.json").write_text(config.model_dump_json(indent=2))
    node.rpc("anvil_setNextBlockBaseFeePerGas", ["0x1"])
    node.rpc("evm_mine")
    return config


def reject_spike(runtime, node, first):
    node.rpc("anvil_setNextBlockBaseFeePerGas", [hex(10**6)])
    node.rpc("evm_mine")
    state = drive(runtime, lambda s: s.pending.family.blocked == "gas_budget")
    assert state.pending.family.proposal is None
    assert len(state.pending.family.candidates) == 1
    assert state.pending.family.candidates[0].submission == first.family.candidates[0].submission
    chosen, receipt = mine_winner(runtime, node, state.pending, 0)
    state = drive(runtime, lambda s: any(o.tx_hash == chosen.tx_hash for o in s.outcomes))
    assert_winner(state, first, receipt)


def assert_shared_costs(state):
    assert_shared_bindings(state)
    costs = sum(o.gas_paid.amount for o in state.outcomes)
    assert costs < state.config.production.gas[0].total_budget.amount
    return costs


def assert_shared_bindings(state):
    profiles = [b.profile for b in state.config.accounts.bindings]
    assert len({p.submitter for p in profiles}) == 1
    assert len({p.fund for p in profiles}) == 2


def test_two_safes_and_multiple_positions_protect_operator_budget(node, tmp_path):
    directory = tmp_path / "deployment"
    config = bounded_setup(node, directory)
    gateway, control = gateway_arguments(node, directory)
    with processes(tmp_path) as launch:
        launch(gateway, ready="gateway_ready")
        runtime = Runtime(
            control[3],
            directory / "gateway-auth.token",
            create=config,
            strategy=StrategyDefinition.read(STRATEGY),
        )
        try:
            first = prepare_pending(runtime, node)
            reject_spike(runtime, node, first)
            state = close(runtime, node)
            cost = assert_shared_costs(state)
            retain(
                node,
                Journal(directory / "state.sqlite"),
                "shared-payer-budget",
                {"actual_payer_cost_wei": cost},
            )
        finally:
            runtime.close()
