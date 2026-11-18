"""A retained deadline cannot be extended by a fee replacement."""

import time

import pytest

from almanak.runtime import Runtime
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from tests.fork.replacement_evidence import gas_price_traces
from tests.fork.support import processes
from tests.fork.test_production import gateway_arguments
from tests.fork.test_replacement_authority import relay_revert
from tests.fork.test_transaction_replacement import drive, escalate, prepare_pending, retain
from tests.fork.test_transaction_replacement import node as node
from tests.fork.test_uniswap_replacement import run_until
from tests.fork.uniswap_replacement_setup import setup

pytestmark = pytest.mark.fork


def expire(runtime, node, pending):
    state = drive(runtime, lambda s: s.pending.family.candidates[-1].submission is not None)
    runtime.tick()
    candidate = state.pending.family.candidates[-1]
    node.rpc("anvil_dropTransaction", [candidate.tx_hash])
    deadline = pending.operation.deadline
    # Keep the simulated block inside the gateway's wall-clock freshness window.
    time.sleep(max(0, deadline + 1 - time.time() - 10))
    node.rpc("evm_setNextBlockTimestamp", [deadline + 1])
    node.rpc("evm_mine")
    blocked = drive(runtime, lambda s: s.pending.family.blocked == "stale_plan")
    assert blocked.pending.operation == pending.operation
    assert len(blocked.pending.family.candidates) == 2
    return candidate


def close_expired(runtime, node):
    with pytest.raises(RuntimeError, match="transaction reverted"):
        runtime.tick()
    runtime.fault = lambda stage, _: (
        node.rpc("evm_setAutomine", [False]) if stage == "prepared" else None
    )
    runtime.journal.teardown()
    node.rpc("anvil_setNextBlockBaseFeePerGas", ["0x1"])
    node.rpc("evm_mine")
    state = run_until(runtime, node, lambda s: s.mode == "closed", 1)
    verify_closed(state)
    assert all(len(o.family.candidates) == 2 for o in state.outcomes[-2:])
    return state


def verify_closed(state):
    assert state.owned[0].inventory1.amount == 0
    assert state.owned[0].position is None
    assert not state.closure[0].needs_cleanup
    assert state.baseline[0].native_balance.amount - state.closure[0].native_balance.amount == sum(
        o.gas_paid.amount for o in state.outcomes
    )


@pytest.mark.parametrize("profile", ["eoa", "safe"])
def test_expired_swap_family_reconciles_revert_without_refreshing_deadline(node, tmp_path, profile):
    directory = tmp_path / "deployment"
    config, source = setup(node, directory, "uniswap_v4_trading", profile, deadline=30)
    gateway, control = gateway_arguments(node, directory)
    with processes(tmp_path) as launch:
        launch(gateway, ready="gateway_ready")
        runtime = Runtime(
            control[3],
            directory / "gateway-auth.token",
            create=config,
            strategy=StrategyDefinition.read(source),
        )
        try:
            first = prepare_pending(runtime, node, "swap")
            pending = escalate(runtime, node, first)
            candidate = expire(runtime, node, pending)
            relay_revert(runtime, node, candidate, first)
            state = close_expired(runtime, node)
            retain(
                node,
                Journal(directory / "state.sqlite"),
                "uniswap-expired-" + profile,
                {"traces": gas_price_traces(node, state)},
            )
        finally:
            runtime.close()
