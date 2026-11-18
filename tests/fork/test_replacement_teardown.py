"""Teardown keeps signed entry candidates while bounded exit replacement remains usable."""

import pytest

from almanak.runtime import Runtime
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from tests.fork.support import processes
from tests.fork.test_production import STRATEGY, gateway_arguments
from tests.fork.test_transaction_replacement import (
    drive,
    escalate,
    prepare_pending,
    retain,
    setup,
)
from tests.fork.test_transaction_replacement import node as node
from tests.fork.test_uniswap_replacement import run_until
from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import assert_zero_obligations

pytestmark = pytest.mark.fork


def stop_new_risk(runtime, node, pending):
    runtime.journal.teardown()
    state = drive(runtime, lambda s: s.pending.family.blocked == "teardown")
    candidate = state.pending.family.candidates[1]
    assert candidate.submission is None
    assert node.rpc("eth_getTransactionByHash", [candidate.tx_hash]) is None
    assert state.pending.family.candidates == pending.family.candidates
    restore_original(node, pending)
    return drive(runtime, lambda s: any(o.tx_hash == pending.tx_hash for o in s.outcomes))


def restore_original(node, pending):
    node.rpc("anvil_setNextBlockBaseFeePerGas", ["0x1"])
    if node.rpc("eth_getTransactionByHash", [pending.tx_hash]) is None:
        node.rpc("eth_sendRawTransaction", [pending.raw_transaction])
    node.rpc("evm_mine")


def finish_teardown(runtime, node):
    start = len(runtime.journal.load().outcomes)
    node.rpc("anvil_setNextBlockBaseFeePerGas", ["0x1"])
    node.rpc("evm_mine")
    runtime.fault = lambda stage, _: (
        node.rpc("evm_setAutomine", [False]) if stage == "prepared" else None
    )
    state = run_until(runtime, node, lambda s: s.mode == "closed", 1)
    for closed in state.closure:
        assert_zero_obligations(closed)
    assert all(len(o.family.candidates) == 2 for o in state.outcomes[start:])
    return state


@pytest.mark.parametrize("profile", ["eoa_supply", "safe_borrow"])
def test_teardown_blocks_unattempted_increase_then_replaces_closure(node, tmp_path, profile):
    directory = tmp_path / "deployment"
    config = setup(node, directory, profile)
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
            pending = escalate(runtime, node, first)
            stop_new_risk(runtime, node, pending)
            finish_teardown(runtime, node)
            retain(node, Journal(directory / "state.sqlite"), "teardown-" + profile)
        finally:
            runtime.close()
