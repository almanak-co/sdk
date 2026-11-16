"""Actual trading and LP strategies replace every executed operation on both backends."""

import time

import pytest

from almanak.runtime import Runtime
from almanak.state import Journal
from almanak.strategy import StrategyDefinition
from tests.fork.replacement_evidence import gas_price_traces
from tests.fork.support import processes
from tests.fork.test_production import gateway_arguments
from tests.fork.test_transaction_replacement import drive, escalate, mine_winner, retain
from tests.fork.test_transaction_replacement import node as node
from tests.fork.uniswap_replacement_setup import setup
from tests.strategy_fork.uniswap_assertions import assert_holding, settled

pytestmark = pytest.mark.fork


def replace_pending(runtime, node, winner):
    first = drive(runtime, lambda s: s.pending.family.candidates[0].submission is not None).pending
    runtime.tick()
    pending = escalate(runtime, node, first)
    assert pending.operation == first.operation
    assert pending.execution == first.execution
    chosen, receipt = mine_winner(runtime, node, pending, winner)
    state = drive(runtime, lambda s: any(o.tx_hash == chosen.tx_hash for o in s.outcomes))
    outcome = state.outcomes[-1]
    assert_replaced(first, outcome, receipt)
    # Isolate each replacement from the preceding fixture's deliberately raised fee market.
    node.rpc("anvil_setNextBlockBaseFeePerGas", ["0x1"])
    node.rpc("evm_mine")
    return outcome


def assert_replaced(first, outcome, receipt):
    assert outcome.gas_paid.amount == int(receipt["gasUsed"], 16) * int(
        receipt["effectiveGasPrice"], 16
    )
    assert len(outcome.family.candidates) == 2
    assert outcome.transaction_plan.operation == first.operation


def run_until(runtime, node, predicate, winner):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        state = runtime.journal.load()
        if predicate(state):
            return state
        if state.pending is not None:
            replace_pending(runtime, node, winner)
        else:
            runtime.tick()
    raise AssertionError("Uniswap replacement lifecycle did not settle")


def closed_checks(node, state):
    assert_holding(node, state)
    owned, before, after = state.owned[0], state.baseline[0], state.closure[0]
    assert owned.position is None and owned.inventory1.amount == 0
    assert not after.needs_cleanup
    assert before.native_balance.amount - after.native_balance.amount == sum(
        o.gas_paid.amount for o in state.outcomes
    )
    assert_unique_families(state)


def assert_unique_families(state):
    assert len({o.nonce for o in state.outcomes}) == len(state.outcomes)
    assert len({o.tx_hash for o in state.outcomes}) == len(state.outcomes)
    assert all(len(o.family.candidates) == 2 for o in state.outcomes)


def holding(state):
    owned = state.owned[0]
    entered = owned.position is not None if owned.target.range else owned.entry_complete
    return settled(state) and entered


def collect(runtime, node):
    state = runtime.journal.load()
    allocation = state.config.allocations[0]
    due = state.memory.values["minted_at"][allocation.name] + allocation.collect_seconds
    latest = int(node.rpc("eth_getBlockByNumber", ["latest", False])["timestamp"], 16)
    node.rpc("evm_setNextBlockTimestamp", [max(latest + 1, due)])
    node.rpc("evm_mine")
    return run_until(
        runtime, node, lambda s: settled(s) and bool(s.memory.values.get("collected_at")), 1
    )


@pytest.mark.parametrize("name", ["uniswap_v4_trading", "uniswap_v4_lp"])
@pytest.mark.parametrize("profile,winner", [("eoa", 0), ("safe", 1)])
def test_every_uniswap_operation_replaced(node, tmp_path, name, profile, winner):
    directory = tmp_path / "deployment"
    config, source = setup(node, directory, name, profile)
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
            runtime.fault = lambda stage, _: (
                node.rpc("evm_setAutomine", [False]) if stage == "prepared" else None
            )
            run_until(runtime, node, holding, winner)
            if (
                name == "uniswap_v4_lp"
                and runtime.journal.load().config.allocations[0].collect_seconds is not None
            ):
                collect(runtime, node)
            runtime.journal.teardown()
            closed = run_until(runtime, node, lambda s: s.mode == "closed", 1 - winner)
            closed_checks(node, closed)
            retain(
                node,
                Journal(directory / "state.sqlite"),
                name + "-" + profile,
                {"traces": gas_price_traces(node, closed)},
            )
        finally:
            runtime.close()
