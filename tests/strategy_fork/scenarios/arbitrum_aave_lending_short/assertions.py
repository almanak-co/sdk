"""Lending-short lifecycle through the shared process harness."""

from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.composed_authority import assert_permissions as assert_permissions
from tests.strategy_fork.leverage_loop_assertions import completed, lending_open
from tests.strategy_fork.pool_lending_lifecycle import assert_successful_outcomes
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_inventory_movements,
)

crash_operation = "swap"
entry_timeout = 1200
teardown_timeout = 1800
hold_timeout = 180


def entered(state):
    return settled(state) and state.memory.values.get("entry_price") is not None


def assert_holding(node, state):
    assert (entered(state), state.mode) == (True, "running")
    assert_successful_outcomes(state)
    assert completed(state)[:3] == ["supply", "borrow", "swap_exact_input"]
    assert_inventory_movements(state)


def assert_closed(node, state):
    assert (state.mode, settled(state)) == ("closed", True)
    assert_successful_outcomes(state)
    assert_inventory_movements(state)
    assert not any(lending_open(owned) for owned in state.owned)
    assert {"repay", "withdraw"} <= set(completed(state))
    assert not state.closure[-1].needs_cleanup
