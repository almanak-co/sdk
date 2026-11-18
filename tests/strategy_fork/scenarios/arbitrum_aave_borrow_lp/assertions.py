"""Borrow-then-liquidity lifecycle through the shared process harness."""

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


def liquidity(state):
    return next(o for o in state.owned if o.target.allocation == "liquidity")


def entered(state):
    return settled(state) and liquidity(state).position is not None


def assert_holding(node, state):
    assert (entered(state), state.mode) == (True, "running")
    assert not state.memory.values["unwinding"]
    assert_successful_outcomes(state)
    assert completed(state) == ["supply", "borrow", "swap_exact_input", "open_liquidity"]
    assert_inventory_movements(state)


def assert_closed(node, state):
    assert_successful_outcomes(state)
    assert_inventory_movements(state)
    checks = {
        "mode closed": state.mode == "closed",
        "settled": settled(state),
        "lending closed": not any(map(lending_open, state.owned)),
        "liquidity closed": liquidity(state).position is None,
        "exit intents": {"close_liquidity", "repay", "withdraw"} <= set(completed(state)),
        "no cleanup": not state.closure[-1].needs_cleanup,
    }
    assert all(checks.values()), checks
