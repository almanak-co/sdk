"""Health-factor-managed borrow lifecycle through the shared process harness."""

from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.aave_v3_authority import assert_permissions as assert_permissions
from tests.strategy_fork.leverage_loop_assertions import completed
from tests.strategy_fork.pool_lending_lifecycle import assert_successful_outcomes, assert_zero_owned

crash_operation = "borrow"


def entered(state):
    return settled(state) and completed(state)[:2] == ["supply", "borrow"]


def assert_holding(fork, state):
    assert (entered(state), state.mode) == (True, "running")
    assert not state.memory.values["unwinding"]
    assert_successful_outcomes(state)
    assert all(owned.debt_scaled or owned.supply_scaled for owned in state.owned)


def assert_closed(fork, state):
    assert (state.mode, settled(state)) == ("closed", True)
    assert_successful_outcomes(state)
    assert_zero_owned(state)
    assert completed(state)[-2:] == ["repay", "withdraw"]
