"""Leverage-loop lifecycle proofs: rounds from parameters, one cash ledger, a closed unwind."""

from almanak.connectors.identifiers import ConnectorId
from tests.strategy_fork import composed_authority, lending_trade_assertions
from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.pool_lending_lifecycle import assert_successful_outcomes
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_inventory_movements,
)

crash_operation = "swap"
# A two-round unwind is seven teardown decisions at about 1.5 to 3 minutes each on a fork.
entry_timeout = 1800
teardown_timeout = 1800
hold_timeout = 180


def loops(state):
    return state.config.parameters["loops"]


def completed(state):
    return [o.decision.intent.kind for o in state.outcomes if o.strategy_result().complete]


def entered(state):
    return settled(state) and state.memory.values.get("rounds") == loops(state)


def assert_permissions(node, config):
    if ConnectorId("morpho_blue") in config.parameter_components:
        return lending_trade_assertions.assert_permissions(node, config)
    return composed_authority.assert_permissions(node, config)


def assert_holding(node, state):
    assert (entered(state), state.mode) == (True, "running")
    assert not state.memory.values["unwinding"]
    assert_successful_outcomes(state)
    assert completed(state) == [
        "supply",
        *["borrow", "swap_exact_input", "supply"] * loops(state),
    ]
    assert_inventory_movements(state)


def lending_open(owned):
    if hasattr(owned, "empty"):
        return not owned.empty
    if hasattr(owned, "supply_scaled"):
        return bool(owned.supply_scaled or owned.debt_scaled or owned.collateral_enabled)
    return False


def assert_closed(node, state):
    assert (state.mode, settled(state)) == ("closed", True)
    assert_successful_outcomes(state)
    assert_inventory_movements(state)
    assert not any(lending_open(owned) for owned in state.owned)
    kinds = completed(state)
    assert {"repay", "withdraw"} <= set(kinds[kinds.index("supply") :])
    exchange = state.closure[-1]
    assert exchange.position is None and not exchange.needs_cleanup
