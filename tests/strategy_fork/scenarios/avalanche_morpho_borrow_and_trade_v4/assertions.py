"""One Avalanche Morpho fund retains V4 proceeds and repays before releasing collateral."""

from almanak.connectors.composed.inventory import cash_evidence
from almanak.connectors.identifiers import ConnectorId
from tests.strategy_fork.lending_trade_assertions import assert_holding as assert_holding
from tests.strategy_fork.lending_trade_assertions import (
    assert_lending_closed,
    assert_roundtrip_order,
)
from tests.strategy_fork.lending_trade_assertions import entered as entered
from tests.strategy_fork.morpho_authority import assert_permissions as lending_permissions
from tests.strategy_fork.pool_lending_lifecycle import assert_successful_outcomes
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_inventory_movements,
)
from tests.strategy_fork.uniswap_v4_base_authority import (
    assert_permissions as exchange_permissions,
)

crash_operation = "swap"
entry_timeout = 600
teardown_timeout = 900


def assert_permissions(node, config):
    lending = lending_permissions(node, config.component(ConnectorId("morpho_blue")))
    exchange = exchange_permissions(node, config.component(ConnectorId("uniswap_v4")))
    return {"lending": lending, "exchange": exchange}


def assert_closed(node, state):
    assert state.mode == "closed" and state.pending is None and state.plan is None
    assert state.decision is None and state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    assert_inventory_movements(state)
    assert_roundtrip_order(state)
    assert_lending_closed(state)
    exchange = state.closure[-1]
    assert exchange.owned.position is None and not exchange.needs_cleanup
    measured = cash_evidence(state.closure)
    assert all(measured[b.key][1] == b.amount for b in state.inventory)
