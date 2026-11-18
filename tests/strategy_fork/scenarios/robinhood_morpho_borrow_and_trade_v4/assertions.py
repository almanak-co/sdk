"""One Morpho fund retains V4 proceeds and repays before releasing collateral."""

from almanak.connectors.composed.inventory import cash_evidence
from almanak.connectors.identifiers import ConnectorId
from tests.strategy_fork.lending_trade_assertions import (
    assert_holding as assert_holding,
)
from tests.strategy_fork.lending_trade_assertions import (
    assert_lending_closed,
)
from tests.strategy_fork.lending_trade_assertions import entered as entered
from tests.strategy_fork.morpho_authority import assert_permissions as lending_permissions
from tests.strategy_fork.pool_lending_lifecycle import assert_successful_outcomes
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_inventory_movements,
    assert_roundtrip_order,
)
from tests.strategy_fork.uniswap_v4_base_authority import (
    assert_permissions as exchange_permissions,
)

crash_operation = "swap"
entry_timeout = 600
# Every composed Morpho outcome carries its market observation, which each gateway
# request decodes and revalidates, so Safe-account teardown after a fault needs more
# than 1200 s to reach the final closure observation.
teardown_timeout = 1800

# The injected crash fires on the trade swap broadcast, which the composed entry
# reaches only after the collateral supply, the borrow and the trade approvals.
# Retained item 4 lanes were still one to two operations short of that broadcast
# at the 180 s default, so this reference uses the composed 420 s bound its
# promoted Aave V4 siblings already declare.
crash_timeout = 420


def assert_closed(node, state):
    assert state.mode == "closed" and state.pending is None and state.plan is None
    assert state.decision is None and state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    assert_inventory_movements(state)
    assert_roundtrip_order(state)
    assert_lending_closed(state)
    exchange = state.closure[-1]
    # V4 snapshots retain ownership one level down; the shared V3-family check
    # reads a top-level position that V4 observations do not carry.
    assert exchange.owned.position is None and not exchange.needs_cleanup
    measured = cash_evidence(state.closure)
    assert all(measured[b.key][1] == b.amount for b in state.inventory)


def assert_permissions(node, config):
    lending = lending_permissions(node, config.component(ConnectorId("morpho_blue")))
    exchange = exchange_permissions(node, config.component(ConnectorId("uniswap_v4")))
    return {"lending": lending, "exchange": exchange}
