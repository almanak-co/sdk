"""GMX price-stop lifecycle, settled by the same recorded keeper actions as the position."""

from tests import gmx_fork
from tests.gmx_fork import assert_closed as assert_closed
from tests.strategy_fork.scenarios.gmx_v2_arbitrum_position.assertions import (
    drive_external as drive_external,
)


def entered(state):
    # This strategy keeps an `opened` flag rather than per-allocation open times.
    return (
        gmx_fork.requests_settled(state)
        and gmx_fork.positions_open(state)
        and state.memory.values.get("opened") is True
    )


def assert_holding(node, state):
    assert entered(state)
    assert all(o.position.size_usd_e30 == 1000 * 10**30 for o in state.owned)
    assert all(not o.accounting_issues for o in state.outcomes)
    assert gmx_fork.keeper_count(state) == 1
