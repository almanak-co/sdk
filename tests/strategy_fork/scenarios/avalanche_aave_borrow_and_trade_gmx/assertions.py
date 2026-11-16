"""The same fund lifecycle uses explicit Avalanche lending and keeper evidence."""

from tests.strategy_fork.scenarios.arbitrum_aave_borrow_and_trade_gmx.assertions import (
    assert_closed as assert_closed,
)
from tests.strategy_fork.scenarios.arbitrum_aave_borrow_and_trade_gmx.assertions import (
    assert_holding as assert_holding,
)
from tests.strategy_fork.scenarios.arbitrum_aave_borrow_and_trade_gmx.assertions import (
    assert_permissions as assert_permissions,
)
from tests.strategy_fork.scenarios.arbitrum_aave_borrow_and_trade_gmx.assertions import (
    drive_external_chain,
)
from tests.strategy_fork.scenarios.arbitrum_aave_borrow_and_trade_gmx.assertions import (
    entered as entered,
)

crash_operation = "gmx_create_swap"
crash_stage = "broadcast"
teardown_timeout = 600


def drive_external(node, journal, running):
    drive_external_chain(node, journal, running, "avalanche")
