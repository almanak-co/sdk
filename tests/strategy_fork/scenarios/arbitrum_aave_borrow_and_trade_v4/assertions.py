"""Arbitrum authority uses the same complete V4 closure checks as Base."""

from tests.strategy_fork.composed_v4_arbitrum_authority import (
    assert_permissions as assert_permissions,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_holding as assert_holding,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import entered as entered
from tests.strategy_fork.scenarios.base_borrow_and_trade_v4.assertions import (
    assert_closed as assert_closed,
)

crash_operation = "swap"
# Production EOA rehearsal bounds follow bnb_aave_borrow_and_trade_v4, which has the
# same connector composition and the same entry, hold and teardown step sequence. In
# hosted run 35980345945 that sibling took at most 63 s from the supply decision to the
# last entry decision and 212 s from the first teardown decision to the withdraw
# decision.
entry_timeout = 600
teardown_timeout = 1500
hold_timeout = 180
