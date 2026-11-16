"""One fund retains borrowed proceeds, exchange fills and lending unwind."""

from tests.strategy_fork.composed_pancake_authority import assert_permissions as assert_permissions
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_closed as assert_closed,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_holding as assert_holding,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    entered as entered,
)

crash_operation = "swap"
# Production EOA rehearsal bounds follow bnb_aave_pancake_borrow_and_trade, which has
# the same connector composition and the same entry, hold and teardown step sequence. In
# hosted run 35980345945 that sibling took at most 63 s from the supply decision to the
# last entry decision and 130 s from the first teardown decision to the withdraw
# decision.
entry_timeout = 900
teardown_timeout = 1500
hold_timeout = 180
