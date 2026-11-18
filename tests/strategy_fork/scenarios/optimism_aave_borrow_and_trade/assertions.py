"""Reuse actual shared-fund lifecycle assertions for this configured pair."""

from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_closed as assert_closed,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_holding as assert_holding,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_permissions as assert_permissions,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    crash_operation as crash_operation,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    entered as entered,
)

# Production EOA rehearsal bounds follow bnb_aave_borrow_and_trade, which has the same
# connector composition and the same entry, hold and teardown step sequence. In hosted
# run 35980345945 that sibling took at most 60 s from the supply decision to the last
# entry decision and 144 s from the first teardown decision to the withdraw decision.
entry_timeout = 900
teardown_timeout = 1800
hold_timeout = 180
