"""Retain one fund's debt proceeds, V4 fills and complete collateral unwind."""

from tests.strategy_fork.composed_v4_authority import assert_permissions as assert_permissions
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_holding as assert_holding,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import entered as entered
from tests.strategy_fork.scenarios.base_borrow_and_trade_v4.assertions import (
    assert_closed as assert_closed,
)

# Production EOA rehearsal bounds follow bnb_aave_borrow_and_trade_v4, which has the same
# connector composition and entry, hold and teardown step sequence; its bounds
# come from hosted run 35943488873. This reference's own Safe lifecycle
# timings in the qualifying run confirm the headroom.
entry_timeout = 600
teardown_timeout = 1500
hold_timeout = 180

crash_operation = "swap"

# The injected crash fires on the trade swap broadcast, which the composed entry
# reaches only after the collateral supply, the borrow and the trade approvals.
# Retained item 4 lanes were still one to two operations short of that broadcast
# at the 180 s default, so this reference uses the composed 420 s bound its
# promoted Aave V4 siblings already declare.
crash_timeout = 420
