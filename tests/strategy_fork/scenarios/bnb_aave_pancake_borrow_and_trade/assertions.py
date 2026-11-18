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
# Production EOA rehearsal bounds from hosted run 35943488873 (accf84af). The
# Safe lifecycle of this reference spent up to 630 s from the supply decision to
# the teardown exit decision and up to 886 s from that exit decision to the
# withdraw decision; production steps took 0.84 to 0.96 of the Safe duration. Each
# bound is at least 1.35 times that Safe-derived estimate, in 300 s steps.
entry_timeout = 900
teardown_timeout = 1500
# A composed hold evaluation observes both connectors; the BNB V4 production
# rehearsal was still observing more than 15 s after an evaluation fell due, so
# two scheduled evaluations need the 180 s its promoted Aave V4 siblings declare.
hold_timeout = 180
