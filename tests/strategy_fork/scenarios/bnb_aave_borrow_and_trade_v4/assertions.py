"""Retain one fund's debt proceeds, V4 fills and complete collateral unwind."""

from tests.strategy_fork.composed_v4_authority import assert_permissions as assert_permissions
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_holding as assert_holding,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import entered as entered
from tests.strategy_fork.scenarios.base_borrow_and_trade_v4.assertions import (
    assert_closed as assert_closed,
)

crash_operation = "swap"
# Hosted run 35943488873 (accf84af) reached the production entry in about 429 s
# and then saw one hold evaluation in the 30 s default. Its Safe lifecycle spent up
# to 898 s from the teardown exit decision to the withdraw decision; the teardown
# bound is at least 1.35 times that Safe-derived estimate, in 300 s steps.
entry_timeout = 600
teardown_timeout = 1500
# A composed hold evaluation observes both connectors; the BNB V4 production
# rehearsal was still observing more than 15 s after an evaluation fell due, so
# two scheduled evaluations need the 180 s its promoted Aave V4 siblings declare.
hold_timeout = 180

# The injected crash fires on the trade swap broadcast, which the composed entry
# reaches only after the collateral supply, the borrow and the trade approvals.
# Retained item 4 lanes were still one to two operations short of that broadcast
# at the 180 s default, so this reference uses the composed 420 s bound its
# promoted Aave V4 siblings already declare.
crash_timeout = 420
