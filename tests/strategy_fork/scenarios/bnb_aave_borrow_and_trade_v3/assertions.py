"""Reuse actual shared-fund lifecycle assertions for this configured pair."""

from almanak.connectors.identifiers import ConnectorId
from tests.strategy_fork.aave_v3_authority import assert_permissions as aave_permissions
from tests.strategy_fork.curve_authority import assert_permissions as curve_permissions
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_closed as assert_closed,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_holding as assert_holding,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    crash_operation as crash_operation,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    entered as entered,
)


def assert_permissions(node, config):
    result = aave_permissions(node, config.component(ConnectorId("aave_v3")))
    result["curve"] = curve_permissions(node, config.component(ConnectorId("curve")))
    return result


# Production EOA rehearsal bounds from hosted run 35943488873 (accf84af). The
# Safe lifecycle of this reference spent up to 494 s from the supply decision to
# the teardown exit decision and up to 688 s from that exit decision to the
# withdraw decision; production steps took 0.84 to 0.96 of the Safe duration. Each
# bound is at least 1.35 times that Safe-derived estimate, in 300 s steps.
entry_timeout = 900
teardown_timeout = 1200
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
