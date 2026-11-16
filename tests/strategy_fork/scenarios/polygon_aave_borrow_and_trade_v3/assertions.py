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

# Production EOA rehearsal bounds follow bnb_aave_borrow_and_trade_v3, which has the same
# connector composition and entry, hold and teardown step sequence; its bounds
# come from hosted run 35943488873. This reference's own Safe lifecycle
# timings in the qualifying run confirm the headroom.
entry_timeout = 900
teardown_timeout = 1200
hold_timeout = 180


def assert_permissions(node, config):
    result = aave_permissions(node, config.component(ConnectorId("aave_v3")))
    result["curve"] = curve_permissions(node, config.component(ConnectorId("curve")))
    return result


# The injected crash fires on the trade swap broadcast, which the composed entry
# reaches only after the collateral supply, the borrow and the trade approvals.
# Retained item 4 lanes were still one to two operations short of that broadcast
# at the 180 s default, so this reference uses the composed 420 s bound its
# promoted Aave V4 siblings already declare.
crash_timeout = 420
