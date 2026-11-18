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

# Production EOA rehearsal bounds follow arbitrum_aave_curve_borrow_and_trade, which has the same
# connector composition and entry, hold and teardown step sequence; its bounds
# come from hosted run 35943488873. This reference's own Safe lifecycle
# timings in the qualifying run confirm the headroom.
entry_timeout = 900
teardown_timeout = 1800
hold_timeout = 180
# Hosted run 35961664049 stopped the production rehearsal at the fixed 20 s runtime
# stop while an evaluation was in flight; the composed Aave V4 references wait 180 s.
process_timeout = 180


def assert_permissions(node, config):
    result = aave_permissions(node, config.component(ConnectorId("aave_v3")))
    result["curve"] = curve_permissions(node, config.component(ConnectorId("curve")))
    return result
