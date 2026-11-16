"""One fund retains borrowed proceeds, exchange fills and lending unwind."""

from tests.strategy_fork.composed_aerodrome_authority import (
    assert_permissions as assert_permissions,
)
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
# Production EOA rehearsal bounds follow base_aave_curve_borrow_and_trade, which has the
# same chain, lending connector and single-swap DEX role and the same entry, hold and
# teardown step sequence. In hosted run 35980345945 that sibling took at most 68 s from
# the supply decision to the last entry decision and 166 s from the first teardown
# decision to the withdraw decision.
entry_timeout = 900
teardown_timeout = 1800
hold_timeout = 180
# The sibling's rehearsal once stopped at the 20 s process stop with an evaluation in
# flight (hosted run 35961664049).
process_timeout = 180
