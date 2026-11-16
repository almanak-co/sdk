from tests.strategy_fork.curve_assertions import assert_closed as assert_closed
from tests.strategy_fork.curve_assertions import assert_holding as assert_holding
from tests.strategy_fork.curve_assertions import entered as entered
from tests.strategy_fork.curve_authority import assert_permissions as assert_permissions

crash_operation = "swap"

# The two funds enter one after the other; on the 2026-09-13 bench the second
# mint landed 371 seconds after the fork anchor and its increase and decrease
# followed, so the 360-second budget the internal EOA lane waited on ran out.
entry_timeout = 900
