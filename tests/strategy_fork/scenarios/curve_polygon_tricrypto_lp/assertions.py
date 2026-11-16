from tests.strategy_fork.curve_assertions import assert_closed as assert_closed
from tests.strategy_fork.curve_assertions import assert_holding as assert_holding
from tests.strategy_fork.curve_assertions import entered as entered
from tests.strategy_fork.curve_authority import assert_permissions as assert_permissions

crash_operation = "swap"

# Retained Polygon runs completed 17 successful effects before the 360-second limit.
# Keep a bounded two-fund entry budget while preserving author-hook isolation limits.
entry_timeout = 600

# The retained Safe run closed both LP balances after 24 effects but reached the
# 180-second limit before final exit bookkeeping and allowance cleanup completed.
teardown_timeout = 300
