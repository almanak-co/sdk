from tests.strategy_fork.curve_assertions import assert_closed as assert_closed
from tests.strategy_fork.curve_assertions import assert_holding as assert_holding
from tests.strategy_fork.curve_assertions import entered as entered
from tests.strategy_fork.curve_authority import assert_permissions as assert_permissions

crash_operation = "swap"

# Retained Avalanche runs completed 17 successful effects before the 360-second limit.
# Keep a bounded two-fund entry budget while preserving author-hook isolation limits.
entry_timeout = 600
