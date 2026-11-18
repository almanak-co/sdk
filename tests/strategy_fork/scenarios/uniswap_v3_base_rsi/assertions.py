from tests.strategy_fork.price_driver import drive_oversold_cross
from tests.strategy_fork.uniswap_v3_assertions import assert_closed as assert_closed
from tests.strategy_fork.uniswap_v3_assertions import assert_holding as assert_holding
from tests.strategy_fork.uniswap_v3_assertions import entered as entered
from tests.strategy_fork.v3_pool_assertions import refresh_fork_head

crash_operation = "swap"

# The pinned market never crosses RSI-14 30, so the scenario drives the pool's own
# hourly closes down with real fork swaps between the first and second decisions.
drive_external = drive_oversold_cross
align_teardown = refresh_fork_head
