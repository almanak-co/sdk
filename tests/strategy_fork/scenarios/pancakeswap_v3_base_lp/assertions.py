from tests.strategy_fork.v3_pool_assertions import assert_closed as assert_closed
from tests.strategy_fork.v3_pool_assertions import assert_holding as assert_holding
from tests.strategy_fork.v3_pool_assertions import entered as entered
from tests.strategy_fork.v3_pool_assertions import refresh_fork_head

crash_operation = "swap"

# Entry takes five decisions at the reference's 60 s interval (swap, mint, increase,
# decrease, collect), so it cannot settle inside the 180 s harness default.
entry_timeout = 600
align_teardown = refresh_fork_head
