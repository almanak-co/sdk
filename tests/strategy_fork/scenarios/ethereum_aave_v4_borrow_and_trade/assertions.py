"""Execute the actual configured V4 borrowing and trading reference."""

from tests.strategy_fork.aave_v4_composed_assertions import (
    assert_closed as assert_closed,
)
from tests.strategy_fork.aave_v4_composed_assertions import (
    assert_holding as assert_holding,
)
from tests.strategy_fork.aave_v4_composed_assertions import (
    entered as entered,
)
from tests.strategy_fork.aave_v4_composed_authority import assert_permissions as assert_permissions

crash_operation = "swap"
teardown_timeout = 600
entry_timeout = 420
crash_timeout = 420
hold_timeout = 180
process_timeout = 180
