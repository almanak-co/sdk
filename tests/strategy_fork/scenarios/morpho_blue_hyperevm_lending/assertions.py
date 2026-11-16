"""Run the reusable Morpho reference through the shared process lifecycle harness."""

from tests.strategy_fork.morpho_assertions import assert_closed as assert_closed
from tests.strategy_fork.morpho_assertions import assert_holding as assert_holding
from tests.strategy_fork.morpho_assertions import assert_permissions as assert_permissions
from tests.strategy_fork.morpho_assertions import crash_operation as crash_operation
from tests.strategy_fork.morpho_assertions import entered as entered

# The Robinhood reference's measured margin, until this lifecycle has its own timing.
teardown_timeout = 360
