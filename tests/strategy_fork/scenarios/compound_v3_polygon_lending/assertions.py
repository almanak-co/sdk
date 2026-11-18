"""Run the reusable Comet reference through the shared process lifecycle harness."""

from tests.strategy_fork.compound_assertions import assert_closed as assert_closed
from tests.strategy_fork.compound_assertions import assert_holding as assert_holding
from tests.strategy_fork.compound_assertions import assert_permissions as assert_permissions
from tests.strategy_fork.compound_assertions import crash_operation as crash_operation
from tests.strategy_fork.compound_assertions import entered as entered

# Final closure verifies the configured Comet collateral set after all exits.
teardown_timeout = 360
