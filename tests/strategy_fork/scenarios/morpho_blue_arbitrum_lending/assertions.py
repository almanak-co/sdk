"""Run the reusable Morpho reference through the shared process lifecycle harness."""

from tests.strategy_fork.morpho_assertions import assert_closed as assert_closed
from tests.strategy_fork.morpho_assertions import assert_holding as assert_holding
from tests.strategy_fork.morpho_assertions import assert_permissions as assert_permissions
from tests.strategy_fork.morpho_assertions import crash_operation as crash_operation
from tests.strategy_fork.morpho_assertions import entered as entered

# The production EOA rehearsal's 180 s default closure wait expired
# mid-teardown in hosted Fork Qualification run 35981866228.
# The supervised lifecycle of this reference closed in 199 to 203 s, and
# the rehearsal's teardown spanned 148 s from its first teardown decision.
teardown_timeout = 360
