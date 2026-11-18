"""Execute the reusable lending/trading strategy through the shared process harness."""

from tests.strategy_fork.lending_trade_assertions import assert_closed as assert_closed
from tests.strategy_fork.lending_trade_assertions import assert_holding as assert_holding
from tests.strategy_fork.lending_trade_assertions import assert_permissions as assert_permissions
from tests.strategy_fork.lending_trade_assertions import crash_operation as crash_operation
from tests.strategy_fork.lending_trade_assertions import entered as entered

# Full reconciliation across both protocols outlasts the default fixture wait.
teardown_timeout = 600

# The injected crash fires on the trade swap broadcast, which the composed entry
# reaches only after the collateral supply, the borrow and the trade approvals.
# Retained item 4 lanes were still one to two operations short of that broadcast
# at the 180 s default, so this reference uses the composed 420 s bound its
# promoted Aave V4 siblings already declare.
crash_timeout = 420
