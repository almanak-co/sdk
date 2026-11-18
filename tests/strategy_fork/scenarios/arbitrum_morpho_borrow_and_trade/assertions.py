"""Execute the reusable lending/trading strategy through the shared process harness."""

from tests.strategy_fork.lending_trade_assertions import assert_closed as assert_closed
from tests.strategy_fork.lending_trade_assertions import assert_holding as assert_holding
from tests.strategy_fork.lending_trade_assertions import assert_permissions as assert_permissions
from tests.strategy_fork.lending_trade_assertions import crash_operation as crash_operation
from tests.strategy_fork.lending_trade_assertions import entered as entered

# Reconciliation includes both lending and trading observations after Safe exits.
teardown_timeout = 360

# The complete entry includes swap submission and reconciliation before fault injection.
entry_timeout = 420
