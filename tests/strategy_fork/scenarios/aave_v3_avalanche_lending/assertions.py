"""Apply the shared lending lifecycle assertions to the configured Avalanche market."""

from tests.strategy_fork.scenarios.aave_v3_ethereum_lending.assertions import (
    assert_closed as assert_closed,
)
from tests.strategy_fork.scenarios.aave_v3_ethereum_lending.assertions import (
    assert_holding as assert_holding,
)
from tests.strategy_fork.scenarios.aave_v3_ethereum_lending.assertions import (
    assert_permissions as assert_permissions,
)
from tests.strategy_fork.scenarios.aave_v3_ethereum_lending.assertions import (
    crash_operation as crash_operation,
)
from tests.strategy_fork.scenarios.aave_v3_ethereum_lending.assertions import (
    entered as entered,
)
