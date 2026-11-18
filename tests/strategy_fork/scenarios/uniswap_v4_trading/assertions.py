from tests.strategy_fork.uniswap_assertions import assert_closed as assert_closed
from tests.strategy_fork.uniswap_assertions import assert_holding as assert_holding
from tests.strategy_fork.uniswap_assertions import settled, timed_exit

crash_operation = "swap"


def entered(state):
    return (
        settled(state)
        and bool(state.memory.values.get("bought_at"))
        and all(o.inventory1.amount > 0 for o in state.owned)
    )


def exercise_hold(node, journal, process):
    timed_exit(node, journal, process, "bought_at")
