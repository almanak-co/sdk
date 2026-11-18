from tests.strategy_fork.uniswap_assertions import assert_closed as assert_closed
from tests.strategy_fork.uniswap_assertions import assert_holding as assert_holding
from tests.strategy_fork.uniswap_assertions import collect_and_exit, settled
from tests.strategy_fork.uniswap_v4_base_authority import assert_permissions as assert_permissions

crash_operation = "mint"


def entered(state):
    return (
        settled(state)
        and bool(state.memory.values.get("minted_at"))
        and all(o.position is not None for o in state.owned)
    )


def exercise_hold(node, journal, process):
    collect_and_exit(node, journal, process, trader_index=567234)


entry_timeout = 600
teardown_timeout = 600
