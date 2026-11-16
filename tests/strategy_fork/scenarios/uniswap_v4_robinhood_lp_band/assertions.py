from tests.strategy_fork.uniswap_assertions import assert_closed_band, settled
from tests.strategy_fork.uniswap_assertions import assert_holding as assert_holding

assert_closed = assert_closed_band
crash_operation = "mint"


def entered(state):
    started = bool(state.memory.values.get("started_at"))
    return settled(state) and started and all(map(holding_band, state.owned))


def holding_band(owned):
    return owned.entry_complete and getattr(owned.position, "range", None) is not None


entry_timeout = 600
teardown_timeout = 600
