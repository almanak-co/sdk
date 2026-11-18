from tests.strategy_fork.bnb_v3_authority import retain_lp_gateway_ownership
from tests.strategy_fork.uniswap_v3_assertions import assert_closed as assert_closed
from tests.strategy_fork.uniswap_v3_assertions import assert_holding as assert_holding
from tests.strategy_fork.uniswap_v3_assertions import entered as entered

crash_operation = "swap"


def exercise_hold(node, journal, process):
    retain_lp_gateway_ownership(node, journal, process, "position")
