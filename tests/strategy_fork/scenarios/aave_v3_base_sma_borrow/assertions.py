"""The SMA-gated loan: supplied collateral and a held loan, then repaid and withdrawn.

The fork anchor's latest hourly close sits above its 48-hour average by more than the
borrowing margin, so the strategy borrows as soon as its collateral is supplied.
"""

from almanak.connectors.aave_v3.reader import balance
from almanak.gateway.ethereum import EthereumReader
from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.aave_v3_authority import assert_permissions as assert_permissions
from tests.strategy_fork.pool_lending_lifecycle import (
    assert_payer_effects,
    assert_successful_outcomes,
    assert_wallet_effects,
    assert_zero_obligations,
    assert_zero_owned,
    borrowed,
)
from tests.strategy_fork.scenarios.aave_v3_ethereum_lending.assertions import (
    assert_owned_position,
)

crash_operation = "borrow"


def entered(state):
    return state.memory.values.get("borrowed") is True and settled(state)


def assert_holding(fork, state):
    assert (entered(state), state.mode) == (True, "running")
    assert_successful_outcomes(state)
    with EthereumReader(state.config.positions[0].chain, fork.rpc.endpoint) as reader:
        assert_held_on_chain(reader, state, reader.pin_block("latest"))
    assert min(state.owned[0].supply_scaled, state.owned[1].debt_scaled) > 0


def assert_held_on_chain(reader, state, block):
    for owned in state.owned:
        assert_owned_position(reader, state, owned, block)
    loan = state.config.target("loan")
    assert balance(reader, loan.asset, loan.fund.address, block) >= borrowed(state, "loan")


def assert_closed(fork, state):
    assert state.mode == "closed" and settled(state)
    assert_successful_outcomes(state)
    assert completed_kinds(state) == ["supply", "borrow", "repay", "withdraw"]
    for before, after in zip(state.baseline, state.closure, strict=True):
        assert_released(state, before, after)
    assert_zero_owned(state)


def completed_kinds(state):
    completed = [o for o in state.outcomes if o.strategy_result().status == "completed"]
    return [o.decision.intent.kind for o in completed]


def assert_released(state, before, after):
    assert_zero_obligations(after)
    assert_wallet_effects(state, before, after)
    assert_payer_effects(state, before, after)
