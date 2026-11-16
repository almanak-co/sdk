"""Verify held SparkLend debt, fund-owned proceeds and exact closure accounting."""

from almanak.connectors.sparklend.reader import balance, read_position
from almanak.gateway.ethereum import EthereumReader
from tests.strategy_fork.pool_lending_authority import assert_permissions as assert_permissions
from tests.strategy_fork.pool_lending_lifecycle import (
    assert_lifecycle_actions as assert_lifecycle_actions,
)
from tests.strategy_fork.pool_lending_lifecycle import assert_payer_effects as assert_payer_effects
from tests.strategy_fork.pool_lending_lifecycle import (
    assert_successful_outcomes as assert_successful_outcomes,
)
from tests.strategy_fork.pool_lending_lifecycle import (
    assert_wallet_effects as assert_wallet_effects,
)
from tests.strategy_fork.pool_lending_lifecycle import (
    assert_zero_obligations as assert_zero_obligations,
)
from tests.strategy_fork.pool_lending_lifecycle import assert_zero_owned as assert_zero_owned
from tests.strategy_fork.pool_lending_lifecycle import borrowed
from tests.strategy_fork.pool_lending_lifecycle import entered as entered
from tests.strategy_fork.pool_lending_lifecycle import wallet_movement as wallet_movement

crash_operation = "borrow"


def assert_holding(fork, state):
    assert (entered(state), state.mode) == (True, "running")
    assert_successful_outcomes(state)
    with EthereumReader(state.config.positions[0].chain, fork.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        for owned in state.owned:
            assert_owned_position(reader, state, owned, block)
        loan = state.config.target("loan")
        assert balance(reader, loan.asset, loan.fund.address, block) >= borrowed(state, "loan")
    assert min(state.owned[0].supply_scaled, state.owned[1].debt_scaled) > 0


def assert_owned_position(reader, state, owned, block):
    target = owned.target
    current = read_position(reader, target.reserve, target.fund, block)
    assert (current.supplied_scaled, current.debt_scaled, current.collateral_enabled) == (
        owned.supply_scaled,
        owned.debt_scaled,
        owned.collateral_enabled,
    )
    payer = state.config.accounts.binding(target.account).profile.submitter
    if payer != target.fund:
        operator = read_position(reader, target.reserve, payer, block)
        assert operator.supplied_scaled == operator.debt_scaled == 0


def assert_closed(fork, state):
    assert (state.mode, state.pending, state.plan, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    assert_lifecycle_actions(state)
    for before, after in zip(state.baseline, state.closure, strict=True):
        assert_zero_obligations(after)
        assert after.position.stable_debt_assets == 0
        assert_wallet_effects(state, before, after)
        assert_payer_effects(state, before, after)
    assert_zero_owned(state)


# The production EOA rehearsal passed well under the 180 s default in
# hosted Fork Qualification run 35981866228; the wait matches the other
# standalone lending references.
# The supervised lifecycle of this reference closed in 105 to 106 s, and
# the rehearsal's teardown spanned 98 s from its first teardown decision.
teardown_timeout = 360
