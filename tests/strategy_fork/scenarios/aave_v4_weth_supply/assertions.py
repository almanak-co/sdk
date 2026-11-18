"""Verify the actual reference's Safe collateral, debt, capital and operator gas."""

from almanak.connectors.aave_v4.debt import position_components
from almanak.connectors.aave_v4.reader import read_position
from almanak.gateway.ethereum import EthereumReader
from almanak.models import Account, SafeRoles, UInt
from tests.fork.aave_assertions import assert_closed_checkpoint, assert_one_supply_log

# The reference strategy borrows its BORROW constant of 5 USDC (6 decimals) per debt.
BORROWED = 5_000_000


def entered(state):
    return all(
        (
            len(state.memory.values.get("supplied_at", {})) == len(state.config.allocations),
            len(state.memory.values.get("borrowed_at", {})) == len(state.config.debts),
            state.pending is None,
            state.plan is None,
            state.decision is None,
            state.delivered == len(state.outcomes),
        )
    )


def assert_holding(fork, state):
    assert (entered(state), state.mode) == (True, "running")
    chain = state.config.allocations[0].reserve.chain
    with EthereumReader(chain, fork.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        for allocation in state.config.allocations:
            assert_supplied_position(fork, state, reader, block, allocation)
        for debt in state.config.debts:
            assert_borrowed_position(state, reader, block, debt)


def assert_supplied_position(fork, state, reader, block, allocation):
    target = state.config.target(allocation.name)
    position = read_position(reader, allocation.reserve, target.fund, block)
    assert position.supplied_shares == state.shares(target) > 0
    assert position.total_debt == 0
    assert_one_supply_log(fork, state, allocation)


def assert_borrowed_position(state, reader, block, debt):
    profile = state.config.accounts.binding(debt.account).profile
    position = read_position(reader, debt.reserve, profile.fund, block)
    assert position.total_debt >= borrowed_assets(state, debt.name)
    if isinstance(profile, SafeRoles):
        assert read_position(reader, debt.reserve, profile.operator, block).total_debt == 0
    assert_drawn_ownership(state, reader, block, debt)


def assert_drawn_ownership(state, reader, block, debt):
    target = state.config.target(debt.name)
    parts, _ = position_components(reader, target, block)
    owned = next(o for o in state.owned if o.target == target)
    assert parts["drawn_shares"] == owned.drawn_shares > 0


def assert_closed(fork, state):
    assert_closed_checkpoint(state)
    assert_unique_outcomes(state)
    assert_nonce_scopes(state)
    for closed in state.closure:
        assert_zero_obligations(closed)
        assert_capital(state, closed)
        assert_payer_gas(state, closed)
    for allocation in state.config.allocations:
        assert_one_supply_log(fork, state, allocation)
    for debt in state.config.debts:
        assert_repayment_cost(state, debt)


def assert_unique_outcomes(state):
    assert len({item.tx_hash for item in state.outcomes}) == len(state.outcomes)


def assert_zero_obligations(closed):
    assert (
        closed.position.total_debt,
        closed.position.supplied_shares,
        closed.allowance,
        closed.drawn_shares,
        closed.premium_shares,
        closed.premium_offset_ray,
        closed.collateral_enabled,
    ) == (0, 0, 0, 0, 0, 0, False)


def position_outcomes(state, target):
    return [
        o
        for o in state.outcomes
        if (o.operation.target.fund, o.operation.target.asset) == (target.fund, target.asset)
    ]


def borrowed_assets(state, name):
    """What the strategy borrowed for one debt; the configuration states no amount."""
    return total_assets([o for o in state.outcomes if o.operation.allocation == name], ("borrow",))


def total_assets(outcomes, kinds):
    return sum(o.assets for o in outcomes if o.operation.kind in kinds)


def assert_capital(state, closed):
    baseline = next(o for o in state.baseline if o.target == closed.target)
    outcomes = position_outcomes(state, closed.target)
    inflow = total_assets(outcomes, ("withdraw", "borrow"))
    outflow = total_assets(outcomes, ("supply", "repay"))
    assert closed.wallet_balance == baseline.wallet_balance + inflow - outflow


def assert_payer_gas(state, closed):
    baseline = next(o for o in state.baseline if o.target == closed.target)
    payer = state.config.accounts.binding(closed.account).profile.submitter
    gas = payer_cost(state, payer)
    assert baseline.native_balance.amount - closed.native_balance.amount == gas


def payer_cost(state, payer):
    return sum(
        o.gas_paid.amount
        for o in state.outcomes
        if state.config.accounts.binding(o.operation.account).profile.submitter == payer
    )


def assert_repayment_cost(state, debt):
    outcomes = [o for o in state.outcomes if o.operation.allocation == debt.name]
    borrowed = total_assets(outcomes, ("borrow",))
    repays = [o for o in outcomes if o.operation.kind == "repay"]
    assert borrowed == BORROWED
    # One full repay pulls the accrued debt at inclusion; assert_zero_obligations
    # proves nothing remained, so the repaid amount has no configured upper bound.
    assert len(repays) == 1
    assert borrowed < repays[0].assets
    assert_one_borrow(outcomes)


def assert_one_borrow(outcomes):
    assert len([o for o in outcomes if o.operation.kind == "borrow"]) == 1


def assert_nonce_scopes(state):
    scopes: dict[Account, list[UInt]] = {}
    for outcome in state.outcomes:
        payer = state.config.accounts.binding(outcome.operation.account).profile.submitter
        assert_execution_scope(outcome, payer)
        scopes.setdefault(payer, []).append(outcome.nonce)
    for nonces in scopes.values():
        assert nonces == list(range(nonces[0], nonces[0] + len(nonces)))


def assert_execution_scope(outcome, payer):
    execution = outcome.execution
    assert execution is not None
    assert execution.submitter == payer
    assert execution.call.fund == outcome.operation.target.fund
