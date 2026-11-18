"""Verify protocol closure and shared cash independently for each fund and chain."""

from almanak.connectors.aave_v3.observations import Ownership as LendingOwnership
from almanak.connectors.aave_v3.observations import Snapshot as LendingSnapshot
from almanak.connectors.composed.inventory import cash_evidence
from almanak.connectors.composed.models import wallet_key
from almanak.gateway.ethereum import EthereumReader
from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.scenarios.aave_v3_ethereum_lending.assertions import (
    assert_owned_position,
    assert_payer_effects,
    assert_successful_outcomes,
    assert_zero_obligations,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_cash_ledger,
    assert_wallet_balances,
)

crash_operation = "swap"
# Production EOA rehearsal bounds follow bnb_aave_borrow_and_trade, which has the same
# connector composition on each of its two chains and the same entry, hold and teardown
# step sequence. In hosted run 35980345945 that sibling took at most 60 s from the
# supply decision to the last entry decision and 144 s from the first teardown decision
# to the withdraw decision. This reference is slower: its own cases in hosted run
# 36098513138, after #73, took at most 178 s of entry decisions and 457 s of teardown
# decisions, inside both bounds with at least 3.9 times headroom.
entry_timeout = 900
teardown_timeout = 1800
hold_timeout = 180


def entered(state):
    return settled(state) and all(
        state.memory.values.get("phases", {}).get(binding.name) == 3
        for binding in state.config.accounts.bindings
    )


def assert_holding(nodes, state):
    assert entered(state) and state.mode == "running"
    assert_successful_outcomes(state)
    assert_cash_ledger(state)
    assert_exchange_views(state)
    for binding in state.config.fork_bindings:
        assert_chain_holding(nodes, state, binding.chain)


def assert_chain_holding(nodes, state, chain):
    from almanak.work import project

    scoped = project(state, chain)
    with EthereumReader(chain, nodes.for_chain(chain).rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        for owned in scoped.owned:
            if isinstance(owned, LendingOwnership):
                assert_owned_position(reader, scoped, owned, block)
        assert_wallet_balances(reader, scoped, block)


def assert_exchange_views(state):
    shared = {b.key: b.amount for b in state.inventory}
    for owned in state.owned:
        if not isinstance(owned, LendingOwnership):
            assert_currency_views(shared, owned)


def assert_currency_views(shared, owned):
    for value in (owned.inventory0, owned.inventory1):
        assert value.amount == shared[wallet_key(owned.target, value.asset)]


def assert_closed(nodes, state):
    assert state.mode == "closed" and settled(state)
    assert_successful_outcomes(state)
    assert_cash_ledger(state)
    assert_exchange_views(state)
    for before, after in closure_pairs(state):
        assert_closed_observation(state, before, after)
    measured = cash_evidence(state.closure)
    assert all(measured[b.key][1] == b.amount for b in state.inventory)


def closure_pairs(state):
    # Each chain appends its closure when it closes (`work.replace_scoped`), so the
    # closure follows chain completion order, not baseline order; pair by target.
    closure = {after.target: after for after in state.closure}
    assert len(closure) == len(state.closure) == len(state.baseline)
    return [(before, closure[before.target]) for before in state.baseline]


def assert_closed_observation(state, before, after):
    if isinstance(after, LendingSnapshot):
        assert_zero_obligations(after)
        assert_payer_effects(state, before, after)
    else:
        assert after.position is None and not after.needs_cleanup


def independent_ready(state, chain):
    from almanak.work import project

    local = project(state, chain)
    names = {owned.target.account for owned in local.owned}
    return settled(local) and all(state.memory.values.get("phases", {}).get(n) == 3 for n in names)


def assert_independent_closed(state, failed, healthy, pending):
    from almanak.work import project, work_for

    assert state.mode == "teardown" and work_for(state, failed).pending == pending
    assert work_for(state, healthy).closed
    local = project(state, healthy)
    for before, after in closure_pairs(local):
        assert_closed_observation(local, before, after)
    assert_cash_ledger(local)
    assert_exchange_views(local)
