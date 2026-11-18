"""Actual configured collateral and debt lifecycles, scoped by chain and fund."""

from almanak.connectors.aave_v4.reader import read_position
from almanak.gateway.ethereum import EthereumReader
from almanak.models import SafeRoles
from tests.fork.aave_assertions import assert_one_supply_log
from tests.fork.support import wait_state
from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import (
    assert_capital,
    assert_drawn_ownership,
    assert_nonce_scopes,
    assert_payer_gas,
    assert_repayment_cost,
    assert_zero_obligations,
    borrowed_assets,
)


def settled(state):
    slots = state.work or (state,)
    return all(
        s.initialized and s.pending is None and s.plan is None and s.decision is None for s in slots
    ) and state.delivered == len(state.outcomes)


def entered(state):
    return settled(state) and all(
        name in state.memory.values.get(field, {})
        for field, items in (
            ("supplied_at", state.config.allocations),
            ("borrowed_at", state.config.debts),
        )
        for name in (item.name for item in items)
    )


def selected_node(nodes, chain):
    return nodes.for_chain(chain) if hasattr(nodes, "for_chain") else nodes


def assert_holding(nodes, state):
    assert entered(state) and state.mode == "running"
    for allocation in state.config.positions:
        node = selected_node(nodes, allocation.chain)
        assert_position_holding(node, state, allocation)


def assert_position_holding(node, state, allocation):
    with EthereumReader(allocation.chain, node.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        target = state.config.target(allocation.name)
        position = read_position(reader, allocation.reserve, target.fund, block)
        if allocation in state.config.allocations:
            assert position.supplied_shares == state.shares(target) > 0
            assert position.total_debt == 0
            assert_one_supply_log(node, state, allocation)
        else:
            assert_debt_holding(state, reader, block, allocation, position)


def assert_debt_holding(state, reader, block, allocation, position):
    assert position.total_debt >= borrowed_assets(state, allocation.name)
    assert_drawn_ownership(state, reader, block, allocation)
    profile = state.config.accounts.binding(allocation.account).profile
    if isinstance(profile, SafeRoles):
        assert read_position(reader, allocation.reserve, profile.operator, block).total_debt == 0


def assert_closed(nodes, state):
    assert state.mode == "closed" and settled(state)
    assert_execution_effects(state)
    assert_nonce_scopes(state)
    for closed in state.closure:
        assert_zero_obligations(closed)
        assert_capital(state, closed)
        assert_payer_gas(state, closed)
    for allocation in state.config.allocations:
        assert_one_supply_log(selected_node(nodes, allocation.chain), state, allocation)
    for debt in state.config.debts:
        assert_repayment_cost(state, debt)


def assert_execution_effects(state):
    assert all(o.success and not o.accounting_issues for o in state.outcomes)
    assert len({(o.operation.target.fund.chain, o.tx_hash) for o in state.outcomes}) == len(
        state.outcomes
    )


def exercise_hold(nodes, journal, process):
    state = journal.load()
    for allocation in state.config.allocations:
        node = selected_node(nodes, allocation.chain)
        started = state.memory.values["supplied_at"][allocation.name]
        node.rpc("evm_setNextBlockTimestamp", [started + allocation.hold_seconds])
        node.rpc("evm_mine")
    wait_state(journal, exited, process)


def exited(state):
    return settled(state) and all(
        not (
            any(
                (
                    o.shares,
                    o.drawn_shares,
                    o.premium_shares,
                    o.premium_offset_ray,
                    o.collateral_enabled,
                )
            )
        )
        for o in state.owned
    )
