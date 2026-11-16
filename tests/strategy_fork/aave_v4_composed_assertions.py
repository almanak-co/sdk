"""Actual V4 composition retains native debt and reconciles the single cash ledger."""

from almanak.connectors.aave_v4.models import Ownership
from almanak.connectors.aave_v4.reader import read_position
from almanak.connectors.composed.inventory import cash_evidence
from almanak.gateway.ethereum import EthereumReader
from almanak.models import SafeRoles
from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.scenarios.aave_v3_ethereum_lending.assertions import (
    assert_successful_outcomes,
)
from tests.strategy_fork.scenarios.aave_v4_weth_supply.assertions import (
    assert_nonce_scopes,
    assert_payer_gas,
    assert_zero_obligations,
    borrowed_assets,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_entry_swap,
    assert_inventory_movements,
    assert_wallet_balances,
)


def entered(state):
    return settled(state) and state.memory.values.get("phase") == 3


def assert_holding(node, state):
    assert (entered(state), state.mode, state.memory.values["traded"]) == (True, "running", True)
    assert_successful_outcomes(state)
    collateral = owned_position(state, "collateral")
    loan = owned_position(state, "loan")
    assert collateral.shares > 0 and loan.drawn_shares > 0
    assert collateral.collateral_enabled
    assert_entry_swap(state)
    assert_inventory_movements(state)
    assert_chain_positions(node, state)


def assert_chain_positions(node, state):
    chain = state.config.target("loan").fund.chain
    with EthereumReader(chain, node.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        for owned in state.owned:
            if isinstance(owned, Ownership):
                assert_protocol_position(reader, state, owned, block)
        assert_wallet_balances(reader, state, block)


def owned_position(state, allocation):
    matches = [
        owned
        for owned in state.owned
        if isinstance(owned, Ownership) and owned.target.allocation == allocation
    ]
    assert len(matches) == 1
    return matches[0]


def assert_protocol_position(reader, state, owned, block):
    target = owned.target
    observed = read_position(reader, target.reserve, target.fund, block)
    assert observed.supplied_shares == owned.shares
    if target.allocation == "loan":
        assert observed.total_debt >= borrowed_assets(state, "loan")
    assert_operator_position(reader, state, target, block)


def assert_operator_position(reader, state, target, block):
    """A Safe's operator holds no supply or debt in the reserve the Safe uses."""
    profile = state.config.accounts.binding(target.account).profile
    if isinstance(profile, SafeRoles):
        operator = read_position(reader, target.reserve, profile.operator, block)
        assert (operator.supplied_shares, operator.total_debt) == (0, 0)


def assert_closed(node, state):
    assert (state.mode, settled(state)) == ("closed", True)
    assert_successful_outcomes(state)
    assert_inventory_movements(state)
    assert_nonce_scopes(state)
    assert_closed_allocations(state)
    assert_closed_exchange(state)
    assert_outcome_order(state)
    assert not state.memory.values["traded"]


def assert_closed_allocations(state):
    for allocation in ("collateral", "loan"):
        closed = closed_position(state, allocation)
        assert_zero_obligations(closed)
        assert_payer_gas(state, closed)


def assert_closed_exchange(state):
    exchange = closed_position(state, "trade")
    assert exchange.owned.position is None and not exchange.needs_cleanup
    measured = cash_evidence(state.closure)
    assert all(measured[b.key][1] == b.amount for b in state.inventory)


def assert_outcome_order(state):
    completed = [o.decision.intent.kind for o in state.outcomes if o.strategy_result().complete]
    assert [kind for kind in completed if kind != "revoke_exchange"] == [
        "supply",
        "borrow",
        "swap_exact_input",
        "exit_exchange",
        "repay",
        "withdraw",
    ]


def closed_position(state, allocation):
    matches = [closed for closed in state.closure if closure_allocation(closed) == allocation]
    assert len(matches) == 1
    return matches[0]


def closure_allocation(closed):
    direct = getattr(closed, "allocation", None)
    return direct if direct is not None else closed.owned.target.allocation
