"""Reusable Morpho references prove independent lender and borrower fund ownership."""

from almanak.connectors.morpho_blue.reader import read_account, read_market
from almanak.gateway.ethereum import EthereumReader
from tests.strategy_fork.morpho_authority import assert_permissions as assert_permissions
from tests.strategy_fork.pool_lending_lifecycle import (
    assert_payer_effects,
    assert_successful_outcomes,
    assert_wallet_effects,
    borrowed,
)

crash_operation = "borrow"


def entered(state):
    return (
        state.memory.values.get("phase") == 3
        and state.pending is None
        and state.plan is None
        and state.decision is None
        and state.delivered == len(state.outcomes)
    )


def assert_entry_positions(state):
    assert entered(state) and state.mode == "running"
    assert_successful_outcomes(state)
    owned = owned_by_allocation(state)
    assert owned["earn"].supply_shares > 0 and owned["loan"].borrow_shares > 0
    assert owned["collateral"].collateral == state.config.allocation("collateral").capital
    assert owned["earn"].target.fund != owned["loan"].target.fund
    return owned


def owned_by_allocation(state):
    return {item.target.allocation: item for item in state.owned}


def assert_operator_positions(reader, market, state):
    for binding in state.config.accounts.bindings:
        if binding.profile.submitter != binding.profile.fund:
            position = read_account(reader, market, binding.profile.submitter).position
            assert position.supply_shares == position.borrow_shares == position.collateral == 0


def assert_holding(node, state):
    owned = assert_entry_positions(state)
    source = state.config.allocation("loan").reserve.market
    with EthereumReader(source.chain, node.rpc.endpoint, local_transport=True) as reader:
        block = reader.pin_block("latest")
        market = read_market(reader, source, block)
        for name in ("earn", "loan"):
            position = read_account(reader, market, owned[name].target.fund).position
            assert (position.supply_shares, position.borrow_shares) == (
                owned[name].supply_shares,
                owned[name].borrow_shares,
            )
        assert_operator_positions(reader, market, state)
        loan = state.config.target("loan")
        proceeds = reader.call(
            loan.asset.reference,
            "balanceOf(address)",
            block,
            inputs=("address",),
            args=(loan.fund.address,),
        )[0]
        assert proceeds >= borrowed(state, "loan")


def assert_closed(node, state):
    assert (state.mode, state.pending, state.plan, state.decision) == ("closed", None, None, None)
    assert state.delivered == len(state.outcomes)
    assert_successful_outcomes(state)
    completed = [
        outcome.decision.intent.kind
        for outcome in state.outcomes
        if outcome.strategy_result().status == "completed"
    ]
    assert completed == [
        "supply",
        "supply",
        "borrow",
        "repay_amount",
        "withdraw_amount",
        "withdraw_amount",
        "repay",
        "withdraw",
        "withdraw",
    ]
    assert all(item.empty for item in state.owned)
    for before, after in zip(state.baseline, state.closure, strict=True):
        position = after.account.position
        assert (
            position.supply_shares
            == position.borrow_shares
            == position.collateral
            == after.allowance
            == 0
        )
        assert_wallet_effects(state, before, after)
        assert_payer_effects(state, before, after)
