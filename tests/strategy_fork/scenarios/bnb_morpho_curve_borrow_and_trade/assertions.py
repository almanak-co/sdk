"""Prove explicit lender liquidity and independently owned borrower trading effects."""

from almanak.connectors.composed.inventory import cash_evidence
from almanak.connectors.identifiers import ConnectorId
from almanak.gateway.ethereum import EthereumReader
from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.lending_trade_assertions import (
    assert_entry_swap,
    assert_inventory_movements,
    assert_measured_holding,
    assert_operator_empty,
    assert_successful_outcomes,
    measured_position,
)
from tests.strategy_fork.lending_trade_assertions import (
    assert_permissions as assert_permissions,
)

crash_operation = "swap"
# Production EOA rehearsal entry: the 180 s default entry wait expired after
# 7 successful entry outcomes in hosted Fork Qualification run 35981866228;
# the supervised lifecycle entry of this reference spanned 178 to 180 s, and
# the rehearsal entered within 132 s under this wait in run 35984947873.
entry_timeout = 420
teardown_timeout = 420


def entered(state):
    return settled(state) and state.memory.values.get("phase") == 4


def assert_holding(node, state):
    assert (entered(state), state.mode, state.memory.values["traded"]) == (True, "running", True)
    assert_successful_outcomes(state)
    held = {position.target.allocation: position for position in state.owned}
    assert held["earn"].supply_shares > 0 and held["loan"].borrow_shares > 0
    assert held["collateral"].collateral == state.config.allocation("collateral").capital
    assert_entry_swap(state)
    assert_inventory_movements(state)
    assert_measured_holding(node, state)
    assert_lender_holding(node, state, held["earn"])


def assert_lender_holding(node, state, owned):
    target = owned.target
    with EthereumReader(target.fund.chain, node.rpc.endpoint, local_transport=True) as reader:
        block = reader.pin_block("latest")
        account = measured_position(
            reader, target.reserve.market, target.fund, block, ConnectorId("morpho_blue")
        )
        assert account.position.supply_shares == owned.supply_shares
        assert account.position.borrow_shares == account.position.collateral == 0
        profile = state.config.accounts.binding("lender").profile
        if profile.submitter != target.fund:
            operator = measured_position(
                reader, target.reserve.market, profile.submitter, block, ConnectorId("morpho_blue")
            )
            assert_operator_empty(operator, ConnectorId("morpho_blue"))


def assert_closed(node, state):
    assert state.mode == "closed" and settled(state)
    assert_successful_outcomes(state)
    assert_inventory_movements(state)
    assert not state.memory.values["traded"]
    assert_lending_closed(state)
    assert_exchange_cash_closed(state)
    assert_lifecycle_order(state)


def assert_lending_closed(state):
    assert all(position.empty for position in state.owned[:-1])
    for observation in state.closure[:-1]:
        assert observation.allowance == 0
        assert_operator_empty(observation.account, ConnectorId("morpho_blue"))


def assert_exchange_cash_closed(state):
    exchange = state.closure[-1]
    assert exchange.position is None and not exchange.needs_cleanup
    measured = cash_evidence(state.closure)
    assert all(measured[balance.key][1] == balance.amount for balance in state.inventory)


def assert_lifecycle_order(state):
    completed = [
        (outcome.decision.intent.kind, str(outcome.decision.intent.target.allocation))
        for outcome in state.outcomes
        if outcome.strategy_result().complete
    ]
    assert completed == [
        ("supply", "earn"),
        ("supply", "collateral"),
        ("borrow", "loan"),
        ("swap_exact_input", "trade"),
        ("exit_exchange", "trade"),
        ("repay", "loan"),
        ("withdraw", "collateral"),
        ("withdraw", "earn"),
    ]
