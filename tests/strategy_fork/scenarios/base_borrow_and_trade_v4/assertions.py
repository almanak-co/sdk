"""The actual mixed reference retains debt and one shared fund cash ledger."""

from almanak.connectors.composed.inventory import cash_evidence
from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.composed_v4_authority import assert_permissions as assert_permissions
from tests.strategy_fork.scenarios.aave_v3_ethereum_lending.assertions import (
    assert_payer_effects,
    assert_successful_outcomes,
    assert_zero_obligations,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_holding as assert_holding,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import (
    assert_inventory_movements,
)
from tests.strategy_fork.scenarios.base_borrow_and_trade.assertions import entered as entered

crash_operation = "swap"
# Production EOA rehearsal bounds follow bnb_aave_borrow_and_trade_v4, which has the
# same connector composition and the same entry, hold and teardown step sequence. In
# hosted run 35980345945 that sibling took at most 63 s from the supply decision to the
# last entry decision and 212 s from the first teardown decision to the withdraw
# decision.
entry_timeout = 600
teardown_timeout = 1500
hold_timeout = 180


def assert_closed(node, state):
    assert (state.mode, settled(state)) == ("closed", True)
    assert_successful_outcomes(state)
    assert_inventory_movements(state)
    assert_lending_closed(state)
    assert_exchange_closed(state)
    assert_completed_lifecycle(state)


def assert_lending_closed(state):
    for before, after in zip(state.baseline[:2], state.closure[:2], strict=True):
        assert_zero_obligations(after)
        assert_payer_effects(state, before, after)


def assert_exchange_closed(state):
    exchange = state.closure[-1]
    assert exchange.owned.position is None and not exchange.needs_cleanup
    measured = cash_evidence(state.closure)
    assert all(measured[b.key][1] == b.amount for b in state.inventory)


def completed_intents(state):
    return [o.decision.intent.kind for o in state.outcomes if o.strategy_result().complete]


def assert_completed_lifecycle(state):
    completed = completed_intents(state)
    # Permit2 expiration may retain cleanup authority after exact swap amounts.
    assert [kind for kind in completed if kind != "revoke_exchange"] == [
        "supply",
        "borrow",
        "swap_exact_input",
        "exit_exchange",
        "repay",
        "withdraw",
    ]
    assert not state.memory.values["traded"]
