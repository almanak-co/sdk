"""Prove one wallet receives lending and trading effects through the shared gateway."""

from almanak.connectors.aave_v3.observations import Ownership as LendingOwnership
from almanak.connectors.aave_v3.reader import balance
from almanak.connectors.composed.inventory import cash_evidence
from almanak.connectors.composed.models import wallet_key
from almanak.gateway.ethereum import EthereumReader
from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.composed_authority import assert_permissions as assert_permissions
from tests.strategy_fork.scenarios.aave_v3_ethereum_lending.assertions import (
    assert_owned_position,
    assert_payer_effects,
    assert_successful_outcomes,
    assert_zero_obligations,
)

crash_operation = "swap"
# Production EOA rehearsal bounds follow bnb_aave_borrow_and_trade, which has the same
# connector composition and the same entry, hold and teardown step sequence. In hosted
# run 35980345945 that sibling took at most 60 s from the supply decision to the last
# entry decision and 144 s from the first teardown decision to the withdraw decision.
entry_timeout = 900
teardown_timeout = 1800
hold_timeout = 180


def entered(state):
    return settled(state) and state.memory.values.get("phase") == 3


def assert_holding(node, state):
    assert (entered(state), state.mode, state.memory.values["traded"]) == (True, "running", True)
    assert_successful_outcomes(state)
    assert min(state.owned[0].supply_scaled, state.owned[1].debt_scaled) > 0
    assert_entry_swap(state)
    assert_inventory_movements(state)
    assert_measured_holding(node, state)


def assert_entry_swap(state):
    swaps = [o for o in state.outcomes if o.operation.kind == "swap"]
    assert len(swaps) == 1
    assert (swaps[0].operation.amount.asset, swaps[0].operation.amount.amount) == (
        state.config.target("loan").asset,
        trade_amount(state.config.allocation("trade")),
    )


def trade_amount(allocation):
    from almanak.asset_amounts import AssetAmounts

    if isinstance(allocation.capital, AssetAmounts):
        return next(
            v.amount for v in allocation.capital.root if v.asset == allocation.funding_asset
        )
    return allocation.capital.amount


def assert_measured_holding(node, state):
    chain = state.config.target("loan").fund.chain
    with EthereumReader(chain, node.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        for owned in state.owned:
            if isinstance(owned, LendingOwnership):
                assert_owned_position(reader, state, owned, block)
        assert_wallet_balances(reader, state, block)


def assert_wallet_balances(reader, state, block):
    for owned_cash in state.inventory:
        fund = state.config.accounts.binding(owned_cash.key.account).profile.fund
        assert balance(reader, owned_cash.key.asset, fund.address, block) == owned_cash.amount


def cash_delta(outcome, key):
    if outcome.operation.target.account != key.account:
        return 0
    if hasattr(outcome, "assets"):
        if outcome.operation.target.asset != key.asset:
            return 0
        signs = {"supply": -1, "withdraw": 1, "borrow": 1, "repay": -1}
        return signs.get(outcome.operation.kind, 0) * outcome.assets
    if hasattr(outcome, "asset_changes"):
        return next((v.amount for v in outcome.asset_changes if v.key.asset == key.asset), 0)
    return dict(
        zip(outcome.operation.target.assets, (outcome.delta0, outcome.delta1), strict=True)
    ).get(key.asset, 0)


def assert_inventory_movements(state):
    assert_cash_ledger(state)
    assert_exchange_views(state)


def assert_cash_ledger(state):
    initial = {b.key: b.amount for b in state.config.inventory}
    for owned_cash in state.inventory:
        assert owned_cash.amount == initial[owned_cash.key] + sum(
            cash_delta(outcome, owned_cash.key) for outcome in state.outcomes
        )


def exchange_position(state):
    return next(owned for owned in state.owned if owned.target.allocation == "trade")


def exchange_inventory_values(exchange):
    if hasattr(exchange, "inventory"):
        return exchange.inventory.root
    return (exchange.inventory0, exchange.inventory1)


def assert_exchange_views(state):
    exchange = exchange_position(state)
    shared = {b.key: b.amount for b in state.inventory}
    for value in exchange_inventory_values(exchange):
        assert value.amount == shared[wallet_key(exchange.target, value.asset)]


def assert_closed(node, state):
    assert (state.mode, settled(state)) == ("closed", True)
    assert_successful_outcomes(state)
    assert_inventory_movements(state)
    assert_closed_positions(state)
    assert_roundtrip_order(state)
    assert not state.memory.values["traded"]


def assert_closed_positions(state):
    for before, after in zip(state.baseline[:2], state.closure[:2], strict=True):
        assert_zero_obligations(after)
        assert_payer_effects(state, before, after)
    exchange = state.closure[-1]
    assert exchange.position is None and not exchange.needs_cleanup
    measured = cash_evidence(state.closure)
    assert all(measured[b.key][1] == b.amount for b in state.inventory)


def assert_roundtrip_order(state):
    completed = [o.decision.intent.kind for o in state.outcomes if o.strategy_result().complete]
    assert completed == [
        "supply",
        "borrow",
        "swap_exact_input",
        "exit_exchange",
        "repay",
        "withdraw",
    ]
