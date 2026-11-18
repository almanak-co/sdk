"""Independent coin, LP and shared-payer checks for the actual Curve references."""

from almanak.gateway.ethereum import EthereumReader
from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.v3_pool_assertions import assert_successful_outcomes


def entered(state):
    return settled(state) and all(position_entered(state, owned) for owned in state.owned)


def position_entered(state, owned):
    if not owned.entry_complete:
        return False
    if not owned.target.liquidity_enabled:
        return trading_inventory(owned)
    completed = state.memory.values.get("completed", {}).get(owned.target.allocation, ())
    return owned.position is not None and "decrease_liquidity" in completed


def trading_inventory(owned):
    return any(v.amount for v in owned.inventory.root if v.asset != owned.target.funding_asset)


def assert_holding(node, state):
    assert (entered(state), state.mode) == (True, "running")
    assert_successful_outcomes(state)
    with EthereumReader(state.owned[0].target.fund.chain, node.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        for owned in state.owned:
            assert_inventory(reader, block, owned)
            assert_shares(reader, block, owned)


def balance(reader, block, token, fund):
    return reader.call(
        token, "balanceOf(address)", block, inputs=("address",), args=(fund.address,)
    )[0]


def assert_inventory(reader, block, owned):
    for value in owned.inventory.root:
        assert balance(reader, block, value.asset.reference, owned.target.fund) == value.amount


def assert_shares(reader, block, owned):
    shares = balance(reader, block, owned.target.pool.pool, owned.target.fund)
    assert shares == (owned.position.shares.amount if owned.position else 0)


def assert_closed(node, state):
    assert (state.mode, settled(state)) == ("closed", True)
    assert_successful_outcomes(state)
    for owned, before, after in zip(state.owned, state.baseline, state.closure, strict=True):
        assert_allocation_closed(state, owned, before, after)


def assert_cleanup(owned, after):
    assert (owned.entry_complete, owned.position, after.position, after.needs_cleanup) == (
        True,
        None,
        None,
        False,
    )
    assert after.wallets == owned.inventory
    assert not trading_inventory(owned)


def assert_allocation_closed(state, owned, before, after):
    assert_cleanup(owned, after)
    events = [o for o in state.outcomes if o.operation.target == owned.target]
    assert_cash(events, before, after)
    assert_operations(owned.target, events)
    assert_payer(state, before, after)


def assert_cash(events, before, after):
    for index, (old, new) in enumerate(zip(before.wallets.root, after.wallets.root, strict=True)):
        assert new.amount == old.amount + sum(o.asset_changes[index].amount for o in events)
    assert sum(o.share_change.amount for o in events) == 0


def assert_operations(target, events):
    swaps = [o for o in events if o.operation.kind == "swap"]
    assert_swap_directions(target, swaps)
    if target.liquidity_enabled:
        assert_liquidity_operations(target, liquidity_events(events))
    else:
        assert len(swaps) == 2


def assert_swap_directions(target, swaps):
    assert swaps[0].operation.amount.asset == target.funding_asset
    # A single-coin LP exit can already return all principal to the funding asset.
    for outcome in swaps[1:]:
        assert outcome.operation.minimum.asset == target.funding_asset


def liquidity_events(events):
    return [o for o in events if o.operation.kind not in ("approve", "swap")]


def assert_liquidity_operations(target, liquidity):
    assert [o.operation.kind for o in liquidity] == ["mint", "increase", "decrease", "close"]
    assert all(o.operation.output_asset == target.single_coin_exit for o in liquidity[2:])


def assert_payer(state, before, after):
    payer = before.payer
    events = [o for o in state.outcomes if o.execution.submitter == payer]
    assert_gas_cost(events, before, after)
    assert_submitter_sequence(events)


def assert_gas_cost(events, before, after):
    assert before.native_balance.amount - after.native_balance.amount == sum(
        o.gas_paid.amount for o in events
    )


def assert_submitter_sequence(events):
    assert [o.nonce for o in events] == list(range(events[0].nonce, events[0].nonce + len(events)))
    assert all(o.call.fund == o.operation.target.fund for o in events)
