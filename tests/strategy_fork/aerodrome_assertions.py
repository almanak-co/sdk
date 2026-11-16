"""Independent fund cash, fungible shares, stored claims and payer closure checks."""

from types import SimpleNamespace

from almanak.connectors.aerodrome.observations import snapshot
from almanak.gateway.ethereum import EthereumReader
from tests.strategy_fork.aave_chain_assertions import settled
from tests.strategy_fork.v3_pool_assertions import (
    assert_closed_position,
    assert_payer,
    assert_successful_outcomes,
    assert_swap_directions,
    assert_wallet_effects,
    counter_inventory,
    position_effects,
)


def entered(state):
    if not settled(state):
        return False
    return all(position_entered(state, owned) for owned in state.owned)


def position_entered(state, owned):
    if not owned.entry_complete:
        return False
    if not owned.target.liquidity_enabled:
        return counter_inventory(owned).amount > 0
    completed = state.memory.values.get("completed", {}).get(owned.target.allocation, ())
    return owned.position is not None and "collect_liquidity_fees" in completed


def assert_holding(node, state):
    assert (entered(state), state.mode) == (True, "running")
    assert_successful_outcomes(state)
    with EthereumReader(state.owned[0].target.fund.chain, node.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        backend = SimpleNamespace(config=state.config, reader=reader)
        for owned in state.owned:
            observed = snapshot(backend, owned.target.allocation, block, owned, quotes=False)
            assert (observed.wallet0, observed.wallet1, observed.position) == (
                owned.inventory0,
                owned.inventory1,
                owned.position,
            )


def assert_closed(node, state):
    assert (state.mode, settled(state)) == ("closed", True)
    assert_successful_outcomes(state)
    for owned, before, after in zip(state.owned, state.baseline, state.closure, strict=True):
        assert_closed_position(owned, after)
        events = position_effects(state, owned.target)
        assert_wallet_effects(events, before, after)
        assert_payer(state, owned.target, before, after, events)
        assert_swap_directions(owned.target, events)
        if owned.target.liquidity_enabled:
            assert_liquidity(events)


def assert_liquidity(events):
    operations = [o for o in events if o.operation.kind not in ("approve", "swap")]
    assert [o.operation.kind for o in operations[:5]] == [
        "mint",
        "increase",
        "decrease",
        "collect",
        "close",
    ]
    assert all(o.operation.kind == "collect" for o in operations[5:])
    assert sum(o.shares_delta for o in events) == 0
    for outcome in operations:
        if outcome.operation.kind == "collect":
            assert (outcome.delta0, outcome.delta1) == (outcome.fees0, outcome.fees1)
