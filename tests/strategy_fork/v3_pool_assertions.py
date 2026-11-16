"""Independent V3 fund, NFT, payer and closure checks for reusable references."""

from types import SimpleNamespace

from almanak.connectors.composition import connector_for
from almanak.gateway.ethereum import EthereumReader
from tests.strategy_fork.aave_chain_assertions import settled


def refresh_fork_head(node, journal, running):
    """Mine one empty block so a teardown plans from the current fork time.

    Automine leaves the head at the last transaction's block through the hold, while the
    next mined block takes the current time. A close whose calldata deadline is the stale
    head plus `deadline_seconds` is then already expired when it is mined, once the held
    lifecycle (three verified holds at a 60 s interval) outlasts that deadline.
    """
    node.rpc("evm_mine", [])


def entered(state):
    if not settled(state):
        return False
    return all(position_entered(state, owned) for owned in state.owned)


def position_entered(state, owned):
    if not owned.entry_complete or counter_inventory(owned).amount == 0:
        return False
    if owned.target.range is None:
        return True
    completed = state.memory.values.get("completed", {}).get(owned.target.allocation, ())
    return owned.position is not None and "collect_liquidity_fees" in completed


def counter_inventory(owned):
    return next(
        value
        for value in (owned.inventory0, owned.inventory1)
        if value.asset != owned.target.funding_asset
    )


def assert_holding(node, state):
    assert (entered(state), state.mode) == (True, "running")
    assert_successful_outcomes(state)
    snapshot = connector_for(state.config).execution.snapshot
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


def assert_successful_outcomes(state):
    assert all((o.success, o.accounting_issues) == (True, ()) for o in state.outcomes)
    assert len({o.tx_hash for o in state.outcomes}) == len(state.outcomes)


def assert_closed(node, state):
    assert (state.mode, settled(state)) == ("closed", True)
    assert_successful_outcomes(state)
    for owned, before, after in zip(state.owned, state.baseline, state.closure, strict=True):
        assert_closed_position(owned, after)
        effects = position_effects(state, owned.target)
        assert_wallet_effects(effects, before, after)
        assert_payer(state, owned.target, before, after, effects)
        assert_operations(owned.target, effects)


def position_effects(state, target):
    return [o for o in state.outcomes if o.operation.target == target]


def assert_closed_position(owned, after):
    assert (owned.entry_complete, owned.position, after.position) == (True, None, None)
    assert (counter_inventory(owned).amount, after.needs_cleanup) == (0, False)
    assert (after.wallet0, after.wallet1) == (owned.inventory0, owned.inventory1)


def assert_wallet_effects(effects, before, after):
    assert after.wallet0.amount == before.wallet0.amount + sum(o.delta0 for o in effects)
    assert after.wallet1.amount == before.wallet1.amount + sum(o.delta1 for o in effects)


def assert_payer(state, target, before, after, effects):
    profile = state.config.accounts.binding(target.account).profile
    assert all(
        o.execution.submitter == profile.submitter and o.call.fund == profile.fund for o in effects
    )
    assert before.native_balance.amount - after.native_balance.amount == sum(
        o.gas_paid.amount for o in effects
    )
    assert [o.nonce for o in effects] == list(
        range(effects[0].nonce, effects[0].nonce + len(effects))
    )


def assert_operations(target, effects):
    assert_swap_directions(target, effects)
    if target.range is not None:
        assert [
            o.operation.kind for o in effects if o.operation.kind not in ("approve", "swap")
        ] == ["mint", "increase", "decrease", "collect", "close"]


def assert_swap_directions(target, effects):
    swaps = [o for o in effects if o.operation.kind == "swap"]
    entry, exit = swaps
    field = ("delta0", "delta1")[target.assets.index(target.funding_asset)]
    assert getattr(entry, field) < 0 < getattr(exit, field)
