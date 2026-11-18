"""Independent fund, operator and contract observations for both Uniswap samples."""

from almanak.connectors.uniswap_v4.observations import snapshot
from almanak.connectors.uniswap_v4.state import Mint
from almanak.gateway.ethereum import EthereumReader
from almanak.models import SafeRoles
from tests.fork.support import wait_state
from tests.fork.uniswap_support import market_trade
from tests.strategy_fork.aave_chain_assertions import settled as chain_settled


def settled(state):
    return chain_settled(state)


def assert_holding(node, state):
    from types import SimpleNamespace

    assert settled(state)
    with EthereumReader(state.owned[0].target.fund.chain, node.rpc.endpoint) as reader:
        block = reader.pin_block("latest")
        for owned in state.owned:
            observed = snapshot(
                SimpleNamespace(config=state.config, reader=reader),
                owned.target.allocation,
                block,
                owned,
            )
            assert_inventory(owned, observed)
    assert all(o.success and not o.accounting_issues for o in state.outcomes)


def assert_inventory(owned, observed):
    assert observed.wallet0.amount >= owned.inventory0.amount
    assert observed.wallet1.amount >= owned.inventory1.amount
    assert observed.position_liquidity == (owned.position.liquidity if owned.position else 0)


def advance(node, timestamp):
    node.rpc("evm_setNextBlockTimestamp", [timestamp])
    node.rpc("evm_mine", [])


def round_trip(state):
    return (
        settled(state)
        and all(
            o.entry_complete and o.position is None and counter_inventory(o).amount == 0
            for o in state.owned
        )
        and all(not o.after.needs_cleanup for o in state.outcomes[-1:])
    )


def timed_exit(node, journal, process, memory_key):
    state = journal.load()
    allocation = state.config.allocations[0]
    start = state.memory.values[memory_key][allocation.name]
    advance(node, start + allocation.hold_seconds)
    wait_state(journal, round_trip, process)


def collect_and_exit(node, journal, process, *, trader_index=63, amount=None, maximum_amount=None):
    state = journal.load()
    allocation = state.config.allocations[0]
    target = state.config.target(allocation.name)
    amount = retain_burn_probe(
        node,
        journal,
        state,
        target,
        trader_index=trader_index,
        amount=amount,
        maximum_amount=maximum_amount,
    )
    if allocation.collect_seconds is not None:
        collect_once(node, journal, process, state, allocation)
    with EthereumReader(target.fund.chain, node.rpc.endpoint) as reader:
        market_trade(node, target, reader, amount=amount, trader_index=trader_index)
    timed_exit(node, journal, process, "minted_at")
    burn = next(o for o in journal.load().outcomes if o.operation.kind == "burn")
    assert burn.fees0 + burn.fees1 > 0


def collect_once(node, journal, process, state, allocation):
    advance(node, state.memory.values["minted_at"][allocation.name] + allocation.collect_seconds)
    collected = wait_state(
        journal, lambda s: settled(s) and bool(s.memory.values.get("collected_at")), process
    )
    assert_collected(collected)


def retain_burn_probe(
    node,
    journal,
    state,
    target,
    *,
    trader_index=63,
    amount=None,
    maximum_amount=None,
):
    import json

    from tests.strategy_fork.lp_minima import check_burn_minima

    with EthereumReader(target.fund.chain, node.rpc.endpoint) as reader:
        if amount is None:
            amount = 100_000 if str(target.fund.chain.chain_id) == "eip155:43114" else 500_000_000
        fee_exercise = None
        if maximum_amount is not None:
            from dataclasses import asdict

            from tests.strategy_fork.lp_fee_exercise import observed_plan

            plan = observed_plan(reader, target, reader.pin_block("latest"), amount, maximum_amount)
            amount = plan.amount
            fee_exercise = asdict(plan)
        market_trade(node, target, reader, amount=amount, trader_index=trader_index)
        evidence = check_burn_minima(node, reader, state)
        evidence["fee_exercise"] = fee_exercise
        (journal.path.parent / "lp-burn-minima.json").write_text(json.dumps(evidence, indent=2))
    return amount


def assert_collected(state):
    outcomes = [o for o in state.outcomes if o.operation.kind == "collect"]
    assert len(outcomes) == 1 and outcomes[0].fees0 + outcomes[0].fees1 > 0
    assert state.owned[0].position == outcomes[0].after.owned.position


def assert_closed(node, state, assert_liquidity=None):
    assert state.mode == "closed" and round_trip(state)
    assert len({o.tx_hash for o in state.outcomes}) == len(state.outcomes)
    assert all(o.success and not o.accounting_issues for o in state.outcomes)
    for owned, before, closed in zip(state.owned, state.baseline, state.closure, strict=True):
        assert all(
            a.amount == 0 and (a.expiration is None or a.expiration <= closed.block.timestamp)
            for a in closed.allowances
        )
        assert counter_inventory(owned).amount == 0
        funding = next(
            v for v in (closed.wallet0, closed.wallet1) if v.asset == owned.target.funding_currency
        )
        assert funding.amount > 0
        assert closed.wallet0.amount == owned.inventory0.amount
        assert closed.wallet1.amount == owned.inventory1.amount
        effects = [o for o in state.outcomes if o.operation.target == owned.target]
        assert closed.wallet0.amount == before.wallet0.amount + sum(o.delta0 for o in effects)
        assert closed.wallet1.amount == before.wallet1.amount + sum(o.delta1 for o in effects)
        profile = state.config.accounts.binding(owned.target.account).profile
        if isinstance(profile, SafeRoles):
            assert profile.fund != profile.submitter
        assert all(
            o.execution.submitter == profile.submitter and o.call.fund == profile.fund
            for o in effects
        )
        assert before.native_balance.amount - closed.native_balance.amount == sum(
            o.gas_paid.amount for o in effects
        )
        assert [o.nonce for o in effects] == list(
            range(effects[0].nonce, effects[0].nonce + len(effects))
        )
        swaps = [o for o in effects if o.operation.kind == "swap"]
        assert len(swaps) == 2
        field = "delta0" if owned.target.funding_currency == owned.target.assets[0] else "delta1"
        assert getattr(swaps[0], field) < 0 < getattr(swaps[1], field)
        if any(isinstance(o.operation, Mint) for o in effects):
            (assert_liquidity or assert_fixed_liquidity)(owned.target, effects)


def assert_closed_band(node, state):
    assert_closed(node, state, assert_band_liquidity)


def liquidity_kinds(effects):
    return [o.operation.kind for o in effects if o.operation.kind in ("mint", "collect", "burn")]


def assert_fixed_liquidity(target, effects):
    assert liquidity_kinds(effects) == ["mint", "collect", "burn"]


def assert_band_liquidity(target, effects):
    """Each cycle mints one band-width position and burns it; the burn collects its fees."""
    kinds = liquidity_kinds(effects)
    assert kinds and kinds == ["mint", "burn"] * (len(kinds) // 2)
    for outcome in effects:
        if outcome.operation.kind in ("mint", "burn"):
            target.band.validate_range(outcome.operation.range, target.pool.tick_spacing)


def counter_inventory(owned):
    return next(
        v for v in (owned.inventory0, owned.inventory1) if v.asset != owned.target.funding_currency
    )
