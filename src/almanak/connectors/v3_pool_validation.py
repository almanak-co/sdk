"""Gateway checks economic authority before even its first approval is submitted."""

from almanak.connectors.concentrated_math import amounts
from almanak.connectors.refusals import below_entry_minimum
from almanak.connectors.v3_pool_authority import admit, admit_centered
from almanak.connectors.v3_pool_operations import (
    Approval,
    Close,
    Collect,
    Decrease,
    Increase,
    Mint,
    Swap,
)
from almanak.connectors.v3_pool_planning import sqrt_bounds
from almanak.dex_intents import (
    CloseLiquidity,
    DecreaseLiquidity,
    ExitExchange,
    IncreaseLiquidity,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.gateway.ethereum import DataError
from almanak.transaction_family import is_signed


def validate_owned(owned, before):
    if owned != before.owned or owned.position != before.position:
        raise DataError(
            "ownership", "observed inventory or NFT checkpoint differs from retained ownership"
        )
    if any(
        wallet.amount < inventory.amount
        for wallet, inventory in zip(
            (before.wallet0, before.wallet1), (owned.inventory0, owned.inventory1), strict=True
        )
    ):
        raise DataError("inventory", "fund wallet does not cover attributed inventory")


def deadline(intent, before, allocation):
    if isinstance(intent, (RevokeExchange, ExitExchange)):
        return
    if (
        not before.block.timestamp
        < intent.deadline
        <= before.block.timestamp + allocation.deadline_seconds
    ):
        raise DataError("stale_plan", "retained deadline expired or exceeds configured policy")


def price_movement(previous, current, tolerance):
    old, new = previous.pool.sqrt_price_x96**2, current.pool.sqrt_price_x96**2
    if abs(new - old) * 10000 > old * tolerance:
        raise DataError(
            "price_movement", "pool price moved beyond the retained liquidity tolerance"
        )


def decision_observation(state, target):
    values = [value for value in state.decision.observations if value.target == target]
    if len(values) != 1 or values[0].simulated:
        raise ValueError("execution requires one retained on-chain decision observation")
    return values[0]


def swap_limits(intent, decision, allocation):
    quotes = [
        q
        for q in (decision.entry_quote, decision.exit_quote, decision.rebalance_quote)
        if q is not None and q.amount_in == intent.amount
    ]
    if not quotes or any(q.calculation != "contract_quoter" for q in quotes):
        raise ValueError("swap requires its exact-size retained contract quote")
    floor = max(
        max(1, q.amount_out.amount * (10000 - allocation.slippage_bps) // 10000) for q in quotes
    )
    if intent.minimum_output.amount < floor:
        raise ValueError("swap minimum widens the configured slippage limit")


def removal_limits(intent, decision, allocation):
    position = decision.owned.position
    if position is None:
        raise ValueError("liquidity removal requires a retained decision NFT")
    liquidity = intent.liquidity if isinstance(intent, DecreaseLiquidity) else position.liquidity
    expected = amounts(
        liquidity, decision.pool.sqrt_price_x96, *sqrt_bounds(intent.target, position=position)
    )
    if any(
        minimum.amount < principal * (10000 - allocation.slippage_bps) // 10000
        for minimum, principal in zip((intent.minimum0, intent.minimum1), expected, strict=True)
    ):
        raise ValueError("liquidity removal minima widen the configured slippage limit")


def requested(intent, before):
    """Input amounts an intent draws from the allocation's attributed inventory."""
    if isinstance(intent, SwapExactInput):
        return (intent.amount,)
    if isinstance(intent, (OpenLiquidity, IncreaseLiquidity)):
        return (intent.maximum0, intent.maximum1)
    if (
        isinstance(intent, ExitExchange)
        and before.owned.position is None
        and before.exit_quote is not None
    ):
        return (before.exit_quote.amount_in,)
    return ()


def validate_inventory(intent, before):
    """An economic action spends only inventory attributed to its allocation.

    There is no cumulative budget (blueprint 27): inventory bounds every input.
    """
    inventory = {a.asset: a.amount for a in (before.owned.inventory0, before.owned.inventory1)}
    if any(value.amount > inventory[value.asset] for value in requested(intent, before)):
        raise DataError("inventory", "economic action exceeds attributed input inventory")


def increases_exposure(config, operation):
    if isinstance(operation, (Close, Decrease, Collect)):
        return False
    if isinstance(operation, Approval):
        return operation.amount > 0 and (
            operation.spender == operation.target.contracts.position_manager
            or operation.asset == operation.target.funding_currency
        )
    if isinstance(operation, Swap):
        return operation.amount.asset == operation.target.funding_currency
    return isinstance(operation, (Mint, Increase))


def validate_entry(state, before, allocation):
    if state.mode != "running" or any(o.accounting_issues for o in state.outcomes):
        raise DataError("accounting", "new exposure requires a running reconciled deployment")
    if (
        before.native_balance is None
        or before.native_balance.amount < allocation.minimum_native_balance
    ):
        raise below_entry_minimum(before.native_balance, allocation.minimum_native_balance)


def validate_operation(backend, state, operation, before, planned_operation):
    if before.simulated or state.decision is None or state.capabilities is None:
        raise ValueError(
            "execution requires an on-chain observation and retained decision authority"
        )
    intent = state.decision.intent
    admit(state.capabilities, intent)
    if (
        operation.target != state.config.target(operation.allocation)
        or before.target != operation.target
    ):
        raise ValueError("operation and observation require the bound target")
    owned = next(o for o in state.owned if o.target == operation.target)
    validate_owned(owned, before)
    allocation = state.config.allocation(operation.allocation)
    deadline(intent, before, allocation)
    decision = decision_observation(state, operation.target)
    validate_inventory(intent, before)
    if isinstance(intent, SwapExactInput):
        swap_limits(intent, decision, allocation)
    if isinstance(intent, (OpenLiquidity, IncreaseLiquidity)):
        price_movement(decision, before, allocation.slippage_bps)
    if isinstance(intent, OpenLiquidity):
        admit_centered(intent, decision.pool.tick)
    if isinstance(intent, (CloseLiquidity, DecreaseLiquidity)):
        removal_limits(intent, decision, allocation)
    if operation != planned_operation(state.config, state, intent, before):
        raise ValueError("operation differs from its retained economic intent or NFT")
    if increases_exposure(state.config, operation):
        validate_entry(state, before, allocation)


def deadline_reached(operation, block) -> bool:
    """Whether preparation or signing at this head is too late for the prepared deadline."""
    return not isinstance(operation, Approval) and operation.deadline <= block.timestamp


def expired(operation, block) -> bool:
    """True once the signed operation's protocol deadline has passed; it can then only revert.

    Periphery refuses only a block after the deadline. Consecutive blocks can share a
    timestamp (Arbitrum, BNB), so a head at the deadline does not prove it dead. A
    standalone collect calls the position manager's `collect`, which takes no deadline,
    so its signed bytes stay executable and are never released.
    """
    return not isinstance(operation, (Approval, Collect)) and operation.deadline < block.timestamp


def validate_current(backend, plan, current):
    if current.simulated:
        raise ValueError("simulated observations cannot authorize submission")
    validate_owned(plan.before.owned, current)
    if plan.before.allowances != current.allowances:
        raise DataError("precondition_changed", "spending authority changed after preparation")
    allocation = backend.config.allocation(plan.operation.allocation)
    # The intent's admission window bounds planning and signing. Recorded signed bytes
    # are bounded only by their own protocol deadline (`expired`): an approval has none,
    # so it stays rebroadcastable after its intent expires instead of being refused forever.
    # Before signing, the prepared deadline still bounds every operation that carries one.
    if not is_signed(plan):
        deadline(plan.decision.intent, current, allocation)
    if (
        expired(plan.operation, current.block)
        if is_signed(plan)
        else deadline_reached(plan.operation, current.block)
    ):
        raise DataError("stale_plan", "prepared protocol deadline expired")
    if isinstance(plan.operation, (Mint, Increase)):
        price_movement(plan.before, current, allocation.slippage_bps)
        if allocation.adopt is not None:
            # A direct addition reverts beyond the tolerance of its decided price, so it
            # is neither signed nor sent while the pool is beyond it.
            decided = decision_observation(plan, plan.operation.target)
            price_movement(decided, current, allocation.slippage_bps)
