"""Check economic limits before granting authority and again before submission."""

from almanak.connectors.aerodrome import math, planning
from almanak.connectors.aerodrome import operations as ops
from almanak.connectors.aerodrome.authority import admit
from almanak.connectors.aerodrome.ownership import (
    require_empty_pool_deposit,
    validate_owned,
    validate_retained,
)
from almanak.connectors.refusals import below_entry_minimum
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


def deadline(intent, before, allocation):
    if isinstance(intent, (RevokeExchange, ExitExchange)):
        return
    if (
        not before.block.timestamp
        < intent.deadline
        <= before.block.timestamp + allocation.deadline_seconds
    ):
        raise DataError("stale_plan", "retained deadline expired or exceeds configured policy")


def swap_limits(intent, decision, allocation):
    quotes = [
        q
        for q in (decision.entry_quote, decision.exit_quote)
        if q is not None and q.amount_in == intent.amount
    ]
    if not quotes or any(q.calculation != "contract_router" for q in quotes):
        raise ValueError("swap requires its exact-size retained router quote")
    floor = max(
        max(1, q.amount_out.amount * (10000 - allocation.slippage_bps) // 10000) for q in quotes
    )
    if intent.minimum_output.amount < floor:
        raise ValueError("swap minimum widens the configured slippage limit")


def addition_limits(intent, decision, before, allocation):
    pool = before.pool
    if (pool.balance0, pool.balance1) != (pool.reserve0, pool.reserve1):
        raise DataError(
            "precondition_changed", "liquidity addition requires synchronized pool balances"
        )
    require_empty_pool_deposit(pool)
    expected = math.addition(
        decision.pool.reserve0,
        decision.pool.reserve1,
        decision.pool.total_supply,
        intent.maximum0.amount,
        intent.maximum1.amount,
    )
    if allocation.adopt is not None:
        require_proportional_offer(intent, expected)
    current = math.addition(
        pool.reserve0,
        pool.reserve1,
        pool.total_supply,
        intent.maximum0.amount,
        intent.maximum1.amount,
    )
    # These are proportional deposit and share bounds, not a reserve-ratio claim
    # about the stable invariant's marginal exchange price.
    floors = tuple(v * (10000 - allocation.slippage_bps) // 10000 for v in expected)
    floors = floors[:2] + (max(1, floors[2]),)
    if any(new < floor for new, floor in zip(current, floors, strict=True)):
        raise DataError(
            "liquidity_movement", "pool movement violates retained deposit or share minima"
        )


def require_proportional_offer(intent, expected):
    """A direct deposit offers exactly what it takes at the decision's proportion.

    The minimums are the offer less the slippage, and the router takes at most the
    offer. Only an offer in the proportion bounds the deposit on both sides: an amount
    offered beyond it could be taken whole at a proportion pushed after signing. A
    strategy's deposit offers its inventory as it always did and is not held to this.
    """
    if (intent.maximum0.amount, intent.maximum1.amount) != expected[:2]:
        raise ValueError("a direct deposit offers exactly what it takes at the observed proportion")


def removal_limits(intent, decision, allocation):
    position = decision.owned.position
    if position is None:
        raise ValueError("liquidity removal requires retained decision shares")
    shares = intent.liquidity if isinstance(intent, DecreaseLiquidity) else position.shares.amount
    pool = decision.pool
    expected = math.removal(pool.balance0, pool.balance1, pool.total_supply, shares)
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
    if isinstance(operation, ops.Approval):
        return operation.amount > 0 and operation.asset == operation.target.funding_currency
    if isinstance(operation, ops.Swap):
        return operation.amount.asset == operation.target.funding_currency
    return isinstance(operation, ops.Addition)


def validate_entry(state, before, allocation):
    if state.mode != "running" or any(o.accounting_issues for o in state.outcomes):
        raise DataError("accounting", "new exposure requires a running reconciled deployment")
    if (
        before.native_balance is None
        or before.native_balance.amount < allocation.minimum_native_balance
    ):
        raise below_entry_minimum(before.native_balance, allocation.minimum_native_balance)


def intent_limits(intent, decision, before, allocation):
    if isinstance(intent, SwapExactInput):
        swap_limits(intent, decision, allocation)
        if before.pool.paused:
            raise DataError("market_paused", "configured pool swaps are paused")
    if isinstance(intent, (OpenLiquidity, IncreaseLiquidity)):
        addition_limits(intent, decision, before, allocation)
    if isinstance(intent, (CloseLiquidity, DecreaseLiquidity)):
        removal_limits(intent, decision, allocation)


def validate_operation(backend, state, operation, before):
    if before.simulated or state.decision is None or state.capabilities is None:
        raise ValueError("execution requires on-chain inputs and retained decision authority")
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
    decision = planning.decision_basis(state.decision, operation.target)
    if decision.simulated:
        raise ValueError("simulated decisions cannot authorize execution")
    deadline(intent, before, allocation)
    validate_inventory(intent, before)
    intent_limits(intent, decision, before, allocation)
    if operation != planning.operation(state.config, state, intent, before):
        raise ValueError("operation differs from its retained economic intent or owned shares")
    if isinstance(intent, (OpenLiquidity, IncreaseLiquidity)) or increases_exposure(
        state.config, operation
    ):
        validate_entry(state, before, allocation)


def validate_current(backend, plan, current):
    if current.simulated:
        raise ValueError("simulated observations cannot authorize submission")
    retained(backend.config, plan)(plan.before.owned, current)
    if plan.before.allowances != current.allowances:
        raise DataError("precondition_changed", "spending authority changed after preparation")
    validate_current_intent(backend.config, plan, current)


def retained(config, plan):
    """The ownership check a plan is held to at the current head.

    Unsigned work, and every plan of a strategy's allocation, is held to the fund's
    stored position. Signed bytes of an adopting allocation are not: they cannot be
    recalled, a share anyone sends the fund would hold them back for good, and their
    outcome is reconciled with the difference listed.
    """
    adopting = config.allocation(plan.operation.allocation).adopt is not None
    return validate_retained if adopting and is_signed(plan) else validate_owned


def expired(operation, block) -> bool:
    """True once the signed operation's protocol deadline has passed; it can then only revert."""
    return (
        isinstance(operation, (ops.Swap, ops.Addition, ops.Removal))
        and operation.deadline < block.timestamp
    )


def deadline_reached(operation, block) -> bool:
    """Whether preparation or signing at this head is too late for the encoded deadline."""
    return (
        isinstance(operation, (ops.Swap, ops.Addition, ops.Removal))
        and operation.deadline <= block.timestamp
    )


def validate_current_intent(config, plan, current):
    allocation = config.allocation(plan.operation.allocation)
    # The intent's admission window bounds planning and signing. Recorded signed bytes
    # are bounded only by their own protocol deadline (`expired`): an approval has none,
    # so it stays rebroadcastable after its intent expires instead of being refused forever.
    if not is_signed(plan):
        deadline(plan.decision.intent, current, allocation)
    if (
        expired(plan.operation, current.block)
        if is_signed(plan)
        else deadline_reached(plan.operation, current.block)
    ):
        raise DataError("stale_plan", "prepared protocol deadline expired")
    basis = planning.decision_basis(plan.decision, plan.operation.target)
    intent_limits(plan.decision.intent, basis, current, allocation)
    if isinstance(plan.operation, ops.Removal):
        require_empty_pool_deposit(current.pool)
