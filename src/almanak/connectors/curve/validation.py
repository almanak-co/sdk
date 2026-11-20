"""Check quotes, slippage, deadlines and inventory before submission and again before sending."""

from almanak.connectors.curve import liquidity_quotes, planning
from almanak.connectors.curve import operations as ops
from almanak.connectors.curve.authority import admit
from almanak.connectors.curve.ownership import validate_owned, validate_retained
from almanak.connectors.refusals import below_entry_minimum
from almanak.dex_intents import (
    ClosePoolLiquidity,
    DecreasePoolLiquidity,
    ExitExchange,
    IncreasePoolLiquidity,
    OpenPoolLiquidity,
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
        for q in (decision.entry_quote, *decision.exit_quotes)
        if q is not None and q.amount_in == intent.amount
    ]
    if not quotes or any(q.calculation != "contract_pool" for q in quotes):
        raise ValueError("swap requires its exact-size retained pool quote")
    floor = max(
        max(1, q.amount_out.amount * (10000 - allocation.slippage_bps) // 10000) for q in quotes
    )
    if intent.minimum_output.amount < floor:
        raise ValueError("swap minimum widens the configured slippage limit")


def addition_limits(intent, decision, before, allocation):
    amounts = tuple(value.amount for value in intent.maximums.root)
    expected = liquidity_quotes.minted(decision.pool, amounts)
    current = liquidity_quotes.minted(before.pool, amounts)
    floor = max(1, expected * (10000 - allocation.slippage_bps) // 10000)
    if current < floor:
        raise DataError("liquidity_movement", "pool movement violates retained LP share minimum")


def removal_limits(intent, decision, allocation):
    position = decision.owned.position
    if position is None:
        raise ValueError("liquidity removal requires retained decision shares")
    shares = (
        intent.liquidity if isinstance(intent, DecreasePoolLiquidity) else position.shares.amount
    )
    floors = planning.minimums(
        intent.target, decision.pool, shares, allocation.slippage_bps, intent.output_asset
    )
    if any(
        minimum.amount < floor.amount
        for minimum, floor in zip(intent.minimums.root, floors.root, strict=True)
    ):
        raise ValueError("liquidity removal minima widen the configured slippage limit")


def spending(intent, before):
    if isinstance(intent, SwapExactInput):
        return (intent.amount,)
    if isinstance(intent, (OpenPoolLiquidity, IncreasePoolLiquidity)):
        return intent.maximums.root
    if isinstance(intent, ExitExchange) and before.owned.position is None and before.exit_quotes:
        return (before.exit_quotes[0].amount_in,)
    return ()


def validate_inventory(intent, before):
    """Each input is covered by the attributed inventory; no cumulative budget applies."""
    inventory = {a.asset: a.amount for a in before.owned.inventory.root}
    for value in spending(intent, before):
        if value.amount > inventory[value.asset]:
            raise DataError("inventory", "economic action exceeds attributed input inventory")


def increases_exposure(config, operation):
    if isinstance(operation, ops.Approval):
        return operation.amount > 0 and operation.asset == operation.target.funding_asset
    if isinstance(operation, ops.Swap):
        return operation.amount.asset == operation.target.funding_asset
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
    if isinstance(intent, (OpenPoolLiquidity, IncreasePoolLiquidity)):
        addition_limits(intent, decision, before, allocation)
    if isinstance(intent, (ClosePoolLiquidity, DecreasePoolLiquidity)):
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
    if isinstance(intent, (OpenPoolLiquidity, IncreasePoolLiquidity)) or increases_exposure(
        state.config, operation
    ):
        validate_entry(state, before, allocation)


def validate_current(backend, plan, current):
    if current.simulated:
        raise ValueError("simulated observations cannot authorize submission")
    # LP shares that reach the fund after a signature do not hold the signed bytes back:
    # they could not be recalled, and their outcome is reconciled with the shares listed.
    (validate_retained if is_signed(plan) else validate_owned)(plan.before.owned, current)
    if plan.before.allowances != current.allowances:
        raise DataError("precondition_changed", "spending authority changed after preparation")
    validate_current_intent(backend.config, plan, current)


def deadline_reached(operation, block) -> bool:
    """Whether preparation or signing at this head is too late for the prepared deadline.

    Curve pool calls encode no deadline, so this bounds preparation and signing only:
    signed bytes stay executable after it, are rebroadcast and are never released.
    """
    return (
        isinstance(operation, (ops.Swap, ops.Addition, ops.Removal))
        and operation.deadline <= block.timestamp
    )


def validate_current_intent(config, plan, current):
    allocation = config.allocation(plan.operation.allocation)
    # The intent's admission window and the prepared deadline bound planning and signing.
    # No Curve call enforces a deadline on chain, so recorded signed bytes stay
    # rebroadcastable after both instead of being refused forever.
    if not is_signed(plan):
        deadline(plan.decision.intent, current, allocation)
        if deadline_reached(plan.operation, current.block):
            raise DataError("stale_plan", "prepared protocol deadline expired")
    basis = planning.decision_basis(plan.decision, plan.operation.target)
    intent_limits(plan.decision.intent, basis, current, allocation)
