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


def deadline(intent, before, allocation, limit=None):
    """Refuse a step observed at or after its deadline, or one whose deadline is too far.

    `limit` is the step's own deadline (`planning.step_deadline`) where it has one;
    otherwise the intent's applies. The refusal names the bound that fired.
    """
    if isinstance(intent, (RevokeExchange, ExitExchange)):
        return
    limit = intent.deadline if limit is None else limit
    block, window = before.block, allocation.deadline_seconds
    if not block.timestamp < limit <= block.timestamp + window:
        raise DataError(
            "stale_plan",
            "retained deadline expired or exceeds configured policy: block"
            f" {block.number} at time {block.timestamp} is outside the {window}s"
            f" `deadline_seconds` window that ends at {limit}" + standing(intent, before),
        )


def standing(intent, before) -> str:
    """What a refusal says of the allowances the request spends at the refused block.

    It names only an allowance equal to what the intent spends of that coin, which the
    same request stated again spends as it is. Any other allowance is reset and
    approved again (`planning.approval`), so it is not named. Only when every coin the
    intent spends is covered does it say that no other approval is needed.
    """
    spent = {value.asset: value.amount for value in spending(intent, before) if value.amount}
    granted = {value.asset: value.amount for value in before.allowances}
    held = [(asset, amount) for asset, amount in spent.items() if granted.get(asset) == amount]
    if not held:
        return ""
    listed = ", ".join(f"{amount} of {asset}" for asset, amount in held)
    if len(held) == len(spent):
        return (
            f"; the pool's allowance of {listed} stands, and the same request stated again"
            " spends it without another approval"
        )
    return f"; the pool's allowance of {listed} stands, which the same request stated again spends"


def fresh_quote(intent, seen, allocation):
    """Past the intent's deadline, a swap needs a fresh quote at or above its minimum.

    A swap planned in the window its mined approval restarted keeps the minimum output
    of the request's original quote. The pool enforces that minimum when the swap is
    mined; this refuses before a signature when the pool, at the block the step is
    planned or signed at, no longer quotes it for the stated amount.
    """
    if seen.block.timestamp < intent.deadline or not seen.quotes_included:
        return
    quotes = [
        q
        for q in (seen.entry_quote, *seen.exit_quotes)
        if q is not None
        and q.amount_in == intent.amount
        and q.amount_out.asset == intent.minimum_output.asset
    ]
    quoted = min((q.amount_out.amount for q in quotes), default=None)
    if quoted is not None and quoted >= intent.minimum_output.amount:
        return
    block, minimum = seen.block, intent.minimum_output
    found = "quotes nothing" if quoted is None else f"quotes {quoted} of {minimum.asset}"
    raise DataError(
        "stale_plan",
        f"the request's {allocation.deadline_seconds}s `deadline_seconds` window ended at"
        f" {intent.deadline}, and at block {block.number} (time {block.timestamp}) the pool"
        f" {found} for the stated amount, below the request's minimum output"
        f" {minimum.amount}" + standing(intent, seen),
    )


def step_limit(intent, operation):
    """The deadline a pool call carries, which may follow a mined approval; else None.

    `validate_operation` binds that deadline to the recorded approval by requiring the
    operation to equal the one planning derives from the retained outcomes.
    """
    if isinstance(intent, (RevokeExchange, ExitExchange)):
        return None
    return operation.deadline if isinstance(operation, (ops.Swap, ops.Addition)) else None


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
    deadline(intent, before, allocation, step_limit(intent, operation))
    validate_inventory(intent, before)
    intent_limits(intent, decision, before, allocation)
    if isinstance(intent, SwapExactInput) and isinstance(operation, ops.Swap):
        fresh_quote(intent, before, allocation)
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
    # A pool call planned after its request's approval was mined carries its own
    # deadline, which preparation bound to the recorded approval.
    if not is_signed(plan):
        intent, operation = plan.decision.intent, plan.operation
        deadline(intent, current, allocation, step_limit(intent, operation))
        if deadline_reached(operation, current.block):
            raise DataError("stale_plan", "prepared protocol deadline expired")
        if isinstance(intent, SwapExactInput) and isinstance(operation, ops.Swap):
            fresh_quote(intent, current, allocation)
    basis = planning.decision_basis(plan.decision, plan.operation.target)
    intent_limits(plan.decision.intent, basis, current, allocation)
