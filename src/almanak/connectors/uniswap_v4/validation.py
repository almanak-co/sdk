"""Independent gateway admission of retained inventory, quotes, ranges and NFT identity."""

from almanak.connectors.refusals import below_entry_minimum
from almanak.connectors.uniswap_v4.authoring import close_liquidity
from almanak.connectors.uniswap_v4.authority import admit_intent
from almanak.connectors.uniswap_v4.hooks import pool_hook
from almanak.connectors.uniswap_v4.planning import operation as planned_operation
from almanak.connectors.uniswap_v4.state import Approval, Burn, Collect, Mint, Swap
from almanak.connectors.v3_pool_authority import admit_centered
from almanak.dex_intents import (
    CloseLiquidity,
    ExitExchange,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.gateway.ethereum import DataError
from almanak.transaction_family import is_signed


def validate_owned(owned, before):
    if owned != before.owned:
        raise DataError("ownership", "observation has another retained inventory or NFT")
    expected = owned.position.liquidity if owned.position else 0
    if before.position_liquidity != expected:
        raise DataError("ownership", "observed NFT liquidity differs from retained ownership")
    if (
        owned.position
        and before.position_growth is not None
        and owned.position.fee_growth is not None
        and before.position_growth != owned.position.fee_growth
    ):
        raise DataError("ownership", "position fees changed outside the retained execution")
    if (
        before.wallet0.amount < owned.inventory0.amount
        or before.wallet1.amount < owned.inventory1.amount
    ):
        raise DataError("inventory", "fund wallet does not cover the attributed inventory")


def deadline(intent, before, allocation):
    if isinstance(intent, (RevokeExchange, ExitExchange)):
        return
    if (
        not int(before.effective_at.timestamp())
        < intent.deadline
        <= int(before.effective_at.timestamp()) + allocation.deadline_seconds
    ):
        raise DataError("stale_plan", "retained transaction deadline expired or exceeds policy")


def swap_limits(state, intent, before, allocation):
    owned = {v.asset: v.amount for v in (before.owned.inventory0, before.owned.inventory1)}
    if intent.amount.amount > owned[intent.amount.asset]:
        raise DataError("inventory", "swap exceeds the attributed input inventory")
    decision = next(o for o in state.decision.observations if o.target == intent.target)
    quotes = (decision.entry_quote, decision.exit_quote, decision.rebalance_quote)
    match = [q for q in quotes if q is not None and q.amount == intent.amount]
    if not match:
        raise ValueError("swap requires its exact-size decision quote")
    floor = max(max(1, q.output.amount * (10000 - allocation.slippage_bps) // 10000) for q in match)
    if intent.minimum_output.amount < floor:
        raise ValueError("swap minimum widens the configured slippage limit")


def mint_limits(state, intent, before, allocation):
    if (
        intent.maximum0.amount > before.owned.inventory0.amount
        or intent.maximum1.amount > before.owned.inventory1.amount
    ):
        raise DataError("inventory", "mint exceeds the attributed inventory")
    previous = next(o for o in state.decision.observations if o.target == intent.target).pool
    old, new = previous.sqrt_price_x96**2, before.pool.sqrt_price_x96**2
    if abs(new - old) * 10000 > old * allocation.slippage_bps:
        raise DataError("price_movement", "pool price moved beyond the retained mint tolerance")
    admit_centered(intent, previous.tick)


def close_limits(state, intent, allocation):
    decision = next(o for o in state.decision.observations if o.target == intent.target)
    expected = close_liquidity(allocation, decision)
    if (
        intent.minimum0.amount < expected.minimum0.amount
        or intent.minimum1.amount < expected.minimum1.amount
    ):
        raise ValueError("close minima widen the configured slippage limit")


def validate_operation(backend, state, operation, before):
    if before.simulated:
        raise ValueError("simulated observations cannot authorize live execution")
    allocation = state.config.allocation(operation.allocation)
    if state.decision is None or state.capabilities is None:
        raise ValueError("execution requires a retained decision and capabilities")
    intent = state.decision.intent
    admit_intent(state.capabilities, intent)
    if (
        operation.target != state.config.target(allocation.name)
        or before.target != operation.target
    ):
        raise ValueError("operation and observation require the bound target")
    owned = next(o for o in state.owned if o.target == operation.target)
    validate_owned(owned, before)
    if operation != planned_operation(state.config, state, intent, before):
        raise ValueError("operation differs from its retained economic intent or NFT")
    deadline(intent, before, allocation)
    if isinstance(intent, SwapExactInput):
        swap_limits(state, intent, before, allocation)
    if isinstance(intent, OpenLiquidity):
        mint_limits(state, intent, before, allocation)
    if isinstance(intent, CloseLiquidity):
        close_limits(state, intent, allocation)
    if increases_exposure(state.config, operation):
        validate_entry(state, before, allocation)
    if isinstance(intent, RevokeExchange) and owned.position is not None:
        raise ValueError("spending cleanup requires verified position closure")


def validate_entry(state, before, allocation):
    if state.mode != "running" or any(o.accounting_issues for o in state.outcomes):
        raise DataError("accounting", "new exposure requires a running reconciled deployment")
    if before.native_balance.amount < allocation.minimum_native_balance:
        raise below_entry_minimum(before.native_balance, allocation.minimum_native_balance)
    require_hook_terms(before)


def require_hook_terms(before):
    """New exposure through an output-haircut hook requires its measured take terms.

    Receipts reconcile the take against them; exits proceed without them.
    """
    entry = pool_hook(before.target.pool)
    if entry is None or entry.reviewed.model != "output_haircut" or before.hook_terms is not None:
        return
    raise DataError(
        "hook_terms",
        f"new exposure through reviewed hook {entry.profile} requires measured hook terms"
        f" ({before.hook_error or 'not observed'}); exits remain available",
    )


def increases_exposure(config, operation):
    if isinstance(operation, (Burn, Collect)):
        return False
    if isinstance(operation, Approval):
        return operation.amount > 0 and operation.asset == operation.target.funding_currency
    if isinstance(operation, Swap):
        return operation.amount.asset == operation.target.funding_currency
    return isinstance(operation, Mint)


def expired(operation, block) -> bool:
    """True once the signed operation's protocol deadline has passed; it can then only revert.

    The router and the PositionManager both refuse a block after the encoded deadline,
    not one at it; consecutive blocks can share a timestamp (Arbitrum, BNB), so only a
    head strictly past the deadline proves no later block can include the call. An
    approval has none: a Permit2 expiration bounds the allowance, not the call.
    """
    return (
        isinstance(operation, (Swap, Mint, Collect, Burn)) and operation.deadline < block.timestamp
    )


def deadline_reached(operation, block) -> bool:
    """Whether preparation or signing at this head is too late for the encoded deadline."""
    return (
        isinstance(operation, (Swap, Mint, Collect, Burn)) and operation.deadline <= block.timestamp
    )


def validate_current(backend, plan, current):
    validate_owned(plan.before.owned, current)
    if plan.before.allowances != current.allowances:
        raise DataError("precondition_changed", "spending authority changed after preparation")
    # The intent's admission window bounds planning and signing. Recorded signed bytes
    # are bounded only by their own protocol deadline (`expired`): an approval has none,
    # so it stays rebroadcastable after its intent expires instead of being refused forever.
    if not is_signed(plan):
        deadline(
            plan.decision.intent, current, backend.config.allocation(plan.operation.allocation)
        )
    if (
        expired(plan.operation, current.block)
        if is_signed(plan)
        else deadline_reached(plan.operation, current.block)
    ):
        raise DataError("stale_plan", "prepared protocol deadline expired")
    if isinstance(plan.operation, Mint):
        old, new = plan.before.pool.sqrt_price_x96**2, current.pool.sqrt_price_x96**2
        tolerance = backend.config.allocation(plan.operation.allocation).slippage_bps
        if abs(new - old) * 10000 > old * tolerance:
            raise DataError("price_movement", "pool moved beyond the prepared mint tolerance")
