"""Hypothetical intent effects along the captured historical market path."""

from almanak.connectors.uniswap_v4.math import accrued, amounts
from almanak.connectors.uniswap_v4.planning import operation as plan_operation
from almanak.connectors.uniswap_v4.reader import quote, sqrt_bounds
from almanak.connectors.uniswap_v4.reconciliation import allowance_after
from almanak.connectors.uniswap_v4.state import Approval, Mint, SimulatedLiquidity, Swap
from almanak.dex_intents import CloseLiquidity, CollectLiquidityFees
from almanak.models import AssetAmount


def moved(owned, deltas):
    values = tuple(
        AssetAmount(asset=a.asset, amount=a.amount + delta)
        for a, delta in zip((owned.inventory0, owned.inventory1), deltas, strict=True)
    )
    return owned.model_copy(update={"inventory0": values[0], "inventory1": values[1]})


def fees_owed(position, growth):
    if not isinstance(position, SimulatedLiquidity) or growth is None:
        raise ValueError("historical fees require a durable simulated position and growth evidence")
    return tuple(
        accrued(position.liquidity, old, new)
        for old, new in zip(position.fee_growth, growth, strict=True)
    )


def liquidity_exit(intent, before, payload):
    owned = before.owned
    position = owned.position
    fees = fees_owed(position, before.fee_growth)
    principal = (0, 0)
    if isinstance(intent, CloseLiquidity):
        principal = amounts(
            position.liquidity,
            before.pool.sqrt_price_x96,
            *sqrt_bounds(intent.target, position=position),
        )
    deltas = tuple(p + f for p, f in zip(principal, fees, strict=True))
    if isinstance(intent, CloseLiquidity):
        # PositionManager BURN_LIQUIDITY subtracts fees before validateMinOut.
        if principal[0] < intent.minimum0.amount or principal[1] < intent.minimum1.amount:
            raise ValueError("historical close fails the retained principal minima")
        position = None
    else:
        position = position.model_copy(update={"fee_growth": before.fee_growth})
    after = moved(owned, deltas).model_copy(update={"position": position})
    return (
        payload.model_copy(
            update={
                "owned": after,
                "fees0": payload.fees0 + fees[0],
                "fees1": payload.fees1 + fees[1],
            }
        ),
        deltas,
        fees,
    )


def swapped(reader, operation, before, payload):
    filled = quote(reader, operation.target, operation.amount, before.block)
    return swap_fill(operation, before, payload, filled)


def swap_fill(operation, before, payload, filled):
    values = (before.owned.inventory0, before.owned.inventory1)
    index = operation.target.assets.index(operation.amount.asset)
    if operation.amount.amount > values[index].amount:
        raise ValueError("historical swap exceeds the attributed inventory")
    if filled.output.amount < operation.minimum.amount:
        raise ValueError("historical exact-size quote fails the retained swap minimum")
    deltas = [0, 0]
    deltas[index], deltas[1 - index] = -operation.amount.amount, filled.output.amount
    owned = moved(before.owned, deltas)
    if operation.amount.asset == operation.target.funding_currency:
        owned = owned.model_copy(update={"entry_complete": True})
    spent = f"swap{index}"
    return (
        payload.model_copy(
            update={"owned": owned, spent: getattr(payload, spent) + operation.amount.amount}
        ),
        tuple(deltas),
        filled,
    )


def minted(operation, before, payload, identity):
    if before.owned.position is not None or before.fee_growth is None:
        raise ValueError("historical mint requires a fresh range and fee-growth evidence")
    if (
        operation.maximum0.amount > before.owned.inventory0.amount
        or operation.maximum1.amount > before.owned.inventory1.amount
    ):
        raise ValueError("historical mint exceeds attributed inventory")
    deposits = amounts(
        operation.liquidity,
        before.pool.sqrt_price_x96,
        *sqrt_bounds(operation.target, requested=operation.range),
        up=True,
    )
    if deposits[0] > operation.maximum0.amount or deposits[1] > operation.maximum1.amount:
        raise ValueError("integer mint deposits exceed the retained maxima")
    deltas = tuple(-v for v in deposits)
    position = SimulatedLiquidity(
        simulation_id=identity,
        liquidity=operation.liquidity,
        fee_growth=before.fee_growth,
        range=operation.range,
    )
    owned = moved(before.owned, deltas).model_copy(
        update={"position": position, "entry_complete": True}
    )
    return payload.model_copy(
        update={
            "owned": owned,
            "mint0": payload.mint0 + operation.maximum0.amount,
            "mint1": payload.mint1 + operation.maximum1.amount,
            "benchmark": (before.owned.inventory0, before.owned.inventory1),
        }
    ), deltas


def apply_step(reader, operation, before, payload, identity):
    filled = None
    deltas = (0, 0)
    if isinstance(operation, Swap):
        payload, deltas, filled = swapped(reader, operation, before, payload)
    if isinstance(operation, Mint):
        payload, deltas = minted(operation, before, payload, identity)
    allowances = allowance_after(before, operation, deltas, before.block.timestamp)
    return payload.model_copy(update={"allowances": allowances}), deltas, filled


def simulate_intent(reader, config, intent, observed, payload, identity):
    if isinstance(intent, (CollectLiquidityFees, CloseLiquidity)):
        updated, deltas, fees = liquidity_exit(intent, observed, payload)
        kind = "collect" if isinstance(intent, CollectLiquidityFees) else "burn"
        return updated, deltas, fees, (kind,), None
    steps = []
    # Each required approval is an explicit costed step at this intent's eligible
    # block. These hypothetical steps never alter the archival provider's state.
    for _ in range(8):
        operation = plan_operation(config, None, intent, observed)
        payload, deltas, filled = apply_step(reader, operation, observed, payload, identity)
        steps.append(operation.kind)
        if not isinstance(operation, Approval):
            return payload, deltas, (0, 0), tuple(steps), filled
        observed = observed.model_copy(update={"allowances": payload.allowances})
        if intent.kind == "revoke_exchange" and not observed.needs_cleanup:
            return payload, deltas, (0, 0), tuple(steps), filled
    raise ValueError("historical intent did not resolve its bounded approval sequence")
