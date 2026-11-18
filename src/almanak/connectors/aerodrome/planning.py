"""Translate economic actions into bounded router calls and exact approvals."""

from almanak.connectors.aerodrome import math
from almanak.connectors.aerodrome import operations as ops
from almanak.connectors.aerodrome.authority import admit
from almanak.connectors.aerodrome.ownership import require_empty_pool_deposit
from almanak.connectors.dex_residual import counter_settled, residual_leg
from almanak.dex_intents import (
    CloseLiquidity,
    CollectLiquidityFees,
    DecreaseLiquidity,
    ExitExchange,
    IncreaseLiquidity,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.models import AssetAmount


def approval(before, asset, amount):
    current = next(a for a in before.allowances if a.asset == asset)
    if current.amount == amount or not amount:
        return None
    return ops.Approval(
        target=before.target,
        asset=asset,
        spender=before.target.contracts.router,
        amount=0 if current.amount else amount,
    )


def cleanup(before):
    active = next((a for a in before.allowances if a.amount), None)
    if active is None:
        raise ValueError("Aerodrome spending authority is already closed")
    return ops.Approval(target=before.target, asset=active.asset, spender=active.spender, amount=0)


def operation(config, state, intent, before):
    if intent.target != before.target or intent.target != config.target(intent.target.allocation):
        raise ValueError("Aerodrome decision must retain its configured observation target")
    admit(state.capabilities, intent)
    if isinstance(intent, ExitExchange):
        return exit_operation(config, before)
    if isinstance(intent, RevokeExchange):
        return cleanup(before)
    if isinstance(intent, SwapExactInput):
        return approval(before, intent.amount.asset, intent.amount.amount) or ops.Swap(
            target=intent.target,
            amount=intent.amount,
            minimum=intent.minimum_output,
            deadline=intent.deadline,
        )
    if isinstance(intent, (OpenLiquidity, IncreaseLiquidity)):
        return addition(config, intent, before, decision_basis(state.decision, intent.target))
    return removal(intent, before)


def decision_basis(decision, target):
    if decision is None:
        raise ValueError("execution requires its retained economic decision")
    values = [value for value in decision.observations if value.target == target]
    if len(values) != 1:
        raise ValueError("execution requires one retained decision observation for its target")
    return values[0]


def addition(config, intent, before, basis):
    position = before.owned.position
    if isinstance(intent, OpenLiquidity) == (position is not None):
        raise ValueError("liquidity addition must preserve existing shares and fee ownership")
    pool = before.pool
    if (pool.balance0, pool.balance1) != (pool.reserve0, pool.reserve1):
        raise ValueError("liquidity addition requires synchronized pool balances")
    require_empty_pool_deposit(pool)
    amount0, amount1, shares = math.addition(
        basis.pool.reserve0,
        basis.pool.reserve1,
        basis.pool.total_supply,
        intent.maximum0.amount,
        intent.maximum1.amount,
    )
    slippage = config.allocation(intent.target.allocation).slippage_bps
    values = dict(
        target=intent.target,
        maximum0=intent.maximum0,
        maximum1=intent.maximum1,
        minimum0=minimum(intent.target.assets[0], amount0, slippage),
        minimum1=minimum(intent.target.assets[1], amount1, slippage),
        minimum_shares=AssetAmount(
            asset=intent.target.pool.shares_asset,
            amount=max(1, shares * (10000 - slippage) // 10000),
        ),
        deadline=intent.deadline,
    )
    for maximum in (intent.maximum0, intent.maximum1):
        step = approval(before, maximum.asset, maximum.amount)
        if step is not None:
            return step
    return ops.Mint(**values) if position is None else ops.Increase(**values)


def minimum(asset, value, slippage):
    return AssetAmount(asset=asset, amount=value * (10000 - slippage) // 10000)


def removal(intent, before):
    position = before.owned.position
    if position is None:
        raise ValueError("liquidity exit requires retained fund-owned shares or fee claims")
    if isinstance(intent, CollectLiquidityFees):
        return ops.Collect(target=intent.target)
    if isinstance(intent, DecreaseLiquidity):
        if not 0 < intent.liquidity < position.shares.amount:
            raise ValueError("partial decrease must retain positive owned shares")
        shares = AssetAmount(asset=position.shares.asset, amount=intent.liquidity)
        kind: type[ops.Decrease] | type[ops.Close] = ops.Decrease
    elif isinstance(intent, CloseLiquidity):
        if not position.shares.amount:
            return ops.Collect(target=intent.target)
        shares, kind = position.shares, ops.Close
    else:
        raise ValueError("unsupported Aerodrome economic action")
    return remove_call(
        before,
        kind(
            target=intent.target,
            shares=shares,
            minimum0=intent.minimum0,
            minimum1=intent.minimum1,
            deadline=intent.deadline,
        ),
    )


def remove_call(before, operation):
    require_empty_pool_deposit(before.pool)
    return approval(before, operation.shares.asset, operation.shares.amount) or operation


def exit_operation(config, before):
    allocation = config.allocation(before.allocation)
    deadline = before.block.timestamp + allocation.deadline_seconds
    position = before.owned.position
    if position is not None:
        if not position.shares.amount:
            return ops.Collect(target=before.target)
        return close_position(allocation, before, deadline)
    if before.exit_quote is not None:
        return exit_swap(allocation, before, deadline)
    unsettled = not counter_settled(before.owned, before.exit_residual)
    return residual_leg(
        unsettled, before, cleanup, "remaining counter inventory requires an explicit exit quote"
    )


def close_position(allocation, before, deadline):
    position, pool = before.owned.position, before.pool
    principal = math.removal(
        pool.balance0, pool.balance1, pool.total_supply, position.shares.amount
    )
    minimums = tuple(
        minimum(asset, value, allocation.slippage_bps)
        for asset, value in zip(before.target.assets, principal, strict=True)
    )
    return remove_call(
        before,
        ops.Close(
            target=before.target,
            shares=position.shares,
            minimum0=minimums[0],
            minimum1=minimums[1],
            deadline=deadline,
        ),
    )


def exit_swap(allocation, before, deadline):
    quote = before.exit_quote
    floor = minimum(quote.amount_out.asset, quote.amount_out.amount, allocation.slippage_bps)
    floor = AssetAmount(asset=floor.asset, amount=max(1, floor.amount))
    return approval(before, quote.amount_in.asset, quote.amount_in.amount) or ops.Swap(
        target=before.target, amount=quote.amount_in, minimum=floor, deadline=deadline
    )
