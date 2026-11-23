"""Admitted economic requests expand to exact approvals and bounded pool calls."""

from almanak.asset_amounts import AssetAmounts
from almanak.connectors.curve import liquidity_quotes
from almanak.connectors.curve import operations as ops
from almanak.connectors.curve.authority import admit
from almanak.connectors.curve.observations import unsettled
from almanak.connectors.dex_residual import residual_leg
from almanak.dex_intents import (
    ClosePoolLiquidity,
    DecreasePoolLiquidity,
    ExitExchange,
    IncreasePoolLiquidity,
    OpenPoolLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.models import AssetAmount


def approval(before, asset, amount):
    current = next(value for value in before.allowances if value.asset == asset)
    if current.amount == amount or amount == 0:
        return None
    return ops.Approval(
        target=before.target,
        asset=asset,
        spender=before.target.pool.pool,
        amount=0 if current.amount else amount,
    )


def cleanup(before):
    active = next((value for value in before.allowances if value.amount), None)
    if active is None:
        raise ValueError("Curve spending authority is already closed")
    return ops.Approval(target=before.target, asset=active.asset, spender=active.spender, amount=0)


def operation(config, state, intent, before):
    if intent.target != before.target or intent.target != config.target(intent.target.allocation):
        raise ValueError("Curve decision must retain its configured observation target")
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
            deadline=step_deadline(config, state, intent),
        )
    if isinstance(intent, (OpenPoolLiquidity, IncreasePoolLiquidity)):
        return addition(
            config,
            intent,
            before,
            decision_basis(state.decision, intent.target),
            step_deadline(config, state, intent),
        )
    return removal(intent, before)


def preceding_approval(state):
    """The mined approval of the retained request that its next step follows, or None.

    Only the latest outcome counts, and only when it is a successful approval recorded
    under the decision still retained: a step of another request, a reverted approval
    or any other operation gives the next step nothing.
    """
    outcomes = getattr(state, "outcomes", ())
    last = outcomes[-1] if outcomes else None
    if (
        last is None
        or not isinstance(getattr(last, "operation", None), ops.Approval)
        or not last.success
        or last.decision != state.decision
    ):
        return None
    return last


def step_deadline(config, state, intent):
    """When the pool call of a swap or an addition may last be planned and signed.

    It is the intent's own deadline until an approval of the same operator request is
    mined. The pool call that follows one is given `deadline_seconds` from the block
    that mined it, so the time the approval took to be mined is not charged to the pool
    call, and never more than `deadline_seconds` past the intent's deadline, so a
    request is never signed later than two windows after it was observed. An approval
    is always held to the intent's deadline. The minimum output or minimum shares the
    pool call carries are unchanged, and no Curve call carries the deadline itself.

    A strategy deployment always keeps the intent's deadline: a refused step there is
    followed by a new decision on fresh observations, where an operator's stated
    request would end.
    """
    from almanak.operator_deployment import is_operator

    mined = preceding_approval(state) if is_operator(state) else None
    if mined is None:
        return intent.deadline
    window = config.allocation(intent.target.allocation).deadline_seconds
    return max(intent.deadline, min(mined.after.block.timestamp, intent.deadline) + window)


def decision_basis(decision, target):
    if decision is None:
        raise ValueError("Curve execution requires its retained economic decision")
    observations = [value for value in decision.observations if value.target == target]
    if len(observations) != 1:
        raise ValueError(
            "Curve execution requires one retained decision observation for its target"
        )
    return observations[0]


def addition(config, intent, before, basis, deadline):
    position = before.owned.position
    if isinstance(intent, OpenPoolLiquidity) == (position is not None):
        raise ValueError("Curve liquidity addition must preserve retained share ownership")
    shares = liquidity_quotes.minted(
        basis.pool, tuple(value.amount for value in intent.maximums.root)
    )
    slippage = config.allocation(intent.target.allocation).slippage_bps
    for maximum in intent.maximums.root:
        step = approval(before, maximum.asset, maximum.amount)
        if step is not None:
            return step
    kind = ops.Mint if position is None else ops.Increase
    return kind(
        target=intent.target,
        amounts=intent.maximums,
        minimum_shares=AssetAmount(
            asset=intent.target.pool.shares_asset,
            amount=max(1, shares * (10000 - slippage) // 10000),
        ),
        deadline=deadline,
    )


def removal(intent, before):
    position = before.owned.position
    if position is None:
        raise ValueError("Curve liquidity exit requires retained fund-owned shares")
    if isinstance(intent, DecreasePoolLiquidity):
        if not 0 < intent.liquidity < position.shares.amount:
            raise ValueError("Curve partial decrease must retain positive owned shares")
        shares = AssetAmount(asset=position.shares.asset, amount=intent.liquidity)
        kind: type[ops.Decrease] | type[ops.Close] = ops.Decrease
    elif isinstance(intent, ClosePoolLiquidity):
        shares, kind = position.shares, ops.Close
    else:
        raise ValueError("unsupported Curve economic action")
    return kind(
        target=intent.target,
        shares=shares,
        minimums=intent.minimums,
        output_asset=intent.output_asset,
        deadline=intent.deadline,
    )


def minimums(target, pool, shares, slippage, output_asset=None):
    index = None if output_asset is None else target.assets.index(output_asset)
    outputs = liquidity_quotes.removed(pool, shares, index)
    return AssetAmounts(
        tuple(
            AssetAmount(asset=asset, amount=value * (10000 - slippage) // 10000)
            for asset, value in zip(target.assets, outputs, strict=True)
        )
    )


def close_position(allocation, before, deadline):
    position = before.owned.position
    output = before.target.single_coin_exit
    return ops.Close(
        target=before.target,
        shares=position.shares,
        minimums=minimums(
            before.target, before.pool, position.shares.amount, allocation.slippage_bps, output
        ),
        output_asset=output,
        deadline=deadline,
    )


def exit_operation(config, before):
    allocation = config.allocation(before.allocation)
    deadline = before.block.timestamp + allocation.deadline_seconds
    if before.owned.position is not None:
        return close_position(allocation, before, deadline)
    if before.exit_quotes:
        return exit_swap(allocation, before, deadline)
    return residual_leg(
        bool(unsettled(before.owned, before.exit_residuals)),
        before,
        cleanup,
        "remaining Curve inventory requires explicit exit quotes",
    )


def exit_swap(allocation, before, deadline):
    quote = before.exit_quotes[0]
    floor = AssetAmount(
        asset=quote.amount_out.asset,
        amount=max(1, quote.amount_out.amount * (10000 - allocation.slippage_bps) // 10000),
    )
    return approval(before, quote.amount_in.asset, quote.amount_in.amount) or ops.Swap(
        target=before.target, amount=quote.amount_in, minimum=floor, deadline=deadline
    )
