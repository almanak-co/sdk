"""Economic intents become bounded periphery steps without author-managed approvals."""

from almanak.connectors.erc20_approval import requires_zero_first
from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4.math import amounts, liquidity_for, sqrt_at_tick
from almanak.connectors.uniswap_v4.reader import concrete_range
from almanak.connectors.uniswap_v4.state import Approval, Burn, Collect, Mint, Swap
from almanak.dex_intents import (
    CloseLiquidity,
    CollectLiquidityFees,
    ExitExchange,
    OpenLiquidity,
    RevokeExchange,
    SwapExactInput,
)
from almanak.gateway.ethereum import DataError


def approval(before, asset, amount, spender, deadline, *, through=None):
    """The next approval a spend of `amount` by `spender` still needs, or None.

    A Permit2 allowance is relied on while it has not lapsed. Given `through`, the
    latest time the spend may be mined, it is relied on only while it outlasts that,
    so one that would lapse before the spend's own deadline is granted again. Only an
    operator request passes it: its allowances may be standing ones from before the
    command. A strategy's are its own, granted for the decision that spends them, and
    its retained steps replay under the rule they were planned with.
    """
    if not amount:
        return None
    token = next(a for a in before.allowances if a.asset == asset and a.expiration is None)
    if token.amount < amount:
        # A mint can leave a residual Permit2 allowance; a token that reverts a
        # nonzero-to-nonzero approve (Ethereum USDT) has it cleared first.
        clear = bool(token.amount) and requires_zero_first(asset)
        return Approval(
            target=before.target,
            kind="revoke_erc20" if clear else "approve_erc20",
            asset=asset,
            spender=token.spender,
            amount=0 if clear else amount,
        )
    permit = next(a for a in before.allowances if a.asset == asset and a.spender == spender)
    lapses = permit.expiration <= before.block.timestamp
    if through is not None:
        lapses = permit.expiration < through
    if permit.amount < amount or lapses:
        return Approval(
            target=before.target,
            kind="approve_permit2",
            asset=asset,
            spender=spender,
            amount=amount,
            expiration=deadline,
        )
    return None


def cleanup(before):
    active = [a for a in before.allowances if a.needs_cleanup(before.block.timestamp)]
    if not active:
        raise ValueError("spending authority is already closed")
    current = active[0]
    permit = current.expiration is not None
    return Approval(
        target=before.target,
        kind="revoke_permit2" if permit else "revoke_erc20",
        asset=current.asset,
        spender=current.spender,
        amount=0,
        expiration=0 if permit else None,
    )


def bounded_maximums(intent, before, tolerance_bps):
    """The most of each token a mint planned at this observation may take.

    A mint is for a fixed liquidity, the most the intent's amounts cover at this
    observation's price, and the PositionManager reverts one that needs more than a
    maximum. Each maximum is the intent's amount, trimmed to what that liquidity takes
    at the edge of the tolerance around this observation's price where it takes the
    most of that token: the price less the tolerance for the first token, the price
    plus the tolerance for the second, rounded up as the PoolManager charges it.

    So a mint mined at a price further than the tolerance from the price it was planned
    at reverts. In the direction that needs more of the token limiting the liquidity
    it reverts on any move: that token's maximum is the intent's amount, which the
    liquidity takes whole at the planned price.

    The range bounds what the tolerance cannot. Where an edge of the tolerance lies at
    or beyond a bound of the range, the maximum is all the range holds of that token at
    this liquidity, and no price in that direction reverts the mint. A range wholly
    beside the price takes one token only, the same amount at every price on that side,
    so no price further from the range reverts it either; a price that enters the range
    past the tolerance does.
    """
    from math import isqrt

    selected = concrete_range(intent.target, requested=intent.range)
    lower, upper = sqrt_at_tick(selected.lower), sqrt_at_tick(selected.upper)
    price = before.pool.sqrt_price_x96
    stated = intent.maximum0, intent.maximum1
    liquidity = liquidity_for(stated[0].amount, stated[1].amount, price, lower, upper)
    # Square-root prices at this observation's price less and plus the tolerance.
    low, high = (isqrt(price * price * (10000 + sign * tolerance_bps) // 10000) for sign in (-1, 1))
    most = (
        amounts(liquidity, low, lower, upper, up=True)[0],
        amounts(liquidity, high, lower, upper, up=True)[1],
    )
    return tuple(
        value.model_copy(update={"amount": min(value.amount, limit)})
        for value, limit in zip(stated, most, strict=True)
    )


def open_operation(intent, before, *, through=None, tolerance_bps=None):
    """The next step of a mint: an approval it still needs, or the mint itself.

    Given `tolerance_bps`, as only an operator request gives it, the mint and its
    approvals take `bounded_maximums` of this observation instead of the intent's whole
    amounts. An approval mined for an earlier observation's smaller maximum is then
    below what this one needs, and is planned again before the mint: a mint never
    relies on more than is approved.
    """
    if before.owned.position is not None:
        raise ValueError("mint cannot replace an owned NFT")
    target = intent.target
    maximums = (intent.maximum0, intent.maximum1)
    if tolerance_bps is not None:
        maximums = bounded_maximums(intent, before, tolerance_bps)
    for maximum in maximums:
        step = approval(
            before,
            maximum.asset,
            maximum.amount,
            target.contracts.position_manager,
            intent.deadline,
            through=through,
        )
        if step:
            return step
    return liquidity_operation(intent, before, maximums)


def liquidity_operation(intent, before, maximums=None):
    """The mint of the most liquidity the intent's amounts cover at this observation's
    price, allowed to take at most `maximums`: the intent's whole amounts unless given."""
    if maximums is None:
        maximums = (intent.maximum0, intent.maximum1)
    target = intent.target
    selected = concrete_range(target, requested=intent.range)
    lower, upper = sqrt_at_tick(selected.lower), sqrt_at_tick(selected.upper)
    liquidity = liquidity_for(
        intent.maximum0.amount, intent.maximum1.amount, before.pool.sqrt_price_x96, lower, upper
    )
    if not 0 < liquidity < 2**128:
        raise DataError("inventory", "available range inventory cannot mint positive liquidity")
    return Mint(
        target=target,
        liquidity=liquidity,
        maximum0=maximums[0],
        maximum1=maximums[1],
        deadline=intent.deadline,
        range=selected if target.band is not None else None,
    )


def exit_operation(intent, before):
    position = before.owned.position
    if position is None:
        raise ValueError("liquidity exit requires the retained strategy NFT")
    if isinstance(intent, CollectLiquidityFees):
        return Collect(
            target=intent.target, nft=position.nft, deadline=intent.deadline, range=position.range
        )
    return Burn(
        target=intent.target,
        nft=position.nft,
        liquidity=position.liquidity,
        minimum0=intent.minimum0,
        minimum1=intent.minimum1,
        deadline=intent.deadline,
        range=position.range,
    )


def operation(config, state, intent, before):
    from almanak.operator_deployment import is_operator

    exiting = isinstance(intent, ExitExchange)
    if exiting:
        from almanak.connectors.uniswap_v4.exit import leg

        intent = leg(config, before)
    # An exit leg derives its deadline afresh from each observation, so the allowance
    # its own approval granted ends at an earlier leg's deadline.
    strict = is_operator(state) and not exiting
    through = getattr(intent, "deadline", None) if strict else None
    if isinstance(intent, SwapExactInput):
        step = approval(
            before,
            intent.amount.asset,
            intent.amount.amount,
            intent.target.contracts.router,
            intent.deadline,
            through=through,
        )
        return step or Swap(
            target=intent.target,
            amount=intent.amount,
            minimum=intent.minimum_output,
            deadline=intent.deadline,
        )
    if isinstance(intent, OpenLiquidity):
        # Only an operator's mint is bounded to its stated slippage on chain: a strategy
        # plans, and its retained steps replay, with the intent's whole amounts.
        tolerance = config.allocation(intent.target.allocation).slippage_bps if strict else None
        return open_operation(intent, before, through=through, tolerance_bps=tolerance)
    if isinstance(intent, (CollectLiquidityFees, CloseLiquidity)):
        return exit_operation(intent, before)
    if isinstance(intent, RevokeExchange):
        return cleanup(before)
    raise ValueError("unsupported Uniswap intent")


def call(config, operation):
    target = operation.target
    if isinstance(operation, Approval):
        if operation.expiration is None:
            return abi.approve(target, operation.asset, operation.amount)
        return abi.permit(
            target, operation.asset, operation.spender, operation.amount, operation.expiration
        )
    if isinstance(operation, Swap):
        return abi.swap(target, operation.amount, operation.minimum.amount, operation.deadline)
    if isinstance(operation, Mint):
        return abi.mint(
            target,
            operation.liquidity,
            operation.maximum0.amount,
            operation.maximum1.amount,
            operation.deadline,
            selected=operation.range,
        )
    if isinstance(operation, Collect):
        return abi.collect(target, int(operation.nft.token_id), operation.deadline)
    if isinstance(operation, Burn):
        return abi.close(
            target,
            int(operation.nft.token_id),
            operation.minimum0.amount,
            operation.minimum1.amount,
            operation.deadline,
        )
    raise ValueError("unsupported Uniswap protocol operation")
