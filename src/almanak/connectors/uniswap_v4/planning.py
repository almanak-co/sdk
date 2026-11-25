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


def band_edges(price, tolerance_bps):
    """The square-root prices at `price` less and plus the tolerance.

    The gateway's price-movement checks compare squared prices against the same
    tolerance, so no square-root price they admit lies outside these two. Unlike
    `concentrated_math.tolerance_edges` the high edge is exact, not one unit above.
    """
    from math import isqrt

    return tuple(isqrt(price * price * (10000 + sign * tolerance_bps) // 10000) for sign in (-1, 1))


def tolerant_liquidity(amount0, amount1, price, lower, upper, tolerance_bps):
    """The liquidity of an operator's mint of the amounts planned at `price`.

    It is the most the amounts cover at every price within the tolerance of `price`.
    A fixed liquidity takes the most of the first token at the lowest price and the
    most of the second at the highest, so the smaller of what the amounts cover at the
    two edges is covered everywhere between them. It is never more than they cover at
    `price` itself.

    The exception is one amount stated beside the range: the price is at or beyond a
    bound, where the range takes one token only, and none of the other token is stated.
    The liquidity is then what the amount covers at `price`, the same at every price on
    that side. Nothing is sized for the price entering the range, where the mint would
    need the token of which it may take none.

    All arguments but the tolerance are square-root prices or base units.
    """
    beside = (price <= lower and not amount1) or (price >= upper and not amount0)
    if beside:
        return liquidity_for(amount0, amount1, price, lower, upper)
    return min(
        liquidity_for(amount0, amount1, edge, lower, upper)
        for edge in band_edges(price, tolerance_bps)
    )


def bounded_mint(intent, before, tolerance_bps):
    """The liquidity of an operator's mint planned at this observation, and the most of
    each token it may take.

    A mint is for a fixed liquidity, and the PositionManager reverts one that needs
    more of a token than its maximum. The liquidity is `tolerant_liquidity` of the
    intent's amounts around this observation's price: what they cover at the edge of
    the tolerance where each is needed most. Each maximum is what that liquidity takes
    at that edge, the price less the tolerance for the first token and the price plus
    the tolerance for the second, rounded up as the PoolManager charges it. Neither
    exceeds the intent's amount.

    So a mint mined at a price within the tolerance of the price it was planned at fits
    its maximums in both directions, and one mined further away reverts. The liquidity
    is smaller than the amounts cover at the planned price, by more the narrower the
    range is against the tolerance, and what the mint does not take stays in the wallet.

    The range bounds what the tolerance cannot. Where an edge of the tolerance lies at
    or beyond a bound of the range, the liquidity is what the amount covers of the
    whole range in that token, the maximum is all the range holds of it, and no price
    in that direction reverts the mint. A range wholly beside the whole tolerance takes
    one token only, the same amount at every price on that side. Where the tolerance
    reaches into a range that the price lies beside, both tokens are needed inside it
    and the liquidity is sized for both. With only the one token stated that the range
    takes at this price, the mint is for all that amount covers and may take none of
    the other, so it reverts once the price has entered the range.
    """
    selected = concrete_range(intent.target, requested=intent.range)
    lower, upper = sqrt_at_tick(selected.lower), sqrt_at_tick(selected.upper)
    price = before.pool.sqrt_price_x96
    stated = intent.maximum0, intent.maximum1
    liquidity = tolerant_liquidity(
        stated[0].amount, stated[1].amount, price, lower, upper, tolerance_bps
    )
    low, high = band_edges(price, tolerance_bps)
    most = (
        amounts(liquidity, low, lower, upper, up=True)[0],
        amounts(liquidity, high, lower, upper, up=True)[1],
    )
    return liquidity, tuple(
        value.model_copy(update={"amount": min(value.amount, limit)})
        for value, limit in zip(stated, most, strict=True)
    )


def bounded_maximums(intent, before, tolerance_bps):
    """The most of each token an operator's mint planned at this observation may take."""
    return bounded_mint(intent, before, tolerance_bps)[1]


def open_operation(intent, before, *, through=None, tolerance_bps=None):
    """The next step of a mint: an approval it still needs, or the mint itself.

    Given `tolerance_bps`, as only an operator request gives it, the mint takes the
    liquidity and the maximums of `bounded_mint` at this observation instead of the
    intent's whole amounts. Its approvals do not follow the observation: a token the
    mint may take any of is approved for the intent's whole amount, which no later
    observation's maximum exceeds, so a price that moves between the steps plans no
    approval again. A standing approval that covers the intent's amount is relied on
    as it is.
    """
    if before.owned.position is not None:
        raise ValueError("mint cannot replace an owned NFT")
    target = intent.target
    stated = (intent.maximum0, intent.maximum1)
    liquidity, maximums = None, stated
    if tolerance_bps is not None:
        liquidity, maximums = bounded_mint(intent, before, tolerance_bps)
    for whole, maximum in zip(stated, maximums, strict=True):
        step = approval(
            before,
            whole.asset,
            whole.amount if maximum.amount else 0,
            target.contracts.position_manager,
            intent.deadline,
            through=through,
        )
        if step:
            return step
    return liquidity_operation(intent, before, maximums, liquidity)


def liquidity_operation(intent, before, maximums=None, liquidity=None):
    """The mint of `liquidity`, allowed to take at most `maximums`.

    Unless given, as only an operator's mint gives them, the liquidity is the most the
    intent's amounts cover at this observation's price and the maximums are the
    intent's whole amounts.
    """
    if maximums is None:
        maximums = (intent.maximum0, intent.maximum1)
    target = intent.target
    selected = concrete_range(target, requested=intent.range)
    if liquidity is None:
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
        # Only an operator's mint is sized for and bounded to its stated slippage: a
        # strategy plans, and its retained steps replay, with the intent's whole amounts
        # and the liquidity they cover at the observed price.
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
