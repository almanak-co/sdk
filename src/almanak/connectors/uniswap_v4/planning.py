"""Economic intents become bounded periphery steps without author-managed approvals."""

from almanak.connectors.erc20_approval import requires_zero_first
from almanak.connectors.uniswap_v4 import abi
from almanak.connectors.uniswap_v4.math import liquidity_for, sqrt_at_tick
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

    A Permit2 allowance is relied on only while it outlasts `through`, the latest time
    the spend may be mined, so one that would lapse before the spend's own deadline is
    granted again. An exit leg derives its deadline afresh from each observation and
    passes none: the allowance its own approval granted ends at an earlier leg's
    deadline, and is relied on while it has not lapsed.
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


def open_operation(intent, before):
    if before.owned.position is not None:
        raise ValueError("mint cannot replace an owned NFT")
    target = intent.target
    for maximum in (intent.maximum0, intent.maximum1):
        step = approval(
            before,
            maximum.asset,
            maximum.amount,
            target.contracts.position_manager,
            intent.deadline,
            through=intent.deadline,
        )
        if step:
            return step
    return liquidity_operation(intent, before)


def liquidity_operation(intent, before):
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
        maximum0=intent.maximum0,
        maximum1=intent.maximum1,
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
    exiting = isinstance(intent, ExitExchange)
    if exiting:
        from almanak.connectors.uniswap_v4.exit import leg

        intent = leg(config, before)
    if isinstance(intent, SwapExactInput):
        step = approval(
            before,
            intent.amount.asset,
            intent.amount.amount,
            intent.target.contracts.router,
            intent.deadline,
            through=None if exiting else intent.deadline,
        )
        return step or Swap(
            target=intent.target,
            amount=intent.amount,
            minimum=intent.minimum_output,
            deadline=intent.deadline,
        )
    if isinstance(intent, OpenLiquidity):
        return open_operation(intent, before)
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
