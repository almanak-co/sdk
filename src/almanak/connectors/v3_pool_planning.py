"""Bounded economic decisions choose exact V3 calls and connector-owned approvals."""

from collections.abc import Callable
from dataclasses import dataclass
from types import ModuleType

from almanak.connectors.concentrated_math import (
    addition_minimums,
    amounts,
    liquidity_for,
    sqrt_at_tick,
)
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


def concrete_range(target, *, position=None, requested=None):
    """The ticks a liquidity operation acts on: the fixed range or the band's selection."""
    if target.band is None:
        if target.range is None:
            raise ValueError("liquidity operation requires an explicit configured tick range")
        if requested is not None and (requested.lower, requested.upper) != (
            target.range.lower,
            target.range.upper,
        ):
            raise ValueError("fixed-range liquidity cannot select another tick range")
        return target.range
    if position is not None:
        if position.range is None:
            raise ValueError("banded liquidity requires the retained position range")
        return position.range
    if requested is None:
        raise ValueError("banded liquidity requires an explicitly selected tick range")
    return target.band.validate_range(requested, target.pool.tick_spacing)


def sqrt_bounds(target, *, position=None, requested=None):
    selected = concrete_range(target, position=position, requested=requested)
    return sqrt_at_tick(selected.lower), sqrt_at_tick(selected.upper)


def operation_range_value(target, selected):
    """Only banded operations carry their range; fixed targets keep their retained shape."""
    return selected if target.band is not None else None


@dataclass(frozen=True)
class Planner:
    operations: ModuleType
    admit: Callable

    def approval(self, before, asset, spender, amount):
        current = next(a for a in before.allowances if a.asset == asset and a.spender == spender)
        if current.amount == amount or not amount:
            return None
        # Reset prior authority before granting a new exact finite amount. This also
        # supports ERC-20 implementations that refuse nonzero-to-nonzero approvals.
        return self.operations.Approval(
            target=before.target,
            asset=asset,
            spender=spender,
            amount=0 if current.amount else amount,
        )

    def cleanup(self, before):
        active = next((a for a in before.allowances if a.amount), None)
        if active is None:
            raise ValueError("V3 spending authority is already closed")
        return self.operations.Approval(
            target=before.target, asset=active.asset, spender=active.spender, amount=0
        )

    def operation(self, config, state, intent, before):
        if intent.target != before.target or intent.target != config.target(
            intent.target.allocation
        ):
            raise ValueError("V3 decision must retain its configured observation target")
        self.admit(state.capabilities, intent)
        if isinstance(intent, ExitExchange):
            return self.exit_operation(config, before)
        if isinstance(intent, RevokeExchange):
            return self.cleanup(before)
        if isinstance(intent, SwapExactInput):
            return self.approval(
                before, intent.amount.asset, intent.target.contracts.router, intent.amount.amount
            ) or self.operations.Swap(
                target=intent.target,
                amount=intent.amount,
                minimum=intent.minimum_output,
                deadline=intent.deadline,
            )
        if isinstance(intent, (OpenLiquidity, IncreaseLiquidity)):
            return self.addition(config, intent, before)
        return self.removal(intent, before)

    def addition(self, config, intent, before):
        position = before.owned.position
        if isinstance(intent, OpenLiquidity) == (position is not None):
            raise ValueError("liquidity addition must preserve existing NFT ownership")
        selected = concrete_range(
            intent.target, position=position, requested=getattr(intent, "range", None)
        )
        lower, upper = sqrt_at_tick(selected.lower), sqrt_at_tick(selected.upper)
        price = before.pool.sqrt_price_x96
        liquidity = liquidity_for(
            intent.maximum0.amount, intent.maximum1.amount, price, lower, upper
        )
        # Floors hold across the price window that price_movement admits until signing,
        # so a mint the gate accepts cannot revert on the position manager's slippage check.
        *minimums, minimum_liquidity = addition_minimums(
            intent.maximum0.amount,
            intent.maximum1.amount,
            price,
            lower,
            upper,
            config.allocation(intent.target.allocation).slippage_bps,
        )
        values = dict(
            target=intent.target,
            maximum0=intent.maximum0,
            maximum1=intent.maximum1,
            minimum0=AssetAmount(
                asset=intent.target.assets[0], amount=min(minimums[0], intent.maximum0.amount)
            ),
            minimum1=AssetAmount(
                asset=intent.target.assets[1], amount=min(minimums[1], intent.maximum1.amount)
            ),
            minimum_liquidity=max(1, minimum_liquidity),
            deadline=intent.deadline,
            range=operation_range_value(intent.target, selected),
        )
        if not 0 < liquidity < 2**128:
            raise ValueError("available V3 inventory cannot provide positive uint128 liquidity")
        for maximum in (intent.maximum0, intent.maximum1):
            step = self.approval(
                before, maximum.asset, intent.target.contracts.position_manager, maximum.amount
            )
            if step is not None:
                return step
        return (
            self.operations.Mint(**values)
            if position is None
            else self.operations.Increase(nft=position.nft, **values)
        )

    def removal(self, intent, before):
        position = before.owned.position
        if position is None:
            raise ValueError("V3 liquidity exit requires the retained fund-owned NFT")
        if isinstance(intent, CollectLiquidityFees):
            return self.operations.Collect(
                target=intent.target,
                nft=position.nft,
                deadline=intent.deadline,
                range=position.range,
            )
        if isinstance(intent, DecreaseLiquidity):
            return self.partial_removal(intent, position)
        if isinstance(intent, CloseLiquidity):
            return self.operations.Close(
                target=intent.target,
                nft=position.nft,
                liquidity=position.liquidity,
                minimum0=intent.minimum0,
                minimum1=intent.minimum1,
                deadline=intent.deadline,
                range=position.range,
            )
        raise ValueError("unsupported V3 economic action")

    def partial_removal(self, intent, position):
        if not 0 < intent.liquidity < position.liquidity:
            raise ValueError("partial liquidity decrease must retain positive owned liquidity")
        return self.operations.Decrease(
            target=intent.target,
            nft=position.nft,
            liquidity=intent.liquidity,
            minimum0=intent.minimum0,
            minimum1=intent.minimum1,
            deadline=intent.deadline,
            range=position.range,
        )

    def exit_operation(self, config, before):
        allocation = config.allocation(before.allocation)
        deadline = before.block.timestamp + allocation.deadline_seconds
        if before.owned.position is not None:
            return self.close_position(allocation, before, deadline)
        if before.exit_quote is not None:
            return self.exit_swap(allocation, before, deadline)
        return residual_leg(
            not counter_settled(before.owned, before.exit_residual),
            before,
            self.cleanup,
            "remaining counter-currency inventory requires an explicit exit quote",
        )

    def close_position(self, allocation, before, deadline):
        position = before.owned.position
        principal = amounts(
            position.liquidity,
            before.pool.sqrt_price_x96,
            *sqrt_bounds(before.target, position=position),
        )
        minimums = tuple(
            AssetAmount(asset=asset, amount=value * (10000 - allocation.slippage_bps) // 10000)
            for asset, value in zip(before.target.assets, principal, strict=True)
        )
        return self.operations.Close(
            target=before.target,
            nft=position.nft,
            liquidity=position.liquidity,
            minimum0=minimums[0],
            minimum1=minimums[1],
            deadline=deadline,
            range=position.range,
        )

    def exit_swap(self, allocation, before, deadline):
        quote = before.exit_quote
        minimum = AssetAmount(
            asset=quote.amount_out.asset,
            amount=max(1, quote.amount_out.amount * (10000 - allocation.slippage_bps) // 10000),
        )
        return self.approval(
            before, quote.amount_in.asset, before.target.contracts.router, quote.amount_in.amount
        ) or self.operations.Swap(
            target=before.target, amount=quote.amount_in, minimum=minimum, deadline=deadline
        )
