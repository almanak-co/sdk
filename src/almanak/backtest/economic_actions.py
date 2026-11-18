"""Single-operation V4 effects over local positions and inventory."""

from almanak.backtest.economic_data import RAY
from almanak.backtest.economic_math import (
    MarketView,
    debts,
    repay,
    require_healthy,
    require_risk_scope,
    reset_premium,
    risk_view,
)
from almanak.backtest.economic_state import EconomicPosition, Intent, RefreshIntent
from almanak.connectors.aave_v4.history import ceiling
from almanak.lending_intents import (
    BorrowIntent,
    CollateralIntent,
    RepayAmountIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawAmountIntent,
    WithdrawIntent,
)
from almanak.models import Model, UInt


class ActionChange(Model):
    positions: tuple[EconomicPosition, ...]
    inventory_delta: int
    assets: UInt | None
    shares: UInt


def refresh_positions(positions, markets, *, dynamic=False, healthy=False):
    refreshed = tuple(
        p.model_copy(update={"dynamic_config_key": m.risk.reserve.dynamic_config_key})
        if dynamic and p.collateral
        else p
        for p, m in zip(positions, markets, strict=True)
    )
    risk = risk_view(refreshed, markets)
    if healthy:
        require_healthy(risk)
    return tuple(
        reset_premium(p, m.borrow_index, risk.risk_premium_bps)
        if p.drawn_shares
        else p.model_copy(update={"risk_premium_bps": risk.risk_premium_bps})
        for p, m in zip(refreshed, markets, strict=True)
    )


def apply_action(
    intent: Intent,
    positions: tuple[EconomicPosition, ...],
    markets: tuple[MarketView, ...],
    inventory: int,
) -> ActionChange:
    require_risk_scope(positions, markets)
    targets = [p.target for p in positions]
    if intent.target not in targets or inventory < 0:
        raise ValueError("action requires its declared position and nonnegative local inventory")
    index = targets.index(intent.target)
    before, market = positions[index], markets[index]
    updated, delta, shares = position_action(intent, before, market)
    if inventory + delta < 0:
        raise ValueError("action requires actual local inventory; implicit funding is unavailable")
    changed = (*positions[:index], updated, *positions[index + 1 :])
    changed = action_refresh(intent, before, changed, markets)
    return ActionChange(
        positions=changed,
        inventory_delta=delta,
        shares=shares,
        assets=abs(delta) if not isinstance(intent, (CollateralIntent, RefreshIntent)) else None,
    )


def position_action(intent, before, market):
    if isinstance(intent, SupplyIntent):
        assets = intent.amount.amount
        shares = market.supply_shares(assets)
        if shares == 0:
            raise ValueError("supply would mint zero shares")
        return (
            before.model_copy(
                update={
                    "shares": before.shares + shares,
                    "supplied": before.supplied + assets,
                }
            ),
            -assets,
            shares,
        )
    if isinstance(intent, (WithdrawIntent, WithdrawAmountIntent)):
        return withdrawal(intent, before, market)
    if isinstance(intent, BorrowIntent):
        if not market.risk.reserve.borrowable:
            raise ValueError("reserve does not support borrowing")
        shares = ceiling(intent.amount.amount * RAY, market.borrow_index)
        return (
            before.model_copy(
                update={
                    "drawn_shares": before.drawn_shares + shares,
                    "borrowed": before.borrowed + intent.amount.amount,
                }
            ),
            intent.amount.amount,
            shares,
        )
    if isinstance(intent, (RepayIntent, RepayAmountIntent)):
        amount = (
            sum(debts(before, market.borrow_index))
            if intent.amount is None
            else intent.amount.amount
        )
        after, assets, shares = repay(before, market, amount)
        return after.model_copy(update={"repaid": before.repaid + assets}), -assets, shares
    if isinstance(intent, CollateralIntent):
        if before.collateral == intent.enabled:
            return before, 0, 0
        key = (
            market.risk.reserve.dynamic_config_key if intent.enabled else before.dynamic_config_key
        )
        return (
            before.model_copy(update={"collateral": intent.enabled, "dynamic_config_key": key}),
            0,
            0,
        )
    if isinstance(intent, RefreshIntent):
        return before, 0, 0
    raise ValueError("unsupported economic action")


def withdrawal(intent, before, market):
    maximum = market.supplied(before.shares)
    assets = maximum if intent.amount is None else min(maximum, intent.amount.amount)
    shares = min(before.shares, market.supply_shares(assets, up=True))
    if shares == 0:
        raise ValueError("withdrawal requires a nonzero owned claim")
    return (
        before.model_copy(
            update={
                "shares": before.shares - shares,
                "withdrawn": before.withdrawn + assets,
            }
        ),
        assets,
        shares,
    )


def action_refresh(intent, before, positions, markets):
    if isinstance(intent, RefreshIntent):
        return refresh_positions(
            positions, markets, dynamic=intent.dynamic_config, healthy=intent.dynamic_config
        )
    validates = (
        isinstance(intent, BorrowIntent)
        or (isinstance(intent, (WithdrawIntent, WithdrawAmountIntent)) and before.collateral)
        or (isinstance(intent, CollateralIntent) and before.collateral and not intent.enabled)
    )
    if validates:
        return refresh_positions(positions, markets, dynamic=True, healthy=True)
    return positions
