"""Economic actions conserve inventory and retain action-dependent V4 risk refresh."""

import pytest

from almanak.backtest.economic_actions import apply_action
from almanak.backtest.economic_data import RAY
from almanak.backtest.economic_math import debts, premium_ray
from almanak.backtest.economic_state import RefreshIntent
from almanak.lending import SupplyTarget
from almanak.lending_intents import (
    BorrowIntent,
    CollateralIntent,
    RepayAmountIntent,
    RepayIntent,
    SupplyIntent,
    WithdrawAmountIntent,
    WithdrawIntent,
)
from almanak.models import AssetAmount
from tests.test_economic_math import debt_reserve, dynamic, market, position
from tests.test_economic_math import reserve as reserve


def amount(target, units):
    return AssetAmount(asset=target.asset, amount=units)


def test_supply_borrow_partial_repay_and_close_conserve_assets(reserve):
    d = debt_reserve(reserve)
    positions = (position(reserve), position(d))
    markets = (market(reserve), market(d, price=10**8))
    ctarget, dtarget = (p.target for p in positions)
    wallet = {ctarget: 100 * 10**18, dtarget: 0}
    actions = (
        SupplyIntent[SupplyTarget](target=ctarget, amount=amount(ctarget, 100 * 10**18)),
        CollateralIntent[SupplyTarget](target=ctarget, enabled=True),
        BorrowIntent[SupplyTarget](target=dtarget, amount=amount(dtarget, 5000 * 10**6)),
        RepayAmountIntent[SupplyTarget](target=dtarget, amount=amount(dtarget, 1000 * 10**6)),
        RepayIntent[SupplyTarget](target=dtarget),
        CollateralIntent[SupplyTarget](target=ctarget, enabled=False),
        WithdrawAmountIntent[SupplyTarget](target=ctarget, amount=amount(ctarget, 20 * 10**18)),
        WithdrawIntent[SupplyTarget](target=ctarget),
    )
    for intent in actions:
        change = apply_action(intent, positions, markets, wallet[intent.target])
        wallet[intent.target] += change.inventory_delta
        positions = change.positions
    assert wallet == {ctarget: 100 * 10**18, dtarget: 0}
    assert positions[0].shares == positions[1].drawn_shares == 0
    assert positions[0].supplied == positions[0].withdrawn == 100 * 10**18
    assert positions[1].borrowed == positions[1].repaid == 5000 * 10**6


def test_unsafe_withdrawal_and_unfunded_repayment_leave_inputs_intact(reserve):
    d = debt_reserve(reserve)
    positions = (
        position(reserve, shares=100 * 10**18, collateral=True),
        position(d, drawn_shares=5000 * 10**6),
    )
    markets = (market(reserve), market(d, price=10**8))
    with pytest.raises(ValueError, match="health factor"):
        apply_action(
            WithdrawIntent[SupplyTarget](target=positions[0].target), positions, markets, 0
        )
    with pytest.raises(ValueError, match="actual local inventory"):
        apply_action(RepayIntent[SupplyTarget](target=positions[1].target), positions, markets, 0)
    assert positions[0].shares == 100 * 10**18
    assert positions[1].drawn_shares == 5000 * 10**6


def test_new_dynamic_config_is_attached_only_by_refreshing_actions(reserve):
    reserve = reserve.model_copy(update={"dynamic_config_key": 1})
    cm = market(reserve.model_copy(update={"dynamic_config_key": 0}))
    cm = cm.model_copy(
        update={
            "risk": cm.risk.model_copy(
                update={"reserve": reserve, "configs": (dynamic(0), dynamic(1, 7000))}
            )
        }
    )
    p = position(reserve, shares=10**18, collateral=True)
    supply = SupplyIntent[SupplyTarget](target=p.target, amount=amount(p.target, 10**18))
    after = apply_action(supply, (p,), (cm,), 10**18).positions
    assert after[0].dynamic_config_key == 0
    for flag in (False, True):
        change = apply_action(RefreshIntent(target=p.target, dynamic_config=flag), after, (cm,), 0)
        assert change.positions[0].dynamic_config_key == int(flag)
    same = CollateralIntent[SupplyTarget](target=p.target, enabled=True)
    assert apply_action(same, (p,), (cm,), 0).positions[0].dynamic_config_key == 0


def test_repayment_does_not_reprice_risk_premium(reserve):
    from almanak.backtest.economic_math import reset_premium

    p = reset_premium(position(reserve, drawn_shares=1000), RAY, 1000)
    m = market(reserve, borrow_index=12 * RAY // 10)
    result = apply_action(
        RepayAmountIntent[SupplyTarget](target=p.target, amount=amount(p.target, 10)),
        (p,),
        (m,),
        10,
    )
    assert result.positions[0].risk_premium_bps == 1000
    assert debts(result.positions[0], m.borrow_index) == (1200, 10)
    assert premium_ray(result.positions[0], m.borrow_index) == 10 * RAY
