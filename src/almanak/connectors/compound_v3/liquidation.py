"""Comet absorption clears debt, retains surplus supply and preserves excluded assets."""

from almanak.connectors.compound_v3.domain import (
    AccountObservation,
    CollateralBalance,
    CollateralObservation,
    Market,
    Principal,
    Revision,
)
from almanak.connectors.compound_v3.math import (
    FACTOR_SCALE,
    mul_div,
    present_value,
    principal_value,
    signed,
    uint,
)
from almanak.connectors.compound_v3.risk import active, is_liquidatable, require_price
from almanak.models import Account, AssetAmount, Block, Model


class Absorption(Model):
    market: Market
    fund: Account
    block: Block
    principal: Principal
    collateral_credit: AssetAmount
    absorbed_deficit: AssetAmount
    seized: tuple[CollateralBalance, ...]
    remaining: tuple[CollateralBalance, ...]


def seizure(account: AccountObservation, collateral: CollateralObservation) -> tuple[int, int]:
    if not active(account, collateral.offset):
        return 0, 0
    controlled = account.market.revision is Revision.CONTROLS
    if controlled and collateral.liquidation_factor == 0:
        return 0, 0
    amount = account.collaterals[collateral.offset].balance
    # The controlled implementation reuses the price array from its liquidation check.
    # A zero liquidate factor leaves that array entry zero even if the asset is seized.
    price = (
        0 if controlled and collateral.liquidate_factor == 0 else require_price(collateral.price)
    )
    value = mul_div(amount, price, collateral.scale)
    return amount, mul_div(value, collateral.liquidation_factor, FACTOR_SCALE)


def absorb(account: AccountObservation) -> Absorption:
    if not is_liquidatable(account):
        raise ValueError("Compound absorption requires a liquidatable fund")
    market = account.market
    seized, remaining = [], []
    value = 0
    for collateral in market.collaterals:
        quantity, credit = seizure(account, collateral)
        value = uint(value + credit)
        seized.append(CollateralBalance(asset=collateral.asset_type, balance=quantity))
        remaining.append(
            CollateralBalance(
                asset=collateral.asset_type,
                balance=account.collaterals[collateral.offset].balance - quantity,
            )
        )
    credit = mul_div(value, 10**market.base_asset.decimals, require_price(market.base_price))
    indices = market.accounting.supply_index, market.accounting.borrow_index
    balance = present_value(account.principal, *indices)
    new_balance = signed(balance + signed(credit))
    return Absorption(
        market=market.source,
        fund=account.fund,
        block=market.block,
        principal=principal_value(max(0, new_balance), *indices),
        collateral_credit=AssetAmount(asset=market.source.base_asset, amount=credit),
        absorbed_deficit=AssetAmount(asset=market.source.base_asset, amount=max(0, -new_balance)),
        seized=tuple(seized),
        remaining=tuple(remaining),
    )
