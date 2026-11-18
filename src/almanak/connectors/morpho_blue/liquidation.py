"""Morpho liquidation reduces shared lender assets when collateral cannot cover debt."""

from almanak.connectors.morpho_blue.domain import Accounting, Position, Uint256
from almanak.connectors.morpho_blue.math import (
    ORACLE_SCALE,
    WAD,
    mul_div,
    to_assets,
    to_shares,
    uint,
)
from almanak.models import Model


class Liquidation(Model):
    accounting: Accounting
    position: Position
    repaid_assets: Uint256
    repaid_shares: Uint256
    seized_collateral: Uint256
    bad_debt_assets: Uint256
    bad_debt_shares: Uint256


def maximum_debt(collateral: int, price: int, lltv: int) -> int:
    uint(lltv)
    if not 0 < lltv < WAD:
        raise ValueError("Morpho LLTV must be between zero and one WAD")
    return mul_div(mul_div(collateral, price, ORACLE_SCALE), lltv, WAD)


def liquidation_incentive(lltv: int) -> int:
    maximum_debt(0, 0, lltv)
    return min(115 * 10**16, mul_div(WAD, WAD, WAD - mul_div(3 * 10**17, WAD - lltv, WAD)))


def liquidation_amounts(
    accounting: Accounting, price: int, lltv: int, seized: int, shares: int
) -> tuple[int, int, int]:
    uint(seized, 128)
    uint(shares, 128)
    if bool(seized) == bool(shares):
        raise ValueError("Morpho liquidation requires exactly one nonzero amount")
    incentive = liquidation_incentive(lltv)
    if seized:
        quote = mul_div(seized, price, ORACLE_SCALE, round_up=True)
        loan = mul_div(quote, WAD, incentive, round_up=True)
        shares = to_shares(loan, accounting.borrow_assets, accounting.borrow_shares, round_up=True)
    else:
        loan = to_assets(shares, accounting.borrow_assets, accounting.borrow_shares)
        seized = mul_div(mul_div(loan, incentive, WAD), ORACLE_SCALE, price)
    assets = to_assets(shares, accounting.borrow_assets, accounting.borrow_shares, round_up=True)
    return assets, uint(shares, 128), uint(seized, 128)


def liquidate(
    accounting: Accounting,
    position: Position,
    price: int,
    lltv: int,
    *,
    seized: int = 0,
    shares: int = 0,
) -> Liquidation:
    debt = to_assets(
        position.borrow_shares, accounting.borrow_assets, accounting.borrow_shares, round_up=True
    )
    if debt <= maximum_debt(position.collateral, price, lltv):
        raise ValueError("Morpho healthy position cannot be liquidated")
    assets, shares, seized = liquidation_amounts(accounting, price, lltv, seized, shares)
    collateral = uint(position.collateral - seized, 128)
    remaining = uint(position.borrow_shares - shares, 128)
    market_shares = uint(accounting.borrow_shares - shares, 128)
    market_assets = max(0, accounting.borrow_assets - assets)
    bad_shares = remaining if collateral == 0 else 0
    bad_assets = (
        min(market_assets, to_assets(bad_shares, market_assets, market_shares, round_up=True))
        if bad_shares
        else 0
    )
    updated = Accounting(
        supply_assets=uint(accounting.supply_assets - bad_assets, 128),
        supply_shares=accounting.supply_shares,
        borrow_assets=uint(market_assets - bad_assets, 128),
        borrow_shares=uint(market_shares - bad_shares, 128),
        last_update=accounting.last_update,
        fee=accounting.fee,
    )
    owned = Position(
        supply_shares=position.supply_shares,
        borrow_shares=remaining - bad_shares,
        collateral=collateral,
    )
    return Liquidation(
        accounting=updated,
        position=owned,
        repaid_assets=assets,
        repaid_shares=shares,
        seized_collateral=seized,
        bad_debt_assets=bad_assets,
        bad_debt_shares=bad_shares,
    )
