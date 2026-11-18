"""Accrual preserves supplier shares and separately issues protocol fee shares."""

from almanak.connectors.morpho_blue.domain import Accounting, Model, Uint256
from almanak.connectors.morpho_blue.math import (
    WAD,
    adaptive_rate,
    compounded,
    mul_div,
    to_shares,
    uint,
)


class Accrual(Model):
    accounting: Accounting
    rate_at_target: Uint256
    interest: Uint256
    fee_shares: Uint256


def accrue(accounting: Accounting, rate_at_target: int, timestamp: int) -> Accrual:
    uint(timestamp, 128)
    if timestamp < accounting.last_update:
        raise ValueError("Morpho accrual cannot precede its retained accounting state")
    elapsed = timestamp - accounting.last_update
    if not elapsed:
        return Accrual(
            accounting=accounting, rate_at_target=rate_at_target, interest=0, fee_shares=0
        )
    rate, target = adaptive_rate(
        accounting.supply_assets, accounting.borrow_assets, rate_at_target, elapsed
    )
    interest = uint(mul_div(accounting.borrow_assets, compounded(rate, elapsed), WAD), 128)
    supplied = uint(accounting.supply_assets + interest, 128)
    borrowed = uint(accounting.borrow_assets + interest, 128)
    fee = mul_div(interest, accounting.fee, WAD)
    fee_shares = uint(to_shares(fee, supplied - fee, accounting.supply_shares), 128)
    result = Accounting(
        supply_assets=supplied,
        supply_shares=uint(accounting.supply_shares + fee_shares, 128),
        borrow_assets=borrowed,
        borrow_shares=accounting.borrow_shares,
        last_update=timestamp,
        fee=accounting.fee,
    )
    return Accrual(
        accounting=result, rate_at_target=target, interest=interest, fee_shares=fee_shares
    )
