"""Revision-specific Comet rates and view-time interest projection."""

from almanak.connectors.compound_v3.domain import MarketObservation, RateCurve, Revision
from almanak.connectors.compound_v3.math import (
    accrue_index,
    kink_rate,
    present_supply,
    present_value,
    uint,
    utilization,
)


def curve_rate(curve: RateCurve, use: int) -> int:
    return kink_rate(
        use,
        curve.kink,
        curve.base_per_second,
        curve.slope_low_per_second,
        curve.slope_high_per_second,
    )


def rates(market: MarketObservation) -> tuple[int, int]:
    accounting = market.accounting
    use = utilization(
        accounting.total_supply_principal,
        accounting.total_borrow_principal,
        (accounting.supply_index, accounting.borrow_index),
    )
    supply = curve_rate(market.supply_curve, use)
    borrow = curve_rate(market.borrow_curve, use)
    if market.revision is Revision.CONTROLS:
        supply = controlled_supply_rate(market, use, supply)
        if accounting.total_borrow_principal == 0:
            borrow = 0
    return supply, borrow


def controlled_supply_rate(market: MarketObservation, use: int, rate: int) -> int:
    accounting = market.accounting
    if accounting.total_supply_principal == 0:
        return 0
    supplied = present_supply(accounting.total_supply_principal, accounting.supply_index)
    if use == 0 and market.supply_curve.base_per_second != 0 and supplied >= market.base_available:
        return 0
    return rate


def indices(market: MarketObservation, timestamp: int) -> tuple[int, int]:
    elapsed = uint(timestamp, 40) - market.accounting.last_accrual_time
    if elapsed < 0:
        raise ValueError("Compound interest projection cannot precede retained accounting")
    supply, borrow = market.accounting.supply_index, market.accounting.borrow_index
    if elapsed == 0:
        return supply, borrow
    supply_rate, borrow_rate = rates(market)
    return accrue_index(supply, supply_rate, elapsed), accrue_index(borrow, borrow_rate, elapsed)


def validate_account(account):
    supply, borrow = indices(account.market, account.market.block.timestamp)
    balance = present_value(account.principal, supply, borrow)
    if (account.supplied_base, account.borrowed_base) != (max(0, balance), max(0, -balance)):
        raise ValueError(
            "Comet base balance views disagree with the retained principal and indices"
        )
    return account
