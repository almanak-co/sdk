"""Morpho health uses the configured collateral-to-loan oracle and accrued debt shares."""

from almanak.connectors.morpho_blue.accounting import accrue
from almanak.connectors.morpho_blue.domain import Accounting, AccountObservation, Position
from almanak.connectors.morpho_blue.liquidation import maximum_debt
from almanak.connectors.morpho_blue.math import WAD, mul_div, to_assets


def accrued_accounting(account: AccountObservation, timestamp: int | None = None) -> Accounting:
    market = account.market
    at = market.block.timestamp if timestamp is None else timestamp
    return accrue(market.accounting, market.rate_at_target, at).accounting


def supplied_assets(account: AccountObservation, timestamp: int | None = None) -> int:
    accounting = accrued_accounting(account, timestamp)
    return to_assets(
        account.position.supply_shares, accounting.supply_assets, accounting.supply_shares
    )


def borrowed_assets(account: AccountObservation, timestamp: int | None = None) -> int:
    accounting = accrued_accounting(account, timestamp)
    return to_assets(
        account.position.borrow_shares,
        accounting.borrow_assets,
        accounting.borrow_shares,
        round_up=True,
    )


def health_factor(
    account: AccountObservation,
    accounting: Accounting | None = None,
    position: Position | None = None,
) -> int | None:
    owned = account.position if position is None else position
    if not owned.borrow_shares:
        return None
    market = account.market
    if market.price is None:
        raise ValueError("Morpho health requires its configured market oracle observation")
    totals = accrued_accounting(account) if accounting is None else accounting
    debt = to_assets(owned.borrow_shares, totals.borrow_assets, totals.borrow_shares, round_up=True)
    maximum = maximum_debt(owned.collateral, market.price.price, market.source.lltv)
    return mul_div(maximum, WAD, debt)


def require_health(
    account: AccountObservation, accounting: Accounting, position: Position, minimum: int
) -> None:
    factor = health_factor(account, accounting, position)
    if factor is not None and factor < minimum:
        raise ValueError("Morpho operation violates the configured fund/market health floor")
