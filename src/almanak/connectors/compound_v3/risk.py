"""Comet risk checks use stored indices, ordered asset bits and revision semantics."""

from almanak.connectors.compound_v3.domain import (
    AccountObservation,
    CollateralObservation,
    OracleQuote,
    Revision,
)
from almanak.connectors.compound_v3.math import (
    FACTOR_SCALE,
    mul_div,
    present_value,
    signed,
    signed_price,
)


def require_price(quote: OracleQuote | None) -> int:
    if quote is None:
        raise ValueError("Compound risk requires the configured market oracle observation")
    return quote.price


def active(account: AccountObservation, offset: int) -> bool:
    bitmap = account.assets_in | (account.extended_assets_in << 16)
    return bool(bitmap & (1 << offset))


def base_liquidity(account: AccountObservation) -> int:
    market = account.market
    accounting = market.accounting
    # Comet's risk views use persisted indices; balanceOf projects interest separately.
    balance = present_value(account.principal, accounting.supply_index, accounting.borrow_index)
    return signed_price(balance, require_price(market.base_price), 10**market.base_asset.decimals)


def collateral_liquidity(
    account: AccountObservation, collateral: CollateralObservation, borrowing: bool
) -> int:
    factor = collateral.borrow_factor if borrowing else collateral.liquidate_factor
    controls = account.market.controls
    if (
        borrowing
        and controls is not None
        and controls.deactivated_collaterals & (1 << collateral.offset)
    ):
        raise ValueError("Compound borrow check encountered deactivated collateral")
    if account.market.revision is Revision.CONTROLS and factor == 0:
        return 0
    balance = account.collaterals[collateral.offset].balance
    value = mul_div(balance, require_price(collateral.price), collateral.scale)
    return signed(mul_div(value, factor, FACTOR_SCALE))


def collateralized(account: AccountObservation, *, borrowing: bool) -> bool:
    if account.principal >= 0:
        return True
    liquidity = base_liquidity(account)
    for collateral in account.market.collaterals:
        if not active(account, collateral.offset):
            continue
        # Preserve the deployed early return before reading later asset controls or prices.
        if liquidity >= 0:
            return True
        liquidity = signed(liquidity + collateral_liquidity(account, collateral, borrowing))
    return liquidity >= 0


def is_borrow_collateralized(account: AccountObservation) -> bool:
    return collateralized(account, borrowing=True)


def is_liquidatable(account: AccountObservation) -> bool:
    return not collateralized(account, borrowing=False)
