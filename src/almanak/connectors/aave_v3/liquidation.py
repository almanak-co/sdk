"""Revision-11 liquidation amounts and scaled consumption, without keeper execution."""

from almanak.connectors.aave_v3.domain import OracleQuote, PositionObservation, Uint256
from almanak.connectors.aave_v3.math import (
    debt_assets,
    mul_div,
    repay_shares,
    supplied_assets,
    transition_indices,
    uint,
    withdraw_shares,
)
from almanak.connectors.aave_v3.risk import RiskInputs, project
from almanak.models import Model

BPS = 10_000
CLOSE_FACTOR_THRESHOLD = 95 * 10**16
MIN_CLOSE_FACTOR_BASE = 2000 * 10**8
MIN_LEFTOVER_BASE = 1000 * 10**8


class LiquidationUnavailable(ValueError):
    """Observed liquidation constraints prevent settlement without erasing obligations."""


def quote(position: PositionObservation) -> OracleQuote:
    result = position.reserve.oracle
    if result is None:
        raise ValueError("liquidation requires the configured oracle observation")
    return result


class LiquidationAmounts(Model):
    collateral_to_liquidator: Uint256
    debt_repaid: Uint256
    protocol_fee: Uint256
    collateral_scaled: Uint256
    fee_scaled: Uint256
    debt_scaled: Uint256
    deficit: Uint256
    no_collateral_left: bool


def maximum_debt(collateral, debt, risk, indices):
    supply_index, debt_index = indices
    collateral_base = mul_div(
        supplied_assets(collateral.supplied_scaled, supply_index),
        quote(collateral).price,
        10**collateral.reserve.asset.decimals,
    )
    maximum = debt_assets(debt.debt_scaled, debt_index)
    debt_base = mul_div(maximum, quote(debt).price, 10**debt.reserve.asset.decimals, ceiling=True)
    if (
        collateral_base >= MIN_CLOSE_FACTOR_BASE
        and debt_base >= MIN_CLOSE_FACTOR_BASE
        and risk.health_factor_wad > CLOSE_FACTOR_THRESHOLD
    ):
        default = uint(risk.total_debt_base * 5000 + 5000) // BPS
        if debt_base > default:
            maximum = mul_div(default, 10**debt.reserve.asset.decimals, quote(debt).price)
    return maximum


def collateral_amounts(collateral, debt, debt_to_cover, supply_index):
    balance = supplied_assets(collateral.supplied_scaled, supply_index)
    config = collateral.reserve.configuration
    price_c, price_d = quote(collateral).price, quote(debt).price
    unit_c, unit_d = 10**collateral.reserve.asset.decimals, 10**debt.reserve.asset.decimals
    base = uint(price_d * debt_to_cover * unit_c) // uint(price_c * unit_d)
    amount = mul_div(base, config.liquidation_bonus_bps, BPS)
    paid = debt_to_cover
    if amount > balance:
        amount = balance
        value = uint(price_c * amount * unit_d) // uint(price_d * unit_c)
        paid = mul_div(value, BPS, config.liquidation_bonus_bps, ceiling=True)
    bonus = amount - mul_div(amount, BPS, config.liquidation_bonus_bps)
    fee = mul_div(bonus, config.liquidation_protocol_fee_bps, BPS, ceiling=True)
    return amount - fee, paid, fee


def require_no_dust(collateral, debt, amount, paid, fee, indices):
    supply_index, debt_index = indices
    balance = supplied_assets(collateral.supplied_scaled, supply_index)
    borrowed = debt_assets(debt.debt_scaled, debt_index)
    if paid == borrowed or amount + fee == balance:
        return
    remaining_c = mul_div(
        balance - amount - fee,
        quote(collateral).price,
        10**collateral.reserve.asset.decimals,
    )
    remaining_d = mul_div(
        borrowed - paid, quote(debt).price, 10**debt.reserve.asset.decimals, ceiling=True
    )
    if min(remaining_c, remaining_d) < MIN_LEFTOVER_BASE:
        raise LiquidationUnavailable(
            "V3 liquidation would leave protocol-forbidden collateral or debt dust"
        )


def scaled_consumption(collateral, amount, fee, index):
    burned = withdraw_shares(amount, index)
    if burned > collateral.supplied_scaled:
        raise ValueError("V3 liquidation exceeds owned collateral shares")
    fee_scaled = min(withdraw_shares(fee, index), collateral.supplied_scaled - burned)
    if fee_scaled < withdraw_shares(fee, index):
        fee = supplied_assets(fee_scaled, index)
    return burned, fee_scaled, fee


def eligible(collateral, debt, inputs, timestamp, grace):
    if collateral not in inputs.positions or debt not in inputs.positions:
        raise ValueError("liquidation requires the complete owned fund/Pool risk scope")
    if not collateral.collateral_enabled or not debt.debt_scaled:
        raise ValueError("liquidation requires enabled collateral and owned variable debt")
    if any(
        not p.reserve.configuration.active or p.reserve.configuration.paused
        for p in (collateral, debt)
    ):
        raise ValueError("liquidation reserve is inactive or paused")
    if any(uint(until, 40) >= timestamp for until in grace):
        raise ValueError("liquidation is inside the reserve grace period")
    risk = project(inputs, timestamp=timestamp).risk
    if risk.health_factor_wad >= 10**18:
        raise ValueError("liquidation requires health below one")
    if collateral.reserve.configuration.liquidation_bonus_bps < BPS:
        raise ValueError("liquidation requires the observed collateral bonus")
    return risk


def calculate(
    collateral: PositionObservation,
    debt: PositionObservation,
    inputs: RiskInputs,
    *,
    timestamp: int,
    grace: tuple[int, int],
    debt_to_cover: int,
) -> LiquidationAmounts:
    risk = eligible(collateral, debt, inputs, timestamp, grace)
    supply_index, _ = transition_indices(collateral.reserve, timestamp)
    _, debt_index = transition_indices(debt.reserve, timestamp)
    return settle(collateral, debt, risk, (supply_index, debt_index), debt_to_cover)


def settle(collateral, debt, risk, indices, debt_to_cover) -> LiquidationAmounts:
    """Use explicitly selected indices after the caller validates complete Pool risk."""
    supply_index, debt_index = indices
    requested = min(uint(debt_to_cover), maximum_debt(collateral, debt, risk, indices))
    amount, paid, fee = collateral_amounts(collateral, debt, requested, supply_index)
    if not amount or not paid:
        raise LiquidationUnavailable("liquidation cannot settle zero collateral or debt")
    require_no_dust(collateral, debt, amount, paid, fee, indices)
    burned, fee_scaled, fee = scaled_consumption(collateral, amount, fee, supply_index)
    consumed = mul_div(
        supplied_assets(burned + fee_scaled, supply_index),
        quote(collateral).price,
        10**collateral.reserve.asset.decimals,
    )
    no_collateral_left = consumed == risk.total_collateral_base
    borrowed = debt_assets(debt.debt_scaled, debt_index)
    return LiquidationAmounts(
        collateral_to_liquidator=amount,
        debt_repaid=paid,
        protocol_fee=fee,
        collateral_scaled=burned,
        fee_scaled=fee_scaled,
        debt_scaled=repay_shares(borrowed if no_collateral_left else paid, debt_index),
        deficit=borrowed - paid if no_collateral_left else 0,
        no_collateral_left=no_collateral_left,
    )
