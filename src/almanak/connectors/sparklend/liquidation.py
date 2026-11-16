"""Revision-4 liquidation consumes paid debt only and retains uncovered borrowing."""

from almanak.connectors.sparklend.domain import OracleQuote, PositionObservation, Uint256
from almanak.connectors.sparklend.math import (
    debt_assets,
    repay_shares,
    supplied_assets,
    transition_indices,
    uint,
    withdraw_shares,
)
from almanak.connectors.sparklend.math import percent_div as percent_div
from almanak.connectors.sparklend.risk import RiskInputs, percent_mul, project
from almanak.models import Model

BPS = 10_000
CLOSE_FACTOR_THRESHOLD = 95 * 10**16


class LiquidationUnavailable(ValueError):
    """Observed constraints prevent settlement without erasing obligations."""


class LiquidationAmounts(Model):
    collateral_to_liquidator: Uint256
    debt_repaid: Uint256
    protocol_fee: Uint256
    collateral_scaled: Uint256
    fee_scaled: Uint256
    debt_scaled: Uint256
    collateral_exhausted: bool


def quote(position: PositionObservation) -> OracleQuote:
    result = position.reserve.oracle
    if result is None:
        raise ValueError("liquidation requires the configured oracle observation")
    return result


def maximum_debt(debt, health_factor, debt_index):
    borrowed = debt_assets(debt.debt_scaled, debt_index)
    factor = 5000 if health_factor > CLOSE_FACTOR_THRESHOLD else BPS
    return percent_mul(borrowed, factor)


def collateral_amounts(collateral, debt, debt_to_cover, supply_index):
    balance = supplied_assets(collateral.supplied_scaled, supply_index)
    config = collateral.reserve.configuration
    price_c, price_d = quote(collateral).price, quote(debt).price
    unit_c, unit_d = 10**collateral.reserve.asset.decimals, 10**debt.reserve.asset.decimals
    base = uint(price_d * debt_to_cover * unit_c) // uint(price_c * unit_d)
    amount = percent_mul(base, config.liquidation_bonus_bps)
    paid = debt_to_cover
    if amount > balance:
        amount = balance
        value = uint(price_c * amount * unit_d) // uint(price_d * unit_c)
        paid = percent_div(value, config.liquidation_bonus_bps)
    fee = 0
    if config.liquidation_protocol_fee_bps:
        bonus = amount - percent_div(amount, config.liquidation_bonus_bps)
        fee = percent_mul(bonus, config.liquidation_protocol_fee_bps)
    return amount - fee, paid, fee


def scaled_consumption(collateral, amount, fee, index):
    burned = withdraw_shares(amount, index)
    if not burned or burned > collateral.supplied_scaled:
        raise LiquidationUnavailable("SparkLend liquidation requires valid owned collateral shares")
    fee_scaled = min(withdraw_shares(fee, index), collateral.supplied_scaled - burned)
    if fee_scaled < withdraw_shares(fee, index):
        fee = supplied_assets(fee_scaled, index)
    return burned, fee_scaled, fee


def eligible(collateral, debt, inputs, timestamp, sentinel_allowed):
    if collateral not in inputs.positions or debt not in inputs.positions:
        raise ValueError("liquidation requires the complete owned fund/Pool risk scope")
    if not collateral.collateral_enabled or not debt.debt_scaled:
        raise ValueError("liquidation requires enabled collateral and owned variable debt")
    if any(
        not p.reserve.configuration.active or p.reserve.configuration.paused
        for p in (collateral, debt)
    ):
        raise ValueError("liquidation reserve is inactive or paused")
    risk = project(inputs, timestamp=timestamp).risk
    if risk.health_factor_wad >= 10**18:
        raise ValueError("liquidation requires health below one")
    if not sentinel_allowed and risk.health_factor_wad >= CLOSE_FACTOR_THRESHOLD:
        raise ValueError("liquidation is refused by the observed oracle sentinel")
    config = collateral.reserve.configuration
    if not config.liquidation_threshold_bps or config.liquidation_bonus_bps < BPS:
        raise ValueError("liquidation requires the observed collateral threshold and bonus")
    return risk


def calculate(
    collateral: PositionObservation,
    debt: PositionObservation,
    inputs: RiskInputs,
    *,
    timestamp: int,
    sentinel_allowed: bool,
    debt_to_cover: int,
) -> LiquidationAmounts:
    risk = eligible(collateral, debt, inputs, timestamp, sentinel_allowed)
    supply_index, _ = transition_indices(collateral.reserve, timestamp)
    _, debt_index = transition_indices(debt.reserve, timestamp)
    return settle(collateral, debt, risk, (supply_index, debt_index), debt_to_cover)


def settle(collateral, debt, risk, indices, debt_to_cover) -> LiquidationAmounts:
    """Use selected indices only after the caller validates full Pool risk and sentinel state."""
    supply_index, debt_index = indices
    requested = min(uint(debt_to_cover), maximum_debt(debt, risk.health_factor_wad, debt_index))
    amount, paid, fee = collateral_amounts(collateral, debt, requested, supply_index)
    if not amount or not paid:
        raise LiquidationUnavailable("liquidation cannot settle zero collateral or debt")
    if amount > collateral.reserve.available_underlying:
        raise LiquidationUnavailable("liquidation requires the observed underlying liquidity")
    consumed_debt = repay_shares(paid, debt_index)
    if not consumed_debt or consumed_debt > debt.debt_scaled:
        raise LiquidationUnavailable("liquidation requires valid owned variable-debt shares")
    exhausted = amount + fee == supplied_assets(collateral.supplied_scaled, supply_index)
    burned, fee_scaled, fee = scaled_consumption(collateral, amount, fee, supply_index)
    return LiquidationAmounts(
        collateral_to_liquidator=amount,
        debt_repaid=paid,
        protocol_fee=fee,
        collateral_scaled=burned,
        fee_scaled=fee_scaled,
        debt_scaled=consumed_debt,
        collateral_exhausted=exhausted,
    )
