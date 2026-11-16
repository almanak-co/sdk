"""Revision-4 ordinary-mode Pool risk with its original half-up accounting."""

from typing import Annotated

from pydantic import Field, model_validator

from almanak.connectors.pool_lending_risk import validate_positions
from almanak.connectors.sparklend.domain import (
    AccountRisk,
    OracleQuote,
    PositionObservation,
    Uint256,
)
from almanak.connectors.sparklend.math import (
    UINT256_MAX,
    debt_assets,
    normalized_indices,
    supplied_assets,
    uint,
)
from almanak.connectors.sparklend.math import percent_mul as percent_mul
from almanak.models import Model

BPS = 10_000
WAD = 10**18


class RiskInputs(Model):
    positions: Annotated[tuple[PositionObservation, ...], Field(min_length=1)]
    user_configuration: Uint256
    emode_category: Annotated[int, Field(ge=0, lt=256)]

    @model_validator(mode="after")
    def scope(self):
        if any(position.stable_debt_assets for position in self.positions):
            raise ValueError("SparkLend risk projection does not support stable-rate debt")
        validate_positions(self.positions, self.user_configuration)
        return self


class RiskProjection(Model):
    risk: AccountRisk
    has_zero_ltv_collateral: bool
    prices: tuple[OracleQuote, ...]


def price_evidence(inputs: RiskInputs) -> tuple[OracleQuote, ...]:
    prices = []
    source = None
    for position in inputs.positions:
        if not (position.collateral_enabled or position.debt_scaled):
            continue
        price = position.reserve.oracle
        if price is None:
            raise ValueError("pool risk requires every exposed reserve's oracle observation")
        identity = price.oracle, price.base_currency, price.base_currency_unit
        if source is not None and source != identity:
            raise ValueError("pool risk cannot combine different oracle base currencies or units")
        source = identity
        prices.append(price)
    return tuple(prices)


def reserve_values(position, timestamp):
    price = position.reserve.oracle
    if price is None:
        raise ValueError("exposed reserve has no oracle evidence")
    supply_index, borrow_index = normalized_indices(position.reserve, timestamp)
    unit = 10**position.reserve.asset.decimals
    collateral = 0
    if position.collateral_enabled and position.reserve.configuration.liquidation_threshold_bps:
        collateral = uint(supplied_assets(position.supplied_scaled, supply_index) * price.price)
    debt = uint(debt_assets(position.debt_scaled, borrow_index) * price.price)
    return collateral // unit, debt // unit


def totals(inputs, timestamp):
    collateral, debt, weighted_ltv, weighted_threshold = 0, 0, 0, 0
    zero_ltv = False
    for position in inputs.positions:
        if not (position.collateral_enabled or position.debt_scaled):
            continue
        value, borrowed = reserve_values(position, timestamp)
        config = position.reserve.configuration
        collateral = uint(collateral + value)
        debt = uint(debt + borrowed)
        weighted_ltv = uint(weighted_ltv + value * config.ltv_bps)
        weighted_threshold = uint(weighted_threshold + value * config.liquidation_threshold_bps)
        zero_ltv |= (
            position.collateral_enabled
            and bool(config.liquidation_threshold_bps)
            and config.ltv_bps == 0
        )
    return collateral, debt, weighted_ltv, weighted_threshold, zero_ltv


def account_totals(collateral: int, debt: int, weighted_ltv: int, threshold: int) -> AccountRisk:
    ltv = weighted_ltv // collateral if collateral else 0
    average_threshold = threshold // collateral if collateral else 0
    # This Pool truncates the average threshold before percentMul and wadDiv.
    health = (
        uint(percent_mul(collateral, average_threshold) * WAD + debt // 2) // debt
        if debt
        else UINT256_MAX
    )
    return AccountRisk(
        total_collateral_base=collateral,
        total_debt_base=debt,
        available_borrows_base=max(percent_mul(collateral, ltv) - debt, 0),
        liquidation_threshold_bps=average_threshold,
        ltv_bps=ltv,
        health_factor_wad=health,
        emode_category=0,
    )


def project(inputs: RiskInputs, *, timestamp: int | None = None) -> RiskProjection:
    if inputs.emode_category != 0:
        raise ValueError("SparkLend risk currently requires ordinary collateral mode")
    observed = inputs.positions[0].reserve.block.timestamp
    effective = observed if timestamp is None else timestamp
    if effective < observed:
        raise ValueError("risk projection cannot precede its historical inputs")
    prices = price_evidence(inputs)
    collateral, debt, weighted_ltv, threshold, zero_ltv = totals(inputs, effective)
    return RiskProjection(
        risk=account_totals(collateral, debt, weighted_ltv, threshold),
        has_zero_ltv_collateral=zero_ltv,
        prices=prices,
    )
