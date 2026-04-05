"""Lending liquidation simulation for PnL backtesting.

Provides standalone functions for updating health factors and simulating
lending liquidation events during backtests.

These functions operate on SimulatedPortfolio instances, modifying
positions in-place when liquidation conditions are met.

Extracted from pnl/portfolio.py for module size management.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.framework.backtesting.models import LendingLiquidationEvent
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.position_models import PositionType, SimulatedPosition

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

logger = logging.getLogger(__name__)


def update_health_factors(portfolio: SimulatedPortfolio, market_state: MarketState) -> None:
    """Update health factors for all borrow positions based on current prices.

    This function:
    1. Calculates total collateral value (SUPPLY positions)
    2. Calculates total debt value (BORROW positions)
    3. Updates health factor for each BORROW position
    4. Emits warnings when health factor drops below threshold
    5. Tracks minimum health factor observed

    Health factor formula:
        HF = (collateral_value * liquidation_threshold) / debt_value

    Note: In a real lending protocol, collateral and debt are per-position.
    For simplicity, this implementation treats all SUPPLY positions as
    collateral for all BORROW positions. For accurate per-position
    health factors, positions should track their collateral reference.

    Args:
        portfolio: The portfolio containing positions to update
        market_state: Current market state with prices
    """
    # Lazy import to avoid circular dependency
    from almanak.framework.backtesting.pnl.calculators.health_factor import (
        HealthFactorCalculator,
    )

    # Calculate total collateral value (SUPPLY positions)
    total_collateral_usd = Decimal("0")
    for pos in portfolio.positions:
        if pos.position_type == PositionType.SUPPLY:
            token = pos.primary_token
            try:
                price = market_state.get_price(token)
            except KeyError:
                price = pos.entry_price
            total_collateral_usd += pos.total_amount * price + pos.interest_accrued

    # If no collateral, cannot update health factors meaningfully
    if total_collateral_usd <= Decimal("0"):
        return

    hf_calculator = HealthFactorCalculator(
        warning_threshold=portfolio.health_factor_warning_threshold,
    )

    # Update health factor for each BORROW position
    for pos in portfolio.positions:
        if pos.position_type == PositionType.BORROW:
            token = pos.primary_token
            try:
                price = market_state.get_price(token)
            except KeyError:
                price = pos.entry_price

            # Calculate debt value for this position
            debt_value_usd = pos.total_amount * price + pos.interest_accrued

            # Get liquidation threshold for this protocol
            liquidation_threshold = hf_calculator.get_liquidation_threshold_for_protocol(pos.protocol)

            # Calculate health factor
            result = hf_calculator.calculate_health_factor(
                collateral_value_usd=total_collateral_usd,
                debt_value_usd=debt_value_usd,
                liquidation_threshold=liquidation_threshold,
            )

            # Update position's health factor
            pos.health_factor = result.health_factor

            # Track minimum health factor observed
            if result.health_factor < portfolio._min_health_factor:
                portfolio._min_health_factor = result.health_factor

            # Check for warnings
            warning = hf_calculator.check_health_factor_warning(
                health_factor=result.health_factor,
                position_id=pos.position_id,
                emit_warning=True,
            )
            if warning:
                portfolio._health_factor_warnings += 1

            # Check for liquidation (health factor < 1.0)
            if result.health_factor < Decimal("1.0"):
                simulate_lending_liquidation(
                    portfolio=portfolio,
                    borrow_position=pos,
                    health_factor=result.health_factor,
                    total_collateral_usd=total_collateral_usd,
                    debt_value_usd=debt_value_usd,
                    market_state=market_state,
                )


def simulate_lending_liquidation(
    portfolio: SimulatedPortfolio,
    borrow_position: SimulatedPosition,
    health_factor: Decimal,
    total_collateral_usd: Decimal,
    debt_value_usd: Decimal,
    market_state: MarketState,
) -> None:
    """Simulate a lending liquidation event when health factor falls below 1.0.

    This function simulates the liquidation process in lending protocols:
    1. A portion of the debt is repaid (typically 50% of the debt or what can be covered)
    2. Corresponding collateral is seized with a liquidation penalty
    3. Position state is updated to reflect the partial liquidation
    4. A LendingLiquidationEvent is recorded

    In real lending protocols:
    - Liquidators repay up to 50% (or close factor) of the borrower's debt
    - Liquidators receive collateral worth (debt_repaid * (1 + penalty))
    - The penalty (e.g., 5%) incentivizes liquidators

    Args:
        portfolio: The portfolio containing positions
        borrow_position: The BORROW position being liquidated
        health_factor: Current health factor (< 1.0)
        total_collateral_usd: Total collateral value in USD
        debt_value_usd: Total debt value for this position in USD
        market_state: Current market state for pricing
    """
    # Calculate how much debt to repay (50% close factor like Aave)
    close_factor = Decimal("0.5")
    debt_to_repay = debt_value_usd * close_factor

    # Calculate collateral to seize (debt + penalty)
    # collateral_seized = debt_repaid * (1 + penalty)
    collateral_seized = debt_to_repay * (Decimal("1") + portfolio.liquidation_penalty)

    # Cap collateral seized at available collateral
    if collateral_seized > total_collateral_usd:
        collateral_seized = total_collateral_usd
        # Recalculate debt repaid based on capped collateral
        debt_to_repay = collateral_seized / (Decimal("1") + portfolio.liquidation_penalty)

    # Update borrow position: reduce the borrowed amount
    token = borrow_position.primary_token
    try:
        price = market_state.get_price(token)
    except KeyError:
        price = borrow_position.entry_price

    # Calculate how much of the token debt is repaid
    token_debt_repaid = debt_to_repay / price if price > 0 else Decimal("0")

    # Reduce the position's borrowed amount
    original_amount = borrow_position.total_amount
    new_amount = max(Decimal("0"), original_amount - token_debt_repaid)
    borrow_position.amounts[token] = new_amount

    # Also reduce accrued interest proportionally
    if original_amount > 0:
        reduction_ratio = token_debt_repaid / original_amount
        interest_reduction = borrow_position.interest_accrued * reduction_ratio
        borrow_position.interest_accrued = max(Decimal("0"), borrow_position.interest_accrued - interest_reduction)

    # Update collateral (SUPPLY positions) - reduce proportionally
    supply_positions = [p for p in portfolio.positions if p.position_type == PositionType.SUPPLY]
    remaining_collateral_to_seize = collateral_seized

    for supply_pos in supply_positions:
        if remaining_collateral_to_seize <= Decimal("0"):
            break

        supply_token = supply_pos.primary_token
        try:
            supply_price = market_state.get_price(supply_token)
        except KeyError:
            supply_price = supply_pos.entry_price

        supply_value_usd = supply_pos.total_amount * supply_price
        supply_value_usd += supply_pos.interest_accrued

        if supply_value_usd <= remaining_collateral_to_seize:
            # Seize entire position
            remaining_collateral_to_seize -= supply_value_usd
            supply_pos.amounts[supply_token] = Decimal("0")
            supply_pos.interest_accrued = Decimal("0")
        else:
            # Seize partial position
            seize_ratio = remaining_collateral_to_seize / supply_value_usd
            token_amount_seized = supply_pos.total_amount * seize_ratio
            supply_pos.amounts[supply_token] -= token_amount_seized
            supply_pos.interest_accrued *= Decimal("1") - seize_ratio
            remaining_collateral_to_seize = Decimal("0")

    # Get timestamp from borrow position's last_updated or use a default
    timestamp = borrow_position.last_updated or borrow_position.entry_time

    # Record the liquidation event
    event = LendingLiquidationEvent(
        timestamp=timestamp,
        position_id=borrow_position.position_id,
        health_factor=health_factor,
        collateral_seized=collateral_seized,
        debt_repaid=debt_to_repay,
        penalty=portfolio.liquidation_penalty,
    )
    portfolio._lending_liquidations.append(event)

    # Log the liquidation
    logger.warning(
        f"Lending liquidation triggered for position {borrow_position.position_id}: "
        f"HF={health_factor:.4f}, debt_repaid=${debt_to_repay:.2f}, "
        f"collateral_seized=${collateral_seized:.2f}, penalty={portfolio.liquidation_penalty * 100:.1f}%"
    )
