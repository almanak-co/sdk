"""Lending liquidation simulation for PnL backtesting.

Provides standalone functions for updating health factors and simulating
lending liquidation events during backtests.

These functions operate on SimulatedPortfolio instances, modifying
positions in-place when liquidation conditions are met.

Extracted from pnl/portfolio.py for module size management.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING

from almanak.framework.backtesting.models import LendingLiquidationEvent
from almanak.framework.backtesting.pnl.data_provider import MarketState
from almanak.framework.backtesting.pnl.position_models import PositionType, SimulatedPosition

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.portfolio import SimulatedPortfolio

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _LiquidationPlan:
    debt_to_repay: Decimal
    collateral_seized: Decimal


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
    hf_calculator = _health_factor_calculator(portfolio)

    for pos in _borrow_positions(portfolio):
        total_collateral_usd = _total_collateral_usd(portfolio, market_state)
        if total_collateral_usd <= Decimal("0"):
            _record_health_factor(portfolio, pos, hf_calculator, Decimal("0"))
            continue

        debt_value_usd = _position_value_usd(pos, market_state)
        liquidation_threshold = hf_calculator.get_liquidation_threshold_for_protocol(pos.protocol)
        result = hf_calculator.calculate_health_factor(
            collateral_value_usd=total_collateral_usd,
            debt_value_usd=debt_value_usd,
            liquidation_threshold=liquidation_threshold,
        )
        _record_health_factor(portfolio, pos, hf_calculator, result.health_factor)

        if result.health_factor < Decimal("1.0"):
            simulate_lending_liquidation(
                portfolio=portfolio,
                borrow_position=pos,
                health_factor=result.health_factor,
                total_collateral_usd=total_collateral_usd,
                debt_value_usd=debt_value_usd,
                market_state=market_state,
            )


def _health_factor_calculator(portfolio: SimulatedPortfolio):
    # Lazy import to avoid circular dependency
    from almanak.framework.backtesting.pnl.calculators.health_factor import (
        HealthFactorCalculator,
    )

    return HealthFactorCalculator(
        warning_threshold=portfolio.health_factor_warning_threshold,
    )


def _borrow_positions(portfolio: SimulatedPortfolio) -> list[SimulatedPosition]:
    return [pos for pos in portfolio.positions if pos.position_type == PositionType.BORROW]


def _supply_positions(portfolio: SimulatedPortfolio) -> list[SimulatedPosition]:
    return [pos for pos in portfolio.positions if pos.position_type == PositionType.SUPPLY]


def _position_price(position: SimulatedPosition, market_state: MarketState) -> Decimal:
    try:
        return market_state.get_price(position.primary_token)
    except KeyError:
        return position.entry_price


def _position_value_usd(position: SimulatedPosition, market_state: MarketState) -> Decimal:
    return position.total_amount * _position_price(position, market_state) + position.interest_accrued


def _total_collateral_usd(portfolio: SimulatedPortfolio, market_state: MarketState) -> Decimal:
    return sum((_position_value_usd(pos, market_state) for pos in _supply_positions(portfolio)), Decimal("0"))


def _record_health_factor(
    portfolio: SimulatedPortfolio,
    position: SimulatedPosition,
    hf_calculator,
    health_factor: Decimal,
) -> None:
    position.health_factor = health_factor
    if health_factor < portfolio._min_health_factor:
        portfolio._min_health_factor = health_factor

    warning = hf_calculator.check_health_factor_warning(
        health_factor=health_factor,
        position_id=position.position_id,
        emit_warning=True,
    )
    if warning:
        portfolio._health_factor_warnings += 1


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
    plan = _liquidation_plan(portfolio, total_collateral_usd, debt_value_usd)
    event_timestamp = _liquidation_event_timestamp(market_state, borrow_position)
    _apply_borrow_liquidation(borrow_position, debt_value_usd, plan.debt_to_repay)
    _seize_collateral(portfolio, market_state, plan.collateral_seized)
    _record_lending_liquidation(portfolio, borrow_position, health_factor, plan, event_timestamp)
    _log_lending_liquidation(portfolio, borrow_position, health_factor, plan, event_timestamp)


def _liquidation_event_timestamp(market_state: MarketState, borrow_position: SimulatedPosition) -> datetime:
    market_timestamp = getattr(market_state, "timestamp", None)
    if market_timestamp is not None:
        return market_timestamp
    return borrow_position.last_updated or borrow_position.entry_time


def _liquidation_plan(
    portfolio: SimulatedPortfolio,
    total_collateral_usd: Decimal,
    debt_value_usd: Decimal,
) -> _LiquidationPlan:
    close_factor = Decimal("0.5")
    debt_to_repay = debt_value_usd * close_factor
    collateral_seized = debt_to_repay * (Decimal("1") + portfolio.liquidation_penalty)
    if collateral_seized > total_collateral_usd:
        collateral_seized = total_collateral_usd
        debt_to_repay = collateral_seized / (Decimal("1") + portfolio.liquidation_penalty)
    return _LiquidationPlan(debt_to_repay=debt_to_repay, collateral_seized=collateral_seized)


def _apply_borrow_liquidation(
    borrow_position: SimulatedPosition,
    debt_value_usd: Decimal,
    debt_to_repay: Decimal,
) -> None:
    token = borrow_position.primary_token
    remaining_ratio = _remaining_debt_ratio(debt_value_usd, debt_to_repay)
    borrow_position.amounts[token] = borrow_position.total_amount * remaining_ratio
    borrow_position.interest_accrued *= remaining_ratio


def _seize_collateral(
    portfolio: SimulatedPortfolio,
    market_state: MarketState,
    collateral_to_seize: Decimal,
) -> None:
    remaining_collateral_to_seize = collateral_to_seize
    for supply_pos in _supply_positions(portfolio):
        if remaining_collateral_to_seize <= Decimal("0"):
            return
        remaining_collateral_to_seize = _seize_from_supply_position(
            supply_pos,
            market_state,
            remaining_collateral_to_seize,
        )


def _seize_from_supply_position(
    supply_position: SimulatedPosition,
    market_state: MarketState,
    collateral_to_seize: Decimal,
) -> Decimal:
    supply_value_usd = _position_value_usd(supply_position, market_state)
    if supply_value_usd <= collateral_to_seize:
        _clear_supply_position(supply_position)
        return collateral_to_seize - supply_value_usd

    seize_ratio = collateral_to_seize / supply_value_usd
    supply_token = supply_position.primary_token
    supply_position.amounts[supply_token] -= supply_position.total_amount * seize_ratio
    supply_position.interest_accrued *= Decimal("1") - seize_ratio
    return Decimal("0")


def _clear_supply_position(supply_position: SimulatedPosition) -> None:
    supply_position.amounts[supply_position.primary_token] = Decimal("0")
    supply_position.interest_accrued = Decimal("0")


def _record_lending_liquidation(
    portfolio: SimulatedPortfolio,
    borrow_position: SimulatedPosition,
    health_factor: Decimal,
    plan: _LiquidationPlan,
    event_timestamp: datetime,
) -> None:
    portfolio._lending_liquidations.append(
        LendingLiquidationEvent(
            timestamp=event_timestamp,
            position_id=borrow_position.position_id,
            health_factor=health_factor,
            collateral_seized=plan.collateral_seized,
            debt_repaid=plan.debt_to_repay,
            penalty=portfolio.liquidation_penalty,
        )
    )


def _log_lending_liquidation(
    portfolio: SimulatedPortfolio,
    borrow_position: SimulatedPosition,
    health_factor: Decimal,
    plan: _LiquidationPlan,
    event_timestamp: datetime,
) -> None:
    logger.warning(
        f"Lending liquidation triggered for position {borrow_position.position_id}: "
        f"HF={health_factor:.4f}, debt_repaid=${plan.debt_to_repay:.2f}, "
        f"collateral_seized=${plan.collateral_seized:.2f}, penalty={portfolio.liquidation_penalty * 100:.1f}%, "
        f"timestamp={event_timestamp.isoformat()}"
    )


def _remaining_debt_ratio(debt_value_usd: Decimal, debt_repaid_usd: Decimal) -> Decimal:
    if debt_value_usd <= Decimal("0"):
        return Decimal("0")
    remaining_debt = max(Decimal("0"), debt_value_usd - debt_repaid_usd)
    return remaining_debt / debt_value_usd
