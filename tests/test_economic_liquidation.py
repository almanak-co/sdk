"""Exact numerical examples for V4 liquidation, separate from contract proofs."""

import pytest

from almanak.backtest.economic_data import RAY, WAD
from almanak.backtest.economic_liquidation import LiquidationPair, liquidation_amounts
from almanak.backtest.economic_math import risk_view
from tests.test_economic_math import debt_reserve, market, position
from tests.test_economic_math import reserve as reserve


def pair(reserve, collateral=100, debt=8500, premium=0):
    debt_source = debt_reserve(reserve)
    cm, dm = market(reserve), market(debt_source, price=10**8)
    c = position(reserve, shares=collateral * 10**18, collateral=True)
    d = position(debt_source, drawn_shares=debt * 10**6, premium_offset_ray=-premium * 10**6 * RAY)
    return LiquidationPair(
        collateral=c,
        debt=d,
        collateral_market=cm,
        debt_market=dm,
        risk=risk_view((c, d), (cm, dm)),
    )


def test_target_health_liquidation_has_no_v3_close_factor(reserve):
    request = pair(reserve)
    result = liquidation_amounts(request)
    # ($8,925 target debt collateralization - $8,000 weighted collateral) / (1.05 - .84).
    # On-chain health-factor flooring makes the last raw unit round upward.
    assert result.drawn_shares == 4_404_761_905
    assert result.premium_ray == 0
    assert result.bonus_bps == 10500
    assert result.collateral_shares == 46_250_000_002_500_000_000
    assert result.fee_shares == 220_238_095_250_000_000
    assert result.drawn_shares > request.debt.drawn_shares // 2


def test_small_position_is_fully_repaid_to_avoid_debt_dust(reserve):
    result = liquidation_amounts(pair(reserve, collateral=10, debt=850))
    assert result.drawn_shares == 850 * 10**6
    assert result.collateral_shares == 8_925 * 10**15


def test_insufficient_collateral_limits_restoration_and_leaves_deficit(reserve):
    result = liquidation_amounts(pair(reserve, collateral=10, debt=1100))
    assert result.collateral_shares == 10 * 10**18
    assert result.drawn_shares == 952_380_953
    assert result.drawn_shares < 1100 * 10**6


def test_premium_is_restored_before_drawn_shares(reserve):
    result = liquidation_amounts(pair(reserve, collateral=10, debt=850, premium=100))
    assert result.premium_ray == 100 * 10**6 * RAY
    assert result.drawn_shares == 850 * 10**6


@pytest.mark.parametrize("health,bonus", [(WAD - 1, 10250), (975 * WAD // 1000, 10375), (0, 10500)])
def test_bonus_interpolates_in_basis_points(reserve, health, bonus):
    request = pair(reserve)
    request = request.model_copy(
        update={"risk": request.risk.model_copy(update={"health_factor_wad": health})}
    )
    assert request.bonus() == bonus


@pytest.mark.parametrize("fault", ["healthy", "disabled", "empty", "no_factor", "scope"])
def test_ineligible_positions_cannot_be_liquidated(reserve, fault):
    request = pair(reserve)
    if fault == "healthy":
        request = pair(reserve, debt=7000)
    elif fault in ("disabled", "empty"):
        change = {"collateral": False} if fault == "disabled" else {"shares": 0}
        request = request.model_copy(
            update={"collateral": request.collateral.model_copy(update=change)}
        )
    elif fault == "scope":
        request = request.model_copy(
            update={"risk": request.risk.model_copy(update={"spoke": "0x" + "ff" * 20})}
        )
    else:
        cm = request.collateral_market
        config = cm.risk.configs[0].model_copy(update={"collateral_factor_bps": 0})
        cm = cm.model_copy(update={"risk": cm.risk.model_copy(update={"configs": (config,)})})
        request = request.model_copy(update={"collateral_market": cm})
    with pytest.raises(ValueError):
        liquidation_amounts(request)


def test_liquidation_closes_debt_or_records_explicit_deficit(reserve):
    from almanak.backtest.economic_liquidation import settle_liquidations

    for debt, deficit in ((850, 0), (1100, 147_619_047)):
        request = pair(reserve, collateral=10, debt=debt)
        positions, movements = settle_liquidations(
            (request.collateral, request.debt), (request.collateral_market, request.debt_market)
        )
        assert len(movements) == 1
        assert positions[1].drawn_shares == positions[1].premium_shares == 0
        assert positions[1].deficit == deficit
        assert positions[0].collateral  # V4 retains the flag even when all shares are seized.
        assert positions[1].liquidated_debt + deficit == debt * 10**6
        assert positions[0].shares + movements[0].collateral_removed.amount == 10 * 10**18


def test_risk_recalculation_stops_at_healthy_account(reserve):
    from almanak.backtest.economic_liquidation import settle_liquidations

    request = pair(reserve)
    positions, movements = settle_liquidations(
        (request.collateral, request.debt), (request.collateral_market, request.debt_market)
    )
    assert len(movements) == 1
    assert (
        risk_view(positions, (request.collateral_market, request.debt_market)).health_factor_wad
        >= WAD
    )


def test_configuration_change_cannot_forgive_debt_without_liquidation(reserve):
    from almanak.backtest.economic_events import first_breach
    from almanak.backtest.economic_liquidation import settle_liquidations

    request = pair(reserve)
    cm = request.collateral_market
    configs = tuple(c.model_copy(update={"collateral_factor_bps": 0}) for c in cm.risk.configs)
    cm = cm.model_copy(update={"risk": cm.risk.model_copy(update={"configs": configs})})
    positions = (request.collateral, request.debt)
    markets = (cm, request.debt_market)
    result, effects = settle_liquidations(positions, markets)
    assert result == positions
    assert not effects
    assert first_breach(positions, lambda _: markets, 0, 1000) is None
