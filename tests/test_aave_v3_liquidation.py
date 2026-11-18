"""Independent arithmetic comparisons and explicit liquidation eligibility boundaries."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v3.domain import ReserveConfiguration
from almanak.connectors.aave_v3.liquidation import (
    calculate,
    collateral_amounts,
    maximum_debt,
    quote,
    scaled_consumption,
)
from almanak.connectors.aave_v3.math import RAY, UINT256_MAX
from almanak.connectors.aave_v3.risk import project
from tests.test_aave_v3_risk import inputs, position

VECTORS = json.loads(
    (Path(__file__).parent / "fixtures/aave-v3-revision11-liquidation-vectors.json").read_text()
)


@pytest.mark.parametrize("vector", VECTORS["vectors"])
def test_collateral_fee_and_debt_amounts_match_compiled_upstream(vector):
    raw, cp, cu, dp, du, cover, balance, bonus = vector["args"]
    collateral = SimpleNamespace(
        supplied_scaled=balance,
        reserve=SimpleNamespace(
            asset=SimpleNamespace(decimals=len(str(cu)) - 1),
            oracle=SimpleNamespace(price=cp),
            configuration=ReserveConfiguration(raw=raw | (bonus << 32)),
            liquidity_index_ray=RAY,
            variable_borrow_index_ray=RAY,
            liquidity_rate_ray=0,
            variable_borrow_rate_ray=0,
            total_scaled_debt=0,
            last_update_timestamp=100,
        ),
    )
    debt = SimpleNamespace(
        reserve=SimpleNamespace(
            asset=SimpleNamespace(decimals=len(str(du)) - 1), oracle=SimpleNamespace(price=dp)
        )
    )
    assert list(collateral_amounts(collateral, debt, cover, RAY)) == vector["result"]


def group(*, collateral=3 * 10**18, debt=8000 * 10**6):
    supplied = position(0, supply=collateral, collateral=True)
    raw = supplied.reserve.configuration.raw
    raw &= ~(((1 << 16) - 1) << 32 | ((1 << 16) - 1) << 152)
    supplied = supplied.model_copy(
        update={
            "reserve": supplied.reserve.model_copy(
                update={"configuration": ReserveConfiguration(raw=raw | 10500 << 32 | 1000 << 152)}
            )
        }
    )
    return inputs((supplied, position(1, debt=debt)))


def liquidate(request, *, debt_to_cover=UINT256_MAX, grace=(0, 0)):
    collateral, debt = request.positions
    return calculate(
        collateral,
        debt,
        request,
        timestamp=collateral.reserve.block.timestamp,
        grace=grace,
        debt_to_cover=debt_to_cover,
    )


def test_close_factor_uses_total_pool_debt_and_leaves_viable_obligations():
    request = group()
    result = liquidate(request)
    assert result.debt_repaid == 4000 * 10**6
    # Debt-to-collateral conversion floors before applying the bonus.
    assert result.collateral_to_liquidator + result.protocol_fee == 14 * 10**17 - 1
    assert result.protocol_fee > 0
    assert result.debt_scaled == 4000 * 10**6
    assert not result.no_collateral_left and result.deficit == 0
    risk = project(request).risk
    # Another debt reserve increases the Pool-wide half-debt allowance.
    risk = risk.model_copy(update={"total_debt_base": 16000 * 10**8})
    assert maximum_debt(*request.positions, risk, (RAY, RAY)) == (8000 * 10**6)


def test_depleted_collateral_records_deficit_and_burns_the_entire_selected_debt():
    request = group(collateral=10**18, debt=2900 * 10**6)
    result = liquidate(request)
    assert result.no_collateral_left
    assert result.collateral_scaled + result.fee_scaled == 10**18
    assert result.debt_scaled == 2900 * 10**6
    assert result.deficit == 2900 * 10**6 - result.debt_repaid > 0


def test_small_position_overrides_half_debt_limit_even_above_close_factor_health():
    request = group(collateral=6 * 10**17, debt=1550 * 10**6)
    assert 95 * 10**16 < project(request).risk.health_factor_wad < 10**18
    result = liquidate(request)
    assert result.debt_repaid == 1550 * 10**6
    assert result.deficit == 0 and not result.no_collateral_left


def test_partial_liquidation_cannot_leave_protocol_forbidden_dust():
    request = group(collateral=10**18, debt=2850 * 10**6)
    with pytest.raises(ValueError, match="dust"):
        liquidate(request, debt_to_cover=2000 * 10**6)


@pytest.mark.parametrize("fault", ["healthy", "paused", "inactive", "grace", "scope", "zero"])
def test_liquidation_cannot_ignore_eligibility_or_scope(fault):
    request = group(debt=1000 * 10**6 if fault == "healthy" else 8000 * 10**6)
    collateral, debt = request.positions
    if fault in ("paused", "inactive"):
        raw = collateral.reserve.configuration.raw
        raw = raw | (1 << 60) if fault == "paused" else raw & ~(1 << 56)
        collateral = collateral.model_copy(
            update={
                "reserve": collateral.reserve.model_copy(
                    update={"configuration": ReserveConfiguration(raw=raw)}
                )
            }
        )
        request = inputs((collateral, debt))
        collateral, debt = request.positions
    if fault == "scope":
        collateral = collateral.model_copy(
            update={"supplied_scaled": collateral.supplied_scaled + 1}
        )
    at = collateral.reserve.block.timestamp
    with pytest.raises(ValueError):
        calculate(
            collateral,
            debt,
            request,
            timestamp=at,
            grace=(at, 0) if fault == "grace" else (0, 0),
            debt_to_cover=0 if fault == "zero" else UINT256_MAX,
        )


def test_fee_scaled_rounding_caps_to_the_actual_remaining_share_balance():
    collateral = SimpleNamespace(
        supplied_scaled=2,
        reserve=SimpleNamespace(
            liquidity_index_ray=2 * RAY,
            variable_borrow_index_ray=RAY,
            liquidity_rate_ray=0,
            variable_borrow_rate_ray=0,
            total_scaled_debt=0,
            last_update_timestamp=100,
        ),
    )
    assert scaled_consumption(collateral, 3, 1, 2 * RAY) == (2, 0, 0)
    with pytest.raises(ValueError, match="exceeds"):
        scaled_consumption(collateral, 5, 0, 2 * RAY)
    missing = position(0)
    missing = missing.model_copy(
        update={"reserve": missing.reserve.model_copy(update={"oracle": None})}
    )
    with pytest.raises(ValueError, match="oracle"):
        quote(missing)
