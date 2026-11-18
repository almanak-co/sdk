"""Verified internal calculations and explicit Spark liquidation obligations."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.sparklend import liquidation
from almanak.connectors.sparklend.domain import ReserveConfiguration
from almanak.connectors.sparklend.math import RAY, UINT256_MAX
from tests.test_sparklend_risk import inputs, position

VECTORS = json.loads(
    (Path(__file__).parent / "fixtures/sparklend-revision4-liquidation-vectors.json").read_text()
)


@pytest.mark.parametrize("row", VECTORS["vectors"])
def test_collateral_and_fee_calculation_matches_exact_verified_library(row):
    price_c, price_d, balance, paid, bonus, fee, decimals_c, decimals_d = row["args"]
    collateral = SimpleNamespace(
        supplied_scaled=balance,
        reserve=SimpleNamespace(
            configuration=SimpleNamespace(
                liquidation_bonus_bps=bonus, liquidation_protocol_fee_bps=fee
            ),
            oracle=SimpleNamespace(price=price_c),
            asset=SimpleNamespace(decimals=decimals_c),
        ),
    )
    debt = SimpleNamespace(
        reserve=SimpleNamespace(
            oracle=SimpleNamespace(price=price_d), asset=SimpleNamespace(decimals=decimals_d)
        )
    )
    assert liquidation.collateral_amounts(collateral, debt, paid, RAY) == tuple(row["result"])


@pytest.mark.parametrize("row", VECTORS["close_factor"])
def test_close_factor_matches_exact_verified_library_at_health_boundary(row):
    amount, health, requested = row["args"]
    maximum = liquidation.maximum_debt(SimpleNamespace(debt_scaled=amount), health, RAY)
    assert tuple(row["result"]) == (amount, amount, min(requested, maximum))


def scope(*, borrowing=4000 * 10**6):
    collateral = position(0, supply=10**18, collateral=True)
    raw = collateral.reserve.configuration.raw
    raw &= ~((65535 << 32) | (65535 << 152))
    raw |= (10500 << 32) | (1000 << 152)
    collateral = collateral.model_copy(
        update={
            "reserve": collateral.reserve.model_copy(
                update={"configuration": ReserveConfiguration(raw=raw)}
            )
        }
    )
    return inputs((collateral, position(1, debt=borrowing)))


def calculate(request, *, sentinel=True, amount=UINT256_MAX):
    collateral, debt = request.positions
    return liquidation.calculate(
        collateral,
        debt,
        request,
        timestamp=collateral.reserve.block.timestamp,
        sentinel_allowed=sentinel,
        debt_to_cover=amount,
    )


def test_exhausted_collateral_preserves_unpaid_variable_debt():
    request = scope()
    result = calculate(request)
    assert result.collateral_exhausted
    assert result.collateral_scaled + result.fee_scaled == 10**18
    assert result.debt_repaid == result.debt_scaled == 2_857_142_857
    assert request.positions[1].debt_scaled - result.debt_scaled == 1_142_857_143
    assert result.protocol_fee == 4_761_904_761_904_762
    assert "deficit" not in type(result).model_fields


def test_half_close_has_no_aave_revision11_minimum_dust_rule():
    request = scope(borrowing=2656_250_000)
    result = calculate(request)
    assert result.debt_repaid == 1_328_125_000
    assert not result.collateral_exhausted
    with pytest.raises(ValueError, match="sentinel"):
        calculate(request, sentinel=False)
    assert calculate(scope(), sentinel=False).collateral_exhausted


@pytest.mark.parametrize("fault", ["healthy", "paused", "inactive", "liquidity", "zero"])
def test_unavailable_liquidation_keeps_obligations_visible(fault):
    request = scope(borrowing=1000 * 10**6 if fault == "healthy" else 4000 * 10**6)
    if fault in ("paused", "inactive", "liquidity"):
        collateral, debt = request.positions
        raw = collateral.reserve.configuration.raw
        if fault == "paused":
            raw |= 1 << 60
        if fault == "inactive":
            raw &= ~(1 << 56)
        updates = {"configuration": ReserveConfiguration(raw=raw)}
        if fault == "liquidity":
            updates["available_underlying"] = 0
        collateral = collateral.model_copy(
            update={"reserve": collateral.reserve.model_copy(update=updates)}
        )
        request = inputs((collateral, debt))
    with pytest.raises(ValueError):
        calculate(request, amount=0 if fault == "zero" else UINT256_MAX)


def test_invalid_percentage_and_missing_price_are_refused():
    with pytest.raises(ValueError, match="zero"):
        liquidation.percent_div(1, 0)
    with pytest.raises(ValueError):
        liquidation.percent_div(UINT256_MAX, 1)
    collateral = scope().positions[0]
    missing = collateral.model_copy(
        update={"reserve": collateral.reserve.model_copy(update={"oracle": None})}
    )
    with pytest.raises(ValueError, match="oracle"):
        liquidation.quote(missing)
