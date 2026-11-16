"""Spark ordinary-mode rounding and retained Pool risk comparisons."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.sparklend.domain import PositionObservation, ReserveConfiguration
from almanak.connectors.sparklend.math import RAY, UINT256_MAX
from almanak.connectors.sparklend.reader import read_position
from almanak.connectors.sparklend.risk import RiskInputs, account_totals, percent_mul, project
from tests.test_aave_v3_reader import replay


def position(index, *, supply=0, debt=0, collateral=False, ltv=8000, threshold=8500):
    path = Path(__file__).parent / "fixtures/sparklend-ethereum-revision4-reads.json.gz"
    raw = json.loads(gzip.decompress(path.read_bytes()))
    observed = PositionObservation.model_validate_json(json.dumps(raw["positions"][index]))
    reserve = observed.reserve
    config = (reserve.configuration.raw & ~((1 << 32) - 1)) | ltv | (threshold << 16)
    oracle = reserve.oracle
    assert oracle is not None
    reserve = reserve.model_copy(
        update={
            "configuration": ReserveConfiguration(raw=config),
            "liquidity_index_ray": RAY,
            "variable_borrow_index_ray": RAY,
            "liquidity_rate_ray": 0,
            "variable_borrow_rate_ray": 0,
            "oracle": oracle.model_copy(update={"price": (3000 if index == 0 else 1) * 10**8}),
        }
    )
    return observed.model_copy(
        update={
            "reserve": reserve,
            "supplied_scaled": supply,
            "supplied_assets": supply,
            "debt_scaled": debt,
            "debt_assets": debt,
            "collateral_enabled": collateral,
        }
    )


def inputs(positions):
    bitmap = sum(
        (int(bool(p.debt_scaled)) + 2 * int(p.collateral_enabled)) << (2 * p.reserve.reserve_id)
        for p in positions
    )
    positions = tuple(p.model_copy(update={"user_configuration": bitmap}) for p in positions)
    return RiskInputs(positions=positions, user_configuration=bitmap, emode_category=0)


def test_average_threshold_is_truncated_before_health_calculation():
    result = project(
        inputs(
            (
                position(0, supply=2 * 10**18, collateral=True),
                position(
                    1,
                    supply=100 * 10**6,
                    debt=1000 * 10**6,
                    collateral=True,
                    ltv=7500,
                    threshold=8000,
                ),
            )
        )
    )
    assert result.risk.total_collateral_base == 6100 * 10**8
    assert result.risk.total_debt_base == 1000 * 10**8
    assert result.risk.liquidation_threshold_bps == 8491
    assert result.risk.health_factor_wad == 5_179_510_000_000_000_000
    assert result.risk.available_borrows_base == 387_451_000_000
    assert len(result.prices) == 2


def test_debt_conversion_truncates_and_zero_ltv_remains_visible():
    collateral = position(0, supply=10**18, collateral=True, ltv=0)
    debt = position(1, debt=1)
    debt = debt.model_copy(
        update={
            "reserve": debt.reserve.model_copy(
                update={"oracle": debt.reserve.oracle.model_copy(update={"price": 100_000_001})}
            )
        }
    )
    result = project(inputs((collateral, debt)))
    assert result.risk.total_debt_base == 100
    assert result.risk.available_borrows_base == 0
    assert result.has_zero_ltv_collateral


def test_zero_threshold_collateral_is_excluded_and_empty_debt_has_infinite_health():
    result = project(inputs((position(0, supply=10**18, collateral=True, ltv=0, threshold=0),)))
    assert result.risk.total_collateral_base == 0
    assert result.risk.health_factor_wad == UINT256_MAX
    assert not result.has_zero_ltv_collateral
    assert account_totals(1, 0, 5000, 5000).available_borrows_base == 1
    assert percent_mul(1, 5000) == 1
    with pytest.raises(ValueError):
        percent_mul(UINT256_MAX, 10_000)


def test_empty_exposure_does_not_require_a_price():
    observed = position(0)
    observed = observed.model_copy(
        update={
            "reserve": observed.reserve.model_copy(
                update={"oracle": None, "oracle_error": "unavailable"}
            )
        }
    )
    result = project(inputs((observed,)))
    assert result.prices == ()
    assert result.risk.total_collateral_base == result.risk.total_debt_base == 0


@pytest.mark.parametrize("fault", ["stable", "emode", "past", "missing", "source"])
def test_unsupported_or_missing_risk_inputs_are_refused(fault):
    collateral = position(0, supply=10**18, collateral=True)
    debt = position(1, debt=10**6)
    if fault == "stable":
        debt = debt.model_copy(update={"stable_debt_assets": 1})
        with pytest.raises(ValueError, match="stable-rate"):
            inputs((collateral, debt))
        return
    if fault in ("missing", "source"):
        changes = (
            {"oracle": None, "oracle_error": "unavailable"}
            if fault == "missing"
            else {"oracle": debt.reserve.oracle.model_copy(update={"base_currency_unit": 1})}
        )
        debt = debt.model_copy(update={"reserve": debt.reserve.model_copy(update=changes)})
    request = inputs((collateral, debt))
    if fault == "emode":
        request = request.model_copy(update={"emode_category": 1})
    with pytest.raises(ValueError):
        project(request, timestamp=0 if fault == "past" else None)


@pytest.mark.parametrize("index", [0, 1])
def test_risk_matches_pinned_pool_for_active_collateral_and_debt_accounts(index):
    path = Path(__file__).parent / "fixtures/sparklend-revision4-risk-reads.json.gz"
    raw = json.loads(gzip.decompress(path.read_bytes()))
    expected = RiskInputs.model_validate_json(json.dumps(raw["comparisons"][index]["request"]))
    first = expected.positions[0]
    with replay(raw) as reader:
        block = reader.pin_block(first.reserve.block.number)
        positions = tuple(
            read_position(reader, p.reserve.source, p.fund, block) for p in expected.positions
        )
    request = RiskInputs(
        positions=positions,
        user_configuration=expected.user_configuration,
        emode_category=expected.emode_category,
    )
    assert request == expected
    actual = project(request)
    assert actual.risk.total_collateral_base > 0
    assert actual.risk.total_debt_base > 0
    assert all(actual.risk == position.risk for position in positions)
    assert actual.model_dump(mode="json") == raw["comparisons"][index]["projection"]
