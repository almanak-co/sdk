"""Pool-wide risk boundaries; deployed-transition comparisons are separate evidence."""

import gzip
import json
from pathlib import Path

import pytest

from almanak.connectors.aave_v3.domain import PositionObservation, ReserveConfiguration
from almanak.connectors.aave_v3.math import RAY, UINT256_MAX
from almanak.connectors.aave_v3.reader import read_position
from almanak.connectors.aave_v3.risk import RiskInputs, project
from tests.test_aave_v3_reader import replay


def position(index, *, supply=0, debt=0, collateral=False, ltv=8000, threshold=8500):
    path = Path(__file__).parent / "fixtures/aave-v3-ethereum-revision11-reads.json.gz"
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


def test_pool_combines_distinct_reserves_before_health_factor_rounding():
    observations = (
        position(0, supply=2 * 10**18, collateral=True),
        position(
            1, supply=100 * 10**6, debt=1000 * 10**6, collateral=True, ltv=7500, threshold=8000
        ),
    )
    result = project(inputs(observations))
    risk = result.risk
    assert risk.total_collateral_base == 6100 * 10**8
    assert risk.total_debt_base == 1000 * 10**8
    assert risk.ltv_bps == 7991
    assert risk.liquidation_threshold_bps == 8491
    assert risk.available_borrows_base == 387_451_000_000
    assert risk.health_factor_wad == 5_180_000_000_000_000_000
    assert len(result.prices) == 2
    assert not result.has_zero_ltv_collateral


def test_empty_exposure_needs_no_invented_price_and_has_protocol_infinite_health():
    observed = position(0)
    observed = observed.model_copy(
        update={
            "reserve": observed.reserve.model_copy(
                update={"oracle": None, "oracle_error": "missing"}
            ),
        }
    )
    result = project(inputs((observed,)))
    assert result.risk.total_collateral_base == result.risk.total_debt_base == 0
    assert result.risk.health_factor_wad == UINT256_MAX
    assert result.prices == ()


def test_debt_price_conversion_rounds_up_and_zero_ltv_is_retained():
    collateral = position(0, supply=10**18, collateral=True, ltv=0)
    debt = position(1, debt=1)
    debt = debt.model_copy(
        update={
            "reserve": debt.reserve.model_copy(
                update={
                    "oracle": debt.reserve.oracle.model_copy(update={"price": 100_000_001}),
                }
            )
        }
    )
    result = project(inputs((collateral, debt)))
    assert result.risk.total_debt_base == 101
    assert result.risk.available_borrows_base == 0
    assert result.has_zero_ltv_collateral


@pytest.mark.parametrize("fault", ["duplicate", "foreign", "bitmap", "fund", "pool", "block"])
def test_incomplete_or_mixed_scope_pool_input_is_refused(fault):
    observed = position(0, supply=10**18, collateral=True)
    valid = inputs((observed,))
    values = {
        "positions": valid.positions,
        "user_configuration": valid.user_configuration,
        "emode_category": 0,
    }
    other = position(1)
    if fault == "duplicate":
        values["positions"] = (observed, observed)
    elif fault == "foreign":
        values["user_configuration"] |= 1 << 254
    elif fault == "bitmap":
        values["user_configuration"] = 0
    else:
        if fault == "fund":
            other = other.model_copy(
                update={
                    "fund": other.fund.model_copy(
                        update={
                            "address": "0x" + "12" * 20,
                        }
                    )
                }
            )
        elif fault == "pool":
            other = other.model_copy(
                update={
                    "reserve": other.reserve.model_copy(
                        update={
                            "source": other.reserve.source.model_copy(
                                update={"pool": "0x" + "12" * 20}
                            ),
                        }
                    )
                }
            )
        else:
            other = other.model_copy(
                update={
                    "reserve": other.reserve.model_copy(
                        update={
                            "block": other.reserve.block.model_copy(update={"timestamp": 1}),
                        }
                    )
                }
            )
        values["positions"] = (observed, other)
    with pytest.raises(ValueError):
        RiskInputs(**values)


@pytest.mark.parametrize("fault", ["missing", "oracle", "unit", "emode", "past"])
def test_risk_never_substitutes_sources_modes_or_future_observations(fault):
    collateral = position(0, supply=10**18, collateral=True)
    debt = position(1, debt=10**6)
    if fault in ("missing", "oracle", "unit"):
        oracle = debt.reserve.oracle
        assert oracle is not None
        if fault == "missing":
            updated = {"oracle": None, "oracle_error": "unavailable"}
        else:
            change = (
                {"oracle": "0x" + "12" * 20} if fault == "oracle" else {"base_currency_unit": 1}
            )
            updated = {"oracle": oracle.model_copy(update=change)}
        debt = debt.model_copy(update={"reserve": debt.reserve.model_copy(update=updated)})
    request = inputs((collateral, debt))
    if fault == "emode":
        request = request.model_copy(update={"emode_category": 1})
    with pytest.raises(ValueError):
        project(request, timestamp=0 if fault == "past" else None)


@pytest.mark.parametrize("index", [0, 1])
def test_risk_matches_pinned_pool_for_active_collateral_and_debt_accounts(index):
    path = Path(__file__).parent / "fixtures/aave-v3-revision11-risk-reads.json.gz"
    raw = json.loads(gzip.decompress(path.read_bytes()))
    expected = RiskInputs.model_validate_json(json.dumps(raw["comparisons"][index]["request"]))
    first = expected.positions[0]
    with replay(raw) as reader:
        block = reader.pin_block(first.reserve.block.number)
        positions = tuple(
            read_position(reader, p.reserve.source, p.fund, block) for p in expected.positions
        )
        bitmap = reader.call(
            first.reserve.source.pool,
            "getUserConfiguration(address)",
            block,
            inputs=("address",),
            args=(first.fund.address,),
            outputs=("(uint256)",),
        )[0][0]
    request = RiskInputs(
        positions=positions, user_configuration=bitmap, emode_category=expected.emode_category
    )
    assert request == expected
    actual = project(request)
    assert actual.risk.total_collateral_base > 0
    assert actual.risk.total_debt_base > 0
    assert all(actual.risk == position.risk for position in positions)
    assert actual.model_dump(mode="json") == raw["comparisons"][index]["projection"]
