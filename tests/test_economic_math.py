"""Independent fixed-point examples; these are not contract or historical model proofs."""

import json
from fractions import Fraction
from pathlib import Path

import pytest

from almanak.backtest.dataset import DataRequirement, Timeline
from almanak.backtest.economic_data import (
    RAY,
    WAD,
    YEAR,
    AccountingSource,
    DynamicConfig,
    LiquidationConfig,
    OraclePrice,
    RateCurve,
    RateRecord,
    Rates,
    RiskParameters,
    grow,
    rate_key,
    require_range,
    supply_rate,
)
from almanak.backtest.economic_math import (
    MarketView,
    debts,
    premium_ray,
    repay,
    require_healthy,
    reset_premium,
    risk_view,
    weighted_risk,
)
from almanak.backtest.economic_state import EconomicPosition
from almanak.backtest.types import UnixTime
from almanak.caip import AssetType
from almanak.connectors.aave_v4.domain import AaveReserve
from almanak.connectors.aave_v4.history import Market
from almanak.lending import SupplyTarget
from almanak.models import Account, Asset


@pytest.fixture
def reserve():
    raw = json.loads(
        (Path(__file__).parent / "fixtures/aave-v4-historical-market.json").read_text()
    )
    return Market.model_validate_json(json.dumps(raw["markets"][0])).reserve


def dynamic(key=0, factor=8000):
    return DynamicConfig(
        key=key,
        collateral_factor_bps=factor,
        max_liquidation_bonus_bps=10500,
        liquidation_fee_bps=1000,
    )


def market(reserve, price=100 * 10**8, **changes):
    return MarketView(
        supply_index=RAY,
        borrow_index=RAY,
        risk=RiskParameters(
            reserve=reserve,
            configs=(dynamic(),),
            liquidation=LiquidationConfig(
                target_health_factor=105 * WAD // 100,
                health_factor_for_max_bonus=95 * WAD // 100,
                bonus_factor_bps=5000,
            ),
        ),
        price=OraclePrice(
            source=reserve.source,
            oracle="0x" + "11" * 20,
            feed="0x" + "22" * 20,
            price=price,
            dependencies=(),
        ),
    ).model_copy(update=changes)


def position(reserve, **changes):
    return EconomicPosition(
        target=SupplyTarget(
            allocation=f"reserve_{reserve.source.reserve_id}",
            account="fund",
            fund=Account(chain=reserve.source.chain, address="0x" + "aa" * 20),
            reserve=reserve.source,
        ),
    ).model_copy(update=changes)


def debt_reserve(reserve):
    asset = AssetType("eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
    source = reserve.source.model_copy(update={"reserve_id": 7, "asset": asset})
    return reserve.model_copy(
        update={
            "source": source,
            "asset": Asset(chain=source.chain, identifier=asset, decimals=6),
            "hub_asset_id": 1,
        }
    )


def test_premium_refresh_partial_and_full_repayment(reserve):
    before = position(reserve, drawn_shares=1000)
    before = reset_premium(before, RAY, 1000)
    assert premium_ray(before, RAY) == 0
    view = market(reserve, borrow_index=12 * RAY // 10)
    assert debts(before, view.borrow_index) == (1200, 20)
    refreshed = reset_premium(before, view.borrow_index, 2000)
    assert premium_ray(refreshed, view.borrow_index) == 20 * RAY
    assert premium_ray(refreshed, 13 * RAY // 10) == 40 * RAY
    premium_only, paid, taken = repay(before, view, 10)
    assert (paid, taken) == (10, 0)
    assert debts(premium_only, view.borrow_index) == (1200, 10)
    partial, paid, taken = repay(before, view, 30)
    assert (paid, taken) == (30, 8)
    assert partial.drawn_shares == 992 and premium_ray(partial, view.borrow_index) == 0
    closed, paid, taken = repay(before, view, 9999)
    assert (paid, taken) == (1220, 1000)
    assert debts(closed, view.borrow_index) == (0, 0)
    assert before.drawn_shares == 1000
    with pytest.raises(ValueError, match="more debt"):
        reset_premium(before, RAY, 100, taken=1001)
    with pytest.raises(ValueError, match="more debt"):
        reset_premium(before, RAY, 100, restored=1)
    with pytest.raises(ValueError, match="nonnegative"):
        repay(before, view, -1)


def test_premium_rounding_and_fixed_point_supply_claims(reserve):
    before = position(reserve, drawn_shares=3)
    before = reset_premium(before, RAY, 1)
    assert before.premium_shares == 1
    view = market(reserve, supply_index=3 * RAY // 2, borrow_index=RAY + 1)
    assert debts(before, RAY + 1) == (4, 1)
    closed, paid, taken = repay(before, view, 5)
    assert (paid, taken) == (5, 3) and debts(closed, view.borrow_index) == (0, 0)
    assert view.supplied(3) == 4 and view.supplied(3, up=True) == 5
    assert view.supply_shares(4) == 2 and view.supply_shares(4, up=True) == 3
    invalid = before.model_copy(update={"premium_offset_ray": 2 * RAY})
    with pytest.raises(ValueError, match="negative debt"):
        premium_ray(invalid, RAY)


def test_spoke_health_uses_correct_units_and_attached_dynamic_configuration(reserve):
    usdc = debt_reserve(reserve)
    collateral = position(reserve, shares=2 * 10**18, collateral=True)
    debt = position(usdc, drawn_shares=100 * 10**6)
    weth_market, usdc_market = market(reserve), market(usdc, 10**8)
    risk = risk_view((collateral, debt), (weth_market, usdc_market))
    assert risk.collateral_value == 200 * 10**26
    assert risk.debt_value_ray == 100 * 10**26 * RAY
    assert risk.health_factor_wad == 16 * WAD // 10
    assert risk.borrow_count == risk.active_collateral_count == 1
    assert risk.fund == collateral.target.fund and risk.spoke == reserve.source.spoke
    require_healthy(risk)
    updated = weth_market.risk.model_copy(
        update={
            "reserve": reserve.model_copy(update={"dynamic_config_key": 1}),
            "configs": (dynamic(), dynamic(1, 5000)),
        }
    )
    markets = (weth_market.model_copy(update={"risk": updated}), usdc_market)
    assert risk_view((collateral, debt), markets).health_factor_wad == 16 * WAD // 10
    attached = collateral.model_copy(update={"dynamic_config_key": 1})
    assert risk_view((attached, debt), markets).health_factor_wad == WAD
    modified = updated.model_copy(update={"configs": (dynamic(factor=7000), dynamic(1, 5000))})
    markets = (weth_market.model_copy(update={"risk": modified}), usdc_market)
    assert risk_view((collateral, debt), markets).health_factor_wad == 14 * WAD // 10
    empty_debt = risk_view((collateral,), (weth_market,))
    assert empty_debt.health_factor_wad is None and empty_debt.risk_premium_bps == 0
    with pytest.raises(ValueError, match="below one"):
        require_healthy(risk_view((debt,), (usdc_market,)))


def test_risk_scope_refuses_cross_fund_spoke_and_duplicate_claims(reserve):
    before, view = position(reserve), market(reserve)
    other_fund = before.target.fund.model_copy(update={"address": "0x" + "bb" * 20})
    other = before.model_copy(
        update={"target": before.target.model_copy(update={"fund": other_fund})}
    )
    with pytest.raises(ValueError, match="mix funds"):
        risk_view((before, other), (view, view))
    with pytest.raises(ValueError, match="duplicate"):
        risk_view((before, before), (view, view))
    with pytest.raises(ValueError, match="each declared"):
        risk_view((), ())
    with pytest.raises(ValueError, match="differs"):
        risk_view((before,), (market(debt_reserve(reserve)),))


def test_weighted_risk_uses_cheapest_collateral_first():
    assert weighted_risk([(200, 100), (100, 50)], 100) == 150
    assert weighted_risk([(100, 50), (200, 100)], 100) == 150
    assert weighted_risk([(200, 100), (100, 50)], 200) == 167
    assert weighted_risk([], 100) == weighted_risk([(100, 50)], 0) == 0


def rate_record(reserve, at, *, supply=RAY, borrow=RAY, index=RAY):
    source = AccountingSource(
        chain=reserve.source.chain,
        hub=reserve.source.hub,
        asset=reserve.source.asset,
        hub_asset_id=reserve.hub_asset_id,
    )
    return RateRecord(
        key=rate_key(source),
        unit="annual_fraction_ray",
        source_time=UnixTime(at),
        available_at=UnixTime(at),
        retrieved_at=float(at + YEAR),
        evidence=("a" * 64,),
        reference=reserve.block.model_copy(update={"timestamp": at}),
        payload=Rates(
            source=source,
            convention="annual_fraction_ray_365_days",
            supply_apr_ray=supply,
            borrow_apr_ray=borrow,
            supply_index_ray=index,
            borrow_index_ray=index,
            added_assets=1,
            added_shares=1,
            liquidity_fee_bps=0,
        ),
    )


def curve(records, step=300):
    req = DataRequirement(
        key=records[0].key,
        start=records[0].available_at,
        end=records[-1].available_at,
        semantics="sampled",
        sample_seconds=step,
        max_age_seconds=step,
        unit="annual_fraction_ray",
        visibility="end_of_block",
        payload_revision="rates-v1",
    )
    return RateCurve(Timeline(records, req))


def test_rate_curve_compounds_only_at_fixed_boundaries_without_overwriting_claims(reserve):
    start = reserve.block.timestamp
    records = tuple(rate_record(reserve, start + offset) for offset in (0, 300, 600))
    original = curve(records)
    # Independently derive the integer-rounded index at each sample boundary.
    linear = RAY + RAY * 300 // YEAR
    expected_supply = (RAY * linear // RAY) * linear // RAY
    expected_borrow = -(-linear * linear // RAY)
    assert original.at(start + 600) == (expected_supply, expected_borrow)
    for cadence in (1, 5, 60, 300):
        observed = curve(records)
        for time in range(start, start + 600, cadence):
            observed.at(time)
        assert observed.at(start + 600) == original.at(start + 600)
    replaced = (
        records[0],
        records[1].model_copy(
            update={
                "payload": records[1].payload.model_copy(update={"supply_index_ray": 9 * RAY}),
            }
        ),
        records[2],
    )
    assert curve(replaced).at(start + 600) == original.at(start + 600)
    changed_rate = records[1].payload.model_copy(update={"supply_apr_ray": 9 * RAY})
    revised = curve(
        (records[0], records[1].model_copy(update={"payload": changed_rate}), records[2])
    )
    assert revised.at(start + 299) == original.at(start + 299)
    assert revised.at(start + 600)[0] > expected_supply
    with pytest.raises(ValueError, match="outside prepared"):
        original.at(start + 601)
    with pytest.raises(ValueError, match="before its initial"):
        original.at(start - 1)


def test_rate_curve_rejects_missing_samples_and_wrong_units(reserve):
    start = reserve.block.timestamp
    records = (rate_record(reserve, start), rate_record(reserve, start + 600))
    with pytest.raises(ValueError, match="missing a fixed"):
        curve(records)
    req = DataRequirement(
        key=records[0].key,
        start=UnixTime(start),
        end=UnixTime(start + 600),
        semantics="changes",
        max_age_seconds=600,
        unit="annual_fraction_ray",
        visibility="end_of_block",
        payload_revision="rates-v1",
    )
    with pytest.raises(ValueError, match="requires sampled"):
        RateCurve(Timeline(records, req))


def test_supply_derivative_accounts_for_premium_fees_and_virtual_assets():
    actual = supply_rate(12 * RAY // 10, 1000, 100, RAY // 10, 1000, 2000)
    expected = Fraction(132, 1_002_000) * Fraction(9, 10) * RAY
    assert actual == expected.numerator // expected.denominator
    assert supply_rate(RAY, 0, 0, RAY, 0, 0) == 0
    assert grow(RAY, RAY // 10, YEAR, up=True) == 11 * RAY // 10
    assert grow(RAY, 0, YEAR, up=False) == RAY
    with pytest.raises(ValueError, match="nonnegative"):
        grow(RAY, RAY, -1, up=True)


def test_selected_market_and_history_bounds_remain_explicit(reserve):
    require_range(reserve.source, UnixTime(1774828800), UnixTime(1774828801))
    with pytest.raises(ValueError, match="backtest_before_start"):
        require_range(reserve.source, UnixTime(1774828799), UnixTime(1774828801))
    invalid = AaveReserve.model_validate(reserve.source.model_dump() | {"reserve_id": 999})
    # Range admission does not substitute a sample market list for chain reads.
    require_range(invalid, UnixTime(1774828800), UnixTime(1774828801))
    with pytest.raises(ValueError, match="configuration version"):
        market(reserve).risk.config(-1)
