"""Annual rates converted from the values the lending reserve readers observe."""

import gzip
import json
import math
from decimal import Decimal
from fractions import Fraction
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors.aave_v3.domain import PositionObservation
from almanak.connectors.compound_v3.domain import Revision
from almanak.connectors.lending_lookup import LENDING_CONNECTORS
from almanak.connectors.lending_rates import (
    RATES,
    SECONDS_PER_YEAR,
    UNIT,
    continuous_apy,
    per_second_apr,
    per_second_apy,
    rates_entry,
    ray_apr,
    shown,
)

USDC = "eip155:1/erc20:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
WBTC = "eip155:1/erc20:0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
ZERO = "0.000000000000"


def token(identifier, decimals=6):
    return SimpleNamespace(identifier=identifier, decimals=decimals)


# A stored Aave V3 liquidity rate `ax lending-reserve` reported, 2.305% APR.
LIQUIDITY_RATE_RAY = 23047691114629590697652518
BORROW_RATE_RAY = 41234567890123456789012345


def close(text: str, expected: float) -> bool:
    # Presentation rounds to 12 places; the float references are good to about 1e-15.
    return math.isclose(float(text), expected, rel_tol=0, abs_tol=6e-13)


def compounded(per_second: float) -> float:
    return math.expm1(SECONDS_PER_YEAR * math.log1p(per_second))


def test_every_lending_connector_converts_its_rates_or_says_why_not():
    unobserved = rates_entry("aave_v4", None, None)
    assert unobserved["rates"] is None and unobserved["rates_unavailable"]
    assert set(RATES) | {"aave_v4"} == set(LENDING_CONNECTORS)


def test_ray_rates_are_per_year_aprs_compounded_per_second():
    apr = ray_apr(LIQUIDITY_RATE_RAY)
    assert apr == Fraction(LIQUIDITY_RATE_RAY, 10**27)
    assert shown(apr) == "0.023047691115"
    apy = per_second_apy(apr)
    assert close(shown(apy), compounded(float(apr) / SECONDS_PER_YEAR))
    assert shown(apy) == "0.023315341424"


def test_per_second_rates_scale_to_a_year():
    assert per_second_apr(10**9) == Fraction(31536, 10**6)
    assert per_second_apr(Fraction(1, 2), scale=1) == Fraction(SECONDS_PER_YEAR, 2)


def test_continuous_apy_is_e_to_the_apr_less_one():
    apr = Fraction(1, 10)
    assert close(shown(continuous_apy(apr)), math.expm1(0.1))
    assert shown(continuous_apy(Fraction(0))) == "0.000000000000"


def test_presentation_rounds_half_even_to_twelve_places():
    assert shown(Fraction(5, 10**13)) == "0.000000000000"
    assert shown(Fraction(15, 10**13)) == "0.000000000002"
    assert shown(Decimal("0.1234567890125")) == "0.123456789012"


def pool_reserve(**fields):
    return SimpleNamespace(
        liquidity_rate_ray=LIQUIDITY_RATE_RAY,
        variable_borrow_rate_ray=BORROW_RATE_RAY,
        asset=token(USDC),
        **fields,
    )


def test_aave_v3_reports_its_ray_rates_and_virtual_balance():
    rates = RATES["aave_v3"](
        pool_reserve(virtual_underlying_balance=1_234_567_890, available_underlying=1)
    )
    assert rates == {
        "asset": USDC,
        "supply_apr": "0.023047691115",
        "supply_apy": "0.023315341424",
        "borrow_apr": "0.041234567890",
        "borrow_apy": shown(per_second_apy(ray_apr(BORROW_RATE_RAY))),
        "unit": UNIT,
        "compounding": "per_second",
        "available_liquidity": "1234.567890",
        "raw": {
            "liquidity_rate_ray": LIQUIDITY_RATE_RAY,
            "variable_borrow_rate_ray": BORROW_RATE_RAY,
            "virtual_underlying_balance": 1_234_567_890,
        },
    }


def test_sparklend_lends_its_physical_balance():
    rates = RATES["sparklend"](pool_reserve(available_underlying=5_000_000))
    assert rates["available_liquidity"] == "5.000000"
    assert rates["raw"]["available_underlying"] == 5_000_000
    assert rates["supply_apr"] == "0.023047691115"


def comet(
    supply_principal,
    borrow_principal,
    revision=Revision.STANDARD,
    indices=(10**15, 10**15),
    supply_base=0,
):
    curve = SimpleNamespace(
        kink=8 * 10**17,
        base_per_second=supply_base,
        slope_low_per_second=10**9,
        slope_high_per_second=10**10,
    )
    borrow = SimpleNamespace(
        kink=8 * 10**17,
        base_per_second=5 * 10**8,
        slope_low_per_second=2 * 10**9,
        slope_high_per_second=2 * 10**10,
    )
    accounting = SimpleNamespace(
        total_supply_principal=supply_principal,
        total_borrow_principal=borrow_principal,
        supply_index=indices[0],
        borrow_index=indices[1],
    )
    return SimpleNamespace(
        accounting=accounting,
        supply_curve=curve,
        borrow_curve=borrow,
        revision=revision,
        base_available=7_000_000,
        base_asset=token(USDC),
    )


def test_compound_rates_follow_the_comet_curves_at_its_utilization():
    # Half utilized, below the kink: supply 0.5e9 per second, borrow 0.5e9 + 1e9.
    rates = RATES["compound_v3"](comet(10_000_000, 5_000_000))
    assert rates["raw"] == {
        "supply_rate_per_second": 5 * 10**8,
        "borrow_rate_per_second": 15 * 10**8,
        "base_available": 7_000_000,
    }
    assert rates["supply_apr"] == "0.015768000000"
    assert rates["borrow_apr"] == "0.047304000000"
    assert close(rates["supply_apy"], compounded(5e-10))
    assert close(rates["borrow_apy"], compounded(1.5e-9))
    assert rates["available_liquidity"] == "7.000000"
    assert rates["compounding"] == "per_second"


def test_an_empty_comet_supplies_at_its_base_rate():
    rates = RATES["compound_v3"](comet(0, 0))
    assert rates["supply_apr"] == "0.000000000000"
    assert rates["borrow_apr"] == shown(per_second_apr(5 * 10**8))


def morpho(supplied, borrowed, fee, rate):
    accounting = SimpleNamespace(supply_assets=supplied, borrow_assets=borrowed, fee=fee)
    return SimpleNamespace(accounting=accounting, borrow_rate=rate, loan_asset=token(USDC))


def test_morpho_supply_earns_the_borrow_rate_on_the_utilized_share_less_the_fee():
    rate = 1_585_489_599  # about 5% a year per second, in 1e18
    rates = RATES["morpho_blue"](morpho(100_000_000, 80_000_000, 10**17, rate))
    apr = rate * SECONDS_PER_YEAR / 1e18
    assert rates["borrow_apr"] == shown(Fraction(rate * SECONDS_PER_YEAR, 10**18))
    assert close(rates["borrow_apy"], math.expm1(apr))
    assert close(rates["supply_apr"], apr * 0.8 * 0.9)
    # Utilization and fee scale the rate before it compounds (docs.morpho.org IRM).
    assert close(rates["supply_apy"], math.expm1(apr * 0.8 * 0.9))
    assert rates["asset"] == USDC
    assert rates["compounding"] == "continuous"
    assert rates["available_liquidity"] == "20.000000"
    assert rates["raw"] == {
        "borrow_rate_per_second": rate,
        "supply_assets": 100_000_000,
        "borrow_assets": 80_000_000,
        "fee": 10**17,
    }


def test_morpho_supply_apy_compounds_the_supply_rate_not_the_borrow_apy():
    # 5% borrow APR, 80% utilized, 10% fee: the supply APR is 0.05 * 0.8 * 0.9 = 3.6%
    # and supply APY e^0.036 - 1 = 3.6655846491%, not (e^0.05 - 1) * 0.72 = 3.6915189%.
    observation = morpho(100_000_000, 80_000_000, 10**17, 0)
    observation.borrow_rate = Fraction(5, 100) * 10**18 / SECONDS_PER_YEAR
    rates = RATES["morpho_blue"](observation)
    assert rates["supply_apr"] == "0.036000000000"
    assert rates["supply_apy"] == "0.036655846491"
    assert close(rates["supply_apy"], math.expm1(0.036))
    assert rates["borrow_apy"] == "0.051271096376"


@pytest.mark.parametrize("supplied", [0, 50])
def test_an_unborrowed_morpho_market_pays_suppliers_nothing(supplied):
    rates = RATES["morpho_blue"](morpho(supplied, 0, 0, 10**9))
    assert rates["supply_apr"] == rates["supply_apy"] == "0.000000000000"
    assert rates["borrow_apr"] == "0.031536000000"


def test_a_recorded_aave_v3_reserve_converts_from_its_own_observed_fields():
    path = Path(__file__).parent / "fixtures/aave-v3-ethereum-revision11-reads.json.gz"
    raw = json.loads(gzip.decompress(path.read_bytes()))
    reserve = PositionObservation.model_validate_json(json.dumps(raw["positions"][0])).reserve
    rates = rates_entry("aave_v3", reserve, reserve.asset.identifier)["rates"]
    assert rates["selected_asset"]["role"] == "lent"
    assert rates["selected_asset"]["supply_apy"] == rates["supply_apy"]
    assert rates["raw"] == {
        "liquidity_rate_ray": reserve.liquidity_rate_ray,
        "variable_borrow_rate_ray": reserve.variable_borrow_rate_ray,
        "virtual_underlying_balance": reserve.virtual_underlying_balance,
    }
    assert close(rates["supply_apr"], reserve.liquidity_rate_ray / 1e27)
    assert close(
        rates["borrow_apy"], compounded(reserve.variable_borrow_rate_ray / 1e27 / SECONDS_PER_YEAR)
    )
    assert Decimal(rates["available_liquidity"]) == Decimal(
        reserve.virtual_underlying_balance
    ).scaleb(-reserve.asset.decimals)
    # A reserve with borrowers pays suppliers less than borrowers pay.
    assert Decimal(rates["supply_apr"]) <= Decimal(rates["borrow_apr"])


def test_compound_above_the_kink_adds_the_high_slope_on_the_excess():
    # 90% utilized, kink 80%: supply 1e9 * 0.8 + 1e10 * 0.1 = 1.8e9 per second,
    # borrow 0.5e9 + 2e9 * 0.8 + 2e10 * 0.1 = 4.1e9 per second.
    rates = RATES["compound_v3"](comet(10_000_000, 9_000_000))
    assert rates["raw"]["supply_rate_per_second"] == 18 * 10**8
    assert rates["raw"]["borrow_rate_per_second"] == 41 * 10**8
    assert rates["supply_apr"] == shown(Fraction(18 * 10**8 * SECONDS_PER_YEAR, 10**18))
    assert close(rates["borrow_apy"], compounded(4.1e-9))


def test_compound_utilization_weighs_principals_by_their_own_indices():
    # Present supply 10M * 2 = 20M, present borrow 8M * 1.5 = 12M: 60% utilized,
    # not the 80% the principals alone suggest.
    rates = RATES["compound_v3"](comet(10_000_000, 8_000_000, indices=(2 * 10**15, 15 * 10**14)))
    assert rates["raw"]["supply_rate_per_second"] == 6 * 10**8
    assert rates["raw"]["borrow_rate_per_second"] == 17 * 10**8


@pytest.mark.parametrize(
    ("revision", "supply", "borrow"),
    [(Revision.STANDARD, 3 * 10**8, 5 * 10**8), (Revision.CONTROLS, 0, 0)],
)
def test_a_controls_comet_pays_and_charges_nothing_while_unborrowed(revision, supply, borrow):
    # Unborrowed, its supply covering the base balance: the controls revision zeroes
    # both base rates, where the standard one still answers them.
    rates = RATES["compound_v3"](comet(10_000_000, 0, revision=revision, supply_base=3 * 10**8))
    assert rates["raw"]["supply_rate_per_second"] == supply
    assert rates["raw"]["borrow_rate_per_second"] == borrow
    assert rates["supply_apr"] == shown(per_second_apr(supply))


@pytest.mark.parametrize(
    ("connector", "observation"),
    [
        ("compound_v3", comet(10_000_000, 5_000_000)),
        ("morpho_blue", morpho(100_000_000, 80_000_000, 10**17, 1_585_489_599)),
    ],
)
def test_collateral_earns_nothing_while_the_lent_asset_earns_the_market_rate(
    connector, observation
):
    collateral = rates_entry(connector, observation, WBTC)["rates"]
    # The market's rates stay the lent asset's, and say so.
    assert collateral["asset"] == USDC
    assert collateral["supply_apr"] != ZERO
    assert collateral["selected_asset"] == {
        "asset": WBTC,
        "role": "collateral",
        "supply_apr": ZERO,
        "supply_apy": ZERO,
    }
    lent = rates_entry(connector, observation, USDC)["rates"]
    assert lent["selected_asset"] == {
        "asset": USDC,
        "role": "lent",
        "supply_apr": lent["supply_apr"],
        "supply_apy": lent["supply_apy"],
    }
    assert {k: v for k, v in lent.items() if k != "selected_asset"} == {
        k: v for k, v in collateral.items() if k != "selected_asset"
    }
