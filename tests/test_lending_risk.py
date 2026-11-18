"""Lending risk blocks label each connector's collateral factors in basis points."""

import gzip
import json
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from almanak.connectors import lending_lookup
from almanak.connectors.aave_v3.domain import PositionObservation
from almanak.connectors.lending_risk import (
    Collateral,
    RiskQuery,
    comet_risk,
    morpho_incentive,
    morpho_risk,
    pool_risk,
    risk_entry,
    spoke_risk,
)
from almanak.connectors.pool_lending_domain import ReserveConfiguration
from almanak.gateway.ethereum import DataError

FIXTURES = Path(__file__).parent / "fixtures"
WBTC = "eip155:1/erc20:0x2260fac5e5542a773aa44fbcfedf7c193bc2c599"
WETH = "eip155:1/erc20:0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
SPOKE = "0x" + "94" * 20
WAD = 10**18


def config_word(ltv, threshold, bonus, decimals=8):
    return ltv | threshold << 16 | bonus << 32 | decimals << 48


def block_of(collateral, notes=None, query=None):
    return risk_entry(lambda: (collateral, notes or {}), query)["risk"]


def test_aave_v3_decodes_the_recorded_reserve_configuration():
    raw = json.loads(
        gzip.decompress((FIXTURES / "aave-v3-ethereum-revision11-reads.json.gz").read_bytes())
    )
    reserve = PositionObservation.model_validate_json(json.dumps(raw["positions"][0])).reserve
    word = reserve.configuration.raw
    collateral, notes = pool_risk(None, None, reserve, None)
    (row,) = block_of(collateral, notes)["collateral"]
    assert row["asset"] == WETH
    assert row["ltv_bps"] == word & 0xFFFF
    assert row["liquidation_threshold_bps"] == word >> 16 & 0xFFFF
    assert row["liquidation_bonus_bps"] == (word >> 32 & 0xFFFF) - 10_000
    assert 0 < row["ltv_bps"] <= row["liquidation_threshold_bps"] < 10_000
    assert "read the collateral reserve" in notes["scope"]


@pytest.mark.parametrize(
    ("word", "expected"),
    [
        # The configuration word `ax lending-reserve` reported raw for a 75/79/5% reserve.
        (5111647255141708, (7500, 7900, 500)),
        # A reserve that is no collateral stores no bonus, never a negative one.
        (config_word(0, 0, 0), (0, 0, 0)),
    ],
)
def test_aave_v3_and_sparklend_bonus_is_the_stored_bonus_over_par(word, expected):
    reserve = SimpleNamespace(
        configuration=ReserveConfiguration(raw=word), asset=SimpleNamespace(identifier=WBTC)
    )
    (row,) = block_of(*pool_risk(None, None, reserve, None))["collateral"]
    assert (row["ltv_bps"], row["liquidation_threshold_bps"], row["liquidation_bonus_bps"]) == (
        expected
    )
    assert lending_lookup.FAMILIES["sparklend"][3] is pool_risk


def test_compound_scales_every_collateral_factor_to_basis_points():
    def collateral(asset, borrow, liquidate, liquidation):
        return SimpleNamespace(
            asset=SimpleNamespace(identifier=asset),
            borrow_factor=borrow * 10**14,
            liquidate_factor=liquidate * 10**14,
            liquidation_factor=liquidation * 10**14,
        )

    market = SimpleNamespace(
        collaterals=(collateral(WETH, 8250, 8950, 9500), collateral(WBTC, 7000, 7700, 9000))
    )
    risk = block_of(*comet_risk(None, None, market, None))
    rows = [
        (r["asset"], r["ltv_bps"], r["liquidation_threshold_bps"], r["liquidation_bonus_bps"])
        for r in risk["collateral"]
    ]
    assert rows == [(WETH, 8250, 8950, 500), (WBTC, 7000, 7700, 1000)]
    assert "liquidationFactor" in risk["liquidation_bonus"]


@pytest.mark.parametrize(
    ("lltv", "incentive", "bonus"),
    [
        (860, "1.043841336116910229", 438),
        (945, "1.01677681748856126", 167),
        # The factor is capped at 1.15 for low LLTVs.
        (385, "1.15", 1500),
    ],
)
def test_morpho_reports_the_lltv_and_its_liquidation_incentive(lltv, incentive, bonus):
    market = SimpleNamespace(lltv=lltv * 10**15, collateral_asset=WBTC)
    (row,) = block_of(*morpho_risk(None, SimpleNamespace(market=market), None, None))["collateral"]
    assert row["asset"] == WBTC
    assert row["ltv_bps"] == row["liquidation_threshold_bps"] == lltv * 10
    assert row["lltv"] == str(Decimal(lltv) / 1000)
    assert row["liquidation_incentive_factor"] == incentive
    assert row["liquidation_bonus_bps"] == bonus
    assert morpho_incentive(lltv * 10**15) == int(Decimal(incentive) * WAD)


class SpokeReader:
    def __init__(self, failing=False):
        self.calls = []
        self.failing = failing

    def call(self, target, signature, block, **kwargs):
        self.calls.append((target, signature, kwargs.get("args")))
        if self.failing:
            raise DataError("revert", "getDynamicReserveConfig reverted")
        if signature == "getDynamicReserveConfig(uint256,uint32)":
            return (7800, 10600, 1000)
        assert signature == "getLiquidationConfig()"
        return (105 * 10**16, 95 * 10**16, 5000)


def spoke_reserve():
    resolved = SimpleNamespace(reserve=SimpleNamespace(spoke=SPOKE, reserve_id=7))
    observation = SimpleNamespace(
        asset=SimpleNamespace(identifier=WETH), dynamic_config_key=2, collateral_risk_bps=500
    )
    return resolved, observation


def test_aave_v4_reads_the_current_dynamic_config_and_labels_the_risk_premium():
    reader = SpokeReader()
    resolved, observation = spoke_reserve()
    risk = block_of(*spoke_risk(reader, resolved, observation, "block"))
    assert reader.calls == [
        (SPOKE, "getDynamicReserveConfig(uint256,uint32)", (7, 2)),
        (SPOKE, "getLiquidationConfig()", None),
    ]
    (row,) = risk["collateral"]
    assert (row["ltv_bps"], row["liquidation_threshold_bps"], row["liquidation_bonus_bps"]) == (
        7800,
        7800,
        600,
    )
    assert row["collateral_factor_bps"] == 7800
    assert risk["dynamic_config_key"] == 2
    assert risk["liquidation"] == {
        "target_health_factor": "1.0500",
        "health_factor_for_max_bonus": "0.9500",
        "bonus_factor_bps": 5000,
        "liquidation_fee_bps": 1000,
    }
    assert risk["collateral_risk_bps"] == 500
    assert "not a collateral factor" in risk["collateral_risk_note"]


def test_a_refused_aave_v4_risk_read_is_reported_beside_the_lookup():
    resolved, observation = spoke_reserve()
    entry = risk_entry(
        lambda: spoke_risk(SpokeReader(failing=True), resolved, observation, "block"), None
    )
    assert entry == {"risk": None, "risk_error": "revert: getDynamicReserveConfig reverted"}


def test_recorded_aave_v4_spoke_code_dispatches_both_risk_reads():
    # PUSH4 of each selector in the recorded Spoke implementations' dispatch tables.
    reads = json.loads((FIXTURES / "aave-v4-economic-authority-reads.json").read_text())
    spokes = [r["result"] for r in reads if "63ec17bb00" in r["result"]]
    assert spokes and all("63e0fd5c5c" in code for code in spokes)


def test_projection_is_the_threshold_over_the_borrowed_fraction():
    collateral = [Collateral(WBTC, Decimal("0.75"), Decimal("0.79"), 500)]
    query = RiskQuery((Decimal("0.3"), Decimal("0.8")), Decimal("1.6"))
    risk = block_of(collateral, query=query)
    (row,) = risk["collateral"]
    # min(0.75, 0.79 / 1.6 = 0.49375), rounded down.
    assert row["max_borrow_fraction"] == {"by_ltv": "0.7500", "at_floor": "0.4937"}
    assert row["projected"] == [
        {"borrow_fraction": "0.3", "health_factor": "2.6333", "within_borrow_limit": True},
        {"borrow_fraction": "0.8", "health_factor": "0.9875", "within_borrow_limit": False},
    ]
    assert "liquidation_threshold / f" in risk["projection"]
    assert risk["health_factor"].startswith("sum(collateral_value x liquidation_threshold)")
    assert risk["borrow_limit"] == "debt_value <= sum(collateral_value x ltv)"


def test_a_floor_looser_than_the_ltv_leaves_the_ltv_as_the_limit():
    (row,) = block_of(
        [Collateral(WBTC, Decimal("0.5"), Decimal("0.9"), 0)],
        query=RiskQuery(health_floor=Decimal("1.1")),
    )["collateral"]
    assert row["max_borrow_fraction"] == {"by_ltv": "0.5000", "at_floor": "0.5000"}
    assert "projected" not in row


def test_a_reserve_that_is_no_collateral_admits_no_borrow():
    (row,) = block_of(
        [Collateral(WBTC, Decimal(0), Decimal(0), 0)],
        query=RiskQuery((Decimal("0.1"),), Decimal(2)),
    )["collateral"]
    assert row["max_borrow_fraction"] == {"by_ltv": "0.0000", "at_floor": "0.0000"}
    assert row["projected"][0]["health_factor"] == "0.0000"


def test_a_ratio_just_under_a_bound_is_not_rounded_up_to_it():
    fraction = Decimal("0.40000000000000000000000000001")
    floor = Decimal("1.00000000000000000000000000000625")
    (row,) = block_of(
        [Collateral(WBTC, Decimal("0.9"), Decimal("0.8"), 0)],
        query=RiskQuery((fraction,), floor),
    )["collateral"]
    assert row["projected"][0]["health_factor"] == "1.9999"
    assert row["max_borrow_fraction"]["at_floor"] == "0.7999"
