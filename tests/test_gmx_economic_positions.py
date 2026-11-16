"""Position pricing distinguishes capped impact from execution and settlement."""

import json
from pathlib import Path

import pytest

from almanak.connectors.gmx_v2 import economic_position_reader
from almanak.connectors.gmx_v2.economic_position_math import (
    decrease_price,
    decrease_tokens,
    increase_price,
    next_interest,
    pending_impact_usd,
    position_imbalance,
    position_impact,
    positive_impact_capacity,
    realized_impact,
    signed_div,
    virtual_interest,
)
from almanak.connectors.gmx_v2.economic_position_types import (
    ImpactSettlementState,
    PositionImpactParameters,
    PositionPricingState,
)
from almanak.connectors.gmx_v2.economic_precision import FLOAT

ROOT = Path(__file__).parents[1] / "docs/internal/reports/evidence/2026-09-09-gmx-prepared"


@pytest.fixture(params=["arbitrum", "avalanche"])
def proof(request):
    return json.loads((ROOT / f"{request.param}-position-reader-comparison.json").read_text())


@pytest.mark.parametrize("index", range(16))
def test_actual_reader_execution_price(proof, index):
    state = PositionPricingState.model_validate_json(json.dumps(proof["state"]))
    row = proof["cases"][index]
    delta, is_long = row["size_delta_usd"], row["is_long"]
    calculation = (
        increase_price(state, delta, is_long)
        if delta > 0
        else decrease_price(state, row["position_usd"], row["position_tokens"], -delta, is_long)
    )
    assert calculation.model_dump(mode="json") == row["calculation"]
    assert (
        calculation.price_impact_usd_e30,
        calculation.execution_price,
        calculation.balance_improved,
    ) == tuple(row["contract_result"][:3])


def test_negative_integer_division_truncates_toward_zero():
    assert signed_div(-10, 3) == -3
    assert signed_div(10, 3) == 3
    with pytest.raises(ValueError, match="positive divisor"):
        signed_div(1, 0)


def test_decrease_rounding_is_side_specific_and_full_exit_is_exact():
    assert decrease_tokens(7, 10, 3, True) == 5
    assert decrease_tokens(7, 10, 3, False) == 4
    for side in (True, False):
        assert decrease_tokens(7, 10, 7, side) == 10
    for args in ((0, 10, 0, True), (7, 0, 3, True), (7, 10, 8, True)):
        with pytest.raises(ValueError, match="positive position"):
            decrease_tokens(*args)


def test_reader_interest_saturation_does_not_claim_admission():
    assert next_interest((10, 20), -11, True) == (0, 20)
    assert next_interest((10, 20), -21, False) == (10, 0)
    assert virtual_interest(100, -25) == (25, 125)
    assert virtual_interest(-100, -25) == (125, 25)


def test_position_curves_adjust_both_positive_factor_and_exponent():
    parameters = PositionImpactParameters(
        positive_factor=2 * 10**24,
        negative_factor=10**24,
        positive_exponent=2 * FLOAT,
        negative_exponent=FLOAT,
        maximum_positive_factor=FLOAT,
    )
    result = position_imbalance((3 * FLOAT, FLOAT), (2 * FLOAT, FLOAT), parameters)
    assert result.usd_e30 == 10**24
    assert result.balance_improved
    crossed = position_imbalance((3 * FLOAT, FLOAT), (0, FLOAT), parameters)
    assert crossed.usd_e30 == 10**24


def test_virtual_interest_uses_repriced_token_delta_and_skips_positive_impact(proof):
    state = PositionPricingState.model_validate_json(json.dumps(proof["state"]))
    state = state.model_copy(
        update={
            "use_open_interest_in_tokens": True,
            "long_open_interest": 20,
            "short_open_interest": 10,
            "virtual_inventory": -100,
        }
    )
    actual = position_impact(state, 999 * FLOAT, -5, True)
    no_virtual = position_impact(state.model_copy(update={"virtual_inventory": None}), 0, -5, True)
    assert actual == no_virtual and actual.usd_e30 >= 0
    usd = state.model_copy(
        update={
            "use_open_interest_in_tokens": False,
            "long_open_interest": 20 * state.index_price.midpoint,
            "short_open_interest": 10 * state.index_price.midpoint,
            "virtual_inventory": -100 * state.index_price.midpoint,
        }
    )
    assert position_impact(state, 1, 5, True) == position_impact(
        usd, 5 * state.index_price.midpoint, 0, True
    )


def test_zero_size_uses_side_specific_oracle_bound(proof):
    state = PositionPricingState.model_validate_json(json.dumps(proof["state"]))
    assert increase_price(state, 0, True).execution_price == state.index_price.maximum
    assert increase_price(state, 0, False).execution_price == state.index_price.minimum
    assert decrease_price(state, 7, 10, 0, True).execution_price == state.index_price.minimum
    assert decrease_price(state, 7, 10, 0, False).execution_price == state.index_price.maximum
    with pytest.raises(ValueError, match="negative"):
        increase_price(state, -1, True)


@pytest.fixture(params=["arbitrum", "avalanche"])
def settlement_proof(request):
    return json.loads(
        (ROOT / f"{request.param}-impact-settlement-reader-comparison.json").read_text()
    )


@pytest.mark.parametrize("index", range(24))
def test_actual_reader_complete_decrease_impact(settlement_proof, index):
    state = ImpactSettlementState.model_validate_json(json.dumps(settlement_proof["state"]))
    row = settlement_proof["cases"][index]
    delta = -row["size_delta_usd"]
    price = decrease_price(
        state.pricing, row["position_usd"], row["position_tokens"], delta, row["is_long"]
    )
    impact = realized_impact(
        state, row["position_usd"], row["pending_impact_amount"], delta, price.price_impact_usd_e30
    )
    assert impact.model_dump(mode="json") == row["impact"]
    assert (
        price.price_impact_usd_e30,
        price.execution_price,
        price.balance_improved,
        impact.proportional_pending_usd_e30,
        impact.total_usd_e30,
        impact.claimable_difference_usd_e30,
    ) == tuple(row["contract_result"])


def test_pending_impact_rounds_against_trader(settlement_proof):
    state = ImpactSettlementState.model_validate_json(json.dumps(settlement_proof["state"]))
    price = state.pricing.index_price
    assert pending_impact_usd(price, 3, -10, 1) == -4 * price.maximum
    assert pending_impact_usd(price, 3, 10, 1) == 3 * price.minimum
    with pytest.raises(ValueError, match="bounded position"):
        pending_impact_usd(price, 3, -10, 4)


def test_lent_impact_capacity_and_delayed_negative_difference(settlement_proof):
    state = ImpactSettlementState.model_validate_json(json.dumps(settlement_proof["state"]))
    state = state.model_copy(
        update={
            "pool_usd_e30": 100 * FLOAT,
            "maximum_lendable_factor": FLOAT,
            "maximum_lendable_usd_e30": 10 * FLOAT,
            "lent_impact_amount": 0,
            "impact_pool_amount": 0,
            "maximum_negative_factor": FLOAT // 100,
        }
    )
    assert positive_impact_capacity(state) == 10 * FLOAT
    borrowed = state.model_copy(update={"lent_impact_amount": 100 * FLOAT})
    assert positive_impact_capacity(borrowed) == 0
    impact = realized_impact(state, 100 * FLOAT, 0, 100 * FLOAT, -5 * FLOAT)
    assert impact.total_usd_e30 == -FLOAT
    assert impact.claimable_difference_usd_e30 == 4 * FLOAT


def test_settlement_acquisition_preserves_pool_asset_precision(settlement_proof, monkeypatch):
    state = ImpactSettlementState.model_validate_json(json.dumps(settlement_proof["state"]))
    pricing = state.pricing
    published = json.loads((ROOT / settlement_proof["published_price_file"]).read_text())
    from almanak.connectors.gmx_v2.published_prices import PublishedPrices

    prices = PublishedPrices.model_validate_json(json.dumps(published)).prices
    values = {
        "MAX_POSITION_IMPACT_FACTOR": state.maximum_negative_factor,
        "POSITION_IMPACT_POOL_AMOUNT": state.impact_pool_amount,
        "MAX_LENDABLE_IMPACT_FACTOR": state.maximum_lendable_factor,
        "MAX_LENDABLE_IMPACT_USD": state.maximum_lendable_usd_e30,
        "LENT_POSITION_IMPACT_POOL_AMOUNT": state.lent_impact_amount,
    }
    calls = []

    def read_uint(reader, market, block, name, *, positive=None):
        assert market == pricing.market and block == pricing.block
        assert positive is False if name == "MAX_POSITION_IMPACT_FACTOR" else positive is None
        return values[name]

    def read_pool(reader, block, store, kind, name, types, args):
        assert (block, store, kind, name, types) == (
            pricing.block,
            pricing.market.contracts.data_store,
            "Uint",
            "POOL_AMOUNT",
            ("address", "address"),
        )
        assert args[0] == pricing.market.share_asset.reference
        calls.append(args[1])
        return 11 if args[1] == pricing.market.long_asset.reference else 19

    monkeypatch.setattr(economic_position_reader, "market_uint", read_uint)
    monkeypatch.setattr(economic_position_reader, "stored", read_pool)
    result = economic_position_reader.prepared_impact_settlement(None, pricing, prices)
    by_asset = {price.asset: price for price in prices}
    expected = (
        11 * by_asset[pricing.market.long_asset].minimum
        + 19 * by_asset[pricing.market.short_asset].minimum
    )
    assert result == state.model_copy(update={"pool_usd_e30": expected})
    assert calls == [pricing.market.long_asset.reference, pricing.market.short_asset.reference]
    with pytest.raises(ValueError, match="same unique"):
        economic_position_reader.prepared_impact_settlement(None, pricing, (*prices, prices[0]))
