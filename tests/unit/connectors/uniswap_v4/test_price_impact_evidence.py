from decimal import Decimal

import pytest

from almanak.connectors.uniswap_v4.price_impact import evaluate_swap_price_impact


def evaluate(**overrides):
    args = {
        "quote_source": "onchain_quoter",
        "quoter_amount": 970_000,
        "amount_in": Decimal("1"),
        "token_out_dec": 6,
        "price_ratio": Decimal("1"),
        "max_price_impact": None,
        "config_max_price_impact": None,
        "using_placeholders": False,
        "managed_fork": False,
    }
    return evaluate_swap_price_impact(**(args | overrides))


def test_measured_pass_preserves_reproducible_inputs():
    evidence = evaluate()
    assert evidence.status == "passed"
    assert evidence.oracle_estimate_raw == 1_000_000
    assert evidence.quote_amount_raw == 970_000
    assert Decimal(evidence.price_impact) == Decimal("0.03")
    assert Decimal(evidence.max_price_impact) == Decimal("0.05")
    assert evidence.to_wire()["schema_version"] == 1


@pytest.mark.parametrize("ratio", [None, Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("Infinity")])
def test_missing_or_invalid_oracle_refuses_live_swap(ratio):
    evidence = evaluate(price_ratio=ratio)
    assert evidence.status == "refused"
    assert evidence.reason == "oracle_unavailable"
    assert evidence.price_impact is None


@pytest.mark.parametrize("limit", [Decimal("NaN"), Decimal("Infinity"), Decimal("-0.01"), Decimal("1.01")])
def test_invalid_limit_is_not_a_guard_pass(limit):
    assert evaluate(max_price_impact=limit).reason == "invalid_impact_limit"


def test_intent_limit_overrides_config_without_rounding_away_breach():
    assert evaluate(max_price_impact=Decimal("0.03")).status == "passed"
    assert evaluate(quoter_amount=969_999, max_price_impact=Decimal("0.03")).status == "refused"
    assert evaluate(max_price_impact=Decimal("0"), config_max_price_impact=Decimal("0.10")).status == "refused"


def test_sub_raw_unit_estimate_is_unmeasured():
    assert evaluate(price_ratio=Decimal("0.0000001")).reason == "oracle_estimate_below_raw_unit"


@pytest.mark.parametrize("overrides", [{"managed_fork": True}, {"using_placeholders": True}, {"quote_source": "local_estimate"}])
def test_test_environment_skip_is_never_a_measured_pass(overrides):
    evidence = evaluate(price_ratio=None, **overrides)
    assert evidence.status == "skipped"
    assert evidence.price_impact is None
    assert evidence.oracle_estimate_raw is None
