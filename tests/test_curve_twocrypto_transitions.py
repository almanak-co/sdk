"""Identified fork transactions constrain complete single-coin withdrawal state."""

import json
from dataclasses import asdict

import pytest

from almanak.connectors.curve.crypto_observation import CryptoObservation
from almanak.connectors.curve.twocrypto_rebalancing import PriceState
from almanak.connectors.curve.twocrypto_transitions import withdraw_one
from tests.test_curve_crypto_revisions import captured

CAPTURE = captured("curve-two-transition-fork.json")


@pytest.mark.parametrize("case", CAPTURE["cases"])
def test_mined_withdrawal_matches_pool_balances_profit_prices_and_shares(case):
    observation = CryptoObservation.model_validate_json(json.dumps(case["observation"]))
    result = withdraw_one(observation, case["index"], case["shares"], timestamp=case["timestamp"])
    assert json.loads(json.dumps(asdict(result))) == case["predicted"]
    assert case["receipt"]["status"] == "0x1"
    assert case["actual_output"] == result.amounts[case["index"]]
    actual = case["actual_storage"]
    assert tuple(actual[str(slot)] for slot in (9, 10)) == result.state.balances
    assert actual["1"] == result.state.price_scale
    assert actual["2"] == result.prices.cached_oracle
    assert actual["3"] == result.prices.last_price
    assert actual["4"] == result.prices.last_timestamp
    assert actual["11"] == result.state.invariant
    assert actual["12"] == result.admin.profit
    assert actual["13"] == result.admin.claimed_profit
    assert actual["14"] == result.admin.virtual_price
    assert actual["17"] == result.admin.last_claim_time
    assert actual["18"] == result.admin.virtual_shares
    assert actual["21"] == result.state.total_supply
    if case["tuning"]:
        assert result.state.price_scale == observation.price_scale
    else:
        assert result.state.price_scale != observation.price_scale


@pytest.mark.parametrize("case", CAPTURE["rejections"])
def test_tiny_withdrawals_retain_contract_arithmetic_and_loss_rejection(case):
    observation = CryptoObservation.model_validate_json(json.dumps(case["observation"]))
    assert "revert" in case["contract_error"]
    with pytest.raises(ValueError, match=case["modeled_error"]):
        withdraw_one(observation, case["index"], case["shares"], timestamp=case["timestamp"])


def test_future_observation_cannot_authorize_an_earlier_transition():
    observation = CryptoObservation.model_validate_json(
        json.dumps(CAPTURE["cases"][0]["observation"])
    )
    with pytest.raises(ValueError, match="future observation"):
        withdraw_one(observation, 0, 100, timestamp=observation.block.timestamp - 1)


@pytest.mark.parametrize("values", [(0, 1, 0, 1), (1, 1, 0, 2**192), (1, 1, 0, 2**64)])
def test_rebalancing_requires_positive_prices_and_bounded_parameters(values):
    with pytest.raises(ValueError):
        PriceState(*values)
