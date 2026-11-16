"""Retained mined TriCrypto outcomes constrain prices, claims and custody effects."""

import gzip
import json
from dataclasses import asdict

import pytest

from almanak.connectors.curve import tricrypto_transitions as transitions
from almanak.connectors.curve.tricrypto_observation import TriCryptoObservation
from tests.retained_fixtures import retained

EVIDENCE = retained("2026-09-07-rollout/curve-tricrypto-transitions")


def captured(name):
    return json.loads(gzip.decompress((EVIDENCE / (name + ".gz")).read_bytes()))


CASES = [
    case
    for name in ("entry", "transition", "zero-receiver", "ramp")
    for case in captured("curve-three-" + name + "-fork.json")["cases"]
]


def evaluated(case):
    observation = TriCryptoObservation.model_validate_json(json.dumps(case["observation"]))
    inputs = case.get("inputs")
    if inputs is None:
        inputs = {"shares": case["shares"]}
        if case["mode"] == "single":
            inputs["index"] = case["index"]
    function = (
        transitions.exchange
        if case["mode"] == "exchange"
        else (
            transitions.add_liquidity
            if case["mode"].startswith("add")
            else transitions.withdraw_one
            if case["mode"] == "single"
            else transitions.withdraw_proportional
        )
    )
    return observation, function(observation, **inputs, timestamp=case["timestamp"])


def packed(values):
    return values[0] + (values[1] << 128)


@pytest.mark.parametrize("case", CASES)
def test_all_mined_storage_and_fund_effects_match_exactly(case):
    observation, result = evaluated(case)
    assert json.loads(json.dumps(asdict(result))) == case["predicted"]
    assert case["receipt"]["status"] == "0x1"
    actual = case["actual_storage"]
    assert tuple(actual[str(slot)] for slot in (11, 12, 13)) == result.state.balances
    assert actual["3"] == packed(result.state.price_scales)
    assert actual["4"] == packed(result.prices.cached_oracle)
    assert actual["5"] == packed(result.prices.last_price)
    assert actual["6"] == result.prices.last_timestamp
    assert actual["14"] == result.state.invariant
    assert actual["15"] == result.admin.profit
    assert actual["16"] == result.admin.claimed_profit
    assert actual["17"] == result.admin.virtual_price
    assert actual["25"] == result.state.total_supply
    assert tuple(case.get("actual_outputs", case.get("actual_amounts"))) == result.amounts
    if "actual_lp_change" in case:
        assert (
            case["actual_lp_change"]
            == result.state.total_supply - observation.total_supply - result.admin_shares
        )


@pytest.mark.parametrize(
    "action",
    [
        transitions.withdraw_one,
        transitions.withdraw_proportional,
        transitions.exchange,
        transitions.add_liquidity,
    ],
)
def test_future_observation_cannot_authorize_earlier_transition(action):
    observation = TriCryptoObservation.model_validate_json(json.dumps(CASES[0]["observation"]))
    arguments = (
        (0, 100)
        if action is transitions.withdraw_one
        else (
            (100,)
            if action is transitions.withdraw_proportional
            else (0, 1, 100)
            if action is transitions.exchange
            else ((1, 0, 0),)
        )
    )
    with pytest.raises(ValueError, match="future observation"):
        action(observation, *arguments, timestamp=observation.block.timestamp - 1)


def test_native_donation_and_zero_receiver_are_distinct_claim_outcomes():
    donation = next(case for case in CASES if case.get("tuning") == "native_donation")
    observation, result = evaluated(donation)
    assert observation.actual_balances[2] > observation.balances[2]
    assert result.admin_shares > 0
    disabled = next(case for case in CASES if case.get("tuning") == "no_receiver")
    _, result = evaluated(disabled)
    assert result.admin_shares == 0
    assert result.admin.claimed_profit > 0
