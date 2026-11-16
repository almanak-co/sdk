"""Mined TwoCrypto swaps and deposits constrain complete retained pool state."""

import json
from dataclasses import asdict

import pytest

from almanak.connectors.curve.crypto_observation import CryptoObservation
from almanak.connectors.curve.twocrypto_transitions import (
    add_liquidity,
    exchange,
    withdraw_one,
    withdraw_proportional,
)
from tests.test_curve_crypto_revisions import captured

CAPTURE = captured("curve-two-entry-fork.json")
RAMP_CAPTURE = captured("curve-two-ramp-fork.json")


@pytest.mark.parametrize("case", [*CAPTURE["cases"], *RAMP_CAPTURE["cases"]])
def test_entries_match_native_balances_shares_fees_and_rebalancing(case):
    observation = CryptoObservation.model_validate_json(json.dumps(case["observation"]))
    operation = {
        "exchange": exchange,
        "add_single": add_liquidity,
        "add_balanced": add_liquidity,
        "single": withdraw_one,
        "proportional": withdraw_proportional,
    }[case["mode"]]
    result = operation(observation, **case["inputs"], timestamp=case["timestamp"])
    assert json.loads(json.dumps(asdict(result))) == case["predicted"]
    assert case["receipt"]["status"] == "0x1"
    assert tuple(case["actual_amounts"]) == result.amounts
    assert case["actual_lp_change"] == result.state.total_supply - observation.total_supply
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
    assert result.admin.last_claim_time == observation.accounting.last_claim_time


@pytest.mark.parametrize(
    "input_index,output_index,amount",
    [(False, 1, 100), (0, 1.0, 100), (0, 0, 100), (2, 1, 100), (0, 1, 0), (0, 1, -1)],
)
def test_exchange_requires_distinct_indices_and_positive_input(input_index, output_index, amount):
    observation = CryptoObservation.model_validate_json(
        json.dumps(CAPTURE["cases"][0]["observation"])
    )
    with pytest.raises(ValueError):
        exchange(
            observation, input_index, output_index, amount, timestamp=observation.block.timestamp
        )


@pytest.mark.parametrize("operation,inputs", [(exchange, (0, 1, 100)), (add_liquidity, ((1, 1),))])
def test_entries_cannot_use_future_observations(operation, inputs):
    observation = CryptoObservation.model_validate_json(
        json.dumps(CAPTURE["cases"][0]["observation"])
    )
    with pytest.raises(ValueError, match="future observation"):
        operation(observation, *inputs, timestamp=observation.block.timestamp - 1)


def test_proportional_removal_refuses_future_state_and_unsupported_empty_pool():
    observation = CryptoObservation.model_validate_json(
        json.dumps(CAPTURE["cases"][0]["observation"])
    )
    with pytest.raises(ValueError, match="future observation"):
        withdraw_proportional(observation, 100, timestamp=observation.block.timestamp - 1)
    with pytest.raises(ValueError, match="emptied global pool"):
        withdraw_proportional(
            observation, observation.total_supply, timestamp=observation.block.timestamp
        )
