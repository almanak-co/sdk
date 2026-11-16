import json

import pytest

from almanak.connectors.aerodrome.models import Parameters
from tests.test_aerodrome_reader import CAPTURE


def wire():
    row = CAPTURE["rows"][0]
    asset0 = row["source"]["currency0"]
    return {
        "connector": "aerodrome",
        "decision_trigger": {"kind": "interval", "seconds": 5},
        "accounts": {
            "bindings": [{"name": "fund", "profile": {"kind": "eoa", "fund": row["fund"]}}]
        },
        "allocations": [
            {
                "name": "pool",
                "account": "fund",
                "pool": row["source"],
                "contracts": row["contracts"],
                "liquidity_enabled": True,
                "capital": {"asset": asset0, "amount": 10**17},
                "entry_swap_bps": 5000,
                "hold_seconds": 120,
                "slippage_bps": 100,
                "deadline_seconds": 300,
                "native_asset": "eip155:8453/slip44:60",
                "minimum_native_balance": 10**16,
            }
        ],
    }


def test_parameters_retain_named_fund_and_pool_scoped_share_identity():
    config = Parameters.model_validate_json(json.dumps(wire()))
    target = config.target("pool")
    assert config.positions == config.allocations
    assert target.pool.shares_asset.reference == target.pool.pool
    assert target.funding_currency == target.assets[0]
    assert target.counter_currency == target.assets[1]
    assert config.allocation("pool").chain == target.fund.chain
    with pytest.raises(ValueError, match="unknown"):
        config.target("absent")


@pytest.mark.parametrize(
    "mutation",
    [
        "wrong_native",
        "retired_maximum",
        "zero_capital",
        "oversized_capital",
        "foreign_capital",
        "duplicate_name",
        "unknown_account",
    ],
)
def test_parameters_refuse_ambiguous_or_unbounded_capital(mutation):
    value = wire()
    allocation = value["allocations"][0]
    if mutation == "wrong_native":
        allocation["native_asset"] = allocation["capital"]["asset"]
    elif mutation == "retired_maximum":
        # Grants are scope-only (blueprint 27): the spending maxima are unknown fields.
        allocation["maximum0"] = dict(allocation["capital"])
    elif mutation == "zero_capital":
        allocation["capital"]["amount"] = 0
    elif mutation == "oversized_capital":
        allocation["capital"]["amount"] = 2**127
    elif mutation == "foreign_capital":
        allocation["capital"]["asset"] = allocation["native_asset"]
    elif mutation == "duplicate_name":
        value["allocations"].append(allocation)
    else:
        allocation["account"] = "absent"
    with pytest.raises(ValueError):
        Parameters.model_validate_json(json.dumps(value))
