"""Actual Pancake fork settlements constrain shared manager behavior and swap events."""

import copy
import gzip
import json
from pathlib import Path

import pytest
from pydantic import TypeAdapter

from almanak.connectors.pancakeswap_v3 import abi, receipts
from almanak.connectors.pancakeswap_v3.operations import ProtocolOperation
from almanak.connectors.pancakeswap_v3.simulation import validate_simulation
from almanak.connectors.uniswap_v3.receipts import SWAP as UNISWAP_SWAP
from almanak.gateway.ethereum import DataError

CAPTURE = json.loads(
    gzip.decompress(
        (
            Path(__file__).with_name("fixtures") / "pancakeswap-v3-bnb-operations.json.gz"
        ).read_bytes()
    )
)


def test_actual_fork_calls_simulations_and_fund_movements_match():
    seen = set()
    for step in CAPTURE["steps"]:
        operation = TypeAdapter(ProtocolOperation).validate_json(json.dumps(step["operation"]))
        assert abi.call(operation).model_dump(mode="json") == step["call"]
        validate_simulation(operation, step["simulation"], None)
        effect = receipts.movement(step["receipt"], operation)
        assert effect.model_dump(mode="json") == step["effects"]
        assert tuple(b - a for a, b in zip(step["before"], step["after"], strict=True)) == (
            effect.delta0,
            effect.delta1,
        )
        seen.add(operation.kind)
    assert seen == {"approve", "swap", "mint", "increase", "decrease", "collect", "close"}
    assert CAPTURE["final_wallets"][0] == 0 and CAPTURE["steps"][-1]["position"] is None


@pytest.mark.parametrize(
    "mutation", ["uniswap_signature", "missing_fee_words", "recipient", "duplicate"]
)
def test_foreign_or_malformed_swap_events_cannot_establish_fund_effects(mutation):
    step = next(s for s in CAPTURE["steps"] if s["operation"]["kind"] == "swap")
    operation = TypeAdapter(ProtocolOperation).validate_json(json.dumps(step["operation"]))
    receipt = copy.deepcopy(step["receipt"])
    event = next(e for e in receipt["logs"] if e["topics"][0] == receipts.SWAP)
    if mutation == "uniswap_signature":
        event["topics"][0] = UNISWAP_SWAP
    elif mutation == "missing_fee_words":
        event["data"] = event["data"][:-128]
    elif mutation == "recipient":
        event["topics"][2] = "0x" + "00" * 12 + "34" * 20
    else:
        receipt["logs"].append(copy.deepcopy(event))
    with pytest.raises(DataError):
        receipts.movement(receipt, operation)
