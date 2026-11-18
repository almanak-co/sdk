"""V3 templates, simulation returns and integer effects match deployed fork contracts."""

import copy
import gzip
import json
from pathlib import Path

import pytest
from pydantic import TypeAdapter

from almanak.connectors.concentrated_math import amounts
from almanak.connectors.uniswap_v3 import abi, receipts
from almanak.connectors.uniswap_v3.observations import Snapshot
from almanak.connectors.uniswap_v3.operations import (
    Close,
    Decrease,
    Increase,
    Mint,
    ProtocolOperation,
)
from almanak.connectors.uniswap_v3.planning import sqrt_bounds
from almanak.connectors.uniswap_v3.simulation import validate_simulation
from almanak.gateway.ethereum import DataError

CAPTURE = json.loads(
    gzip.decompress(
        (Path(__file__).with_name("fixtures") / "uniswap-v3-operations.json.gz").read_bytes()
    )
)


@pytest.mark.parametrize("row", CAPTURE, ids=lambda row: row["origin"]["chain"]["chain_id"])
def test_deployed_operations_reconcile_templates_effects_and_integer_principal(row):
    seen = set()
    for step in row["steps"]:
        operation = TypeAdapter(ProtocolOperation).validate_json(json.dumps(step["operation"]))
        before = Snapshot.model_validate_json(json.dumps(step["before"]))
        after = Snapshot.model_validate_json(json.dumps(step["after"]))
        assert abi.call(operation).model_dump(mode="json") == step["call"]
        validate_simulation(operation, step["simulation"], before)
        effects = receipts.movement(step["receipt"], operation)
        assert effects.model_dump(mode="json") == step["effects"]
        assert (
            after.wallet0.amount - before.wallet0.amount,
            after.wallet1.amount - before.wallet1.amount,
        ) == (effects.delta0, effects.delta1)
        if isinstance(operation, (Mint, Increase)):
            expected = amounts(
                effects.liquidity_delta,
                after.pool.sqrt_price_x96,
                *sqrt_bounds(operation.target),
                up=True,
            )
            assert expected == (-effects.delta0, -effects.delta1)
        elif isinstance(operation, (Decrease, Close)):
            expected = amounts(
                operation.liquidity, after.pool.sqrt_price_x96, *sqrt_bounds(operation.target)
            )
            assert expected == (effects.principal0, effects.principal1)
        seen.add(operation.kind)
    assert seen == {"approve", "swap", "mint", "increase", "decrease", "collect", "close"}
    final = Snapshot.model_validate_json(json.dumps(row["final"]))
    assert final.owned.position is None and not final.needs_cleanup


@pytest.mark.parametrize("kind", ["swap", "mint", "increase", "decrease", "collect", "close"])
@pytest.mark.parametrize("mutation", ["duplicate", "recipient"])
def test_forged_events_cannot_establish_owned_effects(kind, mutation):
    step = next(s for s in CAPTURE[0]["steps"] if s["operation"]["kind"] == kind)
    operation = TypeAdapter(ProtocolOperation).validate_json(json.dumps(step["operation"]))
    receipt = copy.deepcopy(step["receipt"])
    event = next(e for e in receipt["logs"] if e["address"].lower() == operation.target.pool.pool)
    if mutation == "duplicate":
        receipt["logs"].append(copy.deepcopy(event))
    else:
        event["topics"][1] = "0x" + "00" * 12 + "34" * 20
    with pytest.raises(DataError):
        receipts.movement(receipt, operation)


@pytest.mark.parametrize("kind", ["swap", "mint", "increase", "decrease", "collect", "close"])
@pytest.mark.parametrize("mutation", ["trailing", "truncated", "malformed"])
def test_noncanonical_simulation_bytes_cannot_authorize_execution(kind, mutation):
    step = next(s for s in CAPTURE[0]["steps"] if s["operation"]["kind"] == kind)
    operation = TypeAdapter(ProtocolOperation).validate_json(json.dumps(step["operation"]))
    before = Snapshot.model_validate_json(json.dumps(step["before"]))
    result = step["simulation"]
    result = {"trailing": result + "00", "truncated": result[:-2], "malformed": "0xzz"}[mutation]
    with pytest.raises(DataError, match="malformed"):
        validate_simulation(operation, result, before)


@pytest.mark.parametrize("token", ["0x1", "0x" + "gg" * 32, "0x" + "00" * 33])
def test_nft_topics_require_exact_canonical_word(token):
    from almanak.connectors.uniswap_v3.receipts import TRANSFER

    step = next(s for s in CAPTURE[0]["steps"] if s["operation"]["kind"] == "mint")
    operation = TypeAdapter(ProtocolOperation).validate_json(json.dumps(step["operation"]))
    receipt = copy.deepcopy(step["receipt"])
    event = next(
        e
        for e in receipt["logs"]
        if e["address"].lower() == operation.target.contracts.position_manager
        and e["topics"][0] == TRANSFER
    )
    event["topics"][3] = token
    with pytest.raises(DataError, match="canonical"):
        receipts.movement(receipt, operation)
