"""Pancake targets retain connector identity and the original router deadline ABI."""

import gzip
import json
from pathlib import Path

import pytest
from eth_abi import decode
from eth_utils import keccak
from pydantic import TypeAdapter, ValidationError

from almanak.connectors.pancakeswap_v3 import abi
from almanak.connectors.pancakeswap_v3.domain import PancakePool, PoolObservation
from almanak.connectors.pancakeswap_v3.models import Target
from almanak.connectors.pancakeswap_v3.operations import ProtocolOperation, Swap
from almanak.connectors.uniswap_v3.domain import UniswapPool
from almanak.connectors.uniswap_v3.operations import ProtocolOperation as UniswapOperation

ROW = json.loads(
    gzip.decompress(
        (Path(__file__).with_name("fixtures") / "pancakeswap-v3-bnb-reader.json.gz").read_bytes()
    )
)[2]


def target():
    return Target.model_validate_json(
        json.dumps(
            {
                "allocation": "trade",
                "account": "fund",
                "fund": {"chain": ROW["source"]["chain"], "address": "0x" + "12" * 20},
                "pool": ROW["source"],
                "contracts": ROW["contracts"],
                "range": None,
                "funding_asset": ROW["source"]["currency0"],
            }
        )
    )


def test_original_router_has_direct_deadline_bound_exact_input_tuple():
    selected = target()
    operation = Swap.model_validate_json(
        json.dumps(
            {
                "target": selected.model_dump(mode="json"),
                "amount": {"asset": str(selected.assets[0]), "amount": 10**18},
                "minimum": {"asset": str(selected.assets[1]), "amount": 1},
                "deadline": ROW["block"]["timestamp"] + 60,
            }
        )
    )
    call = abi.call(operation)
    raw = bytes.fromhex(call.data[2:])
    assert raw[:4] == keccak(text=f"exactInputSingle({abi.SWAP_PARAMS})")[:4]
    params = decode((abi.SWAP_PARAMS,), raw[4:])[0]
    assert params == (
        selected.assets[0].reference,
        selected.assets[1].reference,
        selected.pool.fee,
        selected.fund.address,
        operation.deadline,
        operation.amount.amount,
        operation.minimum.amount,
        0,
    )
    assert call.to == selected.contracts.router and call.fund == selected.fund and call.value == 0
    encoded = operation.model_dump_json()
    assert TypeAdapter(ProtocolOperation).validate_json(encoded) == operation
    with pytest.raises(ValidationError):
        TypeAdapter(UniswapOperation).validate_json(encoded)


def test_shared_pool_types_do_not_erase_connector_or_callback_evidence():
    assert isinstance(target().pool, PancakePool)
    with pytest.raises(ValidationError):
        UniswapPool.model_validate_json(json.dumps(ROW["source"]))
    data = dict(ROW["observed"])
    del data["lm_pool"]
    with pytest.raises(ValidationError, match="lm_pool"):
        PoolObservation.model_validate_json(json.dumps(data))
    data = dict(ROW["observed"], protocol_fee=4 | (10 << 4))
    with pytest.raises(ValidationError, match="protocol fee"):
        PoolObservation.model_validate_json(json.dumps(data))


def test_pancake_swap_return_refuses_uniswap_router_multicall_envelope():
    from eth_abi import encode

    from almanak.connectors.pancakeswap_v3.simulation import validate_simulation
    from almanak.gateway.ethereum import DataError
    from almanak.models import AssetAmount

    selected = target()
    operation = Swap(
        target=selected,
        amount=AssetAmount(asset=selected.assets[0], amount=10**18),
        minimum=AssetAmount(asset=selected.assets[1], amount=3),
        deadline=ROW["block"]["timestamp"] + 60,
    )
    raw = encode(("uint256",), (3,))
    validate_simulation(operation, "0x" + raw.hex(), None)
    wrapped = encode(("bytes[]",), ((raw,),))
    with pytest.raises(DataError, match="malformed"):
        validate_simulation(operation, "0x" + wrapped.hex(), None)
    with pytest.raises(DataError, match="minimum"):
        validate_simulation(operation, "0x" + encode(("uint256",), (2,)).hex(), None)
