"""The source-selected router adds a fixed field to its V4 single-hop tuple."""

import pytest
from eth_abi import decode, encode
from eth_abi.exceptions import InvalidPointer

from almanak.connectors.uniswap_v4 import abi, permissions
from almanak.gateway.roles_conditions import equal
from almanak.models import AssetAmount
from tests.uniswap_support import live_state


def robinhood_capability():
    original = live_state("trading").capabilities.requirements[0]
    payload = original.model_dump_json().replace("eip155:31337", "eip155:4663")
    payload = payload.replace("eip155:1", "eip155:4663")
    capability = type(original).model_validate_json(payload)
    target = capability.target
    contracts = target.contracts.model_copy(
        update={"router": "0x8876789976decbfcbbbe364623c63652db8c0904"}
    )
    return capability.model_copy(
        update={"target": target.model_copy(update={"contracts": contracts})}
    )


def test_hop_price_revision_retains_existing_minimum_and_empty_hook_data():
    target = robinhood_capability().target
    payload = abi.swap_parameters(target, True, 123, 99)
    values = decode((abi.HOP_PRICE_SWAP_PARAMS,), payload)[0]
    assert values == (abi.pool_key(target.pool), True, 123, 99, 0, b"")
    with pytest.raises(InvalidPointer):
        decode((abi.SWAP_PARAMS,), payload)
    old = encode((abi.SWAP_PARAMS,), ((abi.pool_key(target.pool), True, 123, 99, b""),))
    with pytest.raises(InvalidPointer):
        decode((abi.HOP_PRICE_SWAP_PARAMS,), old)


def test_robinhood_refuses_a_different_router_revision():
    target = robinhood_capability().target
    target = target.model_copy(
        update={
            "contracts": target.contracts.model_copy(update={"router": target.contracts.quoter})
        }
    )
    with pytest.raises(ValueError, match="source-bound router revision"):
        abi.swap_parameters(target, True, 123, 99)


def test_generated_roles_freezes_new_hop_price_word_and_abi_layout():
    target = robinhood_capability().target
    grant = permissions.swap_grant(target, True)
    call = abi.swap(target, AssetAmount(asset=target.pool.currency0, amount=123), 99, 1)
    _, inputs, _ = decode(("bytes", "bytes[]", "uint256"), bytes.fromhex(call.data[10:]))
    actions, params = decode(("bytes", "bytes[]"), inputs[0])
    values = list(decode((abi.HOP_PRICE_SWAP_PARAMS,), params[0])[0])
    values[4] = 1
    changed = encode((abi.HOP_PRICE_SWAP_PARAMS,), (tuple(values),))
    altered = encode(("bytes", "bytes[]"), (actions, (changed, *params[1:])))
    differences = [
        i // 32 for i in range(0, len(altered), 32) if altered[i : i + 32] != inputs[0][i : i + 32]
    ]
    assert len(differences) == 1
    words = grant.condition.children[1].children[0].children
    assert words[differences[0]] == equal("uint256", 0)
    assert len(grant.conditions) <= 256
