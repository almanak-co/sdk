"""Negative controls for receipt-derived exact-proof helper predicates."""

import json
from types import SimpleNamespace

import pytest
from hexbytes import HexBytes
from web3 import Web3

from almanak.connectors.aave_v3.receipt_parser import EVENT_TOPICS
from almanak.connectors.gmx_v2.addresses import GMX_V2
from almanak.framework.intents.vocabulary import IntentType
from tests.intents._aave_v3_exact_proofs import _target_transaction
from tests.intents._gmx_v2_perp_support import (
    _EVENT_EMITTER_SIGNATURE_BY_TOPIC_COUNT,
    assert_gmx_event_key,
)
from tests.intents._uniswap_v3_lp_exact_proofs import (
    _is_execution_revert,
    _PinnedCompileGateway,
    _terminal_position_evidence,
    _zero_minimum_quote_data,
)
from tests.intents.proofs.aave_v3_lending import _TRANSFER_TOPIC, _wallet_transfers


def test_terminal_absence_accepts_only_evm_reverts() -> None:
    assert _is_execution_revert({"error": {"code": -32000, "message": "execution reverted"}})
    assert not _is_execution_revert({"error": {"code": -32000, "message": "missing trie node"}})
    assert not _is_execution_revert({"error": {"code": -32602, "message": "invalid argument"}})


@pytest.mark.parametrize("replaced_hash", [False, True])
def test_lp_terminal_state_stays_at_burn_block_when_latest_advances(replaced_hash) -> None:
    requests = []
    burn_hash = HexBytes("0x" + "cc" * 32)
    revert = {"error": {"code": 3, "message": "execution reverted"}}

    def request(method, params):
        requests.append((method, params))
        assert params[1] == hex(126)
        return revert

    def header(block):
        if block == "latest":
            return {"number": 127, "hash": HexBytes("0x" + "dd" * 32)}
        assert block == 126
        return {"number": block, "hash": HexBytes("0x" + "ee" * 32) if replaced_hash else burn_hash}

    web3 = SimpleNamespace(provider=SimpleNamespace(make_request=request), eth=SimpleNamespace(get_block=header))
    args = {
        "position_manager": "0x" + "11" * 20,
        "position_id": 7,
        "burn_receipt": {"blockNumber": 126, "blockHash": burn_hash},
    }
    if replaced_hash:
        with pytest.raises(AssertionError, match="block hash differs"):
            _terminal_position_evidence(web3, **args)
    else:
        position, owner, block = _terminal_position_evidence(web3, **args)
        assert position == owner == revert
        assert block == {"number": 126, "hash": burn_hash}
    assert len(requests) == 2
    assert {params[0]["data"][:10] for _, params in requests} == {"0x99fbab88", "0x6352211e"}


def test_gmx_key_witness_rejects_a_forged_event_signature() -> None:
    key = "0x" + "22" * 32
    event_name = "OrderCreated"
    event_name_topic = Web3.to_hex(Web3.keccak(text=event_name))
    receipt = {
        "logs": [
            {
                "address": GMX_V2["arbitrum"]["event_emitter"],
                "topics": ["0x" + "ff" * 32, event_name_topic, key],
            }
        ]
    }

    try:
        assert_gmx_event_key(receipt, chain="arbitrum", event_name=event_name, key=key)
    except AssertionError:
        pass
    else:
        raise AssertionError("forged EventEmitter signature was accepted")

    receipt["logs"][0]["topics"][0] = _EVENT_EMITTER_SIGNATURE_BY_TOPIC_COUNT[3]
    witness = assert_gmx_event_key(receipt, chain="arbitrum", event_name=event_name, key=key)
    assert witness["matched_key"] == key
    assert witness["matched_event_name_topic"] == event_name_topic.lower()


def test_aave_transfer_witness_accepts_hexbytes_topics_and_data() -> None:
    wallet = "0x" + "33" * 20
    token = "0x" + "44" * 20
    amount = 123
    receipt = {
        "logs": [
            {
                "address": token,
                "topics": [
                    HexBytes(_TRANSFER_TOPIC),
                    HexBytes("0x" + "00" * 12 + "55" * 20),
                    HexBytes("0x" + "00" * 12 + "33" * 20),
                ],
                "data": HexBytes(amount.to_bytes(32, "big")),
            }
        ]
    }

    assert (
        len(_wallet_transfers(receipt=receipt, wallet=wallet, token_address=token, direction=1, amount_raw=amount)) == 1
    )


def test_aave_target_receipt_accepts_hexbytes_event_topic() -> None:
    receipt = SimpleNamespace(to_dict=lambda: {"logs": [{"topics": [HexBytes(EVENT_TOPICS["Supply"])]}]})
    transaction = SimpleNamespace(receipt=receipt)
    execution_result = SimpleNamespace(transaction_results=[transaction])

    assert _target_transaction(execution_result, IntentType.SUPPLY) is transaction


def _decrease_liquidity_call(amount0_min: int, amount1_min: int) -> dict:
    selector = Web3.keccak(text="decreaseLiquidity((uint256,uint128,uint256,uint256,uint256))")[:4]
    body = Web3().codec.encode(
        ["(uint256,uint128,uint256,uint256,uint256)"], [(1, 84795287261, amount0_min, amount1_min, 1_900_000_000)]
    )
    return {
        "to": "0x" + "11" * 20,
        "data": Web3.to_hex(selector + body),
        "value": 0,
        "tx_type": "lp_decrease_liquidity",
    }


def test_lp_quote_zeroes_only_minimums_in_direct_decrease_call() -> None:
    call = _decrease_liquidity_call(123, 456)
    quote = _zero_minimum_quote_data(call["data"])
    assert Web3().codec.decode(["(uint256,uint128,uint256,uint256,uint256)"], HexBytes(quote)[4:]) == (
        (1, 84795287261, 0, 0, 1_900_000_000),
    )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda data: data[:-64],
        lambda data: data + "0" * 64,
        lambda data: "0xdeadbeef" + data[10:],
        lambda data: data[:150] + "zz" + data[152:],
    ],
    ids=["truncated", "extra-word", "other-selector", "non-hex"],
)
def test_lp_quote_rejects_non_direct_decrease_layout(mutation) -> None:
    with pytest.raises(AssertionError, match="direct decreaseLiquidity calldata"):
        _zero_minimum_quote_data(mutation(_decrease_liquidity_call(123, 456)["data"]))


def test_lp_close_minimums_flag_is_red_on_zero_floors_and_green_when_a_leg_binds() -> None:
    from tests.intents._uniswap_v3_lp_exact_proofs import _decrease_minimums

    binds, witness = _decrease_minimums(_decrease_liquidity_call(0, 0))
    assert binds is False
    assert witness["outcome"] == "UNPROTECTED"
    assert (witness["amount0Min"], witness["amount1Min"]) == ("0", "0")

    binds, witness = _decrease_minimums(_decrease_liquidity_call(0, 999_000))
    assert binds is True
    assert witness["outcome"] == "PROTECTED"
    assert witness["amount1Min"] == "999000"


@pytest.mark.parametrize("latest_block", [127, 128])
def test_close_compile_view_pins_slot0_position_and_rpc_reads(latest_block) -> None:
    from almanak.gateway.proto import gateway_pb2

    observed = []
    position = [0] * 12
    position[7], position[10], position[11] = 100, 3, 4

    def eth_call(chain, to, data, block=None, **kwargs):
        observed.append(block)
        if data.startswith("0x99fbab88"):
            words = position if block == 126 else [0] * 12
        else:
            words = [2**96 if block == 126 else 2**97]
        return "0x" + "".join(f"{word:064x}" for word in words)

    def rpc_call(request, timeout=None):
        assert json.loads(request.params)[1] == hex(126)
        return SimpleNamespace(success=True, result='"0x01"')

    gateway = SimpleNamespace(
        is_connected=True, eth_call=eth_call, rpc=SimpleNamespace(Call=rpc_call), block_number=lambda _: latest_block
    )
    pinned = _PinnedCompileGateway(gateway, 126)
    assert pinned.block_number("arbitrum") == 126
    assert int(pinned.eth_call("arbitrum", "pool", "0x3850c7bd"), 16) == 2**96
    assert pinned.query_position_liquidity("arbitrum", "npm", 7) == 100
    owed = pinned.rpc.QueryPositionTokensOwed(SimpleNamespace(chain="arbitrum", position_manager="npm", token_id=7))
    assert (owed.tokens_owed0, owed.tokens_owed1) == ("3", "4")
    request = gateway_pb2.RpcRequest(chain="arbitrum", method="eth_call", params='[{"to":"pool"},"latest"]')
    assert pinned.rpc.Call(request).success
    assert json.loads(request.params)[1] == "latest"
    assert observed == [126, 126, 126]
    assert int(gateway.eth_call("arbitrum", "pool", "0x3850c7bd"), 16) == 2**97


def test_close_compile_view_refuses_disconnected_gateway() -> None:
    with pytest.raises(ValueError, match="not connected"):
        _PinnedCompileGateway(SimpleNamespace(is_connected=False), 126)


def test_close_compile_view_refuses_truncated_position_response() -> None:
    gateway = SimpleNamespace(is_connected=True, eth_call=lambda **_: "0x" + "00" * 32)
    with pytest.raises(ValueError, match="twelve ABI words"):
        _PinnedCompileGateway(gateway, 126).query_position_liquidity("arbitrum", "npm", 7)
