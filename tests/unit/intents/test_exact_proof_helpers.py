"""Negative controls for receipt-derived exact-proof helper predicates."""

from types import SimpleNamespace

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
from tests.intents._uniswap_v3_lp_exact_proofs import _is_execution_revert
from tests.intents.proofs.aave_v3_lending import _TRANSFER_TOPIC, _wallet_transfers


def test_terminal_absence_accepts_only_evm_reverts() -> None:
    assert _is_execution_revert({"error": {"code": -32000, "message": "execution reverted"}})
    assert not _is_execution_revert({"error": {"code": -32000, "message": "missing trie node"}})
    assert not _is_execution_revert({"error": {"code": -32602, "message": "invalid argument"}})


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
