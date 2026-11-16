"""Request identity and emitting authority are independent of an event's name."""

import pytest
from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.gmx_v2.events import EVENT_DATA, decode_event

EMITTER = "0x" + "11" * 20
HANDLER = "0x" + "22" * 20
KEY = "0x" + "33" * 32


def log():
    name = "OrderExecuted"
    values = (((), ()),) * 7
    return {
        "address": EMITTER,
        "topics": [
            "0x" + keccak(text=f"EventLog1(address,string,string,bytes32,{EVENT_DATA})").hex(),
            "0x" + keccak(text=name).hex(),
            KEY,
        ],
        "data": "0x" + encode(["address", "string", EVENT_DATA], [HANDLER, name, values]).hex(),
        "transactionHash": "0x" + "44" * 32,
        "blockHash": "0x" + "55" * 32,
        "blockNumber": "0x10",
        "logIndex": "0x3",
    }


def test_matching_name_does_not_authorize_another_handler():
    event = decode_event(log(), emitter=EMITTER, sender=HANDLER)
    assert event is not None and event.topics == (KEY,) and event.block_number == 16
    assert decode_event(log(), emitter=HANDLER) is None
    with pytest.raises(ValueError, match="different emitting contract"):
        decode_event(log(), emitter=EMITTER, sender=EMITTER)


@pytest.mark.parametrize("change", ["removed", "topics", "name"])
def test_refuses_noncanonical_or_inconsistent_event(change):
    item = log()
    if change == "removed":
        item["removed"] = True
    elif change == "topics":
        item["topics"].append(KEY)
    else:
        item["topics"][1] = KEY
    with pytest.raises(ValueError):
        decode_event(item, emitter=EMITTER)


def test_missing_field_is_not_measured_zero():
    event = decode_event(log(), emitter=EMITTER)
    assert event is not None
    with pytest.raises(ValueError, match="exactly one"):
        event.value("uint256", "executionFee")


def test_retained_event_json_keeps_bytes_and_array_group_types():
    from almanak.connectors.gmx_v2.events import Event

    item = log()
    groups = list((((), ()),) * 7)
    groups[4] = ((("key", bytes.fromhex(KEY[2:])),), (("keys", (bytes(32),)),))
    item["data"] = (
        "0x" + encode(["address", "string", EVENT_DATA], [HANDLER, "OrderExecuted", groups]).hex()
    )
    event = decode_event(item, emitter=EMITTER)
    assert event is not None and event.value("bytes32", "key") == KEY
    assert Event.model_validate_json(event.model_dump_json()) == event


def test_receipt_envelope_refuses_trailing_bytes():
    item = log()
    item["data"] += "00" * 32
    with pytest.raises(ValueError, match="canonical ABI"):
        decode_event(item, emitter=EMITTER)
