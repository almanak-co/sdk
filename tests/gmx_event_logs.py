"""Synthetic GMX EventEmitter logs for offline decoding and refusal checks.

Every value is invented; the logs only follow the deployed envelope shape that
almanak.connectors.gmx_v2.events decodes.
"""

from eth_abi import encode
from eth_utils import keccak

from almanak.connectors.gmx_v2.events import EVENT_DATA, EVENT_SIGNATURES, VALUE_TYPES

TRANSACTION = "0x" + "a1" * 32
BLOCK_HASH = "0x" + "b2" * 32


def word(address):
    return "0x" + address[2:].rjust(64, "0")


def raw(kind, value):
    return bytes.fromhex(value[2:]) if kind == "bytes32" else value


def event_log(
    name,
    *,
    emitter,
    sender,
    topics=(),
    values=None,
    arrays=None,
    transaction=TRANSACTION,
    block_hash=BLOCK_HASH,
    number=1,
    index=0,
):
    """One EventLog{0,1,2} envelope with `values` and `arrays` keyed by ABI kind."""
    groups = []
    for kind in VALUE_TYPES:
        items = tuple(
            (key, raw(kind, value)) for key, value in (values or {}).get(kind, {}).items()
        )
        lists = tuple(
            (key, tuple(raw(kind, value) for value in entries))
            for key, entries in (arrays or {}).get(kind, {}).items()
        )
        groups.append((items, lists))
    signature = next(value for value, count in EVENT_SIGNATURES.items() if count == len(topics))
    return {
        "address": emitter,
        "topics": [signature, "0x" + keccak(text=name).hex(), *topics],
        "data": "0x" + encode(["address", "string", EVENT_DATA], [sender, name, groups]).hex(),
        "transactionHash": transaction,
        "blockHash": block_hash,
        "blockNumber": hex(number),
        "logIndex": hex(index),
        "transactionIndex": "0x0",
        "removed": False,
    }
