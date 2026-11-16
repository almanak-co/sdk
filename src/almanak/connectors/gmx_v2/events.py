"""Decode GMX EventEmitter envelopes without trusting a matching event name alone."""

from eth_abi import decode, encode
from eth_utils import keccak

from almanak.models import Address, Hash32, Model, UInt

VALUE_TYPES = ("address", "uint256", "int256", "bool", "bytes32", "bytes", "string")
EVENT_DATA = "(" + ",".join(f"((string,{t})[],(string,{t}[])[])" for t in VALUE_TYPES) + ")"
EVENT_SIGNATURES = {
    "0x" + keccak(text=f"{name}(address,string,string{suffix},{EVENT_DATA})").hex(): count
    for name, suffix, count in (
        ("EventLog", "", 0),
        ("EventLog1", ",bytes32", 1),
        ("EventLog2", ",bytes32,bytes32", 2),
    )
}


Scalar = int | str | bool
KeyValue = tuple[str, Scalar]
ArrayKeyValue = tuple[str, tuple[Scalar, ...]]
Group = tuple[tuple[KeyValue, ...], tuple[ArrayKeyValue, ...]]


class Event(Model):
    emitter: Address
    sender: Address
    name: str
    topics: tuple[Hash32, ...]
    transaction_hash: Hash32
    block_hash: Hash32
    block_number: UInt
    log_index: UInt
    # Preserve ABI groups and arrays; key names alone do not define their units or type.
    values: tuple[Group, ...]

    def value(self, kind: str, key: str):
        group = self.values[VALUE_TYPES.index(kind)][0]
        matches = [value for name, value in group if name == key]
        if len(matches) != 1:
            raise ValueError(f"GMX event requires exactly one {kind} field {key}")
        return matches[0]

    def array(self, kind: str, key: str):
        group = self.values[VALUE_TYPES.index(kind)][1]
        matches = [value for name, value in group if name == key]
        if len(matches) != 1:
            raise ValueError(f"GMX event requires exactly one {kind} array field {key}")
        return matches[0]


def decode_event(log: dict, *, emitter: str, sender: str | None = None) -> Event | None:
    if log.get("address", "").lower() != emitter.lower():
        return None
    topics = log.get("topics", ())
    count = EVENT_SIGNATURES.get(topics[0]) if topics else None
    if count is None:
        return None
    if len(topics) != count + 2 or log.get("removed", False):
        raise ValueError("GMX event has invalid topics or removed canonical evidence")
    raw = bytes.fromhex(log["data"][2:])
    types = ["address", "string", EVENT_DATA]
    emitting_contract, name, values = decode(types, raw)
    if encode(types, (emitting_contract, name, values)) != raw:
        raise ValueError("GMX event must retain its canonical ABI envelope")
    if topics[1] != "0x" + keccak(text=name).hex():
        raise ValueError("GMX event name differs from its indexed identity")
    if sender is not None and emitting_contract.lower() != sender.lower():
        raise ValueError("GMX event came from a different emitting contract")
    return Event(
        emitter=emitter,
        sender=emitting_contract,
        name=name,
        topics=tuple(topics[2:]),
        transaction_hash=log["transactionHash"],
        block_hash=log["blockHash"],
        block_number=int(log["blockNumber"], 16),
        log_index=int(log["logIndex"], 16),
        values=tuple(normalize_group(group) for group in values),
    )


def normalize_group(group):
    items, arrays = group
    return (
        tuple((key, normalize_value(value)) for key, value in items),
        tuple((key, tuple(normalize_value(value) for value in values)) for key, values in arrays),
    )


def normalize_value(value):
    return "0x" + value.hex() if isinstance(value, bytes) else value
