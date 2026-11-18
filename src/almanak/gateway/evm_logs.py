"""Canonical EVM event decoding without connector-specific effect assumptions."""

from eth_abi import decode, encode

from almanak.gateway.ethereum import DataError


def topic_address(topic):
    raw = topic_bytes(topic)
    if any(raw[:12]):
        raise DataError("receipt", "event address has noncanonical ABI padding")
    return "0x" + raw[12:].hex()


def topic_bytes(topic):
    try:
        if not isinstance(topic, str) or not topic.startswith("0x") or len(topic) != 66:
            raise ValueError("topic length")
        raw = bytes.fromhex(topic[2:])
        if len(raw) != 32:
            raise ValueError("topic bytes")
    except ValueError:
        raise DataError("receipt", "event topic is not a canonical ABI word") from None
    return raw


def topic_uint(topic):
    return int.from_bytes(topic_bytes(topic))


def events(receipt, emitter, topic):
    result = []
    for event in receipt["logs"]:
        if event["address"].lower() != emitter:
            continue
        if not event["topics"]:
            raise DataError("receipt", "expected contract emitted an unidentifiable event")
        if event["topics"][0] == topic:
            result.append(event)
    return result


def one_event(receipt, emitter, topic, topics):
    matches = events(receipt, emitter, topic)
    if len(matches) != 1 or len(matches[0]["topics"]) != topics:
        raise DataError("receipt", "expected exactly one attributed protocol event")
    return matches[0]


def data(event, types):
    try:
        raw = bytes.fromhex(event["data"][2:])
        values = decode(types, raw)
        if encode(types, values) != raw:
            raise ValueError("noncanonical ABI data")
    except Exception:
        raise DataError("receipt", "event data is malformed or noncanonical") from None
    return values
