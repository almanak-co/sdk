"""Chainlink AggregatorV3 ABI selectors and strict decoders."""

from __future__ import annotations

from dataclasses import dataclass

LATEST_ROUND_DATA_SELECTOR = "0xfeaf968c"
GET_ROUND_DATA_SELECTOR = "0x9a6fc8f5"
DECIMALS_SELECTOR = "0x313ce567"


@dataclass(frozen=True)
class RoundData:
    round_id: int
    answer: int
    started_at: int
    updated_at: int
    answered_in_round: int


def encode_get_round_data(round_id: int) -> str:
    """Encode ``getRoundData(uint80)`` calldata."""
    if round_id < 0 or round_id >= 1 << 80:
        raise ValueError(f"Chainlink round_id must fit uint80, got {round_id}")
    return GET_ROUND_DATA_SELECTOR + round_id.to_bytes(32, byteorder="big").hex()


def decode_round_data(value: str | bytes) -> RoundData:
    """Decode and validate the five-word AggregatorV3 round tuple."""
    if isinstance(value, str):
        try:
            raw = bytes.fromhex(value.removeprefix("0x"))
        except ValueError as exc:
            raise ValueError("Malformed Chainlink round response hex") from exc
    else:
        raw = value
    if len(raw) < 160:
        raise ValueError(f"Chainlink round response is {len(raw)} bytes; expected at least 160")
    words = [raw[index : index + 32] for index in range(0, 160, 32)]
    return RoundData(
        round_id=int.from_bytes(words[0], "big"),
        answer=int.from_bytes(words[1], "big", signed=True),
        started_at=int.from_bytes(words[2], "big"),
        updated_at=int.from_bytes(words[3], "big"),
        answered_in_round=int.from_bytes(words[4], "big"),
    )


def decode_uint8(value: str | bytes) -> int:
    """Decode one ABI uint8 word, rejecting malformed or out-of-range data."""
    if isinstance(value, str):
        try:
            raw = bytes.fromhex(value.removeprefix("0x"))
        except ValueError as exc:
            raise ValueError("Malformed Chainlink uint8 response hex") from exc
    else:
        raw = value
    if len(raw) < 32:
        raise ValueError(f"Chainlink uint8 response is {len(raw)} bytes; expected at least 32")
    decoded = int.from_bytes(raw[-32:], "big")
    if decoded > 255:
        raise ValueError(f"Chainlink uint8 response is out of range: {decoded}")
    return decoded
