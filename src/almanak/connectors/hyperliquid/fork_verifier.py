"""The synthesized HyperCore's own check of an agent's signature (blueprint 30, section 9.8).

At run time the gateway never verifies its own signatures: HyperCore does on mainnet,
and on forks this verifier does. It is a second implementation of the gateway signer's
hashing, written apart from it: its own msgpack encoder for the values an action holds,
and `eth_account`'s EIP-712 typed-data encoder for the phantom agent. Both are held to
the official SDK's fixtures, so a fork run cannot pass on a signer and a verifier that
share one mistake.
"""

from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_utils import keccak

PHANTOM_TYPES = {
    "Agent": [
        {"name": "source", "type": "string"},
        {"name": "connectionId", "type": "bytes32"},
    ],
    "EIP712Domain": [
        {"name": "name", "type": "string"},
        {"name": "version", "type": "string"},
        {"name": "chainId", "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ],
}
PHANTOM_DOMAIN = {
    "chainId": 1337,
    "name": "Exchange",
    "verifyingContract": "0x0000000000000000000000000000000000000000",
    "version": "1",
}


def pack(value) -> bytes:
    """MessagePack of JSON values, in the smallest form, maps in key order."""
    if value is None:
        return b"\xc0"
    if isinstance(value, bool):
        return b"\xc3" if value else b"\xc2"
    if isinstance(value, int):
        return pack_int(value)
    if isinstance(value, str):
        return pack_str(value.encode())
    if isinstance(value, list):
        return container(len(value), 0x90, b"\xdc", b"\xdd") + b"".join(map(pack, value))
    if isinstance(value, dict):
        body = b"".join(pack(k) + pack(v) for k, v in value.items())
        return container(len(value), 0x80, b"\xde", b"\xdf") + body
    raise TypeError(f"an action holds no {type(value).__name__}")


def pack_int(value: int) -> bytes:
    if 0 <= value < 0x80:
        return bytes([value])
    if -32 <= value < 0:
        return (value & 0xFF).to_bytes(1, "big")
    unsigned = ((0xCC, 1), (0xCD, 2), (0xCE, 4), (0xCF, 8))
    signed = ((0xD0, 1), (0xD1, 2), (0xD2, 4), (0xD3, 8))
    for marker, size in unsigned if value >= 0 else signed:
        try:
            return bytes([marker]) + value.to_bytes(size, "big", signed=value < 0)
        except OverflowError:
            continue
    raise OverflowError("an action integer exceeds 64 bits")


def pack_str(raw: bytes) -> bytes:
    if len(raw) < 32:
        return bytes([0xA0 | len(raw)]) + raw
    for marker, size in ((0xD9, 1), (0xDA, 2), (0xDB, 4)):
        if len(raw) < 1 << (8 * size):
            return bytes([marker]) + len(raw).to_bytes(size, "big") + raw
    raise OverflowError("an action string is too long")


def container(length: int, fixed: int, short: bytes, long: bytes) -> bytes:
    if length < 16:
        return bytes([fixed | length])
    if length < 1 << 16:
        return short + length.to_bytes(2, "big")
    return long + length.to_bytes(4, "big")


def connection_id(action: dict, nonce: int, expires_after: int | None) -> bytes:
    data = pack(action) + nonce.to_bytes(8, "big") + b"\x00"
    if expires_after is not None:
        data += b"\x00" + expires_after.to_bytes(8, "big")
    return keccak(data)


def signer_of(action: dict, nonce: int, expires_after: int | None, signature: dict) -> str:
    """The address whose key signed the mainnet phantom agent of this action."""
    message = encode_typed_data(
        full_message={
            "domain": PHANTOM_DOMAIN,
            "types": PHANTOM_TYPES,
            "primaryType": "Agent",
            "message": {
                "source": "a",
                "connectionId": connection_id(action, nonce, expires_after),
            },
        }
    )
    vrs = (signature["v"], int(signature["r"], 16), int(signature["s"], 16))
    return Account.recover_message(message, vrs=vrs).lower()
