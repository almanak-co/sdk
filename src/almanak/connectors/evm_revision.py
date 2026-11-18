"""Exact EVM runtime comparison against an explicit compiled revision."""

from eth_utils import keccak

from almanak.gateway.ethereum import DataError
from almanak.gateway.permission_models import ContractIdentity


def immutable_words(code, slots):
    values = {}
    normalized = bytearray(code)
    for name, references in slots.items():
        observed = set()
        for reference in references:
            start, length = reference["start"], reference["length"]
            observed.add(int.from_bytes(code[start : start + length]))
            normalized[start : start + length] = b"\0" * length
        if len(observed) != 1:
            raise DataError("revision", "constructor immutable occurrences disagree")
        values[name] = observed.pop()
    return normalized, values


def verify_code(raw, expected):
    try:
        code = bytes.fromhex(raw[2:]) if raw.startswith("0x") else b""
    except (ValueError, AttributeError):
        raise DataError("revision", "contract runtime code is malformed") from None
    if len(code) != expected["runtime_length"]:
        raise DataError("revision", "contract runtime length differs from the compiled revision")
    normalized, values = immutable_words(code, expected["immutables"])
    if "0x" + keccak(normalized).hex() != expected["normalized_code_hash"]:
        raise DataError("revision", "contract code differs from the compiled V3 revision")
    return values


def read_code(reader, address, block, expected, role="revision"):
    _, values, identity = read_matching_code(reader, address, block, ((role, expected),))
    return values, identity


def describe(block):
    """Name a block by chain, number and hash, so errors say where a pin came from.

    Equal heights with different hashes also mismatch, so the hash is part of the name.
    """
    if block is None:
        return "no block"
    return f"{block.chain.chain_id} block {block.number} ({block.hash})"


def matching_revision(raw, candidates):
    if not candidates:
        raise ValueError("runtime comparison requires explicit compiled revisions")
    last = None
    for name, expected in candidates:
        try:
            return name, verify_code(raw, expected)
        except DataError as error:
            last = error
    assert last is not None
    raise last


def read_matching_code(reader, address, block, candidates):
    if block.chain != reader.chain or block != reader._pinned:
        roles = "/".join(name for name, _ in candidates)
        raise DataError(
            "block_mismatch",
            f"revision checks require the reader's pinned block: {roles} {address} "
            f"checked at {describe(block)}, reader pinned {describe(reader._pinned)}",
        )
    raw = reader._rpc("eth_getCode", [address, {"blockHash": block.hash, "requireCanonical": True}])
    name, values = matching_revision(raw, candidates)
    identity = ContractIdentity(
        address=address, code_hash="0x" + keccak(bytes.fromhex(raw[2:])).hex()
    )
    return name, values, identity
