"""Multicall3 ``aggregate3`` encode/decode for batched agent-tool reads (VIB-4951).

Pure encoding utilities — no network I/O. The executor owns the egress: it
probes availability with ``eth_getCode`` (Multicall3 is deployed at the same
CREATE2 address on 250+ chains, but NOT literally all, and known bad
deployments exist — never assume; verify) and falls back to the bounded
serial path when the contract is absent or a batch fails.

``aggregate3((address,bool,bytes)[])`` with ``allowFailure=true`` preserves
the tool's per-reserve fail-open contract: a reverted inner call comes back
as ``(success=false, returnData=0x)`` for just that row.
"""

from __future__ import annotations

from eth_abi import decode as _abi_decode
from eth_abi import encode as _abi_encode
from eth_utils import function_signature_to_4byte_selector

__all__ = [
    "MULTICALL3_ADDRESS",
    "MULTICALL3_MAX_BATCH",
    "decode_aggregate3",
    "encode_aggregate3",
]

# Canonical CREATE2 deployment address — re-exported from core (the home for
# chain-agnostic infra addresses, beside ETH_ADDRESS). Presence on a given
# chain MUST be verified via eth_getCode before use.
from almanak.core.constants import MULTICALL3_ADDRESS

# Bounded batching: a single aggregate3 over hundreds of calls can blow
# eth_call gas / payload / provider limits. Large markets are chunked; a
# normal full market (<= ~40 reserves) fits in one batch.
MULTICALL3_MAX_BATCH = 80

_AGGREGATE3_SELECTOR = "0x" + function_signature_to_4byte_selector("aggregate3((address,bool,bytes)[])").hex()


def encode_aggregate3(calls: list[tuple[str, str]]) -> str:
    """Encode ``aggregate3`` calldata for ``[(to, data_hex), ...]`` with
    ``allowFailure=true`` on every inner call."""
    tuples = [
        (bytes.fromhex(to.removeprefix("0x").rjust(40, "0")), True, bytes.fromhex(data.removeprefix("0x")))
        for to, data in calls
    ]
    encoded = _abi_encode(["(address,bool,bytes)[]"], [tuples])
    return _AGGREGATE3_SELECTOR + encoded.hex()


def decode_aggregate3(raw_hex: str, expected: int) -> list[tuple[bool, str]] | None:
    """Decode an ``aggregate3`` return blob into ``[(success, result_hex), ...]``.

    ``result_hex`` carries no ``0x`` prefix (matching the executor's
    ``_rpc_call`` result convention). Returns ``None`` when the blob does not
    decode to exactly ``expected`` results — the caller falls back to the
    serial path rather than fabricating rows (fail-closed on shape).
    """
    raw = raw_hex.removeprefix("0x")
    try:
        (results,) = _abi_decode(["(bool,bytes)[]"], bytes.fromhex(raw))
    except Exception:  # noqa: BLE001 — any malformed blob means "not aggregate3 output"
        return None
    if len(results) != expected:
        return None
    return [(bool(ok), data.hex()) for ok, data in results]
