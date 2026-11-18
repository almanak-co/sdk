"""Bounded, log-safe descriptions of EVM revert data.

Revert data is the reverting contract's own output, not provider text, so it cannot echo
an endpoint or its credentials. It is still untrusted: a reason is decoded only from the
standard encodings, reduced to printable ASCII and truncated before it reaches a message.
"""

import re

from eth_abi import decode
from eth_utils import keccak

# Revert data longer than this is not retained; a standard reason fits well inside it.
MAX_REVERT_BYTES = 4096
# Characters of a decoded Error(string) reason kept in a message.
MAX_REASON_CHARS = 160

ERROR_STRING = bytes.fromhex("08c379a0")
PANIC = bytes.fromhex("4e487b71")
# Solidity's panic codes, from the language documentation.
PANIC_CODES = {
    0x00: "generic compiler panic",
    0x01: "assertion failed",
    0x11: "arithmetic overflow or underflow",
    0x12: "division or modulo by zero",
    0x21: "invalid enum conversion",
    0x22: "invalid storage byte array",
    0x31: "pop on empty array",
    0x32: "array index out of bounds",
    0x41: "memory allocation too large",
    0x51: "call to an uninitialized function",
}
# Roles reverts with this when a permitted inner transaction fails; the inner reason is
# only available from a non-reverting diagnostic call.
MODULE_TRANSACTION_FAILED = keccak(text="ModuleTransactionFailed()")[:4]
KNOWN_ERRORS = {MODULE_TRANSACTION_FAILED: "ModuleTransactionFailed()"}

HEX_DATA = re.compile(r"0x(?:[0-9a-fA-F]{2})*")
UNPRINTABLE = re.compile(r"[^\x20-\x7e]")


def revert_data(error: object) -> bytes | None:
    """The revert bytes a JSON-RPC error carries, or None when absent or out of bounds."""
    data = error.get("data") if isinstance(error, dict) else None
    if isinstance(data, dict):
        # Some providers nest the bytes one level deeper.
        data = data.get("data")
    if not isinstance(data, str) or len(data) > 2 + 2 * MAX_REVERT_BYTES:
        return None
    if HEX_DATA.fullmatch(data) is None:
        return None
    return bytes.fromhex(data[2:])


def is_revert(error: object) -> bool:
    """Whether a JSON-RPC error reports EVM execution reverting.

    Code 3 is the execution-error code with revert data. Some nodes answer a revert without
    data as -32000 "execution reverted"; the message is compared, never repeated.
    """
    if not isinstance(error, dict):
        return False
    code, message = error.get("code"), error.get("message")
    if type(code) is int and code == 3:
        return True
    return (
        type(code) is int
        and code == -32000
        and isinstance(message, str)
        and message.lower().startswith("execution reverted")
    )


def describe(data: bytes | None) -> str:
    """A short, printable description of revert data in its standard encodings."""
    if not data:
        return "no revert data"
    if len(data) < 4:
        return "malformed revert data"
    selector, body = data[:4], data[4:]
    if selector == ERROR_STRING:
        try:
            (text,) = decode(("string",), body)
        except Exception:  # noqa: BLE001 - any decoding failure is described, never raised
            return "Error(string) with malformed data"
        printable = UNPRINTABLE.sub("?", text)
        if len(printable) > MAX_REASON_CHARS:
            printable = printable[:MAX_REASON_CHARS] + "..."
        return f'Error("{printable}")'
    if selector == PANIC:
        try:
            (code,) = decode(("uint256",), body)
        except Exception:  # noqa: BLE001
            return "Panic(uint256) with malformed data"
        meaning = PANIC_CODES.get(code, "unknown panic code")
        return f"Panic(0x{code:x}: {meaning})" if code < 2**32 else "Panic(unknown panic code)"
    name = KNOWN_ERRORS.get(selector)
    if name is not None:
        return name
    return f"custom error 0x{selector.hex()}"
