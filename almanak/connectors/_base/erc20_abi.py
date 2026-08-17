"""Cross-boundary, dependency-free ERC-20 ABI primitives.

This module is the canonical home for ERC-20 selectors, the Transfer topic,
and the small fixed-width encoders used by both strategy- and gateway-side
code.  It deliberately depends only on the standard library so importing a
selector from permission discovery or the gateway cannot pull in Web3 or the
strategy connector graph.
"""

from __future__ import annotations

from typing import Final

ERC20_APPROVE_SELECTOR: Final[str] = "0x095ea7b3"
ERC20_ALLOWANCE_SELECTOR: Final[str] = "0xdd62ed3e"
ERC20_BALANCE_OF_SELECTOR: Final[str] = "0x70a08231"
ERC20_TRANSFER_TOPIC: Final[str] = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
MAX_UINT256: Final[int] = (1 << 256) - 1

_HEX_DIGITS: Final[frozenset[str]] = frozenset("0123456789abcdef")


def pad_address(address: str) -> str:
    """Encode a 20-byte EVM address as one ABI word, without ``0x``.

    Both canonical ``0x``-prefixed addresses and bare 40-hex-character
    payloads are accepted.  Anything else is rejected rather than silently
    zero-padding a different address.
    """
    if not isinstance(address, str):
        raise TypeError(f"address must be a string, got {type(address).__name__}")
    raw = address[2:] if address[:2].lower() == "0x" else address
    raw = raw.lower()
    if len(raw) != 40 or any(char not in _HEX_DIGITS for char in raw):
        raise ValueError(f"address must be 20 bytes (40 hex characters), got {address!r}")
    return raw.zfill(64)


def pad_uint256(value: int) -> str:
    """Encode an unsigned 256-bit integer as one ABI word, without ``0x``."""
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"uint256 value must be an int, got {type(value).__name__}")
    if value < 0 or value > MAX_UINT256:
        raise ValueError(f"uint256 value must be in [0, 2**256), got {value}")
    return value.to_bytes(32, "big").hex()


def encode_approve(spender: str, amount: int) -> str:
    """Encode ``approve(address,uint256)`` calldata."""
    return ERC20_APPROVE_SELECTOR + pad_address(spender) + pad_uint256(amount)


def encode_allowance(owner: str, spender: str) -> str:
    """Encode ``allowance(address,address)`` calldata."""
    return ERC20_ALLOWANCE_SELECTOR + pad_address(owner) + pad_address(spender)


def encode_balance_of(account: str) -> str:
    """Encode ``balanceOf(address)`` calldata."""
    return ERC20_BALANCE_OF_SELECTOR + pad_address(account)


__all__ = [
    "ERC20_ALLOWANCE_SELECTOR",
    "ERC20_APPROVE_SELECTOR",
    "ERC20_BALANCE_OF_SELECTOR",
    "ERC20_TRANSFER_TOPIC",
    "MAX_UINT256",
    "encode_allowance",
    "encode_approve",
    "encode_balance_of",
    "pad_address",
    "pad_uint256",
]
