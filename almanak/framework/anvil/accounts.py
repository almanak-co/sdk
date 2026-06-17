"""Deterministic Anvil account helpers."""

from __future__ import annotations

from functools import cache
from typing import Final

from eth_account import Account

_ANVIL_DEFAULT_PRIVATE_KEYS: Final[tuple[str, ...]] = (
    "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80",  # gitleaks:allow
    "0x59c6995e998f97a5a0044966f0945389dc9e86dae88c7a8412f4603b6b78690d",  # gitleaks:allow
)


def anvil_default_private_key(index: int = 0) -> str:
    """Return the deterministic private key for an Anvil default account."""
    if index < 0:
        raise ValueError(f"No Anvil default account configured for index {index}")
    try:
        return _ANVIL_DEFAULT_PRIVATE_KEYS[index]
    except IndexError as exc:
        raise ValueError(f"No Anvil default account configured for index {index}") from exc


@cache
def anvil_default_address(index: int = 0) -> str:
    """Derive the checksummed public address for an Anvil default account."""
    return Account.from_key(anvil_default_private_key(index)).address


def synthetic_evm_address(seed: int) -> str:
    """Return a deterministic syntactically valid EVM address for tests."""
    if seed <= 0:
        raise ValueError("Synthetic EVM address seed must be positive")
    if seed >= 1 << 160:
        raise ValueError("Synthetic EVM address seed must fit in 160 bits")
    return f"0x{seed:040x}"


ANVIL_DEFAULT_PRIVATE_KEY: Final[str] = anvil_default_private_key()
ANVIL_DEFAULT_ADDRESS: Final[str] = anvil_default_address()

__all__ = [
    "ANVIL_DEFAULT_ADDRESS",
    "ANVIL_DEFAULT_PRIVATE_KEY",
    "anvil_default_address",
    "anvil_default_private_key",
    "synthetic_evm_address",
]
