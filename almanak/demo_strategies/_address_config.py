"""Shared address-first configuration guard for packaged demo strategies."""

from __future__ import annotations

import re
from typing import Any

_EVM_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")


def require_evm_address(strategy: Any, key: str) -> str:
    """Return one explicitly configured EVM address or fail at construction."""
    value = strategy.get_config(key, None)
    if not isinstance(value, str) or not _EVM_ADDRESS_RE.fullmatch(value.strip()):
        raise ValueError(
            f"{type(strategy).__name__} requires config {key!r} to be a chain-specific EVM address"
        )
    return value.strip()
