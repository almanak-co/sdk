"""Chain-aware address normalization for identity comparisons.

This module deliberately normalizes casing only. Address syntax validation,
byte coercion, zero-address policy, and boundary-specific sentinels remain the
responsibility of callers that own those contracts.
"""

from __future__ import annotations

from almanak.core.chains import ChainRegistry
from almanak.core.enums import ChainFamily


def normalize_address(address: str, chain: str) -> str:
    """Return the canonical comparison form of ``address`` on ``chain``.

    EVM addresses are case-insensitive and normalize to lowercase. Solana
    base58 addresses are case-sensitive and preserve their casing. Unknown
    chains retain the historical non-Solana fallback and normalize as EVM.

    This function does not validate address syntax or add an address prefix.
    """
    if not isinstance(address, str):
        raise TypeError("Address must be a string")

    normalized = address.strip()
    descriptor = ChainRegistry.try_resolve(str(chain))
    if descriptor is not None and descriptor.family is ChainFamily.SOLANA:
        return normalized
    return normalized.lower()
