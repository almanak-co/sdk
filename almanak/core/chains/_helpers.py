"""Shared per-chain lookup helpers backed by ``ChainRegistry``.

These small wrappers exist so layers above ``almanak.core.chains`` (``config``,
``framework.execution``, ...) can read per-chain knobs from a single place
without re-implementing the registry-resolve-or-default dance, and without
``almanak.config`` having to import from ``almanak.framework.execution``
(the import would invert the canonical layer order — config sits below
framework).

VIB-4857 (W5).
"""

from __future__ import annotations

from almanak.core.chains._registry import ChainRegistry

# Default receipt-confirmation timeout (seconds) used when the per-chain
# descriptor has no entry. Mirrors the legacy
# ``CHAIN_RECEIPT_TIMEOUTS.get(chain, DEFAULT_RECEIPT_TIMEOUT)`` shape
# byte-for-byte (VIB-4857).
DEFAULT_RECEIPT_TIMEOUT: int = 120


def receipt_timeout_for(chain: str) -> int:
    """Return the per-chain receipt-polling timeout (seconds).

    Per-chain overrides live on
    ``ChainDescriptor.timeouts.receipt_polling`` (mirrors the legacy
    ``CHAIN_RECEIPT_TIMEOUTS`` dict). ``None`` / unknown chain falls
    back to :data:`DEFAULT_RECEIPT_TIMEOUT` — matches the legacy
    ``CHAIN_RECEIPT_TIMEOUTS.get(chain, DEFAULT_RECEIPT_TIMEOUT)`` shape
    byte-for-byte. VIB-4857 (W5).
    """
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None or descriptor.timeouts.receipt_polling is None:
        return DEFAULT_RECEIPT_TIMEOUT
    return descriptor.timeouts.receipt_polling


def native_symbols_for(chain: str) -> frozenset[str]:
    """Return the set of symbols that denote ``chain``'s native gas coin.

    Derived from the single source of truth ``ChainDescriptor.native`` as
    ``{symbol, *accepted_symbols}`` (e.g. ``polygon -> {"MATIC", "POL"}``).
    An unknown / unregistered chain returns an **empty** frozenset — the lookup
    fails CLOSED so callers fall through to the ERC-20 / non-native path rather
    than mis-routing to a native-balance read (the VIB-3137 contract). This is
    the registry-derived replacement for the per-chain ``NATIVE_SYMBOLS_BY_CHAIN``
    / ``_CHAIN_NATIVE_SYMBOLS`` matrices (VIB-4851 A1). Alias-normalises via
    ``ChainRegistry.try_resolve`` so ``native_symbols_for("bnb") == {"BNB"}``.
    """
    if not chain:
        return frozenset()
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None:
        return frozenset()
    native = descriptor.native
    # Upper-case so membership holds against the `token.upper()` consumers even
    # if a descriptor ever defines a symbol in mixed case (defensive — all current
    # descriptors are already upper).
    return frozenset({native.symbol.upper(), *(s.upper() for s in native.accepted_symbols)})
