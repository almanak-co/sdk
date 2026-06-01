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
