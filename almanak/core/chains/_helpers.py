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


def external_id_for(chain: str, vendor: str) -> str | None:
    """Return ``chain``'s identifier for third-party ``vendor``, or ``None``.

    Derived from the single source of truth ``ChainDescriptor.external_ids``
    (a sparse, vendor-keyed mapping; see
    :data:`almanak.core.chains._descriptor.KNOWN_VENDORS`). This is the
    registry-derived replacement for the standalone per-vendor maps
    (``COINGECKO_PLATFORM_IDS``, ``CHAIN_TO_DEXSCREENER_PLATFORM``,
    ``_CHAIN_TO_NETWORK``, ``_CHAIN_TO_LLAMA``, Zerion / Moralis / OKX
    ``_CHAIN_IDS`` …) folded onto the descriptor in VIB-4851 (B1).

    Fail-closed and sparse, mirroring the legacy ``map.get(chain)`` → ``None``
    miss: an unregistered chain, a chain whose descriptor declares no
    ``external_ids`` at all, or a chain whose ``external_ids`` simply lacks
    ``vendor`` all return ``None``. The value is returned **verbatim** —
    e.g. ``external_id_for("arbitrum", "coingecko") == "arbitrum-one"`` and
    ``external_id_for("ethereum", "geckoterminal") == "eth"`` — case included.

    Alias-normalises the chain via ``ChainRegistry.try_resolve`` so an alias
    resolves to its canonical descriptor (e.g.
    ``external_id_for("bnb", "okx") == "56"`` because ``bnb`` resolves to
    ``bsc``). The ``vendor`` key is matched case-insensitively, consistent
    with the lower-cased storage in ``ChainDescriptor.__post_init__``.
    """
    if not chain or not vendor:
        return None
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None or descriptor.external_ids is None:
        return None
    return descriptor.external_ids.get(vendor.lower())


def vendor_chain_map(vendor: str) -> dict[str, str]:
    """Return ``{canonical_chain_name: vendor_id}`` for ``vendor``.

    Inverts ``ChainDescriptor.external_ids`` back into the per-vendor shape the
    legacy standalone maps had, but built **only** from the chains whose
    descriptor actually declares ``vendor``. It is never widened to every
    registered chain — a chain absent from the result is genuinely unsupported
    by that vendor (the anti-widening invariant the B1 equivalence test pins).

    Keys are canonical chain names only; aliases are excluded (each descriptor
    contributes its canonical ``name`` exactly once, never its aliases). The
    ``vendor`` key is matched case-insensitively. An unknown / never-declared
    vendor yields an empty dict.
    """
    if not vendor:
        return {}
    vendor_key = vendor.lower()
    result: dict[str, str] = {}
    for descriptor in ChainRegistry.all():
        external_ids = descriptor.external_ids
        if external_ids is None:
            continue
        vendor_id = external_ids.get(vendor_key)
        if vendor_id is not None:
            result[descriptor.name] = vendor_id
    return result


def chain_name_for_id(chain_id: int) -> str | None:
    """EIP-155 chain id -> canonical chain name, or ``None`` for an unregistered id.

    Mirrors the legacy ``_CHAIN_ID_TO_NAME.get(chain_id)`` contract: an unknown id
    (including Solana, whose registry ``chain_id`` is 0 and is not in ``_by_id``)
    returns ``None`` so callers fall through, never raising. Registry-derived
    replacement for hardcoded ``{chain_id: name}`` matrices (VIB-4851 A2).
    """
    descriptor = ChainRegistry.try_resolve_id(chain_id)
    return descriptor.name if descriptor is not None else None
