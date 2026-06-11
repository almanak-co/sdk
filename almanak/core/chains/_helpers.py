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

from collections.abc import Mapping
from types import MappingProxyType

from almanak.core.chains._registry import ChainRegistry
from almanak.core.enums import ChainFamily

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


def block_time_for(chain: str) -> float | None:
    """Average block time (seconds) for ``chain``, or ``None`` if unknown.

    Derived from ``ChainDescriptor.rpc.block_time_seconds`` (the W5 field;
    ``None`` == "no archive-RPC support in backtesting"). Alias-normalises via
    ``ChainRegistry.try_resolve``. Returns ``None`` (not a default) on a miss so
    CLI callers can apply their own literal fallback at the call site.
    """
    if not chain:
        return None
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None:
        return None
    return descriptor.rpc.block_time_seconds


def blocks_per_day_for(chain: str) -> int | None:
    """Approximate blocks/day for ``chain`` (``round(86400 / block_time)``), or ``None``.

    ``None`` when the chain has no ``block_time_seconds`` — preserving the legacy
    ``BLOCKS_PER_DAY`` membership (only chains with a block time appear). ``round``
    reproduces all six legacy values exactly (7200 / 345600 / 43200). A
    non-positive block time (invalid descriptor data) also yields ``None``
    rather than dividing by zero.
    """
    bt = block_time_for(chain)
    if bt is None or bt <= 0:
        return None
    return round(86400 / bt)


def alchemy_rpc_url_template_for(chain: str) -> str | None:
    """Alchemy RPC URL *template* for ``chain`` with a literal ``{key}`` placeholder.

    Returns ``f"https://{prefix}-mainnet.g.alchemy.com/v2/{{key}}"`` when the chain
    is EVM and declares ``rpc.alchemy_prefix``; ``None`` otherwise. The EVM gate is
    load-bearing: the permissions CLI is EVM/Zodiac-only, so a non-EVM prefix
    (solana) must NOT yield a template (preserves the
    ``_resolve_rpc_url(None, "solana") is None`` contract). The doubled ``{{key}}``
    keeps a literal ``{key}`` in the returned string for the caller's
    ``.replace("{key}", api_key)``.
    """
    if not chain:
        return None
    descriptor = ChainRegistry.try_resolve(chain)
    if descriptor is None or descriptor.family is not ChainFamily.EVM:
        return None
    prefix = descriptor.rpc.alchemy_prefix
    if prefix is None:
        return None
    return f"https://{prefix}-mainnet.g.alchemy.com/v2/{{key}}"


def blocks_per_day_map() -> Mapping[str, int]:
    """Read-only ``{chain: blocks_per_day}`` for every chain with a block time.

    Registry-derived back-compat view for the re-exported ``BLOCKS_PER_DAY`` and
    the replay ``--chain`` choice keys. Membership == chains with
    ``block_time_seconds`` set (the legacy 6).
    """
    return MappingProxyType(
        {
            d.name: round(86400 / d.rpc.block_time_seconds)
            for d in ChainRegistry.all()
            if d.rpc.block_time_seconds is not None and d.rpc.block_time_seconds > 0
        }
    )


def is_solana_chain(chain: str | None) -> bool:
    """True when *chain* resolves to a Solana-family chain (VIB-4851 CS-2).

    Replaces scattered ``chain == "solana"`` / ``chain.lower() == "solana"``
    family branches. Resolution goes through ``ChainRegistry.try_resolve``,
    which lowercases, strips, and accepts registered aliases — so ``"SOLANA"``,
    ``" solana "`` and the ``"sol"`` alias all dispatch to the Solana family
    (the literal comparisons treated alias/cased inputs as EVM, a latent
    mis-route). Unknown or empty chains return ``False``, preserving the
    literal comparisons' behavior for arbitrary strings.
    """
    if not isinstance(chain, str) or not chain.strip():
        # The literal comparisons this replaces were None-safe
        # (``None == "solana"`` is False); keep that contract for call
        # sites that pass ``chain=None`` before chain resolution.
        return False
    descriptor = ChainRegistry.try_resolve(chain)
    return descriptor is not None and descriptor.family is ChainFamily.SOLANA


def solana_chain_names() -> frozenset[str]:
    """Canonical names of every registered Solana-family chain (VIB-4851 CS-2).

    Registry-derived replacement for the literal ``frozenset({"solana"})``
    membership sets. Canonical names only — no aliases — so ``name in
    solana_chain_names()`` is byte-equivalent to the legacy sets for
    canonical inputs.
    """
    return frozenset(d.name for d in ChainRegistry.all() if d.family is ChainFamily.SOLANA)


def evm_chain_names() -> tuple[str, ...]:
    """Canonical names of every registered EVM-family chain, in registration
    order (VIB-4851 CS-3).

    Registry-derived replacement for hand-maintained all-EVM chain tuples
    (e.g. the CLI runtime's ``anvil_chains`` default). Order follows the
    sorted side-effect imports in ``almanak/core/chains/__init__.py``;
    consumers of the legacy tuples are order-insensitive (env-var reads /
    set membership).
    """
    return tuple(d.name for d in ChainRegistry.all() if d.family is ChainFamily.EVM)


def fork_archive_required_chains() -> frozenset[str]:
    """Chains whose managed-Anvil fork needs an archive-capable RPC.

    Membership == descriptors with ``rpc.fork_requires_archive=True``
    (legacy ``gateway/managed.py`` ``ARCHIVE_RPC_REQUIRED_CHAINS``;
    VIB-3971 / VIB-3973 Part B; inverted in VIB-4851 CS-3).
    """
    return frozenset(d.name for d in ChainRegistry.all() if d.rpc.fork_requires_archive)


def rpc_rate_limit_map() -> Mapping[str, int]:
    """Read-only ``{chain: requests_per_minute}`` gateway RPC budget map.

    Registry-derived back-compat view of the legacy ``rpc_service.py``
    ``CHAIN_RATE_LIMITS`` dict. Membership == chains declaring
    ``rpc.rate_limit_rpm``; the gateway lookup keeps its own
    ``.get(chain, <default>)`` miss fallback, so undeclared chains behave
    exactly as before (VIB-4851 CS-3).
    """
    return MappingProxyType(
        {d.name: d.rpc.rate_limit_rpm for d in ChainRegistry.all() if d.rpc.rate_limit_rpm is not None}
    )
