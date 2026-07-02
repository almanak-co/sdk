"""ChainRegistry — singleton registry of every supported chain.

Per-chain descriptor files live as siblings (``ethereum.py``,
``arbitrum.py``, …). Each registers itself via the ``@register_chain``
decorator at import time. The public module ``almanak.core.chains``
imports every sibling so a single ``import almanak.core.chains`` is
sufficient to populate the registry — there is no lazy-load path.

VIB-4801: replaces ~8 chain-keyed dicts scattered across the codebase
with this single registry.

Usage::

    from almanak.core.chains import ChainRegistry

    ChainRegistry.get("ethereum").chain_id                # -> 1
    ChainRegistry.resolve("bnb").name                     # -> "bsc"
    ChainRegistry.by_id(42161).name                       # -> "arbitrum"
    ChainRegistry.all()                                   # -> tuple of descriptors
"""

from __future__ import annotations

from almanak.core.enums import ChainFamily

from ._descriptor import CAIP2_NAMESPACE_BY_FAMILY, ChainDescriptor, GasProfile

# Recognised CAIP-2 namespaces (derived from the family→namespace map). Used to
# detect a CAIP-2-shaped input in ``resolve`` / ``try_resolve`` without
# mistaking a bare chain name for one. VIB-5175.
_CAIP2_NAMESPACES: frozenset[str] = frozenset(CAIP2_NAMESPACE_BY_FAMILY.values())


class ChainRegistry:
    """Process-wide registry of :class:`ChainDescriptor` records.

    The registry is **deterministic at import time** — descriptors are
    registered when their sibling module is imported, and the public
    ``almanak.core.chains.__init__`` imports every sibling. Lookups do not
    trigger imports; if a chain is missing, ``get`` / ``resolve`` raise
    explicitly.

    The class exposes only classmethods; instantiating it is not useful.
    """

    _by_name: dict[str, ChainDescriptor] = {}
    _by_id: dict[int, ChainDescriptor] = {}
    # CAIP-2 id (e.g. "eip155:42161", "solana:5eykt4UsFv8P8…") → descriptor.
    # Keys store the reference VERBATIM (Solana's base58 genesis hash is
    # case-sensitive); the namespace is lowercased. VIB-5175.
    _by_caip2: dict[str, ChainDescriptor] = {}

    @classmethod
    def register(cls, descriptor: ChainDescriptor) -> None:
        """Insert a descriptor.

        Validation runs to completion before any registry map is mutated:
        if any check raises, the registry is left exactly as it was.

        Aliases are stored lowercased to mirror :meth:`resolve` /
        :meth:`try_resolve`, which lowercase their input. ``descriptor.name``
        is already enforced to be lowercase by ``ChainDescriptor.__post_init__``.

        Raises ``ValueError`` if the same canonical name is registered
        twice, an alias collides with another chain, or two EVM chains
        declare the same ``chain_id`` — each almost always means a
        copy/paste bug in a chain file.
        """
        # ----- preflight validation (no mutation) -----
        # Canonical-name identity: any second registration claiming an
        # already-taken name is a copy/paste bug in a chain file (this is
        # what duplicate enum members used to catch via syntax errors).
        existing_for_name = cls._by_name.get(descriptor.name)
        if existing_for_name is not None:
            raise ValueError(
                f"Canonical name {descriptor.name!r} collides with "
                f"already-registered chain {existing_for_name.name!r} "
                f"(existing chain_id={existing_for_name.chain_id}, "
                f"incoming chain_id={descriptor.chain_id})"
            )

        normalized_aliases = tuple(alias.lower() for alias in descriptor.aliases)
        # descriptor.name is already lowercase (enforced by __post_init__).
        for alias in normalized_aliases:
            if alias in cls._by_name and cls._by_name[alias] is not descriptor:
                raise ValueError(
                    f"Alias {alias!r} for {descriptor.name} collides "
                    f"with already-registered chain "
                    f"{cls._by_name[alias].name}"
                )

        # chain_id == 0 is the non-EVM sentinel (Solana); we don't index it
        # by id because multiple non-EVM chains could share id=0 later.
        if descriptor.chain_id != 0 and descriptor.chain_id in cls._by_id:
            raise ValueError(
                f"Duplicate chain_id {descriptor.chain_id} for "
                f"{descriptor.name} (already used by "
                f"{cls._by_id[descriptor.chain_id].name})"
            )

        # Non-EVM chains must declare an explicit caip2_reference to be
        # registered (their chain_id is the 0 sentinel and cannot serve as a
        # CAIP-2 reference). Enforced here rather than in ChainDescriptor
        # __post_init__ so synthetic non-EVM descriptors (test fixtures) stay
        # buildable without one. VIB-5175.
        if descriptor.family is not ChainFamily.EVM and not descriptor.caip2_reference:
            raise ValueError(
                f"Non-EVM ChainDescriptor {descriptor.name} must declare a "
                f"caip2_reference (chain_id is the non-EVM sentinel and cannot serve "
                f"as a CAIP-2 reference)"
            )

        # CAIP-2 id must be unique. For EVM the chain_id check above already
        # guarantees it, but non-EVM chains are keyed only by their explicit
        # ``caip2_reference``, so guard that namespace here. VIB-5175.
        caip2 = descriptor.caip2
        existing_caip2 = cls._by_caip2.get(caip2)
        if existing_caip2 is not None and existing_caip2 is not descriptor:
            raise ValueError(
                f"Duplicate CAIP-2 id {caip2!r} for {descriptor.name} (already used by {existing_caip2.name})"
            )

        # ----- commit (every validation has passed) -----
        cls._by_name[descriptor.name] = descriptor
        for alias in normalized_aliases:
            cls._by_name[alias] = descriptor
        if descriptor.chain_id != 0:
            cls._by_id[descriptor.chain_id] = descriptor
        cls._by_caip2[caip2] = descriptor

    @classmethod
    def get(cls, chain: str | ChainDescriptor) -> ChainDescriptor:
        """Get a descriptor by canonical name / alias / CAIP-2 id.

        A :class:`ChainDescriptor` passes through unchanged; a string
        delegates to :meth:`resolve` (raises ``ValueError`` for unknown
        chains).
        """
        if isinstance(chain, ChainDescriptor):
            return chain
        if not isinstance(chain, str):
            raise TypeError(f"chain must be a canonical name string or ChainDescriptor, got {type(chain).__name__}")
        return cls.resolve(chain)

    @classmethod
    def resolve(cls, name_or_alias: str) -> ChainDescriptor:
        """Resolve any canonical name, alias, or CAIP-2 id to a descriptor.

        Case-insensitive for names/aliases; leading/trailing whitespace is
        stripped. A CAIP-2-shaped input (``eip155:42161``,
        ``solana:5eykt4UsFv8P8…``) is routed to :meth:`by_caip2` with the
        reference case preserved, so ``resolve("eip155:42161")`` and
        ``resolve("arbitrum")`` return the same descriptor (VIB-5175).

        Raises ``ValueError`` for unknown chains (matches the legacy
        ``resolve_chain_name`` contract).
        """
        raw = name_or_alias.strip()
        caip = cls.try_resolve_caip2(raw)
        if caip is not None:
            return caip
        descriptor = cls._by_name.get(raw.lower())
        if descriptor is None:
            raise ValueError(f"Unknown chain: {name_or_alias!r}")
        return descriptor

    @classmethod
    def try_resolve(cls, name_or_alias: str) -> ChainDescriptor | None:
        """Like :meth:`resolve`, but returns ``None`` for unknown chains.

        Used by legacy ``dict.get(chain, DEFAULT)`` call sites that want
        the previous "missing chain → fall back silently" behaviour.
        Accepts CAIP-2 ids in addition to names/aliases (VIB-5175).
        """
        raw = name_or_alias.strip()
        caip = cls.try_resolve_caip2(raw)
        if caip is not None:
            return caip
        return cls._by_name.get(raw.lower())

    @classmethod
    def family_of(cls, name_or_alias: str) -> ChainFamily | None:
        """Execution family for a chain name / alias / CAIP-2 id, or ``None``.

        Registry-backed replacement for the deleted ``get_chain_family`` /
        ``CHAIN_FAMILY_MAP`` (VIB-4801 / VIB-4851): the family is read straight
        off the ``ChainDescriptor`` — the single source of truth — so no
        parallel chain→family literal can drift from it. Returns ``None`` for an
        unresolvable chain — including a missing / blank / non-string input — so
        callers keep the legacy "unknown chain → tolerate" (fail-closed)
        semantics without a hand-rolled ``Chain(x.upper())`` + ``try/except``
        dance. The guard lives here rather than leaning on :meth:`try_resolve`
        (which raises on a ``None`` input) so money-path callers such as
        ``PermissionManifest.is_evm_chain`` stay robust when a chain field is
        unset on a dataclass that carries no runtime validation.
        """
        if not name_or_alias or not isinstance(name_or_alias, str):
            return None
        descriptor = cls.try_resolve(name_or_alias)
        return descriptor.family if descriptor is not None else None

    @classmethod
    def by_caip2(cls, caip2: str) -> ChainDescriptor:
        """Look up a descriptor by its CAIP-2 blockchain id.

        Raises ``ValueError`` for an unknown or malformed CAIP-2 id.
        """
        descriptor = cls.try_resolve_caip2(caip2)
        if descriptor is None:
            raise ValueError(f"Unknown or malformed CAIP-2 chain id: {caip2!r}")
        return descriptor

    @classmethod
    def try_resolve_caip2(cls, value: str) -> ChainDescriptor | None:
        """Resolve a CAIP-2 id to a descriptor, or ``None``.

        Returns ``None`` when ``value`` is not CAIP-2-shaped (no ``:`` or an
        unknown namespace), so callers can use this as a detector. The
        namespace is lowercased; the reference is matched VERBATIM (Solana's
        base58 genesis hash is case-sensitive). VIB-5175.
        """
        if not isinstance(value, str):
            # A detector must never raise on a non-string (e.g. a mocked chain
            # in tests); it simply isn't a CAIP-2 id.
            return None
        namespace, sep, reference = value.strip().partition(":")
        if not sep or not reference or namespace.lower() not in _CAIP2_NAMESPACES:
            return None
        return cls._by_caip2.get(f"{namespace.lower()}:{reference}")

    @classmethod
    def by_id(cls, chain_id: int) -> ChainDescriptor:
        """Look up by EIP-155 chain ID.

        Raises ``ValueError`` if no EVM chain has this id.
        """
        descriptor = cls._by_id.get(chain_id)
        if descriptor is None:
            raise ValueError(f"Unknown chain_id: {chain_id}")
        return descriptor

    @classmethod
    def try_resolve_id(cls, chain_id: int) -> ChainDescriptor | None:
        """Look up by EIP-155 chain ID, or ``None`` if no EVM chain has this id.

        Non-raising sibling of :meth:`by_id` (mirrors :meth:`try_resolve` vs
        :meth:`resolve`). Solana's ``chain_id`` is 0 and is not registered in
        ``_by_id``, so ``try_resolve_id(0)`` returns ``None``.
        """
        return cls._by_id.get(chain_id)

    @classmethod
    def all(cls) -> tuple[ChainDescriptor, ...]:
        """Return every registered descriptor, sorted by canonical name.

        Deterministic ordering matters for tests, log output, and any
        ``frozenset`` derived view (e.g. ``ALLOWED_CHAINS``). Canonical names
        are the lowercase enum names, so this ordering is identical to the
        historical sort-by-enum-name ordering.
        """
        return tuple(cls._by_name[name] for name in cls.names())

    @classmethod
    def names(cls) -> tuple[str, ...]:
        """Return every canonical chain name, sorted."""
        return tuple(sorted({d.name for d in cls._by_name.values()}))

    @classmethod
    def conservative_gas_fallback(cls) -> GasProfile:
        """Gas profile assumed for chains with no usable gas facts.

        Policy: an unregistered chain (or one whose descriptor carries no
        fallback fees) prices like Ethereum mainnet - the most expensive
        common case - so its backtests overstate rather than understate
        execution costs. Owned by the registry so framework consumers
        (e.g. the backtester's default gas resolution, VIB-5088) carry no
        chain literals (VIB-4851 coupling rule).
        """
        return cls._by_name["ethereum"].gas

    @classmethod
    def aliases(cls) -> dict[str, str]:
        """Return the full alias map (canonical names + aliases → canonical name).

        Equivalent to the legacy ``_CHAIN_ALIASES`` dict, with canonical
        lowercase names as values (the Chain enum is being removed —
        VIB-4851).
        """
        return {name: d.name for name, d in cls._by_name.items()}

    # ----- internal: only used by tests --------------------------------

    @classmethod
    def _reset(cls) -> None:
        """Clear every registration. Tests only."""
        cls._by_name.clear()
        cls._by_id.clear()
        cls._by_caip2.clear()


def register_chain(descriptor: ChainDescriptor) -> ChainDescriptor:
    """Module-scope helper: register a descriptor and return it.

    Per-chain files use this as::

        DESCRIPTOR = register_chain(ChainDescriptor(...))
    """
    ChainRegistry.register(descriptor)
    return descriptor


__all__ = ["ChainRegistry", "register_chain"]
