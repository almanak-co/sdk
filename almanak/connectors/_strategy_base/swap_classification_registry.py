"""Strategy-side swap-router classification registry (VIB-4928 PR-3b).

Sibling of
:class:`~almanak.connectors._strategy_base.contract_role_registry.ContractRoleRegistry`
and :class:`~almanak.connectors._strategy_base.gas_estimate_registry.GasEstimateConnectorRegistry`,
scoped to the swap-router-ABI + fee-tier classification concern. The intent
compiler's five swap-classification symbols — ``SWAP_FEE_TIERS``,
``DEFAULT_SWAP_FEE_TIER``, ``SWAP_ROUTER_V1_PROTOCOLS``,
``SWAP_ROUTER_V1_CHAIN_OVERRIDES``, ``SWAP_ROUTER_ALGEBRA_PROTOCOLS`` — fan out
over this registry instead of hand-importing each DEX connector's
``swap_classification`` module.

Why a sibling, not bolted onto ``ContractRoleRegistry``
-------------------------------------------------------
``ContractRoleRegistry`` answers *"what address fills a semantic contract
slot"*; this registry answers *"what ABI shape / fee-tier behaviour does a DEX
protocol's swap router have"* — a distinct concern (classification metadata, not
addresses). Folding them would conflate the two, the exact coupling VIB-4851 set
out to retire.

Collision semantics
-------------------
Unlike ``ContractRoleRegistry.register`` (last-wins), :meth:`register` raises
:class:`SwapClassificationConflictError` (a ``ValueError``) when two
registrations contribute the **same** protocol slug with **different**
``fee_tiers`` or ``default_fee_tier``. This preserves the pre-PR-3b
cross-connector collision guard that lived in the ``_build_swap_fee_tiers`` /
``_build_default_swap_fee_tier`` helpers. The boolean / chain-membership roles
(``router_v1``, ``router_v1_chains``, ``router_algebra``) are union-semantics:
when two registrations agree on the fee-tier roles but differ on these, the
contributions are **merged** (``or`` / ordered set-union), never overwritten.
That preserves the pre-PR-3b ``_build_swap_router_*`` builders' ``|=`` /
``.update`` behaviour, where two ``swap_constants`` modules could each
contribute a V1 / Algebra / per-chain override for one slug without the later
one clobbering the earlier.

Gateway-boundary note: this module is strategy-side and performs no network
egress. It holds only classification metadata (plain tuples / ints / bools).
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import ClassVar

__all__ = [
    "SWAP_CLASSIFICATION_REGISTRY",
    "SwapClassificationConflictError",
    "SwapClassificationRegistry",
    "SwapClassificationSpec",
]


class SwapClassificationConflictError(ValueError):
    """Two connectors registered conflicting swap classification for one slug.

    Subclasses ``ValueError`` so the cross-connector collision contract (the
    byte-equivalence tests assert ``pytest.raises(ValueError, ...)``) keeps
    holding after the registry inversion.
    """


@dataclass(frozen=True)
class SwapClassificationSpec:
    """One DEX connector slug's swap-router classification.

    A connector's ``swap_classification.SWAP_CLASSIFICATION`` is an ordered
    tuple of these — one per framework-facing protocol slug the connector owns
    (the Uniswap V3 connector owns ``uniswap_v3`` + its ``agni_finance`` fork).

    Attributes:
        protocol: The framework-facing protocol slug.
        fee_tiers: Supported fee tiers in bps, or ``()`` for Algebra DEXes with
            dynamically-priced pools and no fixed tiers (Camelot). A slug with
            empty ``fee_tiers`` is absent from the derived ``SWAP_FEE_TIERS``.
        default_fee_tier: AUTO-selection fallback tier in bps, or ``None`` when
            the slug publishes no fixed tiers. ``None`` → absent from the
            derived ``DEFAULT_SWAP_FEE_TIER``.
        router_v1: ``True`` if the slug uses the original SwapRouter ABI
            (8-param ``exactInputSingle`` WITH ``deadline``) on every chain,
            rather than SwapRouter02 (7-param, no deadline).
        router_v1_chains: Chains on which this slug uses the V1 ABI as a
            per-chain override (even when ``router_v1`` is ``False``). Inverted
            into ``SWAP_ROUTER_V1_CHAIN_OVERRIDES`` (``{chain: {slug, ...}}``).
        router_algebra: ``True`` if the slug uses the Algebra V1.9 router ABI
            (selector ``0xbc651188``, no ``fee`` parameter).
    """

    protocol: str
    fee_tiers: tuple[int, ...] = ()
    default_fee_tier: int | None = None
    router_v1: bool = False
    router_v1_chains: tuple[str, ...] = ()
    router_algebra: bool = False


class SwapClassificationRegistry:
    """Protocol slug -> swap-router classification. Insertion-ordered.

    Populated once at boot by
    ``almanak.connectors._strategy_swap_classification_registry``. The intent
    compiler's ``compiler_constants`` builders consult the derived-view
    accessors (:meth:`fee_tiers`, :meth:`router_v1_protocols`, ...) — never
    naming a connector.
    """

    #: protocol slug -> spec, in registration order (dict insertion order). No
    #: consumer of the derived symbols iterates them in order (all use
    #: ``protocol in X`` / ``X.get(protocol)``), so order is not load-bearing —
    #: it is kept stable only for diagnostic determinism.
    _specs: ClassVar[dict[str, SwapClassificationSpec]] = {}

    @classmethod
    def register(cls, spec: SwapClassificationSpec) -> None:
        """Register one slug's classification (idempotent on identical re-add).

        Raises :class:`SwapClassificationConflictError` if ``spec.protocol`` is
        already registered with a different ``fee_tiers`` or ``default_fee_tier``
        — the cross-connector collision guard. When the fee-tier roles agree but
        the union-semantics router roles (``router_v1``, ``router_v1_chains``,
        ``router_algebra``) differ, the contributions are **merged** rather than
        overwritten — preserving the pre-PR-3b ``_build_swap_router_*`` builders'
        union behaviour so a later registration cannot silently drop an earlier
        slug's V1 / Algebra / per-chain override. An identical re-registration
        (same spec) is a no-op, so a connector re-imported under pytest-xdist
        does not raise.
        """
        existing = cls._specs.get(spec.protocol)
        if existing is not None and existing != spec:
            if existing.fee_tiers != spec.fee_tiers:
                raise SwapClassificationConflictError(
                    f"protocol {spec.protocol!r} has conflicting SWAP_FEE_TIERS "
                    f"contributions: {existing.fee_tiers} vs {spec.fee_tiers}"
                )
            if existing.default_fee_tier != spec.default_fee_tier:
                raise SwapClassificationConflictError(
                    f"protocol {spec.protocol!r} has conflicting "
                    f"DEFAULT_SWAP_FEE_TIER contributions: "
                    f"{existing.default_fee_tier} vs {spec.default_fee_tier}"
                )
            # Fee-tier roles agree; the router roles are union-semantics (see the
            # module "Collision semantics" note). Merge them so a later
            # contribution cannot clobber an earlier slug's override.
            cls._specs[spec.protocol] = replace(
                existing,
                router_v1=existing.router_v1 or spec.router_v1,
                router_v1_chains=tuple(dict.fromkeys((*existing.router_v1_chains, *spec.router_v1_chains))),
                router_algebra=existing.router_algebra or spec.router_algebra,
            )
            return
        cls._specs[spec.protocol] = spec

    @classmethod
    def fee_tiers(cls) -> dict[str, tuple[int, ...]]:
        """``{protocol: fee_tiers}`` for every slug publishing fixed tiers."""
        return {p: s.fee_tiers for p, s in cls._specs.items() if s.fee_tiers}

    @classmethod
    def default_fee_tiers(cls) -> dict[str, int]:
        """``{protocol: default_fee_tier}`` for every slug publishing one."""
        return {p: s.default_fee_tier for p, s in cls._specs.items() if s.default_fee_tier is not None}

    @classmethod
    def router_v1_protocols(cls) -> frozenset[str]:
        """Slugs using the V1 SwapRouter ABI on every chain (union)."""
        return frozenset(p for p, s in cls._specs.items() if s.router_v1)

    @classmethod
    def router_v1_chain_overrides(cls) -> dict[str, frozenset[str]]:
        """``{chain: {slug, ...}}`` per-chain V1-ABI overrides (union)."""
        overrides: dict[str, set[str]] = {}
        for proto, spec in cls._specs.items():
            for chain in spec.router_v1_chains:
                overrides.setdefault(chain, set()).add(proto)
        return {chain: frozenset(protos) for chain, protos in overrides.items()}

    @classmethod
    def router_algebra_protocols(cls) -> frozenset[str]:
        """Slugs using the Algebra V1.9 router ABI (union)."""
        return frozenset(p for p, s in cls._specs.items() if s.router_algebra)

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Whether ``protocol`` has a classification registration."""
        return protocol in cls._specs

    @classmethod
    def registered_protocols(cls) -> tuple[str, ...]:
        """Every registered slug, in registration order."""
        return tuple(cls._specs)

    @classmethod
    def reset(cls) -> None:
        """Test helper: drop all registrations (production never calls this)."""
        cls._specs.clear()


#: The single in-process registry. Concrete connectors are registered into it
#: by ``almanak.connectors._strategy_swap_classification_registry`` (boot file).
SWAP_CLASSIFICATION_REGISTRY = SwapClassificationRegistry
