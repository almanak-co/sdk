"""Strategy-side dispatch registry for connector-owned funding-history venues.

Sibling of :class:`~almanak.connectors._strategy_base.perps_read_registry.PerpsReadRegistry`.
Owns the protocol-identifier → funding-history venue mapping derived from each
connector's ``funding_history=FundingHistoryDecl(...)`` manifest declaration, so
framework funding consumers (``backtesting/pnl/providers/funding_rates.py`` and
``backtesting/pnl/providers/perp/``) never hardcode a venue name, an alias
tuple, or a chain list.

The decl carries pure data (venue string, chains, aliases) — there is no
ImportRef to resolve, so lookups never import connector modules. The venue
string is the dispatch key of the gateway's ``RateHistoryService``
``GetFundingRateHistory`` RPC; a parity test pins each decl venue to the
connector's ``GatewayFundingHistoryCapability.funding_venue()``.

Gateway-boundary note: this module is strategy-side and performs no network
egress.

VIB-4851 Phase D.
"""

from __future__ import annotations

import logging
from typing import ClassVar

logger = logging.getLogger(__name__)

__all__ = ["FundingHistoryRegistry"]


class FundingHistoryRegistry:
    """Protocol-identifier → connector funding-history-venue dispatch registry.

    Adding a funding-history venue is one folder: the connector's ``CONNECTOR``
    manifest declares ``funding_history=FundingHistoryDecl(...)`` — no framework
    or registry edit. Aliases (e.g. the legacy ``"gmx"`` name for GMX V2) are
    declared on the owning connector's ``FundingHistoryDecl``.
    """

    # Manifest-derived maps, built lazily on first use (deferred
    # ``CONNECTOR_REGISTRY`` import — never at module import). ``None`` means
    # "not built yet".
    _venue_map: ClassVar[dict[str, str] | None] = None
    _alias_map: ClassVar[dict[str, str] | None] = None
    _chains_map: ClassVar[dict[str, tuple[str, ...]] | None] = None

    @classmethod
    def _build_dispatch(cls) -> None:
        """Derive the venue, alias, and chains maps from connector manifests."""
        # Deferred import: avoids a module-level cycle through the connector
        # descriptor.
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        venues: dict[str, str] = {}
        aliases: dict[str, str] = {}
        chains: dict[str, tuple[str, ...]] = {}
        for connector_manifest in CONNECTOR_REGISTRY.with_funding_history():
            decl = connector_manifest.funding_history
            assert decl is not None
            venues[connector_manifest.name] = decl.venue
            chains[connector_manifest.name] = decl.chains
            for alias in decl.aliases:
                aliases[alias] = connector_manifest.name
        cls._venue_map = venues
        cls._alias_map = aliases
        cls._chains_map = chains

    @classmethod
    def _venues(cls) -> dict[str, str]:
        """Return the manifest-derived ``canonical key -> venue`` map."""
        if cls._venue_map is None:
            cls._build_dispatch()
        assert cls._venue_map is not None
        return cls._venue_map

    @classmethod
    def _aliases(cls) -> dict[str, str]:
        """Return the manifest-derived ``alias -> canonical key`` map."""
        if cls._alias_map is None:
            cls._build_dispatch()
        assert cls._alias_map is not None
        return cls._alias_map

    @classmethod
    def _chains(cls) -> dict[str, tuple[str, ...]]:
        """Return the manifest-derived ``canonical key -> declared chains`` map."""
        if cls._chains_map is None:
            cls._build_dispatch()
        assert cls._chains_map is not None
        return cls._chains_map

    @classmethod
    def _normalize(cls, protocol: str | None) -> str:
        # Total by design (mirrors PerpsReadRegistry._normalize): ``None`` /
        # non-``str`` input normalises to "" so every public entry point fails
        # closed instead of raising on ``.lower()``.
        if not isinstance(protocol, str):
            return ""
        key = protocol.strip().lower().replace("-", "_")
        return cls._aliases().get(key, key)

    @classmethod
    def has(cls, protocol: str | None) -> bool:
        """Return True when ``protocol`` has a connector-owned funding venue."""
        return cls._normalize(protocol) in cls._venues()

    @classmethod
    def canonical(cls, protocol: str | None) -> str | None:
        """Return the canonical key for ``protocol``, or None when unknown."""
        if not isinstance(protocol, str) or not protocol:
            return None
        key = cls._normalize(protocol)
        return key if key in cls._venues() else None

    @classmethod
    def venue_for(cls, protocol: str | None) -> str | None:
        """Return the gateway funding venue for ``protocol``, or None.

        The returned string is the ``GetFundingRateHistory`` dispatch key
        (``GatewayFundingHistoryCapability.funding_venue()``).
        """
        key = cls._normalize(protocol)
        return cls._venues().get(key)

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return every accepted protocol identifier (canonical keys + aliases).

        Unlike :meth:`PerpsReadRegistry.supported_protocols` (canonical-only),
        this includes decl aliases: the consumers are validation gates over
        user-supplied protocol identifiers, and the legacy alias spellings
        (e.g. ``"gmx"``) are accepted identifiers.
        """
        return tuple(sorted({*cls._venues(), *cls._aliases()}))

    @classmethod
    def declared_chains(cls, protocol: str | None) -> tuple[str, ...]:
        """Return the chains ``protocol`` declares funding data for.

        Empty tuple means chain-agnostic (off-chain venues) or unknown
        protocol — callers gate on :meth:`has` first when the distinction
        matters.
        """
        key = cls._normalize(protocol)
        return cls._chains().get(key, ())

    @classmethod
    def all_declared_chains(cls) -> frozenset[str]:
        """Union of every declared funding chain across connectors.

        Consumers that accept a chain before knowing the venue (e.g. a
        provider constructor) validate against this set instead of a
        hardcoded per-venue chain table.
        """
        return frozenset(chain for chains in cls._chains().values() for chain in chains)

    @classmethod
    def reset_cache(cls) -> None:
        """Clear derived maps (test hook, mirrors sibling registries)."""
        cls._venue_map = None
        cls._alias_map = None
        cls._chains_map = None
