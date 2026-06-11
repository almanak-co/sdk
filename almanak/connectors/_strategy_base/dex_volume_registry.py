"""Strategy-side dispatch registry for connector-owned DEX backtesting data.

Sibling of :class:`~almanak.connectors._strategy_base.funding_history_registry.FundingHistoryRegistry`.
Owns the protocol-identifier → per-DEX backtesting facts mapping derived from
each connector's ``dex_volume=DexVolumeDecl(...)`` manifest declaration, so the
framework volume aggregator (``backtesting/pnl/providers/multi_dex_volume.py``)
and liquidity-depth provider (``liquidity_depth.py``) never hardcode a DEX
name, an alias tuple, a chain table, an AMM-family list, or a provenance
string.

The decl carries pure data — lookups never import connector modules. The
``dex`` value is the dispatch key of the gateway's
``GetDexVolumeHistory`` RPC; a parity test pins each decl to the connector's
``GatewayDexVolumeCapability`` (``dex_name()`` + ``volume_supported_chains()``).

Gateway-boundary note: this module is strategy-side and performs no network
egress.

VIB-4851 Phase D.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import ClassVar

logger = logging.getLogger(__name__)

__all__ = ["DexVolumeEntry", "DexVolumeRegistry"]


@dataclass(frozen=True)
class DexVolumeEntry:
    """Resolved per-DEX backtesting facts for one connector declaration."""

    key: str  # primary dispatch key (decl name or connector name)
    dex: str  # gateway GetDexVolumeHistory routing key
    chains: tuple[str, ...]
    volume_data_source: str
    amm_family: str
    chain_default: tuple[str, ...]
    generic_default: bool


class DexVolumeRegistry:
    """Protocol-identifier → connector DEX-backtesting-facts dispatch registry.

    Adding a DEX's backtesting data is one folder: the connector's
    ``CONNECTOR`` manifest declares ``dex_volume=DexVolumeDecl(...)`` — no
    framework or registry edit. Aliases (e.g. ``"uni_v3"``, ``"crv"``) and the
    legacy primary key for connectors whose folder name differs
    (``balancer_v2`` -> ``"balancer"``) are declared on the decl.
    """

    # Manifest-derived maps, built lazily on first use (deferred
    # ``CONNECTOR_REGISTRY`` import — never at module import).
    _entry_map: ClassVar[dict[str, DexVolumeEntry] | None] = None
    _alias_map: ClassVar[dict[str, str] | None] = None

    @classmethod
    def _build_dispatch(cls) -> None:
        """Derive the entry and alias maps from connector manifests."""
        # Deferred import: avoids a module-level cycle through the connector
        # descriptor.
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        entries: dict[str, DexVolumeEntry] = {}
        aliases: dict[str, str] = {}
        for connector_manifest in CONNECTOR_REGISTRY.with_dex_volume():
            decl = connector_manifest.dex_volume
            assert decl is not None
            key = decl.name or connector_manifest.name
            entries[key] = DexVolumeEntry(
                key=key,
                dex=decl.dex or connector_manifest.name,
                chains=decl.chains,
                volume_data_source=decl.volume_data_source or f"{key}_subgraph",
                amm_family=decl.amm_family,
                chain_default=decl.chain_default,
                generic_default=decl.generic_default,
            )
            for alias in decl.aliases:
                aliases[alias] = key
            if connector_manifest.name != key:
                # The connector folder name resolves too (e.g. "balancer_v2").
                aliases[connector_manifest.name] = key
        cls._entry_map = entries
        cls._alias_map = aliases

    @classmethod
    def _entries(cls) -> dict[str, DexVolumeEntry]:
        """Return the manifest-derived ``primary key -> entry`` map."""
        if cls._entry_map is None:
            cls._build_dispatch()
        assert cls._entry_map is not None
        return cls._entry_map

    @classmethod
    def _aliases(cls) -> dict[str, str]:
        """Return the manifest-derived ``alias -> primary key`` map."""
        if cls._alias_map is None:
            cls._build_dispatch()
        assert cls._alias_map is not None
        return cls._alias_map

    @classmethod
    def _normalize(cls, protocol: str | None) -> str:
        # Total by design (mirrors the sibling registries): ``None`` /
        # non-``str`` input normalises to "" so public entry points fail
        # closed instead of raising.
        if not isinstance(protocol, str):
            return ""
        key = protocol.strip().lower().replace("-", "_")
        return cls._aliases().get(key, key)

    @classmethod
    def has(cls, protocol: str | None) -> bool:
        """Return True when ``protocol`` has connector-owned DEX data."""
        return cls._normalize(protocol) in cls._entries()

    @classmethod
    def canonical(cls, protocol: str | None) -> str | None:
        """Return the primary dispatch key for ``protocol``, or None."""
        if not isinstance(protocol, str) or not protocol:
            return None
        key = cls._normalize(protocol)
        return key if key in cls._entries() else None

    @classmethod
    def entry_for(cls, protocol: str | None) -> DexVolumeEntry | None:
        """Return the resolved facts for ``protocol``, or None when unknown."""
        return cls._entries().get(cls._normalize(protocol))

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Primary dispatch keys, sorted."""
        return tuple(sorted(cls._entries()))

    @classmethod
    def protocols_by_family(cls, amm_family: str) -> tuple[str, ...]:
        """Primary keys whose declaration carries ``amm_family``, sorted."""
        return tuple(sorted(e.key for e in cls._entries().values() if e.amm_family == amm_family))

    @classmethod
    def chain_default(cls, chain: str | None) -> str | None:
        """Protocol-detection default for ``chain``.

        A DEX declaring ``chain_default`` for the chain wins (aerodrome on
        base, traderjoe_v2 on avalanche); otherwise the ``generic_default``
        DEX (uniswap_v3) wins when it supports the chain. Mirrors the legacy
        ``MultiDEXVolumeProvider._detect_protocol_from_chain`` /
        ``liquidity_depth`` heuristics, now declaration-driven.
        """
        if not isinstance(chain, str) or not chain:
            return None
        chain_lower = chain.strip().lower()
        for entry in cls._entries().values():
            if chain_lower in entry.chain_default:
                return entry.key
        for entry in cls._entries().values():
            if entry.generic_default and chain_lower in entry.chains:
                return entry.key
        return None

    @classmethod
    def all_supported_chains(cls) -> frozenset[str]:
        """Union of every declared DEX-history chain across connectors."""
        return frozenset(chain for entry in cls._entries().values() for chain in entry.chains)

    @classmethod
    def twap_reference_pools(cls) -> dict[str, dict]:
        """Merged TWAP reference tables from every declaring connector.

        Returns ``{"pools": {chain: {pool_key: address}}, "token_to_pool":
        {TOKEN: {chain: pool_key}}}`` — today only uniswap_v3 declares one;
        merging keeps the consumer connector-blind. Lazy per declaration
        (ImportRef resolved on first call).
        """
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        merged: dict[str, dict] = {"pools": {}, "token_to_pool": {}}
        for connector_manifest in CONNECTOR_REGISTRY.with_dex_volume():
            decl = connector_manifest.dex_volume
            assert decl is not None
            ref = decl.twap_reference_pools
            if ref is None:
                continue
            tables = ref.load()
            if not isinstance(tables, dict):
                # Fail loud: silently skipping a malformed declaration would
                # hide a connector bug behind an empty reference table.
                raise TypeError(
                    f"{connector_manifest.name}: dex_volume.twap_reference_pools "
                    f"({ref.module}.{ref.attribute}) must resolve to a dict, "
                    f"got {type(tables).__name__}"
                )
            for chain, pools in tables.get("pools", {}).items():
                merged["pools"].setdefault(chain, {}).update(pools)
            for token, chain_map in tables.get("token_to_pool", {}).items():
                merged["token_to_pool"].setdefault(token, {}).update(chain_map)
        return merged

    @classmethod
    def reset_cache(cls) -> None:
        """Clear derived maps (test hook, mirrors sibling registries)."""
        cls._entry_map = None
        cls._alias_map = None
