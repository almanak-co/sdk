"""Strategy-side dispatch registry for connector-owned perp-position reads.

Sibling of :class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`.
Owns the single protocol-identifier → owning-connector ``perps_read`` mapping and
lazily imports *only* the connector that owns a requested protocol, so a broken
sibling connector cannot poison an unrelated lookup, and the framework perp
reader/valuer never hardcodes a venue name, a contract role, or an ABI selector.

Each perp connector that supports an on-chain position read publishes a
module-level :data:`PERPS_READ_SPEC` (a
:class:`~almanak.connectors._strategy_base.perps_read_base.PerpsReadSpec`) in its
``perps_read`` module. The registry resolves the spec, resolves each declared
contract-role address through :class:`AddressRegistry`, and materialises a
:class:`~almanak.connectors._strategy_base.perps_read_base.PerpsPositionPlan` the
framework reader executes via the gateway. Valuation and market metadata are
reached through :meth:`PerpsReadRegistry.value_position` /
:meth:`PerpsReadRegistry.market_metadata` so the framework names no venue.

Gateway-boundary note: this module is strategy-side and performs no network
egress. The owning connector ``perps_read`` modules it imports are pure data +
pure functions; the gateway-routed ``eth_call`` lives in the framework reader.

VIB-4930 (epic VIB-4851).
"""

from __future__ import annotations

import importlib
import logging
from dataclasses import replace
from typing import Any, ClassVar

from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.perps_read_base import (
    PerpsMarketMeta,
    PerpsPositionPlan,
    PerpsPositionQuery,
    PerpsPositionValue,
    PerpsReadSpec,
)

logger = logging.getLogger(__name__)

__all__ = ["PerpsReadRegistry"]


class PerpsReadRegistry:
    """Protocol-identifier → connector perps-read-spec dispatch registry.

    Adding a perp venue is one folder: the connector's ``perps_read`` module
    publishes ``PERPS_READ_SPEC`` and its ``CONNECTOR`` manifest declares
    ``perps_read=PerpsReadDecl(...)`` — no framework or registry edit. Aliases
    (e.g. the deprecated ``pancakeswap_perps`` name for the Aster Diamond) are
    declared on the owning connector's ``PerpsReadDecl``.
    """

    # Manifest-derived ``protocol -> (module path, attribute)`` spec map and
    # ``alias -> canonical key`` map, built lazily on first use. ``None`` means
    # "not built yet". Values stay (module, attribute) so per-protocol imports
    # remain lazy (importlib on first lookup, never at derivation time).
    _spec_loader_map: ClassVar[dict[str, tuple[str, str]] | None] = None
    _alias_map: ClassVar[dict[str, str] | None] = None

    _spec_cache: ClassVar[dict[str, PerpsReadSpec]] = {}

    @classmethod
    def _build_dispatch(cls) -> None:
        """Derive the spec-loader and alias maps from connector manifests."""
        # Deferred import: avoids a module-level cycle through the connector
        # descriptor.
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        spec_loaders: dict[str, tuple[str, str]] = {}
        aliases: dict[str, str] = {}
        for connector_manifest in CONNECTOR_REGISTRY.with_perps_read():
            decl = connector_manifest.perps_read
            assert decl is not None
            spec_loaders[connector_manifest.name] = (decl.spec.module, decl.spec.attribute)
            for alias in decl.aliases:
                aliases[alias] = connector_manifest.name
        cls._spec_loader_map = spec_loaders
        cls._alias_map = aliases

    @classmethod
    def _spec_loaders(cls) -> dict[str, tuple[str, str]]:
        """Return the manifest-derived ``protocol -> (module, attribute)`` map."""
        if cls._spec_loader_map is None:
            cls._build_dispatch()
        assert cls._spec_loader_map is not None
        return cls._spec_loader_map

    @classmethod
    def _aliases(cls) -> dict[str, str]:
        """Return the manifest-derived ``alias -> canonical key`` map."""
        if cls._alias_map is None:
            cls._build_dispatch()
        assert cls._alias_map is not None
        return cls._alias_map

    @classmethod
    def _normalize(cls, protocol: str | None) -> str:
        # Total by design: ``None`` / non-``str`` input (loosely typed
        # ``position.protocol`` reaching ``market_metadata`` / ``value_position``)
        # normalises to the empty string rather than raising ``AttributeError``
        # on ``.lower()`` — every public entry point then fails closed (no spec
        # for "" ⇒ ``None`` / ``False``) instead of crashing the snapshot.
        if not isinstance(protocol, str):
            return ""
        key = protocol.lower().replace("-", "_")
        return cls._aliases().get(key, key)

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Return True when ``protocol`` has a connector-owned perps read."""
        return cls._normalize(protocol) in cls._spec_loaders()

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return every protocol identifier with a connector-owned perps read.

        The framework's perp discovery iterates this instead of hardcoding a
        venue list, so adding a connector extends discovery with no framework
        edit.
        """
        return tuple(sorted(cls._spec_loaders()))

    @classmethod
    def canonical(cls, protocol: str | None) -> str | None:
        """Return the canonical key for ``protocol`` if it has a perps read.

        Total by design: ``None`` / non-``str`` input (loosely typed strategy
        metadata) returns ``None`` rather than raising, so callers can use it in a
        ``canonical(p) or fallback`` normalisation.
        """
        if not isinstance(protocol, str) or not protocol:
            return None
        key = cls._normalize(protocol)
        return key if key in cls._spec_loaders() else None

    @classmethod
    def _load_spec(cls, protocol: str) -> PerpsReadSpec | None:
        """Resolve and cache one protocol's perps-read spec.

        Imports ONLY the connector module that owns ``protocol`` (per the
        manifest-derived dispatch) — a broken sibling connector cannot block this lookup.
        Returns ``None`` when the protocol is unknown.
        """
        cached = cls._spec_cache.get(protocol)
        if cached is not None:
            return cached
        entry = cls._spec_loaders().get(protocol)
        if entry is None:
            return None
        module_path, attribute = entry
        module = importlib.import_module(module_path)
        spec = getattr(module, attribute, None)
        if not isinstance(spec, PerpsReadSpec):
            raise TypeError(
                f"Registry maps {protocol!r} to {module_path}.{attribute}, "
                f"but that attribute is {type(spec).__name__}, not a PerpsReadSpec."
            )
        cls._spec_cache[protocol] = spec
        return spec

    @classmethod
    def resolve_plan(cls, protocol: str, query: PerpsPositionQuery) -> PerpsPositionPlan | None:
        """Materialise a perp position read for ``(protocol, query.chain)``.

        Resolves the connector's spec, then every declared contract-role address
        through :class:`AddressRegistry` (the spec's ``contract_kinds``) into the
        query's generic ``targets`` map, fills ``markets`` for per-market venues,
        and invokes the connector's pure ``build_calls`` planner. Returns ``None``
        when the protocol is unknown or any declared role has no address on the
        chain — the framework reader fails closed on ``None`` (this is also the
        fast "chain not deployed" gate the discovery scan relies on).
        """
        key = cls._normalize(protocol)
        spec = cls._load_spec(key)
        if spec is None:
            logger.debug("No perps-read spec for protocol %s", protocol)
            return None

        targets: dict[str, str] = {}
        for role, kinds in spec.contract_kinds.items():
            address = AddressRegistry.resolve_contract_address(key, query.chain, kinds)
            if not address:
                logger.debug(
                    "No %s address for perp protocol %s on chain %s",
                    kinds,
                    key,
                    query.chain,
                )
                return None
            targets[role] = address

        markets = query.markets
        if spec.markets_for_chain is not None and not markets:
            markets = tuple(spec.markets_for_chain(query.chain))
            if not markets:
                # A per-market venue with no markets on this chain is not deployed
                # here — the markets-scoped analogue of the missing contract-address
                # gate above. A precompile/markets venue (e.g. hyperliquid) has no
                # AddressRegistry entry to fail on, so an empty market set is the
                # only "not deployed on this chain" signal; return None so the
                # discovery scan skips it silently instead of issuing an empty read.
                logger.debug("No markets for perp protocol %s on chain %s (not deployed)", key, query.chain)
                return None

        resolved = replace(query, targets=targets, markets=markets)
        calls = tuple(spec.build_calls(resolved))
        return PerpsPositionPlan(query=resolved, calls=calls, reduce=spec.reduce_calls)

    @classmethod
    def market_metadata(cls, protocol: str, market_address: str, chain: str) -> PerpsMarketMeta | None:
        """Resolve a market's index-token symbol + decimals for valuation.

        Delegates to the connector spec's ``market_metadata`` so the framework
        valuer never imports a connector's market table. Returns ``None`` when the
        protocol is unknown or the market is unrecognised (callers fail closed).
        """
        spec = cls._load_spec(cls._normalize(protocol))
        return spec.market_metadata(market_address, chain) if spec is not None else None

    @classmethod
    def value_position(cls, protocol: str, **kwargs: Any) -> PerpsPositionValue | None:
        """Value a decoded position via the connector's mark-to-market formula.

        Delegates to the connector spec's ``value_position`` (keyword-only) so the
        framework valuer never imports a venue's pricing math. Returns ``None``
        when the protocol has no perps read.
        """
        spec = cls._load_spec(cls._normalize(protocol))
        return spec.value_position(**kwargs) if spec is not None else None

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: drop the resolved-spec cache so the next call re-imports.

        Production code should never call this — it exists for narrow test setups
        that intentionally re-trigger a connector import.
        """
        cls._spec_cache.clear()
        cls._spec_loader_map = None
        cls._alias_map = None
