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

    Adding a perp venue is one folder (the connector's ``perps_read`` module
    publishing ``PERPS_READ_SPEC``) plus one row in :data:`_SPEC_LOADERS` — no
    framework edit. The table is empty until a connector opts in (the GMX row
    lands with ``gmx_v2/perps_read.py``; Aster with ``aster_perps/perps_read.py``).
    """

    # Protocol identifier -> (module path, attribute) naming the connector's
    # published ``PerpsReadSpec``.
    _SPEC_LOADERS: ClassVar[dict[str, tuple[str, str]]] = {
        "gmx_v2": ("almanak.connectors.gmx_v2.perps_read", "PERPS_READ_SPEC"),
        "aster_perps": ("almanak.connectors.aster_perps.perps_read", "PERPS_READ_SPEC"),
    }

    # Protocol aliases that map onto a canonical key in ``_SPEC_LOADERS``.
    # ``pancakeswap_perps`` is the deprecated name for the Aster Diamond (PCS
    # Perps is broker id=2 on Aster; see pancakeswap_perps/__init__.py), so the
    # legacy name resolves to the canonical ``aster_perps`` spec.
    _ALIASES: ClassVar[dict[str, str]] = {
        "pancakeswap_perps": "aster_perps",
    }

    _spec_cache: ClassVar[dict[str, PerpsReadSpec]] = {}

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
        return cls._ALIASES.get(key, key)

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Return True when ``protocol`` has a connector-owned perps read."""
        return cls._normalize(protocol) in cls._SPEC_LOADERS

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return every protocol identifier with a connector-owned perps read.

        The framework's perp discovery iterates this instead of hardcoding a
        venue list, so adding a connector extends discovery with no framework
        edit.
        """
        return tuple(sorted(cls._SPEC_LOADERS))

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
        return key if key in cls._SPEC_LOADERS else None

    @classmethod
    def _load_spec(cls, protocol: str) -> PerpsReadSpec | None:
        """Resolve and cache one protocol's perps-read spec.

        Imports ONLY the connector module that owns ``protocol`` (per
        ``_SPEC_LOADERS``) — a broken sibling connector cannot block this lookup.
        Returns ``None`` when the protocol is unknown.
        """
        cached = cls._spec_cache.get(protocol)
        if cached is not None:
            return cached
        entry = cls._SPEC_LOADERS.get(protocol)
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
