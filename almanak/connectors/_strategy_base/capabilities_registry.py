"""Lazy registry for connector-owned protocol capabilities.

Each connector with capability data ships a ``capabilities.py`` module
exporting a module-level ``PROTOCOL_CAPABILITIES`` dict keyed by every
protocol identifier the connector validates against. This registry imports
those modules on demand and exposes a single aggregated view to the framework
validators (``vocabulary.PROTOCOL_CAPABILITIES`` is the consumer-facing seam
that calls into this module).

Per-protocol lookups (``get`` / ``get_protocol_capabilities``) import ONLY
the connector module that owns the requested protocol — a broken sibling
module cannot poison an unrelated capability lookup. Bulk consumers (the
aggregated ``PROTOCOL_CAPABILITIES`` view) call ``all_capabilities`` which
loads every module on first use and caches the merged dict.

Identity contract: every ``all_capabilities`` access returns the same
aggregated dict instance, and every value-dict is the connector module's own
dict — not a copy — so test fixtures that monkey-patch a capability value
(e.g. ``aave_v3 → interest_rate_modes``) see the change reflected in
subsequent validator calls within the same Python process. Restoring the
original value in the same test undoes the change, matching the long-standing
contract of the previously hand-written ``PROTOCOL_CAPABILITIES`` table.
"""

from __future__ import annotations

import importlib
from typing import Any, ClassVar


class CapabilitiesRegistry:
    """Protocol-name to connector capability-dict registry.

    ``_BUILTIN_LOADERS`` maps each protocol identifier to the connector module
    that owns it. Multiple identifiers can point at the same connector module
    (``morpho`` and ``morpho_blue`` both resolve to ``morpho_blue.capabilities``).
    The capability module itself is responsible for declaring every key it
    owns -- the registry does not infer key lists from the protocol→module
    mapping. This keeps the alias contract explicit at the connector level.
    """

    _BUILTIN_LOADERS: ClassVar[dict[str, str]] = {
        # Lending — pooled
        "aave_v3": "almanak.connectors.aave_v3.capabilities",
        "spark": "almanak.connectors.spark.capabilities",
        "compound_v3": "almanak.connectors.compound_v3.capabilities",
        "benqi": "almanak.connectors.benqi.capabilities",
        "radiant_v2": "almanak.connectors.radiant_v2.capabilities",
        "euler_v2": "almanak.connectors.euler_v2.capabilities",
        # Lending — isolated markets
        "morpho": "almanak.connectors.morpho_blue.capabilities",
        "morpho_blue": "almanak.connectors.morpho_blue.capabilities",
        "curvance": "almanak.connectors.curvance.capabilities",
        "silo_v2": "almanak.connectors.silo_v2.capabilities",
        # Lending — Solana
        "kamino": "almanak.connectors.kamino.capabilities",
        # Perps
        "gmx_v2": "almanak.connectors.gmx_v2.capabilities",
        "hyperliquid": "almanak.connectors.hyperliquid.capabilities",
        "drift": "almanak.connectors.drift.capabilities",
        # Concentrated-liquidity / swap
        "uniswap_v3": "almanak.connectors.uniswap_v3.capabilities",
        "enso": "almanak.connectors.enso.capabilities",
        "pendle": "almanak.connectors.pendle.capabilities",
        # ERC-4626 vaults
        "metamorpho": "almanak.connectors.morpho_vault.capabilities",
        # Prediction markets
        "polymarket": "almanak.connectors.polymarket.capabilities",
        # Solana LP
        "raydium_clmm": "almanak.connectors.raydium.capabilities",
        "meteora_dlmm": "almanak.connectors.meteora.capabilities",
        "orca_whirlpools": "almanak.connectors.orca.capabilities",
    }

    # Stable aggregated dict. Populated incrementally: ``get`` adds entries
    # one at a time as protocols are first looked up; ``all_capabilities``
    # ensures every entry is loaded. Once an entry is added it is never
    # replaced or removed within a process, preserving the long-standing
    # identity contract of the hand-written ``PROTOCOL_CAPABILITIES`` table.
    _aggregated: ClassVar[dict[str, dict[str, Any]]] = {}
    # True once every module in ``_BUILTIN_LOADERS`` has been loaded.
    _all_loaded: ClassVar[bool] = False

    @classmethod
    def _load_module_capabilities(cls, module_path: str) -> dict[str, dict[str, Any]]:
        """Import a connector capabilities module and return its dict."""
        module = importlib.import_module(module_path)
        caps = getattr(module, "PROTOCOL_CAPABILITIES", None)
        if not isinstance(caps, dict):
            raise TypeError(f"{module_path}.PROTOCOL_CAPABILITIES must be a dict, got {type(caps).__name__}")
        return caps

    @classmethod
    def _load_protocol(cls, key: str) -> dict[str, Any] | None:
        """Resolve a single protocol's capability dict and cache it.

        Imports ONLY the connector module that owns ``key`` (per the
        ``_BUILTIN_LOADERS`` mapping) — a broken sibling connector cannot
        block this lookup. Returns ``None`` when the protocol is unknown.
        """
        if key in cls._aggregated:
            return cls._aggregated[key]
        module_path = cls._BUILTIN_LOADERS.get(key)
        if module_path is None:
            return None
        module_caps = cls._load_module_capabilities(module_path)
        if key not in module_caps:
            raise KeyError(
                f"Registry maps '{key}' to '{module_path}' but that module's "
                f"PROTOCOL_CAPABILITIES has no '{key}' key. Available keys in "
                f"{module_path}.PROTOCOL_CAPABILITIES: {sorted(module_caps)}"
            )
        cls._aggregated[key] = module_caps[key]
        return module_caps[key]

    @classmethod
    def all_capabilities(cls) -> dict[str, dict[str, Any]]:
        """Return the aggregated protocol-name to capability-dict mapping.

        Imports every connector capability module on first call and merges
        them into the shared ``_aggregated`` dict. Identity is stable -- the
        same dict instance is returned on every call -- and value-dicts are
        the connector modules' own dicts (not copies) so monkey-patching
        ``aggregated["aave_v3"][...]`` mutates the underlying connector
        module value too.
        """
        if cls._all_loaded:
            return cls._aggregated
        for key in cls._BUILTIN_LOADERS:
            cls._load_protocol(key)
        cls._all_loaded = True
        return cls._aggregated

    @classmethod
    def get(cls, protocol: str) -> dict[str, Any] | None:
        """Return the capability dict for ``protocol`` (lower-cased), or None.

        Imports ONLY the connector module that owns ``protocol`` -- unrelated
        connectors with broken imports cannot break this lookup. The cached
        value is the connector module's own dict (stable identity), so test
        fixtures that mutate ``capabilities[key]`` see the change reflected
        on the connector module too.
        """
        return cls._load_protocol(protocol.lower())

    @classmethod
    def has(cls, protocol: str) -> bool:
        """Return True when ``protocol`` has a connector-owned capability entry."""
        return protocol.lower() in cls._BUILTIN_LOADERS

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return all protocol names with connector-owned capability entries."""
        return tuple(sorted(cls._BUILTIN_LOADERS))

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: clear the aggregated cache so the next call rebuilds.

        Clears the ``_aggregated`` dict **in place** so external references to
        it (PEP 562 ``__getattr__`` caches the dict in
        ``vocabulary.globals()`` and in ``framework.intents.__init__.globals()``
        on first access) stay live and refill correctly on the next
        ``all_capabilities`` / ``get`` call. Production code should never call
        this -- it exists for narrow test setups that intentionally need to
        re-trigger connector imports.
        """
        cls._aggregated.clear()
        cls._all_loaded = False


def get_protocol_capabilities(protocol: str) -> dict[str, Any]:
    """Module-level convenience wrapper.

    Returns an empty dict (not ``None``) when the protocol is unknown so
    consumers can write ``capabilities.get(key, default)`` directly without a
    None-check. This matches the long-standing semantics of
    ``PROTOCOL_CAPABILITIES.get(protocol_lower, {})``.
    """
    return CapabilitiesRegistry.get(protocol) or {}


def all_protocol_capabilities() -> dict[str, dict[str, Any]]:
    """Module-level convenience wrapper returning the aggregated view."""
    return CapabilitiesRegistry.all_capabilities()


__all__ = [
    "CapabilitiesRegistry",
    "all_protocol_capabilities",
    "get_protocol_capabilities",
]
