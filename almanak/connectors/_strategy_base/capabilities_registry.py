"""Lazy registry for connector-owned protocol capabilities.

Each connector with capability data ships a ``capabilities.py`` module
exporting a module-level ``PROTOCOL_CAPABILITIES`` dict keyed by every
protocol identifier the connector validates against, and declares that
ownership on its ``CONNECTOR`` manifest via
``capabilities=CapabilitiesSpec(keys=..., module=...)``. This registry
derives the protocol → module ownership map from the connector manifests
(``CONNECTOR_REGISTRY``), imports capability modules on demand, and exposes a
single aggregated view to the framework validators
(``vocabulary.PROTOCOL_CAPABILITIES`` is the consumer-facing seam that calls
into this module). Adding a connector therefore requires no edit here — the
manifest declaration in the connector's own folder is the registration.

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

from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec


class CapabilitiesRegistry:
    """Protocol-name to connector capability-dict registry.

    The protocol → module ownership map is derived from connector manifests:
    each connector declares ``capabilities=CapabilitiesSpec(keys=..., module=...)``
    and multiple identifiers can point at the same connector module
    (``morpho`` and ``morpho_blue`` both resolve to ``morpho_blue.capabilities``).
    The capability module must declare every key its manifest claims -- the
    registry raises on lookup when the two drift. Cross-connector key
    collisions are rejected at manifest discovery
    (``ConnectorRegistry._discover``).
    """

    # ``protocol identifier -> capabilities module path`` derived from the
    # connector manifests on first use. ``None`` means "not built yet".
    _loader_map: ClassVar[dict[str, str] | None] = None

    @classmethod
    def _loaders(cls) -> dict[str, str]:
        """Return the manifest-derived protocol → module ownership map."""
        if cls._loader_map is None:
            # Deferred import: this module is imported by the connector
            # descriptor (for ``CapabilitiesSpec``), so importing the
            # registry at module level would be circular.
            from almanak.connectors._connector import CONNECTOR_REGISTRY

            loaders: dict[str, str] = {}
            for connector_manifest in CONNECTOR_REGISTRY.with_capabilities():
                spec = connector_manifest.capabilities
                assert spec is not None
                for key in spec.keys:
                    loaders[key] = spec.module
            cls._loader_map = loaders
        return cls._loader_map

    # Stable aggregated dict. Populated incrementally: ``get`` adds entries
    # one at a time as protocols are first looked up; ``all_capabilities``
    # ensures every entry is loaded. Once an entry is added it is never
    # replaced or removed within a process, preserving the long-standing
    # identity contract of the hand-written ``PROTOCOL_CAPABILITIES`` table.
    _aggregated: ClassVar[dict[str, dict[str, Any]]] = {}
    # True once every module in the ownership map has been loaded.
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
        manifest-derived ownership map) — a broken sibling connector cannot
        block this lookup. Returns ``None`` when the protocol is unknown.
        """
        if key in cls._aggregated:
            return cls._aggregated[key]
        module_path = cls._loaders().get(key)
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
        for key in cls._loaders():
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
        return protocol.lower() in cls._loaders()

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Return all protocol names with connector-owned capability entries."""
        return tuple(sorted(cls._loaders()))

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
        cls._loader_map = None


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
    "CapabilitiesSpec",
    "all_protocol_capabilities",
    "get_protocol_capabilities",
]
