"""Strategy-side dispatch registry for connector-owned LP/vault position reads.

Sibling of :class:`~almanak.connectors._strategy_base.lending_read_registry.LendingReadRegistry`
and :class:`~almanak.connectors._strategy_base.perps_read_registry.PerpsReadRegistry`.
Owns the single protocol-identifier → owning-connector ``position_read``
mapping, so the framework LP/vault valuer never hardcodes a protocol-name set
to decide which on-chain repricer family marks a position.

Each connector whose LP/vault position is valued by a protocol-name-gated
framework repricer declares ``position_read=PositionReadDecl(kind=..., ...)``
on its ``CONNECTOR`` manifest. The registry derives a ``protocol -> (kind,
builder ref)`` map from those declarations and answers two questions the
capability-gated framework readers ask:

* :meth:`kind` — which repricer family owns ``protocol`` (``"fungible_lp"`` /
  ``"curve_lp"``), or ``None``. The framework ``FungibleLpPositionReader`` /
  ``CurveLpPositionReader`` ``supports()`` checks consult this instead of an
  inline ``protocol in {...}`` literal set.
* :meth:`builder` — the connector-side fungible-LP builder callable for a
  protocol of the ``fungible_lp`` kind, lazily imported from the declared
  ImportRef (``None`` for framework-valued kinds like ``curve_lp``).

Adding a protocol-name-gated LP/vault connector is one manifest opt-in inside
the connector folder, with no framework or registry edit — the VIB-5126 /
VIB-5420 promotion this registry was built for (it removes the two dated
``FungibleLpPositionReader._BOOTSTRAP`` / ``CurveLpPositionReader._SUPPORTED_PROTOCOLS``
coupling-baseline exceptions).

Gateway-boundary note: this module is strategy-side and performs no network
egress. The connector builder modules it lazily imports are pure data + pure
functions; the gateway-routed ``eth_call`` lives in the framework reader.
"""

from __future__ import annotations

import importlib
import logging
from typing import ClassVar

from almanak.connectors._strategy_base.position_read_base import (
    POSITION_READ_KINDS,
    PositionReadBuilder,
)

logger = logging.getLogger(__name__)

__all__ = ["PositionReadRegistry"]


class PositionReadRegistry:
    """Protocol-identifier → connector position-read dispatch registry.

    Dispatch is derived from connector manifests: each connector whose LP/vault
    position is valued by a protocol-name-gated framework repricer declares
    ``position_read=PositionReadDecl(...)`` on its ``CONNECTOR``. The framework
    readers consult :meth:`kind` / :meth:`builder` instead of a hardcoded
    protocol-name set, so adding such a connector requires no framework edit.
    """

    # Manifest-derived ``protocol -> kind`` map, ``protocol -> (module, attribute)``
    # builder-loader map, and ``alias -> canonical key`` map, built lazily on
    # first use. ``None`` means "not built yet". Builder values stay
    # ``(module, attribute)`` so per-protocol imports remain lazy (importlib on
    # first lookup, never at derivation time — the xdist member-drop hazard).
    _kind_map: ClassVar[dict[str, str] | None] = None
    _builder_loader_map: ClassVar[dict[str, tuple[str, str]] | None] = None
    _alias_map: ClassVar[dict[str, str] | None] = None

    _builder_cache: ClassVar[dict[str, PositionReadBuilder]] = {}

    @classmethod
    def _build_dispatch(cls) -> None:
        """Derive the kind, builder-loader, and alias maps from connector manifests."""
        # Deferred import: avoids a module-level cycle through the connector
        # descriptor.
        from almanak.connectors._connector import CONNECTOR_REGISTRY

        kinds: dict[str, str] = {}
        builder_loaders: dict[str, tuple[str, str]] = {}
        aliases: dict[str, str] = {}
        for connector_manifest in CONNECTOR_REGISTRY.with_position_read():
            decl = connector_manifest.position_read
            assert decl is not None
            key = connector_manifest.name
            kinds[key] = decl.kind
            if decl.builder is not None:
                builder_loaders[key] = (decl.builder.module, decl.builder.attribute)
            for alias in decl.aliases:
                aliases[alias] = key
        cls._kind_map = kinds
        cls._builder_loader_map = builder_loaders
        cls._alias_map = aliases

    @classmethod
    def _kinds(cls) -> dict[str, str]:
        """Return the manifest-derived ``protocol -> kind`` map."""
        if cls._kind_map is None:
            cls._build_dispatch()
        assert cls._kind_map is not None
        return cls._kind_map

    @classmethod
    def _builder_loaders(cls) -> dict[str, tuple[str, str]]:
        """Return the manifest-derived ``protocol -> (module, attribute)`` builder map."""
        if cls._builder_loader_map is None:
            cls._build_dispatch()
        assert cls._builder_loader_map is not None
        return cls._builder_loader_map

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
        # ``PositionInfo.protocol`` reaching a reader's ``supports()``) normalises
        # to the empty string rather than raising on ``.lower()`` — every public
        # entry point then fails closed (no kind for "" ⇒ ``None`` / ``False``)
        # instead of crashing the snapshot.
        if not isinstance(protocol, str):
            return ""
        key = protocol.lower().replace("-", "_")
        return cls._aliases().get(key, key)

    @classmethod
    def has(cls, protocol: str | None) -> bool:
        """Return True when ``protocol`` has a connector-owned position read."""
        return cls._normalize(protocol) in cls._kinds()

    @classmethod
    def kind(cls, protocol: str | None) -> str | None:
        """Return the repricer-family kind for ``protocol``, or ``None``.

        The framework ``FungibleLpPositionReader`` / ``CurveLpPositionReader``
        ``supports()`` checks compare this against their own
        ``POSITION_READ_KINDS`` constant, so the dispatch decision stays
        manifest-owned and no reader names a protocol.

        Total by design: ``None`` / non-``str`` / unknown input returns ``None``.
        """
        return cls._kinds().get(cls._normalize(protocol))

    @classmethod
    def supported_protocols(cls) -> tuple[str, ...]:
        """Every protocol identifier with a connector-owned position read, sorted."""
        return tuple(sorted(cls._kinds()))

    @classmethod
    def supported_protocols_for_kind(cls, kind: str) -> tuple[str, ...]:
        """Every protocol identifier dispatched to ``kind``, sorted.

        Lets a framework consumer (e.g. boot strand-detection drift coverage)
        enumerate the protocols a given repricer family owns without naming any.
        """
        return tuple(sorted(p for p, k in cls._kinds().items() if k == kind))

    @classmethod
    def canonical(cls, protocol: str | None) -> str | None:
        """Return the canonical key for ``protocol`` if it has a position read.

        Total by design: ``None`` / non-``str`` / empty input returns ``None``,
        so callers can use it in a ``canonical(p) or fallback`` normalisation.
        """
        if not isinstance(protocol, str) or not protocol:
            return None
        key = cls._normalize(protocol)
        return key if key in cls._kinds() else None

    @classmethod
    def builder(cls, protocol: str | None) -> PositionReadBuilder | None:
        """Resolve and cache the connector-side builder callable for ``protocol``.

        Imports ONLY the connector module that owns ``protocol``'s builder (per
        the manifest-derived dispatch), lazily on first access — a broken
        sibling connector cannot block this lookup. Returns ``None`` when the
        protocol is unknown or its kind is framework-valued (no builder, e.g.
        ``curve_lp``).
        """
        key = cls._normalize(protocol)
        cached = cls._builder_cache.get(key)
        if cached is not None:
            return cached
        entry = cls._builder_loaders().get(key)
        if entry is None:
            return None
        module_path, attribute = entry
        module = importlib.import_module(module_path)
        fn = getattr(module, attribute, None)
        if not callable(fn):
            raise TypeError(
                f"Registry maps {protocol!r} builder to {module_path}.{attribute}, "
                f"but that attribute is {type(fn).__name__}, not callable."
            )
        cls._builder_cache[key] = fn
        return fn

    @classmethod
    def known_kinds(cls) -> frozenset[str]:
        """The closed set of recognised position-read kinds (from ``position_read_base``)."""
        return POSITION_READ_KINDS

    @classmethod
    def reset_cache(cls) -> None:
        """Test helper: drop the resolved-builder cache + dispatch maps.

        Production code should never call this — it exists for narrow test
        setups that intentionally re-trigger a connector import.
        """
        cls._builder_cache.clear()
        cls._kind_map = None
        cls._builder_loader_map = None
        cls._alias_map = None
