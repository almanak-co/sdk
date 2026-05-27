"""Strategy-side receipt-parser connector registry (VIB-4854 / W2).

This is the **strategy-side** sibling of
``almanak/connectors/_base/gateway_registry.py``. The gateway-side
registry (``GATEWAY_REGISTRY`` in
``almanak/connectors/_gateway_registry.py``) holds connector instances
that the gateway boot loop discovers and routes capability-keyed
requests to. Receipt parsing, however, runs **inside the strategy
container** — the framework's ``ResultEnricher`` and migration
backfill construct parser instances at runtime, then call
``parse_receipt(...)`` on already-fetched transaction receipts.

Strategy-side code cannot import the gateway-side ``GATEWAY_REGISTRY``
(enforced by ``tests/static/test_strategy_import_boundary.py``), so the
"capability-keyed dispatch via registry" pattern that VIB-4811 lifted
into the gateway needs a strategy-side mirror for the receipt-parser
case. This module provides it.

What lives here
===============

* :class:`ReceiptParserCapability` — a ``@runtime_checkable`` Protocol
  a connector declares by implementing two methods:

    - ``receipt_parser_keys() -> frozenset[str]`` — every alias key the
      connector publishes (e.g. ``frozenset({"morpho_blue", "morpho"})``).
      Cheap, metadata-only — does **not** import the parser module.
    - ``receipt_parser_class(key: str) -> type`` — return the parser
      class for one of the connector's keys. The connector decides
      when to import the parser module; the registry calls this
      lazily on first ``get(protocol)`` for a given key.

* :class:`ReceiptParserConnector` — base class for the lightweight
  strategy-side connector instances registered here. Mirrors
  ``GatewayConnector`` (a ``ProtocolName`` + ``ProtocolKind`` carrier).

* :class:`ReceiptParserConnectorRegistry` — the strategy-side registry
  itself. Same shape as ``GatewayConnectorRegistry``
  (``register`` / ``get`` / ``all`` / ``with_capability``), plus a
  ``classes_by_key`` helper that returns the resolved
  ``{protocol_key: parser_class}`` map a ``ReceiptParserRegistry``
  consumer wants. The map is built lazily on first access and cached.

* :data:`STRATEGY_RECEIPT_PARSER_REGISTRY` — the single in-process
  instance. Concrete strategy-side connectors are registered into it
  by :func:`_register_all` in
  ``almanak/connectors/_strategy_receipt_registry.py`` (mirrors the
  ``_gateway_registry.py`` boot file). Adding a new connector with a
  receipt parser means one import + one ``register`` line in that
  file — there is no central protocol-name table.

Why a Protocol + per-connector classes (vs. a central
``_BUILTIN_LOADERS`` dict)
==========================================================

``CapabilitiesRegistry`` in
``almanak/connectors/_strategy_base/capabilities_registry.py`` uses a
central ``_BUILTIN_LOADERS: dict[str, str]`` mapping protocol →
module path. That works because the protocol → module relationship is
1:1 and static.

The receipt-parser case is more interesting:

* Multiple keys map to the same connector
  (``morpho_blue``/``morpho``; ``raydium``/``raydium_clmm``;
  ``aster_perps``/``pancakeswap_perps``).
* Several connectors take constructor kwargs (``chain=``,
  ``pool_addresses=``, ``underlying_decimals=``, …), so the registry
  cannot pre-instantiate.
* The per-connector ``ReceiptParserCapability`` shape matches the
  gateway-side ``Gateway*Capability`` pattern an agent reading the
  blueprint already knows; preserving that pattern across the trust
  boundary keeps the mental model uniform.

The result is a small, structurally-typed registry: zero strings in
the framework consumer, the protocol-key table lives **on each
connector** (its own ``receipt_parser_keys()``), and the dispatch site
in ``almanak/framework/execution/receipt_registry.py`` is a thin
adapter over ``classes_by_key()``.

Gateway-boundary note: this module is strategy-side. It imports
``ProtocolKind`` / ``ProtocolName`` from ``_base/types.py`` (the
cross-boundary type module), but it does **not** touch
``_base/gateway_*`` — those remain gateway-only.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from typing import ClassVar, Protocol, TypeVar, runtime_checkable

from almanak.connectors._base.types import ProtocolKind, ProtocolName

__all__ = [
    "STRATEGY_RECEIPT_PARSER_REGISTRY",
    "LazyParserClassMap",
    "ReceiptParserCapability",
    "ReceiptParserConnector",
    "ReceiptParserConnectorRegistry",
    "ReceiptParserRegistryError",
]


class ReceiptParserRegistryError(Exception):
    """Registry contract violation (collision, unknown key, etc.)."""


@runtime_checkable
class ReceiptParserCapability(Protocol):
    """Connector publishes one or more receipt-parser classes.

    Two methods so the registry can answer ``is this key handled?``
    without importing the parser module (cheap metadata-only) and only
    import the class on first ``classes_by_key()`` resolution.

    Contract
    --------

    * ``receipt_parser_keys() -> frozenset[str]`` — every protocol
      identifier the connector claims. Multiple keys are legal: a
      Uniswap V3 connector may publish ``frozenset({"uniswap_v3",
      "agni_finance"})`` so the registry routes both to the same
      parser class. Returning an empty frozenset is **not** legal —
      a connector that declares the capability must publish at least
      one key; an empty set would silently disable the capability.
    * ``receipt_parser_class(key: str) -> type`` — return the parser
      class for one of the connector's keys. Imports the parser
      module on first call; subsequent calls may return a cached
      class (the registry caches at its own layer too). Raises
      ``KeyError`` if ``key`` is not in
      ``receipt_parser_keys()`` — the registry never asks for an
      unpublished key, so a raise here is a programming error.

    Why not return a parser **instance**? Several parsers take
    constructor kwargs (``chain=``, ``pool_addresses=``,
    ``underlying_decimals=``, …); a single instance can't serve every
    call site. The framework consumer constructs an instance per-call
    with the kwargs it has on hand.
    """

    def receipt_parser_keys(self) -> frozenset[str]: ...

    def receipt_parser_class(self, key: str) -> type: ...


class ReceiptParserConnector:
    """Base class for strategy-side connector instances registered here.

    Mirrors :class:`almanak.connectors._base.gateway_connector.GatewayConnector`
    on the gateway side: a ``ProtocolName`` + ``ProtocolKind`` carrier
    with capability surface declared by also inheriting from one of
    the strategy-side capability Protocols (currently only
    :class:`ReceiptParserCapability`; more may follow as other
    strategy-side cross-cutting concerns get lifted into capabilities).

    Required class attributes
    -------------------------

    * ``protocol`` — canonical ``ProtocolName`` for this connector.
      Used as the registry key (collision is a hard error).
    * ``kind`` — static ``ProtocolKind`` for logging / dashboards.

    Example::

        class UniswapV3ReceiptParserConnector(
            ReceiptParserConnector, ReceiptParserCapability,
        ):
            protocol: ClassVar[ProtocolName] = ProtocolName("uniswap_v3")
            kind: ClassVar[ProtocolKind] = ProtocolKind.LP

            def receipt_parser_keys(self) -> frozenset[str]:
                return frozenset({"uniswap_v3", "agni_finance"})

            def receipt_parser_class(self, key: str) -> type:
                from almanak.connectors.uniswap_v3.receipt_parser import (
                    UniswapV3ReceiptParser,
                )
                return UniswapV3ReceiptParser
    """

    protocol: ClassVar[ProtocolName]
    kind: ClassVar[ProtocolKind]


T = TypeVar("T")


class LazyParserClassMap(Mapping[str, type]):
    """Read-only mapping that lazily resolves parser classes on access.

    Built from a pre-resolved ``{key: connector}`` map so the registry
    can answer ``"is this key handled?"`` (``__contains__`` /
    ``__iter__``) **without** importing any parser module. The parser
    module is imported on the first ``__getitem__(key)`` call and the
    resolved class is cached for subsequent reads.

    Why this matters: without lazy class resolution, the very first
    ``ResultEnricher.get_parser("spark")`` lookup would force-import
    every registered connector's parser module (Across, Polymarket,
    PancakeSwap perps, …) — defeating the lazy-loading design and
    making the registry fail unrelated lookups if *any* connector
    parser's module raises ``ImportError`` at import time. The
    framework's ``ResultEnricher`` only catches ``ValueError`` from
    ``get_parser``, so an import error on a never-used connector would
    abort enrichment for a totally unrelated protocol.

    Equivalent to the pre-W2 ``_BUILTIN_LOADERS`` behaviour where each
    parser module was loaded on demand via ``importlib.import_module``.
    """

    def __init__(self, key_to_connector: dict[str, ReceiptParserConnector]) -> None:
        self._key_to_connector = key_to_connector
        self._cache: dict[str, type] = {}

    def __getitem__(self, key: str) -> type:
        cached = self._cache.get(key)
        if cached is not None:
            return cached
        connector = self._key_to_connector.get(key)
        if connector is None:
            raise KeyError(key)
        # ``isinstance`` guard mirrors the eager-resolution path: only
        # connectors implementing the capability participate. The
        # registry filters at insertion time, so this branch is defensive.
        if not isinstance(connector, ReceiptParserCapability):
            raise KeyError(key)
        cls = connector.receipt_parser_class(key)
        self._cache[key] = cls
        return cls

    def __contains__(self, key: object) -> bool:
        return key in self._key_to_connector

    def __iter__(self) -> Iterator[str]:
        return iter(self._key_to_connector)

    def __len__(self) -> int:
        return len(self._key_to_connector)


class ReceiptParserConnectorRegistry:
    """In-process registry of strategy-side receipt-parser connectors.

    Same shape as
    :class:`almanak.connectors._base.gateway_registry.GatewayConnectorRegistry`:
    keyed by ``ProtocolName``, collision is a hard error, instances
    (not classes) are stored so ``isinstance(connector, Cap)`` dispatch
    works.

    The :meth:`classes_by_key` helper expands every registered
    connector's ``receipt_parser_keys()`` into a flat
    ``{protocol_key: parser_class}`` map. This is the shape the
    framework's ``ReceiptParserRegistry`` (façade in
    ``almanak/framework/execution/receipt_registry.py``) consumes. The
    map is built lazily on first call and cached; subsequent
    ``register`` calls invalidate the cache.
    """

    def __init__(self) -> None:
        self._connectors: dict[ProtocolName, ReceiptParserConnector] = {}
        # Resolved {key: class} lazy map. None means "not built yet".
        # The map's *keys* are resolved eagerly from
        # ``connector.receipt_parser_keys()`` (cheap metadata-only call),
        # but each parser *class* is loaded on first
        # ``__getitem__(key)`` to preserve the lazy-import design.
        self._classes_by_key: LazyParserClassMap | None = None

    def register(self, connector: ReceiptParserConnector) -> None:
        """Register a connector instance. Collision on protocol raises.

        The registry stores instances so it can dispatch capability
        calls (``isinstance(connector, Cap)``) and read per-instance
        ``protocol``. Passing a class — a common slip — would break
        both, so reject it loudly at registration time rather than at
        first capability lookup.
        """
        if not isinstance(connector, ReceiptParserConnector):
            raise ReceiptParserRegistryError(
                "register() expects a ReceiptParserConnector instance, got "
                f"{type(connector).__qualname__!s} "
                f"({connector!r}); did you forget to instantiate the class?"
            )
        proto = connector.protocol
        existing = self._connectors.get(proto)
        if existing is not None:
            raise ReceiptParserRegistryError(
                f"protocol {proto!r} already registered by "
                f"{type(existing).__qualname__}; refusing to overwrite "
                f"with {type(connector).__qualname__}"
            )
        self._connectors[proto] = connector
        # Any cached resolution is now stale.
        self._classes_by_key = None

    def get(self, protocol: ProtocolName) -> ReceiptParserConnector | None:
        """Return the connector registered under ``protocol`` (or ``None``)."""
        return self._connectors.get(protocol)

    def all(self) -> tuple[ReceiptParserConnector, ...]:
        """Return every registered connector in registration order."""
        return tuple(self._connectors.values())

    def with_capability(self, capability: type[T]) -> tuple[T, ...]:
        """Return every registered connector implementing ``capability``.

        ``capability`` must be a ``@runtime_checkable`` Protocol.
        Order matches registration order.
        """
        return tuple(c for c in self._connectors.values() if isinstance(c, capability))

    def classes_by_key(self) -> Mapping[str, type]:
        """Return the lazy ``{protocol_key: parser_class}`` mapping.

        Built by iterating every connector that implements
        :class:`ReceiptParserCapability`, calling its
        ``receipt_parser_keys()`` (cheap, metadata-only) to enumerate
        every key the connector claims, and validating the key set.
        Resolving a key to its concrete parser class via
        ``receipt_parser_class(key)`` is deferred to first
        ``mapping[key]`` access (see :class:`LazyParserClassMap`); the
        result is cached at both the map's per-key cache layer and at
        this method's cache layer. Registering a new connector
        invalidates the cache.

        Collision detection runs at map-build time (here): if two
        connectors publish the same key (e.g. ``"morpho"`` from both
        ``morpho_blue`` and a hypothetical ``morpho_v1``), this raises
        before the first parser-class import. Two keys on the same
        connector resolving to the same class is fine (the canonical
        alias pattern).

        The returned object is a read-only ``Mapping[str, type]``;
        callers must not mutate it. Callers needing a snapshot should
        ``dict(mapping)`` themselves — that **will** trigger the lazy
        resolution of every key, so use it sparingly (and never on the
        hot path).
        """
        if self._classes_by_key is None:
            self._classes_by_key = self._resolve_classes_by_key()
        return self._classes_by_key

    def _resolve_classes_by_key(self) -> LazyParserClassMap:
        """Build the ``{key: connector}`` map and wrap it lazily.

        Validates keys + detects collisions eagerly (every claim is
        checked at map-build time). Defers the actual parser-class
        import to first ``mapping[key]`` access via
        :class:`LazyParserClassMap`, so an unrelated connector's
        unimportable parser module never breaks a ``get_parser("spark")``
        lookup.
        """
        key_to_connector: dict[str, ReceiptParserConnector] = {}
        for connector in self._connectors.values():
            if not isinstance(connector, ReceiptParserCapability):
                continue
            keys = connector.receipt_parser_keys()
            if not isinstance(keys, frozenset) or not keys:
                raise ReceiptParserRegistryError(
                    f"{type(connector).__qualname__}.receipt_parser_keys() "
                    f"must return a non-empty frozenset, got {keys!r}"
                )
            for key in keys:
                if not isinstance(key, str) or not key:
                    raise ReceiptParserRegistryError(
                        f"{type(connector).__qualname__}.receipt_parser_keys() "
                        f"returned an invalid key {key!r} (must be a non-empty str)"
                    )
                existing_owner = key_to_connector.get(key)
                if existing_owner is not None and existing_owner is not connector:
                    raise ReceiptParserRegistryError(
                        f"protocol key {key!r} claimed by both "
                        f"{type(existing_owner).__qualname__} and "
                        f"{type(connector).__qualname__}"
                    )
                key_to_connector[key] = connector
        return LazyParserClassMap(key_to_connector)

    def clear(self) -> None:
        """Test helper — clear registrations. NOT used in production paths."""
        self._connectors.clear()
        self._classes_by_key = None


STRATEGY_RECEIPT_PARSER_REGISTRY: ReceiptParserConnectorRegistry = ReceiptParserConnectorRegistry()
