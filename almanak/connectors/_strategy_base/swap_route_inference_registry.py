"""Strategy-side swap route inference registry.

Connectors can claim protocol-less ``SwapIntent`` objects before the framework
falls back to its configured default swap protocol. This keeps token-shape
heuristics connector-owned: the framework asks "does any connector own this
swap?" without naming the concrete connector or its token prefixes.
"""

from __future__ import annotations

from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable

from almanak.connectors._base.types import ProtocolKind, ProtocolName

__all__ = [
    "SWAP_ROUTE_INFERENCE_REGISTRY",
    "SwapRouteInferenceCapability",
    "SwapRouteInferenceConnector",
    "SwapRouteInferenceRegistry",
    "SwapRouteInferenceRegistryError",
]


class SwapRouteInferenceRegistryError(Exception):
    """Registry contract violation."""


@runtime_checkable
class SwapRouteInferenceCapability(Protocol):
    """Connector declares whether it owns a protocol-less swap intent."""

    def claims_swap_route(self, intent: Any) -> bool: ...


class SwapRouteInferenceConnector:
    """Base class for strategy-side swap route inference connectors."""

    protocol: ClassVar[ProtocolName]
    kind: ClassVar[ProtocolKind]


T = TypeVar("T")


class SwapRouteInferenceRegistry:
    """In-process registry of connector-owned swap route inference hooks."""

    def __init__(self) -> None:
        self._connectors: dict[ProtocolName, SwapRouteInferenceConnector] = {}

    def register(self, connector: SwapRouteInferenceConnector) -> None:
        """Register a connector instance. Same-type re-registration is a no-op."""
        if not isinstance(connector, SwapRouteInferenceConnector):
            raise SwapRouteInferenceRegistryError(
                "register() expects a SwapRouteInferenceConnector instance, got "
                f"{type(connector).__qualname__!s} ({connector!r}); did you "
                "forget to instantiate the class?"
            )
        if not isinstance(connector, SwapRouteInferenceCapability):
            raise SwapRouteInferenceRegistryError(
                "register() expects a connector implementing SwapRouteInferenceCapability "
                f"in addition to SwapRouteInferenceConnector; {type(connector).__qualname__!s} "
                "is missing claims_swap_route()."
            )
        proto = connector.protocol
        existing = self._connectors.get(proto)
        if existing is not None:
            if type(existing) is type(connector):
                return
            raise SwapRouteInferenceRegistryError(
                f"protocol {proto!r} already registered by "
                f"{type(existing).__qualname__}; refusing to overwrite with "
                f"{type(connector).__qualname__}"
            )
        self._connectors[proto] = connector

    def infer_protocol(self, intent: Any) -> str | None:
        """Return the sole connector protocol that claims ``intent``.

        Multiple matches are treated as an architecture error. A protocol-less
        swap should have one owner before default routing, not an ordering race.
        Connector predicates must be total: a raising predicate blocks all
        protocol-less swap routing so the compiler never silently misroutes.
        """
        matches: list[ProtocolName] = []
        for connector in self._connectors.values():
            if not isinstance(connector, SwapRouteInferenceCapability):
                continue
            try:
                if connector.claims_swap_route(intent):
                    matches.append(connector.protocol)
            except Exception as exc:
                raise SwapRouteInferenceRegistryError(
                    f"swap route inference connector {connector.protocol!r} failed: {exc}"
                ) from exc

        if not matches:
            return None
        if len(matches) > 1:
            protocols = ", ".join(repr(str(protocol)) for protocol in matches)
            raise SwapRouteInferenceRegistryError(f"multiple connectors claim protocol-less swap route: {protocols}")
        return str(matches[0])

    def all(self) -> tuple[SwapRouteInferenceConnector, ...]:
        """Return every registered connector in registration order."""
        return tuple(self._connectors.values())

    def with_capability(self, capability: type[T]) -> tuple[T, ...]:
        """Return every registered connector implementing ``capability``."""
        return tuple(c for c in self._connectors.values() if isinstance(c, capability))

    def clear(self) -> None:
        """Test helper: clear registrations."""
        self._connectors.clear()


SWAP_ROUTE_INFERENCE_REGISTRY: SwapRouteInferenceRegistry = SwapRouteInferenceRegistry()
