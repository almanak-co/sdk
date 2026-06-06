"""Strategy-side deferred transaction refresh registry.

Aggregator connectors can compile transactions whose calldata goes stale before
execution. They publish refresh providers here so the framework execution path
can ask for fresh calldata without importing concrete connector adapters.
"""

from __future__ import annotations

from typing import Any, ClassVar, Protocol, TypeVar, runtime_checkable

from almanak.connectors._base.types import ProtocolKind, ProtocolName

__all__ = [
    "DEFERRED_REFRESH_REGISTRY",
    "DeferredRefreshCapability",
    "DeferredRefreshConnector",
    "DeferredRefreshRegistry",
    "DeferredRefreshRegistryError",
]


class DeferredRefreshRegistryError(Exception):
    """Registry contract violation."""


@runtime_checkable
class DeferredRefreshCapability(Protocol):
    """Connector refreshes stale transaction calldata for a deferred bundle."""

    def refresh_transaction(
        self,
        metadata: dict[str, Any],
        wallet_address: str,
        *,
        rpc_url: str | None = None,
    ) -> dict[str, Any]: ...


class DeferredRefreshConnector:
    """Base class for strategy-side deferred refresh connector instances."""

    protocol: ClassVar[ProtocolName]
    kind: ClassVar[ProtocolKind]


T = TypeVar("T")


class DeferredRefreshRegistry:
    """In-process registry of connector-owned deferred refresh hooks."""

    def __init__(self) -> None:
        self._connectors: dict[ProtocolName, DeferredRefreshConnector] = {}

    def register(self, connector: DeferredRefreshConnector) -> None:
        """Register a connector instance. Same-type re-registration is a no-op."""
        if not isinstance(connector, DeferredRefreshConnector):
            raise DeferredRefreshRegistryError(
                "register() expects a DeferredRefreshConnector instance, got "
                f"{type(connector).__qualname__!s} ({connector!r}); did you "
                "forget to instantiate the class?"
            )
        if not isinstance(connector, DeferredRefreshCapability):
            raise DeferredRefreshRegistryError(
                "register() expects a connector implementing DeferredRefreshCapability "
                f"in addition to DeferredRefreshConnector; {type(connector).__qualname__!s} "
                "is missing refresh_transaction()."
            )
        proto = connector.protocol
        existing = self._connectors.get(proto)
        if existing is not None:
            if type(existing) is type(connector):
                return
            raise DeferredRefreshRegistryError(
                f"protocol {proto!r} already registered by "
                f"{type(existing).__qualname__}; refusing to overwrite with "
                f"{type(connector).__qualname__}"
            )
        self._connectors[proto] = connector

    def lookup(self, protocol: str) -> DeferredRefreshCapability | None:
        """Return the refresh capability for ``protocol``, or ``None``."""
        connector = self._connectors.get(ProtocolName(protocol))
        if connector is None:
            return None
        if not isinstance(connector, DeferredRefreshCapability):
            return None
        return connector

    def refresh_transaction(
        self,
        protocol: str,
        metadata: dict[str, Any],
        wallet_address: str,
        *,
        rpc_url: str | None = None,
    ) -> dict[str, Any]:
        """Refresh transaction calldata through the connector that owns ``protocol``."""
        capability = self.lookup(protocol)
        if capability is None:
            raise DeferredRefreshRegistryError(f"protocol {protocol!r} does not publish deferred refresh")
        return capability.refresh_transaction(metadata, wallet_address, rpc_url=rpc_url)

    def all(self) -> tuple[DeferredRefreshConnector, ...]:
        """Return every registered connector in registration order."""
        return tuple(self._connectors.values())

    def with_capability(self, capability: type[T]) -> tuple[T, ...]:
        """Return every registered connector implementing ``capability``."""
        return tuple(c for c in self._connectors.values() if isinstance(c, capability))

    def clear(self) -> None:
        """Test helper: clear registrations."""
        self._connectors.clear()


DEFERRED_REFRESH_REGISTRY: DeferredRefreshRegistry = DeferredRefreshRegistry()
