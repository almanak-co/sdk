"""Gateway-side connector registry.

The single global instance lives at
``almanak.connectors._gateway_registry.GATEWAY_REGISTRY``. Concrete
connectors are registered there; the gateway boot loop asks the
registry for capability providers and never instantiates connectors
itself.

Collision is a hard error: two ``register`` calls with the same
``ProtocolName`` raise ``GatewayRegistryError``. Silent overwrite would
make protocol identity ambiguous and undermine the 1:1 deployment
identity model (CLAUDE.md §"1 Gateway : 1 Strategy").

Strategy-side code MUST NOT import this module.
"""

from __future__ import annotations

from typing import TypeVar

from .gateway_connector import GatewayConnector
from .types import ProtocolName

__all__ = [
    "GatewayConnectorRegistry",
    "GatewayRegistryError",
]


class GatewayRegistryError(Exception):
    """Registry contract violation (collision, unknown protocol, etc.)."""


T = TypeVar("T")


class GatewayConnectorRegistry:
    """In-process registry of gateway-side connectors keyed by ``ProtocolName``."""

    def __init__(self) -> None:
        self._connectors: dict[ProtocolName, GatewayConnector] = {}

    def register(self, connector: GatewayConnector) -> None:
        """Register a connector instance. Collision on protocol raises.

        The registry stores instances (not classes) so it can dispatch
        capability calls (``isinstance(connector, Cap)``) and read
        per-instance ``protocol`` / ``kind``. Passing a class — a common
        slip — would break both, so reject it loudly at registration
        time rather than at first capability lookup.
        """
        if not isinstance(connector, GatewayConnector):
            raise GatewayRegistryError(
                "register() expects a GatewayConnector instance, got "
                f"{type(connector).__qualname__!s} "
                f"({connector!r}); did you forget to instantiate the class?"
            )
        proto = connector.protocol
        existing = self._connectors.get(proto)
        if existing is not None:
            raise GatewayRegistryError(
                f"protocol {proto!r} already registered by "
                f"{type(existing).__qualname__}; refusing to overwrite "
                f"with {type(connector).__qualname__}"
            )
        self._connectors[proto] = connector

    def get(self, protocol: ProtocolName) -> GatewayConnector | None:
        """Return the connector for ``protocol`` or ``None`` if unregistered."""
        return self._connectors.get(protocol)

    def all(self) -> tuple[GatewayConnector, ...]:
        """Return every registered connector in registration order."""
        return tuple(self._connectors.values())

    def capability_providers(self, capability: type[T]) -> tuple[T, ...]:
        """Return every registered connector that implements ``capability``.

        ``capability`` must be a ``@runtime_checkable`` Protocol (one of
        the ``Gateway*Capability`` classes). Order matches registration
        order.
        """
        return tuple(c for c in self._connectors.values() if isinstance(c, capability))

    def clear(self) -> None:
        """Test helper — clear registrations. NOT used in production paths."""
        self._connectors.clear()
