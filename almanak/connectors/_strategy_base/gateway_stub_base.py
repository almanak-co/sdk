"""Strategy-side base type for connector-published gRPC client stubs (VIB-4989).

The framework's universal gateway channel (``framework/gateway_client.py``
``GatewayClient``) is connector-agnostic plumbing — it owns the gRPC channel and
the core service stubs. A connector that ships its **own** gRPC service (e.g.
Polymarket's ``PolymarketService``) historically forced the framework client to
import that connector's generated proto stub and expose a hardcoded ``.polymarket``
property, leaking the connector into the framework (VIB-4989, part of VIB-4851).

This module owns the venue-neutral half of the seam: a connector publishes a
:class:`GatewayStubSpec` naming its service and a factory that builds its client
stub from a channel. The framework's ``GatewayClient`` builds the connector stubs
generically at connect time and exposes them by name
(``client.connector_stub("polymarket")``), so adding a connector with its own gRPC
service is one folder + one ``_SPEC_LOADERS`` row — no framework edit. This is the
strategy-side client mirror of the gateway-side ``GatewayServicerCapability`` /
``GATEWAY_REGISTRY`` pattern.

Gateway-boundary note: this module performs no network egress; it only *describes*
a stub factory. The connector spec module imports the connector's generated proto
(pure codegen, no egress at import). The channel passed to ``stub_factory`` is the
framework-owned gateway channel — the connector never opens its own.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

__all__ = ["GatewayStubSpec"]

# Builds a connector's gRPC client stub from a channel: ``(grpc.Channel) -> Stub``.
# Typed loosely (``Any``) so this ``_strategy_base`` module need not import grpc or
# any connector's generated proto types.
StubFactory = Callable[[Any], Any]


@dataclass(frozen=True)
class GatewayStubSpec:
    """Connector-published descriptor: a named gRPC client stub factory.

    Attributes:
        service_name: The lookup key the framework client exposes the stub under
            (e.g. ``"polymarket"`` → ``client.connector_stub("polymarket")``). Must
            be a non-empty string; collides loudly if two connectors claim the same
            name (the registry cannot silently pick a side).
        stub_factory: ``(grpc.Channel) -> Stub``. Builds the connector's client
            stub from the framework-owned gateway channel. Called once per
            ``GatewayClient.connect()``.
    """

    service_name: str
    stub_factory: StubFactory

    def __post_init__(self) -> None:
        if not isinstance(self.service_name, str) or not self.service_name.strip():
            raise TypeError(f"service_name must be a non-empty, non-whitespace string, got {self.service_name!r}.")
        if not callable(self.stub_factory):
            raise TypeError(f"stub_factory must be callable, got {type(self.stub_factory).__name__}.")
