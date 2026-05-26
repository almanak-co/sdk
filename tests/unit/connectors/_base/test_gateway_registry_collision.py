"""``GatewayConnectorRegistry`` contract tests.

* Collision on ``ProtocolName`` raises ``GatewayRegistryError``.
* ``get`` returns ``None`` for unregistered protocols.
* ``capability_providers`` filters by ``isinstance``.
* ``all`` and ``capability_providers`` preserve registration order.
"""

from __future__ import annotations

from typing import Any, ClassVar

import pytest

from almanak.connectors._base.gateway_capabilities import (
    GatewayMarketLookupCapability,
    GatewayServicerCapability,
)
from almanak.connectors._base.gateway_connector import GatewayConnector
from almanak.connectors._base.gateway_registry import (
    GatewayConnectorRegistry,
    GatewayRegistryError,
)
from almanak.connectors._base.types import ProtocolKind, ProtocolName


class _ServicerConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("alpha")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PREDICTION_MARKET
    servicer: Any | None = None

    def register_servicers(self, server: Any, settings: Any) -> None:
        pass


class _LookupConnector(GatewayConnector):
    protocol: ClassVar[ProtocolName] = ProtocolName("beta")
    kind: ClassVar[ProtocolKind] = ProtocolKind.LENDING

    def market_lookup(self) -> Any:
        return object()


class _Collider(GatewayConnector):
    """Same ``protocol`` as ``_ServicerConnector`` — should collide."""

    protocol: ClassVar[ProtocolName] = ProtocolName("alpha")
    kind: ClassVar[ProtocolKind] = ProtocolKind.PREDICTION_MARKET
    servicer: Any | None = None

    def register_servicers(self, server: Any, settings: Any) -> None:
        pass


def test_register_and_get() -> None:
    reg = GatewayConnectorRegistry()
    inst = _ServicerConnector()
    reg.register(inst)
    assert reg.get(ProtocolName("alpha")) is inst
    assert reg.get(ProtocolName("unknown")) is None


def test_collision_raises() -> None:
    reg = GatewayConnectorRegistry()
    reg.register(_ServicerConnector())
    with pytest.raises(GatewayRegistryError, match="alpha"):
        reg.register(_Collider())


def test_collision_does_not_overwrite() -> None:
    """A collision must leave the original registration intact."""
    reg = GatewayConnectorRegistry()
    original = _ServicerConnector()
    reg.register(original)
    with pytest.raises(GatewayRegistryError):
        reg.register(_Collider())
    assert reg.get(ProtocolName("alpha")) is original


def test_capability_providers_filters() -> None:
    reg = GatewayConnectorRegistry()
    servicer = _ServicerConnector()
    lookup = _LookupConnector()
    reg.register(servicer)
    reg.register(lookup)
    assert reg.capability_providers(GatewayServicerCapability) == (servicer,)
    assert reg.capability_providers(GatewayMarketLookupCapability) == (lookup,)


def test_all_returns_registration_order() -> None:
    reg = GatewayConnectorRegistry()
    a = _ServicerConnector()
    b = _LookupConnector()
    reg.register(a)
    reg.register(b)
    assert reg.all() == (a, b)


def test_clear_drops_all() -> None:
    reg = GatewayConnectorRegistry()
    reg.register(_ServicerConnector())
    reg.register(_LookupConnector())
    reg.clear()
    assert reg.all() == ()
    assert reg.get(ProtocolName("alpha")) is None


def test_register_rejects_class_instead_of_instance() -> None:
    """Passing a class (a common slip) must raise, not silently store."""
    reg = GatewayConnectorRegistry()
    with pytest.raises(GatewayRegistryError, match="instance"):
        reg.register(_ServicerConnector)  # type: ignore[arg-type]
    assert reg.all() == ()


def test_register_rejects_non_gateway_connector() -> None:
    """Random objects that happen to carry a ``protocol`` attr must raise."""
    reg = GatewayConnectorRegistry()

    class _Imposter:
        protocol = ProtocolName("alpha")
        kind = ProtocolKind.LENDING

    with pytest.raises(GatewayRegistryError, match="instance"):
        reg.register(_Imposter())  # type: ignore[arg-type]
    assert reg.all() == ()
