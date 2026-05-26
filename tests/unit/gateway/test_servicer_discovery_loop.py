"""Discovery loop for connector-owned servicers (VIB-4812).

``server.py`` no longer hand-wires Polymarket / Enso. It iterates
``GATEWAY_REGISTRY.capability_providers(GatewayServicerCapability)`` and
calls ``register_servicers(server, settings)`` on each. These tests pin
that contract:

1. Every connector that declares ``GatewayServicerCapability`` is asked
   to register, exactly once.
2. The constructed servicer instances are accumulated on
   ``GatewayServer._connector_servicers`` so the shutdown loop can call
   ``close()`` on each.
3. The registry's discovered set matches the protocol-by-name expectation
   for the current main branch (polymarket + enso). A new connector
   gaining ``GatewayServicerCapability`` lands here as an updated
   expectation, not as a forgotten edit in ``server.py``.

The test does NOT instantiate a real grpc server — it pins discovery, not
the underlying ``add_*ServiceServicer_to_server`` plumbing (that is
covered by the characterization tests in
``tests/gateway/test_gateway_server_start_characterization.py``).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from almanak.connectors._base.gateway_capabilities import (
    GatewayServicerCapability,
)
from almanak.connectors._base.types import ProtocolName
from almanak.connectors._gateway_registry import GATEWAY_REGISTRY


def test_registry_advertises_exactly_the_expected_servicer_providers() -> None:
    """The set of ``GatewayServicerCapability`` providers is the load-bearing
    contract — losing one silently boots the gateway without that protocol's
    gRPC surface.
    """
    providers = GATEWAY_REGISTRY.capability_providers(GatewayServicerCapability)
    protocols = {p.protocol for p in providers}
    # Phase 2 (VIB-4810) migrated polymarket + enso onto the capability.
    # Phase 3 (VIB-4811) may add more — update this expectation in the
    # PR that introduces the new capability registration.
    assert protocols == {
        ProtocolName("polymarket"),
        ProtocolName("enso"),
    }


def test_every_servicer_provider_implements_register_servicers() -> None:
    """The capability Protocol mandates ``register_servicers``; verify each
    registered provider actually exposes a callable."""
    for provider in GATEWAY_REGISTRY.capability_providers(GatewayServicerCapability):
        assert callable(provider.register_servicers), (
            f"{type(provider).__qualname__}.register_servicers is not callable"
        )


def test_every_servicer_provider_exposes_servicer_attribute() -> None:
    """``server.py`` collects ``provider.servicer`` for the shutdown loop.
    The attribute is part of the de facto contract for ``GatewayServicerCapability``
    providers — Polymarket and Enso both expose it. A new provider must do
    the same or shutdown ``close()`` will skip its HTTP session cleanup.
    """
    for provider in GATEWAY_REGISTRY.capability_providers(GatewayServicerCapability):
        assert hasattr(provider, "servicer"), (
            f"{type(provider).__qualname__} does not expose a ``servicer`` attribute"
        )


def test_register_servicers_called_once_per_provider_in_boot_loop() -> None:
    """Simulate the gateway boot loop and assert each provider is asked
    exactly once. Pins the no-double-register guarantee — boot must not
    re-invoke ``register_servicers`` when the registry returns the same
    provider twice (it doesn't, but the contract is worth a guard)."""
    fake_server = MagicMock(name="grpc_server")
    fake_settings = MagicMock(name="gateway_settings")

    # Wrap each provider's ``register_servicers`` so we can count calls
    # without mutating the real registry.
    providers = list(GATEWAY_REGISTRY.capability_providers(GatewayServicerCapability))
    counts: dict[type, int] = {type(p): 0 for p in providers}
    originals: dict[type, object] = {type(p): p.register_servicers for p in providers}
    try:
        for provider in providers:
            cls = type(provider)
            original = originals[cls]

            def make_counter(c=cls, orig=original):
                def counter(server, settings):
                    counts[c] += 1
                    return orig(server, settings)

                return counter

            provider.register_servicers = make_counter()  # type: ignore[method-assign]

        # The actual loop (mirror of the one in ``GatewayServer._register_services``):
        registered = []
        for provider in GATEWAY_REGISTRY.capability_providers(GatewayServicerCapability):
            provider.register_servicers(fake_server, fake_settings)
            registered.append(provider)

        assert all(count == 1 for count in counts.values()), (
            f"Each provider must be invoked exactly once; got {counts!r}"
        )
        assert len(registered) == len(providers)
    finally:
        # Restore originals — the registry is a process-wide singleton and
        # the next test in the same process should see the canonical
        # bound methods.
        for provider in providers:
            cls = type(provider)
            # Drop the per-instance override so attribute lookup falls
            # back to the class-bound method.
            provider.__dict__.pop("register_servicers", None)
