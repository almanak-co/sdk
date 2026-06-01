"""Unit tests for the strategy-side bridge provider registry (VIB-4837).

Proves the registry that ``BridgeCompiler._build_selector`` now dispatches
through:

* the global ``BRIDGE_PROVIDER_REGISTRY`` is populated at boot with the two
  built-in bridges in a byte-stable order (the selector candidate order — and
  therefore the selection for a given input — depends on it),
* ``build_default_bridge_selector`` builds a ``BridgeSelector`` over those
  adapters with the call-time ``token_resolver`` threaded through each one,
* duplicate registration is a hard error (no silent shadowing of one connector
  by another claiming the same bridge name), and
* ``BridgeCompiler._build_selector`` is genuinely decoupled — a fake factory
  registered into a fresh registry flows straight through it.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from almanak.connectors._strategy_base.bridge_compiler import BridgeCompiler
from almanak.connectors._strategy_base.bridge_registry import BridgeProviderRegistry

# Import the *singleton* from the boot file (not the bare definition site): the
# boot module runs ``_register_all()`` at import, so this guarantees the global
# registry is populated regardless of test/worker import order — mirroring how
# the compiler reaches it in production.
from almanak.connectors._strategy_bridge_registry import BRIDGE_PROVIDER_REGISTRY
from almanak.connectors.across.adapter import AcrossBridgeAdapter
from almanak.connectors.stargate.adapter import StargateBridgeAdapter
from almanak.framework.intents.bridge_selector import (
    BridgeSelector,
    build_default_bridge_selector,
)


def test_builtin_bridges_registered_in_stable_order() -> None:
    # (a) Order is load-bearing: it fixes the selector candidate order and
    # therefore the selection for a given input. Keep it pinned.
    assert BRIDGE_PROVIDER_REGISTRY.names() == ("across", "stargate")


def test_build_default_selector_builds_right_adapters_with_resolver_threaded() -> None:
    # (b) The factory returns a BridgeSelector over fresh Across/Stargate
    # adapters, in registration order, with the call-time resolver threaded
    # through each (not the get_token_resolver() singleton fallback).
    resolver = object()  # sentinel — must be threaded through unchanged
    selector = build_default_bridge_selector(resolver)

    assert isinstance(selector, BridgeSelector)
    assert [type(b).__name__ for b in selector.bridges] == [
        "AcrossBridgeAdapter",
        "StargateBridgeAdapter",
    ]
    assert isinstance(selector.bridges[0], AcrossBridgeAdapter)
    assert isinstance(selector.bridges[1], StargateBridgeAdapter)
    # The call-time resolver is threaded into each adapter.
    assert all(bridge._token_resolver is resolver for bridge in selector.bridges)


def test_build_all_mints_fresh_instances_per_call() -> None:
    resolver = object()
    first = BRIDGE_PROVIDER_REGISTRY.build_all(resolver)
    second = BRIDGE_PROVIDER_REGISTRY.build_all(resolver)
    # A fresh instance per call — selections must not share mutable candidates.
    assert all(a is not b for a, b in zip(first, second, strict=True))


def test_duplicate_name_is_rejected() -> None:
    # (c) A second connector claiming the same bridge name is a programming
    # error — fail loud, never silently shadow the first. Names are normalized
    # (stripped + lowercased), so a case-/whitespace-variant cannot bypass it.
    registry = BridgeProviderRegistry()
    registry.register(name="across", factory=AcrossBridgeAdapter)
    with pytest.raises(ValueError, match="already registered"):
        registry.register(name="across", factory=StargateBridgeAdapter)
    with pytest.raises(ValueError, match="already registered"):
        registry.register(name="  ACROSS  ", factory=StargateBridgeAdapter)


def test_name_is_normalized_on_registration() -> None:
    # A non-canonical name (mixed case + surrounding whitespace) is stored as the
    # canonical lowercase slug, so names() / build order stay byte-stable.
    registry = BridgeProviderRegistry()
    registry.register(name="  Across  ", factory=AcrossBridgeAdapter)
    assert registry.names() == ("across",)


def test_blank_name_is_rejected() -> None:
    registry = BridgeProviderRegistry()
    with pytest.raises(ValueError, match="non-empty"):
        registry.register(name="   ", factory=AcrossBridgeAdapter)


def test_register_preserves_insertion_order() -> None:
    registry = BridgeProviderRegistry()
    registry.register(name="across", factory=AcrossBridgeAdapter)
    registry.register(name="stargate", factory=StargateBridgeAdapter)
    assert registry.names() == ("across", "stargate")
    assert registry.factories() == (AcrossBridgeAdapter, StargateBridgeAdapter)


def test_clear_empties_the_registry() -> None:
    registry = BridgeProviderRegistry()
    registry.register(name="across", factory=AcrossBridgeAdapter)
    registry.clear()
    assert registry.names() == ()
    assert registry.build_all(object()) == []


class _FakeBridge:
    """Minimal duck-typed bridge stand-in whose factory records its resolver.

    ``_build_selector`` only constructs the ``BridgeSelector`` (which reads
    ``.name``); it never invokes the quote/route machinery, so a full
    ``BridgeAdapter`` subclass is unnecessary here.
    """

    def __init__(self, *, token_resolver: Any) -> None:
        self.token_resolver = token_resolver

    @property
    def name(self) -> str:
        return "fake"


def test_fake_factory_flows_through_build_selector(monkeypatch: pytest.MonkeyPatch) -> None:
    # (d) Prove the decoupling end-to-end: whatever the registry holds is what
    # _build_selector produces. build_default_bridge_selector imports
    # BRIDGE_PROVIDER_REGISTRY from the boot module *at call time*, so swapping
    # that module attribute for a fresh registry is sufficient — no need to
    # mutate the real global (and monkeypatch restores it after the test).
    fresh = BridgeProviderRegistry()
    fresh.register(name="fake", factory=_FakeBridge)
    monkeypatch.setattr(
        "almanak.connectors._strategy_bridge_registry.BRIDGE_PROVIDER_REGISTRY",
        fresh,
    )

    sentinel = object()
    ctx = SimpleNamespace(token_resolver=sentinel)
    selector = BridgeCompiler()._build_selector(ctx)

    assert [bridge.name for bridge in selector.bridges] == ["fake"]
    assert isinstance(selector.bridges[0], _FakeBridge)
    assert selector.bridges[0].token_resolver is sentinel
