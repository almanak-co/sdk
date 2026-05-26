"""Behaviour of :class:`ConnectorRegistry`.

Covers the singleton semantics ``register_connector`` relies on: idempotent
within one registration, hard-fail on duplicate name, deterministic
ordering from ``all()``, and the test-only ``_clear`` escape hatch.
"""

from __future__ import annotations

import sys
import types
from pkgutil import ModuleInfo

import pytest

from almanak.connectors._strategy_base.registry import (
    ConnectorManifest,
    ConnectorRegistry,
    _import_one_connector,
    _is_protocol_leaf,
    register_connector,
)
from almanak.framework.intents.vocabulary import IntentType


def _manifest(name: str = "x", intent: IntentType = IntentType.SWAP) -> ConnectorManifest:
    return ConnectorManifest(name=name, intents=(intent,), chains=("ethereum",))


def test_register_then_all_round_trip() -> None:
    m = _manifest()
    ConnectorRegistry.register(m)
    assert ConnectorRegistry.all() == (m,)


def test_all_is_sorted_by_name() -> None:
    ConnectorRegistry.register(_manifest("uniswap_v3"))
    ConnectorRegistry.register(_manifest("aave_v3"))
    ConnectorRegistry.register(_manifest("morpho_blue"))
    names = [m.name for m in ConnectorRegistry.all()]
    assert names == ["aave_v3", "morpho_blue", "uniswap_v3"]


def test_get_returns_registered_manifest() -> None:
    m = _manifest("kraken")
    ConnectorRegistry.register(m)
    assert ConnectorRegistry.get("kraken") is m


def test_get_returns_none_for_unknown_name() -> None:
    assert ConnectorRegistry.get("nonexistent") is None


def test_names_returns_frozen_set_of_registered_names() -> None:
    ConnectorRegistry.register(_manifest("aave_v3"))
    ConnectorRegistry.register(_manifest("uniswap_v3"))
    names = ConnectorRegistry.names()
    assert names == frozenset({"aave_v3", "uniswap_v3"})
    assert isinstance(names, frozenset)


def test_duplicate_registration_raises() -> None:
    ConnectorRegistry.register(_manifest("aave_v3"))
    with pytest.raises(ValueError, match=r"already registered"):
        ConnectorRegistry.register(_manifest("aave_v3"))


def test_duplicate_message_includes_existing_manifest() -> None:
    # The existing manifest is included in the error so the author can see
    # WHAT they're clashing with — useful when the duplicate comes from a
    # copy-paste of another connector's __init__.py.
    first = _manifest("aave_v3", IntentType.SUPPLY)
    ConnectorRegistry.register(first)
    with pytest.raises(ValueError) as exc:
        ConnectorRegistry.register(_manifest("aave_v3", IntentType.SWAP))
    assert "SUPPLY" in str(exc.value)


def test_register_connector_function_populates_registry() -> None:
    register_connector(
        name="aave_v3",
        intents=(IntentType.SUPPLY, IntentType.BORROW),
        chains=("ethereum", "arbitrum"),
    )
    m = ConnectorRegistry.get("aave_v3")
    assert m is not None
    assert m.intents == (IntentType.SUPPLY, IntentType.BORROW)
    assert m.chains == ("ethereum", "arbitrum")


def test_register_connector_function_validates_at_call_site() -> None:
    # Validation happens in ConnectorManifest.__post_init__, BEFORE registration.
    # A failed registration must not pollute the registry.
    with pytest.raises(ValueError):
        register_connector(name="x", intents=(), chains=("ethereum",))
    assert ConnectorRegistry.names() == frozenset()


def test_clear_empties_the_registry() -> None:
    ConnectorRegistry.register(_manifest("aave_v3"))
    ConnectorRegistry.register(_manifest("uniswap_v3"))
    ConnectorRegistry._clear()
    assert ConnectorRegistry.all() == ()
    assert ConnectorRegistry.names() == frozenset()


def test_off_chain_connector_registers_with_chains_none() -> None:
    register_connector(name="kraken", intents=(IntentType.SWAP,), chains=None)
    m = ConnectorRegistry.get("kraken")
    assert m is not None
    assert m.chains is None


# --- helpers powering _import_all_connectors -------------------------------
#
# These cover the two functions extracted in VIB-4835 Phase 2 cleanup so the
# CI-only loop in _import_all_connectors stays cheap on CRAP. Each helper is
# focused enough to unit-test directly without faking the full
# `pkgutil.iter_modules` walk.


def _info(name: str, *, ispkg: bool = True) -> ModuleInfo:
    """Minimal stand-in for the ``ModuleInfo`` namedtuple ``iter_modules`` yields."""
    return ModuleInfo(module_finder=None, name=name, ispkg=ispkg)


def test_is_protocol_leaf_accepts_named_subpackage() -> None:
    assert _is_protocol_leaf(_info("uniswap_v3")) is True


def test_is_protocol_leaf_rejects_non_package_module() -> None:
    assert _is_protocol_leaf(_info("helpers", ispkg=False)) is False


def test_is_protocol_leaf_rejects_underscore_prefixed_foundation() -> None:
    assert _is_protocol_leaf(_info("_strategy_base")) is False
    assert _is_protocol_leaf(_info("_gateway_registry")) is False


def _install_fake_subpackage(monkeypatch: pytest.MonkeyPatch, dotted: str, mod: types.ModuleType) -> None:
    """Make ``importlib.import_module(dotted)`` resolve to ``mod`` for the duration of the test."""
    monkeypatch.setitem(sys.modules, dotted, mod)


def test_import_one_connector_eager_registration_no_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    # Eager-registered connectors register at module import and have no _register_once.
    mod = types.ModuleType("almanak.connectors._fake_eager")
    _install_fake_subpackage(monkeypatch, "almanak.connectors._fake_eager", mod)
    assert _import_one_connector("almanak.connectors", "_fake_eager") == []


def test_import_one_connector_lazy_calls_register_once(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = types.ModuleType("almanak.connectors._fake_lazy")
    calls: list[int] = []

    def _register_once() -> None:
        calls.append(1)

    mod._register_once = _register_once  # type: ignore[attr-defined]
    _install_fake_subpackage(monkeypatch, "almanak.connectors._fake_lazy", mod)

    assert _import_one_connector("almanak.connectors", "_fake_lazy") == []
    assert calls == [1]


def test_import_one_connector_import_failure_is_reported_and_skips_register() -> None:
    # No fake module installed — importlib will raise ModuleNotFoundError.
    errors = _import_one_connector("almanak.connectors", "_does_not_exist_xyz")
    assert len(errors) == 1
    assert "_does_not_exist_xyz" in errors[0]
    assert "ModuleNotFoundError" in errors[0]


def test_import_one_connector_register_once_failure_is_reported(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = types.ModuleType("almanak.connectors._fake_broken")

    def _register_once() -> None:
        raise RuntimeError("boom")

    mod._register_once = _register_once  # type: ignore[attr-defined]
    _install_fake_subpackage(monkeypatch, "almanak.connectors._fake_broken", mod)

    errors = _import_one_connector("almanak.connectors", "_fake_broken")
    assert len(errors) == 1
    assert "_fake_broken._register_once" in errors[0]
    assert "RuntimeError" in errors[0]
    assert "boom" in errors[0]


def test_import_one_connector_ignores_non_callable_register_once_attr(monkeypatch: pytest.MonkeyPatch) -> None:
    # If a connector happens to expose ``_register_once`` as a non-callable
    # (string, module re-export, etc.), the helper must not try to call it.
    mod = types.ModuleType("almanak.connectors._fake_attr")
    mod._register_once = "not callable"  # type: ignore[attr-defined]
    _install_fake_subpackage(monkeypatch, "almanak.connectors._fake_attr", mod)

    assert _import_one_connector("almanak.connectors", "_fake_attr") == []
