"""Behaviour of :class:`ConnectorRegistry`.

Covers the singleton semantics ``register_connector`` relies on: idempotent
within one registration, hard-fail on duplicate name, deterministic
ordering from ``all()``, and the test-only ``_clear`` escape hatch.
"""

from __future__ import annotations

import pytest

from almanak.framework.connectors.registry import (
    ConnectorManifest,
    ConnectorRegistry,
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
