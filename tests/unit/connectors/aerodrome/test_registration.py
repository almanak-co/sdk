"""Aerodrome ConnectorRegistry descriptor registration (VIB-4468 §W5).

The CI gates ``scripts/ci/check_connector_registry.py`` and
``scripts/ci/check_intent_coverage.py`` enforce the *shape* of the
registry (no duplicate names, every required ``(connector, intent, chain)``
triple has a test). Neither pins which *specific* chains a given connector
declares, so a regression that reverts the Optimism flip would pass both
gates by simply having fewer required triples.

This file is the regression guard: Aerodrome must declare both
``base`` (native deployment) and ``optimism`` (Velodrome V2 alias).
"""

from collections.abc import Iterator

import pytest

from almanak.connectors._strategy_base.registry import (
    ConnectorRegistry,
    _register_descriptor_connectors,
)
from almanak.framework.intents.vocabulary import IntentType


@pytest.fixture(autouse=True)
def _clear_connector_registry() -> Iterator[None]:
    ConnectorRegistry._clear()
    yield
    ConnectorRegistry._clear()


def _hydrate_descriptor_registration() -> None:
    """Populate strategy manifests from connector.py descriptors.

    Strategy registration is descriptor-owned now; package lazy access no
    longer fires registration side effects.
    """
    _register_descriptor_connectors()


def test_aerodrome_registers_base_and_optimism() -> None:
    _hydrate_descriptor_registration()

    manifest = ConnectorRegistry.get("aerodrome")
    assert manifest is not None, "aerodrome must be registered"
    assert manifest.chains == ("base", "optimism"), (
        f"aerodrome.chains must be ('base', 'optimism'); got {manifest.chains!r}. "
        "Optimism is required (Velodrome V2 alias); reverting to ('base',) loses "
        "intent-coverage attribution for Velodrome via the alias map."
    )


def test_aerodrome_intents_unchanged_by_optimism_flip() -> None:
    _hydrate_descriptor_registration()

    manifest = ConnectorRegistry.get("aerodrome")
    assert manifest is not None
    assert set(manifest.intents) == {
        IntentType.SWAP,
        IntentType.LP_OPEN,
        IntentType.LP_CLOSE,
    }, (
        "Adding Optimism to chains must not change the intent set. "
        "LP_COLLECT_FEES still ships under the separate aerodrome_slipstream "
        "literal, not under the aerodrome connector."
    )
