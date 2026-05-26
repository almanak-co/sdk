"""Aerodrome ConnectorRegistry registration (VIB-4468 §W5).

The CI gates ``scripts/ci/check_connector_registry.py`` and
``scripts/ci/check_intent_coverage.py`` enforce the *shape* of the
registry (no duplicate names, every required ``(connector, intent, chain)``
triple has a test). Neither pins which *specific* chains a given connector
declares — so a regression that reverts the Optimism flip would pass both
gates by simply having fewer required triples.

This file is the regression guard: Aerodrome must register both
``base`` (native deployment) and ``optimism`` (Velodrome V2 alias).
"""

from almanak.connectors._strategy_base.registry import ConnectorRegistry
from almanak.framework.intents.vocabulary import IntentType


def _force_aerodrome_registration() -> None:
    """Touch a public symbol on the connector to fire its lazy registration.

    VIB-4835 made every connector's ``__init__.py`` PEP 562 lazy:
    importing the package no longer triggers ``register_connector`` on
    its own — that fires on first attribute access. The
    ``AerodromeAdapter`` lookup below is the canonical "I'm a real
    strategy-side caller" shape and is enough to flip the deferred
    registration.
    """
    from almanak.connectors.aerodrome import AerodromeAdapter  # noqa: F401


def test_aerodrome_registers_base_and_optimism() -> None:
    _force_aerodrome_registration()

    manifest = ConnectorRegistry.get("aerodrome")
    assert manifest is not None, "aerodrome must be registered"
    assert manifest.chains == ("base", "optimism"), (
        f"aerodrome.chains must be ('base', 'optimism') — got {manifest.chains!r}. "
        "Optimism is required (Velodrome V2 alias); reverting to ('base',) loses "
        "intent-coverage attribution for Velodrome via the alias map."
    )


def test_aerodrome_intents_unchanged_by_optimism_flip() -> None:
    _force_aerodrome_registration()

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
