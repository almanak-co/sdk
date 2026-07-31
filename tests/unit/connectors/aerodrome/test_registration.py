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

from almanak.connectors._connector import CONNECTOR_REGISTRY


def test_aerodrome_registers_base_and_optimism() -> None:
    connector = CONNECTOR_REGISTRY.get("aerodrome")
    assert connector is not None, "aerodrome must be registered"
    assert connector.supported_chains_for_protocol("aerodrome") == ("base", "optimism"), (
        "aerodrome supported chains must be ('base', 'optimism'); got "
        f"{connector.supported_chains_for_protocol('aerodrome')!r}. "
        "Optimism is required (Velodrome V2 alias); reverting to ('base',) loses "
        "intent-coverage attribution for Velodrome via the alias map."
    )


def test_aerodrome_intents_unchanged_by_optimism_flip() -> None:
    connector = CONNECTOR_REGISTRY.get("aerodrome")
    assert connector is not None
    assert set(connector.strategy_intent_names or ()) == {
        "SWAP",
        "LP_OPEN",
        "LP_CLOSE",
    }, (
        "Adding Optimism to chains must not change the intent set. "
        "LP_COLLECT_FEES still ships under the separate aerodrome_slipstream "
        "literal, not under the aerodrome connector."
    )
