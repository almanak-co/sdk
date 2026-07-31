"""Tests for the derived ``SUPPORTED_PROTOCOLS`` matrix (VIB-4857 / W5).

The ``protocol -> {chains}`` matrix is derived directly from inline connector
``supported_chains`` declarations;
``almanak.framework.execution.config.SUPPORTED_PROTOCOLS`` is the back-compat
materialised view its consumers (``almanak.config.runtime`` protocol
validation, ``MultiChainRuntimeConfig._validate_protocols``) iterate.

These tests pin the *shape* and *copy semantics* of that view. The data itself
is owned by the connectors — there is deliberately no frozen snapshot of the
matrix here (a snapshot would re-create the central hand-maintained list W5
removed and would false-trip on every legitimate connector chain change). The
self-containment invariants live in
``tests/unit/connectors/test_supported_chains_descriptor_registry.py``.
"""

from __future__ import annotations


def test_derived_matrix_shape_matches_consumers() -> None:
    """Consumers iterate a ``dict[str, set[str]]`` with mutable set values.

    ``MultiChainRuntimeConfig._validate_protocols`` and
    ``almanak.config.runtime`` both do ``chain in SUPPORTED_PROTOCOLS[p]`` and
    ``sorted(SUPPORTED_PROTOCOLS[p])`` — so the value type must be a plain
    ``set`` (not ``frozenset``) to preserve the historical contract.
    """
    from almanak.framework.execution.config import SUPPORTED_PROTOCOLS

    assert isinstance(SUPPORTED_PROTOCOLS, dict)
    for protocol, chains in SUPPORTED_PROTOCOLS.items():
        assert isinstance(protocol, str)
        assert type(chains) is set


def test_registry_matrix_is_independent_copy() -> None:
    """Each ``supported_protocols_matrix()`` call returns fresh mutable sets.

    A consumer that mutates a value set (or the dict) must not corrupt the
    registry's cached frozensets or a later caller's view.
    """
    from almanak.connectors._connector import CONNECTOR_REGISTRY

    first = CONNECTOR_REGISTRY.supported_protocols_matrix()
    first["aave_v3"].add("__poison__")
    first["__poison_protocol__"] = {"nowhere"}

    second = CONNECTOR_REGISTRY.supported_protocols_matrix()
    assert "__poison__" not in second["aave_v3"]
    assert "__poison_protocol__" not in second


def test_runtime_map_covers_all_onchain_strategy_connectors_and_aliases() -> None:
    from almanak.connectors._connector import CONNECTOR_REGISTRY
    from almanak.framework.execution.config import SUPPORTED_PROTOCOLS

    assert SUPPORTED_PROTOCOLS == CONNECTOR_REGISTRY.supported_protocols_matrix()
    assert "kraken" not in SUPPORTED_PROTOCOLS
    for connector in CONNECTOR_REGISTRY.with_strategy_support():
        if connector.supported_chains is None or connector.supported_chains.is_offchain:
            continue
        for protocol in connector.protocol_keys:
            assert protocol in SUPPORTED_PROTOCOLS


def test_runtime_map_pins_intentional_narrowing_and_alias_override() -> None:
    from almanak.framework.execution.config import SUPPORTED_PROTOCOLS

    assert SUPPORTED_PROTOCOLS["lido"] == {"ethereum"}
    assert SUPPORTED_PROTOCOLS["enso"] == {
        "ethereum",
        "arbitrum",
        "optimism",
        "polygon",
        "base",
        "avalanche",
        "bsc",
    }
    # The alias is pinned to its own chain by `protocol_overrides` — it does not
    # inherit uniswap_v3's union. That both protocols have a real Mantle
    # deployment is not a conflict: UNISWAP_V3["mantle"] shares no contract with
    # AGNI_FINANCE["mantle"] (different factory, position manager, quoter,
    # router), so the runtime map admits mantle for each independently.
    assert SUPPORTED_PROTOCOLS["agni_finance"] == {"mantle"}
    assert SUPPORTED_PROTOCOLS["agni_finance"] < SUPPORTED_PROTOCOLS["uniswap_v3"]
    # mantle / xlayer / zerog are SWAP-only for uniswap_v3 — the runtime gate is
    # the per-protocol union, so they appear here; the per-intent narrowing is
    # asserted against `supported_chains_for(intent=...)` in
    # tests/unit/connectors/test_supported_chains_descriptor_registry.py.
    assert {"mantle", "xlayer", "zerog"} <= SUPPORTED_PROTOCOLS["uniswap_v3"]
    # linea is declared for no uniswap_v3 verb: no intent suite covers it.
    assert "linea" not in SUPPORTED_PROTOCOLS["uniswap_v3"]


def test_view_refreshes_after_a_registry_reset() -> None:
    """``CONNECTOR_REGISTRY.clear()`` must not leave this view answering from the old universe.

    A snapshot taken once at import keeps validating against the pre-reset
    matrix, so config accepts or rejects protocols the registry no longer
    agrees with — and every ``from ... import SUPPORTED_PROTOCOLS`` consumer
    inherits that stale answer.
    """
    from almanak.connectors._connector import CONNECTOR_REGISTRY
    from almanak.framework.execution.config import SUPPORTED_PROTOCOLS

    SUPPORTED_PROTOCOLS["__poison_protocol__"] = {"nowhere"}
    assert "__poison_protocol__" in SUPPORTED_PROTOCOLS

    CONNECTOR_REGISTRY.clear()

    assert "__poison_protocol__" not in SUPPORTED_PROTOCOLS
    assert SUPPORTED_PROTOCOLS == CONNECTOR_REGISTRY.supported_protocols_matrix()


def test_refresh_reaches_consumers_that_imported_the_name() -> None:
    """The refresh is in place, so a by-value re-export cannot go stale.

    ``almanak.framework.execution`` does ``from .config import
    SUPPORTED_PROTOCOLS``. Rebinding the module global would leave that
    binding — and every consumer that imported the name — pointing at the
    pre-reset object.
    """
    import almanak.framework.execution as execution_pkg
    from almanak.connectors._connector import CONNECTOR_REGISTRY
    from almanak.framework.execution.config import SUPPORTED_PROTOCOLS

    assert execution_pkg.SUPPORTED_PROTOCOLS is SUPPORTED_PROTOCOLS

    CONNECTOR_REGISTRY.clear()

    assert execution_pkg.SUPPORTED_PROTOCOLS == CONNECTOR_REGISTRY.supported_protocols_matrix()
