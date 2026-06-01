"""Tests for the derived ``SUPPORTED_PROTOCOLS`` matrix (VIB-4857 / W5).

The ``protocol -> {chains}`` matrix is derived from per-connector
``supported_chains.py`` declarations aggregated by the strategy-side
``SupportedChainsRegistry``;
``almanak.framework.execution.config.SUPPORTED_PROTOCOLS`` is the back-compat
materialised view its consumers (``almanak.config.runtime`` protocol
validation, ``MultiChainRuntimeConfig._validate_protocols``) iterate.

These tests pin the *shape* and *copy semantics* of that view. The data itself
is owned by the connectors — there is deliberately no frozen snapshot of the
matrix here (a snapshot would re-create the central hand-maintained list W5
removed and would false-trip on every legitimate connector chain change). The
self-containment invariants live in
``tests/unit/connectors/test_supported_chains_registry.py``.
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
    from almanak.connectors._strategy_base.supported_chains_registry import (
        supported_protocols_matrix,
    )

    first = supported_protocols_matrix()
    first["aave_v3"].add("__poison__")
    first["__poison_protocol__"] = {"nowhere"}

    second = supported_protocols_matrix()
    assert "__poison__" not in second["aave_v3"]
    assert "__poison_protocol__" not in second
