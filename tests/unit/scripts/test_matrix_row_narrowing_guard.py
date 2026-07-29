"""Negative control for the matrix-row narrowing guard (VIB-6205).

`_check_matrix_entries_cover_their_intents` in
`scripts/ci/check_connector_registry.py` protects a money-adjacent published
claim: what Edge believes is executable on which chain. Until this file existed,
the only thing exercising it was CI running it against the real registry, which
asserts it does NOT fire — never that it CAN.

That is not a hypothetical gap. The guard shipped **vacuous for every renaming
override** (pendle -> `yield`, enso/lifi -> `aggregator`): their category matches
no intent, so the per-category comparison came up empty and the guard could never
fire for exactly the rows best able to hide a narrowing. Every gate stayed green
through both the broken and the fixed version; it was caught by going looking.

So these tests assert the guard FIRES on both defects it exists for, not merely
that today's registry is clean.
"""

from __future__ import annotations

import dataclasses
import importlib.util
import sys
from pathlib import Path
from typing import Any

import pytest


def _guard():
    """Load the CI script and return the guard function.

    `sys.modules[name] = mod` before `exec_module` is required, not cosmetic:
    the script defines a `@dataclass`, and dataclass construction resolves
    `cls.__module__` through `sys.modules`. Without the registration it raises
    `AttributeError: 'NoneType' object has no attribute '__dict__'`.
    """
    script = Path(__file__).parents[3] / "scripts" / "ci" / "check_connector_registry.py"
    assert script.is_file(), f"guard script not found at {script}"
    spec = importlib.util.spec_from_file_location("_ccr_under_test", script)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules["_ccr_under_test"] = mod
    try:
        spec.loader.exec_module(mod)
    finally:
        sys.modules.pop("_ccr_under_test", None)
    return mod._check_matrix_entries_cover_their_intents


def _manifests() -> list[Any]:
    from almanak.connectors._strategy_base.registry import (
        ConnectorRegistry,
        _import_all_connectors,
    )

    _import_all_connectors()
    return list(ConnectorRegistry.all())


def _named(name: str) -> Any:
    manifest = next((m for m in _manifests() if m.name == name), None)
    if manifest is None:  # pragma: no cover - connector removed
        pytest.skip(f"{name} is no longer registered")
    return manifest


def test_real_registry_is_clean() -> None:
    """The invariant holds today — the baseline the other two are measured against."""
    assert _guard()(_manifests()) == []


def test_fires_on_a_narrowed_renaming_row() -> None:
    """The regression that shipped: a renaming row's category matches no intent.

    A per-category comparison therefore comes up empty and the guard says
    nothing, which is why it was vacuous for pendle, enso and lifi. Narrow
    pendle's `yield` row below what its own intents claim and the guard must
    catch it.
    """
    from almanak.connectors._strategy_base.registry import MatrixEntry

    narrowed = dataclasses.replace(
        _named("pendle"),
        matrix_entries=(
            MatrixEntry(
                matrix_name="pendle", category="yield", chains=frozenset({"ethereum"})
            ),
        ),
    )
    violations = _guard()([narrowed])
    assert violations, "a renaming row narrowed below its intents must be caught"
    assert "arbitrum" in violations[0].detail
    assert violations[0].kind == "matrix-row-narrows-below-its-intents"


def test_fires_on_the_fluid_shape_it_was_written_for() -> None:
    """fluid as it stood on `main`: the motivating defect.

    The lending row was pinned to arbitrum+base via `matrix_entries` while
    `chains_for_intent(SUPPLY)` still answered all four chains, so schema v2
    would have published the wider answer as executable coverage.
    """
    from almanak.connectors._strategy_base.registry import MatrixEntry

    as_on_main = dataclasses.replace(
        _named("fluid"),
        intent_chain_exclusions=None,
        matrix_entries=(
            MatrixEntry(
                matrix_name="fluid",
                category="swap",
                chains=frozenset({"arbitrum", "base", "ethereum", "polygon"}),
            ),
            MatrixEntry(
                matrix_name="fluid", category="lending", chains=frozenset({"arbitrum", "base"})
            ),
        ),
    )
    violations = _guard()([as_on_main])
    assert violations, "the motivating defect must fail the guard"
    detail = violations[0].detail
    assert "ethereum" in detail and "polygon" in detail


def test_off_chain_manifest_is_skipped_not_crashed() -> None:
    """`chains is None` has no cross-product to compare; it must not raise."""
    off_chain = [m for m in _manifests() if m.chains is None]
    assert off_chain, "precondition: at least one off-chain venue (e.g. kraken)"
    assert _guard()(off_chain) == []
