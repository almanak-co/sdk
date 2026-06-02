"""Guard test: every connector ``swap_classification.py`` must be registered.

VIB-4928 PR-3b moved the five swap-classification symbols in
``almanak/framework/intents/compiler_constants.py`` off hand-imported connector
``swap_constants.py`` modules and onto a connector-self-registering
``SWAP_CLASSIFICATION_REGISTRY``, aggregated by
``almanak/connectors/_strategy_swap_classification_registry.py``. Self-containment
only holds if a connector that ships a ``swap_classification.py`` ALSO registers
it in the boot file: otherwise its slugs silently vanish from ``SWAP_FEE_TIERS``
/ ``DEFAULT_SWAP_FEE_TIER`` / ``SWAP_ROUTER_V1_PROTOCOLS`` /
``SWAP_ROUTER_V1_CHAIN_OVERRIDES`` / ``SWAP_ROUTER_ALGEBRA_PROTOCOLS`` — a silent
swap-compile drift.

This test turns "forgot to register the new connector's swap classification"
into a CI failure — the structural sibling of
``test_contract_role_registry_completeness.py``.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

# Importing the boot file populates the registry.
import almanak.connectors._strategy_swap_classification_registry  # noqa: F401
from almanak.connectors._strategy_base.swap_classification_registry import (
    SWAP_CLASSIFICATION_REGISTRY,
    SwapClassificationSpec,
)

CONNECTORS_DIR = Path(__file__).resolve().parents[3] / "almanak" / "connectors"

# Infrastructure dirs hold shared base classes, not a concrete connector.
EXCLUDED_DIRS = {"_base", "_strategy_base", "__pycache__"}


def _discover_modules() -> list[str]:
    """Return dotted module paths for every connector ``swap_classification.py``."""
    modules: list[str] = []
    for f in sorted(CONNECTORS_DIR.rglob("swap_classification.py")):
        rel_parts = f.relative_to(CONNECTORS_DIR).with_suffix("").parts
        if any(part in EXCLUDED_DIRS for part in rel_parts):
            continue
        modules.append("almanak.connectors." + ".".join(rel_parts))
    return modules


def _declared_specs(module_name: str) -> tuple[SwapClassificationSpec, ...]:
    module = importlib.import_module(module_name)
    specs = getattr(module, "SWAP_CLASSIFICATION", None)
    assert isinstance(specs, tuple), (
        f"{module_name} must export SWAP_CLASSIFICATION: tuple[SwapClassificationSpec, ...]"
    )
    return specs


def test_discovery_finds_known_modules() -> None:
    # Sanity-check discovery so a silently-empty walk can't make the
    # completeness assertions vacuously pass.
    modules = _discover_modules()
    for expected in (
        "almanak.connectors.uniswap_v3.swap_classification",
        "almanak.connectors.sushiswap_v3.swap_classification",
        "almanak.connectors.pancakeswap_v3.swap_classification",
        "almanak.connectors.camelot.swap_classification",
    ):
        assert expected in modules, f"{expected} not discovered"


@pytest.mark.parametrize("module_name", _discover_modules())
def test_every_declared_protocol_is_registered(module_name: str) -> None:
    """Every protocol slug a ``swap_classification.py`` declares must register."""
    specs = _declared_specs(module_name)
    assert specs, f"{module_name} exports an empty SWAP_CLASSIFICATION tuple"
    for spec in specs:
        assert SWAP_CLASSIFICATION_REGISTRY.has(spec.protocol), (
            f"{module_name} declares protocol {spec.protocol!r} but it is not "
            f"registered in SWAP_CLASSIFICATION_REGISTRY. Add it to "
            f"almanak/connectors/_strategy_swap_classification_registry.py."
        )


def test_registered_set_equals_declared_union() -> None:
    """The boot file registers exactly the union of every discovered module's
    declared slugs — none declared-but-unregistered, none registered from thin
    air."""
    declared: set[str] = set()
    for module_name in _discover_modules():
        for spec in _declared_specs(module_name):
            declared.add(spec.protocol)
    registered = set(SWAP_CLASSIFICATION_REGISTRY.registered_protocols())
    assert registered == declared, (
        f"registry / connector swap_classification drift — "
        f"registered-only: {sorted(registered - declared)}; "
        f"declared-only: {sorted(declared - registered)}"
    )
