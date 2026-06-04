"""Guard test: every connector ``swap_classification.py`` must be manifest-published.

VIB-4928 PR-3b moved the five swap-classification symbols in
``almanak/framework/intents/compiler_constants.py`` off hand-imported connector
``swap_constants.py`` modules and onto a connector-self-registering
``SWAP_CLASSIFICATION_REGISTRY``, aggregated by
``almanak/connectors/_strategy_swap_classification_registry.py``. Self-containment
only holds if a connector that ships a ``swap_classification.py`` also publishes
it from ``CONNECTOR.swap_classification``: otherwise its slugs silently vanish
from ``SWAP_FEE_TIERS`` / ``DEFAULT_SWAP_FEE_TIER`` /
``SWAP_ROUTER_V1_PROTOCOLS`` / ``SWAP_ROUTER_V1_CHAIN_OVERRIDES`` /
``SWAP_ROUTER_ALGEBRA_PROTOCOLS``. That is silent swap-compile drift.

This test turns "forgot to register the new connector's swap classification"
into a CI failure: the structural sibling of
``test_contract_role_registry_completeness.py``.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

import almanak.connectors._strategy_swap_classification_registry as _boot
from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.swap_classification_registry import (
    SWAP_CLASSIFICATION_REGISTRY,
    SwapClassificationSpec,
)

CONNECTORS_DIR = Path(__file__).resolve().parents[3] / "almanak" / "connectors"

# Infrastructure dirs hold shared base classes, not a concrete connector.
EXCLUDED_DIRS = {"_base", "_strategy_base", "__pycache__"}


@pytest.fixture(autouse=True)
def _bootstrapped_swap_classification_registry() -> None:
    """Repopulate the mutable singleton from connector manifests for each test."""
    CONNECTOR_REGISTRY.clear()
    SWAP_CLASSIFICATION_REGISTRY.reset()
    _boot._register_all()


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


def _connector_name(module_name: str) -> str:
    """Return the connector folder name from a dotted swap-classification module."""
    return module_name.split(".")[2]


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
def test_every_swap_classification_module_is_manifest_published(module_name: str) -> None:
    """Every swap-classification data module must be owned by its connector manifest."""
    connector_manifest = CONNECTOR_REGISTRY.get(_connector_name(module_name))

    assert connector_manifest is not None
    assert connector_manifest.swap_classification is not None, (
        f"{module_name} is not published from CONNECTOR.swap_classification. Add an "
        f"ImportRef to almanak/connectors/{_connector_name(module_name)}/connector.py."
    )
    assert connector_manifest.swap_classification.module == module_name
    assert connector_manifest.swap_classification.attribute == "SWAP_CLASSIFICATION"


def test_swap_classification_boot_file_has_no_concrete_connector_imports() -> None:
    """The swap-classification boot file discovers manifests instead of naming connectors."""
    source = (CONNECTORS_DIR / "_strategy_swap_classification_registry.py").read_text()

    for module_name in _discover_modules():
        assert module_name not in source


@pytest.mark.parametrize("module_name", _discover_modules())
def test_every_declared_protocol_is_registered(module_name: str) -> None:
    """Every protocol slug a ``swap_classification.py`` declares must register."""
    specs = _declared_specs(module_name)
    assert specs, f"{module_name} exports an empty SWAP_CLASSIFICATION tuple"
    for spec in specs:
        assert SWAP_CLASSIFICATION_REGISTRY.has(spec.protocol), (
            f"{module_name} declares protocol {spec.protocol!r} but it is not "
            "registered in SWAP_CLASSIFICATION_REGISTRY. Publish it from "
            f"almanak/connectors/{_connector_name(module_name)}/connector.py."
        )


def test_registered_set_equals_declared_union() -> None:
    """The boot file registers exactly the union of every discovered module's
    declared slugs: none declared-but-unregistered, none registered from thin
    air."""
    declared: set[str] = set()
    for module_name in _discover_modules():
        for spec in _declared_specs(module_name):
            declared.add(spec.protocol)
    registered = set(SWAP_CLASSIFICATION_REGISTRY.registered_protocols())
    assert registered == declared, (
        "registry / connector swap_classification drift: "
        f"registered-only: {sorted(registered - declared)}; "
        f"declared-only: {sorted(declared - registered)}"
    )
