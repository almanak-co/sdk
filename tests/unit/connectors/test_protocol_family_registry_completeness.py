"""Guard test: every connector ``protocol_family.py`` must be registered.

VIB-4928 PR-3b moved ``AAVE_COMPATIBLE_PROTOCOLS`` + ``UNIV3_LP_GROUPING_PROTOCOLS``
in ``almanak/framework/intents/compiler_constants.py`` off hand-imported connector
``lending_constants.py`` / ``lp_constants.py`` modules and onto a
connector-self-registering ``PROTOCOL_FAMILY_REGISTRY``, aggregated by
``almanak/connectors/_strategy_protocol_family_registry.py``. Self-containment only
holds if a connector that ships a ``protocol_family.py`` ALSO registers it in the
boot file: otherwise its slugs silently vanish from the family membership sets.

This test turns "forgot to register the new connector's protocol family" into a
CI failure — the structural sibling of
``test_contract_role_registry_completeness.py``.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

# Importing the boot file populates the registry.
import almanak.connectors._strategy_protocol_family_registry  # noqa: F401
from almanak.connectors._strategy_base.protocol_family_registry import (
    PROTOCOL_FAMILY_REGISTRY,
    ProtocolFamilySpec,
)

CONNECTORS_DIR = Path(__file__).resolve().parents[3] / "almanak" / "connectors"

# Infrastructure dirs hold shared base classes, not a concrete connector.
EXCLUDED_DIRS = {"_base", "_strategy_base", "__pycache__"}


def _discover_modules() -> list[str]:
    """Return dotted module paths for every connector ``protocol_family.py``."""
    modules: list[str] = []
    for f in sorted(CONNECTORS_DIR.rglob("protocol_family.py")):
        rel_parts = f.relative_to(CONNECTORS_DIR).with_suffix("").parts
        if any(part in EXCLUDED_DIRS for part in rel_parts):
            continue
        modules.append("almanak.connectors." + ".".join(rel_parts))
    return modules


def _declared_spec(module_name: str) -> ProtocolFamilySpec:
    module = importlib.import_module(module_name)
    spec = getattr(module, "PROTOCOL_FAMILY", None)
    assert isinstance(spec, ProtocolFamilySpec), f"{module_name} must export PROTOCOL_FAMILY: ProtocolFamilySpec"
    return spec


def test_discovery_finds_known_modules() -> None:
    # Sanity-check discovery so a silently-empty walk can't make the
    # completeness assertions vacuously pass.
    modules = _discover_modules()
    for expected in (
        "almanak.connectors.uniswap_v3.protocol_family",
        "almanak.connectors.aave_v3.protocol_family",
        "almanak.connectors.aerodrome.protocol_family",
    ):
        assert expected in modules, f"{expected} not discovered"


@pytest.mark.parametrize("module_name", _discover_modules())
def test_every_declared_member_is_registered(module_name: str) -> None:
    """Every slug a ``protocol_family.py`` declares must be in the family union."""
    spec = _declared_spec(module_name)
    assert spec.families, f"{module_name} PROTOCOL_FAMILY declares no families"
    for family, slugs in spec.families.items():
        registered = PROTOCOL_FAMILY_REGISTRY.members(family)
        missing = set(slugs) - set(registered)
        assert not missing, (
            f"{module_name} declares {sorted(missing)} in family {family!r} but they "
            f"are not registered in PROTOCOL_FAMILY_REGISTRY. Add it to "
            f"almanak/connectors/_strategy_protocol_family_registry.py."
        )
