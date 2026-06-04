"""Guard test: every connector ``contract_roles.py`` must be manifest-published.

VIB-4928 PR-3a moved the six per-protocol address tables in
``almanak/framework/intents/compiler_constants.py`` off hand-imported connector
``addresses.py`` modules and onto a connector-self-registering
``CONTRACT_ROLE_REGISTRY``, aggregated by
``almanak/connectors/_strategy_contract_role_registry.py``. Self-containment
only holds if a connector that ships a ``contract_roles.py`` also publishes it
from ``CONNECTOR.contract_roles``: otherwise its addresses silently vanish from
``PROTOCOL_ROUTERS`` / ``LP_POSITION_MANAGERS`` / ``SWAP_QUOTER_ADDRESSES`` /
``LENDING_POOL_ADDRESSES`` / ``LENDING_POOL_DATA_PROVIDERS`` /
``BALANCER_VAULT_ADDRESSES``. That is a silent address-table drop on a
live-money hot path.

This test turns "forgot to register the new connector's contract roles" into a
CI failure: the structural sibling of
``test_flash_loan_registry_completeness.py``.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

import almanak.connectors._strategy_contract_role_registry as _boot
from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.address_registry import AddressRegistry
from almanak.connectors._strategy_base.contract_role_registry import (
    CONTRACT_ROLE_REGISTRY,
    ContractRole,
    ContractRoleSpec,
)

CONNECTORS_DIR = Path(__file__).resolve().parents[3] / "almanak" / "connectors"

# Infrastructure dirs hold shared base classes, not a concrete connector.
EXCLUDED_DIRS = {"_base", "_strategy_base", "__pycache__"}


@pytest.fixture(autouse=True)
def _bootstrapped_contract_role_registry() -> None:
    """Repopulate the mutable singleton from connector manifests for each test."""
    CONNECTOR_REGISTRY.clear()
    CONTRACT_ROLE_REGISTRY.reset()
    _boot._register_all()


def _discover_contract_role_modules() -> list[str]:
    """Return dotted module paths for every connector ``contract_roles.py``."""
    modules: list[str] = []
    for roles_file in sorted(CONNECTORS_DIR.rglob("contract_roles.py")):
        rel_parts = roles_file.relative_to(CONNECTORS_DIR).with_suffix("").parts
        if any(part in EXCLUDED_DIRS for part in rel_parts):
            continue
        modules.append("almanak.connectors." + ".".join(rel_parts))
    return modules


def _declared_specs(module_name: str) -> tuple[ContractRoleSpec, ...]:
    module = importlib.import_module(module_name)
    specs = getattr(module, "CONTRACT_ROLES", None)
    assert isinstance(specs, tuple), f"{module_name} must export CONTRACT_ROLES: tuple[ContractRoleSpec, ...]"
    return specs


def _connector_name(module_name: str) -> str:
    """Return the connector folder name from a dotted contract-role module."""
    return module_name.split(".")[2]


def test_discovery_finds_the_known_modules() -> None:
    # Sanity-check the discovery so a silently-empty walk can't make the
    # completeness assertions vacuously pass.
    modules = _discover_contract_role_modules()
    for expected in (
        "almanak.connectors.uniswap_v3.contract_roles",
        "almanak.connectors.aave_v3.contract_roles",
        "almanak.connectors.balancer_v2.contract_roles",
        "almanak.connectors.aerodrome.contract_roles",
    ):
        assert expected in modules, f"{expected} not discovered"


@pytest.mark.parametrize("module_name", _discover_contract_role_modules())
def test_every_contract_role_module_is_manifest_published(module_name: str) -> None:
    """Every contract-role data module must be owned by its connector manifest."""
    connector_manifest = CONNECTOR_REGISTRY.get(_connector_name(module_name))

    assert connector_manifest is not None
    assert connector_manifest.contract_roles is not None, (
        f"{module_name} is not published from CONNECTOR.contract_roles. Add an "
        f"ImportRef to almanak/connectors/{_connector_name(module_name)}/connector.py."
    )
    assert connector_manifest.contract_roles.module == module_name
    assert connector_manifest.contract_roles.attribute == "CONTRACT_ROLES"


def test_contract_role_boot_file_has_no_concrete_connector_imports() -> None:
    """The contract-role boot file discovers manifests instead of naming connectors."""
    source = (CONNECTORS_DIR / "_strategy_contract_role_registry.py").read_text()

    for module_name in _discover_contract_role_modules():
        assert module_name not in source


@pytest.mark.parametrize("module_name", _discover_contract_role_modules())
def test_every_declared_protocol_is_registered(module_name: str) -> None:
    """Every protocol slug a ``contract_roles.py`` declares must be registered."""
    specs = _declared_specs(module_name)
    assert specs, f"{module_name} exports an empty CONTRACT_ROLES tuple"
    for spec in specs:
        assert CONTRACT_ROLE_REGISTRY.has(spec.protocol), (
            f"{module_name} declares protocol {spec.protocol!r} but it is not "
            "registered in CONTRACT_ROLE_REGISTRY. Publish it from "
            f"almanak/connectors/{_connector_name(module_name)}/connector.py."
        )


def test_registered_set_equals_declared_union() -> None:
    """The boot file registers exactly the union of every discovered module's
    declared slugs: no slug declared-but-unregistered, none registered from
    thin air."""
    declared: set[str] = set()
    for module_name in _discover_contract_role_modules():
        for spec in _declared_specs(module_name):
            declared.add(spec.protocol)
    registered = set(CONTRACT_ROLE_REGISTRY.registered_protocols())
    assert registered == declared, (
        "registry / connector contract_roles drift: "
        f"registered-only: {sorted(registered - declared)}; "
        f"declared-only: {sorted(declared - registered)}"
    )


def test_every_role_protocol_owns_an_address_table() -> None:
    """Every protocol with a role must resolve to an ``AddressRegistry`` table.

    A role declaration is useless if the backing address table is missing:
    the derived compiler tables would silently drop the slug. ``address_protocol``
    resolves the pseudo-slug alias (``aerodrome_slipstream`` → ``aerodrome``).
    """
    for protocol in CONTRACT_ROLE_REGISTRY.registered_protocols():
        addr_proto = CONTRACT_ROLE_REGISTRY.address_protocol(protocol)
        assert AddressRegistry.has(addr_proto), (
            f"{protocol!r} (address_protocol={addr_proto!r}) has a contract-role "
            f"registration but no AddressRegistry._BUILTIN_LOADERS table"
        )
        assert AddressRegistry.address_chains_ordered(addr_proto), (
            f"{protocol!r} resolves to an empty address table via {addr_proto!r}"
        )


def test_spark_declares_lending_pool_only() -> None:
    """Spark must declare LENDING_POOL but NOT LENDING_DATA_PROVIDER.

    This is the connector-side source of the intentional Spark omission from
    ``LENDING_POOL_DATA_PROVIDERS`` (VIB-4928 PR-3a). If Spark ever gains a
    LENDING_DATA_PROVIDER role, that surface widens. Make it a deliberate
    decision, not an accident.
    """
    assert CONTRACT_ROLE_REGISTRY.kinds_for("spark", ContractRole.LENDING_POOL) == ("pool",)
    assert CONTRACT_ROLE_REGISTRY.kinds_for("spark", ContractRole.LENDING_DATA_PROVIDER) is None


def test_traderjoe_v2_has_no_router_role() -> None:
    """TraderJoe V2 fills only the LP slot, never PROTOCOL_ROUTERS (Liquidity
    Book is not a V3-style swap router). Guards against re-introducing a ROUTER
    role that would leak traderjoe_v2 into ``PROTOCOL_ROUTERS``.
    """
    assert CONTRACT_ROLE_REGISTRY.kinds_for("traderjoe_v2", ContractRole.LP_POSITION_MANAGER) == ("router",)
    assert CONTRACT_ROLE_REGISTRY.kinds_for("traderjoe_v2", ContractRole.ROUTER) is None


def test_reregister_without_alias_clears_stale_alias() -> None:
    """Re-registering a slug WITHOUT ``address_protocol`` must drop a prior alias.

    ``register`` advertises a "register (or replace)" contract. Before the
    VIB-4928 PR-3a fix it only ever *wrote* ``_aliases`` (when an alias was
    given) and never cleared one, so a slug first registered with
    ``address_protocol="X"`` and later re-registered without it kept resolving
    ``address_protocol`` to the stale ``"X"`` table key: a wrong-table address
    resolution on a live-money path. Snapshot/restore the shared class state so
    this test cannot leak into the boot-populated registry other tests read.
    """
    roles_snapshot = {p: dict(r) for p, r in CONTRACT_ROLE_REGISTRY._roles.items()}
    aliases_snapshot = dict(CONTRACT_ROLE_REGISTRY._aliases)
    try:
        proto = "test_reregister_probe"
        role_map = {ContractRole.ROUTER: ("router",)}

        CONTRACT_ROLE_REGISTRY.register(protocol=proto, roles=role_map, address_protocol="X")
        assert CONTRACT_ROLE_REGISTRY.address_protocol(proto) == "X"

        # Re-register the same slug WITHOUT an alias: the stale alias must clear,
        # so address_protocol falls back to the slug itself.
        CONTRACT_ROLE_REGISTRY.register(protocol=proto, roles=role_map)
        assert CONTRACT_ROLE_REGISTRY.address_protocol(proto) == proto
        assert proto not in CONTRACT_ROLE_REGISTRY._aliases
    finally:
        CONTRACT_ROLE_REGISTRY._roles.clear()
        CONTRACT_ROLE_REGISTRY._roles.update(roles_snapshot)
        CONTRACT_ROLE_REGISTRY._aliases.clear()
        CONTRACT_ROLE_REGISTRY._aliases.update(aliases_snapshot)
