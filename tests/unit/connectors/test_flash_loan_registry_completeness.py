"""Guard test: every connector with flash_loan_provider.py must publish it.

VIB-4837 moved the flash-loan provider list out of
``almanak/framework/intents/compiler_flash_loan.py`` and onto each connector,
aggregated by ``almanak/connectors/_strategy_flash_loan_registry.py``. Connector
self-containment only holds if a new connector that ships a
``flash_loan_provider.py`` also publishes it from that connector's manifest.
Otherwise the provider is silently invisible to the selector and to
``flash-loan`` intents.

This test turns "forgot to publish the new provider" into a CI failure. It is
the flash-loan sibling of ``test_receipt_parser_registry_completeness.py``.
"""

from __future__ import annotations

import importlib
import inspect
from pathlib import Path

import pytest

import almanak.connectors._strategy_flash_loan_registry as _boot
from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.flash_loan_base import FlashLoanProvider

CONNECTORS_DIR = Path(__file__).resolve().parents[3] / "almanak" / "connectors"
FLASH_LOAN_PROVIDER_REGISTRY = _boot.FLASH_LOAN_PROVIDER_REGISTRY

# Infrastructure dirs hold shared base classes, not a concrete provider.
EXCLUDED_DIRS = {"_base", "_strategy_base", "__pycache__"}


def _discover_provider_modules() -> list[str]:
    """Return dotted module paths for every connector ``flash_loan_provider.py``."""
    modules: list[str] = []
    for provider_file in sorted(CONNECTORS_DIR.rglob("flash_loan_provider.py")):
        rel_parts = provider_file.relative_to(CONNECTORS_DIR).with_suffix("").parts
        if any(part in EXCLUDED_DIRS for part in rel_parts):
            continue
        modules.append("almanak.connectors." + ".".join(rel_parts))
    return modules


def _provider_classes(module_name: str) -> list[type[FlashLoanProvider]]:
    """Concrete ``FlashLoanProvider`` subclasses defined in ``module_name``."""
    module = importlib.import_module(module_name)
    return [
        obj
        for _, obj in inspect.getmembers(module, inspect.isclass)
        if issubclass(obj, FlashLoanProvider) and obj is not FlashLoanProvider and obj.__module__ == module.__name__
    ]


def _connector_name(module_name: str) -> str:
    """Return the connector folder name from a provider module path."""
    return module_name.split(".")[2]


def _builder_module(module_name: str) -> str:
    """Return the sibling flash-loan builder module for a provider module."""
    return f"{module_name.rsplit('.', 1)[0]}.flash_loan"


def test_discovery_finds_the_known_providers() -> None:
    # Sanity-check the discovery itself so a silently-empty walk can't make the
    # completeness assertion vacuously pass.
    modules = _discover_provider_modules()
    assert "almanak.connectors.aave_v3.flash_loan_provider" in modules
    assert "almanak.connectors.balancer_v2.flash_loan_provider" in modules
    assert "almanak.connectors.morpho_blue.flash_loan_provider" in modules


@pytest.mark.parametrize("module_name", _discover_provider_modules())
def test_every_connector_flash_loan_provider_is_manifest_published(module_name: str) -> None:
    connector_manifest = CONNECTOR_REGISTRY.get(_connector_name(module_name))
    provider_classes = _provider_classes(module_name)

    assert connector_manifest is not None, f"{module_name} has no connector manifest"
    assert provider_classes, f"{module_name} contains no FlashLoanProvider subclass"
    assert len(provider_classes) == 1, f"{module_name} should define exactly one FlashLoanProvider subclass"
    provider_cls = provider_classes[0]
    assert connector_manifest.flash_loan_provider_name == provider_cls().name
    assert connector_manifest.flash_loan_provider is not None, (
        f"{module_name}.{provider_cls.__name__} is not published in "
        f"{_connector_name(module_name)}/connector.py. Set CONNECTOR.flash_loan_provider."
    )
    assert connector_manifest.flash_loan_provider.module == module_name
    assert connector_manifest.flash_loan_provider.attribute == provider_cls.__name__
    assert connector_manifest.flash_loan_builder is not None, (
        f"{module_name}.{provider_cls.__name__} is missing a connector-owned flash-loan builder. "
        f"Set CONNECTOR.flash_loan_builder in {_connector_name(module_name)}/connector.py."
    )
    assert connector_manifest.flash_loan_builder.module == _builder_module(module_name)
    assert callable(connector_manifest.flash_loan_builder.load())


@pytest.mark.parametrize("module_name", _discover_provider_modules())
def test_every_manifest_flash_loan_provider_is_boot_registered(module_name: str) -> None:
    registered = FLASH_LOAN_PROVIDER_REGISTRY.names()
    provider_classes = _provider_classes(module_name)
    assert provider_classes, f"{module_name} contains no FlashLoanProvider subclass"
    for cls in provider_classes:
        name = cls().name
        assert name in registered, (
            f"{module_name}.{cls.__name__} (name={name!r}) was not registered from the connector manifest. "
            f"Publish CONNECTOR.flash_loan_provider_name, CONNECTOR.flash_loan_provider, and "
            f"CONNECTOR.flash_loan_builder in {_connector_name(module_name)}/connector.py."
        )


def test_flash_loan_boot_file_has_no_concrete_connector_imports() -> None:
    CONNECTOR_REGISTRY.clear()
    source = Path(_boot.__file__).read_text()

    for connector_manifest in CONNECTOR_REGISTRY.with_flash_loan():
        assert connector_manifest.flash_loan_provider is not None
        assert connector_manifest.flash_loan_builder is not None
        assert connector_manifest.flash_loan_provider.module not in source
        assert connector_manifest.flash_loan_provider.attribute not in source
        assert connector_manifest.flash_loan_builder.module not in source
        assert connector_manifest.flash_loan_builder.attribute not in source
