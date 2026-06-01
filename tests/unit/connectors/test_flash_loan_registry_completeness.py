"""Guard test: every connector with a flash_loan_provider.py must be registered.

VIB-4837 moved the flash-loan provider list out of
``almanak/framework/intents/compiler_flash_loan.py`` and onto each connector,
aggregated by ``almanak/connectors/_strategy_flash_loan_registry.py``. Self-
containment only holds if a new connector that ships a ``flash_loan_provider.py``
ALSO registers it: otherwise the provider is silently invisible to the selector
and to ``flash-loan`` intents.

This test turns "forgot to register the new provider" into a CI failure — the
structural sibling of ``test_receipt_parser_registry_completeness.py``.
"""

from __future__ import annotations

import importlib
import inspect
from pathlib import Path

import pytest

from almanak.connectors._strategy_base.flash_loan_base import FlashLoanProvider
from almanak.connectors._strategy_flash_loan_registry import (
    FLASH_LOAN_PROVIDER_REGISTRY,
)

CONNECTORS_DIR = Path(__file__).resolve().parents[3] / "almanak" / "connectors"

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


def test_discovery_finds_the_known_providers() -> None:
    # Sanity-check the discovery itself so a silently-empty walk can't make the
    # completeness assertion vacuously pass.
    modules = _discover_provider_modules()
    assert "almanak.connectors.aave_v3.flash_loan_provider" in modules
    assert "almanak.connectors.balancer_v2.flash_loan_provider" in modules
    assert "almanak.connectors.morpho_blue.flash_loan_provider" in modules


@pytest.mark.parametrize("module_name", _discover_provider_modules())
def test_every_connector_flash_loan_provider_is_registered(module_name: str) -> None:
    registered = FLASH_LOAN_PROVIDER_REGISTRY.names()
    provider_classes = _provider_classes(module_name)
    assert provider_classes, f"{module_name} contains no FlashLoanProvider subclass"
    for cls in provider_classes:
        name = cls().name
        assert name in registered, (
            f"{module_name}.{cls.__name__} (name={name!r}) is not registered in "
            f"FLASH_LOAN_PROVIDER_REGISTRY. Add a FlashLoanProviderRegistration in "
            f"almanak/connectors/_strategy_flash_loan_registry.py."
        )
