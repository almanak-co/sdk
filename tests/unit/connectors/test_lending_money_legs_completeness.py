"""Guard test: a connector that DECLARES ``primitive_money_legs`` must implement it.

VIB-5218 migrated the lending family (euler_v2 / spark / morpho_blue) onto the
typed ``extract_primitive_money_legs`` seam so their SUPPLY / WITHDRAW / BORROW /
REPAY rows carry a real ``token_in`` instead of the empty string that made the
lending handler resolve ``UNKNOWN`` → ``deployed_capital_usd = 0``.

The seam only holds if the declaration (``EXTRA_EXTRACTIONS_BY_INTENT`` advertising
``primitive_money_legs``) and the implementation (``extract_primitive_money_legs``)
never drift apart. This test turns "declared the field but forgot the method" (or
the reverse for the migrated set) into a CI failure — the lending sibling of
``test_flash_loan_registry_completeness.py``, so the next connector can't silently
regress the lending-accounting fix.
"""

from __future__ import annotations

import importlib
import inspect
from pathlib import Path

import pytest

CONNECTORS_DIR = Path(__file__).resolve().parents[3] / "almanak" / "connectors"
EXCLUDED_DIRS = {"_base", "_strategy_base", "__pycache__"}

# The lending connectors migrated onto the money-leg seam by VIB-5218. Each MUST
# both declare ``primitive_money_legs`` for ALL FOUR lending actions and implement
# the extractor. Add a connector here when it joins the migrated set.
_MIGRATED_LENDING_CONNECTORS = ("euler_v2", "spark", "morpho_blue")
_REQUIRED_INTENTS = ("SUPPLY", "WITHDRAW", "BORROW", "REPAY")


def _discover_receipt_parser_modules() -> list[str]:
    modules: list[str] = []
    for parser_file in sorted(CONNECTORS_DIR.rglob("receipt_parser.py")):
        rel_parts = parser_file.relative_to(CONNECTORS_DIR).with_suffix("").parts
        if any(part in EXCLUDED_DIRS for part in rel_parts):
            continue
        modules.append("almanak.connectors." + ".".join(rel_parts))
    return modules


def _parser_classes(module_name: str) -> list[type]:
    module = importlib.import_module(module_name)
    return [
        obj
        for _, obj in inspect.getmembers(module, inspect.isclass)
        if obj.__module__ == module.__name__ and obj.__name__.endswith("ReceiptParser")
    ]


def _declares_money_legs(cls: type) -> bool:
    extra = getattr(cls, "EXTRA_EXTRACTIONS_BY_INTENT", None)
    if not isinstance(extra, dict):
        return False
    return any("primitive_money_legs" in (fields or ()) for fields in extra.values())


def test_discovery_finds_known_parsers() -> None:
    # Guard the walk itself so a silently-empty discovery can't make the
    # completeness assertion vacuously pass.
    modules = _discover_receipt_parser_modules()
    assert "almanak.connectors.euler_v2.receipt_parser" in modules
    assert "almanak.connectors.spark.receipt_parser" in modules
    assert "almanak.connectors.morpho_blue.receipt_parser" in modules


@pytest.mark.parametrize("module_name", _discover_receipt_parser_modules())
def test_declared_money_legs_implies_extractor(module_name: str) -> None:
    """Any receipt parser that declares ``primitive_money_legs`` in
    ``EXTRA_EXTRACTIONS_BY_INTENT`` MUST define ``extract_primitive_money_legs``.

    Closes the "declared the field but forgot the method" drift: the enricher
    would otherwise skip the field silently and the ledger row would fall back to
    the legacy ``UNKNOWN`` guesser.
    """
    for cls in _parser_classes(module_name):
        if not _declares_money_legs(cls):
            continue
        method = getattr(cls, "extract_primitive_money_legs", None)
        assert callable(method), (
            f"{module_name}.{cls.__name__} declares 'primitive_money_legs' in "
            f"EXTRA_EXTRACTIONS_BY_INTENT but does not implement extract_primitive_money_legs()."
        )


@pytest.mark.parametrize("connector", _MIGRATED_LENDING_CONNECTORS)
def test_migrated_lending_connector_declares_and_implements(connector: str) -> None:
    """The VIB-5218 migrated lending connectors stay wired: they declare
    ``primitive_money_legs`` for SUPPLY / WITHDRAW / BORROW / REPAY and
    implement the extractor."""
    module_name = f"almanak.connectors.{connector}.receipt_parser"
    parser_classes = [cls for cls in _parser_classes(module_name) if _declares_money_legs(cls)]
    assert parser_classes, (
        f"{connector} no longer declares 'primitive_money_legs' in any receipt parser's "
        f"EXTRA_EXTRACTIONS_BY_INTENT — the VIB-5218 lending-accounting fix has regressed."
    )
    for cls in parser_classes:
        extra = cls.EXTRA_EXTRACTIONS_BY_INTENT
        for intent in _REQUIRED_INTENTS:
            assert "primitive_money_legs" in (extra.get(intent) or ()), (
                f"{connector} {cls.__name__} must declare 'primitive_money_legs' for {intent}."
            )
        assert callable(getattr(cls, "extract_primitive_money_legs", None))
