"""Strategy-side accounting-treatment registration site."""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.accounting_treatment_registry import (
    AccountingCategoryDecision,
    AccountingTreatmentRegistry,
    AccountingTreatmentSpec,
)

__all__ = [
    "AccountingCategoryDecision",
    "AccountingTreatmentRegistry",
    "AccountingTreatmentSpec",
]


def _register_discovered_accounting_treatments() -> None:
    """Register accounting-treatment specs published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_accounting_treatment():
        if connector_manifest.accounting_treatment is None:
            continue
        AccountingTreatmentRegistry.register_loader(
            connector_manifest.name,
            connector_manifest.accounting_treatment.module,
            connector_manifest.accounting_treatment.attribute,
        )


def _register_all() -> None:
    """Register every descriptor-backed accounting-treatment spec."""
    _register_discovered_accounting_treatments()


_register_all()
