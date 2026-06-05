"""Strategy-side accounting report provider registration site."""

from __future__ import annotations

import logging

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.accounting_report_registry import (
    ACCOUNTING_REPORT_REGISTRY,
    AccountingReportConnector,
    AccountingReportRegistry,
    AccountingReportSection,
    AccountingReportSectionCapability,
)

__all__ = [
    "ACCOUNTING_REPORT_REGISTRY",
    "AccountingReportConnector",
    "AccountingReportRegistry",
    "AccountingReportSection",
    "AccountingReportSectionCapability",
]

logger = logging.getLogger(__name__)


def _register_discovered_accounting_report_providers() -> None:
    """Register accounting report providers published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_accounting_report():
        if connector_manifest.accounting_report is None:
            continue
        try:
            ACCOUNTING_REPORT_REGISTRY.register(connector_manifest.accounting_report.instantiate())
        except Exception:
            logger.exception(
                "Failed to register accounting report provider for connector %s",
                connector_manifest.name,
            )


def _register_all() -> None:
    """Register every descriptor-backed accounting report provider."""
    _register_discovered_accounting_report_providers()


_register_all()
