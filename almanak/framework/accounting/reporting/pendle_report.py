"""Compatibility exports for Pendle accounting report sections.

The Pendle report implementation is owned by
``almanak.connectors.pendle.reporting``. This module remains so legacy imports
from ``almanak.framework.accounting.reporting.pendle_report`` continue to work
while framework report assembly discovers Pendle sections through connector
capabilities.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_COMPAT_EXPORTS = (
    "PendlePositionSummary",
    "PendleSection",
    "build_pendle_report",
)

__all__ = list(_COMPAT_EXPORTS)
_CONNECTOR_MODULE = "almanak.connectors.pendle.reporting"


def _connector_reporting() -> Any:
    return import_module(_CONNECTOR_MODULE)


def build_pendle_report(data: Any) -> Any:
    """Build Pendle report sections through the connector-owned implementation."""
    return _connector_reporting().build_pendle_report(data)


def __getattr__(name: str) -> object:
    if name in _COMPAT_EXPORTS:
        value = getattr(_connector_reporting(), name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
