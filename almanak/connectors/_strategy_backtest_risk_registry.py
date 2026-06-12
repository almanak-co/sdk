"""Strategy-side backtest risk-parameter registration site (plan 022).

Iterates the connector registry and registers every connector that declares
``CONNECTOR.backtest_risk`` into ``BACKTEST_RISK_REGISTRY``. The registration
runs eagerly at import time so that ``LiquidationParamRegistry._initialize_defaults``
receives a fully-populated registry on first call.

This module is imported lazily by ``liquidation_params._initialize_defaults()``
— it must NOT be imported at the top-level of any framework module that is
imported at process start, to keep the connector-registry walk off the hot path.
"""

from __future__ import annotations

from almanak.connectors._connector import CONNECTOR_REGISTRY
from almanak.connectors._strategy_base.backtest_risk_registry import BACKTEST_RISK_REGISTRY

__all__ = ["BACKTEST_RISK_REGISTRY"]


def _register_discovered_backtest_risk() -> None:
    """Register backtest risk-parameter decls published by connector manifests."""
    for connector_manifest in CONNECTOR_REGISTRY.with_backtest_risk():
        if connector_manifest.backtest_risk is None:
            continue
        BACKTEST_RISK_REGISTRY.register(connector_manifest.name, connector_manifest.backtest_risk)


def _register_all() -> None:
    """Register every descriptor-backed backtest risk declaration."""
    _register_discovered_backtest_risk()


_register_all()
