"""Connector-owned fixed tolerances for teardown actions outside the swap ladder."""

from decimal import Decimal
from typing import Any

from almanak.connectors._connector import CONNECTOR_REGISTRY


def fixed_teardown_slippage(intent: Any) -> Decimal | None:
    """Resolve a fixed tolerance, or leave an intent on the ordinary escalation ladder."""
    protocol = intent.get("protocol") if isinstance(intent, dict) else getattr(intent, "protocol", None)
    if not isinstance(protocol, str):
        return None
    connector = CONNECTOR_REGISTRY.get(protocol)
    if connector is None or connector.fixed_teardown_slippage is None:
        return None
    tolerance = connector.fixed_teardown_slippage.load()(intent)
    if tolerance is not None and (
        not isinstance(tolerance, Decimal) or not tolerance.is_finite() or not Decimal(0) <= tolerance < Decimal(1)
    ):
        raise ValueError("Connector fixed teardown tolerance must be a finite Decimal in [0, 1)")
    return tolerance
