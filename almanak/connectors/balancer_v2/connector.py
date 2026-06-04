"""Balancer V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="balancer_v2",
    kind=ProtocolKind.LP,
    gateway_connector=ImportRef(
        module="almanak.connectors.balancer_v2.gateway.provider",
        attribute="BalancerV2GatewayConnector",
        order=16,
    ),
    gas_estimate_connector=ImportRef(
        module="almanak.connectors.balancer_v2.gas_estimate_provider",
        attribute="BalancerV2GasEstimateConnector",
    ),
)

__all__ = ["CONNECTOR"]
