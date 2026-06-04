"""Fluid connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="fluid",
    kind=ProtocolKind.LP,
    gateway_connector=ImportRef(
        module="almanak.connectors.fluid.gateway.provider",
        attribute="FluidGatewayConnector",
        order=4,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.fluid.receipt_parser_provider",
        attribute="FluidReceiptParserConnector",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.fluid.contract_roles",
        attribute="CONTRACT_ROLES",
        order=8,
    ),
)

__all__ = ["CONNECTOR"]
