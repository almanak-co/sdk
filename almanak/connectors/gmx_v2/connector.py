"""GMX V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="gmx_v2",
    kind=ProtocolKind.PERP,
    gateway_connector=ImportRef(
        module="almanak.connectors.gmx_v2.gateway.provider",
        attribute="GmxV2GatewayConnector",
        order=14,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.gmx_v2.receipt_parser_provider",
        attribute="GmxV2ReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.gmx_v2.contract_monitoring",
        attribute="GMX_V2_CONTRACT_MONITORING_SPECS",
    ),
)

__all__ = ["CONNECTOR"]
