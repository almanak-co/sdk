"""Trader Joe V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="traderjoe_v2",
    kind=ProtocolKind.LP,
    address_tables=(
        AddressTableSpec(
            protocol="traderjoe_v2",
            module="almanak.connectors.traderjoe_v2.addresses",
            attribute="TRADERJOE_V2",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.traderjoe_v2.gateway.provider",
        attribute="TraderJoeV2GatewayConnector",
        order=19,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.traderjoe_v2.receipt_parser_provider",
        attribute="TraderJoeV2ReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.traderjoe_v2.contract_monitoring",
        attribute="TRADERJOE_V2_CONTRACT_MONITORING_SPECS",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.traderjoe_v2.contract_roles",
        attribute="CONTRACT_ROLES",
        order=6,
    ),
)

__all__ = ["CONNECTOR"]
