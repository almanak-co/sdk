"""Aerodrome connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="aerodrome",
    kind=ProtocolKind.LP,
    aliases=("aerodrome_slipstream",),
    address_tables=(
        AddressTableSpec(
            protocol="aerodrome",
            module="almanak.connectors.aerodrome.addresses",
            attribute="AERODROME",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.aerodrome.gateway.provider",
        attribute="AerodromeGatewayConnector",
        order=13,
    ),
    agent_read_connector=ImportRef(
        module="almanak.connectors.aerodrome.agent_read_provider",
        attribute="AerodromeSlipstreamAgentReadConnector",
        order=3,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.aerodrome.receipt_parser_provider",
        attribute="AerodromeReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.aerodrome.contract_monitoring",
        attribute="AERODROME_CONTRACT_MONITORING_SPECS",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.aerodrome.contract_roles",
        attribute="CONTRACT_ROLES",
        order=5,
    ),
    protocol_family=ImportRef(
        module="almanak.connectors.aerodrome.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
)

__all__ = ["CONNECTOR"]
