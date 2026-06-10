"""GMX V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    PerpsReadDecl,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec

CONNECTOR = Connector(
    name="gmx_v2",
    kind=ProtocolKind.PERP,
    address_tables=(
        AddressTableSpec(
            protocol="gmx_v2",
            module="almanak.connectors.gmx_v2.addresses",
            attribute="GMX_V2",
        ),
    ),
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
    compiler=ImportRef(
        module="almanak.connectors.gmx_v2.compiler",
        attribute="GMXV2Compiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("gmx_v2",),
        module="almanak.connectors.gmx_v2.capabilities",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("gmx_v2",),
        module="almanak.connectors.gmx_v2.supported_chains",
    ),
    primitive=ImportRef(
        module="almanak.connectors.gmx_v2.primitive",
        attribute="PRIMITIVE",
    ),
    perps_read=PerpsReadDecl(
        spec=ImportRef(module="almanak.connectors.gmx_v2.perps_read", attribute="PERPS_READ_SPEC"),
    ),
    strategy_intents=("PERP_OPEN", "PERP_CLOSE"),
    strategy_chains=("arbitrum", "avalanche"),
)

__all__ = ["CONNECTOR"]
