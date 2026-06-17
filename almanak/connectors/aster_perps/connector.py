"""Aster Perps connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    PerpsReadDecl,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="aster_perps",
    kind=ProtocolKind.PERP,
    address_tables=(
        AddressTableSpec(
            protocol="aster_perps",
            module="almanak.connectors.aster_perps.addresses",
            attribute="ASTER_PERPS",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors._aster_perps_core.gateway.provider",
        attribute="AsterPerpsGatewayConnector",
        order=28,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.aster_perps.receipt_parser_provider",
        attribute="AsterPerpsReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.aster_perps.compiler",
        attribute="AsterPerpsCompiler",
    ),
    # 'pancakeswap_perps' is the deprecated name for the Aster Diamond (PCS Perps = broker id 2).
    perps_read=PerpsReadDecl(
        spec=ImportRef(module="almanak.connectors.aster_perps.perps_read", attribute="PERPS_READ_SPEC"),
        aliases=("pancakeswap_perps",),
    ),
    strategy_intents=("PERP_OPEN", "PERP_CLOSE"),
    strategy_chains=("bnb",),
)

__all__ = ["CONNECTOR"]
