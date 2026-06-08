"""Compound V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="compound_v3",
    kind=ProtocolKind.LENDING,
    address_tables=(
        AddressTableSpec(
            protocol="compound_v3",
            module="almanak.connectors.compound_v3.addresses",
            attribute="COMPOUND_V3_COMET_ADDRESSES",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.compound_v3.gateway.provider",
        attribute="CompoundV3GatewayConnector",
        order=3,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.compound_v3.receipt_parser_provider",
        attribute="CompoundV3ReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.compound_v3.compiler",
        attribute="CompoundV3Compiler",
    ),
    strategy_intents=("SUPPLY", "BORROW", "REPAY", "WITHDRAW"),
    strategy_chains=("ethereum", "arbitrum", "base", "optimism", "polygon"),
)

__all__ = ["CONNECTOR"]
