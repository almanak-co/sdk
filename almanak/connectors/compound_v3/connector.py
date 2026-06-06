"""Compound V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="compound_v3",
    kind=ProtocolKind.LENDING,
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
