"""Curve connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="curve",
    kind=ProtocolKind.LP,
    gateway_connector=ImportRef(
        module="almanak.connectors.curve.gateway.provider",
        attribute="CurveGatewayConnector",
        order=24,
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors.curve.swap_quote_provider",
        attribute="CurveSwapQuoteConnector",
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.curve.receipt_parser_provider",
        attribute="CurveReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.curve.compiler",
        attribute="CurveCompiler",
    ),
    strategy_intents=("SWAP", "LP_OPEN", "LP_CLOSE"),
    strategy_chains=("ethereum", "arbitrum", "optimism", "polygon", "base"),
)

__all__ = ["CONNECTOR"]
