"""Jupiter connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="jupiter",
    kind=ProtocolKind.SWAP,
    gateway_connector=ImportRef(
        module="almanak.connectors.jupiter.gateway.provider",
        attribute="JupiterGatewayConnector",
        order=7,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.jupiter.receipt_parser_provider",
        attribute="JupiterReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.jupiter.compiler",
        attribute="JupiterCompiler",
    ),
    strategy_intents=("SWAP",),
    strategy_chains=("solana",),
)

__all__ = ["CONNECTOR"]
