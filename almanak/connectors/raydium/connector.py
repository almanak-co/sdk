"""Raydium connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="raydium",
    kind=ProtocolKind.LP,
    aliases=("raydium_clmm",),
    gateway_connector=ImportRef(
        module="almanak.connectors.raydium.gateway.provider",
        attribute="RaydiumGatewayConnector",
        order=21,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.raydium.receipt_parser_provider",
        attribute="RaydiumReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.raydium.compiler",
        attribute="RaydiumCompiler",
    ),
    compiler_protocols=("raydium_clmm",),
    strategy_intents=("LP_OPEN", "LP_CLOSE"),
    strategy_chains=("solana",),
)

__all__ = ["CONNECTOR"]
