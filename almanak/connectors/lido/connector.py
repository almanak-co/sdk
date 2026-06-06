"""Lido connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="lido",
    kind=ProtocolKind.LENDING,
    gateway_connector=ImportRef(
        module="almanak.connectors.lido.gateway.provider",
        attribute="LidoGatewayConnector",
        order=17,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.lido.receipt_parser_provider",
        attribute="LidoReceiptParserConnector",
    ),
    strategy_intents=("STAKE", "UNSTAKE"),
    strategy_chains=("ethereum",),
)

__all__ = ["CONNECTOR"]
