"""Ethena connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="ethena",
    kind=ProtocolKind.LENDING,
    gateway_connector=ImportRef(
        module="almanak.connectors.ethena.gateway.provider",
        attribute="EthenaGatewayConnector",
        order=18,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.ethena.receipt_parser_provider",
        attribute="EthenaReceiptParserConnector",
    ),
    strategy_intents=("STAKE", "UNSTAKE"),
    strategy_chains=("ethereum",),
)

__all__ = ["CONNECTOR"]
