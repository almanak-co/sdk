"""Benqi connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="benqi",
    kind=ProtocolKind.LENDING,
    gateway_connector=ImportRef(
        module="almanak.connectors.benqi.gateway.provider",
        attribute="BenqiGatewayConnector",
        order=23,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.benqi.receipt_parser_provider",
        attribute="BenqiReceiptParserConnector",
    ),
    strategy_intents=("SUPPLY", "BORROW", "REPAY", "WITHDRAW"),
    strategy_chains=("avalanche",),
)

__all__ = ["CONNECTOR"]
