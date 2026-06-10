"""Lido connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.protocol_ownership import SupportedChainsSpec

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
    compiler=ImportRef(
        module="almanak.connectors.lido.compiler",
        attribute="LidoCompiler",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("lido",),
        module="almanak.connectors.lido.supported_chains",
    ),
    strategy_intents=("STAKE", "UNSTAKE"),
    strategy_chains=("ethereum",),
)

__all__ = ["CONNECTOR"]
