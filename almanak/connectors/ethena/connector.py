"""Ethena connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.protocol_ownership import SupportedChainsSpec

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
    compiler=ImportRef(
        module="almanak.connectors.ethena.compiler",
        attribute="EthenaCompiler",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("ethena",),
        module="almanak.connectors.ethena.supported_chains",
    ),
    strategy_intents=("STAKE", "UNSTAKE"),
    strategy_chains=("ethereum",),
)

__all__ = ["CONNECTOR"]
