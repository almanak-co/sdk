"""Stargate connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="stargate",
    kind=ProtocolKind.BRIDGE,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.stargate.receipt_parser_provider",
        attribute="StargateReceiptParserConnector",
    ),
    bridge_adapter=ImportRef(
        module="almanak.connectors.stargate.adapter",
        attribute="StargateBridgeAdapter",
        order=2,
    ),
    compiler=ImportRef(
        module="almanak.connectors._strategy_base.bridge_compiler",
        attribute="BridgeCompiler",
    ),
    strategy_intents=("BRIDGE",),
    strategy_chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb"),
)

__all__ = ["CONNECTOR"]
