"""Across connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="across",
    kind=ProtocolKind.BRIDGE,
    gas_estimate_connector=ImportRef(
        module="almanak.connectors.across.gas_estimate_provider",
        attribute="AcrossGasEstimateConnector",
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.across.receipt_parser_provider",
        attribute="AcrossReceiptParserConnector",
    ),
    bridge_adapter=ImportRef(
        module="almanak.connectors.across.adapter",
        attribute="AcrossBridgeAdapter",
        order=1,
    ),
    compiler=ImportRef(
        module="almanak.connectors._strategy_base.bridge_compiler",
        attribute="BridgeCompiler",
    ),
    compiler_default_keys=("BRIDGE",),
    strategy_intents=("BRIDGE",),
    strategy_chains=("ethereum", "arbitrum", "base", "optimism", "polygon", "linea"),
)

__all__ = ["CONNECTOR"]
