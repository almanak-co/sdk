"""Across connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    SupportedChainsSpec,
)
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.linea import DESCRIPTOR as LINEA
from almanak.core.chains.optimism import DESCRIPTOR as OPTIMISM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON

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
    supported_chains=SupportedChainsSpec(chains=(ETHEREUM, ARBITRUM, BASE, OPTIMISM, POLYGON, LINEA)),
)

__all__ = ["CONNECTOR"]
