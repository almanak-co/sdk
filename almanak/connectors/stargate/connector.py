"""Stargate connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    SupportedChainsSpec,
)
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.avalanche import DESCRIPTOR as AVALANCHE
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.bsc import DESCRIPTOR as BSC
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.optimism import DESCRIPTOR as OPTIMISM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON
from almanak.core.intent_types import IntentType

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
    strategy_intents=(IntentType.BRIDGE,),
    supported_chains=SupportedChainsSpec(chains=(ETHEREUM, ARBITRUM, OPTIMISM, POLYGON, BASE, AVALANCHE, BSC)),
)

__all__ = ["CONNECTOR"]
