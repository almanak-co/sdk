"""Silo V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec

CONNECTOR = Connector(
    name="silo_v2",
    kind=ProtocolKind.LENDING,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.silo_v2.receipt_parser_provider",
        attribute="SiloV2ReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.silo_v2.compiler",
        attribute="SiloV2Compiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("silo_v2",),
        module="almanak.connectors.silo_v2.capabilities",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("silo_v2",),
        module="almanak.connectors.silo_v2.supported_chains",
    ),
    strategy_intents=("SUPPLY", "BORROW", "REPAY", "WITHDRAW"),
    strategy_chains=("avalanche",),
)

__all__ = ["CONNECTOR"]
