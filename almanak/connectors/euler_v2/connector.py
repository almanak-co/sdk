"""Euler V2 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec

CONNECTOR = Connector(
    name="euler_v2",
    kind=ProtocolKind.LENDING,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.euler_v2.receipt_parser_provider",
        attribute="EulerV2ReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.euler_v2.compiler",
        attribute="EulerV2Compiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("euler_v2",),
        module="almanak.connectors.euler_v2.capabilities",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("euler_v2",),
        module="almanak.connectors.euler_v2.supported_chains",
    ),
    strategy_intents=("SUPPLY", "BORROW", "REPAY", "WITHDRAW"),
    strategy_chains=("ethereum", "avalanche"),
)

__all__ = ["CONNECTOR"]
