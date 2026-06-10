"""Benqi connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec

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
    compiler=ImportRef(
        module="almanak.connectors.benqi.compiler",
        attribute="BenqiCompiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("benqi",),
        module="almanak.connectors.benqi.capabilities",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("benqi",),
        module="almanak.connectors.benqi.supported_chains",
    ),
    strategy_intents=("SUPPLY", "BORROW", "REPAY", "WITHDRAW"),
    strategy_chains=("avalanche",),
)

__all__ = ["CONNECTOR"]
