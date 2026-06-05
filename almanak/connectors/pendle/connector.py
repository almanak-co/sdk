"""Pendle connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="pendle",
    kind=ProtocolKind.YIELD_TRADING,
    gateway_connector=ImportRef(
        module="almanak.connectors.pendle.gateway.provider",
        attribute="PendleGatewayConnector",
        order=6,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.pendle.receipt_parser_provider",
        attribute="PendleReceiptParserConnector",
    ),
    protocol_metadata=ImportRef(
        module="almanak.connectors.pendle.metadata_provider",
        attribute="PendleProtocolMetadataConnector",
    ),
    principal_token_market_reader=ImportRef(
        module="almanak.connectors.pendle.on_chain_reader_provider",
        attribute="PendlePrincipalTokenMarketReadConnector",
    ),
    swap_route_inference=ImportRef(
        module="almanak.connectors.pendle.swap_route_inference",
        attribute="PendleSwapRouteInferenceConnector",
    ),
    accounting_treatment=ImportRef(
        module="almanak.connectors.pendle.accounting_spec",
        attribute="ACCOUNTING_TREATMENT_SPEC",
    ),
)

__all__ = ["CONNECTOR"]
