"""Polymarket connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec

CONNECTOR = Connector(
    name="polymarket",
    kind=ProtocolKind.PREDICTION_MARKET,
    gateway_connector=ImportRef(
        module="almanak.connectors.polymarket.gateway.provider",
        attribute="PolymarketGatewayConnector",
        order=11,
    ),
    gateway_settings=ImportRef(
        module="almanak.connectors.polymarket.gateway.settings",
        attribute="PolymarketGatewaySettings",
        order=10,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.polymarket.receipt_parser_provider",
        attribute="PolymarketReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.polymarket.compiler",
        attribute="PolymarketCompiler",
    ),
    compiler_default_keys=("PREDICTION",),
    capabilities=CapabilitiesSpec(
        keys=("polymarket",),
        module="almanak.connectors.polymarket.capabilities",
    ),
    primitive=ImportRef(
        module="almanak.connectors.polymarket.primitive",
        attribute="PRIMITIVE",
    ),
    prediction_read=ImportRef(
        module="almanak.connectors.polymarket.prediction_read",
        attribute="PREDICTION_READ_SPEC",
    ),
    prediction_execute=ImportRef(
        module="almanak.connectors.polymarket.clob_handler",
        attribute="PREDICTION_EXECUTE_SPEC",
    ),
    gateway_stub=ImportRef(
        module="almanak.connectors.polymarket.gateway_stub",
        attribute="GATEWAY_STUB_SPEC",
    ),
    strategy_intents=("PREDICTION_BUY", "PREDICTION_SELL", "PREDICTION_REDEEM"),
    strategy_chains=("polygon",),
)

__all__ = ["CONNECTOR"]
