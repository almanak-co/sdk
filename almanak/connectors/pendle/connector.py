"""Pendle connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    StrategyMatrixEntry,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="pendle",
    kind=ProtocolKind.YIELD_TRADING,
    address_tables=(
        AddressTableSpec(
            protocol="pendle",
            module="almanak.connectors.pendle.addresses",
            attribute="PENDLE",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.pendle.gateway.provider",
        attribute="PendleGatewayConnector",
        order=6,
    ),
    gateway_settings=ImportRef(
        module="almanak.connectors.pendle.gateway.settings",
        attribute="PendleGatewaySettings",
        order=30,
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
    accounting_report=ImportRef(
        module="almanak.connectors.pendle.reporting",
        attribute="PendleAccountingReportConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.pendle.contract_monitoring",
        attribute="PENDLE_CONTRACT_MONITORING_SPECS",
    ),
    compiler=ImportRef(
        module="almanak.connectors.pendle.compiler",
        attribute="PendleCompiler",
    ),
    strategy_intents=("SWAP", "LP_OPEN", "LP_CLOSE", "WITHDRAW"),
    strategy_chains=("arbitrum", "ethereum"),
    # Matrix output renders Pendle as yield across deployed markets.
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="pendle",
            category="yield",
            chains=frozenset(("arbitrum", "ethereum", "plasma", "sonic", "base", "mantle", "bsc")),
        ),
    ),
)

__all__ = ["CONNECTOR"]
