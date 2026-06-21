"""Pendle connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    StrategyMatrixEntry,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec

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
    capabilities=CapabilitiesSpec(
        keys=("pendle",),
        module="almanak.connectors.pendle.capabilities",
    ),
    strategy_intents=("SWAP", "LP_OPEN", "LP_CLOSE", "WITHDRAW"),
    strategy_chains=("arbitrum", "ethereum"),
    # Matrix output renders Pendle as "yield" — an explicit entry is required
    # because intent-derived categories would map WITHDRAW→lending and SWAP/LP
    # to swap/lp, never "yield".
    #
    # Chains: Pendle's compiler genuinely supports {arbitrum, ethereum, plasma}
    # (see `_check_pendle_chain_supported`, compiler.py:471, and the SWAP inline
    # check). We advertise only {arbitrum, ethereum} — the chains with intent-test
    # coverage (tests/intents/{arbitrum,ethereum}). The previously-advertised
    # sonic/base/mantle/bsc do NOT compile (no PT/YT market data) and were the
    # actual over-advertise bug (VIB-5300). plasma compiles but is intentionally
    # NOT advertised yet, pending intent-test coverage — tracked in VIB-5328.
    # The drift guard (tests/connectors/pendle/test_chain_truth_matrix_alignment.py)
    # enforces advertised ⊆ real-compile-truth; it deliberately does NOT key on
    # the advisory `PendleCompiler.chains` ClassVar. Compiler chain extension is
    # VIB-5324.
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="pendle",
            category="yield",
            chains=frozenset(("arbitrum", "ethereum")),
        ),
    ),
)

__all__ = ["CONNECTOR"]
