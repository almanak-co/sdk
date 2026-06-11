"""Aerodrome connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    DexVolumeDecl,
    FeeModelDecl,
    ImportRef,
    MetadataAmountEncoding,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="aerodrome",
    kind=ProtocolKind.LP,
    dex_volume=DexVolumeDecl(
        chains=("base",),
        amm_family="solidly_v2",
        chain_default=("base",),
    ),
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.aerodrome.fee_model", attribute="AerodromeFeeModel"),
        description="Aerodrome DEX fee model with stable/volatile pool distinction",
        aliases=("aero", "velodrome"),
    ),
    aliases=("aerodrome_slipstream",),
    address_tables=(
        AddressTableSpec(
            protocol="aerodrome",
            module="almanak.connectors.aerodrome.addresses",
            attribute="AERODROME",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.aerodrome.gateway.provider",
        attribute="AerodromeGatewayConnector",
        order=13,
    ),
    pool_reader=ImportRef(
        module="almanak.connectors.aerodrome.pool_reader",
        attribute="POOL_READER_SPEC",
    ),
    agent_read_connector=ImportRef(
        module="almanak.connectors.aerodrome.agent_read_provider",
        attribute="AerodromeSlipstreamAgentReadConnector",
        order=3,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.aerodrome.receipt_parser_provider",
        attribute="AerodromeReceiptParserConnector",
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors.aerodrome.swap_quote_provider",
        attribute="AerodromeSwapQuoteConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.aerodrome.contract_monitoring",
        attribute="AERODROME_CONTRACT_MONITORING_SPECS",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.aerodrome.contract_roles",
        attribute="CONTRACT_ROLES",
        order=5,
    ),
    protocol_family=ImportRef(
        module="almanak.connectors.aerodrome.protocol_family",
        attribute="PROTOCOL_FAMILY",
    ),
    compiler=ImportRef(
        module="almanak.connectors.aerodrome.compiler",
        attribute="AerodromeCompiler",
    ),
    primitive=ImportRef(
        module="almanak.connectors.aerodrome.primitive",
        attribute="PRIMITIVE",
    ),
    # Aerodrome's SWAP compiler ships amount_in as a human-readable Decimal (VIB-3747).
    metadata_amount_encoding=MetadataAmountEncoding(swap="human"),
    strategy_intents=("SWAP", "LP_OPEN", "LP_CLOSE"),
    strategy_chains=("base", "optimism"),
)

__all__ = ["CONNECTOR"]
