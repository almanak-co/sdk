"""Compound V3 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    LendingReadDecl,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec

CONNECTOR = Connector(
    name="compound_v3",
    kind=ProtocolKind.LENDING,
    address_tables=(
        AddressTableSpec(
            protocol="compound_v3",
            module="almanak.connectors.compound_v3.addresses",
            attribute="COMPOUND_V3_COMET_ADDRESSES",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.compound_v3.gateway.provider",
        attribute="CompoundV3GatewayConnector",
        order=3,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.compound_v3.receipt_parser_provider",
        attribute="CompoundV3ReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.compound_v3.compiler",
        attribute="CompoundV3Compiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("compound_v3",),
        module="almanak.connectors.compound_v3.capabilities",
    ),
    primitive=ImportRef(
        module="almanak.connectors.compound_v3.primitive",
        attribute="PRIMITIVE",
    ),
    # Market-scoped Comet reads (VIB-4929 PR-3b) + summed multi-collateral health (VIB-4851 PR-2).
    lending_read=LendingReadDecl(
        account_state=ImportRef(
            module="almanak.connectors.compound_v3.lending_read", attribute="ACCOUNT_STATE_READ_SPEC"
        ),
        market_table=ImportRef(
            module="almanak.connectors.compound_v3.addresses", attribute="COMPOUND_V3_ACCOUNT_STATE_MARKETS"
        ),
        market_health=ImportRef(
            module="almanak.connectors.compound_v3.lending_read", attribute="read_compound_v3_market_health"
        ),
    ),
    strategy_intents=("SUPPLY", "BORROW", "REPAY", "WITHDRAW"),
    strategy_chains=("ethereum", "arbitrum", "base", "optimism", "polygon"),
)

__all__ = ["CONNECTOR"]
