"""Fluid connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    LendingReadDecl,
    MetadataAmountEncoding,
    StrategyMatrixEntry,
    SupportedChainsSpec,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON
from almanak.core.intent_types import IntentType

CONNECTOR = Connector(
    name="fluid",
    external_ids={"defillama": "fluid-dex"},
    # SWAP is the primary kind; intent and chain declarations scope lending.
    kind=ProtocolKind.SWAP,
    # The platform spec emits ``protocol: "fluid_lending"`` for fToken
    # supply strategies — same connector, alias resolved at compile ingress.
    aliases=("fluid_lending",),
    address_tables=(
        AddressTableSpec(
            protocol="fluid",
            module="almanak.connectors.fluid.addresses",
            attribute="FLUID",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors._fluid_core.gateway.provider",
        attribute="FluidGatewayConnector",
        order=4,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.fluid.receipt_parser_provider",
        attribute="FluidReceiptParserConnector",
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors._fluid_core.swap_quote_provider",
        attribute="FluidSwapQuoteConnector",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.fluid.contract_roles",
        attribute="CONTRACT_ROLES",
        order=8,
    ),
    compiler=ImportRef(
        module="almanak.connectors.fluid.compiler",
        attribute="FluidCompiler",
    ),
    pool_data=ImportRef(
        module="almanak.connectors.fluid.pool_data",
        attribute="POOL_DATA_SPEC",
    ),
    # fToken account-state reads are market-scoped per underlying token.
    lending_read=LendingReadDecl(
        account_state=ImportRef(
            module="almanak.connectors.fluid.lending_read",
            attribute="ACCOUNT_STATE_READ_SPEC",
        ),
        market_table=ImportRef(
            module="almanak.connectors.fluid.lending_read",
            attribute="FLUID_FTOKEN_MARKETS",
        ),
        # Normalize the lending alias for accounting gates and position keys.
        aliases=("fluid_lending",),
        # fTokens have no market_id, so each underlying token is a distinct
        # position; Fluid vault CDPs remain market-keyed.
        token_keyed=True,
    ),
    # Lending metadata amounts are ERC-4626 asset base units.
    metadata_amount_encoding=MetadataAmountEncoding(lending="wei"),
    strategy_intents=(IntentType.SWAP, IntentType.SUPPLY, IntentType.WITHDRAW),
    # Lending is limited to Arbitrum and Base; SWAP is supported on all four chains.
    supported_chains=SupportedChainsSpec(
        chains=(ARBITRUM, BASE),
        intent_overrides={IntentType.SWAP: (ARBITRUM, BASE, ETHEREUM, POLYGON)},
    ),
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="fluid",
            category="swap",
            intents=(IntentType.SWAP,),
        ),
        StrategyMatrixEntry(
            matrix_name="fluid",
            category="lending",
            intents=(IntentType.SUPPLY, IntentType.WITHDRAW),
        ),
    ),
)

__all__ = ["CONNECTOR"]
