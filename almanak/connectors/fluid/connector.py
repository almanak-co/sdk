"""Fluid connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="fluid",
    # SWAP-only at Phase 1 (VIB-5029). Fluid's LP surface is whitelist-gated
    # on-chain (Phase-0 finding, VIB-5028 §V4) and ships later via SmartLending
    # / smart vaults (VIB-5032); lending (fTokens) and vault borrow are
    # VIB-5030 / VIB-5031.
    kind=ProtocolKind.SWAP,
    address_tables=(
        AddressTableSpec(
            protocol="fluid",
            module="almanak.connectors.fluid.addresses",
            attribute="FLUID",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.fluid.gateway.provider",
        attribute="FluidGatewayConnector",
        order=4,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.fluid.receipt_parser_provider",
        attribute="FluidReceiptParserConnector",
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors.fluid.swap_quote_provider",
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
    strategy_intents=("SWAP",),
    strategy_chains=("arbitrum", "base", "ethereum", "polygon"),
)

__all__ = ["CONNECTOR"]
