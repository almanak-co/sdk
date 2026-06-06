"""PancakeSwap Perps connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="pancakeswap_perps",
    kind=ProtocolKind.PERP,
    address_tables=(
        AddressTableSpec(
            protocol="pancakeswap_perps",
            module="almanak.connectors.pancakeswap_perps.addresses",
            attribute="PANCAKESWAP_PERPS",
        ),
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.pancakeswap_perps.receipt_parser_provider",
        attribute="PancakeSwapPerpsReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.aster_perps.compiler",
        attribute="AsterPerpsCompiler",
    ),
    strategy_intents=("PERP_OPEN", "PERP_CLOSE"),
    strategy_chains=("bnb",),
)

__all__ = ["CONNECTOR"]
