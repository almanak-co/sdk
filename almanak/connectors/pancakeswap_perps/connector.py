"""PancakeSwap Perps connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    SupportedChainsSpec,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec
from almanak.core.chains.bsc import DESCRIPTOR as BSC
from almanak.core.intent_types import IntentType

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
        module="almanak.connectors._aster_perps_core.compiler",
        attribute="AsterPerpsCompiler",
    ),
    strategy_intents=(IntentType.PERP_OPEN, IntentType.PERP_CLOSE),
    supported_chains=SupportedChainsSpec(chains=(BSC,)),
)

__all__ = ["CONNECTOR"]
