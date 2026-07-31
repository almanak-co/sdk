"""Gimo connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    SupportedChainsSpec,
)
from almanak.core.chains.zerog import DESCRIPTOR as ZEROG
from almanak.core.intent_types import IntentType

CONNECTOR = Connector(
    name="gimo",
    kind=ProtocolKind.LENDING,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.gimo.receipt_parser_provider",
        attribute="GimoReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.gimo.compiler",
        attribute="GimoCompiler",
    ),
    strategy_intents=(IntentType.STAKE, IntentType.UNSTAKE),
    supported_chains=SupportedChainsSpec(chains=(ZEROG,)),
)

__all__ = ["CONNECTOR"]
