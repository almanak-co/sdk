"""Curvance connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="curvance",
    kind=ProtocolKind.LENDING,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.curvance.receipt_parser_provider",
        attribute="CurvanceReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.curvance.compiler",
        attribute="CurvanceCompiler",
    ),
    strategy_intents=("SUPPLY", "BORROW", "REPAY", "WITHDRAW"),
    strategy_chains=("monad",),
)

__all__ = ["CONNECTOR"]
