"""Kamino connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.solana_program import SolanaProgramSpec

KAMINO_LENDING_PROGRAM_ID = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"

CONNECTOR = Connector(
    name="kamino",
    kind=ProtocolKind.LENDING,
    aliases=("kamino_klend",),
    solana_programs=(
        SolanaProgramSpec(
            protocol="kamino",
            program_id=KAMINO_LENDING_PROGRAM_ID,
            notes="Kamino Lending V2 (KLend).",
        ),
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.kamino.receipt_parser_provider",
        attribute="KaminoReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.kamino.compiler",
        attribute="KaminoCompiler",
    ),
    compiler_protocols=("kamino",),
    strategy_intents=("SUPPLY", "BORROW", "REPAY", "WITHDRAW"),
    strategy_chains=("solana",),
)

__all__ = ["CONNECTOR", "KAMINO_LENDING_PROGRAM_ID"]
