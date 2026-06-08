"""Jupiter connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.solana_program import SolanaProgramSpec

JUPITER_V6_PROGRAM_ID = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4"

CONNECTOR = Connector(
    name="jupiter",
    kind=ProtocolKind.SWAP,
    solana_programs=(
        SolanaProgramSpec(
            protocol="jupiter",
            program_id=JUPITER_V6_PROGRAM_ID,
            notes="Jupiter v6 aggregator - required for any Jupiter-routed swap.",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.jupiter.gateway.provider",
        attribute="JupiterGatewayConnector",
        order=7,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.jupiter.receipt_parser_provider",
        attribute="JupiterReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.jupiter.compiler",
        attribute="JupiterCompiler",
    ),
    strategy_intents=("SWAP",),
    strategy_chains=("solana",),
)

__all__ = ["CONNECTOR", "JUPITER_V6_PROGRAM_ID"]
