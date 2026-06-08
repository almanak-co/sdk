"""Drift connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.solana_program import SolanaProgramSpec
from almanak.connectors.drift.constants import DRIFT_PROGRAM_ID

CONNECTOR = Connector(
    name="drift",
    kind=ProtocolKind.PERP,
    solana_programs=(
        SolanaProgramSpec(
            protocol="drift",
            program_id=DRIFT_PROGRAM_ID,
            notes="Drift V2 perpetual futures (Anchor program).",
        ),
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.drift.receipt_parser_provider",
        attribute="DriftReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.drift.compiler",
        attribute="DriftCompiler",
    ),
    strategy_intents=("PERP_OPEN", "PERP_CLOSE"),
    strategy_chains=("solana",),
)

__all__ = ["CONNECTOR"]
