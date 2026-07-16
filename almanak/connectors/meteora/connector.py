"""Meteora connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.connectors._strategy_base.solana_program import SolanaProgramSpec
from almanak.connectors.meteora.constants import DLMM_PROGRAM_ID

CONNECTOR = Connector(
    name="meteora",
    kind=ProtocolKind.LP,
    aliases=("meteora_dlmm",),
    solana_programs=(
        SolanaProgramSpec(
            protocol="meteora",
            program_id=DLMM_PROGRAM_ID,
            notes="Meteora DLMM (discrete-bin liquidity book).",
        ),
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.meteora.receipt_parser_provider",
        attribute="MeteoraReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.meteora.compiler",
        attribute="MeteoraCompiler",
    ),
    compiler_protocols=("meteora_dlmm",),
    capabilities=CapabilitiesSpec(
        keys=("meteora_dlmm",),
        module="almanak.connectors.meteora.capabilities",
    ),
    strategy_intents=("LP_OPEN", "LP_CLOSE"),
    strategy_chains=("solana",),
    backtest_strategy_type=BacktestStrategyTypeDecl(
        strategy_type="lp",
        aliases=("meteora_dlmm",),
        # Meteora DLMM: discrete-bin Liquidity Book economics.
        lp_economic_family="bin",
    ),
)

__all__ = ["CONNECTOR"]
