"""Orca connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    ImportRef,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.connectors._strategy_base.solana_program import SolanaProgramSpec
from almanak.connectors.orca.constants import METADATA_PROGRAM_ID, WHIRLPOOL_PROGRAM_ID

CONNECTOR = Connector(
    name="orca",
    kind=ProtocolKind.LP,
    aliases=("orca_whirlpools",),
    solana_programs=(
        SolanaProgramSpec(
            protocol="metaplex_token_metadata",
            program_id=METADATA_PROGRAM_ID,
            notes="Required by Orca openPositionWithMetadata for LP NFTs.",
        ),
        SolanaProgramSpec(
            protocol="orca",
            program_id=WHIRLPOOL_PROGRAM_ID,
            notes="Orca Whirlpools concentrated liquidity (CLMM).",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.orca.gateway.provider",
        attribute="OrcaGatewayConnector",
        order=22,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.orca.receipt_parser_provider",
        attribute="OrcaReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.orca.compiler",
        attribute="OrcaCompiler",
    ),
    compiler_protocols=("orca_whirlpools",),
    capabilities=CapabilitiesSpec(
        keys=("orca_whirlpools",),
        module="almanak.connectors.orca.capabilities",
    ),
    strategy_intents=("LP_OPEN", "LP_CLOSE"),
    strategy_chains=("solana",),
    backtest_strategy_type=BacktestStrategyTypeDecl(
        strategy_type="lp",
        aliases=("orca_whirlpools",),
        # Orca Whirlpools CLMM: tick ranges gate fee accrual.
        lp_economic_family="concentrated",
    ),
)

__all__ = ["CONNECTOR"]
