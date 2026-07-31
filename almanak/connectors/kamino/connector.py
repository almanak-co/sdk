"""Kamino connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    SupportedChainsSpec,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.connectors._strategy_base.solana_program import SolanaProgramSpec
from almanak.core.chains.solana import DESCRIPTOR as SOLANA
from almanak.core.intent_types import IntentType

KAMINO_LENDING_PROGRAM_ID = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"

CONNECTOR = Connector(
    name="kamino",
    kind=ProtocolKind.LENDING,
    external_ids={"defillama": "kamino-lending"},
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
    capabilities=CapabilitiesSpec(
        keys=("kamino",),
        module="almanak.connectors.kamino.capabilities",
    ),
    strategy_intents=(IntentType.SUPPLY, IntentType.BORROW, IntentType.REPAY, IntentType.WITHDRAW),
    supported_chains=SupportedChainsSpec(chains=(SOLANA,)),
)

__all__ = ["CONNECTOR", "KAMINO_LENDING_PROGRAM_ID"]
