"""Kraken connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import Connector, SupportedChainsSpec
from almanak.core.intent_types import IntentType

CONNECTOR = Connector(
    name="kraken",
    kind=ProtocolKind.SWAP,
    strategy_intents=(IntentType.SWAP,),
    supported_chains=SupportedChainsSpec(chains=None),
)

__all__ = ["CONNECTOR"]
