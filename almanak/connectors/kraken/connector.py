"""Kraken connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import Connector

CONNECTOR = Connector(
    name="kraken",
    kind=ProtocolKind.SWAP,
    strategy_intents=("SWAP",),
    strategy_chains=None,
)

__all__ = ["CONNECTOR"]
