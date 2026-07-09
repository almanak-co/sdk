"""LiFi connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    StrategyMatrixEntry,
)

CONNECTOR = Connector(
    name="lifi",
    kind=ProtocolKind.BRIDGE,
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.lifi.receipt_parser_provider",
        attribute="LiFiReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.lifi.compiler",
        attribute="LiFiCompiler",
    ),
    deferred_refresh=ImportRef(
        module="almanak.connectors.lifi.deferred_refresh_provider",
        attribute="LiFiDeferredRefreshConnector",
    ),
    strategy_intents=("SWAP", "BRIDGE"),
    strategy_chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bsc"),
    # Aggregators render as aggregator rows instead of generic swap/bridge rows.
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="lifi",
            category="aggregator",
            chains=frozenset(
                ("ethereum", "optimism", "bsc", "gnosis", "polygon", "base", "arbitrum", "avalanche", "sonic", "linea")
            ),
        ),
    ),
)

__all__ = ["CONNECTOR"]
