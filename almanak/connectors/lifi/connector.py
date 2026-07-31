"""LiFi connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    StrategyMatrixEntry,
    SupportedChainsSpec,
)
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.avalanche import DESCRIPTOR as AVALANCHE
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.bsc import DESCRIPTOR as BSC
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.optimism import DESCRIPTOR as OPTIMISM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON

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
    supported_chains=SupportedChainsSpec(chains=(ETHEREUM, ARBITRUM, OPTIMISM, POLYGON, BASE, AVALANCHE, BSC)),
    # Aggregators render as aggregator rows instead of generic swap/bridge rows.
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="lifi",
            category="aggregator",
            intents=("SWAP", "BRIDGE"),
        ),
    ),
)

__all__ = ["CONNECTOR"]
