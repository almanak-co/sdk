"""Enso connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    StrategyMatrixEntry,
)

CONNECTOR = Connector(
    name="enso",
    kind=ProtocolKind.SWAP,
    gateway_connector=ImportRef(
        module="almanak.connectors.enso.gateway.provider",
        attribute="EnsoGatewayConnector",
        order=10,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.enso.receipt_parser_provider",
        attribute="EnsoReceiptParserConnector",
    ),
    permission_infrastructure=ImportRef(
        module="almanak.connectors.enso.permission_hints",
        attribute="build_enso_infrastructure_permissions",
    ),
    strategy_intents=("SWAP",),
    strategy_chains=("ethereum", "arbitrum", "optimism", "polygon", "base", "avalanche", "bnb"),
    # Aggregators render as aggregator rows instead of generic swap rows.
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="enso",
            category="aggregator",
            chains=frozenset(
                (
                    "ethereum",
                    "optimism",
                    "bsc",
                    "gnosis",
                    "polygon",
                    "zksync",
                    "base",
                    "arbitrum",
                    "avalanche",
                    "sonic",
                    "linea",
                    "berachain",
                    "sepolia",
                )
            ),
        ),
    ),
)

__all__ = ["CONNECTOR"]
