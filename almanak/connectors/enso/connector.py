"""Enso connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    StrategyMatrixEntry,
    SupportedChainsSpec,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.core.chains.arbitrum import DESCRIPTOR as ARBITRUM
from almanak.core.chains.avalanche import DESCRIPTOR as AVALANCHE
from almanak.core.chains.base import DESCRIPTOR as BASE
from almanak.core.chains.bsc import DESCRIPTOR as BSC
from almanak.core.chains.ethereum import DESCRIPTOR as ETHEREUM
from almanak.core.chains.optimism import DESCRIPTOR as OPTIMISM
from almanak.core.chains.polygon import DESCRIPTOR as POLYGON

CONNECTOR = Connector(
    name="enso",
    kind=ProtocolKind.SWAP,
    gateway_connector=ImportRef(
        module="almanak.connectors.enso.gateway.provider",
        attribute="EnsoGatewayConnector",
        order=10,
    ),
    gateway_settings=ImportRef(
        module="almanak.connectors.enso.gateway.settings",
        attribute="EnsoGatewaySettings",
        order=20,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.enso.receipt_parser_provider",
        attribute="EnsoReceiptParserConnector",
    ),
    permission_infrastructure=ImportRef(
        module="almanak.connectors.enso.permission_hints",
        attribute="build_enso_infrastructure_permissions",
    ),
    deferred_refresh=ImportRef(
        module="almanak.connectors.enso.deferred_refresh_provider",
        attribute="EnsoDeferredRefreshConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.enso.compiler",
        attribute="EnsoCompiler",
    ),
    compiler_default_keys=("SWAP_CROSS_CHAIN",),
    capabilities=CapabilitiesSpec(
        keys=("enso",),
        module="almanak.connectors.enso.capabilities",
    ),
    strategy_intents=("SWAP",),
    supported_chains=SupportedChainsSpec(chains=(ETHEREUM, ARBITRUM, OPTIMISM, POLYGON, BASE, AVALANCHE, BSC)),
    # Aggregators render as aggregator rows instead of generic swap rows.
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="enso",
            category="aggregator",
            intents=("SWAP",),
        ),
    ),
)

__all__ = ["CONNECTOR"]
