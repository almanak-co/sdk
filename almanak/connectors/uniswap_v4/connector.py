"""Uniswap V4 connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    Connector,
    ImportRef,
    StrategyMatrixEntry,
)
from almanak.connectors._strategy_base.address_table import AddressTableSpec

CONNECTOR = Connector(
    name="uniswap_v4",
    kind=ProtocolKind.LP,
    address_tables=(
        AddressTableSpec(
            protocol="uniswap_v4",
            module="almanak.connectors.uniswap_v4.addresses",
            attribute="UNISWAP_V4",
        ),
    ),
    gateway_connector=ImportRef(
        module="almanak.connectors.uniswap_v4.gateway.provider",
        attribute="UniswapV4GatewayConnector",
        order=1,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.uniswap_v4.receipt_parser_provider",
        attribute="UniswapV4ReceiptParserConnector",
    ),
    contract_monitoring=ImportRef(
        module="almanak.connectors.uniswap_v4.contract_monitoring",
        attribute="UNISWAP_V4_CONTRACT_MONITORING_SPECS",
    ),
    runner_hook_connector=ImportRef(
        module="almanak.connectors.uniswap_v4.runner_hooks",
        attribute="UniswapV4RunnerHookConnector",
    ),
    swap_quote_connector=ImportRef(
        module="almanak.connectors.uniswap_v4.swap_quote_provider",
        attribute="UniswapV4SwapQuoteConnector",
    ),
    contract_roles=ImportRef(
        module="almanak.connectors.uniswap_v4.contract_roles",
        attribute="CONTRACT_ROLES",
        order=2,
    ),
    compiler=ImportRef(
        module="almanak.connectors.uniswap_v4.compiler",
        attribute="UniswapV4Compiler",
    ),
    strategy_intents=("SWAP", "LP_OPEN", "LP_CLOSE", "LP_COLLECT_FEES"),
    strategy_chains=("ethereum", "arbitrum", "base"),
    # Matrix output covers deployed V4 chains for both swap and LP rows.
    strategy_matrix_entries=(
        StrategyMatrixEntry(
            matrix_name="uniswap_v4",
            category="swap",
            chains=frozenset(("ethereum", "base", "arbitrum", "optimism", "polygon", "avalanche", "bsc")),
        ),
        StrategyMatrixEntry(
            matrix_name="uniswap_v4",
            category="lp",
            chains=frozenset(("ethereum", "base", "arbitrum", "optimism", "polygon", "avalanche", "bsc")),
        ),
    ),
)

__all__ = ["CONNECTOR"]
