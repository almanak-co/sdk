"""Ethena connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    ImportRef,
    SupportedChainsSpec,
)

CONNECTOR = Connector(
    name="ethena",
    kind=ProtocolKind.LENDING,
    # sUSDe staking backtests as a yield strategy even though the kind is LENDING.
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="yield"),
    gateway_connector=ImportRef(
        module="almanak.connectors.ethena.gateway.provider",
        attribute="EthenaGatewayConnector",
        order=18,
    ),
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.ethena.receipt_parser_provider",
        attribute="EthenaReceiptParserConnector",
    ),
    compiler=ImportRef(
        module="almanak.connectors.ethena.compiler",
        attribute="EthenaCompiler",
    ),
    strategy_intents=("STAKE", "UNSTAKE"),
    supported_chains=SupportedChainsSpec(chains=("ethereum",)),
)

__all__ = ["CONNECTOR"]
