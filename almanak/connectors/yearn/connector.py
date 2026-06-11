"""Yearn connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    ImportRef,
)

CONNECTOR = Connector(
    name="yearn",
    kind=ProtocolKind.VAULT,
    # Vault deposits backtest as a yield strategy.
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="yield"),
    gateway_connector=ImportRef(
        module="almanak.connectors.yearn.gateway.provider",
        attribute="YearnGatewayConnector",
        order=9,
    ),
)

__all__ = ["CONNECTOR"]
