"""Hyperliquid connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    FeeModelDecl,
    FundingHistoryDecl,
    ImportRef,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec
from almanak.connectors.hyperliquid.backtest_risk import BACKTEST_RISK as _BACKTEST_RISK

CONNECTOR = Connector(
    name="hyperliquid",
    kind=ProtocolKind.PERP,
    fee_model=FeeModelDecl(
        model=ImportRef(module="almanak.connectors.hyperliquid.fee_model", attribute="HyperliquidFeeModel"),
        description="Hyperliquid perpetuals protocol fee model with maker/taker fees and volume tiers",
        aliases=("hl", "hyper"),
    ),
    backtest_strategy_type=BacktestStrategyTypeDecl(strategy_type="perp"),
    gateway_connector=ImportRef(
        module="almanak.connectors.hyperliquid.gateway.provider",
        attribute="HyperliquidGatewayConnector",
        order=15,
    ),
    compiler=ImportRef(
        module="almanak.connectors.hyperliquid.compiler",
        attribute="HyperliquidCompiler",
    ),
    capabilities=CapabilitiesSpec(
        keys=("hyperliquid",),
        module="almanak.connectors.hyperliquid.capabilities",
    ),
    supported_chains=SupportedChainsSpec(
        keys=("hyperliquid",),
        module="almanak.connectors.hyperliquid.supported_chains",
    ),
    primitive=ImportRef(
        module="almanak.connectors.hyperliquid.primitive",
        attribute="PRIMITIVE",
    ),
    funding_history=FundingHistoryDecl(
        venue="hyperliquid",
    ),
    backtest_risk=_BACKTEST_RISK,
)

__all__ = ["CONNECTOR"]
