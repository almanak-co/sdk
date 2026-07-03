"""Hyperliquid connector manifest."""

from __future__ import annotations

from almanak.connectors._base.types import ProtocolKind
from almanak.connectors._connector import (
    BacktestStrategyTypeDecl,
    Connector,
    FeeModelDecl,
    FundingHistoryDecl,
    ImportRef,
    PerpsReadDecl,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec, SupportedChainsSpec
from almanak.connectors.hyperliquid.backtest_risk import BACKTEST_RISK as _BACKTEST_RISK

CONNECTOR = Connector(
    name="hyperliquid",
    kind=ProtocolKind.PERP,
    # Strategy-facing execution surface: market open/close via CoreWriter on
    # HyperEVM (chain 999), plus PERP_WITHDRAW (a CoreWriter spotSend
    # HyperCore->HyperEVM USDC bridge, VIB-5617). See compiler.py for the scope
    # bounded by the CoreWriter action set + the perp intent vocabulary.
    strategy_intents=("PERP_OPEN", "PERP_CLOSE", "PERP_WITHDRAW"),
    strategy_chains=("hyperevm",),
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
    receipt_parser_connector=ImportRef(
        module="almanak.connectors.hyperliquid.receipt_parser_provider",
        attribute="HyperliquidReceiptParserConnector",
    ),
    # VIB-5595 — post-receipt fill-economics enrichment: read HyperCore
    # userFills / userFunding through the gateway and stamp a measured PerpData
    # onto the result so the perp accounting handler records fee / realized-PnL /
    # funding (the CoreWriter submit receipt settles off-EVM and carries none).
    runner_hook_connector=ImportRef(
        module="almanak.connectors.hyperliquid.runner_hooks",
        attribute="HyperliquidRunnerHookConnector",
    ),
    perps_read=PerpsReadDecl(
        spec=ImportRef(module="almanak.connectors.hyperliquid.perps_read", attribute="PERPS_READ_SPEC"),
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
        markets=(
            "ETH-USD",
            "BTC-USD",
            "ARB-USD",
            "LINK-USD",
            "SOL-USD",
            "DOGE-USD",
            "ATOM-USD",
            "APT-USD",
        ),
        backtest_provider=ImportRef(
            module="almanak.connectors.hyperliquid.backtest_funding",
            attribute="HyperliquidFundingProvider",
        ),
    ),
    backtest_risk=_BACKTEST_RISK,
)

__all__ = ["CONNECTOR"]
