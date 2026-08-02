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
    SupportedChainsSpec,
)
from almanak.connectors._strategy_base.protocol_ownership import CapabilitiesSpec
from almanak.connectors.hyperliquid.backtest_risk import BACKTEST_RISK as _BACKTEST_RISK
from almanak.core.chains.hyperevm import DESCRIPTOR as HYPEREVM
from almanak.core.intent_types import IntentType

CONNECTOR = Connector(
    name="hyperliquid",
    kind=ProtocolKind.PERP,
    # Strategy-facing execution surface: market open/close via CoreWriter on
    # HyperEVM (chain 999), plus PERP_WITHDRAW (a CoreWriter spotSend
    # HyperCore->HyperEVM USDC bridge, VIB-5617). See compiler.py for the scope
    # bounded by the CoreWriter action set + the perp intent vocabulary.
    strategy_intents=(IntentType.PERP_OPEN, IntentType.PERP_CLOSE, IntentType.PERP_WITHDRAW),
    supported_chains=SupportedChainsSpec(chains=(HYPEREVM,)),
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
    # VIB-6387 — TD-14 closure authority. Without it
    # ``get_teardown_post_condition("hyperliquid")`` is None, Plan-A's PERP branch
    # returns UNVERIFIABLE, and the VIB-6285 ratchet fails a teardown that closed
    # every position (measured on mainnet 2026-08-01, deployment:919d5bab4916).
    # ``_connector_teardown_slugs`` (VIB-5573) registers it under this connector's
    # ``discovery_keys`` | ``compiler_protocols`` | ``name``, which here is exactly
    # {"hyperliquid"} — the protocol string both position producers emit (the demo's
    # ``_PROTOCOL`` and the registry perp arm). The ``hl`` / ``hyper`` aliases above
    # belong to ``FeeModelDecl`` and are NOT connector protocol keys, so they are
    # deliberately not part of that set; a position never carries them.
    teardown_post_condition=ImportRef(
        module="almanak.connectors.hyperliquid.teardown_post_condition",
        attribute="hyperliquid_teardown_post_condition",
    ),
    capabilities=CapabilitiesSpec(
        keys=("hyperliquid",),
        module="almanak.connectors.hyperliquid.capabilities",
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
