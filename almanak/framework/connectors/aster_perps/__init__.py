"""Aster Perps connector (Aster/ApolloX Diamond on BSC).

Aster is the on-chain perpetual trading platform (formerly ApolloX, rebranded
March 2025). PancakeSwap Perps runs on top of Aster as broker id = 2; raw Aster
use is broker id = 0. The canonical connector lives here; ``pancakeswap_perps``
is a thin shim that binds ``broker_id=2`` for backward compatibility.

Phase 1 scope (PRD: `docs/internal/discussions/aster-dex-integration-20260418.md`):
    - BSC only
    - Market orders only
    - Crypto markets (BTC/USD, ETH/USD, BNB/USD)
    - No SL/TP, no limit orders
    - Native BNB margin (openMarketTradeBNB) or ERC20 margin (openMarketTrade)

Multi-chain EVM, spot, Solana, funding-rate data are deferred to later phases
gated on named deep-research items (VIB-3044 epic).

Example usage — strategy-author facing::

    # Inside an IntentStrategy.decide()
    return Intent.perp_open(
        market="BTC/USD",
        collateral_token="BNB",
        collateral_amount=Decimal("0.1"),
        size_usd=Decimal("300"),
        is_long=True,
        max_slippage=Decimal("0.01"),
        protocol="aster_perps",              # canonical key
        leverage=Decimal("3"),
    )

    # Legacy callers may still pass protocol="pancakeswap_perps"; the compiler
    # routes that through the pancakeswap_perps shim, which forces broker_id=2.
"""

from .adapter import (
    GAS_CLOSE_TRADE,
    GAS_OPEN_MARKET_TRADE,
    GAS_OPEN_MARKET_TRADE_BNB,
    AsterPerpsAdapter,
    AsterPerpsConfig,
    AsterPerpsTx,
    PerpOpenOrderResult,
    build_close_transaction,
    build_open_transaction,
)
from .receipt_parser import (
    AsterPerpsReceiptParser,
    CloseTradeReceivedEvent,
    CloseTradeSuccessfulEvent,
    MarketPendingTradeEvent,
    OpenMarketTradeEvent,
    ParsedReceipt,
    PendingTradeRefundEvent,
)
from .sdk import (
    ASTER_BROKER_RAW,
    EVENT_CLOSE_TRADE_RECEIVED,
    EVENT_CLOSE_TRADE_SUCCESSFUL,
    EVENT_MARKET_PENDING_TRADE,
    EVENT_OPEN_MARKET_TRADE,
    EVENT_PENDING_TRADE_REFUND,
    NATIVE_BNB_ADDRESS,
    PCS_BROKER_ID,
    PRICE_DECIMALS,
    QTY_DECIMALS,
    SELECTOR_CLOSE_TRADE,
    SELECTOR_OPEN_MARKET_TRADE,
    SELECTOR_OPEN_MARKET_TRADE_BNB,
    OpenTradeStruct,
    encode_close_trade_calldata,
    encode_get_pending_trade_calldata,
    encode_get_position_by_hash_calldata,
    encode_open_market_trade_calldata,
    get_margin_token_address,
    get_pair_base,
    get_router_address,
    slippage_to_limit_price,
    usd_size_to_qty,
)

__all__ = [
    # Broker ids
    "ASTER_BROKER_RAW",
    "PCS_BROKER_ID",
    # Event topics
    "EVENT_CLOSE_TRADE_RECEIVED",
    "EVENT_CLOSE_TRADE_SUCCESSFUL",
    "EVENT_MARKET_PENDING_TRADE",
    "EVENT_OPEN_MARKET_TRADE",
    "EVENT_PENDING_TRADE_REFUND",
    # Gas budgets
    "GAS_CLOSE_TRADE",
    "GAS_OPEN_MARKET_TRADE",
    "GAS_OPEN_MARKET_TRADE_BNB",
    # Sentinels / constants
    "NATIVE_BNB_ADDRESS",
    "PRICE_DECIMALS",
    "QTY_DECIMALS",
    # Selectors
    "SELECTOR_CLOSE_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE_BNB",
    # Parser
    "AsterPerpsReceiptParser",
    "CloseTradeReceivedEvent",
    "CloseTradeSuccessfulEvent",
    "MarketPendingTradeEvent",
    "OpenMarketTradeEvent",
    "ParsedReceipt",
    "PendingTradeRefundEvent",
    # Adapter
    "AsterPerpsAdapter",
    "AsterPerpsConfig",
    "AsterPerpsTx",
    "OpenTradeStruct",
    "PerpOpenOrderResult",
    # Convenience
    "build_close_transaction",
    "build_open_transaction",
    "encode_close_trade_calldata",
    "encode_get_pending_trade_calldata",
    "encode_get_position_by_hash_calldata",
    "encode_open_market_trade_calldata",
    "get_margin_token_address",
    "get_pair_base",
    "get_router_address",
    "slippage_to_limit_price",
    "usd_size_to_qty",
]

# Connector registration (VIB-4298). The registry powers the (connector,
# intent, chain) coverage gate in scripts/ci/check_connector_registry.py
# and will be consumed by PR 2's intent-test coverage check.
from almanak.framework.connectors.registry import register_connector  # noqa: E402
from almanak.framework.intents.vocabulary import IntentType  # noqa: E402

register_connector(
    name="aster_perps",
    intents=(
        IntentType.PERP_OPEN,
        IntentType.PERP_CLOSE,
    ),
    chains=("bnb",),
)
