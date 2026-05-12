"""Backwards-compatibility shim for ``pancakeswap_perps``.

PancakeSwap Perps is powered by Aster (formerly ApolloX, rebranded March 2025).
The canonical connector moved to ``almanak.framework.connectors.aster_perps``
(PRD: ``docs/internal/discussions/aster-dex-integration-20260418.md`` · VIB-3044).

This shim re-exports every previously-public symbol. Legacy consumers using
``from almanak.framework.connectors.pancakeswap_perps import ...`` continue to
work unchanged. The shim binds ``broker_id=2`` (PancakeSwap attribution) in
every config it constructs, which preserves pre-rebrand behaviour byte-for-byte.

Deprecation: importing from this module emits ``DeprecationWarning`` once per
process. New strategies must import from ``aster_perps`` directly and set
``protocol="aster_perps"`` on intents.
"""

from __future__ import annotations

import warnings

from almanak.core.contracts import PANCAKESWAP_PERPS_BROKER_ID

# Re-export every previously-public symbol via aster_perps. Class aliases keep
# the legacy names (PancakeSwapPerps*) pointing at the canonical Aster* class
# objects; factory functions mimic the old positional-default API shape.
from almanak.framework.connectors.aster_perps import (
    EVENT_CLOSE_TRADE_RECEIVED,
    EVENT_CLOSE_TRADE_SUCCESSFUL,
    EVENT_MARKET_PENDING_TRADE,
    EVENT_OPEN_MARKET_TRADE,
    EVENT_PENDING_TRADE_REFUND,
    GAS_CLOSE_TRADE,
    GAS_OPEN_MARKET_TRADE,
    GAS_OPEN_MARKET_TRADE_BNB,
    NATIVE_BNB_ADDRESS,
    PCS_BROKER_ID,
    PRICE_DECIMALS,
    QTY_DECIMALS,
    SELECTOR_CLOSE_TRADE,
    SELECTOR_OPEN_MARKET_TRADE,
    SELECTOR_OPEN_MARKET_TRADE_BNB,
    AsterPerpsAdapter,
    AsterPerpsConfig,
    AsterPerpsReceiptParser,
    AsterPerpsTx,
    CloseTradeReceivedEvent,
    CloseTradeSuccessfulEvent,
    MarketPendingTradeEvent,
    OpenMarketTradeEvent,
    OpenTradeStruct,
    ParsedReceipt,
    PendingTradeRefundEvent,
    PerpOpenOrderResult,
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
from almanak.framework.connectors.aster_perps import (
    build_close_transaction as _aster_build_close_transaction,
)
from almanak.framework.connectors.aster_perps import (
    build_open_transaction as _aster_build_open_transaction,
)

_DEPRECATION_EMITTED = False


def _emit_deprecation() -> None:
    """Emit the one-shot deprecation warning for the pancakeswap_perps import path.

    Called on module import and from the factory helpers below so callers that
    skip a star-import still see it.
    """
    global _DEPRECATION_EMITTED
    if _DEPRECATION_EMITTED:
        return
    _DEPRECATION_EMITTED = True
    warnings.warn(
        "almanak.framework.connectors.pancakeswap_perps is deprecated; "
        "use almanak.framework.connectors.aster_perps instead "
        "(PancakeSwap Perps is broker id=2 on the Aster Diamond).",
        DeprecationWarning,
        stacklevel=3,
    )


_emit_deprecation()


# -----------------------------------------------------------------------------
# Class aliases — legacy names continue to resolve to the canonical Aster* types.
# -----------------------------------------------------------------------------

PancakeSwapPerpsAdapter = AsterPerpsAdapter
PancakeSwapPerpsTx = AsterPerpsTx
PancakeSwapPerpsReceiptParser = AsterPerpsReceiptParser


def PancakeSwapPerpsConfig(  # noqa: N802 — class-style factory for back-compat API shape.
    chain: str = "bsc",
    wallet_address: str | None = None,
    broker_id: int = PANCAKESWAP_PERPS_BROKER_ID,
) -> AsterPerpsConfig:
    """Backwards-compat factory mimicking the old dataclass signature.

    The legacy ``PancakeSwapPerpsConfig`` had ``broker_id`` defaulted to 2. The
    canonical ``AsterPerpsConfig`` requires ``broker_id`` with no default so
    raw-aster callers are forced to pick 0 explicitly. This factory preserves
    the old default (=2) for every call that comes through the shim.
    """
    return AsterPerpsConfig(broker_id=broker_id, chain=chain, wallet_address=wallet_address)


# -----------------------------------------------------------------------------
# Convenience helpers — same old signatures, now route through the shim config.
# -----------------------------------------------------------------------------


def build_open_transaction(
    *,
    chain: str = "bsc",
    wallet_address: str | None = None,
    broker_id: int = PANCAKESWAP_PERPS_BROKER_ID,
    **open_kwargs,
) -> PerpOpenOrderResult:
    """Legacy ``build_open_transaction`` signature: defaults broker_id=2 (PCS)."""
    _emit_deprecation()
    return _aster_build_open_transaction(
        broker_id=broker_id,
        chain=chain,
        wallet_address=wallet_address,
        **open_kwargs,
    )


def build_close_transaction(
    *,
    trade_hash,
    chain: str = "bsc",
    wallet_address: str | None = None,
    broker_id: int = PANCAKESWAP_PERPS_BROKER_ID,
) -> AsterPerpsTx:
    """Legacy ``build_close_transaction`` signature: defaults broker_id=2 (PCS)."""
    _emit_deprecation()
    return _aster_build_close_transaction(
        trade_hash=trade_hash,
        broker_id=broker_id,
        chain=chain,
        wallet_address=wallet_address,
    )


__all__ = [
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
    "PCS_BROKER_ID",
    "PRICE_DECIMALS",
    "QTY_DECIMALS",
    # Selectors
    "SELECTOR_CLOSE_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE_BNB",
    # Parser
    "CloseTradeReceivedEvent",
    "CloseTradeSuccessfulEvent",
    "MarketPendingTradeEvent",
    "OpenMarketTradeEvent",
    "PancakeSwapPerpsReceiptParser",
    "ParsedReceipt",
    "PendingTradeRefundEvent",
    # Adapter
    "OpenTradeStruct",
    "PancakeSwapPerpsAdapter",
    "PancakeSwapPerpsConfig",
    "PancakeSwapPerpsTx",
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
    name="pancakeswap_perps",
    intents=(
        IntentType.PERP_OPEN,
        IntentType.PERP_CLOSE,
    ),
    chains=("bnb",),
)
