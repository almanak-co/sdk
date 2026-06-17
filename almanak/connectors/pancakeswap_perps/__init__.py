"""Backwards-compatibility shim for ``pancakeswap_perps``.

PancakeSwap Perps is powered by Aster, formerly ApolloX. The canonical
connector lives in ``almanak.connectors.aster_perps``; this package keeps the
legacy import path and binds ``broker_id=2`` for PancakeSwap attribution.

The package body is intentionally lazy. Importing submodules such as
``almanak.connectors.pancakeswap_perps.addresses`` must stay pure so descriptor
discovery can load connector-owned data without triggering strategy registry
registration. Accessing a legacy public symbol emits a one-shot
``DeprecationWarning`` and resolves the canonical Aster symbol. Strategy
registration is descriptor-owned in ``connector.py``.
"""

from __future__ import annotations

import importlib
import warnings
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.connectors._aster_perps_core import (
        AsterPerpsConfig,
        AsterPerpsTx,
        PerpOpenOrderResult,
    )

PANCAKESWAP_PERPS_BROKER_ID = 2

_DEPRECATION_EMITTED = False
_registered = False


def _emit_deprecation() -> None:
    """Emit the one-shot deprecation warning for the legacy import path."""
    global _DEPRECATION_EMITTED
    if _DEPRECATION_EMITTED:
        return
    _DEPRECATION_EMITTED = True
    warnings.warn(
        "almanak.connectors.pancakeswap_perps is deprecated; "
        "use almanak.connectors.aster_perps instead "
        "(PancakeSwap Perps is broker id=2 on the Aster Diamond).",
        DeprecationWarning,
        stacklevel=3,
    )


def _register_once() -> None:
    """Compatibility no-op; strategy registration lives in connector.py."""
    global _registered
    if _registered:
        return
    _registered = True


def _aster_symbol(name: str) -> Any:
    """Resolve one public symbol from the shared Aster implementation core.

    Resolves from ``_aster_perps_core`` (foundation), not the sibling
    ``aster_perps`` leaf connector, so this shim does not import another
    concrete connector (self-containment: deleting ``aster_perps/`` must not
    break ``pancakeswap_perps/``).
    """
    module = importlib.import_module("almanak.connectors._aster_perps_core")
    return getattr(module, name)


_ASTER_EXPORTS: dict[str, str] = {
    # Event topics
    "EVENT_CLOSE_TRADE_RECEIVED": "EVENT_CLOSE_TRADE_RECEIVED",
    "EVENT_CLOSE_TRADE_SUCCESSFUL": "EVENT_CLOSE_TRADE_SUCCESSFUL",
    "EVENT_MARKET_PENDING_TRADE": "EVENT_MARKET_PENDING_TRADE",
    "EVENT_OPEN_MARKET_TRADE": "EVENT_OPEN_MARKET_TRADE",
    "EVENT_PENDING_TRADE_REFUND": "EVENT_PENDING_TRADE_REFUND",
    # Gas budgets
    "GAS_CLOSE_TRADE": "GAS_CLOSE_TRADE",
    "GAS_OPEN_MARKET_TRADE": "GAS_OPEN_MARKET_TRADE",
    "GAS_OPEN_MARKET_TRADE_BNB": "GAS_OPEN_MARKET_TRADE_BNB",
    # Sentinels / constants
    "NATIVE_BNB_ADDRESS": "NATIVE_BNB_ADDRESS",
    "PCS_BROKER_ID": "PCS_BROKER_ID",
    "PRICE_DECIMALS": "PRICE_DECIMALS",
    "QTY_DECIMALS": "QTY_DECIMALS",
    # Selectors
    "SELECTOR_CLOSE_TRADE": "SELECTOR_CLOSE_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE": "SELECTOR_OPEN_MARKET_TRADE",
    "SELECTOR_OPEN_MARKET_TRADE_BNB": "SELECTOR_OPEN_MARKET_TRADE_BNB",
    # Parser aliases
    "CloseTradeReceivedEvent": "CloseTradeReceivedEvent",
    "CloseTradeSuccessfulEvent": "CloseTradeSuccessfulEvent",
    "MarketPendingTradeEvent": "MarketPendingTradeEvent",
    "OpenMarketTradeEvent": "OpenMarketTradeEvent",
    "PancakeSwapPerpsReceiptParser": "AsterPerpsReceiptParser",
    "ParsedReceipt": "ParsedReceipt",
    "PendingTradeRefundEvent": "PendingTradeRefundEvent",
    # Adapter aliases
    "OpenTradeStruct": "OpenTradeStruct",
    "PancakeSwapPerpsAdapter": "AsterPerpsAdapter",
    "PancakeSwapPerpsTx": "AsterPerpsTx",
    "PerpOpenOrderResult": "PerpOpenOrderResult",
    # Convenience
    "encode_close_trade_calldata": "encode_close_trade_calldata",
    "encode_get_pending_trade_calldata": "encode_get_pending_trade_calldata",
    "encode_get_position_by_hash_calldata": "encode_get_position_by_hash_calldata",
    "encode_open_market_trade_calldata": "encode_open_market_trade_calldata",
    "get_margin_token_address": "get_margin_token_address",
    "get_pair_base": "get_pair_base",
    "get_router_address": "get_router_address",
    "slippage_to_limit_price": "slippage_to_limit_price",
    "usd_size_to_qty": "usd_size_to_qty",
}


def PancakeSwapPerpsConfig(  # noqa: N802 - class-style factory for back-compat API shape.
    chain: str = "bsc",
    wallet_address: str | None = None,
    broker_id: int = PANCAKESWAP_PERPS_BROKER_ID,
) -> AsterPerpsConfig:
    """Backwards-compatible factory defaulting broker id to PancakeSwap."""
    _emit_deprecation()
    _register_once()
    config_cls = _aster_symbol("AsterPerpsConfig")
    return config_cls(broker_id=broker_id, chain=chain, wallet_address=wallet_address)


def build_open_transaction(
    *,
    chain: str = "bsc",
    wallet_address: str | None = None,
    broker_id: int = PANCAKESWAP_PERPS_BROKER_ID,
    **open_kwargs: Any,
) -> PerpOpenOrderResult:
    """Legacy ``build_open_transaction`` signature: defaults broker_id=2."""
    _emit_deprecation()
    _register_once()
    builder = _aster_symbol("build_open_transaction")
    return builder(
        broker_id=broker_id,
        chain=chain,
        wallet_address=wallet_address,
        **open_kwargs,
    )


def build_close_transaction(
    *,
    trade_hash: Any,
    chain: str = "bsc",
    wallet_address: str | None = None,
    broker_id: int = PANCAKESWAP_PERPS_BROKER_ID,
) -> AsterPerpsTx:
    """Legacy ``build_close_transaction`` signature: defaults broker_id=2."""
    _emit_deprecation()
    _register_once()
    builder = _aster_symbol("build_close_transaction")
    return builder(
        trade_hash=trade_hash,
        broker_id=broker_id,
        chain=chain,
        wallet_address=wallet_address,
    )


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access for legacy Aster aliases."""
    aster_name = _ASTER_EXPORTS.get(name)
    if aster_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    _emit_deprecation()
    value = _aster_symbol(aster_name)
    globals()[name] = value
    _register_once()
    return value


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
