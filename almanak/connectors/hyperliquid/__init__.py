"""Hyperliquid perpetuals connector — HyperEVM / CoreWriter.

Executes ``PERP_OPEN`` / ``PERP_CLOSE`` on **HyperEVM** (chain id 999) by
calling the CoreWriter system contract (``0x3333…3333``) with a versioned
action, submitted as an ordinary gateway ActionBundle transaction. Reads
(position, oracle price) go through HyperCore read precompiles via the gateway.
The strategy holds no keys and signs nothing — it returns an ``Intent``.

Scope is bounded by the CoreWriter action set and the perp intent vocabulary:
market open (IOC) and market close (reduce-only IOC, full/partial). CoreWriter
has no set-leverage action and no native trigger orders, so leverage changes
and TP/SL are not reachable through this path (they need the L1 EIP-712 API);
see ``compiler.py``.

Order encoding lives in ``sdk.py`` (byte-exact, szDecimals-aware), market
resolution in ``markets.py`` (static seed of the liquid majors, fail-closed on
unknowns — see the module docstring for the seed vs. dynamic-universe seam).

Note: ``adapter.py`` (the abandoned V1-style native-L1 REST simulation) is
retained only for its type definitions and is NOT on the execution path — the
CoreWriter compiler does not use it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        HYPERLIQUID_API_URLS,
        HYPERLIQUID_ASSETS,
        HYPERLIQUID_CHAIN_IDS,
        HYPERLIQUID_GAS_ESTIMATES,
        HYPERLIQUID_WS_URLS,
        CancelResult,
        ExternalSigner,
        HyperliquidAdapter,
        HyperliquidConfig,
        HyperliquidMarginMode,
        HyperliquidNetwork,
        HyperliquidOrder,
        HyperliquidOrderSide,
        HyperliquidOrderStatus,
        HyperliquidOrderType,
        HyperliquidPosition,
        HyperliquidPositionSide,
        HyperliquidTimeInForce,
        MessageSigner,
        OrderResult,
        SignedAction,
    )

__all__ = [
    "CancelResult",
    "ExternalSigner",
    "HYPERLIQUID_API_URLS",
    "HYPERLIQUID_ASSETS",
    "HYPERLIQUID_CHAIN_IDS",
    "HYPERLIQUID_GAS_ESTIMATES",
    "HYPERLIQUID_WS_URLS",
    "HyperliquidAdapter",
    "HyperliquidConfig",
    "HyperliquidMarginMode",
    "HyperliquidNetwork",
    "HyperliquidOrder",
    "HyperliquidOrderSide",
    "HyperliquidOrderStatus",
    "HyperliquidOrderType",
    "HyperliquidPosition",
    "HyperliquidPositionSide",
    "HyperliquidTimeInForce",
    "MessageSigner",
    "OrderResult",
    "SignedAction",
]

_LAZY: dict[str, tuple[str, str]] = {
    "CancelResult": (".adapter", "CancelResult"),
    "ExternalSigner": (".adapter", "ExternalSigner"),
    "HYPERLIQUID_API_URLS": (".adapter", "HYPERLIQUID_API_URLS"),
    "HYPERLIQUID_ASSETS": (".adapter", "HYPERLIQUID_ASSETS"),
    "HYPERLIQUID_CHAIN_IDS": (".adapter", "HYPERLIQUID_CHAIN_IDS"),
    "HYPERLIQUID_GAS_ESTIMATES": (".adapter", "HYPERLIQUID_GAS_ESTIMATES"),
    "HYPERLIQUID_WS_URLS": (".adapter", "HYPERLIQUID_WS_URLS"),
    "HyperliquidAdapter": (".adapter", "HyperliquidAdapter"),
    "HyperliquidConfig": (".adapter", "HyperliquidConfig"),
    "HyperliquidMarginMode": (".adapter", "HyperliquidMarginMode"),
    "HyperliquidNetwork": (".adapter", "HyperliquidNetwork"),
    "HyperliquidOrder": (".adapter", "HyperliquidOrder"),
    "HyperliquidOrderSide": (".adapter", "HyperliquidOrderSide"),
    "HyperliquidOrderStatus": (".adapter", "HyperliquidOrderStatus"),
    "HyperliquidOrderType": (".adapter", "HyperliquidOrderType"),
    "HyperliquidPosition": (".adapter", "HyperliquidPosition"),
    "HyperliquidPositionSide": (".adapter", "HyperliquidPositionSide"),
    "HyperliquidTimeInForce": (".adapter", "HyperliquidTimeInForce"),
    "MessageSigner": (".adapter", "MessageSigner"),
    "OrderResult": (".adapter", "OrderResult"),
    "SignedAction": (".adapter", "SignedAction"),
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    return value
