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
        HYPERLIQUID_CHAIN_IDS,
        HYPERLIQUID_EIP712_DOMAIN_IDS,
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
    from .markets import PerpMarket, normalize_symbol, resolve_market, seeded_symbols

__all__ = [
    "CancelResult",
    "ExternalSigner",
    "HYPERLIQUID_API_URLS",
    "HYPERLIQUID_CHAIN_IDS",
    "HYPERLIQUID_EIP712_DOMAIN_IDS",
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
    "PerpMarket",
    "SignedAction",
    "normalize_symbol",
    "resolve_market",
    "seeded_symbols",
]

# ALM-3186: names removed from this connector's public surface, mapped to the
# replacement. Listed so a stale import fails with an actionable message rather
# than a bare "no attribute" — the removed map was WRONG (SOL→2 is ATOM) and
# silently defaulted unknown symbols to index 0 (BTC), so re-exporting it under
# any alias would keep the defect reachable.
_REMOVED: dict[str, str] = {
    "HYPERLIQUID_ASSETS": (
        "HYPERLIQUID_ASSETS was removed in ALM-3186: its indices were unverified "
        "and several were wrong on the live venue (SOL was mapped to 2, which is "
        "ATOM), and callers read it with .get(asset, 0) so an unknown symbol "
        "silently meant asset 0 (BTC). Use "
        "almanak.connectors.hyperliquid.resolve_market(symbol).asset_index, which "
        "is verified against the venue and raises on an unknown symbol."
    ),
}

_LAZY: dict[str, tuple[str, str]] = {
    "CancelResult": (".adapter", "CancelResult"),
    "ExternalSigner": (".adapter", "ExternalSigner"),
    "HYPERLIQUID_API_URLS": (".adapter", "HYPERLIQUID_API_URLS"),
    "HYPERLIQUID_CHAIN_IDS": (".adapter", "HYPERLIQUID_CHAIN_IDS"),
    "HYPERLIQUID_EIP712_DOMAIN_IDS": (".adapter", "HYPERLIQUID_EIP712_DOMAIN_IDS"),
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
    "PerpMarket": (".markets", "PerpMarket"),
    "SignedAction": (".adapter", "SignedAction"),
    "normalize_symbol": (".markets", "normalize_symbol"),
    "resolve_market": (".markets", "resolve_market"),
    "seeded_symbols": (".markets", "seeded_symbols"),
}


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        if name in _REMOVED:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}: {_REMOVED[name]}")
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    return value
