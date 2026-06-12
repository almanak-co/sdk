"""Hyperliquid Connector — EXPERIMENTAL / NOT PRODUCTION-READY.

The Hyperliquid PERP production execution path has not shipped (VIB-4774).
No demo, no incubating strategy, no on-chain intent test references this
connector. The adapter / type definitions remain in-tree as a scaffold for
VIB-4774 — they are NOT a supported strategy-layer connector. The adapter
holds no keys and performs no in-process signing: write operations delegate
through ``MessageSigner`` / ``ExternalSigner``, and real signing ships
gateway-side with the VIB-4774 execution lane.

This connector is intentionally:
- Omitted from ``ConnectorRegistry`` (see deregistration block at end of file)
- Removed from ``almanak strat matrix`` (no longer probed in
  ``almanak/framework/cli/support_matrix.py``)
- Removed from public docs (``docs/api/connectors/`` + ``README.md``)

See ``docs/internal/plans/connector-status-audit-2026-05-23.html`` for the
audit that flagged this gap and VIB-4774 for the production-execution work.
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
