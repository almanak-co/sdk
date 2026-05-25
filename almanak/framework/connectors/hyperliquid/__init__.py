"""Hyperliquid Connector — EXPERIMENTAL / NOT PRODUCTION-READY.

The Hyperliquid PERP production execution path has not shipped (VIB-4774).
No demo, no incubating strategy, no on-chain intent test references this
connector. The adapter / signer / type definitions remain in-tree as a
scaffold for VIB-4774 — they are NOT a supported strategy-layer connector.

This connector is intentionally:
- Omitted from ``ConnectorRegistry`` (see deregistration block at end of file)
- Removed from ``almanak strat matrix`` (no longer probed in
  ``almanak/framework/cli/support_matrix.py``)
- Removed from public docs (``docs/api/connectors/`` + ``README.md``)

See ``docs/internal/plans/connector-status-audit-2026-05-23.html`` for the
audit that flagged this gap and VIB-4774 for the production-execution work.
"""

from .adapter import (
    # Constants
    HYPERLIQUID_API_URLS,
    HYPERLIQUID_ASSETS,
    HYPERLIQUID_CHAIN_IDS,
    HYPERLIQUID_GAS_ESTIMATES,
    HYPERLIQUID_WS_URLS,
    CancelResult,
    # Signers
    EIP712Signer,
    ExternalSigner,
    # Main adapter
    HyperliquidAdapter,
    HyperliquidConfig,
    HyperliquidMarginMode,
    HyperliquidNetwork,
    HyperliquidOrder,
    HyperliquidOrderSide,
    HyperliquidOrderStatus,
    # Enums
    HyperliquidOrderType,
    # Position and Order types
    HyperliquidPosition,
    HyperliquidPositionSide,
    HyperliquidTimeInForce,
    MessageSigner,
    # Results
    OrderResult,
    SignedAction,
)

__all__ = [
    # Adapter
    "HyperliquidAdapter",
    "HyperliquidConfig",
    # Position and Order
    "HyperliquidPosition",
    "HyperliquidOrder",
    # Enums
    "HyperliquidOrderType",
    "HyperliquidOrderSide",
    "HyperliquidOrderStatus",
    "HyperliquidPositionSide",
    "HyperliquidTimeInForce",
    "HyperliquidMarginMode",
    "HyperliquidNetwork",
    # Results
    "OrderResult",
    "CancelResult",
    "SignedAction",
    # Signers
    "EIP712Signer",
    "ExternalSigner",
    "MessageSigner",
    # Constants
    "HYPERLIQUID_API_URLS",
    "HYPERLIQUID_WS_URLS",
    "HYPERLIQUID_CHAIN_IDS",
    "HYPERLIQUID_ASSETS",
    "HYPERLIQUID_GAS_ESTIMATES",
]

# Connector registration intentionally OMITTED (VIB-4774).
#
# Hyperliquid PERP production execution has not shipped. Registering the
# connector would pin (hyperliquid, PERP_OPEN/PERP_CLOSE, hyperliquid) cells
# in the intent-coverage required-set that no test or demo can satisfy.
#
# The adapter and signer code stay (above) as the scaffold for VIB-4774.
# Re-add the ``register_connector(...)`` call when production exec lands and
# at least one on-chain intent test + demo cover both PERP_OPEN / PERP_CLOSE.
