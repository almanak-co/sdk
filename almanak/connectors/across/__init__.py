"""Across Bridge Adapter.

Across is a fast, secure, and capital-efficient cross-chain bridge that uses
an optimistic verification model with UMA's oracle for dispute resolution.

Features:
- Fast finality (~1-4 minutes for most routes)
- Low fees using relayer competition
- Supports ETH, USDC, WBTC and other major tokens
- Available on Ethereum, Arbitrum, Optimism, Base, Polygon, and more

Example:
    from almanak.connectors.across import AcrossBridgeAdapter, AcrossConfig

    config = AcrossConfig(timeout_seconds=1800)  # 30 min timeout
    adapter = AcrossBridgeAdapter(config)

    # Get a quote
    quote = adapter.get_quote(
        token="USDC",
        amount=Decimal("1000"),
        from_chain="arbitrum",
        to_chain="optimism",
    )

    # Build deposit transaction
    tx = adapter.build_deposit_tx(quote, recipient="0x...")
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .adapter import (
        ACROSS_CHAIN_IDS,
        ACROSS_SPOKE_POOL_ADDRESSES,
        ACROSS_SUPPORTED_TOKENS,
        AcrossBridgeAdapter,
        AcrossConfig,
        AcrossError,
        AcrossQuoteError,
        AcrossStatusError,
        AcrossTransactionError,
    )
    from .receipt_parser import AcrossReceiptParser

__all__ = [
    "ACROSS_CHAIN_IDS",
    "ACROSS_SPOKE_POOL_ADDRESSES",
    "ACROSS_SUPPORTED_TOKENS",
    "AcrossBridgeAdapter",
    "AcrossConfig",
    "AcrossError",
    "AcrossQuoteError",
    "AcrossReceiptParser",
    "AcrossStatusError",
    "AcrossTransactionError",
]

_LAZY: dict[str, tuple[str, str]] = {
    "ACROSS_CHAIN_IDS": (".adapter", "ACROSS_CHAIN_IDS"),
    "ACROSS_SPOKE_POOL_ADDRESSES": (".adapter", "ACROSS_SPOKE_POOL_ADDRESSES"),
    "ACROSS_SUPPORTED_TOKENS": (".adapter", "ACROSS_SUPPORTED_TOKENS"),
    "AcrossBridgeAdapter": (".adapter", "AcrossBridgeAdapter"),
    "AcrossConfig": (".adapter", "AcrossConfig"),
    "AcrossError": (".adapter", "AcrossError"),
    "AcrossQuoteError": (".adapter", "AcrossQuoteError"),
    "AcrossReceiptParser": (".receipt_parser", "AcrossReceiptParser"),
    "AcrossStatusError": (".adapter", "AcrossStatusError"),
    "AcrossTransactionError": (".adapter", "AcrossTransactionError"),
}

_registered = False


def _register_once() -> None:
    """Fire ``register_connector`` once on first strategy-side access.

    Deferred so importing the connector's gateway-side surface during
    gateway boot does not pull ``framework.intents.vocabulary`` into the
    partially-initialised config-init chain (VIB-4835).
    """
    global _registered
    if _registered:
        return
    _registered = True
    try:
        from almanak.connectors._strategy_base.registry import register_connector
        from almanak.framework.intents.vocabulary import IntentType

        # Across is bridge-only. Strategy ``chains`` historically listed
        # five core EVM chains; the matrix has rendered Linea as well
        # since the SpokePool went live there. Include Linea here so the
        # derived matrix row matches the CLI baseline. (Zksync is in
        # ``ACROSS_CHAIN_IDS`` but is intentionally excluded from
        # ``KNOWN_VENUES``.)
        register_connector(
            name="across",
            intents=(IntentType.BRIDGE,),
            chains=("ethereum", "arbitrum", "base", "optimism", "polygon", "linea"),
        )
    except Exception:
        _registered = False
        raise


def __getattr__(name: str) -> Any:
    """PEP 562 lazy attribute access."""
    if name not in _LAZY:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY[name]
    import importlib

    module = importlib.import_module(submodule, package=__name__)
    value = getattr(module, attr)
    globals()[name] = value
    _register_once()
    return value
