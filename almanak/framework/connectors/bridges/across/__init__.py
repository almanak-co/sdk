"""Across Bridge Adapter.

Across is a fast, secure, and capital-efficient cross-chain bridge that uses
an optimistic verification model with UMA's oracle for dispute resolution.

Features:
- Fast finality (~1-4 minutes for most routes)
- Low fees using relayer competition
- Supports ETH, USDC, WBTC and other major tokens
- Available on Ethereum, Arbitrum, Optimism, Base, Polygon, and more

Example:
    from almanak.framework.connectors.bridges.across import AcrossBridgeAdapter, AcrossConfig

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

__all__ = [
    "AcrossBridgeAdapter",
    "AcrossConfig",
    "AcrossError",
    "AcrossQuoteError",
    "AcrossTransactionError",
    "AcrossStatusError",
    "ACROSS_CHAIN_IDS",
    "ACROSS_SPOKE_POOL_ADDRESSES",
    "ACROSS_SUPPORTED_TOKENS",
]
