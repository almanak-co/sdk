"""Aster Pro perpetuals connector (off-chain order book).

Aster's live perps venue is Aster Pro: an off-chain order book reached through
the Futures V3 API. Margin is deposited into Aster's vault on BSC, orders are
EIP-712-signed API requests, and fills settle in Aster's ledger. The gateway
holds the trading identity and signs; strategies only emit intents::

    Intent.perp_open(
        market="ETH/USD",
        collateral_token="USDT",
        collateral_amount=Decimal("1.2"),
        size_usd=Decimal("6"),
        is_long=True,
        leverage=Decimal("5"),
        protocol="aster_perps",
    )
    Intent.perp_close(market="ETH/USD", collateral_token="USDT", is_long=True, protocol="aster_perps")

Margin is drawn from the Aster account's USDT balance (cross margin);
``collateral_amount`` is recorded but not transferred. Closes flatten the whole
one-way position. The legacy on-chain Aster Diamond on BSC is reduce-only and is
reachable only through ``pancakeswap_perps``.
"""
