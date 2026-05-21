"""Shared symbol-keyed token decimals fallback for dashboard helpers.

Dashboard adapters and the custom API client both need a small symbol-keyed
fallback when proto rows expose only a token symbol (not an address).
Address-keyed canonical lookups still flow through
``almanak.framework.data.tokens.get_token_resolver``; this mapping is the
last-resort fallback for symbol-only display paths.
"""

from __future__ import annotations

TOKEN_DECIMALS: dict[str, int] = {
    "ETH": 18,
    "WETH": 18,
    "WBTC": 8,
    "BTC": 8,
    "USDC": 6,
    "USDT": 6,
    "DAI": 18,
}


def token_decimals(symbol: str, default: int = 18) -> int:
    """Return decimals for a symbol with a safe fallback.

    Use only in dashboard display code where addresses are unavailable.
    """
    return TOKEN_DECIMALS.get(symbol.upper(), default)


__all__ = ["TOKEN_DECIMALS", "token_decimals"]
