"""Shared token extraction for intent price pre-fetching.

Both StrategyRunner (local execution) and IntentExecutionService (gateway
execution) need to extract token symbols from intents before compilation so
that real prices can be supplied to the compiler.  This module provides a
single canonical implementation to prevent the two paths from drifting.
"""

from __future__ import annotations

from typing import Any, TypeGuard

# All intent fields that may contain a token symbol.
TOKEN_FIELDS: tuple[str, ...] = (
    "from_token",
    "to_token",
    "token_in",
    "token_out",
    "token",
    "token_a",
    "token_b",
    "borrow_token",
    "collateral_token",
)

MAX_SYMBOL_LENGTH = 20
MAX_CALLBACK_DEPTH = 3

# Pool-type suffixes used by AMMs (e.g., Aerodrome "WETH/USDC/volatile").
# These are pool metadata, not token symbols, and must be stripped during extraction.
_POOL_TYPE_SUFFIXES: frozenset[str] = frozenset(
    {
        "volatile",
        "stable",
        "concentrated",
        "cl",  # Aerodrome Slipstream concentrated liquidity
    }
)

# Pure fiat quote symbols that appear in market descriptors (e.g., "BTC/USD")
# but have no on-chain ERC20 representation. Querying balance/price for these
# always fails (no gateway resolution, no Chainlink `USD/USD` feed). Real
# dollar-pegged stablecoins (USDC, USDT, DAI, USDS, FRAX, USDE, ...) are NOT in
# this set — they resolve to actual on-chain tokens.
FIAT_QUOTE_SYMBOLS: frozenset[str] = frozenset({"USD", "EUR", "GBP", "JPY"})


def is_fiat_quote_symbol(symbol: str) -> bool:
    """Return True if *symbol* is a fiat quote denomination (not a token)."""
    return isinstance(symbol, str) and symbol.strip().upper() in FIAT_QUOTE_SYMBOLS


def _is_symbol(value: Any) -> TypeGuard[str]:
    """Return True if *value* looks like a token symbol (not an address)."""
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return bool(stripped) and len(stripped) < MAX_SYMBOL_LENGTH and not stripped.lower().startswith("0x")


def parse_pool_tokens(pool: str) -> list[str]:
    """Extract token symbols from a slash-separated pool descriptor.

    Handles the pool-string format used across DEX intents:

        "WETH/USDC/500"          -> ["WETH", "USDC"]     (Uniswap V3 fee tier)
        "WETH/USDC/volatile"     -> ["WETH", "USDC"]     (Solidly/Aerodrome pool type)
        "WETH/USDC"              -> ["WETH", "USDC"]     (two-token)
        "USDC (0.05%)/WETH/500"  -> ["USDC", "WETH"]     (decorations stripped)
        "VOLATILE/WETH/500"      -> ["VOLATILE", "WETH"] (suffix filter is trailing-only)

    Filters:
    - Numeric segments (fee tiers, bin steps).
    - Trailing pool-type suffixes: volatile, stable, concentrated, cl.
      Filter applies only at the last position — a token that happens to be
      named "VOLATILE" in positions 0/1 is preserved.
    - Fiat quote symbols (USD/EUR/GBP/JPY): market descriptors like
      "BTC/USD" name the quote denomination, not an on-chain token.
    - Segments that don't look like token symbols (addresses, empty, overlong).

    Preserves first-seen order; does NOT deduplicate (caller's concern).

    Args:
        pool: Pool descriptor string.

    Returns:
        List of token symbols in order of appearance. Empty list if *pool*
        is not a string, has no "/" separator, or yields no valid symbols.
    """
    if not isinstance(pool, str) or "/" not in pool:
        return []

    parts = [p for p in pool.split("/") if p.strip()]
    last_idx = len(parts) - 1
    tokens: list[str] = []

    for idx, raw in enumerate(parts):
        # Strip whitespace and common pool decorations (e.g., "USDC (0.05%)").
        part = raw.strip().split("(")[0].split(" ")[0].strip()
        # Skip numeric parts (fee tiers like "500", "3000", bin steps like "20").
        if part.isdigit():
            continue
        # Skip pool-type suffixes only in trailing position.
        if idx == last_idx and part.lower() in _POOL_TYPE_SUFFIXES:
            continue
        # Skip fiat quote denominations (e.g., "BTC/USD" -> ["BTC"]).
        if is_fiat_quote_symbol(part):
            continue
        if _is_symbol(part):
            tokens.append(part)

    return tokens


def extract_token_symbols(intent: Any, *, _depth: int = 0) -> list[str]:
    """Extract token symbols from an intent for price pre-fetching.

    Works with both Intent objects (attribute access) and plain dicts
    (key access), and recurses into ``callback_intents`` for FlashLoanIntent
    with a depth guard to prevent infinite loops.

    Returns a deduplicated list of token symbols preserving first-seen order.
    """
    if _depth > MAX_CALLBACK_DEPTH:
        return []

    symbols: list[str] = []

    # Attribute access (Intent objects) or key access (dicts)
    _get = intent.get if isinstance(intent, dict) else lambda k, d=None: getattr(intent, k, d)

    for field in TOKEN_FIELDS:
        val = _get(field)
        if _is_symbol(val) and not is_fiat_quote_symbol(val):
            symbols.append(val.strip())

    # Parse pool name (e.g., "WETH/USDC/500") for LP intents
    pool = _get("pool")
    if isinstance(pool, str):
        symbols.extend(parse_pool_tokens(pool))

    # Recurse into callback_intents (FlashLoanIntent)
    callbacks = _get("callback_intents")
    if callbacks and isinstance(callbacks, list):
        for cb in callbacks:
            symbols.extend(extract_token_symbols(cb, _depth=_depth + 1))

    # Deduplicate preserving order
    return list(dict.fromkeys(symbols))
