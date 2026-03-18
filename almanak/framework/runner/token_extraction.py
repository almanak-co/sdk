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


def _is_symbol(value: Any) -> TypeGuard[str]:
    """Return True if *value* looks like a token symbol (not an address)."""
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    return bool(stripped) and len(stripped) < MAX_SYMBOL_LENGTH and not stripped.lower().startswith("0x")


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
        if _is_symbol(val):
            symbols.append(val.strip())

    # Parse pool name (e.g., "WETH/USDC") for LP intents
    pool = _get("pool")
    if isinstance(pool, str) and "/" in pool:
        for part in pool.split("/"):
            # Strip whitespace and common pool decorations (e.g., "USDC (0.05%)")
            part = part.strip().split("(")[0].split(" ")[0].strip()
            if _is_symbol(part):
                symbols.append(part)

    # Recurse into callback_intents (FlashLoanIntent)
    callbacks = _get("callback_intents")
    if callbacks and isinstance(callbacks, list):
        for cb in callbacks:
            symbols.extend(extract_token_symbols(cb, _depth=_depth + 1))

    # Deduplicate preserving order
    return list(dict.fromkeys(symbols))
