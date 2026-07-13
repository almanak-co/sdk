"""Shared token-decimals resolution for dashboard display helpers.

Dashboard adapters and the custom API client need per-token decimals to scale
raw on-chain amounts and to convert Uniswap ticks into human prices. The
**authoritative** source is the SDK's canonical token registry
(``almanak.framework.data.tokens``), which knows per-chain decimals for every
token the SDK can trade. Resolve through it first.

``TOKEN_DECIMALS`` is a tiny, chain-agnostic **last-resort** map for the
handful of majors/stables that a symbol-only display path may hit when the
registry is unavailable (e.g. a gateway-less unit test). It is intentionally
NOT the primary lookup: a static map silently mis-renders any token it does not
list (VIB-5738 — a 6-dec stable like USDG rendered with the 18-dec default made
LP prices off by 10**12, collapsing the liquidity-distribution price axis to
"0.00"). Do not grow it with new symbols — extend the registry instead.
"""

from __future__ import annotations

# Last-resort, chain-agnostic fallback. Majors + the most common stables only.
# NOT the primary source — see the module docstring. Do not add symbols here;
# the canonical registry is the single source of truth for decimals.
TOKEN_DECIMALS: dict[str, int] = {
    "ETH": 18,
    "WETH": 18,
    "WBTC": 8,
    "BTC": 8,
    "USDC": 6,
    "USDT": 6,
    "DAI": 18,
}


def _registry_decimals(symbol: str, chain: str) -> int | None:
    """Per-chain decimals from the canonical token registry, or ``None``.

    Never raises — a resolver miss / gateway-less context / unknown token all
    collapse to ``None`` so the caller falls back rather than mis-scaling.
    """
    try:
        from almanak.framework.data.tokens.resolver import get_token_resolver

        return get_token_resolver().get_decimals(chain, symbol)
    except Exception:
        return None


def resolve_token_decimals(symbol: str | None, chain: str | None = None, *, default: int | None = None) -> int | None:
    """Resolve a token's decimals, registry-first, for dashboard display.

    Resolution order:

    1. **Canonical registry** (authoritative, per-chain) — used when ``chain``
       is provided. This is the correct source and covers every SDK token.
    2. **Static fallback map** (``TOKEN_DECIMALS``) — a chain-agnostic
       last-resort for common majors/stables when the registry can't answer
       (no chain, or a gateway-less test context).
    3. ``default`` — returned only when the token is genuinely unresolvable.
       Pass ``None`` (the default) to let the caller decide how to render an
       unknown token (typically: skip / render "—" rather than mis-scale);
       pass a concrete int (e.g. ``18``) only where a number is structurally
       required and a conservative guess is acceptable.
    """
    if symbol:
        sym = symbol.strip()
        if sym:
            if chain:
                decimals = _registry_decimals(sym, chain)
                if decimals is not None:
                    return decimals
            static = TOKEN_DECIMALS.get(sym.upper())
            if static is not None:
                return static
    return default


def token_decimals(symbol: str, default: int = 18) -> int:
    """Return decimals for a symbol with a safe fallback (chain-agnostic).

    Back-compat shim for callers that have no chain context. Prefer
    :func:`resolve_token_decimals` with a ``chain`` wherever one is available —
    the static map only knows a handful of majors/stables.
    """
    # resolve_token_decimals already returns ``default`` on a miss; ``or default``
    # would additionally (and wrongly) coerce a legitimately-resolved 0-decimals
    # token to the default, so return the resolved value directly.
    resolved = resolve_token_decimals(symbol, default=default)
    return resolved if resolved is not None else default


__all__ = ["TOKEN_DECIMALS", "resolve_token_decimals", "token_decimals"]
