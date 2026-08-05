"""Address→symbol resolution seam for warm/join boundaries (token-identity PR).

The SDK's pricing, warming, and accounting joins are keyed by canonical token
SYMBOL, while (post symbol-deprecation) strategies reference tokens by
chain-specific contract ADDRESS. Every boundary where an address-form
reference enters a symbol-keyed lane needs the same cheap, offline
resolution: address → canonical upper-cased symbol.

This module is that single seam, shared by the runner price pre-fetch, the
snapshot oracle-dict export, the compiler price fallback, the teardown
warmup/prefetch lanes, and the swap-back clamp join. Two ad-hoc precedents
existed before it — ``_run_loop_helpers._augment_intent_tokens_with_address_resolution``
(VIB-4318 gemini fix) and ``swap_handler._resolve_price_lookup_key``
(VIB-4304) — this seam is the same pattern, centralized.

Not to be confused with :mod:`.identity`'s ``canonicalize_token_identity``,
which canonicalizes to ``(chain, address)`` for read-side inventory grouping
and is scoped away from writers. This seam resolves the OTHER direction
(address → symbol) so symbol-keyed lanes can join; it never touches
persisted payload shapes.

Design rules:

- **Offline only** (``skip_gateway=True``): these helpers run on hot paths
  (per-intent pre-fetch, oracle-dict export, clamp joins) where a gateway
  round-trip is unacceptable. A token the static registry / caches cannot
  name resolves to ``None`` and the caller keeps its existing
  fail-closed / skip behaviour — Empty ≠ Zero, never guess a symbol.
- **Additive at call sites**: callers only ever *add* a resolved symbol next
  to their existing lookups; symbol-form inputs pass through untouched, so
  symbol-based behaviour stays byte-identical.
- **Case-insensitive address detection**: checksummed (``0xAbC…``) and
  uppercased (``0XABC…`` — the Instrument-canonicalization form) addresses
  are addresses. The lower-cased form is what reaches the resolver.
"""

from __future__ import annotations

import re
from functools import lru_cache

from almanak.core.chains._helpers import is_solana_chain

# Case-insensitive EVM address. The deprecation/resolver regexes are
# lowercase-``0x``-only by contract; this seam accepts any casing and
# normalizes before resolution.
_EVM_ADDRESS_ANY_CASE = re.compile(r"^0x[a-fA-F0-9]{40}$", re.IGNORECASE)
_SOLANA_MINT = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")

# Bounded: one entry per distinct (token, chain) pair seen this process.
_CACHE_SIZE = 4096


def looks_like_evm_address(value: str | None) -> bool:
    """True for a 0x-prefixed 40-hex-char EVM address in ANY casing."""
    return isinstance(value, str) and bool(_EVM_ADDRESS_ANY_CASE.match(value.strip()))


def looks_like_address(value: str | None, chain: str | None = None) -> bool:
    """True when ``value`` is address-shaped for ``chain``.

    EVM addresses match on every chain (any casing). Base58 mints match only
    when ``chain`` is a Solana-family chain — length alone is too weak a
    signal elsewhere. Also accepts the snapshot-native ``chain:0xaddr``
    composite key form.
    """
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    if not stripped:
        return False
    _prefix, sep, rest = stripped.partition(":")
    if sep and looks_like_evm_address(rest):
        return True
    if looks_like_evm_address(stripped):
        return True
    return is_solana_chain((chain or "").lower()) and bool(_SOLANA_MINT.match(stripped))


@lru_cache(maxsize=_CACHE_SIZE)
def _resolve_cached(lookup: str, chain: str) -> str | None:
    from almanak.framework.data.tokens import get_token_resolver

    try:
        info = get_token_resolver().resolve(lookup, chain=chain, log_errors=False, skip_gateway=True)
    except Exception:  # noqa: BLE001 — best-effort seam; caller keeps fail-closed behaviour
        return None
    symbol = getattr(info, "symbol", None)
    if not symbol:
        return None
    return str(symbol).upper()


def resolve_token_symbol(value: str | None, chain: str | None) -> str | None:
    """Resolve an address-shaped token reference to its canonical UPPER symbol.

    Returns ``None`` when ``value`` is not address-shaped, ``chain`` is
    missing (cross-chain disambiguation requires one), or the offline
    resolver cannot name it. Symbol-shaped inputs return ``None`` on
    purpose — callers already handle symbols; this seam only bridges
    addresses.
    """
    if not isinstance(value, str):
        return None
    stripped = value.strip()
    chain_lower = (chain or "").lower().strip()

    # ``chain:0xaddr`` composite (snapshot-native cache-key form): the
    # embedded chain wins over the caller-supplied one.
    prefix, sep, rest = stripped.partition(":")
    if sep and looks_like_evm_address(rest):
        embedded_chain = prefix.lower().strip()
        if embedded_chain:
            chain_lower = embedded_chain
        stripped = rest

    if not chain_lower:
        return None
    if looks_like_evm_address(stripped):
        return _resolve_cached(stripped.lower(), chain_lower)
    if is_solana_chain(chain_lower) and _SOLANA_MINT.match(stripped):
        # Base58 is case-sensitive — resolve verbatim.
        return _resolve_cached(stripped, chain_lower)
    return None


__all__ = [
    "looks_like_address",
    "looks_like_evm_address",
    "resolve_token_symbol",
]
