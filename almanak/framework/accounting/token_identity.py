"""Read-side token identity canonicalization helper (W1-4 / VIB-4779).

This module provides a best-effort helper that normalizes heterogeneous token
identifiers found in persisted accounting payloads into a canonical form
suitable for inventory matching and FIFO lot-matching.

Problem context (TA-8):
    Different connectors write different token forms to accounting events:
    - Uniswap V3 direct writer stores symbols: ``token_in="USDC"``
    - Enso aggregator writer stores addresses: ``token_in="0xaf88d065e77c8cC2239327C5EDb3A432268e5831"``

    When code iterates SWAP events to compute inventory or FIFO-match, the two
    are different keys which causes fragile matching.

    This helper normalizes both forms to the same ``CanonicalToken`` so callers
    can bucket either form under a single key.

Hard constraint (Wave 1):
    **Read-side only.** No changes to writers, no changes to persisted payloads.
    Persisted payload normalization is deferred to Wave 3 / W3-4.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

# EVM address pattern: 0x + 40 hex chars.
_EVM_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
# Solana base58 address pattern (32–44 chars; excludes 0, O, I, l).
_SOLANA_ADDRESS_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")
# Chain-prefix pattern: "<chain>:<identifier>" e.g. "arbitrum:0xaf88..."
_CHAIN_PREFIX_RE = re.compile(r"^([a-zA-Z0-9_-]+):(.+)$")


@dataclass(frozen=True)
class CanonicalToken:
    """Canonical token identity for inventory matching.

    Two equal ``CanonicalToken`` instances (by equality) represent the same
    on-chain token and are safe to use as bucket keys for inventory matching.

    Attributes:
        chain:   Lowercased chain identifier (e.g. ``"arbitrum"``, ``"ethereum"``).
        address: Lowercased 0x-prefixed address for EVM chains; base58 for Solana;
                 empty string when only a symbol was available and resolution failed.
        symbol:  Uppercase symbol when known (e.g. ``"USDC"``); empty string otherwise.
    """

    chain: str
    address: str
    symbol: str


def _is_evm_address(token: str) -> bool:
    return bool(_EVM_ADDRESS_RE.match(token))


def _is_solana_address(token: str, chain: str) -> bool:
    if chain.lower() == "solana":
        return bool(_SOLANA_ADDRESS_RE.match(token))
    return False


def canonicalize_token_for_read(
    token: str,
    chain: str,
    *,
    fallback_to_input: bool = True,
) -> CanonicalToken | None:
    """Best-effort canonicalize a token identity from accounting payloads.

    Accepts either symbol form (``'USDC'``, ``'WETH'``) or address form
    (``'0xaf88...'``, ``'0x82af...'``), or a ``'chain:addr'`` prefixed string.
    Returns a :class:`CanonicalToken` with normalized chain/address/symbol.

    Returns ``None`` when input is empty / chain is empty AND
    ``fallback_to_input`` is ``False``.  When ``fallback_to_input`` is ``True``
    (default) and the resolver cannot identify the token, returns a
    ``CanonicalToken`` with whatever can be salvaged:

    - If input looked like an address:
      ``CanonicalToken(chain=lower(chain), address=lower(input), symbol="")``
    - If input looked like a symbol:
      ``CanonicalToken(chain=lower(chain), address="", symbol=upper(input))``

    Two equal ``CanonicalToken`` values (by ``==``) are interchangeable for
    inventory matching.

    Args:
        token:             Token identifier — symbol, EVM address, Solana address,
                           or ``"<chain>:<identifier>"`` prefixed string.
        chain:             Chain name (case-insensitive).  May be empty when using
                           the ``"<chain>:<identifier>"`` prefix form.
        fallback_to_input: When ``True`` (default), always return a
                           ``CanonicalToken``; only the address/symbol fields may
                           be incomplete.  When ``False``, return ``None`` on any
                           resolution failure.

    Returns:
        :class:`CanonicalToken` or ``None``.

    Examples:
        >>> from almanak.framework.accounting.token_identity import canonicalize_token_for_read
        >>> r1 = canonicalize_token_for_read("USDC", "arbitrum")
        >>> r2 = canonicalize_token_for_read("0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum")
        >>> assert r1 == r2  # W1-4 key invariant
    """
    # ── 1. Reject obviously-empty inputs ──────────────────────────────────────
    if not token or not token.strip():
        return None

    token = token.strip()

    # ── 2. Strip "chain:identifier" prefix ────────────────────────────────────
    prefix_match = _CHAIN_PREFIX_RE.match(token)
    if prefix_match:
        prefix_chain = prefix_match.group(1)
        identifier = prefix_match.group(2)
        # Honor the explicit prefix chain over the passed chain argument
        # (caller may pass empty string when using prefix form).
        effective_chain = prefix_chain
        token = identifier
    else:
        effective_chain = chain

    # ── 3. Require a chain at this point ──────────────────────────────────────
    if not effective_chain or not effective_chain.strip():
        if fallback_to_input:
            # We have a token string but no chain — minimal salvage.
            if _is_evm_address(token):
                return CanonicalToken(chain="", address=token.lower(), symbol="")
            # Treat Solana-looking base58 tokens as address-form too — without
            # this branch a Solana mint address would be uppercased into the
            # symbol slot and lost.
            if _SOLANA_ADDRESS_RE.match(token):
                return CanonicalToken(chain="", address=token, symbol="")
            return CanonicalToken(chain="", address="", symbol=token.upper())
        return None

    chain_lower = effective_chain.lower()

    # ── 4. Determine input form ────────────────────────────────────────────────
    is_solana = chain_lower == "solana"
    is_evm_address = _is_evm_address(token)
    is_sol_address = _is_solana_address(token, chain_lower)
    is_address = is_evm_address or is_sol_address

    # ── 5. Attempt resolver lookup ────────────────────────────────────────────
    try:
        from almanak.framework.data.tokens import get_token_resolver

        resolved = get_token_resolver().resolve(token, chain_lower, log_errors=False, skip_gateway=True)
        # ResolvedToken has .symbol (str), .address (str), .chain (Chain enum).
        # The resolver already normalizes: EVM addresses are lowercased, Solana
        # base58 addresses are preserved as-is (per _normalize_address_for_chain).
        return CanonicalToken(
            chain=chain_lower,
            address=resolved.address,
            symbol=resolved.symbol.upper() if resolved.symbol else "",
        )
    except Exception:
        # Resolver unavailable, not found, or raised — fall through to fallback.
        pass

    # ── 6. Fallback ───────────────────────────────────────────────────────────
    if not fallback_to_input:
        return None

    if is_address:
        # Preserve Solana addresses case-sensitively; lowercase EVM.
        normalized_addr = token if is_solana else token.lower()
        return CanonicalToken(chain=chain_lower, address=normalized_addr, symbol="")

    # Symbol form: upper-case, no address.
    return CanonicalToken(chain=chain_lower, address="", symbol=token.upper())
