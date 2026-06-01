"""Canonical token-identity helper for read-side inventory matching.

This module exposes a single read-side helper,
:func:`canonicalize_token_identity`, that takes either a symbol form (e.g.
``"USDC"``) or an address form (EVM hex like
``"0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"`` or Solana base58 like
``"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"``) together with a chain,
and returns a canonical ``(chain, token_address)`` tuple suitable for
inventory matching across mixed-form persisted payloads.

Why this exists (audit doc §C.3 TA-8 + §5 Wave 1 W1-4)
------------------------------------------------------
RSI / BTD strategies persist symbol-form tokens (``"USDC"``, ``"WETH"``) in
SWAP accounting rows, while the Enso aggregator path (MACD) returns and
persists canonical address-form tokens (``"0xaf88..."``, ``"0x82af..."``).
A reader grouping inventory by raw token field gets two distinct buckets
for what is actually the same on-chain token. The writer-side fix (TA-H
"Normalize token identity in SWAP payloads") is deferred to Wave 3; this
helper unblocks readers / dashboards TODAY without touching any persisted
payload, any writer, or any receipt parser.

Hard scope (audit doc §5 Wave 1 row W1-4)
-----------------------------------------
This helper is **strictly read-side**:

* It is consumed by reporting, CLI, dashboard, and inventory-reconciliation
  code paths.
* It must **never** be plumbed into an accounting writer, a receipt parser,
  or any code path that mutates a persisted payload. Doing so re-opens the
  Wave 3 design space prematurely and risks the canonical form drifting
  from whatever the writer-side fix eventually settles on.

The output shape
----------------
A 2-tuple ``(chain, token_address)``:

* ``chain`` — the resolver's normalized chain string in lowercase
  (e.g. ``"arbitrum"``, ``"ethereum"``, ``"solana"``). Chains are looked
  up through :func:`almanak.core.constants.resolve_chain_name`, so common
  aliases (``"eth"`` -> ``"ethereum"``) are accepted.
* ``token_address`` — the contract address on that chain. EVM addresses
  are lowercased hex (``"0xaf88..."``); Solana mint addresses retain their
  original base58 case. This matches the existing repo convention used by
  :func:`_normalize_address_for_chain` in
  :mod:`almanak.framework.data.tokens.resolver`.

Idempotency
-----------
Passing the helper's output back through the helper yields the same tuple:

>>> first = canonicalize_token_identity("USDC", "arbitrum")
>>> first == canonicalize_token_identity(first[1], first[0])
True

Failure modes
-------------
This helper never silently defaults. Unknown symbols raise
:class:`TokenNotFoundError`; malformed or cross-family addresses raise
:class:`InvalidTokenAddressError`. Both are subclasses of
:class:`TokenResolutionError`, so callers that already handle the existing
resolver exception hierarchy need no new branches.
"""

from __future__ import annotations

from almanak.core.enums import Chain
from almanak.framework.data.tokens.exceptions import (
    InvalidTokenAddressError,
    TokenResolutionError,
)
from almanak.framework.data.tokens.resolver import (
    SOLANA_ADDRESS_PATTERN,
    _is_solana_chain,
    _looks_like_address,
    _normalize_address_for_chain,
    _normalize_chain,
    _validate_address,
    get_token_resolver,
)


def _reject_cross_family_address(token: str, chain_lower: str) -> None:
    """Reject inputs whose address shape doesn't match the chain family.

    The TokenResolver already rejects EVM-hex on Solana via
    :func:`_validate_address`, but a Solana-base58 string on an EVM chain
    falls through the resolver's symbol path and surfaces as a generic
    :class:`TokenNotFoundError`. For a read-side identity helper that's
    misleading: the input is *trying* to be an address, just for the wrong
    family. Raise :class:`InvalidTokenAddressError` so callers can
    distinguish "unknown symbol" from "wrong-family address" cleanly.

    This guard is intentionally conservative: it only fires on inputs that
    unambiguously match the foreign family's address pattern. Short
    symbol strings that happen to share the base58 alphabet (e.g. ``"USDC"``)
    are not affected because :data:`SOLANA_ADDRESS_PATTERN` requires
    32-44 characters.
    """
    if _is_solana_chain(chain_lower):
        # The resolver already handles the EVM-on-Solana case; nothing extra
        # to do here. The validation path inside ``resolve()`` will raise
        # InvalidTokenAddressError for ``0x...`` shapes.
        return

    # EVM chain: reject a clearly Solana-shaped base58 address up front
    # so the error type matches the user's likely intent.
    if not token.startswith("0x") and SOLANA_ADDRESS_PATTERN.match(token):
        raise InvalidTokenAddressError(
            token=token,
            chain=chain_lower,
            reason=(
                "Address looks like a Solana base58 mint, but chain "
                f"'{chain_lower}' is an EVM chain. Did you mean chain='solana'?"
            ),
        )


def canonicalize_token_identity(token: str, chain: str | Chain) -> tuple[str, str]:
    """Canonicalize a token identifier for read-side inventory matching.

    Accepts either a symbol form (``"USDC"``, ``"WETH"``, ``"USDC.e"``) or an
    address form (EVM hex or Solana base58) and returns a canonical
    ``(chain, token_address)`` tuple suitable for grouping mixed-form
    persisted payloads.

    The chain in the returned tuple is the resolver's lowercased chain
    string. The address is lowercased hex on EVM chains and original-case
    base58 on Solana — matching the existing repo convention from
    :func:`_normalize_address_for_chain`.

    Args:
        token: Token identifier — symbol (case-insensitive) OR address
            (EVM hex, any case; or Solana base58, case-sensitive).
        chain: Chain name as a string or :class:`Chain` enum value. String
            aliases handled by :func:`resolve_chain_name` are accepted
            (e.g. ``"eth"`` -> ``"ethereum"``).

    Returns:
        ``(chain_lower, canonical_address)`` — both strings, both safe to
        use as dict keys for inventory matching.

    Raises:
        TokenNotFoundError: Symbol could not be resolved on the given chain.
        InvalidTokenAddressError: Input looks like an address but is
            malformed for the chain's family (e.g. base58 on EVM, or hex
            on Solana).
        TokenResolutionError: Other resolution errors (unknown chain,
            ambiguous symbol, etc.) — see the resolver exception hierarchy.

    Examples:
        Symbol and address resolve to the same tuple::

            >>> canonicalize_token_identity("USDC", "arbitrum")
            ('arbitrum', '0xaf88d065e77c8cc2239327c5edb3a432268e5831')
            >>> canonicalize_token_identity(
            ...     "0xaf88d065e77c8cC2239327C5EDb3A432268e5831", "arbitrum"
            ... )
            ('arbitrum', '0xaf88d065e77c8cc2239327c5edb3a432268e5831')

        Same symbol on different chains yields different tuples::

            >>> canonicalize_token_identity("USDC", "arbitrum") != \\
            ...     canonicalize_token_identity("USDC", "ethereum")
            True

        Solana preserves case::

            >>> canonicalize_token_identity("USDC", "solana")
            ('solana', 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v')
    """
    if not isinstance(token, str) or not token.strip():
        raise TokenResolutionError(
            token=token if isinstance(token, str) else "",
            chain=str(chain),
            reason="Token identifier must be a non-empty string",
        )

    # Normalize the chain first so the rest of the helper consistently
    # operates on lowercased chain names. ``_normalize_chain`` raises
    # ``TokenResolutionError`` for unknown chains.
    chain_lower, _chain_enum = _normalize_chain(chain)

    stripped = token.strip()

    # If the input is shaped like an address, validate it eagerly against
    # the chain's family. This catches malformed addresses and cross-family
    # mistakes before they reach the resolver's symbol path (which would
    # otherwise surface as a confusing TokenNotFoundError).
    #
    # ``_looks_like_address`` only matches EVM ``0x...`` hex shapes. On
    # Solana, a 32-44 char input is almost always meant as a mint address
    # (token symbols are short — JUP, USDC, BONK). Treating such inputs as
    # address-shaped lets ``_validate_address`` raise
    # ``InvalidTokenAddressError`` on malformed base58 (e.g. characters
    # ``0`` / ``O`` / ``I`` / ``l``) instead of falling through to a
    # confusing ``TokenNotFoundError``.
    is_solana = _is_solana_chain(chain_lower)
    if _looks_like_address(stripped) or (is_solana and 32 <= len(stripped) <= 44):
        _validate_address(stripped, chain_lower)
    _reject_cross_family_address(stripped, chain_lower)

    # Delegate to the singleton resolver. It handles symbol normalization,
    # bridged-token aliases (USDC.e), case-insensitive symbol matching,
    # address-form lookups, and on-chain gateway discovery when available.
    # ``skip_gateway=True`` keeps this helper static-registry / cache only:
    # read-side reconciliation must not block on a 30s gateway round-trip,
    # and we don't want to widen the strategy-container egress surface from
    # what callers already accept.
    resolver = get_token_resolver()
    resolved = resolver.resolve(stripped, chain_lower, skip_gateway=True)

    canonical_address = _normalize_address_for_chain(resolved.address, chain_lower)
    return (chain_lower, canonical_address)


__all__ = ["canonicalize_token_identity"]
