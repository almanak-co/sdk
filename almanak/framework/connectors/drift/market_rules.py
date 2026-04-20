"""Drift collateral rules.

Single source of truth for the valid collateral tokens on Drift across all
Drift perp markets.

Why this module exists
----------------------

Unlike per-market collateral protocols (e.g. GMX V2, where each market is
bound to a specific ``longToken``/``shortToken`` pair), Drift is a
**cross-margin** perpetuals protocol. A user's margin is netted across every
spot position they hold in Drift's on-chain spot markets, and any of those
spot markets' assets can back any perp position. The collateral rule is
therefore a single global allow-list of supported mints — identical for
every Drift perp market.

If a user submits a ``PERP_OPEN`` intent whose ``collateral_token`` is not
one of Drift's registered spot-market mints, the Solana transaction will
fail opaquely at the Drift program (no matching spot market account, margin
calc fails). Validating the mint at intent-compile time surfaces a clean,
actionable error to the strategy author before any fee is paid.

Authoritative source
--------------------

The allow-list is derived from
:data:`almanak.framework.connectors.drift.constants.SPOT_MARKETS`, which
mirrors the ``SpotMarkets`` table in Drift's ``protocol-v2`` SDK
(https://github.com/drift-labs/protocol-v2 — ``sdk/src/constants/spotMarkets.ts``).
When Drift DAO adds a new spot market on-chain, update
``SPOT_MARKETS`` in the Drift constants module and this module automatically
picks it up.

Cross-reference: GMX V2's equivalent module is
``almanak.framework.connectors.gmx_v2.market_rules``. The two modules share
the same error type (:class:`InvalidCollateralForMarketError`) so that
strategy-level error handling can treat collateral misconfiguration
uniformly across venues.
"""

from __future__ import annotations

import logging

from almanak.framework.intents.intent_errors import InvalidCollateralForMarketError

from .constants import SPOT_MARKETS

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Allow-list
# -----------------------------------------------------------------------------
#
# Frozen set of canonical (case-normalised) Drift spot-market symbols that may
# be used as collateral for any Drift perp position. Comparison is always done
# against the uppercased form so that ``"usdc"``, ``"USDC"``, ``"Usdc"``,
# and ``"msol"``/``"MSOL"`` all resolve identically regardless of how Drift
# originally cased the symbol in its SDK table.
ALLOWED_COLLATERAL_MINTS: frozenset[str] = frozenset(s.upper() for s in SPOT_MARKETS.values())


# Base58 alphabet used by Solana pubkeys — the Bitcoin base58 alphabet.
# ``0``, ``O``, ``I``, ``l`` are intentionally excluded to avoid visual
# ambiguity.
_BASE58_ALPHABET: frozenset[str] = frozenset("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

# Hex alphabet for EVM-style 0x-prefixed addresses. Lower+upper so we accept
# both ``0xAbCd...`` and ``0xabcd...`` variants.
_HEX_ALPHABET: frozenset[str] = frozenset("0123456789abcdefABCDEF")


def _looks_like_base58(token: str) -> bool:
    """Return True if every character in ``token`` is a base58 alphabet char.

    Used to distinguish a raw Solana pubkey from a human-readable non-symbol
    string that only happens to fall in the base58 length range.
    """
    return all(ch in _BASE58_ALPHABET for ch in token)


def _looks_like_evm_address(token: str) -> bool:
    """Return True if ``token`` is a well-formed 0x-prefixed 42-char EVM address.

    Requires an exact length of 42 and that every character after the ``0x``
    prefix is a hex digit. Malformed inputs like ``"0xfoo"`` or ``"0x"`` return
    False so they fall through to symbol-based validation and get rejected
    with a clean error instead of silently skipping the gate.
    """
    if len(token) != 42:
        return False
    if token[:2].lower() != "0x":
        return False
    return all(ch in _HEX_ALPHABET for ch in token[2:])


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------


def is_supported_collateral(collateral_token: str) -> bool:
    """Return ``True`` if ``collateral_token`` is a Drift spot-market symbol.

    The check is case-insensitive. Raw 0x/base58 addresses are NOT recognised
    by this predicate — address-based collaterals are considered
    non-validatable here and should be treated as permissive by callers (the
    compiler's address-resolution path handles that).

    Args:
        collateral_token: Collateral token symbol (e.g. ``"USDC"``,
            ``"mSOL"``). Case-insensitive.

    Returns:
        ``True`` if the symbol is a registered Drift spot-market collateral,
        ``False`` otherwise.
    """
    return collateral_token.strip().upper() in ALLOWED_COLLATERAL_MINTS


def validate_drift_collateral(collateral_token: str) -> None:
    """Validate that ``collateral_token`` is a legal Drift collateral symbol.

    Compile-path validation. This is the entry point called by the intent
    compiler's PERP_OPEN path for Drift intents. It must be invoked BEFORE
    the Drift adapter is instantiated so that invalid configurations fail
    before any transaction is built.

    Behaviour:
      * Empty / whitespace-only strings raise
        :class:`InvalidCollateralForMarketError`.
      * 0x-prefixed or base58-looking raw addresses (anything that does not
        look like a short ticker symbol) are skipped with a debug log — the
        compiler's downstream resolution path handles address collateral.
      * A recognised symbol returns normally.
      * An unrecognised symbol raises :class:`InvalidCollateralForMarketError`
        whose ``allowed_collaterals`` lists every Drift spot-market symbol.

    Args:
        collateral_token: Collateral token symbol (e.g. ``"USDC"``,
            ``"SOL"``, ``"mSOL"``). Case-insensitive.

    Raises:
        InvalidCollateralForMarketError: When the supplied symbol is not
            one of Drift's registered spot-market collaterals.
    """
    if collateral_token is None or not collateral_token.strip():
        raise InvalidCollateralForMarketError(
            market="*",  # cross-margin: rule is global, not per-market
            collateral=str(collateral_token),
            allowed_collaterals=sorted(ALLOWED_COLLATERAL_MINTS),
            chain="solana",
            protocol="drift",
        )

    token = collateral_token.strip()

    # Skip address-shaped inputs — let the adapter's address resolution path
    # handle raw mints/addresses. Two recognised address shapes:
    #   * EVM-style: exactly 42 chars, ``0x`` / ``0X`` sentinel, remainder hex.
    #     Malformed ``0x``-prefixed garbage (e.g. ``"0xfoo"``, ``"0x"``) does
    #     NOT match and falls through to symbol-based validation — otherwise
    #     the gate could be trivially bypassed.
    #   * Solana-style: 32-44 characters, base58 alphabet only (no hyphen,
    #     underscore, or other separator). This lets us skip valid base58
    #     pubkeys while still rejecting human-readable non-symbol garbage
    #     like ``"WETH-on-arbitrum"`` which contains hyphens.
    if _looks_like_evm_address(token):
        logger.debug(
            "Drift collateral '%s' is an EVM-style raw address; skipping symbol-based collateral validation.",
            token,
        )
        return
    if 32 <= len(token) <= 44 and _looks_like_base58(token):
        logger.debug(
            "Drift collateral '%s' is a Solana-style raw address; skipping symbol-based collateral validation.",
            token,
        )
        return

    if token.upper() not in ALLOWED_COLLATERAL_MINTS:
        raise InvalidCollateralForMarketError(
            market="*",  # cross-margin: rule is global, not per-market
            collateral=collateral_token,
            allowed_collaterals=sorted(ALLOWED_COLLATERAL_MINTS),
            chain="solana",
            protocol="drift",
        )


__all__ = [
    "ALLOWED_COLLATERAL_MINTS",
    "InvalidCollateralForMarketError",
    "is_supported_collateral",
    "validate_drift_collateral",
]
