"""GMX V2 market collateral rules.

Single source of truth for the valid ``(market, collateral_token)`` pairs on
GMX V2 across supported chains.

Why this module exists
----------------------

GMX V2 markets have a fixed ``longToken`` and ``shortToken``. A ``PERP_OPEN``
order must supply collateral whose address equals either the market's
``longToken`` or its ``shortToken``. If the order is created with any other
token (for example, WETH collateral for a SOL/USD market on Arbitrum), the
``createOrder`` transaction still succeeds on-chain, but the GMX keeper
cancels the order and keeps the execution fee (~0.001 ETH). The user sees a
silent burn: no position, no refund, no clear error.

Validating the pair at intent-compile time eliminates this class of bug.

Authoritative source
--------------------

The authoritative rule is the on-chain ``Reader.getMarket(marketAddress)``
call, which returns ``(marketToken, indexToken, longToken, shortToken)``
directly from ``DataStore``. This module mirrors that data for the curated
set of markets the SDK ships with (see
``almanak.connectors.gmx_v2.adapter.GMX_V2_MARKETS``) so that
validation can happen locally in the compile path, without an RPC round
trip.

The market data here was cross-checked against:
  * GMX interface config: https://github.com/gmx-io/gmx-interface/blob/master/sdk/src/configs/markets.ts
  * On-chain ``SyntheticsReader.getMarket()`` on Arbitrum and Avalanche.

If a market is added to ``GMX_V2_MARKETS`` it MUST also be registered here.
See :func:`get_allowed_collaterals` for the contract.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable

from almanak.core.perp_markets import perp_market_pair_key
from almanak.framework.intents.intent_errors import InvalidCollateralForMarketError

logger = logging.getLogger(__name__)


# -----------------------------------------------------------------------------
# Rule table
# -----------------------------------------------------------------------------
#
# Shape: _GMX_V2_MARKET_COLLATERALS[chain][market] = tuple of allowed collateral
# token symbols (canonical GMX V2 symbols — i.e. the symbols that appear in
# ``GMX_V2_TOKENS`` for the same chain).
#
# The first symbol in each tuple is conventionally the market's ``longToken``
# and the second is the ``shortToken``, but callers MUST NOT rely on ordering —
# treat the tuple as an unordered set.
#
# Synthetic markets (index token not native to the chain, e.g. DOGE/USD on
# Arbitrum) use WETH as the long token on Arbitrum and WAVAX as the long token
# on Avalanche, per GMX's synthetic-pool design.
#
# Every row below is verified against on-chain ``Reader.getMarket()`` by
# ``tests/audit/test_gmx_v2_market_identity.py`` — that audit, not this comment,
# is what keeps the table true.
#
# KNOWN LIMITATION (VIB-6401): naming a symbol here does not make it usable.
# ``compiler._resolve_collateral`` resolves symbols only through
# ``GMX_V2_TOKENS``, which lacks the long token of nine of these markets
# (LINK, ARB, UNI, AAVE, GMX, SOL, WAVAX and OP on Arbitrum; SOL on Avalanche).
# Those markets currently reject their own correct long-side collateral with
# "Unknown collateral token" and can only be opened short-side.

_GMX_V2_MARKET_COLLATERALS: dict[str, dict[str, tuple[str, ...]]] = {
    "arbitrum": {
        # Native-index markets.
        "ETH/USD": ("WETH", "USDC"),
        "BTC/USD": ("WBTC", "USDC"),
        "LINK/USD": ("LINK", "USDC"),
        "ARB/USD": ("ARB", "USDC"),
        "UNI/USD": ("UNI", "USDC"),
        "AAVE/USD": ("AAVE", "USDC"),
        "GMX/USD": ("GMX", "USDC"),
        # SOL/USD on Arbitrum uses the bridged Wormhole SOL token as longToken.
        "SOL/USD": ("SOL", "USDC"),
        # Synthetic markets — no native long token on Arbitrum; pool uses WETH
        # as longToken per GMX synthetic-pool convention.
        "DOGE/USD": ("WETH", "USDC"),
        "LTC/USD": ("WETH", "USDC"),
        "XRP/USD": ("WETH", "USDC"),
        "ATOM/USD": ("WETH", "USDC"),
        "NEAR/USD": ("WETH", "USDC"),
        # AVAX/USD and OP/USD are NOT synthetic on Arbitrum — both pools hold
        # the bridged index token itself as longToken. They were listed as
        # WETH-long because the market addresses above pointed at the wrong
        # markets entirely (VIB-6155); Reader.getMarket() on the corrected
        # addresses returns (WAVAX, USDC) and (OP, USDC).
        "AVAX/USD": ("WAVAX", "USDC"),
        "OP/USD": ("OP", "USDC"),
    },
    "avalanche": {
        # Native-index markets.
        "AVAX/USD": ("WAVAX", "USDC"),
        "ETH/USD": ("WETH.e", "USDC"),
        # VIB-6155: was "WBTC.e", which is not the symbol of this market's
        # longToken NOR a key in GMX_V2_TOKENS["avalanche"] — so the one
        # correct collateral, BTC.b, was rejected at compile time.
        "BTC/USD": ("BTC.b", "USDC"),
        # SOL/USD holds bridged SOL as longToken (not synthetic); only LTC/USD
        # follows the WAVAX-long synthetic convention here (VIB-6155).
        "SOL/USD": ("SOL", "USDC"),
        "LTC/USD": ("WAVAX", "USDC"),
    },
}


# -----------------------------------------------------------------------------
# Public API
# -----------------------------------------------------------------------------


def _normalise_chain(chain: str) -> str:
    """Normalise a chain identifier for registry lookup.

    Chain keys in the registry are always lowercase; accept any case from the
    caller so that ``"Arbitrum"``, ``"ARBITRUM"``, and ``"arbitrum"`` all
    resolve identically.
    """
    return chain.lower()


def canonicalise_market(market: str) -> str:
    """Canonicalise a market identifier to the GMX registry form.

    Market identifiers in the registry use the canonical GMX form
    (``"ETH/USD"``). The funding form (``"ETH-USD"``), underscore/colon
    spellings, case variants, and the documented slash form all collapse to
    that identity. Raw addresses pass through byte-for-byte so checksum case is
    never changed. Bare SDK aliases such as ``"ETH"`` remain bare.
    """
    candidate = market.strip()
    if candidate.lower().startswith("0x"):
        return candidate
    return perp_market_pair_key(candidate) or candidate.upper()


def is_market_registered(chain: str, market: str) -> bool:
    """Return ``True`` if collateral rules are known for ``(chain, market)``.

    Used by the compiler to distinguish between "market is unknown, cannot
    validate locally" (permissive path) and "market is known and the collateral
    is wrong" (strict reject path).

    Chain and market inputs are case-insensitive.
    """
    chain_table = _GMX_V2_MARKET_COLLATERALS.get(_normalise_chain(chain))
    if chain_table is None:
        return False
    return canonicalise_market(market) in chain_table


def get_allowed_collaterals(chain: str, market: str) -> tuple[str, ...]:
    """Return the tuple of allowed collateral token symbols for a market.

    Args:
        chain: Chain name (``"arbitrum"`` or ``"avalanche"``). Case-insensitive.
        market: Market identifier as used in ``GMX_V2_MARKETS`` (e.g.
            ``"ETH/USD"``). Case-insensitive.

    Returns:
        Tuple of allowed collateral symbols. The tuple is non-empty for any
        registered ``(chain, market)`` pair.

    Raises:
        KeyError: If the market is not registered for the chain. Callers that
            want the "unknown market" path should call :func:`is_market_registered`
            first.
    """
    chain_key = _normalise_chain(chain)
    chain_table = _GMX_V2_MARKET_COLLATERALS.get(chain_key)
    if chain_table is None:
        raise KeyError(f"GMX V2 market rules are not registered for chain '{chain}'")
    market_key = canonicalise_market(market)
    try:
        return chain_table[market_key]
    except KeyError as e:
        raise KeyError(
            f"GMX V2 market '{market}' is not registered for chain '{chain}'. "
            f"Registered markets: {sorted(chain_table.keys())}"
        ) from e


def validate_collateral(chain: str, market: str, collateral_token: str) -> None:
    """Validate that ``collateral_token`` is a legal collateral for ``market``.

    Compile-path validation. This is the main entry point used by the
    ``_compile_perp_open`` path in the intent compiler. It must be called
    BEFORE any transaction actions are emitted.

    Behaviour:
      * If ``market`` is registered for ``chain`` and ``collateral_token`` is
        not one of the allowed symbols, raise
        :class:`InvalidCollateralForMarketError`.
      * If ``market`` is registered and ``collateral_token`` is recognised but
        passed as a raw 0x-address, the validation is skipped with a debug log
        and the compiler falls through to address-based resolution.
      * If ``market`` is NOT registered (unknown / new market), log a warning
        and return (permissive). The strict on-chain check still fires at
        order-time via the GMX keeper, but this module cannot do better
        without an RPC round trip.

    Args:
        chain: Chain name (``"arbitrum"`` or ``"avalanche"``). Case-insensitive.
        market: Market identifier (e.g. ``"ETH/USD"``). Case-insensitive.
        collateral_token: Collateral token symbol (e.g. ``"USDC"``) or
            0x-address. Comparison is case-insensitive for symbols; raw
            addresses are detected case-insensitively (``0x...`` or ``0X...``).

    Raises:
        InvalidCollateralForMarketError: When both the market is registered
            AND the collateral is a symbol that is not in the allowed set.
    """
    # Address-based collaterals are not validated here — the compiler's
    # symbol->address resolution step catches most of those, and an on-chain
    # address collision with a disallowed token is extremely rare. We prefer
    # a false-negative here over a false-positive that blocks a valid strategy.
    # Case-insensitive prefix check so ``0X...`` is treated the same as ``0x...``.
    if collateral_token[:2].lower() == "0x":
        logger.debug(
            "GMX V2 collateral '%s' supplied as raw address; skipping symbol-based "
            "collateral-for-market validation (market=%s chain=%s).",
            collateral_token,
            market,
            chain,
        )
        return

    if not is_market_registered(chain, market):
        logger.warning(
            "GMX V2 market '%s' on chain '%s' is not in the local collateral-rules "
            "registry; cannot validate (collateral_token=%s) before dispatch. "
            "If the market is valid on-chain the order will still be created; "
            "register the market in market_rules.py to enable local validation.",
            market,
            chain,
            collateral_token,
        )
        return

    allowed = get_allowed_collaterals(chain, market)
    # Canonical GMX V2 token symbols are uppercased in the token registry,
    # except for bridged-variant suffixes like ``WETH.e`` which preserve case.
    # Compare case-insensitively so ``"usdc"``, ``"USDC"``, and ``"Usdc"`` all
    # resolve identically.
    normalised = collateral_token.upper()
    normalised_allowed = {a.upper() for a in allowed}
    if normalised not in normalised_allowed:
        raise InvalidCollateralForMarketError(
            market=market,
            collateral=collateral_token,
            allowed_collaterals=list(allowed),
            chain=chain,
            protocol="gmx_v2",
        )


def registered_markets(chain: str) -> Iterable[str]:
    """Return the iterable of market identifiers registered for a chain.

    Useful for diagnostics / operator card messages. Chain input is
    case-insensitive.
    """
    return sorted(_GMX_V2_MARKET_COLLATERALS.get(_normalise_chain(chain), {}).keys())


__all__ = [
    "canonicalise_market",
    "InvalidCollateralForMarketError",
    "get_allowed_collaterals",
    "is_market_registered",
    "registered_markets",
    "validate_collateral",
]
