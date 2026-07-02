from types import MappingProxyType

from almanak.core.chains import ChainRegistry

ETH_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

# Known stablecoins pegged to ~$1 USD.
# Single source of truth for stablecoin identification across the SDK.
# Used by: IntentCompiler (price fallback), backtesting (price fallback),
# CoinGecko provider (unlisted token fallback), token defaults.
STABLECOINS: frozenset[str] = frozenset(
    {
        "USDC",
        "USDT",
        "DAI",
        "USDC.E",
        "USDBC",
        "USDT.E",
        "USDE",
        "SUSDE",
        "SDAI",
        "FRAX",
        "LUSD",
        "TUSD",
        "BUSD",
        "CRVUSD",
        "PYUSD",
        "GHO",
        "FUSDT0",
        "USDP",
        "USDT0",
        "USDG",
    }
)

# Curve StableSwap USD-numeraire allowlist (VIB-5536; previously
# ``_USD_STABLE_SYMBOLS`` in ``framework/valuation/curve_lp_position_reader.py``).
#
# A pool whose coins are ALL in this set marks at ``peg = $1`` (its LP-token
# invariant unit IS a USD unit); a pool with any non-member coin is treated as
# non-USD-numeraire and left UNAVAILABLE rather than mis-marked. Used by BOTH
# the Curve NAV repricer (valuation) and the Curve basis-peg (accounting LP
# handler) — hence it lives here in ``core`` so neither layer imports from the
# other (accounting → valuation would be a backward import). Both consumers MUST
# reference this same frozenset object; do not fork or duplicate it.
#
# This is a SEPARATE, purpose-built ~$1-peg allowlist — NOT a subset of
# ``STABLECOINS`` and MUST NOT be merged into it. The two sets are partially
# DISJOINT by design, in both directions:
#   * ``CURVE_USD_STABLE_SYMBOLS`` EXCLUDES the yield-bearing / rebasing dollar
#     tokens ``STABLECOINS`` (correctly, for its own looser "is a dollar-ish
#     stablecoin" uses) includes — e.g. ``SDAI`` / ``SUSDE`` — which trade ABOVE
#     $1; pegging a Curve LP to $1 on those would mis-mark it.
#   * It INCLUDES several $1-numeraire coins ``STABLECOINS`` happens to omit
#     (e.g. ``GUSD``, ``DOLA``, ``USDBC``, ``AXLUSDC``).
# Membership asserts "this coin is a $1 NUMERAIRE by design" — NOT that it can
# never depeg. Some members are SOFT-pegged and have depegged historically
# (``MIM``, ``USDD``, ``SUSD``, ``DOLA``). That is deliberate and safe ONLY
# because the runtime peg is cross-checked: the Curve NAV repricer degrades a
# marked pool to UNAVAILABLE when the oracle-vs-pool depeg guard (VIB-5426) sees
# divergence, so a depegged member never silently marks at par. VIB-5570 tracks
# verifying that same depeg protection covers the accounting-basis peg path (not
# just NAV) before this allowlist is trusted for soft-pegged members there.
CURVE_USD_STABLE_SYMBOLS: frozenset[str] = frozenset(
    {
        "USDC",
        "USDC.E",  # bridged USDC (Arbitrum/Optimism/Polygon) — 1:1 USDC
        "USDT",
        "DAI",
        "FRAX",
        "CRVUSD",
        "USDD",
        "TUSD",
        "BUSD",
        "GUSD",
        "LUSD",
        "MIM",
        "SUSD",
        "USDP",
        "DOLA",
        "GHO",
        "PYUSD",
        "USDE",
        # Bridged / wrapped USDC variants held by PLAIN USD-stable Curve pools
        # (audit P0-3). Each is a 1:1 USD-pegged wrapper of canonical USDC, so the
        # peg = $1 numeraire holds exactly as for native USDC:
        "USDBC",  # USD Base Coin — native-bridge bridged USDC on Base, 1:1 USDC
        "AXLUSDC",  # Axelar-wrapped USDC, 1:1 backed by USDC. Used by Base 4pool.
    }
)

# Numeric chain IDs for each chain (EIP-155), keyed by canonical lowercase
# chain name.
#
# Derived view over :class:`ChainRegistry` (VIB-4801). The registry is the
# single source of truth; this mapping is preserved as a read-only
# :class:`MappingProxyType` so legacy imports keep working unchanged.
# Do NOT mutate this — add or change a descriptor under
# ``almanak/core/chains/`` instead.
CHAIN_IDS: MappingProxyType[str, int] = MappingProxyType({d.name: d.chain_id for d in ChainRegistry.all()})

# Common aliases mapping to the canonical lowercase chain name.
#
# Derived view over :class:`ChainRegistry` (VIB-4801). Each descriptor's
# canonical name and every alias resolve to the canonical name.
_CHAIN_ALIASES: MappingProxyType[str, str] = MappingProxyType(ChainRegistry.aliases())


def resolve_chain_name(chain: str) -> str:
    """Resolve any chain alias or CAIP-2 id to its canonical lowercase name.

    This normalizes aliases like "bnb" -> "bsc", "eth" -> "ethereum", "avax" -> "avalanche".
    A CAIP-2-shaped input (``eip155:42161``, ``solana:5eykt4UsFv8P8…``) resolves to
    the same canonical name as its alias form (VIB-5175); the reference case is
    preserved so Solana's base58 genesis hash matches.

    Args:
        chain: Chain name, alias, or CAIP-2 id (e.g. "bsc", "bnb", "arbitrum",
            "eip155:42161")

    Returns:
        Canonical lowercase chain name

    Raises:
        ValueError: If chain name is not recognized
    """
    # CAIP-2 ids carry a case-sensitive reference (Solana), so detect and route
    # them BEFORE lowercasing. Non-CAIP inputs fall through unchanged.
    caip = ChainRegistry.try_resolve_caip2(chain.strip())
    if caip is not None:
        return caip.name
    canonical = _CHAIN_ALIASES.get(chain.lower().strip())
    if canonical is None:
        raise ValueError(f"Unknown chain: {chain!r}")
    return canonical


def get_chain_id(chain: str | int) -> int:
    """Get the numeric chain ID (EIP-155) for a chain name string or int.

    Args:
        chain: Chain name string (e.g., "ethereum", "eth", "arbitrum")
               or numeric chain ID

    Returns:
        Numeric chain ID (EIP-155)

    Raises:
        ValueError: If chain is not recognized
    """
    # If already an int, return it directly
    if isinstance(chain, int):
        return chain

    # Handle string input — canonical name or alias.
    if isinstance(chain, str):
        canonical = _CHAIN_ALIASES.get(chain.lower().strip())
        if canonical is None:
            raise ValueError(f"Unknown chain: {chain}")

        return CHAIN_IDS[canonical]

    raise ValueError(f"Invalid chain type: {type(chain)}")
