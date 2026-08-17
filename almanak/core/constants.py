from types import MappingProxyType

from almanak.core.chains import ChainRegistry

ETH_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

# Multicall3's canonical CREATE2 deployment address (github.com/mds1/multicall3).
# Chain-agnostic by construction (same address on 250+ chains) but NOT
# universal — consumers MUST verify presence per chain via eth_getCode before
# use (VIB-4951); known incorrect deployments exist. Lives here beside
# ETH_ADDRESS as vital infra, not a per-protocol contract: pools/markets/
# vaults still belong in connector AddressRegistry tables.
MULTICALL3_ADDRESS = "0xcA11bde05977b3631167028862bE2a173976CA11"

# Legacy symbol-level stablecoin classification retained for compatibility.
# This is descriptive metadata only and MUST NOT authorize a synthetic price.
# Peg eligibility lives in the token registry and is queried by exact identity
# through ``almanak.framework.data.tokens.is_pegged``.
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


def canonical_chain_name(chain: str) -> str:
    """Best-effort alias-to-canonical chain normalization ("bnb" -> "bsc").

    The tolerant sibling of :func:`resolve_chain_name` for boundary seams that
    must not raise on unknown input: a recognized name, alias, or CAIP-2 id
    resolves to its canonical lowercase name; anything else passes through
    UNCHANGED so the caller's own fail-closed / unsupported-chain path fires
    with the original value (VIB-5293 defect class).
    """
    descriptor = ChainRegistry.try_resolve(chain)
    return descriptor.name if descriptor is not None else chain


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
