from types import MappingProxyType

from almanak import Chain
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

# Numeric chain IDs for each chain (EIP-155).
#
# Derived view over :class:`ChainRegistry` (VIB-4801). The registry is the
# single source of truth; this mapping is preserved as a read-only
# :class:`MappingProxyType` so legacy imports keep working unchanged.
# Do NOT mutate this — add or change a descriptor under
# ``almanak/core/chains/`` instead.
CHAIN_IDS: MappingProxyType[Chain, int] = MappingProxyType({d.enum: d.chain_id for d in ChainRegistry.all()})

# Common aliases mapping to Chain enum.
#
# Derived view over :class:`ChainRegistry` (VIB-4801). Each descriptor's
# canonical name and every alias resolve back to the descriptor; this map
# preserves the legacy ``alias -> Chain`` shape.
_CHAIN_ALIASES: MappingProxyType[str, Chain] = MappingProxyType(dict(ChainRegistry.aliases()))


def resolve_chain_name(chain: str) -> str:
    """Resolve any chain alias to its canonical lowercase name.

    The canonical name is derived from the Chain enum value (e.g., Chain.BSC -> "bsc").
    This normalizes aliases like "bnb" -> "bsc", "eth" -> "ethereum", "avax" -> "avalanche".

    Args:
        chain: Chain name string (e.g., "bsc", "bnb", "eth", "arbitrum")

    Returns:
        Canonical lowercase chain name

    Raises:
        ValueError: If chain name is not recognized
    """
    chain_lower = chain.lower().strip()
    chain_enum = _CHAIN_ALIASES.get(chain_lower)
    if chain_enum is None:
        raise ValueError(f"Unknown chain: {chain!r}")
    return chain_enum.value.lower()


def get_chain_id(chain: Chain | str | int) -> int:
    """Get the numeric chain ID (EIP-155) for a Chain enum, string name, or int.

    Args:
        chain: Chain enum, chain name string (e.g., "ethereum", "eth", "arbitrum"),
               or numeric chain ID

    Returns:
        Numeric chain ID (EIP-155)

    Raises:
        ValueError: If chain is not recognized
    """
    # If already an int, return it directly
    if isinstance(chain, int):
        return chain

    # If it's a Chain enum, look it up directly
    if isinstance(chain, Chain):
        chain_id = CHAIN_IDS.get(chain)
        if chain_id is None:
            raise ValueError(f"Unknown chain: {chain}")
        return chain_id

    # Handle string input - try to match to Chain enum
    if isinstance(chain, str):
        chain_str = chain.lower().strip()

        chain_enum = _CHAIN_ALIASES.get(chain_str)
        if chain_enum is None:
            raise ValueError(f"Unknown chain: {chain}")

        return CHAIN_IDS[chain_enum]

    raise ValueError(f"Invalid chain type: {type(chain)}")
