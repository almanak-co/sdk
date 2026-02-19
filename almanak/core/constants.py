from almanak import Chain

ETH_ADDRESS = "0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE"

# Numeric chain IDs for each chain (EIP-155)
CHAIN_IDS: dict[Chain, int] = {
    Chain.ETHEREUM: 1,
    Chain.ARBITRUM: 42161,
    Chain.OPTIMISM: 10,
    Chain.BASE: 8453,
    Chain.AVALANCHE: 43114,
    Chain.POLYGON: 137,
    Chain.BSC: 56,
    Chain.SONIC: 146,
    Chain.PLASMA: 9745,
    Chain.BLAST: 81457,
    Chain.MANTLE: 5000,
    Chain.BERACHAIN: 80094,
}

# Common aliases mapping to Chain enum
_CHAIN_ALIASES: dict[str, Chain] = {
    "ethereum": Chain.ETHEREUM,
    "eth": Chain.ETHEREUM,
    "mainnet": Chain.ETHEREUM,
    "arbitrum": Chain.ARBITRUM,
    "arb": Chain.ARBITRUM,
    "optimism": Chain.OPTIMISM,
    "op": Chain.OPTIMISM,
    "base": Chain.BASE,
    "avalanche": Chain.AVALANCHE,
    "avax": Chain.AVALANCHE,
    "polygon": Chain.POLYGON,
    "matic": Chain.POLYGON,
    "bsc": Chain.BSC,
    "bnb": Chain.BSC,
    "binance": Chain.BSC,
    "sonic": Chain.SONIC,
    "plasma": Chain.PLASMA,
    "blast": Chain.BLAST,
    "mantle": Chain.MANTLE,
    "berachain": Chain.BERACHAIN,
    "bera": Chain.BERACHAIN,
}


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
