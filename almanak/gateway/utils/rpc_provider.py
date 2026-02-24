"""Dynamic RPC URL Provider for EVM Chains.

This module provides flexible RPC URL resolution with env-var-driven precedence,
supporting custom RPC URLs with Alchemy/Tenderly as optional fallbacks.

Key Features:
    - Bring-your-own RPC URL via env vars (any provider: Infura, QuickNode, self-hosted, etc.)
    - Alchemy API key as optional fallback (works for all chains)
    - Support for multiple node providers (Alchemy, Tenderly)
    - Free public RPC fallback (no config needed -- works out of the box)
    - Anvil detection for local development
    - Caching of Web3 clients

Env Var Precedence (mainnet/sepolia):
    1. ALMANAK_{CHAIN}_RPC_URL   (e.g. ALMANAK_ARBITRUM_RPC_URL)
    2. {CHAIN}_RPC_URL           (e.g. ARBITRUM_RPC_URL)
    3. ALMANAK_RPC_URL           (generic, all chains)
    4. RPC_URL                   (bare generic)
    5. ALCHEMY_API_KEY           (dynamic URL construction)
    6. TENDERLY_API_KEY_{CHAIN}  (per-chain Tenderly keys)
    7. Free public RPCs          (PublicNode, last resort)

    For bsc/bnb: both variants are checked (e.g. BSC_RPC_URL and BNB_RPC_URL).

Usage:
    # Get RPC URL for a chain (checks custom env vars, then ALCHEMY_API_KEY)
    url = get_rpc_url("arbitrum")
    url = get_rpc_url("base", network="mainnet")

    # Explicit provider
    url = get_rpc_url("arbitrum", provider=NodeProvider.TENDERLY)

    # Local Anvil
    url = get_rpc_url("arbitrum", network="anvil")  # Returns http://127.0.0.1:8545

Environment Variables:
    ALMANAK_{CHAIN}_RPC_URL: Per-chain custom RPC URL (highest priority)
    {CHAIN}_RPC_URL: Per-chain bare custom RPC URL
    ALMANAK_RPC_URL: Generic custom RPC URL for all chains
    RPC_URL: Bare generic custom RPC URL
    ALCHEMY_API_KEY: Alchemy API key fallback (works for all chains)
    ANVIL_PORT: Optional custom Anvil port (default: 8545)
    TENDERLY_API_KEY_{CHAIN}: Per-chain Tenderly keys (e.g., TENDERLY_API_KEY_ARBITRUM)
"""

import logging
import os
from enum import StrEnum
from functools import lru_cache

from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


# =============================================================================
# Enums
# =============================================================================


class NodeProvider(StrEnum):
    """Supported node providers for RPC endpoints."""

    ALCHEMY = "alchemy"
    TENDERLY = "tenderly"
    ANVIL = "anvil"
    CUSTOM = "custom"  # For explicitly provided URLs
    PUBLIC = "public"  # Free public RPCs (no API key, last resort)


class Network(StrEnum):
    """Network environment for chains."""

    MAINNET = "mainnet"
    TESTNET = "testnet"  # Generic testnet
    SEPOLIA = "sepolia"  # Ethereum testnet
    ANVIL = "anvil"  # Local fork


# =============================================================================
# Chain Configuration
# =============================================================================


# Mapping of chain names to Alchemy URL prefixes
# Format: https://{prefix}-mainnet.g.alchemy.com/v2/{API_KEY}
ALCHEMY_CHAIN_KEYS: dict[str, str] = {
    "ethereum": "eth",
    "arbitrum": "arb",
    "optimism": "opt",
    "base": "base",
    "avalanche": "avax",
    "polygon": "polygon",
    "bnb": "bnb",
    "sonic": "sonic",
    "plasma": "plasma",
    "linea": "linea",
}

# Mapping of chain names to Tenderly subdomains
# Format: https://{subdomain}.gateway.tenderly.co/{API_KEY}
TENDERLY_SUBDOMAINS: dict[str, str] = {
    "ethereum": "mainnet",
    "arbitrum": "arbitrum",
    "base": "base",
    "avalanche": "avalanche",
    "plasma": "plasma",
    # Note: Optimism, Polygon, BSC, Sonic not supported by Tenderly RPC
}

# Free public RPC endpoints (no API key required).
# Used as last-resort fallback when no custom URL or API key is configured.
# Source: PublicNode (publicnode.com) + official chain RPCs.
PUBLIC_RPC_URLS: dict[str, str] = {
    "ethereum": "https://ethereum-rpc.publicnode.com",
    "arbitrum": "https://arbitrum-one-rpc.publicnode.com",
    "optimism": "https://optimism-rpc.publicnode.com",
    "base": "https://base-rpc.publicnode.com",
    "avalanche": "https://avalanche-c-chain-rpc.publicnode.com",
    "polygon": "https://polygon-bor-rpc.publicnode.com",
    "bnb": "https://bsc-rpc.publicnode.com",
    "bsc": "https://bsc-rpc.publicnode.com",
    "sonic": "https://sonic-rpc.publicnode.com",
    "linea": "https://linea-rpc.publicnode.com",
    "plasma": "https://rpc.plasma.to",
}

# Chains that require POA middleware (geth_poa_middleware)
POA_CHAINS: set[str] = {"avalanche", "bnb", "polygon"}

# Default Anvil port mapping for multi-chain local development
# When running multiple Anvil instances, use ANVIL_{CHAIN}_PORT env vars
# These defaults match the Makefile _start-anvils target
ANVIL_CHAIN_PORTS: dict[str, int] = {
    "arbitrum": 8545,
    "bsc": 8546,
    "bnb": 8546,  # Alias for bsc
    "avalanche": 8547,
    "base": 8548,
    "ethereum": 8549,
    "optimism": 8550,
    "polygon": 8551,
    "linea": 8552,
    "sonic": 8553,
    "plasma": 8554,
}


# =============================================================================
# RPC URL Construction
# =============================================================================


def _get_custom_url(chain: str) -> str:
    """Check env vars for a custom RPC URL in precedence order.

    Precedence:
        1. ALMANAK_{CHAIN}_RPC_URL
        2. {CHAIN}_RPC_URL
        3. ALMANAK_RPC_URL
        4. RPC_URL

    For bsc/bnb chains, both variants are checked at each level.

    Args:
        chain: Normalized chain name (lowercase)

    Returns:
        Custom RPC URL from environment

    Raises:
        ValueError: If no custom URL env var is set
    """
    chain_upper = chain.upper()

    # Determine alias variants for bsc/bnb (requested chain checked first)
    variants = [chain_upper]
    if chain_upper == "BSC":
        variants = ["BSC", "BNB"]
    elif chain_upper == "BNB":
        variants = ["BNB", "BSC"]

    # 1. ALMANAK_{CHAIN}_RPC_URL
    for variant in variants:
        env_var = f"ALMANAK_{variant}_RPC_URL"
        url = os.environ.get(env_var)
        if url:
            logger.debug(f"Using custom RPC URL from {env_var}")
            return url

    # 2. {CHAIN}_RPC_URL
    for variant in variants:
        env_var = f"{variant}_RPC_URL"
        url = os.environ.get(env_var)
        if url:
            logger.debug(f"Using custom RPC URL from {env_var}")
            return url

    # 3. ALMANAK_RPC_URL
    url = os.environ.get("ALMANAK_RPC_URL")
    if url:
        logger.debug("Using custom RPC URL from ALMANAK_RPC_URL")
        return url

    # 4. RPC_URL
    url = os.environ.get("RPC_URL")
    if url:
        logger.debug("Using custom RPC URL from RPC_URL")
        return url

    raise ValueError(
        f"No custom RPC URL found for chain '{chain}'. "
        f"Set ALMANAK_{chain_upper}_RPC_URL, {chain_upper}_RPC_URL, ALMANAK_RPC_URL, or RPC_URL."
    )


def _has_custom_url(chain: str) -> bool:
    """Check if a custom RPC URL env var is set for this chain.

    Same precedence as _get_custom_url but returns bool without logging/error.
    """
    chain_upper = chain.upper()
    variants = [chain_upper]
    if chain_upper == "BSC":
        variants = ["BSC", "BNB"]
    elif chain_upper == "BNB":
        variants = ["BNB", "BSC"]

    for variant in variants:
        if os.environ.get(f"ALMANAK_{variant}_RPC_URL"):
            return True
        if os.environ.get(f"{variant}_RPC_URL"):
            return True

    if os.environ.get("ALMANAK_RPC_URL"):
        return True
    if os.environ.get("RPC_URL"):
        return True

    return False


def get_rpc_url(
    chain: str,
    network: str = "mainnet",
    provider: NodeProvider | None = None,
    custom_url: str | None = None,
) -> str:
    """Get RPC URL for a chain, using custom env vars or API key fallback.

    Checks custom RPC URL env vars first, then falls back to Alchemy/Tenderly
    API keys for dynamic URL construction.

    Args:
        chain: Chain name (e.g., "arbitrum", "base", "ethereum")
        network: Network environment - "mainnet", "sepolia", "testnet", or "anvil"
        provider: Optional node provider override. If None, auto-selects based on
                  available env vars and API keys. Use NodeProvider.CUSTOM
                  with custom_url parameter for explicit URLs.
        custom_url: Custom RPC URL when provider=NodeProvider.CUSTOM. If not
                    provided with CUSTOM provider, falls back to env var lookup.

    Returns:
        RPC URL for the chain

    Raises:
        ValueError: If chain is unsupported and no RPC source is available.

    Example:
        # Custom RPC URL from env (RPC_URL, ARBITRUM_RPC_URL, etc.):
        url = get_rpc_url("arbitrum")

        # Alchemy fallback (ALCHEMY_API_KEY):
        url = get_rpc_url("base")  # https://base-mainnet.g.alchemy.com/v2/xxx

        # Local development
        url = get_rpc_url("arbitrum", network="anvil")  # http://127.0.0.1:8545

        # Explicit custom URL (parameter takes precedence)
        url = get_rpc_url("arbitrum", provider=NodeProvider.CUSTOM,
                          custom_url="https://my-rpc.example.com")
    """
    chain_lower = chain.lower()
    network_lower = network.lower()

    # Normalize testnet to sepolia (Ethereum's primary testnet)
    if network_lower == "testnet":
        network_lower = "sepolia"

    # Handle Anvil (local development)
    if network_lower == "anvil":
        return _get_anvil_url(chain_lower)

    # Handle custom provider
    if provider == NodeProvider.CUSTOM:
        if custom_url:
            return custom_url
        # Fall back to env var lookup when no explicit URL provided
        return _get_custom_url(chain_lower)

    # Auto-select provider if not specified
    if provider is None:
        provider = _auto_select_provider(chain_lower)

    # Build URL based on provider
    if provider == NodeProvider.ALCHEMY:
        return _get_alchemy_url(chain_lower, network_lower)
    elif provider == NodeProvider.TENDERLY:
        return _get_tenderly_url(chain_lower)
    elif provider == NodeProvider.CUSTOM:
        return _get_custom_url(chain_lower)
    elif provider == NodeProvider.PUBLIC:
        return _get_public_url(chain_lower)
    elif provider == NodeProvider.ANVIL:
        return _get_anvil_url(chain_lower)
    else:
        raise ValueError(f"Unsupported provider: {provider}")


def _auto_select_provider(chain: str) -> NodeProvider:
    """Auto-select the best available provider for a chain.

    Priority:
    1. Custom URL (if any custom RPC URL env var is set)
    2. Alchemy (if ALCHEMY_API_KEY is set)
    3. Tenderly (if chain-specific key is set)
    4. Public RPC (free, no API key -- last resort)
    5. Raise error
    """
    # Check custom URL env vars first
    if _has_custom_url(chain):
        return NodeProvider.CUSTOM

    # Check Alchemy
    if os.environ.get("ALCHEMY_API_KEY"):
        if chain in ALCHEMY_CHAIN_KEYS:
            return NodeProvider.ALCHEMY

    # Check Tenderly
    tenderly_key_var = f"TENDERLY_API_KEY_{chain.upper()}"
    if os.environ.get(tenderly_key_var):
        if chain in TENDERLY_SUBDOMAINS:
            return NodeProvider.TENDERLY

    # Fall back to free public RPC
    if chain in PUBLIC_RPC_URLS:
        logger.info(f"No API key configured -- using free public RPC for {chain} (rate limits may apply)")
        return NodeProvider.PUBLIC

    # No provider available
    chain_upper = chain.upper()
    raise ValueError(
        f"No RPC provider available for chain '{chain}'. "
        f"Set RPC_URL, {chain_upper}_RPC_URL, ALMANAK_RPC_URL, or ALCHEMY_API_KEY environment variable."
    )


def _get_anvil_url(chain: str | None = None) -> str:
    """Get Anvil RPC URL for local development.

    For single-chain, uses ANVIL_PORT env var or default 8545.
    For multi-chain, uses chain-specific port from ANVIL_CHAIN_PORTS mapping,
    or ANVIL_{CHAIN}_PORT env var override.

    Args:
        chain: Optional chain name for multi-chain port mapping

    Returns:
        Local Anvil RPC URL (e.g., http://127.0.0.1:8545)
    """
    # Check for chain-specific port override first
    if chain:
        chain_port_var = f"ANVIL_{chain.upper()}_PORT"
        chain_port = os.environ.get(chain_port_var)
        if chain_port:
            return f"http://127.0.0.1:{chain_port}"

        # Use default chain port mapping
        port = ANVIL_CHAIN_PORTS.get(chain.lower(), 8545)
        return f"http://127.0.0.1:{port}"

    # Single-chain: use generic ANVIL_PORT or default
    port_str = os.environ.get("ANVIL_PORT", "8545")
    return f"http://127.0.0.1:{port_str}"


def _get_alchemy_url(chain: str, network: str = "mainnet") -> str:
    """Build Alchemy RPC URL for the specified chain and network.

    URL format: https://{chain_key}-{network}.g.alchemy.com/v2/{api_key}

    Args:
        chain: Normalized chain name (lowercase)
        network: Network name ("mainnet" or "sepolia")

    Returns:
        Full Alchemy RPC URL

    Raises:
        ValueError: If chain is unsupported or API key is missing
    """
    if chain not in ALCHEMY_CHAIN_KEYS:
        supported = ", ".join(sorted(ALCHEMY_CHAIN_KEYS.keys()))
        raise ValueError(f"Chain '{chain}' not supported by Alchemy. Supported chains: {supported}")

    api_key = os.environ.get("ALCHEMY_API_KEY")
    if not api_key:
        raise ValueError(
            "ALCHEMY_API_KEY environment variable not set. Get your API key from https://dashboard.alchemy.com/"
        )

    chain_key = ALCHEMY_CHAIN_KEYS[chain]

    # Build URL based on network
    if network == "mainnet":
        return f"https://{chain_key}-mainnet.g.alchemy.com/v2/{api_key}"
    elif network == "sepolia":
        return f"https://{chain_key}-sepolia.g.alchemy.com/v2/{api_key}"
    else:
        raise ValueError(f"Unsupported network '{network}' for Alchemy. Use 'mainnet' or 'sepolia'.")


def _get_tenderly_url(chain: str) -> str:
    """Build Tenderly RPC URL for the specified chain.

    URL format: https://{subdomain}.gateway.tenderly.co/{api_key}

    Note: Tenderly requires per-chain API keys (TENDERLY_API_KEY_{CHAIN}).

    Args:
        chain: Normalized chain name (lowercase)

    Returns:
        Full Tenderly RPC URL

    Raises:
        ValueError: If chain is unsupported or API key is missing
    """
    if chain not in TENDERLY_SUBDOMAINS:
        supported = ", ".join(sorted(TENDERLY_SUBDOMAINS.keys()))
        raise ValueError(f"Chain '{chain}' not supported by Tenderly. Supported chains: {supported}")

    api_key_var = f"TENDERLY_API_KEY_{chain.upper()}"
    api_key = os.environ.get(api_key_var)
    if not api_key:
        raise ValueError(f"{api_key_var} environment variable not set for Tenderly on {chain}.")

    subdomain = TENDERLY_SUBDOMAINS[chain]
    return f"https://{subdomain}.gateway.tenderly.co/{api_key}"


def _get_public_url(chain: str) -> str:
    """Get free public RPC URL for the specified chain.

    These are rate-limited community endpoints that require no API key.
    Used as a last-resort fallback when no custom URL or API key is configured.

    Args:
        chain: Normalized chain name (lowercase)

    Returns:
        Public RPC URL

    Raises:
        ValueError: If no public RPC is available for the chain
    """
    url = PUBLIC_RPC_URLS.get(chain)
    if not url:
        supported = ", ".join(sorted(PUBLIC_RPC_URLS.keys()))
        raise ValueError(f"No free public RPC available for chain '{chain}'. Supported: {supported}")
    return url


# =============================================================================
# Helper Functions
# =============================================================================


def is_poa_chain(chain: str) -> bool:
    """Check if a chain requires POA middleware.

    POA (Proof of Authority) chains like Avalanche, BNB, and Polygon require
    special middleware to handle their block format.

    Args:
        chain: Chain name

    Returns:
        True if chain requires POA middleware
    """
    return chain.lower() in POA_CHAINS


def is_local_rpc(rpc_url: str) -> bool:
    """Check if an RPC URL is a local endpoint (Anvil, Hardhat, etc.).

    Args:
        rpc_url: RPC URL to check

    Returns:
        True if URL points to localhost
    """
    return "127.0.0.1" in rpc_url or "localhost" in rpc_url


def get_supported_chains() -> list[str]:
    """Get list of chains supported by Alchemy.

    Returns:
        Sorted list of supported chain names
    """
    return sorted(ALCHEMY_CHAIN_KEYS.keys())


def has_api_key_configured() -> bool:
    """Check if at least one RPC source is configured (custom URL or API key).

    Returns:
        True if any custom RPC URL env var, ALCHEMY_API_KEY, or any TENDERLY_API_KEY_* is set
    """
    # Check generic custom URL env vars
    if os.environ.get("RPC_URL"):
        return True
    if os.environ.get("ALMANAK_RPC_URL"):
        return True

    # Check per-chain custom URL env vars (check all known chains)
    for chain in ALCHEMY_CHAIN_KEYS:
        chain_upper = chain.upper()
        variants = [chain_upper]
        if chain_upper == "BNB":
            variants.append("BSC")
        for variant in variants:
            if os.environ.get(f"ALMANAK_{variant}_RPC_URL"):
                return True
            if os.environ.get(f"{variant}_RPC_URL"):
                return True

    if os.environ.get("ALCHEMY_API_KEY"):
        return True

    # Check for any Tenderly key
    for chain in TENDERLY_SUBDOMAINS:
        if os.environ.get(f"TENDERLY_API_KEY_{chain.upper()}"):
            return True

    return False


@lru_cache(maxsize=32)
def get_rpc_url_cached(
    chain: str,
    network: str = "mainnet",
    custom_url: str | None = None,
) -> str:
    """Cached version of get_rpc_url for performance.

    Use this when making repeated calls for the same chain/network.

    Note: When using custom_url, the URL itself becomes part of the cache key.
    For NodeProvider.CUSTOM, pass the custom_url directly; the provider will
    be inferred.

    Args:
        chain: Chain name
        network: Network environment
        custom_url: Custom RPC URL (when provided, returns this URL directly)

    Returns:
        Cached RPC URL
    """
    if custom_url:
        return get_rpc_url(chain, network, provider=NodeProvider.CUSTOM, custom_url=custom_url)
    return get_rpc_url(chain, network)


# =============================================================================
# Exports
# =============================================================================


__all__ = [
    "ALCHEMY_CHAIN_KEYS",
    "Network",
    "NodeProvider",
    "POA_CHAINS",
    "get_rpc_url",
    "get_rpc_url_cached",
    "get_supported_chains",
    "has_api_key_configured",
    "is_local_rpc",
    "is_poa_chain",
    "_get_custom_url",
    "_has_custom_url",
]
