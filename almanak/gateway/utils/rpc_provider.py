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
from enum import StrEnum
from functools import lru_cache

from almanak.config.gateway_runtime import (
    anvil_generic_port_string,
    anvil_port_for_chain,
    chain_specific_rpc_url,
    gateway_prefixed_or_bare,
    generic_rpc_url,
    generic_rpc_url_env_name,
    has_chain_specific_rpc_url,
    has_generic_rpc_url,
    tenderly_api_key_for_chain,
)
from almanak.core.chains import ChainRegistry
from almanak.core.chains.solana import SOLANA_CLUSTERS

logger = logging.getLogger(__name__)

# Phase 1 removed the module-load ``load_dotenv()`` call. VIB-4424 routes the
# remaining dynamic RPC/anvil env contracts through
# ``almanak.config.gateway_runtime`` so this module no longer reads
# ``os.environ`` directly either.


def _get_gateway_api_key(name: str) -> str | None:
    """Get an API key, checking the ALMANAK_GATEWAY_ prefixed var first.

    In V2 deployed containers, the deployer injects keys with the
    ALMANAK_GATEWAY_ prefix (e.g. ALMANAK_GATEWAY_ALCHEMY_API_KEY).
    In local dev, users set the bare name (e.g. ALCHEMY_API_KEY).
    Check prefixed first so deployed containers resolve correctly.
    """
    return gateway_prefixed_or_bare(name)


def get_gateway_api_key(name: str) -> str | None:
    """Return a gateway-owned API key by config name."""
    return _get_gateway_api_key(name)


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
# Chain Configuration (derived from ChainRegistry)
# =============================================================================
#
# Until VIB-4801 this module loaded ``config/rpc_defaults.json`` (with a
# duplicate ``_BUILTIN_CHAINS`` Python literal as fallback for wheel installs
# that did not ship the repo's ``config/`` directory). The data is now folded
# onto ``ChainDescriptor.rpc`` — each chain file under
# ``almanak/core/chains/`` is the single source of truth.
#
# The module-level dict names below are preserved so existing consumers and
# the ``patch("...PUBLIC_RPC_URLS", ...)`` test sites keep working without
# changes. They are built once at import time over ``ChainRegistry.all()``.

_DESCRIPTORS = ChainRegistry.all()

# Mapping of chain names to Alchemy URL prefixes
# Format: https://{prefix}-mainnet.g.alchemy.com/v2/{API_KEY}
ALCHEMY_CHAIN_KEYS: dict[str, str] = {
    d.name: d.rpc.alchemy_prefix for d in _DESCRIPTORS if d.rpc.alchemy_prefix is not None
}

# Mapping of chain names to Tenderly subdomains
# Format: https://{subdomain}.gateway.tenderly.co/{API_KEY}
TENDERLY_SUBDOMAINS: dict[str, str] = {
    d.name: d.rpc.tenderly_subdomain for d in _DESCRIPTORS if d.rpc.tenderly_subdomain is not None
}

# Free public RPC endpoints (no API key required).
# Used as last-resort fallback when no custom URL or API key is configured.
# Source: PublicNode (publicnode.com) + official chain RPCs.
PUBLIC_RPC_URLS: dict[str, str] = {d.name: d.rpc.public_rpc for d in _DESCRIPTORS if d.rpc.public_rpc is not None}

# Solana cluster URLs (Solana uses cluster names instead of chain IDs).
# Owned by the Solana chain descriptor module.
SOLANA_CLUSTER_URLS: dict[str, str] = dict(SOLANA_CLUSTERS)

# Chains that require POA middleware (ExtraDataToPOAMiddleware).
POA_CHAINS: set[str] = {d.name for d in _DESCRIPTORS if d.rpc.poa}

# Default Anvil port mapping for multi-chain local development.
# When running multiple Anvil instances, use ANVIL_{CHAIN}_PORT env vars.
# These defaults match the Makefile _start-anvils target.
ANVIL_CHAIN_PORTS: dict[str, int] = {d.name: d.rpc.anvil_port for d in _DESCRIPTORS if d.rpc.anvil_port is not None}


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
    url = chain_specific_rpc_url(chain)
    if url:
        logger.debug("Using chain-specific custom RPC URL for %s", chain_upper)
        return url

    url = generic_rpc_url()
    if url:
        logger.debug("Using custom RPC URL from %s", generic_rpc_url_env_name())
        return url

    raise ValueError(
        f"No custom RPC URL found for chain '{chain}'. "
        f"Set ALMANAK_{chain_upper}_RPC_URL, {chain_upper}_RPC_URL, ALMANAK_RPC_URL, or RPC_URL."
    )


def _has_chain_specific_url(chain: str) -> bool:
    """Check if a chain-specific RPC URL env var is set.

    Only checks chain-specific vars (ALMANAK_{CHAIN}_RPC_URL, {CHAIN}_RPC_URL).
    Does NOT check generic catch-all vars (ALMANAK_RPC_URL, RPC_URL).
    """
    return has_chain_specific_rpc_url(chain)


def _has_generic_url() -> bool:
    """Check if a generic catch-all RPC URL env var is set (ALMANAK_RPC_URL or RPC_URL)."""
    return has_generic_rpc_url()


def _has_custom_url(chain: str) -> bool:
    """Check if any custom RPC URL env var is set (chain-specific or generic).

    Same precedence as _get_custom_url but returns bool without logging/error.
    """
    return _has_chain_specific_url(chain) or _has_generic_url()


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
    # Normalize chain alias (e.g., "bnb" -> "bsc")
    try:
        from almanak.core.constants import resolve_chain_name

        chain_lower = resolve_chain_name(chain_lower)
    except (ValueError, ImportError):
        pass
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
    1. Chain-specific custom URL (e.g., BASE_RPC_URL, ALMANAK_BASE_RPC_URL)
    2. Alchemy (if ALCHEMY_API_KEY is set and chain is supported)
    3. Tenderly (if chain-specific key is set)
    4. Generic catch-all URL (RPC_URL, ALMANAK_RPC_URL) -- with warning
    5. Public RPC (free, no API key)
    6. Raise error

    IMPORTANT: Generic RPC_URL must NOT override Alchemy for supported chains.
    A single RPC_URL pointing to Arbitrum would silently fork all chains from
    Arbitrum state, causing silent wrong-chain execution.
    """
    # 1. Chain-specific custom URL (highest priority -- user explicitly set it for this chain)
    if _has_chain_specific_url(chain):
        return NodeProvider.CUSTOM

    # 2. Alchemy (API key set, chain supported)
    if _get_gateway_api_key("ALCHEMY_API_KEY"):
        if chain in ALCHEMY_CHAIN_KEYS:
            return NodeProvider.ALCHEMY

    # 3. Tenderly
    if tenderly_api_key_for_chain(chain):
        if chain in TENDERLY_SUBDOMAINS:
            return NodeProvider.TENDERLY

    # 4. Generic catch-all URL (RPC_URL, ALMANAK_RPC_URL)
    # WARNING: This uses the same URL for ALL chains, which is almost certainly wrong
    # for multi-chain setups. Log a warning so the user notices.
    if _has_generic_url():
        generic_var = generic_rpc_url_env_name() or "RPC_URL"
        logger.warning(
            f"Using generic {generic_var} for chain '{chain}'. "
            f"This URL is shared across ALL chains -- if it points to a specific chain "
            f"(e.g., Arbitrum), other chains will get wrong-chain state. "
            f"Set {chain.upper()}_RPC_URL for chain-specific routing."
        )
        return NodeProvider.CUSTOM

    # 5. Fall back to free public RPC
    if chain in PUBLIC_RPC_URLS:
        logger.info(f"No API key configured -- using free public RPC for {chain} (rate limits may apply)")
        return NodeProvider.PUBLIC

    # No provider available
    chain_upper = chain.upper()
    raise ValueError(
        f"No RPC provider available for chain '{chain}'. "
        f"Set RPC_URL, {chain_upper}_RPC_URL, ALMANAK_RPC_URL, or ALCHEMY_API_KEY environment variable."
    )


def fork_upstream_is_public_rpc(chain: str, network: str = "mainnet") -> bool:
    """Whether a managed-Anvil fork of ``chain`` would use the free public RPC.

    Single source of truth for the VIB-5869 archive gate: rather than
    re-deriving "is an archive RPC configured?" with a parallel env-var check
    that can drift from the real fork path, the gate asks THIS — the same
    ``_auto_select_provider`` that :func:`get_rpc_url` uses to pick the fork
    upstream. So an operator who set only ``ALMANAK_GATEWAY_ALCHEMY_API_KEY``
    (resolved via ``gateway_prefixed_or_bare``), a chain-specific
    ``{CHAIN}_RPC_URL``, a generic ``RPC_URL``, or a Tenderly key is seen as
    configured by the gate exactly when the fork will actually use it — no
    false-positive fail-fast, and no false-negative pass.

    Returns ``True`` (⇒ gate should fire) when the resolver falls all the way
    through to the pruned public endpoint, or when no provider resolves at all.
    """
    chain_lower = chain.lower()
    try:
        from almanak.core.constants import resolve_chain_name

        chain_lower = resolve_chain_name(chain_lower)
    except (ValueError, ImportError):
        pass
    if network.lower() in ("anvil",):
        # Local Anvil is not a fork upstream; nothing to gate.
        return False
    try:
        return _auto_select_provider(chain_lower) is NodeProvider.PUBLIC
    except ValueError:
        # No provider resolves at all — the fork cannot use a configured
        # archive RPC, so the gate must fire just as it would for public.
        return True


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
        chain_port = anvil_port_for_chain(chain)
        if chain_port is not None:
            return f"http://127.0.0.1:{chain_port}"

        # Use default chain port mapping
        port = ANVIL_CHAIN_PORTS.get(chain.lower(), 8545)
        return f"http://127.0.0.1:{port}"

    # Single-chain: use generic ANVIL_PORT or default
    port_str = anvil_generic_port_string()
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

    api_key = _get_gateway_api_key("ALCHEMY_API_KEY")
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
    api_key = tenderly_api_key_for_chain(chain)
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
    # Normalize chain alias (e.g., "bnb" -> "bsc")
    try:
        from almanak.core.constants import resolve_chain_name

        chain_lower = resolve_chain_name(chain)
    except (ValueError, ImportError):
        chain_lower = chain.lower()
    return chain_lower in POA_CHAINS


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
    if has_generic_rpc_url():
        return True

    # Check per-chain custom URL env vars (check all known chains)
    for chain in ALCHEMY_CHAIN_KEYS:
        if has_chain_specific_rpc_url(chain):
            return True

    if _get_gateway_api_key("ALCHEMY_API_KEY"):
        return True

    # Check for any Tenderly key
    for chain in TENDERLY_SUBDOMAINS:
        if tenderly_api_key_for_chain(chain):
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


@lru_cache(maxsize=8)
def get_cached_web3(chain: str, network: str = "mainnet"):  # noqa: ANN201
    """Return a cached sync ``web3.Web3`` instance for the given chain/network.

    Uses the same URL resolution as ``get_rpc_url``. The returned instance is
    cached at the process level so multiple gateway services share a single
    HTTPProvider connection pool instead of each instantiating their own. POA
    chains (Polygon, BSC, Avalanche, Sonic) automatically have the
    ``ExtraDataToPOAMiddleware`` injected so ``eth.get_block("latest")`` does
    not raise on the 32-byte ``extraData`` field.

    The cache key is ``(chain, network)``; tests that need a fresh client (e.g.
    after env mutation) should call ``get_cached_web3.cache_clear()``.

    Args:
        chain: Chain name (e.g. "polygon", "ethereum", "arbitrum")
        network: Network environment ("mainnet", "sepolia", "anvil")

    Returns:
        Cached ``web3.Web3`` instance bound to a process-shared HTTPProvider.
    """
    # Local import keeps web3 cost off module load (matches the other call sites
    # in this gateway that lazy-import web3).
    from web3 import Web3

    rpc_url = get_rpc_url(chain, network=network)
    w3 = Web3(Web3.HTTPProvider(rpc_url))

    # POA chains include a 32-byte extraData on every block; the default
    # web3.py block validator rejects that as malformed. Inject the POA
    # middleware so eth.get_block("latest") works for fee_history / EIP-1559
    # checks downstream.
    if is_poa_chain(chain):
        try:
            from web3.middleware import ExtraDataToPOAMiddleware

            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        except ImportError:
            # Older web3.py releases name it differently. Best-effort: if
            # neither name resolves, callers that read latest block extraData
            # will see a clear web3 error instead of a silent malformation.
            try:
                from web3.middleware import geth_poa_middleware  # type: ignore[attr-defined]

                w3.middleware_onion.inject(geth_poa_middleware, layer=0)
            except ImportError:
                logger.warning("POA middleware unavailable for chain %s; latest-block reads may fail", chain)

    return w3


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
