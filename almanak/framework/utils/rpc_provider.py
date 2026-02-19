"""Dynamic RPC URL Provider for EVM Chains.

NOTE: This module has been moved to almanak.gateway.utils.rpc_provider.
This file re-exports from the gateway for backward compatibility.
Direct RPC access should only be used in the gateway or CLI/local development.
Strategy containers should use GatewayWeb3Provider instead.

For new code, import from almanak.gateway.utils:
    from almanak.gateway.utils import get_rpc_url, is_poa_chain
"""

# Re-export everything from gateway for backward compatibility
from almanak.gateway.utils.rpc_provider import (
    ALCHEMY_CHAIN_KEYS,
    POA_CHAINS,
    Network,
    NodeProvider,
    get_rpc_url,
    get_rpc_url_cached,
    get_supported_chains,
    has_api_key_configured,
    is_local_rpc,
    is_poa_chain,
)

__all__ = [
    "NodeProvider",
    "Network",
    "get_rpc_url",
    "get_rpc_url_cached",
    "is_poa_chain",
    "is_local_rpc",
    "get_supported_chains",
    "has_api_key_configured",
    "ALCHEMY_CHAIN_KEYS",
    "POA_CHAINS",
]
