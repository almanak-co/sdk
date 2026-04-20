"""Gateway-backed Web3 provider.

This module provides a web3.py provider that routes JSON-RPC calls through
the gateway sidecar. This keeps API keys secure in the gateway while allowing
strategies to use web3.py for custom contract calls.

Example:
    from almanak.framework.web3 import GatewayWeb3Provider, get_gateway_web3

    # Using the provider directly
    provider = GatewayWeb3Provider(gateway_client, chain="arbitrum")
    w3 = Web3(provider)
    balance = w3.eth.get_balance("0x...")

    # Using the convenience function
    w3 = get_gateway_web3(gateway_client, chain="arbitrum")
    balance = w3.eth.get_balance("0x...")
"""

import json
import logging
from typing import Any

from web3 import Web3
from web3.providers.base import JSONBaseProvider
from web3.types import RPCEndpoint, RPCResponse

from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


class GatewayWeb3Provider(JSONBaseProvider):
    """Web3.py provider that routes JSON-RPC calls through the gateway.

    This provider implements the web3.py BaseProvider interface and forwards
    all JSON-RPC requests to the gateway's RpcService. API keys remain secure
    in the gateway while strategies can use the full web3.py API.

    Attributes:
        gateway_client: Connected gateway client
        chain: Chain identifier (e.g., "arbitrum", "base", "ethereum")

    Example:
        client = GatewayClient()
        client.connect()

        provider = GatewayWeb3Provider(client, chain="arbitrum")
        w3 = Web3(provider)

        # Now use web3.py as normal
        block = w3.eth.get_block("latest")
        balance = w3.eth.get_balance("0x...")
    """

    def __init__(
        self,
        gateway_client: GatewayClient,
        chain: str,
        request_timeout: float = 30.0,
    ):
        """Initialize the gateway web3 provider.

        Args:
            gateway_client: Connected gateway client
            chain: Chain identifier
            request_timeout: Timeout for RPC calls in seconds
        """
        super().__init__()
        self._gateway_client = gateway_client
        self._chain = chain.lower()
        self._request_timeout = request_timeout
        self._request_counter = 0

        logger.info("Initialized GatewayWeb3Provider for chain: %s", chain)

    def _get_request_id(self) -> str:
        """Generate a unique request ID."""
        self._request_counter += 1
        return str(self._request_counter)

    def make_request(self, method: RPCEndpoint, params: Any) -> RPCResponse:
        """Make a JSON-RPC request through the gateway.

        Args:
            method: JSON-RPC method (e.g., "eth_call")
            params: Method parameters

        Returns:
            JSON-RPC response
        """
        request_id = self._get_request_id()

        # Serialize params to JSON
        params_json = json.dumps(params) if params else "[]"

        # Create the RPC request
        rpc_request = gateway_pb2.RpcRequest(
            chain=self._chain,
            method=str(method),
            params=params_json,
            id=request_id,
        )

        try:
            # Make the call through the gateway
            response = self._gateway_client.rpc.Call(
                rpc_request,
                timeout=self._request_timeout,
            )

            if response.success:
                # Parse the result
                result = json.loads(response.result) if response.result else None
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": result,
                }
            else:
                # Parse the error
                error = json.loads(response.error) if response.error else {"code": -32603, "message": "Unknown error"}
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": error,  # type: ignore[typeddict-item]
                }

        except Exception as e:
            logger.error("Gateway RPC call failed: %s", e)
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": str(e)},
            }


class AsyncGatewayWeb3Provider(JSONBaseProvider):
    """Async version of GatewayWeb3Provider.

    For use with AsyncWeb3. Routes JSON-RPC calls through the gateway
    asynchronously.

    Example:
        client = GatewayClient()
        client.connect()

        provider = AsyncGatewayWeb3Provider(client, chain="arbitrum")
        w3 = AsyncWeb3(provider)

        block = await w3.eth.get_block("latest")
    """

    is_async = True

    def __init__(
        self,
        gateway_client: GatewayClient,
        chain: str,
        request_timeout: float = 30.0,
    ):
        """Initialize the async gateway web3 provider.

        Args:
            gateway_client: Connected gateway client
            chain: Chain identifier
            request_timeout: Timeout for RPC calls in seconds
        """
        super().__init__()
        self._gateway_client = gateway_client
        self._chain = chain.lower()
        self._request_timeout = request_timeout
        self._request_counter = 0

    def _get_request_id(self) -> str:
        """Generate a unique request ID."""
        self._request_counter += 1
        return str(self._request_counter)

    async def make_request(self, method: RPCEndpoint, params: Any) -> RPCResponse:  # type: ignore[override]
        """Make an async JSON-RPC request through the gateway.

        Args:
            method: JSON-RPC method
            params: Method parameters

        Returns:
            JSON-RPC response
        """
        request_id = self._get_request_id()
        params_json = json.dumps(params) if params else "[]"

        rpc_request = gateway_pb2.RpcRequest(
            chain=self._chain,
            method=str(method),
            params=params_json,
            id=request_id,
        )

        try:
            # For async we need to use the async stub
            # The gateway client would need async support
            # For now, run sync in thread pool
            import asyncio

            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self._gateway_client.rpc.Call(
                    rpc_request,
                    timeout=self._request_timeout,
                ),
            )

            if response.success:
                result = json.loads(response.result) if response.result else None
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": result,
                }
            else:
                error = json.loads(response.error) if response.error else {"code": -32603, "message": "Unknown error"}
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "error": error,  # type: ignore[typeddict-item]
                }

        except Exception as e:
            logger.error("Gateway RPC call failed: %s", e)
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": str(e)},
            }


def get_gateway_web3(
    gateway_client: GatewayClient,
    chain: str,
    request_timeout: float = 30.0,
) -> Web3:
    """Create a Web3 instance backed by the gateway.

    Convenience function that creates a GatewayWeb3Provider and returns
    a configured Web3 instance.

    Args:
        gateway_client: Connected gateway client
        chain: Chain identifier (e.g., "arbitrum", "base")
        request_timeout: Timeout for RPC calls in seconds

    Returns:
        Configured Web3 instance

    Example:
        with GatewayClient() as client:
            w3 = get_gateway_web3(client, "arbitrum")
            balance = w3.eth.get_balance("0x...")
    """
    provider = GatewayWeb3Provider(gateway_client, chain, request_timeout)
    return Web3(provider)
