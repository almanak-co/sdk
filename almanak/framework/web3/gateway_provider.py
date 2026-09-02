"""Web3 providers that route JSON-RPC calls through the gateway sidecar."""

import json
import logging
from typing import Any

import grpc
from web3 import Web3
from web3.providers.async_base import AsyncJSONBaseProvider
from web3.providers.base import JSONBaseProvider
from web3.types import RPCEndpoint, RPCResponse

from almanak.framework.gateway_client import GatewayClient
from almanak.gateway.proto import gateway_pb2

logger = logging.getLogger(__name__)


class GatewayWeb3Provider(JSONBaseProvider):
    """Web3.py provider that routes JSON-RPC calls through the gateway.

    API keys remain in the gateway while strategies use the web3.py API.
    """

    def __init__(
        self,
        gateway_client: GatewayClient,
        chain: str,
        request_timeout: float = 30.0,
    ):
        super().__init__()
        self._gateway_client = gateway_client
        self._chain = chain.lower()
        self._request_timeout = request_timeout
        self._request_counter = 0

        logger.info("Initialized GatewayWeb3Provider for chain: %s", chain)

    def _get_request_id(self) -> str:
        self._request_counter += 1
        return str(self._request_counter)

    def make_request(self, method: RPCEndpoint, params: Any) -> RPCResponse:
        """Make a JSON-RPC request through the gateway."""
        request_id = self._get_request_id()

        params_json = json.dumps(params) if params else "[]"

        rpc_request = gateway_pb2.RpcRequest(
            chain=self._chain,
            method=str(method),
            params=params_json,
            id=request_id,
        )

        try:
            response = self._gateway_client.rpc.Call(
                rpc_request,
                timeout=self._request_timeout,
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

        except grpc.RpcError:
            raise
        except Exception as e:
            logger.error("Gateway RPC call failed: %s", e)
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32603, "message": str(e)},
            }


class AsyncGatewayWeb3Provider(AsyncJSONBaseProvider):
    """Gateway-backed provider for use with AsyncWeb3."""

    is_async = True

    def __init__(
        self,
        gateway_client: GatewayClient,
        chain: str,
        request_timeout: float = 30.0,
    ):
        super().__init__()
        self._gateway_client = gateway_client
        self._chain = chain.lower()
        self._request_timeout = request_timeout
        self._request_counter = 0

    def _get_request_id(self) -> str:
        self._request_counter += 1
        return str(self._request_counter)

    async def make_request(self, method: RPCEndpoint, params: Any) -> RPCResponse:
        """Make an async JSON-RPC request through the gateway."""
        request_id = self._get_request_id()
        params_json = json.dumps(params) if params else "[]"

        rpc_request = gateway_pb2.RpcRequest(
            chain=self._chain,
            method=str(method),
            params=params_json,
            id=request_id,
        )

        try:
            # The GatewayClient stub is synchronous, so offload it rather than
            # block the event loop.
            import asyncio

            response = await asyncio.to_thread(
                self._gateway_client.rpc.Call,
                rpc_request,
                timeout=self._request_timeout,
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

        except grpc.RpcError:
            raise
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
    """Create a Web3 instance backed by the gateway."""
    provider = GatewayWeb3Provider(gateway_client, chain, request_timeout)
    return Web3(provider)
