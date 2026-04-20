"""Direct JSON-RPC adapter for position readers.

The LP and lending position readers expect a gateway client with
``_rpc_stub.Call()``.  This adapter wraps a raw JSON-RPC endpoint
(such as an Anvil fork) so those readers can query on-chain state
without a running gateway.

Used by Paper Trading to get LP / lending position data from the fork.
"""

import json
import logging
from dataclasses import dataclass

import requests

logger = logging.getLogger(__name__)

# Timeout for HTTP requests to the RPC endpoint (seconds).
_DEFAULT_TIMEOUT = 10


@dataclass(frozen=True)
class _AdapterConfig:
    """Mimics the ``gateway_client.config`` interface."""

    timeout: int = _DEFAULT_TIMEOUT


class _RpcResponse:
    """Mimics ``gateway_pb2.RpcResponse``."""

    __slots__ = ("success", "error", "result")

    def __init__(self, *, success: bool, error: str = "", result: str = "") -> None:
        self.success = success
        self.error = error
        self.result = result


class _DirectRpcStub:
    """Mimics ``gateway_client._rpc_stub`` using plain HTTP JSON-RPC."""

    def __init__(self, rpc_url: str) -> None:
        self._rpc_url = rpc_url
        self._request_id = 0

    def Call(self, request: object, timeout: int | None = None) -> _RpcResponse:  # noqa: N802 — matches gateway proto
        """Execute an ``eth_call`` via the JSON-RPC endpoint.

        Args:
            request: Object with ``chain``, ``method``, ``params`` attributes
                     (mirrors ``gateway_pb2.RpcRequest``).
            timeout: Request timeout in seconds.

        Returns:
            _RpcResponse with the result hex string (JSON-encoded).
        """
        method = getattr(request, "method", "eth_call")
        params_json = getattr(request, "params", "[]")

        try:
            params = json.loads(params_json) if isinstance(params_json, str) else params_json
        except json.JSONDecodeError:
            return _RpcResponse(success=False, error="Invalid params JSON")

        self._request_id += 1
        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": self._request_id,
        }

        try:
            resp = requests.post(
                self._rpc_url,
                json=payload,
                timeout=timeout or _DEFAULT_TIMEOUT,
            )
            resp.raise_for_status()
            body = resp.json()

            if "error" in body:
                return _RpcResponse(success=False, error=str(body["error"]))

            # Position readers expect the result as a JSON-encoded string
            return _RpcResponse(success=True, result=json.dumps(body.get("result", "")))

        except Exception as e:
            logger.debug("DirectRpcAdapter call failed: %s", e, exc_info=True)
            return _RpcResponse(success=False, error=str(e))


class DirectRpcAdapter:
    """Wraps a JSON-RPC URL to satisfy the gateway client interface.

    The :class:`LPPositionReader` and :class:`LendingPositionReader` access
    ``self._gateway._rpc_stub.Call()`` and ``self._gateway.config.timeout``.
    This adapter provides exactly those attributes backed by direct HTTP calls.

    Usage::

        adapter = DirectRpcAdapter("http://localhost:8545")
        reader = LPPositionReader(adapter)
        position = reader.read_position(chain="arbitrum", token_id=12345)
    """

    def __init__(self, rpc_url: str, timeout: int = _DEFAULT_TIMEOUT) -> None:
        self._rpc_stub = _DirectRpcStub(rpc_url)
        self.config = _AdapterConfig(timeout=timeout)
