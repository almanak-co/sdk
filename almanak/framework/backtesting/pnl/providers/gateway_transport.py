"""Gateway transports for runner-side backtest data lanes (ALM-2952).

The platform runner mounts no vendor keys (#3300 — they live on the gateway
sidecar), so direct-HTTP lanes there run keyless. Transports in this family
route those lanes through the gateway's vendor RPCs instead. Shared
semantics: activate only when a gateway host is configured
(``BacktestConfig.gateway_host``), first gRPC transport failure marks the
transport dead for the run (warn-once, callers fall back to direct HTTP),
application-level failures never fall back — a keyless retry cannot beat
the gateway's key.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

import grpc

from almanak.config.backtest import backtest_config_from_env

logger = logging.getLogger(__name__)

_DEFAULT_RPC_TIMEOUT = 30.0

# Status codes that mean the CHANNEL (not the request) is unusable — only
# these mark the transport dead. Everything else is an application-level
# answer from a healthy gateway: falling back to keyless direct HTTP could
# not do better, so those surface to the caller instead.
_TRANSPORT_DEAD_CODES = frozenset(
    {
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.CANCELLED,
        grpc.StatusCode.UNIMPLEMENTED,
        grpc.StatusCode.UNAUTHENTICATED,
        grpc.StatusCode.PERMISSION_DENIED,
    }
)


def gateway_backtest_configured() -> bool:
    """True when a gateway host is configured (not proven reachable)."""
    return bool(backtest_config_from_env().gateway_host)


class GatewayTransportBase:
    """Sticky-availability gateway connection shared by lane transports."""

    lane_label = "gateway"

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._handles: tuple[Any, Any] | None = None
        self._dead = False
        self._announced = False
        self._timeout = _DEFAULT_RPC_TIMEOUT

    async def _ensure(self) -> tuple[Any, Any] | None:
        """Return ``(client, gateway_pb2)`` connected, or None (marks dead)."""
        if self._dead:
            return None
        async with self._lock:
            if self._dead:
                return None
            if self._handles is not None:
                return self._handles
            try:
                from almanak.framework.gateway_client import get_gateway_client
                from almanak.gateway.proto import gateway_pb2
            except ImportError as exc:
                self._mark_dead(f"gateway client unavailable: {exc}")
                return None
            client = get_gateway_client()
            if not client.is_connected:
                try:
                    await asyncio.to_thread(client.connect)
                except Exception as exc:
                    self._mark_dead(f"gateway connect failed: {exc}")
                    return None
            self._timeout = float(getattr(getattr(client, "config", None), "timeout", None) or _DEFAULT_RPC_TIMEOUT)
            self._handles = (client, gateway_pb2)
            return self._handles

    async def _call_unary(self, method: Any, request: Any, *, rpc_name: str) -> tuple[Any | None, str | None]:
        """Invoke a unary RPC with a deadline and classified failure handling.

        Returns ``(response, None)`` on success and ``(None, detail)`` when a
        healthy gateway answered with an application-level error status.
        Channel-level failures (see ``_TRANSPORT_DEAD_CODES``) mark the
        transport dead and return ``(None, None)`` — the caller falls back.
        """
        try:
            response = await asyncio.to_thread(method, request, timeout=self._timeout)
        except grpc.RpcError as exc:
            code = exc.code() if callable(getattr(exc, "code", None)) else None
            details = exc.details() if callable(getattr(exc, "details", None)) else str(exc)
            if code is None or code in _TRANSPORT_DEAD_CODES:
                self._mark_dead(f"{rpc_name} RPC failed ({code}): {details}")
                return None, None
            return None, details or str(code)
        except Exception as exc:
            self._mark_dead(f"{rpc_name} RPC failed: {exc}")
            return None, None
        return response, None

    def _mark_dead(self, reason: str) -> None:
        self._dead = True
        self._handles = None
        logger.warning(
            "%s gateway transport unavailable — falling back to direct HTTP: %s",
            self.lane_label,
            reason,
        )

    def _announce_serving(self) -> None:
        if not self._announced:
            self._announced = True
            logger.info("%s lane served via gateway (transport=gateway_%s)", self.lane_label, self.lane_label.lower())


class GatewaySubgraphTransport(GatewayTransportBase):
    """Serves TheGraph GraphQL queries over the gateway's ``TheGraphQuery`` RPC.

    ``query_payload()`` returns the REST-shaped ``{"data": ..., "errors":
    [...]}`` body ``SubgraphClient`` already parses, or ``None`` when the
    gateway is unreachable (caller falls back to direct HTTP). GraphQL
    errors travel back in the payload so the client's single error
    classification applies to both transports.
    """

    lane_label = "TheGraph"

    async def query_payload(
        self,
        subgraph_id: str,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        if self._dead:
            return None
        handles = await self._ensure()
        if handles is None:
            return None
        client, pb2 = handles
        request = pb2.TheGraphQueryRequest(
            subgraph_id=subgraph_id,
            query=query,
            variables=json.dumps(variables) if variables else "",
        )
        response, app_error = await self._call_unary(
            client.integration.TheGraphQuery, request, rpc_name="TheGraphQuery"
        )
        if response is None:
            if app_error is not None:
                # Healthy gateway, failed query — surface through the caller's
                # normal GraphQL error classification, never the keyless fallback.
                return {"errors": [{"message": app_error}]}
            return None
        payload: dict[str, Any] = {}
        if response.data:
            payload["data"] = json.loads(response.data)
        if response.errors:
            payload["errors"] = json.loads(response.errors)
        elif not response.success:
            payload["errors"] = [{"message": "gateway returned success=false with no error detail"}]
        if not payload.get("errors"):
            self._announce_serving()
        return payload
