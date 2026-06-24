"""Gateway-native eth_call transport for the connector PT-price reader (VIB-5348).

The gateway's ``MarketService.GetPtPrice`` prices a Pendle PT by reading
``pt_to_asset_rate`` / ``expiry`` on-chain through the connector's
:class:`~almanak.connectors.pendle.on_chain_reader.PendleOnChainReader`. That
reader supports two transports:

* **gateway mode** — every read routes through an injected client's
  ``client.rpc.Call(RpcRequest, timeout=...)`` and reads ``.success`` /
  ``.result`` / ``.error`` off the response. ``reader.web3 is None`` — no web3
  provider is ever instantiated.
* **direct mode** — a raw ``Web3(Web3.HTTPProvider(rpc_url))`` (the
  ``# vib-2986-exempt`` migration-debt path).

Before VIB-5348 the gateway built the reader in **direct mode** (passing an
``rpc_url``), so the hosted security perimeter instantiated a raw HTTPProvider
on its PT-price path. This module supplies a **gateway-native** client that
duck-types the reader's ``client.rpc.Call`` seam and is backed by the gateway's
own async ``aiohttp`` eth_call — the SAME audited egress transport already used
by :mod:`almanak.gateway.data.price.onchain` and
:mod:`almanak.gateway.services.rpc_service`. The reader then runs in its
already-implemented gateway mode with ``web3 = None`` and **no HTTPProvider**.

Why this module lives under ``almanak/gateway/`` and not the connector's
``pendle/gateway/`` subpackage: ``almanak/gateway/`` IS the egress layer
(blueprint 20 §Milestone 5.5). The connector-boundary egress scan
(``scripts/ci/check_connector_gateway_compliance.sh``) only scans
``almanak/connectors/``, so an ``aiohttp.ClientSession`` here needs no
``# vib-2986-exempt`` marker — exactly like ``rpc_service.py`` and
``onchain.py``. Putting the transport gateway-side adds ZERO new exempt markers.

The reader's synchronous read methods are driven off the asyncio event loop by
``MarketService`` via ``asyncio.to_thread`` (perimeter liveness). This module's
:class:`GatewayPtRpcClient` therefore runs each async eth_call to completion on
that worker thread with :func:`asyncio.run` — safe because a ``to_thread``
worker has no running event loop. At most a handful of eth_calls run per
``GetPtPrice`` (oracle-state, pt-rate, expiry), so a fresh per-call loop is
negligible against the RPC round-trip and avoids any cross-thread loop
lifecycle.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

import aiohttp

from almanak.gateway.utils import get_rpc_url
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)

# Bound on each gateway-driven PT on-chain read. Mirrors the gateway's own
# eth_call timeout (RpcService and OnChainPriceSource both budget on the same
# order) so a hung RPC can't hold the worker thread (and the gRPC handler it was
# dispatched from via ``asyncio.to_thread``) open forever.
GATEWAY_PT_RPC_TIMEOUT_SECONDS = 30.0


class GatewayPtRpcError(Exception):
    """Raised when the gateway-native eth_call transport fails.

    Surfaced to the reader as a failed ``rpc.Call`` (``success=False``), which
    the reader maps to its own ``PendleOnChainError`` — preserving the
    ``rate is None → UNMEASURED`` (Empty≠Zero) contract end to end.
    """


@dataclass(frozen=True)
class _RpcResult:
    """Duck-types the ``gateway_pb2.RpcResponse`` fields the reader reads.

    The reader only ever touches ``.success`` / ``.result`` / ``.error`` and
    ``json.loads(.result)``; this struct mirrors exactly that surface so the
    reader's gateway-mode branch runs unchanged.
    """

    success: bool
    result: str  # JSON-encoded hex string, matching RpcResponse.result semantics
    error: str = ""


class _NativeEthCall:
    """Self-contained async ``eth_call`` over ``aiohttp`` (gateway egress layer).

    A trimmed analogue of :meth:`OnChainPriceSource._eth_call`: bounded
    :class:`aiohttp.ClientTimeout`, JSON-RPC error / empty-result raising. The
    RPC URL is resolved once from the gateway's credentials.

    The ``aiohttp`` session is created and closed **inside each ``eth_call``**
    rather than cached across calls. This is deliberate: each call is driven by
    a fresh :func:`asyncio.run` loop (see :func:`_run_sync`), and an
    ``aiohttp.ClientSession`` is bound to the loop that created it — reusing a
    cached session under a later loop raises ``RuntimeError: Event loop is
    closed``. At most a handful of eth_calls run per ``GetPtPrice``, so a
    per-call session is correct and cheap.
    """

    def __init__(self, chain: str, network: str, request_timeout: float) -> None:
        self._chain = chain.lower()
        self._network = network
        self._request_timeout = request_timeout
        self._rpc_request_id = 0
        # Resolve once; a missing URL is a hard failure for this transport (the
        # gateway is supposed to hold creds), surfaced when a call is attempted.
        try:
            self._rpc_url: str | None = get_rpc_url(self._chain, network=self._network)
        except ValueError as e:
            self._rpc_url = None
            logger.warning(
                "GatewayPtRpcClient: no RPC URL for chain=%s network=%s — PT on-chain reads unavailable: %s",
                self._chain,
                self._network,
                e,
            )

    async def eth_call(self, to: str, data: str) -> str:
        """Run a single ``eth_call`` against ``latest`` and return the hex result.

        Opens and closes its own ``aiohttp`` session within this coroutine so the
        session never outlives the per-call event loop.

        Raises:
            GatewayPtRpcError: on missing RPC URL, HTTP error, JSON-RPC error
                object, or an empty (``0x``) result — the same failure surface
                the reader's gateway-mode branch expects to map to UNMEASURED.
        """
        if not self._rpc_url:
            raise GatewayPtRpcError(f"no RPC URL configured for chain={self._chain}")

        self._rpc_request_id += 1
        request_id = self._rpc_request_id
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": to, "data": data}, "latest"],
            "id": request_id,
        }
        timeout = aiohttp.ClientTimeout(total=self._request_timeout)
        connector = aiohttp.TCPConnector(ssl=build_ssl_context())
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(self._rpc_url, json=payload, timeout=timeout) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        raise GatewayPtRpcError(f"RPC HTTP {resp.status} (id={request_id}, to={to}): {text[:200]}")
                    body = await resp.json()
        except GatewayPtRpcError:
            raise
        except Exception as e:  # network error, timeout, malformed JSON
            raise GatewayPtRpcError(f"eth_call transport error (id={request_id}, to={to}): {e}") from e

        if "error" in body:
            raise GatewayPtRpcError(f"JSON-RPC error (id={request_id}, to={to}): {body['error']}")

        result = body.get("result", "0x")
        if result in ("0x", "0x0", None):
            raise GatewayPtRpcError(f"empty eth_call result (id={request_id}, to={to})")
        return result


class _GatewayNativeRpc:
    """Duck-types the reader's ``client.rpc`` — only ``.Call`` is exercised.

    The reader builds a ``gateway_pb2.RpcRequest`` with ``.method == "eth_call"``
    and ``.params == json.dumps([{"to","data"}, "latest"])`` and calls
    ``client.rpc.Call(req, timeout=...)`` (see
    ``PendleOnChainReader._gateway_eth_call``). We unpack that request, run the
    gateway-native async ``eth_call`` to completion on the current (worker)
    thread, and return an :class:`_RpcResult` shaped like ``RpcResponse``.
    """

    def __init__(self, source: _NativeEthCall) -> None:
        self._source = source

    def Call(self, request: Any, timeout: float = 30.0) -> _RpcResult:  # noqa: N802 — mirrors the gRPC stub
        method = getattr(request, "method", "")
        if method != "eth_call":
            # The PT reader only ever issues eth_call; anything else is a wiring
            # bug, surfaced as a failed call (never a silent wrong answer).
            return _RpcResult(success=False, result="", error=f"unsupported RPC method for PT reader: {method!r}")
        try:
            parsed = json.loads(request.params)
            call = parsed[0]
            to = call["to"]
            data = call["data"]
        except (ValueError, KeyError, IndexError, TypeError) as e:
            return _RpcResult(success=False, result="", error=f"malformed eth_call params: {e}")

        try:
            hex_result = _run_sync(self._source.eth_call(to, data))
        except Exception as e:  # GatewayPtRpcError + any defensive leak
            return _RpcResult(success=False, result="", error=str(e))

        # The reader does ``json.loads(resp.result)`` and expects a hex string,
        # mirroring how the real gateway encodes RpcResponse.result.
        return _RpcResult(success=True, result=json.dumps(hex_result), error="")


class GatewayPtRpcClient:
    """Gateway-native stand-in for the strategy-side ``GatewayClient``.

    Exposes the single ``.rpc.Call`` seam the
    :class:`~almanak.connectors.pendle.on_chain_reader.PendleOnChainReader`
    needs in gateway mode, backed by the gateway's own audited ``aiohttp``
    eth_call. Injected into the reader's ``gateway_client=`` parameter so the
    reader runs with ``web3 = None`` — NO raw ``HTTPProvider`` on the perimeter.
    """

    def __init__(self, *, chain: str, network: str, request_timeout: float = GATEWAY_PT_RPC_TIMEOUT_SECONDS) -> None:
        self._source = _NativeEthCall(chain=chain, network=network, request_timeout=request_timeout)
        self.rpc = _GatewayNativeRpc(self._source)


def _run_sync(coro: Any) -> Any:
    """Run an async coroutine to completion from a thread with no running loop.

    Safe because ``MarketService`` drives the reader via ``asyncio.to_thread``;
    a worker thread has no running event loop, so :func:`asyncio.run` creates,
    runs, and tears down a fresh loop per call. A small number of eth_calls run
    per ``GetPtPrice``, so per-call loop creation is negligible against the RPC
    round-trip and avoids any cross-thread loop lifecycle.
    """
    return asyncio.run(coro)


__all__ = ["GatewayPtRpcClient", "GatewayPtRpcError"]
