"""RpcService implementation - JSON-RPC proxy for blockchain access.

This service proxies JSON-RPC requests to blockchain nodes, keeping API keys
secure in the gateway while allowing strategies to make arbitrary RPC calls.

Key features:
- Chain allowlist for security
- Rate limiting per chain
- Request validation
- Error handling with structured responses
"""

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import aiohttp
import grpc

from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.metrics import record_rpc_latency, record_rpc_request
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.validation import (
    ALLOWED_CHAINS,
    ValidationError,
    is_solana_chain,
    validate_address,
    validate_batch_size,
    validate_chain,
    validate_rpc_method,
)

logger = logging.getLogger(__name__)


# Rate limits per chain (requests per minute)
CHAIN_RATE_LIMITS = {
    "ethereum": 300,
    "arbitrum": 300,
    "base": 300,
    "optimism": 300,
    "polygon": 300,
    "avalanche": 300,
    "bsc": 300,
    "sonic": 300,
    "plasma": 300,
    "solana": 300,
}


@dataclass
class ChainRateLimiter:
    """Simple thread-safe rate limiter for a chain."""

    requests_per_minute: int
    request_times: list[float] = field(default_factory=list)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __init__(self, requests_per_minute: int):
        self.requests_per_minute = requests_per_minute
        self.request_times = []
        self._lock = asyncio.Lock()

    async def check_rate_limit(self, count: int = 1) -> tuple[bool, float]:
        """Check if rate limit would be exceeded for the given number of requests.

        Args:
            count: Number of requests to check (for batch operations)

        Returns:
            Tuple of (allowed, wait_time_seconds)
        """
        async with self._lock:
            # Reject oversized batches that can never fit within the rate limit
            if count > self.requests_per_minute:
                # Return a full window wait time since the batch is too large
                return False, 60.0

            now = datetime.now(UTC).timestamp()
            cutoff = now - 60.0  # 1 minute window

            # Remove old requests
            self.request_times = [t for t in self.request_times if t > cutoff]

            # Check if adding count requests would exceed the limit
            if len(self.request_times) + count > self.requests_per_minute:
                # Calculate wait time based on how many slots need to free up
                slots_needed = len(self.request_times) + count - self.requests_per_minute

                # If no recent requests, we should have capacity (edge case safety)
                if not self.request_times:
                    return True, 0.0

                # Sort times to find when enough slots will expire
                sorted_times = sorted(self.request_times)
                # The (slots_needed - 1)th oldest request determines when we have enough capacity
                expiration_index = min(slots_needed - 1, len(sorted_times) - 1)
                expiration_time = sorted_times[expiration_index]
                wait_time = expiration_time + 60.0 - now
                return False, max(0, wait_time)

            return True, 0.0

    async def record_request(self) -> None:
        """Record a request."""
        async with self._lock:
            self.request_times.append(datetime.now(UTC).timestamp())


@dataclass
class RpcMetrics:
    """Metrics for RPC service."""

    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    rate_limited_requests: int = 0
    total_latency_ms: float = 0.0
    # VIB-4985 / ALM-2777: bounded retries spent absorbing upstream
    # receipt-indexer lag (e.g. "Unknown block" on a pinned post-execution
    # read of a just-confirmed block). Lets us quantify provider lag per
    # deployment without parsing logs.
    indexer_lag_retries: int = 0


class RpcServiceServicer(gateway_pb2_grpc.RpcServiceServicer):
    """Implements RpcService gRPC interface.

    Proxies JSON-RPC requests to blockchain nodes while keeping API keys
    secure in the gateway. Supports both single and batch RPC calls.
    """

    def __init__(self, settings: GatewaySettings):
        """Initialize RpcService.

        Args:
            settings: Gateway settings with API keys
        """
        self.settings = settings
        self._session: aiohttp.ClientSession | None = None
        self._rate_limiters: dict[str, ChainRateLimiter] = {}
        self._metrics = RpcMetrics()

        logger.debug("Initialized RpcService with allowed chains: %s", ALLOWED_CHAINS)

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            from almanak.gateway.utils.ssl_context import build_ssl_context

            timeout = aiohttp.ClientTimeout(total=30.0)
            connector = aiohttp.TCPConnector(ssl=build_ssl_context())
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self._session

    async def close(self) -> None:
        """Close the HTTP session and release resources."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
            logger.info("RpcService HTTP session closed")

    def _get_rate_limiter(self, chain: str) -> ChainRateLimiter:
        """Get rate limiter for a chain."""
        if chain not in self._rate_limiters:
            limit = CHAIN_RATE_LIMITS.get(chain, 100)
            self._rate_limiters[chain] = ChainRateLimiter(limit)
        return self._rate_limiters[chain]

    def _chain_not_configured_error(self, chain: str) -> str | None:
        """Return an error message if chain is not in the gateway's configured list.

        When settings.chains is non-empty, reject RPC calls to other chains —
        otherwise a gateway started with ``--chains zerog`` would silently
        forward calls to whatever chain the CLI default happens to name
        (e.g. "arbitrum"), returning data from the wrong chain. Empty
        settings.chains = accept any chain (on-demand mode).

        Aliases are resolved on both sides (e.g. ``bsc`` matches a configured
        ``bnb``). ``settings.chains`` is canonicalized at load time by the
        Pydantic validator, but normalizing both sides at compare time also
        covers any path that constructs the settings without the validator.
        """
        if not self.settings.chains:
            return None

        from almanak.core.constants import resolve_chain_name

        def _canonical(name: object) -> str:
            # Defense-in-depth: if settings.chains is constructed/mutated
            # outside validation, a non-string entry must not raise. Return
            # an empty sentinel that won't match any real chain name.
            if not isinstance(name, str):
                return ""
            normalized = name.strip().lower()
            if not normalized:
                return ""
            try:
                return resolve_chain_name(normalized)
            except ValueError:
                return normalized

        request_canonical = _canonical(chain)
        # Fast path: settings.chains is already canonicalized by the validator
        # at construction time, so the common case avoids re-normalizing.
        if request_canonical in self.settings.chains:
            return None
        # Defense-in-depth: re-normalize on the configured side too in case some
        # path bypassed the validator (e.g. ``GatewaySettings.model_construct``
        # or post-construction mutation).
        if any(_canonical(c) == request_canonical for c in self.settings.chains):
            return None

        # Coerce to strings before sorting — settings.chains is normally
        # already a list[str] but the comparator above tolerates non-string
        # entries; the human-readable error must do the same.
        configured_strs = sorted(str(c) for c in self.settings.chains)
        configured = ", ".join(configured_strs)
        suggested = configured_strs[0] if configured_strs else chain
        return (
            f"Chain '{chain}' is not configured on this gateway. "
            f"Configured chains: [{configured}]. "
            f"Pass --chain {suggested} "
            f"or start the gateway with --chains {chain}."
        )

    def _get_rpc_url(self, chain: str, network_override: str | None = None) -> str | None:
        """Get RPC URL for a chain.

        This function looks up the RPC URL with the API key from settings.
        Uses the network setting from GatewaySettings (mainnet or anvil),
        unless a per-request network_override is provided.

        Args:
            chain: Chain name
            network_override: Optional per-request network override. When set,
                takes precedence over the gateway's default network setting.

        Returns:
            RPC URL or None if chain not configured

        Raises:
            Exception: Re-raises unexpected errors after logging
        """
        # Use the framework's get_rpc_url function which handles API key lookup
        from almanak.gateway.utils import get_rpc_url

        try:
            # Per-request override takes precedence over gateway default
            network = network_override or self.settings.network
            return get_rpc_url(chain, network=network)
        except ValueError as e:
            # ValueError is raised for unsupported chains or missing API keys
            logger.warning("Failed to get RPC URL for %s: %s", chain, e)
            return None
        except Exception:
            # Unexpected errors should be logged with full traceback and re-raised
            logger.exception("Unexpected error getting RPC URL for %s", chain)
            raise

    # Retry policy for transient upstream RPC failures.
    # VIB-2984: the original bug was a single Alchemy RPC hiccup crashing a
    # strategy. Retry 429 + 5xx + network errors with small exponential
    # backoff. Total worst case ~3.5s — fits under the 30s decide timeout.
    _RETRY_STATUSES: frozenset[int] = frozenset({429, 500, 502, 503, 504})
    _RETRY_MAX_ATTEMPTS: int = 3
    _RETRY_BASE_DELAY: float = 0.5
    _RETRY_MAX_AFTER: float = 5.0  # cap honored Retry-After header to avoid stalling decide loop

    # VIB-4985 / ALM-2777: receipt-indexer-lag retry.
    # A *pinned* post-execution read (block=receipt.block_number, VIB-4589/F7)
    # can race the upstream RPC's receipt indexer: the block is confirmed but
    # the node serving the eth_call has not ingested it yet, so it answers
    # "Unknown block" (as HTTP 400 OR a JSON-RPC error body). Without a retry
    # the read fails closed → empty post_state_json → the lending row drops to
    # confidence=ESTIMATED ("gateway read unavailable for this row").
    #
    # These markers describe exactly one condition — "the block/state you asked
    # for is not available on this node right now" — which for a just-confirmed
    # block means transient lag and is SAFE to retry. They deliberately do NOT
    # overlap with execution reverts ("execution reverted", "out of gas"), auth
    # ("unauthorized", "invalid api key"), or malformed params ("invalid
    # argument") — those must keep failing fast. Matched case-insensitively as
    # substrings against the upstream error message. JSON-RPC error CODE is not
    # used: providers reuse -32000 for reverts too, so code alone is too broad.
    _INDEXER_LAG_ERROR_MARKERS: frozenset[str] = frozenset(
        {
            "unknown block",  # geth / erigon / alchemy — block not yet on this node
            "header not found",  # geth — block header not yet available
            "missing trie node",  # geth archival — state for the block not yet available
            "block not found",  # erigon / nethermind / various providers
            "no state available for block",  # alchemy / erigon — state not yet indexed
        }
    )

    # Transaction-submission methods are NOT idempotent at the upstream layer.
    # Even if we get a 5xx back, the node may have already accepted and propagated
    # the signed tx. On EVM the nonce prevents replay cost (other than a wasted
    # second submission); on Solana the same signed blob is valid within the
    # recent-blockhash window (~2 min) and a retry can double-broadcast. Let
    # these errors surface to the tx submitter, which has nonce-aware retry.
    _NON_RETRYABLE_WRITE_METHODS: frozenset[str] = frozenset(
        {
            "eth_sendRawTransaction",
            "eth_sendTransaction",
            "sendTransaction",  # Solana
        }
    )

    async def _make_rpc_call(
        self,
        rpc_url: str,
        method: str,
        params: list | dict,
        request_id: str,
        chain: str | None = None,
    ) -> tuple[Any, dict | None]:
        """Make a single JSON-RPC call with bounded retries on transients.

        Retries HTTP 429 / 5xx responses and network errors (client disconnect,
        connection reset, timeout) with 0.5s base exponential backoff + jitter.
        Honors upstream ``Retry-After`` headers (capped to ``_RETRY_MAX_AFTER``).

        Also retries **receipt-indexer lag** (VIB-4985 / ALM-2777): a node that
        answers "Unknown block" / "header not found" / … for a *just-confirmed*
        block it has not ingested yet, delivered either as a non-2xx HTTP status
        OR as a JSON-RPC error body. Only the narrow lag-marker set
        (:attr:`_INDEXER_LAG_ERROR_MARKERS`) is retried — every other JSON-RPC
        error (reverts, auth, malformed params) still propagates immediately as
        a typed error, so a real failure is never masked by retries.

        Transaction-submission methods (``eth_sendRawTransaction``,
        ``eth_sendTransaction``, Solana ``sendTransaction``) are never retried:
        they are not idempotent at the upstream layer and a retry after a 5xx
        may double-broadcast the same signed transaction. (Their ``max_attempts``
        is 1, so the lag-retry guards below are also inert for them.)

        Args:
            rpc_url: RPC endpoint URL
            method: JSON-RPC method
            params: JSON-RPC params
            request_id: Request ID for correlation
            chain: Originating chain, for per-chain lag observability only
                (does not affect routing — ``rpc_url`` is already resolved).

        Returns:
            Tuple of (result, error)
        """
        session = await self._get_session()

        payload = {
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": request_id,
        }

        # Non-idempotent tx-submission methods get a single attempt.
        max_attempts = 1 if method in self._NON_RETRYABLE_WRITE_METHODS else self._RETRY_MAX_ATTEMPTS

        last_error: dict | None = None
        for attempt in range(1, max_attempts + 1):
            try:
                async with session.post(
                    rpc_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    if response.status in self._RETRY_STATUSES:
                        error_text = await response.text()
                        last_error = {
                            "code": -32603,
                            "message": f"HTTP {response.status}: {error_text}",
                        }
                        if attempt < max_attempts:
                            retry_after = self._parse_retry_after(response.headers.get("Retry-After"))
                            await self._retry_sleep(attempt, retry_after=retry_after)
                            continue
                        return None, last_error

                    if response.status != 200:
                        error_text = await response.text()
                        # VIB-4985: some providers wrap "Unknown block" indexer
                        # lag in a non-2xx (e.g. HTTP 400). Retry that family;
                        # any other non-2xx still fails fast.
                        if self._is_indexer_lag_error(error_text) and attempt < max_attempts:
                            self._record_indexer_lag_retry(method, chain, attempt, max_attempts, error_text)
                            await self._retry_sleep(attempt)
                            continue
                        return None, {"code": -32603, "message": f"HTTP {response.status}: {error_text}"}

                    try:
                        data = await response.json()
                    except (aiohttp.ContentTypeError, json.JSONDecodeError) as e:
                        return None, {"code": -32700, "message": f"Invalid JSON response: {e!s}"}

                    if "error" in data:
                        rpc_error = data["error"]
                        # VIB-4985: a JSON-RPC-level "Unknown block" on a pinned
                        # post-execution read is transient indexer lag — retry it.
                        # All other JSON-RPC errors (reverts, auth, bad params)
                        # propagate immediately as before.
                        error_message = rpc_error.get("message", "") if isinstance(rpc_error, dict) else ""
                        if self._is_indexer_lag_error(error_message) and attempt < max_attempts:
                            self._record_indexer_lag_retry(method, chain, attempt, max_attempts, error_message)
                            await self._retry_sleep(attempt)
                            continue
                        return None, rpc_error

                    return data.get("result"), None

            except aiohttp.ClientError as e:
                # Detect connections to localhost specifically so the error message is
                # actionable rather than a generic "Cannot connect".
                from urllib.parse import urlparse

                _hostname = urlparse(rpc_url).hostname or ""
                if _hostname in {"127.0.0.1", "localhost", "::1"}:
                    # Local Anvil not running — don't retry, the user needs to
                    # start it; retrying just delays the clear error message.
                    return None, {
                        "code": -32603,
                        "message": (
                            f"Cannot connect to local RPC at {rpc_url}. "
                            "The local node process (Anvil or other) may not be running. "
                            f"Original error: {e!s}"
                        ),
                    }
                last_error = {"code": -32603, "message": f"Network error: {e!s}"}
                if attempt < max_attempts:
                    await self._retry_sleep(attempt)
                    continue
                return None, last_error
            except TimeoutError:
                last_error = {"code": -32603, "message": "Request timeout"}
                if attempt < max_attempts:
                    await self._retry_sleep(attempt)
                    continue
                return None, last_error

        return None, last_error or {"code": -32603, "message": "RPC call failed after retries"}

    @classmethod
    def _is_indexer_lag_error(cls, message: str | None) -> bool:
        """True if ``message`` is an upstream "block not available yet" error.

        VIB-4985 / ALM-2777. Matches the narrow lag-marker set case-insensitively
        as substrings. Deliberately conservative: a non-string / empty / None
        message is NOT lag (returns False → fail fast), and the markers do not
        overlap with execution reverts, auth failures, or malformed-param errors.
        The ``isinstance`` guard tolerates a non-compliant provider that returns
        a non-string ``message`` field in its JSON-RPC error object — never crash
        the proxy on a malformed upstream response.
        """
        if not isinstance(message, str) or not message:
            return False
        lowered = message.lower()
        return any(marker in lowered for marker in cls._INDEXER_LAG_ERROR_MARKERS)

    def _record_indexer_lag_retry(
        self, method: str, chain: str | None, attempt: int, max_attempts: int, message: str
    ) -> None:
        """Count + log one receipt-indexer-lag retry (VIB-4985 / ALM-2777)."""
        self._metrics.indexer_lag_retries += 1
        logger.info(
            "RPC %s (chain=%s): upstream receipt-indexer lag, retrying %d/%d (%s)",
            method,
            chain or "unknown",
            attempt,
            max_attempts - 1,
            message.strip()[:160],
        )

    @staticmethod
    def _parse_retry_after(header: str | None) -> float | None:
        """Parse a ``Retry-After`` HTTP header value (delta-seconds form).

        Returns the delay in seconds, or ``None`` if the header is absent or
        unparseable. HTTP-date form is not supported — upstream RPC providers
        (Alchemy, QuickNode, Infura) all emit delta-seconds.
        """
        if not header:
            return None
        try:
            value = float(header.strip())
        except ValueError:
            return None
        return value if value >= 0 else None

    async def _retry_sleep(self, attempt: int, *, retry_after: float | None = None) -> None:
        """Sleep between retry attempts.

        Honors an upstream-supplied ``Retry-After`` when present (clamped to
        ``_RETRY_MAX_AFTER`` to keep total backoff bounded). Otherwise uses
        exponential backoff with 50%–150% jitter to avoid thundering-herd
        retries across a portfolio of strategies hitting the same upstream.
        """
        import asyncio
        import random

        if retry_after is not None:
            delay = min(retry_after, self._RETRY_MAX_AFTER)
        else:
            base = self._RETRY_BASE_DELAY * (2 ** (attempt - 1))
            delay = base * random.uniform(0.5, 1.5)
        await asyncio.sleep(delay)

    async def Call(
        self,
        request: gateway_pb2.RpcRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.RpcResponse:
        """Make a single JSON-RPC call.

        Args:
            request: RPC request with chain, method, params
            context: gRPC context

        Returns:
            RpcResponse with result or error
        """
        self._metrics.total_requests += 1

        # Validate chain
        try:
            chain = validate_chain(request.chain)
        except ValidationError as e:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.RpcResponse(
                success=False,
                error=json.dumps({"code": -32600, "message": str(e)}),
                id=request.id,
            )

        msg = self._chain_not_configured_error(chain)
        if msg is not None:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(msg)
            return gateway_pb2.RpcResponse(
                success=False,
                error=json.dumps({"code": -32603, "message": msg}),
                id=request.id,
            )

        # Resolve effective network so validation and execution use the same policy
        network_override = request.network if request.network else None
        effective_network = network_override or self.settings.network

        # Validate RPC method against allowlist (chain-aware: Solana vs EVM, network-aware: Anvil)
        try:
            validate_rpc_method(request.method, chain=chain, network=effective_network)
        except ValidationError as e:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.RpcResponse(
                success=False,
                error=json.dumps({"code": -32601, "message": str(e)}),
                id=request.id,
            )

        # Check rate limit
        limiter = self._get_rate_limiter(chain)
        allowed, wait_time = await limiter.check_rate_limit()
        if not allowed:
            self._metrics.rate_limited_requests += 1
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details(f"Rate limited, retry after {wait_time:.2f}s")
            return gateway_pb2.RpcResponse(
                success=False,
                error=json.dumps({"code": -32005, "message": f"Rate limited, retry after {wait_time:.2f}s"}),
                id=request.id,
            )

        # Get RPC URL (per-request network override takes precedence over gateway default)
        rpc_url = self._get_rpc_url(chain, network_override=network_override)
        if not rpc_url:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"Chain '{chain}' is not configured")
            return gateway_pb2.RpcResponse(
                success=False,
                error=json.dumps({"code": -32603, "message": f"Chain '{chain}' is not configured"}),
                id=request.id,
            )

        # Parse params
        try:
            params = json.loads(request.params) if request.params else []
        except json.JSONDecodeError:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("Invalid params JSON")
            return gateway_pb2.RpcResponse(
                success=False,
                error=json.dumps({"code": -32700, "message": "Invalid params JSON"}),
                id=request.id,
            )

        # Record request for rate limiting
        await limiter.record_request()

        # Make the call
        import time

        start_time = time.time()
        result, error = await self._make_rpc_call(rpc_url, request.method, params, request.id, chain=request.chain)
        latency_s = time.time() - start_time
        latency_ms = latency_s * 1000

        # Record Prometheus metrics
        record_rpc_request(chain, request.method)
        record_rpc_latency(chain, latency_s)

        if error:
            self._metrics.failed_requests += 1
            return gateway_pb2.RpcResponse(
                success=False,
                error=json.dumps(error),
                id=request.id,
            )

        # Only record latency for successful requests (matches division by successful_requests)
        self._metrics.total_latency_ms += latency_ms
        self._metrics.successful_requests += 1
        return gateway_pb2.RpcResponse(
            success=True,
            result=json.dumps(result) if result is not None else "",
            id=request.id,
        )

    async def BatchCall(  # noqa: C901
        self,
        request: gateway_pb2.RpcBatchRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.RpcBatchResponse:
        """Make a batch of JSON-RPC calls.

        Args:
            request: Batch request with chain and list of RPC requests
            context: gRPC context

        Returns:
            RpcBatchResponse with list of responses
        """
        # Count requests early for metrics accounting on early exits
        num_requests = len(request.requests)
        self._metrics.total_requests += num_requests

        # Validate chain
        try:
            chain = validate_chain(request.chain)
        except ValidationError as e:
            self._metrics.failed_requests += num_requests
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.RpcBatchResponse(responses=[])

        msg = self._chain_not_configured_error(chain)
        if msg is not None:
            self._metrics.failed_requests += num_requests
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(msg)
            return gateway_pb2.RpcBatchResponse(responses=[])

        # Validate batch size
        try:
            validate_batch_size(list(request.requests))
        except ValidationError as e:
            self._metrics.failed_requests += num_requests
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.RpcBatchResponse(responses=[])

        # Resolve effective network for the batch (reject mixed per-request overrides)
        batch_networks = {r.network.strip().lower() for r in request.requests if r.network}
        if len(batch_networks) > 1:
            self._metrics.failed_requests += num_requests
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details("All requests in a batch must use the same network override")
            return gateway_pb2.RpcBatchResponse(responses=[])
        network_override = next(iter(batch_networks), None)
        effective_network = network_override or self.settings.network

        # Validate all RPC methods in batch (chain-aware: Solana vs EVM, network-aware: Anvil)
        for rpc_request in request.requests:
            try:
                validate_rpc_method(rpc_request.method, chain=chain, network=effective_network)
            except ValidationError as e:
                self._metrics.failed_requests += num_requests
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details(f"Request {rpc_request.id}: {e}")
                return gateway_pb2.RpcBatchResponse(responses=[])

        # Check rate limit (count as multiple requests)
        limiter = self._get_rate_limiter(chain)
        allowed, wait_time = await limiter.check_rate_limit(num_requests)
        if not allowed:
            self._metrics.rate_limited_requests += num_requests
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details(f"Rate limited, retry after {wait_time:.2f}s")
            return gateway_pb2.RpcBatchResponse(responses=[])

        # Get RPC URL using the resolved network override
        rpc_url = self._get_rpc_url(chain, network_override=network_override)
        if not rpc_url:
            self._metrics.failed_requests += num_requests
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"Chain '{chain}' is not configured")
            return gateway_pb2.RpcBatchResponse(responses=[])

        # Record requests for rate limiting
        for _ in range(num_requests):
            await limiter.record_request()

        # Build coroutines/error placeholders and track request IDs for parallel execution
        # Error placeholders are used for requests with invalid JSON params
        request_ids: list[str] = []
        coros_or_errors: list = []
        for rpc_request in request.requests:
            request_ids.append(rpc_request.id)
            try:
                params = json.loads(rpc_request.params) if rpc_request.params else []
                coros_or_errors.append(self._make_rpc_call(rpc_url, rpc_request.method, params, rpc_request.id))
            except json.JSONDecodeError as e:
                # Store error placeholder as a tuple matching _make_rpc_call return format
                coros_or_errors.append((None, {"code": -32700, "message": f"Invalid params JSON: {e!s}"}))

        # Separate coroutines from error placeholders for asyncio.gather
        coro_indices: list[int] = []
        coros: list = []
        for i, item in enumerate(coros_or_errors):
            if asyncio.iscoroutine(item):
                coro_indices.append(i)
                coros.append(item)

        # Execute all RPC coroutines concurrently, capturing exceptions per-request
        coro_results = await asyncio.gather(*coros, return_exceptions=True) if coros else []

        # Merge coroutine results back into the full results list
        results: list = list(coros_or_errors)  # Start with error placeholders in place
        for idx, coro_result in zip(coro_indices, coro_results, strict=False):
            results[idx] = coro_result

        # Build responses from gathered results
        responses = []
        for request_id, result_item in zip(request_ids, results, strict=False):
            # Handle exceptions from asyncio.gather
            if isinstance(result_item, Exception):
                self._metrics.failed_requests += 1
                responses.append(
                    gateway_pb2.RpcResponse(
                        success=False,
                        error=json.dumps({"code": -32603, "message": f"Internal error: {result_item!s}"}),
                        id=request_id,
                    )
                )
            else:
                # result_item is a tuple of (result, error) from _make_rpc_call or error placeholder
                result, error = result_item
                if error:
                    self._metrics.failed_requests += 1
                    responses.append(
                        gateway_pb2.RpcResponse(
                            success=False,
                            error=json.dumps(error),
                            id=request_id,
                        )
                    )
                else:
                    self._metrics.successful_requests += 1
                    responses.append(
                        gateway_pb2.RpcResponse(
                            success=True,
                            result=json.dumps(result) if result is not None else "",
                            id=request_id,
                        )
                    )

        return gateway_pb2.RpcBatchResponse(responses=responses)

    async def QueryAllowance(
        self,
        request: gateway_pb2.AllowanceRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.AllowanceResponse:
        """Query ERC-20 allowance for a token/owner/spender.

        For Solana chains, returns max allowance since SPL tokens don't use
        the ERC-20 approve/allowance pattern.

        Args:
            request: Allowance query with chain, token, owner, spender
            context: gRPC context

        Returns:
            AllowanceResponse with allowance in wei
        """
        self._metrics.total_requests += 1

        # Validate chain
        try:
            chain = validate_chain(request.chain)
        except ValidationError as e:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.AllowanceResponse(success=False, error=str(e))

        msg = self._chain_not_configured_error(chain)
        if msg is not None:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(msg)
            return gateway_pb2.AllowanceResponse(success=False, error=msg)

        # Solana SPL tokens don't use ERC-20 allowances — return max (always approved)
        if is_solana_chain(chain):
            self._metrics.successful_requests += 1
            return gateway_pb2.AllowanceResponse(success=True, allowance=str(2**64 - 1))

        # Validate addresses
        try:
            token_address = validate_address(request.token_address, "token_address")
            owner_address = validate_address(request.owner_address, "owner_address")
            spender_address = validate_address(request.spender_address, "spender_address")
        except ValidationError as e:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.AllowanceResponse(success=False, error=str(e))

        # Check rate limit
        limiter = self._get_rate_limiter(chain)
        allowed, wait_time = await limiter.check_rate_limit()
        if not allowed:
            self._metrics.rate_limited_requests += 1
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details(f"Rate limited, retry after {wait_time:.2f}s")
            return gateway_pb2.AllowanceResponse(success=False, error=f"Rate limited, retry after {wait_time:.2f}s")

        # Get RPC URL
        rpc_url = self._get_rpc_url(chain)
        if not rpc_url:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"Chain '{chain}' is not configured")
            return gateway_pb2.AllowanceResponse(success=False, error=f"Chain '{chain}' is not configured")

        # Build eth_call params for allowance(owner, spender)
        # ERC-20 allowance selector: 0xdd62ed3e
        owner_padded = owner_address.lower().replace("0x", "").zfill(64)
        spender_padded = spender_address.lower().replace("0x", "").zfill(64)
        calldata = "0xdd62ed3e" + owner_padded + spender_padded

        params = [
            {"to": token_address, "data": calldata},
            "latest",
        ]

        # Record request for rate limiting
        await limiter.record_request()

        # Make the call with metrics
        import time

        start_time = time.time()
        result, error = await self._make_rpc_call(rpc_url, "eth_call", params, "allowance")
        latency_s = time.time() - start_time

        # Record Prometheus metrics
        record_rpc_request(chain, "eth_call")
        record_rpc_latency(chain, latency_s)

        if error:
            self._metrics.failed_requests += 1
            return gateway_pb2.AllowanceResponse(success=False, error=error.get("message", "RPC call failed"))

        # Parse hex result to decimal string
        try:
            allowance = int(result, 16) if result else 0
            self._metrics.successful_requests += 1
            return gateway_pb2.AllowanceResponse(success=True, allowance=str(allowance))
        except (ValueError, TypeError) as e:
            self._metrics.failed_requests += 1
            return gateway_pb2.AllowanceResponse(success=False, error=f"Failed to parse allowance: {e}")

    async def QueryBalance(
        self,
        request: gateway_pb2.BalanceQueryRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.BalanceQueryResponse:
        """Query ERC-20 balance for a token/wallet.

        This is an EVM-only convenience method using eth_call. For Solana,
        use MarketService.GetBalance() instead which routes to SolanaBalanceProvider.

        Args:
            request: Balance query with chain, token, wallet
            context: gRPC context

        Returns:
            BalanceQueryResponse with balance in wei
        """
        self._metrics.total_requests += 1

        # Validate chain
        try:
            chain = validate_chain(request.chain)
        except ValidationError as e:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BalanceQueryResponse(success=False, error=str(e))

        msg = self._chain_not_configured_error(chain)
        if msg is not None:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(msg)
            return gateway_pb2.BalanceQueryResponse(success=False, error=msg)

        # Solana doesn't support ERC-20 eth_call queries — use MarketService.GetBalance()
        if is_solana_chain(chain):
            self._metrics.successful_requests += 1
            return gateway_pb2.BalanceQueryResponse(
                success=False, error="Use MarketService.GetBalance() for Solana token balances"
            )

        # Validate addresses
        try:
            token_address = validate_address(request.token_address, "token_address")
            wallet_address = validate_address(request.wallet_address, "wallet_address")
        except ValidationError as e:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.BalanceQueryResponse(success=False, error=str(e))

        # Check rate limit
        limiter = self._get_rate_limiter(chain)
        allowed, wait_time = await limiter.check_rate_limit()
        if not allowed:
            self._metrics.rate_limited_requests += 1
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details(f"Rate limited, retry after {wait_time:.2f}s")
            return gateway_pb2.BalanceQueryResponse(success=False, error=f"Rate limited, retry after {wait_time:.2f}s")

        # Get RPC URL
        rpc_url = self._get_rpc_url(chain)
        if not rpc_url:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"Chain '{chain}' is not configured")
            return gateway_pb2.BalanceQueryResponse(success=False, error=f"Chain '{chain}' is not configured")

        # Build eth_call params for balanceOf(address)
        # ERC-20 balanceOf selector: 0x70a08231
        wallet_padded = wallet_address.lower().replace("0x", "").zfill(64)
        calldata = "0x70a08231" + wallet_padded

        params = [
            {"to": token_address, "data": calldata},
            "latest",
        ]

        # Record request for rate limiting
        await limiter.record_request()

        # Make the call with metrics
        import time

        start_time = time.time()
        result, error = await self._make_rpc_call(rpc_url, "eth_call", params, "balance", chain=request.chain)
        latency_s = time.time() - start_time

        # Record Prometheus metrics
        record_rpc_request(chain, "eth_call")
        record_rpc_latency(chain, latency_s)

        if error:
            self._metrics.failed_requests += 1
            return gateway_pb2.BalanceQueryResponse(success=False, error=error.get("message", "RPC call failed"))

        # Parse hex result to decimal string
        try:
            balance = int(result, 16) if result else 0
            self._metrics.successful_requests += 1
            return gateway_pb2.BalanceQueryResponse(success=True, balance=str(balance))
        except (ValueError, TypeError) as e:
            self._metrics.failed_requests += 1
            return gateway_pb2.BalanceQueryResponse(success=False, error=f"Failed to parse balance: {e}")

    async def QueryPositionLiquidity(
        self,
        request: gateway_pb2.PositionLiquidityRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PositionLiquidityResponse:
        """Query Uniswap V3 position liquidity.

        EVM-only: Solana LP positions use program-specific queries, not eth_call.

        Args:
            request: Position query with chain, position_manager, token_id
            context: gRPC context

        Returns:
            PositionLiquidityResponse with liquidity value
        """
        self._metrics.total_requests += 1

        # Validate chain
        try:
            chain = validate_chain(request.chain)
        except ValidationError as e:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.PositionLiquidityResponse(success=False, error=str(e))

        msg = self._chain_not_configured_error(chain)
        if msg is not None:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(msg)
            return gateway_pb2.PositionLiquidityResponse(success=False, error=msg)

        # Solana LP positions don't use Uniswap V3 position manager
        if is_solana_chain(chain):
            self._metrics.successful_requests += 1
            return gateway_pb2.PositionLiquidityResponse(
                success=False, error="Position liquidity queries not applicable for Solana"
            )

        # Validate address
        try:
            position_manager = validate_address(request.position_manager, "position_manager")
        except ValidationError as e:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.PositionLiquidityResponse(success=False, error=str(e))

        # Validate token_id is non-negative
        if request.token_id < 0:
            error_msg = "token_id must be a non-negative integer"
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error_msg)
            return gateway_pb2.PositionLiquidityResponse(success=False, error=error_msg)

        # Check rate limit
        limiter = self._get_rate_limiter(chain)
        allowed, wait_time = await limiter.check_rate_limit()
        if not allowed:
            self._metrics.rate_limited_requests += 1
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details(f"Rate limited, retry after {wait_time:.2f}s")
            return gateway_pb2.PositionLiquidityResponse(
                success=False, error=f"Rate limited, retry after {wait_time:.2f}s"
            )

        # Get RPC URL
        rpc_url = self._get_rpc_url(chain)
        if not rpc_url:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"Chain '{chain}' is not configured")
            return gateway_pb2.PositionLiquidityResponse(success=False, error=f"Chain '{chain}' is not configured")

        # Build eth_call params for positions(uint256)
        # Uniswap V3 positions selector: 0x99fbab88
        token_id_hex = hex(request.token_id)[2:].zfill(64)
        calldata = "0x99fbab88" + token_id_hex

        params = [
            {"to": position_manager, "data": calldata},
            "latest",
        ]

        # Record request for rate limiting
        await limiter.record_request()

        # Make the call with metrics
        import time

        start_time = time.time()
        result, error = await self._make_rpc_call(rpc_url, "eth_call", params, "position")
        latency_s = time.time() - start_time

        # Record Prometheus metrics
        record_rpc_request(chain, "eth_call")
        record_rpc_latency(chain, latency_s)

        if error:
            self._metrics.failed_requests += 1
            return gateway_pb2.PositionLiquidityResponse(success=False, error=error.get("message", "RPC call failed"))

        # Parse result - liquidity is at offset 7 * 32 = 224 bytes (index 7 in tuple)
        # Position struct: nonce, operator, token0, token1, fee, tickLower, tickUpper, liquidity, ...
        try:
            if not result or result == "0x":
                self._metrics.failed_requests += 1
                return gateway_pb2.PositionLiquidityResponse(success=False, error="Position not found")

            # Remove 0x prefix and decode
            hex_data = result[2:] if result.startswith("0x") else result
            if len(hex_data) < 512:  # 8 * 64 hex chars minimum
                self._metrics.failed_requests += 1
                return gateway_pb2.PositionLiquidityResponse(
                    success=False, error=f"Unexpected result length: {len(hex_data)}"
                )

            # Liquidity is at word 7 (0-indexed), each word is 64 hex chars
            liquidity_hex = hex_data[7 * 64 : 8 * 64]
            liquidity = int(liquidity_hex, 16)
            self._metrics.successful_requests += 1
            return gateway_pb2.PositionLiquidityResponse(success=True, liquidity=str(liquidity))
        except (ValueError, TypeError) as e:
            self._metrics.failed_requests += 1
            return gateway_pb2.PositionLiquidityResponse(success=False, error=f"Failed to parse liquidity: {e}")

    async def QueryPositionTokensOwed(
        self,
        request: gateway_pb2.PositionTokensOwedRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.PositionTokensOwedResponse:
        """Query Uniswap V3 position tokens owed (fees + withdrawn liquidity).

        EVM-only: Solana LP positions use program-specific queries.

        Args:
            request: Position query with chain, position_manager, token_id
            context: gRPC context

        Returns:
            PositionTokensOwedResponse with tokensOwed0 and tokensOwed1 values
        """
        self._metrics.total_requests += 1

        # Validate chain
        try:
            chain = validate_chain(request.chain)
        except ValidationError as e:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.PositionTokensOwedResponse(success=False, error=str(e))

        msg = self._chain_not_configured_error(chain)
        if msg is not None:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(msg)
            return gateway_pb2.PositionTokensOwedResponse(success=False, error=msg)

        # Solana LP positions don't use Uniswap V3 position manager
        if is_solana_chain(chain):
            self._metrics.successful_requests += 1
            return gateway_pb2.PositionTokensOwedResponse(
                success=False, error="Position tokens owed queries not applicable for Solana"
            )

        # Validate address
        try:
            position_manager = validate_address(request.position_manager, "position_manager")
        except ValidationError as e:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.PositionTokensOwedResponse(success=False, error=str(e))

        # Validate token_id is non-negative
        if request.token_id < 0:
            error_msg = "token_id must be a non-negative integer"
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error_msg)
            return gateway_pb2.PositionTokensOwedResponse(success=False, error=error_msg)

        # Check rate limit
        limiter = self._get_rate_limiter(chain)
        allowed, wait_time = await limiter.check_rate_limit()
        if not allowed:
            self._metrics.rate_limited_requests += 1
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details(f"Rate limited, retry after {wait_time:.2f}s")
            return gateway_pb2.PositionTokensOwedResponse(
                success=False, error=f"Rate limited, retry after {wait_time:.2f}s"
            )

        # Get RPC URL
        rpc_url = self._get_rpc_url(chain)
        if not rpc_url:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"Chain '{chain}' is not configured")
            return gateway_pb2.PositionTokensOwedResponse(success=False, error=f"Chain '{chain}' is not configured")

        # Build eth_call params for positions(uint256)
        # Uniswap V3 positions selector: 0x99fbab88
        token_id_hex = hex(request.token_id)[2:].zfill(64)
        calldata = "0x99fbab88" + token_id_hex

        params = [
            {"to": position_manager, "data": calldata},
            "latest",
        ]

        # Record request for rate limiting
        await limiter.record_request()

        # Make the call with metrics
        import time

        start_time = time.time()
        result, error = await self._make_rpc_call(rpc_url, "eth_call", params, "position_tokens_owed")
        latency_s = time.time() - start_time

        # Record Prometheus metrics
        record_rpc_request(chain, "eth_call")
        record_rpc_latency(chain, latency_s)

        if error:
            self._metrics.failed_requests += 1
            return gateway_pb2.PositionTokensOwedResponse(success=False, error=error.get("message", "RPC call failed"))

        # Parse result - tokensOwed0 is at offset 10 * 32 = 320 bytes, tokensOwed1 at 11 * 32 = 352 bytes
        # Position struct: nonce, operator, token0, token1, fee, tickLower, tickUpper, liquidity, feeGrowthInside0LastX128, feeGrowthInside1LastX128, tokensOwed0, tokensOwed1, ...
        try:
            if not result or result == "0x":
                self._metrics.failed_requests += 1
                return gateway_pb2.PositionTokensOwedResponse(success=False, error="Position not found")

            # Remove 0x prefix and decode
            hex_data = result[2:] if result.startswith("0x") else result
            if len(hex_data) < 768:  # 12 * 64 hex chars minimum (need 12 words)
                self._metrics.failed_requests += 1
                return gateway_pb2.PositionTokensOwedResponse(
                    success=False, error=f"Unexpected result length: {len(hex_data)}"
                )

            # tokensOwed0 is at word 10 (0-indexed), tokensOwed1 at word 11, each word is 64 hex chars
            tokens_owed0_hex = hex_data[10 * 64 : 11 * 64]
            tokens_owed1_hex = hex_data[11 * 64 : 12 * 64]
            tokens_owed0 = int(tokens_owed0_hex, 16)
            tokens_owed1 = int(tokens_owed1_hex, 16)
            self._metrics.successful_requests += 1
            return gateway_pb2.PositionTokensOwedResponse(
                success=True, tokens_owed0=str(tokens_owed0), tokens_owed1=str(tokens_owed1)
            )
        except (ValueError, TypeError) as e:
            self._metrics.failed_requests += 1
            return gateway_pb2.PositionTokensOwedResponse(success=False, error=f"Failed to parse tokens owed: {e}")

    def get_metrics(self) -> dict:
        """Get RPC service metrics."""
        return {
            "total_requests": self._metrics.total_requests,
            "successful_requests": self._metrics.successful_requests,
            "failed_requests": self._metrics.failed_requests,
            "rate_limited_requests": self._metrics.rate_limited_requests,
            # VIB-4985 / ALM-2777: exposed so deployment metrics callers can
            # quantify upstream receipt-indexer lag per deployment without
            # parsing logs.
            "indexer_lag_retries": self._metrics.indexer_lag_retries,
            "average_latency_ms": (
                self._metrics.total_latency_ms / self._metrics.successful_requests
                if self._metrics.successful_requests > 0
                else 0.0
            ),
        }
