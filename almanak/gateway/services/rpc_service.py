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
            timeout = aiohttp.ClientTimeout(total=30.0)
            self._session = aiohttp.ClientSession(timeout=timeout)
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

    def _get_rpc_url(self, chain: str) -> str | None:
        """Get RPC URL for a chain.

        This function looks up the RPC URL with the API key from settings.
        Uses the network setting from GatewaySettings (mainnet or anvil).

        Args:
            chain: Chain name

        Returns:
            RPC URL or None if chain not configured

        Raises:
            Exception: Re-raises unexpected errors after logging
        """
        # Use the framework's get_rpc_url function which handles API key lookup
        from almanak.gateway.utils import get_rpc_url

        try:
            # Use network from settings (default: mainnet, can be set to anvil for testing)
            network = self.settings.network
            return get_rpc_url(chain, network=network)
        except ValueError as e:
            # ValueError is raised for unsupported chains or missing API keys
            logger.warning("Failed to get RPC URL for %s: %s", chain, e)
            return None
        except Exception:
            # Unexpected errors should be logged with full traceback and re-raised
            logger.exception("Unexpected error getting RPC URL for %s", chain)
            raise

    async def _make_rpc_call(
        self,
        rpc_url: str,
        method: str,
        params: list | dict,
        request_id: str,
    ) -> tuple[Any, dict | None]:
        """Make a single JSON-RPC call.

        Args:
            rpc_url: RPC endpoint URL
            method: JSON-RPC method
            params: JSON-RPC params
            request_id: Request ID for correlation

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

        try:
            async with session.post(
                rpc_url,
                json=payload,
                headers={"Content-Type": "application/json"},
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    return None, {"code": -32603, "message": f"HTTP {response.status}: {error_text}"}

                try:
                    data = await response.json()
                except (aiohttp.ContentTypeError, json.JSONDecodeError) as e:
                    return None, {"code": -32700, "message": f"Invalid JSON response: {e!s}"}

                if "error" in data:
                    return None, data["error"]

                return data.get("result"), None

        except aiohttp.ClientError as e:
            # Detect connections to localhost specifically so the error message is
            # actionable rather than a generic "Cannot connect".
            from urllib.parse import urlparse

            _hostname = urlparse(rpc_url).hostname or ""
            if _hostname in {"127.0.0.1", "localhost", "::1"}:
                return None, {
                    "code": -32603,
                    "message": (
                        f"Cannot connect to local RPC at {rpc_url}. "
                        "The local node process (Anvil or other) may not be running. "
                        f"Original error: {e!s}"
                    ),
                }
            return None, {"code": -32603, "message": f"Network error: {e!s}"}
        except TimeoutError:
            return None, {"code": -32603, "message": "Request timeout"}

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

        # Validate RPC method against allowlist (chain-aware: Solana vs EVM)
        try:
            validate_rpc_method(request.method, chain=chain)
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

        # Get RPC URL
        rpc_url = self._get_rpc_url(chain)
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
        result, error = await self._make_rpc_call(rpc_url, request.method, params, request.id)
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

    async def BatchCall(
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

        # Validate batch size
        try:
            validate_batch_size(list(request.requests))
        except ValidationError as e:
            self._metrics.failed_requests += num_requests
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.RpcBatchResponse(responses=[])

        # Validate all RPC methods in batch (chain-aware: Solana vs EVM)
        for rpc_request in request.requests:
            try:
                validate_rpc_method(rpc_request.method, chain=chain)
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

        # Get RPC URL
        rpc_url = self._get_rpc_url(chain)
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
        result, error = await self._make_rpc_call(rpc_url, "eth_call", params, "balance")
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
            "average_latency_ms": (
                self._metrics.total_latency_ms / self._metrics.successful_requests
                if self._metrics.successful_requests > 0
                else 0.0
            ),
        }
