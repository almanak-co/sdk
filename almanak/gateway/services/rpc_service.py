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
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import aiohttp
import grpc
from eth_utils import keccak

from almanak.core.chains._helpers import rpc_rate_limit_map
from almanak.gateway.core.settings import GatewaySettings
from almanak.gateway.metrics import record_rpc_latency, record_rpc_request
from almanak.gateway.proto import gateway_pb2, gateway_pb2_grpc
from almanak.gateway.utils.indexer_lag import INDEXER_LAG_ERROR_MARKERS, is_indexer_lag_error
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


# Rate limits per chain (requests per minute). Derived from
# ``ChainDescriptor.rpc.rate_limit_rpm`` (VIB-4851 CS-3); chains with no
# declared budget keep falling back to the conservative default at the
# lookup site, byte-equivalent to the legacy literal dict.
CHAIN_RATE_LIMITS: Mapping[str, int] = rpc_rate_limit_map()


# StateView.getSlot0(bytes32 poolId) selector — keccak256("getSlot0(bytes32)")[:4].
# The deployed V4 StateView takes the PoolId (bytes32 = keccak256(abi.encode(PoolKey))),
# NOT a PoolKey tuple: the tuple-arg selector (0xe924c4df) reverts with "no data"
# on-chain. Validated against live Base positions (VIB-5024). poolId is computed
# in-servicer via keccak of the ABI-encoded PoolKey words returned by
# getPoolAndPositionInfo (abi.encode of the static PoolKey struct == those 5 words).
_GET_SLOT0_SELECTOR = "c815641c"


def _strip_0x(hex_result: object) -> str:
    """Normalize an eth_call result to a 0x-less lowercase hex string, or raise."""
    if not isinstance(hex_result, str) or not hex_result or hex_result == "0x":
        raise ValueError(f"empty or non-hex result: {hex_result!r}")
    clean = hex_result[2:] if hex_result.startswith(("0x", "0X")) else hex_result
    return clean.lower()


def _decode_uint_word(hex_data: str, word_index: int) -> int:
    """Decode the uint256 at ``word_index`` (each word is 64 hex chars)."""
    start = word_index * 64
    word = hex_data[start : start + 64]
    if len(word) != 64:
        raise ValueError(f"missing word {word_index} in {len(hex_data)}-char payload")
    return int(word, 16)


def _as_int24(value: int) -> int:
    """Interpret the low 24 bits of ``value`` as a signed int24 (two's complement)."""
    value &= (1 << 24) - 1
    if value >= (1 << 23):
        value -= 1 << 24
    return value


def _decode_v4_liquidity(hex_result: object) -> int:
    """Decode ``PositionManager.getPositionLiquidity(uint256)`` → liquidity (uint128)."""
    hex_data = _strip_0x(hex_result)
    return _decode_uint_word(hex_data, 0)


def _decode_v4_pool_and_position_info(hex_result: object) -> tuple[str, int, int, str]:
    """Decode ``PositionManager.getPoolAndPositionInfo(uint256)``.

    Returns ``(pool_key_words, tick_lower, tick_upper, pool_id)`` where:

    - ``pool_key_words`` is the 5-word (320 hex char) ABI head of the returned
      ``PoolKey`` struct, reused verbatim as the ``getSlot0`` argument.
    - ``tick_lower`` / ``tick_upper`` are decoded from the packed ``PositionInfo``
      uint256 (v4-periphery ``PositionInfoLibrary``): bits 8-31 = tickLower,
      bits 32-55 = tickUpper, bits 56-255 = truncated poolId.
    - ``pool_id`` is the 0x-prefixed truncated poolId (top 200 bits → 25 bytes,
      right-padded to 32 for an identity cross-check; the framework only
      prefix-compares it).

    The ABI return is ``(PoolKey poolKey, uint256 info)``. ``PoolKey`` has no
    dynamic fields, so it is encoded inline as 5 words; ``info`` is word 5.
    """
    hex_data = _strip_0x(hex_result)
    if len(hex_data) < 6 * 64:
        raise ValueError(f"getPoolAndPositionInfo payload too short: {len(hex_data)} hex chars")
    pool_key_words = hex_data[0 : 5 * 64]
    info = _decode_uint_word(hex_data, 5)
    tick_lower = _as_int24(info >> 8)
    tick_upper = _as_int24(info >> 32)
    pool_id_top = info >> 56  # top 200 bits = truncated poolId
    # Right-pad the 25-byte truncated id to 32 bytes for a stable 64-hex string.
    pool_id = "0x" + format(pool_id_top << 56, "064x")
    return pool_key_words, tick_lower, tick_upper, pool_id


def _decode_v4_slot0(hex_result: object) -> tuple[int, int]:
    """Decode ``StateView.getSlot0(PoolKey)`` → ``(sqrtPriceX96, tick)``.

    Returns ``(sqrtPriceX96, currentTick)``. getSlot0 returns
    ``(uint160 sqrtPriceX96, int24 tick, uint24 protocolFee, uint24 lpFee)`` — a
    full, well-formed return is 4 words; require all 4 to reject truncated/malformed
    payloads (we only read words 0-1, but a short payload signals a bad response).
    """
    hex_data = _strip_0x(hex_result)
    if len(hex_data) < 4 * 64:
        raise ValueError(f"getSlot0 payload too short: {len(hex_data)} hex chars")
    sqrt_price_x96 = _decode_uint_word(hex_data, 0)
    tick = _as_int24(_decode_uint_word(hex_data, 1))
    return sqrt_price_x96, tick


# StateView.getPositionInfo(bytes32 poolId, bytes32 positionId) selector.
_GET_POSITION_INFO_SELECTOR = "97fd7b42"
# StateView.getFeeGrowthInside(bytes32 poolId, int24 tickLower, int24 tickUpper) selector.
_GET_FEE_GROWTH_INSIDE_SELECTOR = "53e9c1fb"
_UINT256_MASK = (1 << 256) - 1
_Q128 = 1 << 128


def _abi_int24_word(tick: int) -> str:
    """Two's-complement 32-byte ABI word for an int24 (negative ⇒ sign-extended)."""
    return format(tick & _UINT256_MASK, "064x")


def _v4_position_id(position_manager: str, tick_lower: int, tick_upper: int, token_id: int) -> str:
    """V4 pool-position id = ``keccak256(abi.encodePacked(owner, tickLower, tickUpper, salt))``.

    For PositionManager-custodied positions the pool ``owner`` is the
    PositionManager and the ``salt`` is the tokenId (bytes32); ticks are packed
    as 3-byte two's-complement int24. Verified on-chain: the liquidity returned by
    ``StateView.getPositionInfo(poolId, this)`` equals ``getPositionLiquidity(tokenId)``
    — the servicer asserts that match before trusting the fee snapshot.
    """
    owner = bytes.fromhex(position_manager.lower().removeprefix("0x"))
    packed = (
        owner
        + (tick_lower & 0xFFFFFF).to_bytes(3, "big")
        + (tick_upper & 0xFFFFFF).to_bytes(3, "big")
        + token_id.to_bytes(32, "big")
    )
    return "0x" + keccak(packed).hex()


def _decode_v4_position_info(hex_result: object) -> tuple[int, int, int]:
    """Decode ``StateView.getPositionInfo`` → ``(liquidity, feeGrowthInside0LastX128, feeGrowthInside1LastX128)``."""
    hex_data = _strip_0x(hex_result)
    if len(hex_data) < 3 * 64:
        raise ValueError(f"getPositionInfo payload too short: {len(hex_data)} hex chars")
    return _decode_uint_word(hex_data, 0), _decode_uint_word(hex_data, 1), _decode_uint_word(hex_data, 2)


def _decode_v4_fee_growth_inside(hex_result: object) -> tuple[int, int]:
    """Decode ``StateView.getFeeGrowthInside`` → ``(feeGrowthInside0X128, feeGrowthInside1X128)``."""
    hex_data = _strip_0x(hex_result)
    if len(hex_data) < 2 * 64:
        raise ValueError(f"getFeeGrowthInside payload too short: {len(hex_data)} hex chars")
    return _decode_uint_word(hex_data, 0), _decode_uint_word(hex_data, 1)


def _v4_uncollected_fees(liquidity: int, fg_inside: int, fg_last: int) -> int:
    """Uncollected fees for one token: ``liquidity * (fgInside - fgLast) / 2^128``.

    The delta is computed with unchecked-uint256 wraparound (Uniswap's
    ``feeGrowthInside`` accumulators overflow by design), then floor-divided by Q128.
    """
    return liquidity * ((fg_inside - fg_last) & _UINT256_MASK) // _Q128


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

    def _append_slots(self, count: int) -> None:
        """Append ``count`` request timestamps. Caller MUST hold ``self._lock``."""
        now = datetime.now(UTC).timestamp()
        self.request_times.extend([now] * count)

    async def check_rate_limit(self, count: int = 1, *, reserve: bool = False) -> tuple[bool, float]:
        """Check if rate limit would be exceeded for the given number of requests.

        Args:
            count: Number of requests to check (for batch operations)
            reserve: When ``True`` and the request is allowed, atomically record
                the ``count`` slots before returning — the check and the
                reservation happen under one lock acquisition, so a concurrent
                caller cannot pass the same check before the slots are taken (the
                per-chain cap holds under concurrency for multi-call RPCs). The
                caller must then NOT also call ``record_request`` for those slots.

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
                    if reserve:
                        self._append_slots(count)
                    return True, 0.0

                # Sort times to find when enough slots will expire
                sorted_times = sorted(self.request_times)
                # The (slots_needed - 1)th oldest request determines when we have enough capacity
                expiration_index = min(slots_needed - 1, len(sorted_times) - 1)
                expiration_time = sorted_times[expiration_index]
                wait_time = expiration_time + 60.0 - now
                return False, max(0, wait_time)

            if reserve:
                self._append_slots(count)
            return True, 0.0

    async def record_request(self) -> None:
        """Record a request."""
        async with self._lock:
            self._append_slots(1)


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
    # block means transient lag and is SAFE to retry. The marker set + classifier
    # are owned by ``almanak.gateway.utils.indexer_lag`` so the block-pinned
    # balance-read path (VIB-3350, Web3BalanceProvider) recognises the same error
    # class identically — one source of truth, no drift.
    _INDEXER_LAG_ERROR_MARKERS: frozenset[str] = INDEXER_LAG_ERROR_MARKERS

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

        VIB-4985 / ALM-2777. Thin wrapper over the shared classifier so callers
        that already hold an ``RpcService`` keep the historical class API; the
        logic and marker set live in ``almanak.gateway.utils.indexer_lag``.
        """
        return is_indexer_lag_error(message)

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

    async def QueryV4PositionState(
        self,
        request: gateway_pb2.V4PositionStateRequest,
        context: grpc.aio.ServicerContext,
    ) -> gateway_pb2.V4PositionStateResponse:
        """Read live Uniswap V4 LP position state on-chain (VIB-5024).

        EVM-only. Performs three eth_calls against connector-supplied addresses:

        1. ``PositionManager.getPositionLiquidity(tokenId)`` → liquidity (uint128).
        2. ``PositionManager.getPoolAndPositionInfo(tokenId)`` → ``(PoolKey, info)``
           where ``info`` is a packed uint256 (v4-periphery ``PositionInfoLibrary``):
           bits 8-31 = tickLower(int24), bits 32-55 = tickUpper(int24),
           bits 56-255 = truncated poolId. The returned PoolKey (5 ABI words)
           is keccak'd to the canonical PoolId for the StateView call below.
        3. ``StateView.getSlot0(poolId)`` → ``(sqrtPriceX96, tick, ...)``, where
           ``poolId = keccak256(abi.encode(PoolKey))`` (bytes32). The deployed
           StateView takes the PoolId, not a PoolKey tuple.

        ``success`` is true ONLY when all three reads decode cleanly. Numeric
        fields are decimal strings so the framework can keep Empty ("") distinct
        from a measured zero ("0"). A partial / failed read returns
        ``success=False`` so the valuer falls back to the ESTIMATED OPEN-amount
        path rather than ever emitting a HIGH-confidence value from incomplete
        on-chain data (the never-wrong-HIGH guarantee).
        """
        self._metrics.total_requests += 1

        try:
            chain = validate_chain(request.chain)
        except ValidationError as e:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.V4PositionStateResponse(success=False, error=str(e))

        msg = self._chain_not_configured_error(chain)
        if msg is not None:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(msg)
            return gateway_pb2.V4PositionStateResponse(success=False, error=msg)

        if is_solana_chain(chain):
            self._metrics.failed_requests += 1
            return gateway_pb2.V4PositionStateResponse(
                success=False, error="V4 position state queries not applicable for Solana"
            )

        try:
            position_manager = validate_address(request.position_manager, "position_manager")
            state_view = validate_address(request.state_view, "state_view")
        except ValidationError as e:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(str(e))
            return gateway_pb2.V4PositionStateResponse(success=False, error=str(e))

        if request.token_id < 0:
            error_msg = "token_id must be a non-negative integer"
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
            context.set_details(error_msg)
            return gateway_pb2.V4PositionStateResponse(success=False, error=error_msg)

        limiter = self._get_rate_limiter(chain)
        # This RPC fires up to 5 eth_calls (liquidity, pool+position info, slot0,
        # position fee snapshot, feeGrowthInside). Reserve all 5 slots atomically
        # up front so concurrent V4 reads can't oversubscribe the per-chain cap
        # (reserve=True records them under the same lock — no separate
        # record_request calls for this RPC).
        allowed, wait_time = await limiter.check_rate_limit(5, reserve=True)
        if not allowed:
            self._metrics.rate_limited_requests += 1
            context.set_code(grpc.StatusCode.RESOURCE_EXHAUSTED)
            context.set_details(f"Rate limited, retry after {wait_time:.2f}s")
            return gateway_pb2.V4PositionStateResponse(
                success=False, error=f"Rate limited, retry after {wait_time:.2f}s"
            )

        rpc_url = self._get_rpc_url(chain)
        if not rpc_url:
            self._metrics.failed_requests += 1
            context.set_code(grpc.StatusCode.FAILED_PRECONDITION)
            context.set_details(f"Chain '{chain}' is not configured")
            return gateway_pb2.V4PositionStateResponse(success=False, error=f"Chain '{chain}' is not configured")

        token_id_hex = format(request.token_id, "064x")

        # 1. getPositionLiquidity(uint256) — selector 0x1efeed33
        liq_params = [{"to": position_manager, "data": "0x1efeed33" + token_id_hex}, "latest"]
        start_time = time.time()
        liq_result, liq_error = await self._make_rpc_call(rpc_url, "eth_call", liq_params, "v4_position")
        record_rpc_request(chain, "eth_call")
        record_rpc_latency(chain, time.time() - start_time)
        if liq_error:
            self._metrics.failed_requests += 1
            return gateway_pb2.V4PositionStateResponse(
                success=False, error=liq_error.get("message", "getPositionLiquidity failed")
            )

        # 2. getPoolAndPositionInfo(uint256) — selector 0x7ba03aad
        info_params = [{"to": position_manager, "data": "0x7ba03aad" + token_id_hex}, "latest"]
        start_time = time.time()
        info_result, info_error = await self._make_rpc_call(rpc_url, "eth_call", info_params, "v4_position")
        record_rpc_request(chain, "eth_call")
        record_rpc_latency(chain, time.time() - start_time)
        if info_error:
            self._metrics.failed_requests += 1
            return gateway_pb2.V4PositionStateResponse(
                success=False, error=info_error.get("message", "getPoolAndPositionInfo failed")
            )

        try:
            liquidity = _decode_v4_liquidity(liq_result)
            pool_key_words, tick_lower, tick_upper, pool_id_trunc = _decode_v4_pool_and_position_info(info_result)
        except (ValueError, TypeError) as e:
            self._metrics.failed_requests += 1
            return gateway_pb2.V4PositionStateResponse(success=False, error=f"Failed to decode V4 position info: {e}")

        # 3. StateView.getSlot0(poolId) — the deployed V4 StateView takes the
        #    PoolId (bytes32 = keccak256(abi.encode(PoolKey))), not a PoolKey
        #    tuple. abi.encode of the static PoolKey struct equals the 5 words
        #    returned verbatim by getPoolAndPositionInfo, so the canonical PoolId
        #    is their keccak — no re-encoding can diverge from the on-chain key.
        pool_id_full = "0x" + keccak(bytes.fromhex(pool_key_words)).hex()
        # Integrity guard: v4-periphery packs bytes25(PoolId) (top 200 bits) into
        # PositionInfo, so it must prefix the canonical keccak PoolId. Divergence
        # ⇒ the decoded PoolKey words and the packed info don't correspond — fail
        # closed rather than read slot0 for the wrong pool.
        if int(pool_id_full, 16) >> 56 != int(pool_id_trunc, 16) >> 56:
            self._metrics.failed_requests += 1
            return gateway_pb2.V4PositionStateResponse(
                success=False, error="V4 poolId mismatch (PositionInfo vs PoolKey keccak)"
            )
        # Reads 3-5 (pool price via getSlot0 + uncollected fees via getPositionInfo
        # / getFeeGrowthInside) off the now-verified canonical poolId. Returns an
        # error string on any partial/inconsistent read → success=False → ESTIMATED.
        pool_state = await self._read_v4_pool_and_fee_state(
            rpc_url=rpc_url,
            chain=chain,
            state_view=state_view,
            position_manager=position_manager,
            pool_id_full=pool_id_full,
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            token_id=request.token_id,
            liquidity=liquidity,
        )
        if isinstance(pool_state, str):
            self._metrics.failed_requests += 1
            return gateway_pb2.V4PositionStateResponse(success=False, error=pool_state)
        current_tick, sqrt_price_x96, tokens_owed0, tokens_owed1 = pool_state

        self._metrics.successful_requests += 1
        return gateway_pb2.V4PositionStateResponse(
            success=True,
            liquidity=str(liquidity),
            tick_lower=tick_lower,
            tick_upper=tick_upper,
            current_tick=current_tick,
            sqrt_price_x96=str(sqrt_price_x96),
            pool_id=pool_id_full,
            tokens_owed0=str(tokens_owed0),
            tokens_owed1=str(tokens_owed1),
        )

    async def _read_v4_pool_and_fee_state(
        self,
        *,
        rpc_url: str,
        chain: str,
        state_view: str,
        position_manager: str,
        pool_id_full: str,
        tick_lower: int,
        tick_upper: int,
        token_id: int,
        liquidity: int,
    ) -> tuple[int, int, int, int] | str:
        """Read pool price (getSlot0) + the position's uncollected fees off ``pool_id_full``.

        Returns ``(current_tick, sqrt_price_x96, tokens_owed0, tokens_owed1)`` on a
        clean read, else an error string (the caller maps it to ``success=False`` so
        the valuer falls back to ESTIMATED — never a fee-less / fabricated HIGH).
        Fees are V3 parity: ``owed = liquidity·(fgInside − fgLast)/2^128`` per token,
        gated by a self-verifying check that the pool-position liquidity read via the
        derived positionId equals ``liquidity`` (the getPositionLiquidity value).
        """
        # StateView.getSlot0(poolId) — the deployed V4 StateView takes the bytes32 PoolId.
        slot0_params = [{"to": state_view, "data": "0x" + _GET_SLOT0_SELECTOR + pool_id_full[2:]}, "latest"]
        start_time = time.time()
        slot0_result, slot0_error = await self._make_rpc_call(rpc_url, "eth_call", slot0_params, "v4_slot0")
        record_rpc_request(chain, "eth_call")
        record_rpc_latency(chain, time.time() - start_time)
        if slot0_error:
            return slot0_error.get("message", "StateView.getSlot0 failed")
        try:
            sqrt_price_x96, current_tick = _decode_v4_slot0(slot0_result)
        except (ValueError, TypeError) as e:
            return f"Failed to decode slot0: {e}"
        if sqrt_price_x96 == 0:
            return "V4 pool not initialized (sqrtPriceX96 == 0)"

        # Uncollected fees (V3 parity). positionId = keccak(owner=PM, ticks, salt=tokenId).
        position_id = _v4_position_id(position_manager, tick_lower, tick_upper, token_id)
        posinfo_params = [
            {"to": state_view, "data": "0x" + _GET_POSITION_INFO_SELECTOR + pool_id_full[2:] + position_id[2:]},
            "latest",
        ]
        start_time = time.time()
        posinfo_result, posinfo_error = await self._make_rpc_call(rpc_url, "eth_call", posinfo_params, "v4_posinfo")
        record_rpc_request(chain, "eth_call")
        record_rpc_latency(chain, time.time() - start_time)
        if posinfo_error:
            return posinfo_error.get("message", "StateView.getPositionInfo failed")

        fgi_params = [
            {
                "to": state_view,
                "data": "0x"
                + _GET_FEE_GROWTH_INSIDE_SELECTOR
                + pool_id_full[2:]
                + _abi_int24_word(tick_lower)
                + _abi_int24_word(tick_upper),
            },
            "latest",
        ]
        start_time = time.time()
        fgi_result, fgi_error = await self._make_rpc_call(rpc_url, "eth_call", fgi_params, "v4_feegrowth")
        record_rpc_request(chain, "eth_call")
        record_rpc_latency(chain, time.time() - start_time)
        if fgi_error:
            return fgi_error.get("message", "StateView.getFeeGrowthInside failed")

        try:
            pos_liquidity, fg0_last, fg1_last = _decode_v4_position_info(posinfo_result)
            fg0_inside, fg1_inside = _decode_v4_fee_growth_inside(fgi_result)
        except (ValueError, TypeError) as e:
            return f"Failed to decode V4 fees: {e}"

        # Self-verifying identity guard: the pool-position liquidity read via the
        # derived positionId MUST equal getPositionLiquidity(tokenId). A mismatch
        # means the positionId convention is wrong — fail closed rather than
        # attribute another position's fee snapshot.
        if pos_liquidity != liquidity:
            return "V4 position liquidity mismatch (positionId vs tokenId)"

        return (
            current_tick,
            sqrt_price_x96,
            _v4_uncollected_fees(liquidity, fg0_inside, fg0_last),
            _v4_uncollected_fees(liquidity, fg1_inside, fg1_last),
        )

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
