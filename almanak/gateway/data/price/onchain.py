"""On-chain price source using Chainlink oracles.

Provides pricing with zero API keys by reading directly from on-chain contracts
via Chainlink `latestRoundData()` -- aggregated oracle, direct USD, 25+ tokens
across 6 chains. High confidence (0.95).

When no CoinGecko API key is present, this source becomes the primary price
provider, making local dev and Anvil testing work without rate-limit issues.

Example:
    from almanak.gateway.data.price.onchain import OnChainPriceSource

    source = OnChainPriceSource(chain="arbitrum", network="mainnet")
    result = await source.get_price("WETH", "USD")
    print(f"Price: {result.price}, Confidence: {result.confidence}")
    await source.close()
"""

import logging
import time
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp

from almanak.framework.backtesting.pnl.providers.chainlink import (
    CHAINLINK_PRICE_FEEDS,
    LATEST_ROUND_DATA_SELECTOR,
    TOKEN_TO_PAIR,
)
from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceUnavailable,
    PriceResult,
)
from almanak.framework.data.tokens import TokenResolutionError, get_token_resolver
from almanak.gateway.data.price.aggregator import STABLECOIN_FALLBACK_TOKENS
from almanak.gateway.utils import get_rpc_url

logger = logging.getLogger(__name__)

# Chainlink feed decimals -- most USD feeds use 8 decimals.
# Override here for any feeds that differ.
_FEED_DECIMALS: dict[str, int] = {}  # pair -> decimals override
_DEFAULT_FEED_DECIMALS = 8

# Staleness threshold: warn if Chainlink updatedAt is older than this (seconds).
# On Anvil forks, data is always "stale" by wall-clock since time doesn't advance,
# so we log a warning but still return the price.
_STALENESS_THRESHOLD = 3600  # 1 hour


class RPCError(RuntimeError):
    """Raised when JSON-RPC returns an error object."""

    def __init__(self, *, request_id: int, to: str, error: object):
        self.request_id = request_id
        self.to = to
        self.error = error
        super().__init__(f"RPC eth_call failed (id={request_id}, to={to})")


class EmptyRPCResponseError(RuntimeError):
    """Raised when JSON-RPC returns an empty eth_call result."""

    def __init__(self, *, request_id: int, to: str):
        self.request_id = request_id
        self.to = to
        super().__init__(f"Empty eth_call result (id={request_id}, to={to})")


class OnChainPriceSource(BasePriceSource):
    """Price source that reads prices directly from Chainlink on-chain oracles.

    Uses Chainlink `latestRoundData()` for direct USD pricing of major tokens.
    All RPC calls are async via aiohttp (no Web3 dependency).

    Args:
        chain: Primary chain for on-chain reads (e.g., "arbitrum", "ethereum")
        network: Network type for RPC URL resolution ("mainnet" or "anvil")
        cache_ttl: In-memory cache TTL in seconds (default 10)
        request_timeout: Per-RPC-call timeout in seconds (default 5)
    """

    def __init__(
        self,
        chain: str = "arbitrum",
        network: str = "mainnet",
        cache_ttl: float = 10.0,
        request_timeout: float = 5.0,
    ):
        self._chain = chain.lower()
        self._network = network
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout

        # In-memory cache: key -> (PriceResult, timestamp_seconds)
        self._cache: dict[str, tuple[PriceResult, float]] = {}

        # Lazy-initialized aiohttp session
        self._session: aiohttp.ClientSession | None = None

        # Resolve RPC URL once
        self._rpc_url: str | None = None
        try:
            self._rpc_url = get_rpc_url(self._chain, network=self._network)
        except ValueError:
            self._rpc_url = None
            logger.warning(
                "OnChainPriceSource: no RPC URL for chain=%s network=%s -- on-chain pricing will be unavailable",
                self._chain,
                self._network,
            )

        # Chain-specific Chainlink feeds
        self._feeds = CHAINLINK_PRICE_FEEDS.get(self._chain, {})

        # Build per-instance token->pair lookup with resolver canonicalization.
        self._token_resolver = get_token_resolver()
        self._token_to_pair = self._build_token_pair_map()

        # Monotonic JSON-RPC request id for easier correlation.
        self._rpc_request_id = 0

    @property
    def source_name(self) -> str:
        return "onchain"

    @property
    def supported_tokens(self) -> list[str]:
        """Tokens supported via Chainlink feeds on this chain."""
        return sorted(self._token_to_pair)

    @property
    def cache_ttl_seconds(self) -> int:
        return int(self._cache_ttl)

    async def get_price(self, token: str, quote: str = "USD") -> PriceResult:
        """Get price for a token via on-chain sources.

        Resolution order:
        1. Check in-memory cache
        2. Stablecoins -> return $1.00 directly (no RPC needed)
        3. Chainlink feed -> latestRoundData()

        Args:
            token: Token symbol (e.g., "ETH", "WETH", "ARB")
            quote: Quote currency (only "USD" supported)

        Returns:
            PriceResult with price and confidence score

        Raises:
            DataSourceUnavailable: If token cannot be priced on-chain
        """
        if quote.upper() != "USD":
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Only USD quote supported, got {quote}",
            )

        token_upper = token.upper()

        # Check cache
        cache_key = f"{token_upper}/USD"
        cached = self._cache.get(cache_key)
        if cached:
            result, cached_at = cached
            if time.time() - cached_at < self._cache_ttl:
                return result

        # Stablecoins: $1.00 without RPC
        if token_upper in STABLECOIN_FALLBACK_TOKENS:
            result = PriceResult(
                price=Decimal("1.00"),
                source=self.source_name,
                timestamp=datetime.now(UTC),
                confidence=0.99,
                stale=False,
            )
            self._cache[cache_key] = (result, time.time())
            return result

        if not self._rpc_url:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"No RPC URL available for chain={self._chain}",
            )

        # Tier 1: Chainlink
        pair = self._resolve_pair(token)
        if pair and pair in self._feeds:
            feed_address = self._feeds[pair]
            price, confidence = await self._fetch_chainlink(feed_address, pair)
            result = PriceResult(
                price=price,
                source=f"{self.source_name}_chainlink",
                timestamp=datetime.now(UTC),
                confidence=confidence,
                stale=confidence < 0.95,
            )
            self._cache[cache_key] = (result, time.time())
            return result

        raise DataSourceUnavailable(
            source=self.source_name,
            reason=(
                f"No Chainlink feed for {token_upper} on {self._chain}. "
                f"Available feeds: {list(self._feeds.keys())[:5]}..."
            ),
        )

    def _build_token_pair_map(self) -> dict[str, str]:
        """Build chain-aware token->pair map using resolver canonical symbols."""
        token_to_pair: dict[str, str] = {}

        for token, pair in TOKEN_TO_PAIR.items():
            if pair not in self._feeds:
                continue

            token_upper = token.upper()
            token_to_pair[token_upper] = pair

            try:
                resolved = self._token_resolver.resolve(token_upper, self._chain)
            except TokenResolutionError:
                continue

            token_to_pair[resolved.symbol.upper()] = pair

        return token_to_pair

    def _resolve_pair(self, token: str) -> str | None:
        """Resolve symbol/address input to a Chainlink pair in this source."""
        token_upper = token.upper()
        pair = self._token_to_pair.get(token_upper)
        if pair is not None:
            return pair

        try:
            resolved = self._token_resolver.resolve(token, self._chain)
        except TokenResolutionError:
            return None

        return self._token_to_pair.get(resolved.symbol.upper())

    async def _fetch_chainlink(self, feed_address: str, pair: str) -> tuple[Decimal, float]:
        """Fetch price from a Chainlink aggregator via latestRoundData().

        Args:
            feed_address: Chainlink aggregator contract address
            pair: Price pair name (e.g., "ETH/USD") for logging

        Returns:
            Tuple of (price_decimal, confidence)

        Raises:
            DataSourceUnavailable: If RPC call fails or response is invalid
        """
        try:
            response_hex = await self._eth_call(feed_address, LATEST_ROUND_DATA_SELECTOR)
        except Exception as e:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Chainlink RPC call failed for {pair}: {e}",
            ) from e

        # Decode: latestRoundData returns (roundId, answer, startedAt, updatedAt, answeredInRound)
        # answer is int256; other values are uints. Each ABI word is 32 bytes (160 bytes total).
        try:
            data = bytes.fromhex(response_hex.removeprefix("0x"))
        except ValueError as exc:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Malformed RPC hex for {pair}: {exc}",
            ) from exc
        if len(data) < 160:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Chainlink response too short for {pair}: {len(data)} bytes (need 160)",
            )

        # Parse 5 x 32-byte words (answer is int256, must decode as signed)
        answer = int.from_bytes(data[32:64], byteorder="big", signed=True)
        updated_at = int.from_bytes(data[96:128], byteorder="big")

        if answer <= 0:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Chainlink returned non-positive answer for {pair}: {answer}",
            )

        # Convert to price
        decimals = _FEED_DECIMALS.get(pair, _DEFAULT_FEED_DECIMALS)
        price = Decimal(answer) / Decimal(10**decimals)

        # Check staleness
        now = int(time.time())
        age = now - updated_at
        confidence = 0.95

        if age > _STALENESS_THRESHOLD:
            logger.debug(
                "Chainlink %s data is %d seconds old (threshold %d) -- this is expected on Anvil forks",
                pair,
                age,
                _STALENESS_THRESHOLD,
            )
            confidence = 0.85

        logger.debug(
            "Chainlink %s: price=%s, confidence=%.2f, age=%ds",
            pair,
            price,
            confidence,
            age,
        )

        return price, confidence

    async def _eth_call(self, to: str, data: str) -> str:
        """Make an async eth_call via JSON-RPC.

        Args:
            to: Contract address
            data: Hex-encoded calldata (with 0x prefix)

        Returns:
            Hex-encoded response data

        Raises:
            RuntimeError: On RPC HTTP error
            RPCError: On JSON-RPC error object
            EmptyRPCResponseError: On empty JSON-RPC result
        """
        if not self._rpc_url:
            raise RuntimeError("No RPC URL configured")
        url = self._rpc_url

        session = await self._get_session()
        self._rpc_request_id += 1
        request_id = self._rpc_request_id
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [{"to": to, "data": data}, "latest"],
            "id": request_id,
        }

        timeout = aiohttp.ClientTimeout(total=self._request_timeout)
        async with session.post(url, json=payload, timeout=timeout) as resp:
            if resp.status != 200:
                text = await resp.text()
                raise RuntimeError(f"RPC HTTP {resp.status}: {text[:200]}")
            body = await resp.json()

        if "error" in body:
            raise RPCError(request_id=request_id, to=to, error=body["error"])

        result = body.get("result", "0x")
        if result == "0x" or result == "0x0":
            raise EmptyRPCResponseError(request_id=request_id, to=to)

        return result

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create the aiohttp session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
