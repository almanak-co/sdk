"""Gateway-only live Chainlink price source.

Provides pricing with zero API keys by reading directly from on-chain contracts
via Chainlink `latestRoundData()` -- aggregated oracle, direct USD, 25+ tokens
across 6 chains. High confidence (0.95).

When no CoinGecko API key is present, this source becomes the primary price
provider, making local dev and Anvil testing work without rate-limit issues.

Example:
    from almanak.integrations.chainlink.gateway.live import ChainlinkPriceSource

    source = ChainlinkPriceSource(chain="arbitrum", network="mainnet")
    result = await source.get_price("WETH", "USD")
    print(f"Price: {result.price}, Confidence: {result.confidence}")
    await source.close()
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import aiohttp

from almanak.core.rpc_network import Network

if TYPE_CHECKING:
    from almanak.framework.data.tokens.models import ResolvedToken

from almanak.core.chains import DEFAULT_CHAIN, ChainRegistry
from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceUnavailable,
    PriceResult,
)
from almanak.framework.data.tokens import TokenResolutionError, get_token_resolver
from almanak.framework.data.tokens.pegs import is_pegged
from almanak.gateway.data.price.aggregator import is_stablecoin_for_fallback
from almanak.gateway.utils import get_rpc_url
from almanak.gateway.utils.ssl_context import build_ssl_context
from almanak.integrations.chainlink.catalog import (
    CATALOG,
    CHAINLINK_CHAIN_IDS,
    CHAINLINK_PRICE_FEEDS,
    ETH_DENOMINATED_FEEDS,
    TOKEN_TO_ETH_PAIR,
    TOKEN_TO_PAIR,
)
from almanak.integrations.chainlink.codec import DECIMALS_SELECTOR, LATEST_ROUND_DATA_SELECTOR, decode_uint8
from almanak.integrations.chainlink.models import CHAINLINK_SOURCE_NAME, ChainlinkFeedObservation, FeedKind

logger = logging.getLogger(__name__)

_DIRECT_PRICE_STALENESS_THRESHOLD_SECONDS = 3600
_MAX_FUTURE_TIMESTAMP_SKEW_SECONDS = 60


def _canonical_chain(chain: object | None) -> str | None:
    """Canonical lowercase chain name for cross-chain comparison, or ``None``.

    Canonicalizes aliases (e.g. ``"bnb"`` -> ``"bsc"``) via the ChainRegistry so
    a ``ResolvedToken.chain`` can be compared apples-to-apples against a source's
    bound chain. Unknown chains pass through lowercased. Mirrors the resolution
    ``DexScreenerPriceSource._resolve_chain_for_call`` performs (VIB-5651).
    """
    if not chain:
        return None
    desc = ChainRegistry.try_resolve(str(chain).lower())
    return desc.name if desc is not None else str(chain).lower()


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


class ChainlinkPriceSource(BasePriceSource):
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
        chain: str = DEFAULT_CHAIN,
        network: Network = Network.MAINNET,
        cache_ttl: float = 10.0,
        request_timeout: float = 5.0,
    ):
        self._chain = chain.lower()
        self._network = Network.parse(network)
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout

        # In-memory cache: key -> (PriceResult, timestamp_seconds)
        self._cache: dict[str, tuple[PriceResult, float]] = {}

        # Lazy-initialized aiohttp session
        self._session: aiohttp.ClientSession | None = None

        # chainId validation deferred to first get_price() call (async)
        self._chain_id_validation_attempted = False
        self._chain_id_validated = False
        self._chain_id_lock = asyncio.Lock()

        # Resolve RPC URL once
        self._rpc_url: str | None = None
        try:
            self._rpc_url = get_rpc_url(self._chain, network=self._network)
        except ValueError:
            self._rpc_url = None
            logger.warning(
                "ChainlinkPriceSource: no RPC URL for chain=%s network=%s -- pricing will be unavailable",
                self._chain,
                self._network,
            )

        # Chain-specific Chainlink feeds
        self._feeds = CHAINLINK_PRICE_FEEDS.get(self._chain, {})

        # ETH-denominated feeds for derived pricing (TOKEN/ETH × ETH/USD)
        self._eth_feeds = ETH_DENOMINATED_FEEDS.get(self._chain, {})

        # Build per-instance token->pair lookup with resolver canonicalization.
        self._token_resolver = get_token_resolver()
        self._token_to_pair = self._build_token_pair_map()
        self._token_to_eth_pair = self._build_token_eth_pair_map()

        # Monotonic JSON-RPC request id for easier correlation.
        self._rpc_request_id = 0
        self._feed_decimals: dict[str, int] = {}

    @property
    def source_name(self) -> str:
        return CHAINLINK_SOURCE_NAME

    @property
    def provides_stablecoin_verification(self) -> bool:
        """Advertise the measured stablecoin verification capability."""
        return True

    @property
    def supported_tokens(self) -> list[str]:
        """Tokens supported via Chainlink feeds on this chain."""
        return sorted(set(self._token_to_pair) | set(self._token_to_eth_pair))

    @property
    def cache_ttl_seconds(self) -> int:
        return int(self._cache_ttl)

    async def _validate_chain_id(self) -> None:
        """Validate that the RPC endpoint serves the expected chain.

        Makes a single eth_chainId call on first use. If the chain ID doesn't
        match expectations, disables on-chain pricing (sets _rpc_url = None)
        rather than crashing the gateway.

        Uses a lock to prevent concurrent get_price() calls from bypassing
        in-flight validation.
        """
        async with self._chain_id_lock:
            if self._chain_id_validated or self._chain_id_validation_attempted or not self._rpc_url:
                return

            self._chain_id_validation_attempted = True
            expected_chain_id = CHAINLINK_CHAIN_IDS.get(self._chain)
            if expected_chain_id is None:
                return  # Unknown chain, skip validation

            try:
                session = await self._get_session()
                self._rpc_request_id += 1
                payload = {
                    "jsonrpc": "2.0",
                    "method": "eth_chainId",
                    "params": [],
                    "id": self._rpc_request_id,
                }
                timeout = aiohttp.ClientTimeout(total=self._request_timeout)
                async with session.post(self._rpc_url, json=payload, timeout=timeout) as resp:
                    if resp.status != 200:
                        logger.warning(
                            "ChainlinkPriceSource: chainId check failed (HTTP %d), skipping validation", resp.status
                        )
                        return
                    body = await resp.json()

                actual_chain_id = int(body.get("result") or "0x0", 16)
                if actual_chain_id != expected_chain_id:
                    logger.warning(
                        "ChainlinkPriceSource: chainId mismatch! expected=%d (%s) got=%d -- disabling pricing",
                        expected_chain_id,
                        self._chain,
                        actual_chain_id,
                    )
                    self._rpc_url = None
                else:
                    self._chain_id_validated = True
                    logger.debug("ChainlinkPriceSource: chainId validated: %d (%s)", actual_chain_id, self._chain)
            except Exception as e:
                # Broad catch intentional: chainId validation is best-effort and must never crash the gateway
                logger.warning("ChainlinkPriceSource: chainId validation failed: %s -- skipping", e)

    async def get_price(
        self,
        token: str,
        quote: str = "USD",
        *,
        resolved_token: ResolvedToken | None = None,
        bypass_stablecoin_fallback: bool = False,
    ) -> PriceResult:
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
        # Chain-correctness guard (VIB-5651): this instance is bound to
        # ``self._chain`` at construction and holds only that chain's Chainlink
        # feeds. If the caller tags the request with a DIFFERENT chain, answering
        # with this chain's price would be silent cross-chain corruption — miss
        # instead (the aggregator treats DataSourceUnavailable as a skip). The
        # per-chain aggregator routing is the primary guarantee; this guard makes
        # a mis-route unrepresentable rather than merely unlikely.
        if resolved_token is not None:
            rt_chain = _canonical_chain(getattr(resolved_token, "chain", None))
            if rt_chain and rt_chain != _canonical_chain(self._chain):
                raise DataSourceUnavailable(
                    source=self.source_name,
                    reason=f"chain_mismatch:{rt_chain}!={self._chain}",
                )

        if quote.upper() != "USD":
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Only USD quote supported, got {quote}",
            )

        token_upper = token.upper()

        # Check cache
        identity = getattr(getattr(resolved_token, "token_ref", None), "identity_key", None)
        cache_subject = f"{identity[0]}:{identity[1]}" if identity is not None else token_upper
        cache_key = f"{cache_subject}/USD" + (":measured" if bypass_stablecoin_fallback else "")
        cached = self._cache.get(cache_key)
        if cached:
            result, cached_at = cached
            if time.time() - cached_at < self._cache_ttl:
                return result

        # Stablecoins: $1.00 without RPC. ``resolved_token`` lets us
        # disambiguate symbol clashes (e.g. Polymarket pUSD vs ``Pleasing USD``).
        if not bypass_stablecoin_fallback and is_stablecoin_for_fallback(token, resolved_token):
            peg = is_pegged(resolved_token.token_ref) if resolved_token is not None else None
            if peg is None:
                raise DataSourceUnavailable(source=self.source_name, reason="peg_identity_unavailable")
            assert resolved_token is not None
            identity = resolved_token.token_ref.identity_key
            result = PriceResult(
                price=peg,
                source=self.source_name,
                timestamp=datetime.now(UTC),
                confidence=0.99,
                stale=False,
                peg_tokens=(f"{identity[0]}:{identity[1]}",),
            )
            self._cache[cache_key] = (result, time.time())
            return result

        # Validate chainId on first RPC-dependent call (after cache/stablecoin fast-paths)
        if not self._chain_id_validated and not self._chain_id_validation_attempted and self._rpc_url:
            await self._validate_chain_id()

        if not self._rpc_url:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"No RPC URL available for chain={self._chain}",
            )

        # Tier 1: Direct Chainlink USD feed
        pair = self._resolve_pair(token)
        if pair and pair in self._feeds:
            feed_address = self._feeds[pair]
            observation = await self._fetch_chainlink(feed_address, pair)
            result = PriceResult(
                price=observation.price,
                source=f"{self.source_name}_chainlink",
                timestamp=observation.updated_at,
                confidence=observation.confidence,
                stale=observation.stale,
                source_details={
                    "method": "direct",
                    "pair": pair,
                    "feed": feed_address.lower(),
                },
            )
            self._cache[cache_key] = (result, time.time())
            return result

        # Tier 2: Derived price via TOKEN/ETH × ETH/USD
        # Resolve canonical symbol first so address-based lookups can match
        # ETH-denominated feeds (e.g., passing an address should resolve to
        # "WSTETH" which then matches the WSTETH/ETH feed).
        if pair:
            resolved_symbol = pair.split("/")[0]
        else:
            # No direct feed found -- try to resolve the canonical symbol via
            # the token resolver before falling back to the raw input.
            try:
                resolved_info = self._token_resolver.resolve(token, self._chain, log_errors=False)
                resolved_symbol = resolved_info.symbol.upper()
            except TokenResolutionError:
                resolved_symbol = token_upper
        derived = await self._try_derived_price(resolved_symbol, cache_key)
        if derived is not None:
            return derived

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
                resolved = self._token_resolver.resolve(token_upper, self._chain, log_errors=False)
            except TokenResolutionError:
                continue

            token_to_pair[resolved.symbol.upper()] = pair

        return token_to_pair

    def _build_token_eth_pair_map(self) -> dict[str, str]:
        """Build token->ETH-denominated pair map for derived pricing."""
        token_to_eth_pair: dict[str, str] = {}
        for token, pair in TOKEN_TO_ETH_PAIR.items():
            if pair not in self._eth_feeds:
                continue
            token_to_eth_pair[token.upper()] = pair
        return token_to_eth_pair

    async def _try_derived_price(self, token_upper: str, cache_key: str) -> PriceResult | None:
        """Try to compute TOKEN/USD = TOKEN/ETH × ETH/USD.

        Returns None if no ETH-denominated feed exists or if fetching fails.
        """
        eth_pair = self._token_to_eth_pair.get(token_upper)
        if not eth_pair or eth_pair not in self._eth_feeds:
            return None

        # Need ETH/USD feed on this chain
        eth_usd_feed = self._feeds.get("ETH/USD")
        if not eth_usd_feed:
            return None

        try:
            token_eth = await self._fetch_chainlink(self._eth_feeds[eth_pair], eth_pair)
            eth_usd = await self._fetch_chainlink(eth_usd_feed, "ETH/USD")
        except DataSourceUnavailable as e:
            logger.debug("Derived price failed for %s: %s", token_upper, e)
            return None

        derived_price = token_eth.price * eth_usd.price
        # Confidence is the minimum of both feeds, slightly penalized for composition
        confidence = min(token_eth.confidence, eth_usd.confidence) * 0.95
        observed_at = min(token_eth.updated_at, eth_usd.updated_at)

        logger.info(
            "Derived %s/USD: %s (= %s × %s, confidence=%.2f)",
            token_upper,
            derived_price,
            token_eth.price,
            eth_usd.price,
            confidence,
        )

        result = PriceResult(
            price=derived_price,
            source=f"{self.source_name}_derived",
            timestamp=observed_at,
            confidence=confidence,
            stale=token_eth.stale or eth_usd.stale,
            source_details={
                "method": "derived",
                "formula": f"{eth_pair} * ETH/USD",
                "feeds": [
                    {
                        "pair": eth_pair,
                        "address": self._eth_feeds[eth_pair].lower(),
                        "observed_at": token_eth.updated_at.isoformat(),
                    },
                    {
                        "pair": "ETH/USD",
                        "address": eth_usd_feed.lower(),
                        "observed_at": eth_usd.updated_at.isoformat(),
                    },
                ],
            },
        )
        self._cache[cache_key] = (result, time.time())
        return result

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

    async def _fetch_chainlink(self, feed_address: str, pair: str) -> ChainlinkFeedObservation:
        """Fetch price from a Chainlink aggregator via latestRoundData().

        Args:
            feed_address: Chainlink aggregator contract address
            pair: Price pair name (e.g., "ETH/USD") for logging

        Returns:
            Decoded observation carrying the provider timestamp and freshness.

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
        now = int(time.time())
        if updated_at <= 0 or updated_at > now + _MAX_FUTURE_TIMESTAMP_SKEW_SECONDS:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Chainlink returned invalid updatedAt for {pair}: {updated_at}",
            )

        # Convert to price
        spec = CATALOG.feed(self._chain, pair)
        if spec is None:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Chainlink catalogue has no feed metadata for {pair} on {self._chain}",
            )
        decimals = await self._verified_feed_decimals(feed_address, pair, spec.decimals)
        price = Decimal(answer) / Decimal(10**decimals)

        # Check staleness
        age = now - updated_at
        confidence = 0.95

        # Preserve the existing one-hour safety policy for direct USD prices.
        # ETH-denominated exchange-rate legs use their provider heartbeat so a
        # slow-moving, healthy rETH/ETH feed is not rejected as stale.
        staleness_threshold = (
            spec.heartbeat_seconds if spec.kind is FeedKind.ETH else _DIRECT_PRICE_STALENESS_THRESHOLD_SECONDS
        )
        stale = age > staleness_threshold
        if stale:
            logger.debug(
                "Chainlink %s data is %d seconds old (threshold %d) -- this is expected on Anvil forks",
                pair,
                age,
                staleness_threshold,
            )
            confidence = 0.85

        logger.debug(
            "Chainlink %s: price=%s, confidence=%.2f, age=%ds",
            pair,
            price,
            confidence,
            age,
        )

        try:
            observed_at = datetime.fromtimestamp(updated_at, tz=UTC)
        except (OverflowError, OSError, ValueError) as exc:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Chainlink returned invalid updatedAt for {pair}: {updated_at}",
            ) from exc

        return ChainlinkFeedObservation(
            price=price,
            updated_at=observed_at,
            confidence=confidence,
            stale=stale,
        )

    async def get_reference_price(self, instrument: str, quote: str = "USD") -> PriceResult:
        """Read one catalogued feed without generic symbol aggregation.

        This isolated path intentionally leaves :meth:`get_price` unchanged.
        Commodity symbols must never be medianed with same-symbol crypto assets,
        and freshness must use Chainlink's ``updatedAt`` rather than gateway time.
        """
        pair = f"{instrument.strip().upper()}/{quote.strip().upper()}"
        spec = CATALOG.feed(self._chain, pair)
        if spec is None or spec.kind is not FeedKind.REFERENCE:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"No catalogued Chainlink reference feed for {pair} on {self._chain}",
            )

        # Reference reads are RPC-dependent and must preserve the same
        # chain-correctness guard as generic Chainlink pricing. A misconfigured
        # endpoint is unavailable, never an opportunity to read the catalogued
        # address on a different chain.
        if not self._chain_id_validated and not self._chain_id_validation_attempted and self._rpc_url:
            await self._validate_chain_id()
        if not self._rpc_url or not self._chain_id_validated:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"No positively chain-validated RPC URL available for chain={self._chain}",
            )

        observation = await self._fetch_reference_feed(spec.address, pair)
        return PriceResult(
            price=observation.price,
            source=f"chainlink:{self._chain}:{pair}:{spec.address.lower()}",
            timestamp=observation.updated_at,
            confidence=observation.confidence,
            stale=observation.stale,
        )

    async def _fetch_reference_feed(self, feed_address: str, pair: str) -> ChainlinkFeedObservation:
        """Decode an exact reference feed under its catalogued heartbeat."""
        try:
            response_hex = await self._eth_call(feed_address, LATEST_ROUND_DATA_SELECTOR)
            data = bytes.fromhex(response_hex.removeprefix("0x"))
        except Exception as exc:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Chainlink RPC call failed for reference feed {pair}: {exc}",
            ) from exc

        if len(data) < 160:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Chainlink response too short for reference feed {pair}: {len(data)} bytes",
            )

        answer = int.from_bytes(data[32:64], byteorder="big", signed=True)
        updated_at = int.from_bytes(data[96:128], byteorder="big")
        if answer <= 0 or updated_at <= 0:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Chainlink returned invalid answer/updatedAt for reference feed {pair}",
            )

        spec = CATALOG.feed(self._chain, pair)
        if spec is None or spec.address.lower() != feed_address.lower():
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Chainlink catalogue mismatch for reference feed {pair} on {self._chain}",
            )
        decimals = await self._verified_feed_decimals(feed_address, pair, spec.decimals)
        now = int(time.time())
        age = now - updated_at
        # A future provider timestamp is invalid, but it is not heartbeat
        # staleness. Preserve the timestamp so ReferencePriceData reports the
        # more precise ``reference_price_timestamp_in_future`` block reason.
        stale = age > spec.heartbeat_seconds
        return ChainlinkFeedObservation(
            price=Decimal(answer) / Decimal(10**decimals),
            updated_at=datetime.fromtimestamp(updated_at, tz=UTC),
            confidence=0.85 if stale else 0.95,
            stale=stale,
        )

    async def _verified_feed_decimals(self, feed_address: str, pair: str, expected: int) -> int:
        """Measure feed decimals once and fail closed on catalogue drift."""
        key = feed_address.lower()
        cached = self._feed_decimals.get(key)
        if cached is not None:
            return cached
        try:
            measured = decode_uint8(await self._eth_call(feed_address, DECIMALS_SELECTOR))
        except Exception as exc:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Chainlink decimals call failed for {pair}: {exc}",
            ) from exc
        if measured != expected:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Chainlink decimals mismatch for {pair}: catalogue={expected}, on-chain={measured}",
            )
        self._feed_decimals[key] = measured
        return measured

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
            connector = aiohttp.TCPConnector(ssl=build_ssl_context())
            self._session = aiohttp.ClientSession(connector=connector)
        return self._session

    async def close(self) -> None:
        """Close the aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None


# Historical public name retained while the implementation moves to the
# provider-owned package. Keeping the alias here lets the legacy module be a
# true module alias, so exception classes and monkeypatch targets preserve
# identity as well as the main price-source class.
OnChainPriceSource = ChainlinkPriceSource

__all__ = [
    "ChainlinkPriceSource",
    "EmptyRPCResponseError",
    "OnChainPriceSource",
    "RPCError",
]
