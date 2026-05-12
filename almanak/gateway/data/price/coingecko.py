"""CoinGecko Price Source implementation.

This module provides a production-ready price source using the CoinGecko API,
with proper caching, rate limiting, and error handling.

Key Features:
    - Response caching with configurable TTL
    - Graceful degradation on timeout (returns stale data with reduced confidence)
    - Bounded retry (single 1s pause) on 429, then fail-fast so the aggregator
      can fall over to other sources without stalling on compounding backoff
    - Comprehensive logging for observability

Example:
    from almanak.gateway.data.price.coingecko import CoinGeckoPriceSource

    source = CoinGeckoPriceSource(api_key="your-api-key")
    result = await source.get_price("WETH", "USD")
    print(f"Price: {result.price}, Confidence: {result.confidence}")
"""

import asyncio
import enum
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import aiohttp

from almanak.core.constants import STABLECOINS
from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceRateLimited,
    DataSourceUnavailable,
    PriceResult,
)
from almanak.gateway.utils.rpc_provider import _get_gateway_api_key
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)


# CoinGecko "asset platform" identifiers keyed by our internal chain name.
# Used by the contract-address endpoint (/simple/token_price/{platform}),
# which prices a token by its on-chain address — no CoinGecko ID needed.
# This lets us resolve tokens that aren't in our static symbol registry
# (e.g. cbBTC, niche LSTs) as long as the caller supplies chain + address.
COINGECKO_PLATFORM_IDS: dict[str, str] = {
    "ethereum": "ethereum",
    "arbitrum": "arbitrum-one",
    "optimism": "optimistic-ethereum",
    "base": "base",
    "polygon": "polygon-pos",
    "avalanche": "avalanche",
    "bsc": "binance-smart-chain",
    "sonic": "sonic",
    "mantle": "mantle",
    "berachain": "berachain",
    "monad": "monad",
    "xlayer": "xlayer",
    "zerog": "zerog",
    "linea": "linea",
    "blast": "blast",
    "plasma": "plasma",
}


# Token ID mappings for Arbitrum tokens
# CoinGecko uses specific IDs for each token
ARBITRUM_TOKEN_IDS: dict[str, str] = {
    "ETH": "ethereum",
    "WETH": "weth",
    "USDC": "usd-coin",
    "USDC.E": "usd-coin",
    "ARB": "arbitrum",
    "WBTC": "wrapped-bitcoin",
    "USDT": "tether",
    "DAI": "dai",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "GMX": "gmx",
    "PENDLE": "pendle",
    "RDNT": "radiant-capital",
    "SOL": "solana",
    "JOE": "trader-joe",
    "LDO": "lido-dao",
    "BTC": "bitcoin",
    "STETH": "lido-dao-wrapped-staked-eth",
    "WSTETH": "wrapped-steth",
    "CBETH": "coinbase-wrapped-staked-eth",
    "USDE": "ethena-usde",
    "SUSDE": "ethena-staked-usde",
}

# Token ID mappings for Avalanche tokens
AVALANCHE_TOKEN_IDS: dict[str, str] = {
    "AVAX": "avalanche-2",
    "WAVAX": "avalanche-2",  # Wrapped AVAX uses same price as AVAX
    "USDC": "usd-coin",
    "USDC.E": "usd-coin",
    "USDT": "tether",
    "USDT.E": "tether",
    "DAI": "dai",
    "DAI.E": "dai",
    "WETH": "weth",
    "WETH.E": "weth",
    "WBTC": "wrapped-bitcoin",
    "WBTC.E": "wrapped-bitcoin",
    "JOE": "trader-joe",
    "PNG": "pangolin",
    "QI": "benqi",
    "LINK": "chainlink",
    "AAVE": "aave",
    "BTC.B": "bitcoin",
}

# Token ID mappings for Base tokens
BASE_TOKEN_IDS: dict[str, str] = {
    "ETH": "ethereum",
    "WETH": "weth",
    "USDC": "usd-coin",
    "USDBC": "usd-coin",  # Bridged USDC on Base, pegged to $1
    "USDT": "tether",
    "DAI": "dai",
    "CBETH": "coinbase-wrapped-staked-eth",
    "WSTETH": "wrapped-steth",
    "AERO": "aerodrome-finance",
    "BASE": "base-protocol",
    "DEGEN": "degen-base",
    "BRETT": "brett",
}

# Token ID mappings for BSC tokens
BSC_TOKEN_IDS: dict[str, str] = {
    "BNB": "binancecoin",
    "WBNB": "binancecoin",  # Wrapped BNB uses same price as BNB
    "USDC": "usd-coin",
    "USDT": "tether",
    "DAI": "dai",
    "WETH": "weth",  # Bridged ETH on BSC
    "BTCB": "bitcoin",
    "CAKE": "pancakeswap-token",
    "BUSD": "binance-usd",
}

# Token ID mappings for Solana tokens
SOLANA_TOKEN_IDS: dict[str, str] = {
    "SOL": "solana",
    "WSOL": "solana",  # Wrapped SOL uses same price as SOL
    "USDC": "usd-coin",
    "USDT": "tether",
    "JUP": "jupiter-exchange-solana",
    "RAY": "raydium",
    "ORCA": "orca",
    "BONK": "bonk",
    "WIF": "dogwifcoin",
    "JTO": "jito-governance-token",
    "PYTH": "pyth-network",
    "MSOL": "msol",
    "JITOSOL": "jito-staked-sol",
}

# Combined token mappings (chain-agnostic fallback)
# Used when chain-specific mapping not found
MANTLE_TOKEN_IDS: dict[str, str] = {
    "MNT": "mantle",
    "WMNT": "mantle",  # Wrapped MNT uses same price as MNT
    "WETH": "weth",
    "USDC": "usd-coin",
    "USDT": "tether",
}

XLAYER_TOKEN_IDS: dict[str, str] = {
    "OKB": "okb",
    "WOKB": "okb",  # Wrapped OKB uses same price as OKB
    "WETH": "weth",
    "USDC": "usd-coin",
    "USDT0": "tether",  # USD₮0 is Stargate-bridged USDT
    "USDG": "usd-coin",  # Gravity USD stablecoin, pegged ~$1
}

ETHEREUM_TOKEN_IDS: dict[str, str] = {
    "ETH": "ethereum",
    "WETH": "weth",
    "USDC": "usd-coin",
    "USDT": "tether",
    "DAI": "dai",
    "WBTC": "wrapped-bitcoin",
    "LINK": "chainlink",
    "UNI": "uniswap",
    "AAVE": "aave",
    "CRV": "curve-dao-token",
    "CVX": "convex-finance",
    "COMP": "compound-governance-token",
    "MKR": "maker",
    "SNX": "havven",
    "LDO": "lido-dao",
    "RPL": "rocket-pool",
    "ENS": "ethereum-name-service",
    "PENDLE": "pendle",
    "GHO": "gho",
    "CRVUSD": "crvusd",
    "WSTETH": "wrapped-steth",
    "RETH": "rocket-pool-eth",
    "CBETH": "coinbase-wrapped-staked-eth",
    "WEETH": "wrapped-eeth",
    "PUFETH": "pufeth",
    "USDE": "ethena-usde",
    "SUSDE": "ethena-staked-usde",
}

MONAD_TOKEN_IDS: dict[str, str] = {
    # MON (native) / WMON (wrapped) — Monad's gas token. Curvance markets use
    # WMON as the canonical collateral/debt asset.
    "MON": "monad",
    "WMON": "monad",
    # Monad-bridged WETH / WBTC / USDC — priced at the underlying asset's CG id.
    "WETH": "weth",
    "USDC": "usd-coin",
    "WBTC": "wrapped-bitcoin",
    # LST / LRT collateral supported by Curvance markets.
    # Keys are uppercase to match get_price()'s symbol normalization.
    "EZETH": "renzo-restaked-eth",
    "WSTETH": "wrapped-steth",
    # APRMON / SHMON intentionally unmapped: their CG ids could not be verified
    # and pinning a wrong id would suppress the address-endpoint fallback.
}

GLOBAL_TOKEN_IDS: dict[str, str] = {
    **ARBITRUM_TOKEN_IDS,
    **AVALANCHE_TOKEN_IDS,
    **BASE_TOKEN_IDS,
    **BSC_TOKEN_IDS,
    **MANTLE_TOKEN_IDS,
    **XLAYER_TOKEN_IDS,
    **SOLANA_TOKEN_IDS,
    **MONAD_TOKEN_IDS,
    # Ethereum last so canonical IDs (e.g. WSTETH -> wrapped-steth) win over chain variants
    **ETHEREUM_TOKEN_IDS,
}


@dataclass
class CacheEntry:
    """Cache entry for price data."""

    result: PriceResult
    cached_at: datetime
    fetch_latency_ms: float = 0.0


@dataclass
class RateLimitState:
    """Tracks rate limit state.

    Since the switch to fail-fast (bounded 1s retry on 429, then raise so
    the aggregator falls over to other sources), the only consumer of this
    state is ``DataSourceRateLimited.retry_after``, which surfaces
    ``backoff_seconds`` to callers as advisory metadata. ``consecutive_429s``
    is retained for observability/metrics.
    """

    backoff_seconds: float = 1.0
    consecutive_429s: int = 0
    max_backoff_seconds: float = 10.0

    def record_rate_limit(self) -> None:
        """Record a rate limit hit and increase backoff."""
        self.consecutive_429s += 1
        # Exponential backoff: 1s, 2s, 4s, 8s, max 10s. No longer drives
        # sleeps; surfaced via DataSourceRateLimited.retry_after only.
        self.backoff_seconds = min(self.max_backoff_seconds, 2 ** (self.consecutive_429s - 1))

    def record_success(self) -> None:
        """Record successful request, fully reset backoff state."""
        self.consecutive_429s = 0
        self.backoff_seconds = 1.0


@dataclass
class SourceHealthMetrics:
    """Health metrics for observability."""

    total_requests: int = 0
    successful_requests: int = 0
    cache_hits: int = 0
    timeouts: int = 0
    rate_limits: int = 0
    errors: int = 0
    total_latency_ms: float = 0.0
    last_error: str | None = None
    last_error_time: datetime | None = None

    @property
    def success_rate(self) -> float:
        """Calculate success rate as percentage."""
        if self.total_requests == 0:
            return 100.0
        return (self.successful_requests / self.total_requests) * 100

    @property
    def average_latency_ms(self) -> float:
        """Calculate average latency in milliseconds."""
        if self.successful_requests == 0:
            return 0.0
        return self.total_latency_ms / self.successful_requests

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for logging/metrics."""
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "cache_hits": self.cache_hits,
            "timeouts": self.timeouts,
            "rate_limits": self.rate_limits,
            "errors": self.errors,
            "success_rate": round(self.success_rate, 2),
            "average_latency_ms": round(self.average_latency_ms, 2),
            "last_error": self.last_error,
            "last_error_time": (self.last_error_time.isoformat() if self.last_error_time else None),
        }


# Sentinel returned by _attempt_id_fetch / _attempt_address_fetch when the
# response was 429 and the caller should retry. An Enum-singleton gives mypy a
# narrowable type after `outcome is _RETRY`, so the orchestrator can `return
# outcome` and have it land as the right narrowed type.
class _RetrySentinel(enum.Enum):
    RETRY = enum.auto()


_RETRY = _RetrySentinel.RETRY


class CoinGeckoPriceSource(BasePriceSource):
    """CoinGecko price source with caching, rate limiting, and graceful degradation.

    This implementation follows the contract defined by BasePriceSource:
    1. On success: Return fresh PriceResult with confidence=1.0
    2. On timeout with cache: Return stale PriceResult with reduced confidence
    3. On timeout without cache: Raise DataSourceUnavailable
    4. On rate limit: Raise DataSourceRateLimited with retry_after

    Attributes:
        api_key: Optional CoinGecko API key (uses pro API if provided)
        cache_ttl: Cache time-to-live in seconds (default 30)
        request_timeout: HTTP request timeout in seconds (default 10)

    Example:
        # Create source with default settings
        source = CoinGeckoPriceSource()

        # Create source with API key and custom TTL
        source = CoinGeckoPriceSource(
            api_key="your-api-key",
            cache_ttl=60,
            request_timeout=15,
        )

        # Fetch price
        result = await source.get_price("WETH", "USD")
        if result.stale:
            logger.warning("Using stale price data")
    """

    # API endpoints
    _FREE_API_BASE = "https://api.coingecko.com/api/v3"
    _PRO_API_BASE = "https://pro-api.coingecko.com/api/v3"

    # Supported tokens on Arbitrum
    _SUPPORTED_TOKENS = list(ARBITRUM_TOKEN_IDS.keys())

    def __init__(
        self,
        api_key: str | None = None,
        cache_ttl: int = 30,
        request_timeout: float = 10.0,
        stale_confidence_multiplier: float = 0.7,
    ) -> None:
        """Initialize the CoinGecko price source.

        Args:
            api_key: Optional CoinGecko API key. If provided, uses pro API.
            cache_ttl: Cache time-to-live in seconds. Default 30.
            request_timeout: HTTP request timeout in seconds. Default 10.
            stale_confidence_multiplier: Confidence multiplier for stale data (0-1).
                Default 0.7 means stale data has 70% of original confidence.
        """
        self._api_key = (_get_gateway_api_key("COINGECKO_API_KEY") or "") if api_key is None else api_key
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._stale_confidence_multiplier = stale_confidence_multiplier

        # Select API base URL based on whether we have an API key (from param or env)
        self._api_base = self._PRO_API_BASE if self._api_key else self._FREE_API_BASE

        # Cache: key -> CacheEntry
        self._cache: dict[str, CacheEntry] = {}

        # Rate limit tracking per endpoint
        self._rate_limit_state = RateLimitState()

        # Health metrics
        self._metrics = SourceHealthMetrics()

        # HTTP session (created on first request)
        self._session: aiohttp.ClientSession | None = None
        self._session_loop: asyncio.AbstractEventLoop | None = None

        logger.info(
            "Initialized CoinGeckoPriceSource",
            extra={
                "api_type": "pro" if self._api_key else "free",
                "cache_ttl": cache_ttl,
                "request_timeout": request_timeout,
            },
        )

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session, recreating if event loop changed."""
        current_loop = asyncio.get_running_loop()
        if self._session is not None and not self._session.closed:
            if self._session_loop is not None and self._session_loop is not current_loop:
                try:
                    await self._session.close()
                except Exception:
                    pass
                self._session = None
                self._session_loop = None
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._request_timeout)
            connector = aiohttp.TCPConnector(ssl=build_ssl_context())
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
            self._session_loop = current_loop
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            self._session_loop = None

    def _get_cache_key(self, token: str, quote: str) -> str:
        """Generate cache key for token/quote pair."""
        return f"{token.upper()}/{quote.upper()}"

    def _get_cached(self, token: str, quote: str) -> CacheEntry | None:
        """Get cached entry if exists and not expired."""
        cache_key = self._get_cache_key(token, quote)
        entry = self._cache.get(cache_key)
        if entry is None:
            return None

        # Check if expired
        age_seconds = (datetime.now(UTC) - entry.cached_at).total_seconds()
        if age_seconds > self._cache_ttl:
            return None

        return entry

    def _get_stale_cached(self, token: str, quote: str) -> CacheEntry | None:
        """Get cached entry even if expired (for fallback)."""
        cache_key = self._get_cache_key(token, quote)
        return self._cache.get(cache_key)

    def _update_cache(self, token: str, quote: str, result: PriceResult, latency_ms: float) -> None:
        """Update cache with fresh result."""
        cache_key = self._get_cache_key(token, quote)
        self._cache[cache_key] = CacheEntry(
            result=result,
            cached_at=datetime.now(UTC),
            fetch_latency_ms=latency_ms,
        )

    def _stale_fallback_result(self, cache_key: str, quote_upper: str) -> PriceResult | None:
        """Return a stale PriceResult from cache with reduced confidence, or None.

        Shared fallback for 429 / non-200 / timeout / network-error branches
        across ``get_price`` and ``_try_fetch_by_address``. When a stale cache
        entry exists we prefer returning it (downgraded by
        ``stale_confidence_multiplier``) over raising, so the aggregator keeps
        a usable signal from this source during transient outages.
        """
        stale = self._get_stale_cached(cache_key, quote_upper)
        if stale is None:
            return None
        self._metrics.successful_requests += 1
        return PriceResult(
            price=stale.result.price,
            source=self.source_name,
            timestamp=stale.result.timestamp,
            confidence=stale.result.confidence * self._stale_confidence_multiplier,
            stale=True,
        )

    def _stale_or_raise_unavailable(
        self,
        cache_token_key: str,
        quote_upper: str,
        log_format: str,
        log_arg: str,
        reason: str,
        cause: Exception,
    ) -> PriceResult:
        """Return stale data if cached, else raise DataSourceUnavailable.

        Common terminal-error path for TimeoutError / ClientError handlers in
        ``_try_fetch_by_address``: returning a stale-fallback PriceResult lets
        the aggregator keep a downgraded signal from this source, while a raise
        surfaces the outage when nothing is cached so another source can take
        over. ``log_format`` / ``log_arg`` keep the warm-path log lines
        contextual to the caller.
        """
        stale_result = self._stale_fallback_result(cache_token_key, quote_upper)
        if stale_result is not None:
            logger.info(log_format, log_arg, quote_upper)
            return stale_result
        raise DataSourceUnavailable(
            source=self.source_name,
            reason=reason,
        ) from cause

    def _resolve_token_id(self, token: str) -> str | None:
        """Resolve a token SYMBOL to CoinGecko ID.

        Resolution order:
        1. DEFAULT_TOKENS by symbol (uses Token.coingecko_id)
        2. Hardcoded symbol mappings (backward compatibility)

        Address-based resolution was REMOVED in VIB-3259 Phase 2. The prior
        process-wide ``{address.lower() → coingecko_id}`` reverse map was
        not chain-scoped, so two tokens sharing an address across chains
        (USDC/USDT variants, same-address same-bytecode deploys) resolved
        first-write-wins to the wrong chain's CoinGecko ID.

        Address-based lookups now go exclusively through
        ``_try_fetch_by_address`` which hits
        ``/simple/token_price/{platform}`` keyed on ``ResolvedToken.chain``
        — correct by construction because the endpoint is chain-scoped.

        Args:
            token: Token symbol (already uppercased). Address inputs return
                None here; ``get_price`` routes them via
                ``_try_fetch_by_address`` when ``resolved_token`` is present.

        Returns:
            CoinGecko ID if the symbol is known, None otherwise.
        """
        # Try DEFAULT_TOKENS first (uses Token.coingecko_id)
        try:
            from almanak.framework.data.tokens.defaults import get_coingecko_id

            cg_id = get_coingecko_id(token)
            if cg_id:
                return cg_id
        except ImportError:
            pass

        # Fall back to hardcoded symbol-based mappings.
        return GLOBAL_TOKEN_IDS.get(token)

    async def get_price(  # noqa: C901
        self,
        token: str,
        quote: str = "USD",
        *,
        resolved_token: "Any | None" = None,
    ) -> PriceResult:
        """Fetch the current price for a token.

        Args:
            token: Token symbol (e.g., "WETH", "ARB", "USDC") or contract address.
            quote: Quote currency (default "USD")
            resolved_token: Optional ResolvedToken with chain + address. When the
                symbol/ID registry misses, the source falls back to CoinGecko's
                contract-address endpoint (/simple/token_price/{platform}) using
                resolved_token.address + resolved_token.chain. This lets the
                source price unknown-to-our-registry tokens without adding them
                to any hardcoded list.

        Returns:
            PriceResult with price and metadata

        Raises:
            DataSourceUnavailable: If source is unavailable and no cache exists
            DataSourceRateLimited: If rate limit is exceeded
        """
        self._metrics.total_requests += 1
        token_upper = token.upper()
        quote_upper = quote.upper()

        # Check the primary cache (keyed by symbol/address string as given).
        cached = self._get_cached(token_upper, quote_upper)
        if cached is not None:
            self._metrics.cache_hits += 1
            self._metrics.successful_requests += 1
            logger.debug(
                "Cache hit for %s/%s",
                token_upper,
                quote_upper,
                extra={"token": token_upper, "quote": quote_upper, "source": "cache"},
            )
            return cached.result

        # Address-endpoint hits are stored under a chain-scoped key so that
        # the same contract address on two different chains doesn't collide.
        # Check this cache separately before firing any request.
        address_cache_key = self._address_cache_key(resolved_token)
        if address_cache_key is not None:
            cached_addr = self._get_cached(address_cache_key, quote_upper)
            if cached_addr is not None:
                self._metrics.cache_hits += 1
                self._metrics.successful_requests += 1
                return cached_addr.result

        # No proactive sleep before the request. The aggregator fans out
        # sources concurrently; sleeping here would stall every other source
        # waiting on this single slow one. On 429 we give CoinGecko one
        # bounded 1s retry inside the response handler below.

        # Resolve token ID (symbol/address -> CoinGecko ID via static registry)
        token_id = self._resolve_token_id(token_upper)

        # If ID resolution missed but we have a chain + contract address,
        # fall back to CoinGecko's contract-address endpoint. That way a
        # token not in our registry (e.g. cbBTC) is still priceable.
        if token_id is None and resolved_token is not None and address_cache_key is not None:
            address_result = await self._try_fetch_by_address(
                resolved_token,
                address_cache_key,
                quote_upper,
            )
            if address_result is not None:
                return address_result

        if token_id is None:
            # Stablecoin fallback: tokens like FUSDT0, USDbC, etc. may not be
            # listed on CoinGecko but are known USD-pegged stablecoins.
            if token_upper in STABLECOINS and quote_upper == "USD":
                logger.info(f"Token {token_upper} not on CoinGecko, using stablecoin fallback ($1.00)")
                result = PriceResult(
                    price=Decimal("1"),
                    source=f"{self.source_name}/stablecoin_fallback",
                    timestamp=datetime.now(UTC),
                    confidence=0.9,
                    stale=False,
                )
                self._update_cache(token_upper, quote_upper, result, 0.0)
                self._metrics.successful_requests += 1
                return result

            error_msg = f"Unknown token: {token_upper}"
            self._metrics.errors += 1
            self._metrics.last_error = error_msg
            self._metrics.last_error_time = datetime.now(UTC)
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=error_msg,
            )

        # Build API URL
        url = f"{self._api_base}/simple/price"
        params: dict[str, str] = {
            "ids": token_id,
            "vs_currencies": quote_upper.lower(),
        }
        if self._api_key:
            params["x_cg_pro_api_key"] = self._api_key

        # Bounded retry: at most one 1s pause after a 429 before giving up.
        # 1s is enough for CoinGecko free's rate-limit window (~30/min) to
        # roll over in most cases. Hardcoded; longer values compound across
        # decide()'s ~5 price calls and risk the framework's 30s
        # decide_timeout_seconds. Per-attempt logic is in _attempt_id_fetch.
        try:
            for attempt in range(2):
                if attempt > 0:
                    await asyncio.sleep(1.0)

                outcome = await self._attempt_id_fetch(
                    url,
                    params,
                    token_id,
                    token_upper,
                    quote_upper,
                    attempt,
                )
                if outcome is _RETRY:
                    continue
                return outcome  # PriceResult on success or stale fallback

            # Both attempts returned _RETRY (429 twice). Try stale, else raise.
            stale_result = self._stale_fallback_result(token_upper, quote_upper)
            if stale_result is not None:
                logger.info(
                    "Returning stale data for %s/%s due to rate limit",
                    token_upper,
                    quote_upper,
                )
                return stale_result
            raise DataSourceRateLimited(
                source=self.source_name,
                retry_after=self._rate_limit_state.backoff_seconds,
            )

        except TimeoutError as e:
            self._metrics.timeouts += 1
            logger.warning(
                "Timeout fetching %s/%s after %.0fs",
                token_upper,
                quote_upper,
                self._request_timeout,
                extra={
                    "token": token_upper,
                    "quote": quote_upper,
                    "timeout_seconds": self._request_timeout,
                },
            )
            return self._stale_or_raise_unavailable(
                token_upper,
                quote_upper,
                "Returning stale data for %s/%s due to timeout",
                token_upper,
                f"Timeout after {self._request_timeout}s with no cache",
                e,
            )

        except aiohttp.ClientError as e:
            self._metrics.errors += 1
            self._metrics.last_error = str(e)
            self._metrics.last_error_time = datetime.now(UTC)
            logger.error(
                "Network error fetching %s/%s: %s",
                token_upper,
                quote_upper,
                str(e),
            )
            return self._stale_or_raise_unavailable(
                token_upper,
                quote_upper,
                "Returning stale data for %s/%s due to network error",
                token_upper,
                str(e),
                e,
            )

    @staticmethod
    def _address_cache_key(resolved_token: Any) -> str | None:
        """Chain-scoped cache key for address-based CoinGecko lookups.

        Returns something like ``BASE:0xcbb7c0000...`` so the same contract
        address on two different chains caches independently. Returns None
        if ``resolved_token`` doesn't carry a chain + address.
        """
        if resolved_token is None:
            return None
        address = getattr(resolved_token, "address", None)
        chain = getattr(resolved_token, "chain", None)
        if not address or chain is None:
            return None
        chain_key = getattr(chain, "value", chain)
        if not isinstance(chain_key, str):
            return None
        return f"{chain_key.upper()}:{address.lower()}"

    async def _try_fetch_by_address(  # noqa: C901  (response-code branching + bounded retry)
        self,
        resolved_token: Any,
        cache_token_key: str,
        quote_upper: str,
    ) -> PriceResult | None:
        """Fetch price via CoinGecko's contract-address endpoint.

        Return semantics mirror the main `/simple/price` path:
          - ``None``: "not applicable" — no chain context, chain not on
            CoinGecko, or the token is simply absent from the endpoint.
            Caller falls through to the symbol/ID path and the
            "Unknown token" error.
          - ``PriceResult``: success (fresh or stale-from-cache fallback).
          - raises ``DataSourceRateLimited`` / ``DataSourceUnavailable``:
            transient CoinGecko outages that callers should see as real
            failures, not silent token misses.

        Args:
            resolved_token: ResolvedToken with chain + address. Typed as Any
                to avoid an import cycle with the framework data layer.
            cache_token_key: Cache key used elsewhere (uppercased symbol/address),
                so address-endpoint hits participate in the same TTL cache.
            quote_upper: Uppercased quote currency (e.g. "USD").
        """
        address = getattr(resolved_token, "address", None)
        chain = getattr(resolved_token, "chain", None)
        if not address or chain is None:
            return None

        # ResolvedToken.chain is a Chain enum; accept str too for safety.
        chain_key = getattr(chain, "value", chain)
        if not isinstance(chain_key, str):
            return None
        platform = COINGECKO_PLATFORM_IDS.get(chain_key.lower())
        if not platform:
            logger.debug(
                "CoinGecko has no platform mapping for chain %r; skipping address endpoint",
                chain_key,
            )
            return None

        address_lower = address.lower()
        url = f"{self._api_base}/simple/token_price/{platform}"
        params: dict[str, str] = {
            "contract_addresses": address_lower,
            "vs_currencies": quote_upper.lower(),
        }
        if self._api_key:
            params["x_cg_pro_api_key"] = self._api_key

        try:
            # Bounded retry: at most one 1s pause after a 429, then give up.
            # Each attempt's per-response logic lives in _attempt_address_fetch
            # so this orchestration function stays trivial.
            for attempt in range(2):
                if attempt > 0:
                    await asyncio.sleep(1.0)

                outcome = await self._attempt_address_fetch(
                    url,
                    params,
                    address,
                    address_lower,
                    platform,
                    cache_token_key,
                    quote_upper,
                    attempt,
                )
                if outcome is _RETRY:
                    continue
                return outcome  # PriceResult on success/stale, None on miss

            # Both attempts returned _RETRY (429 twice). Try stale, else raise.
            stale_result = self._stale_fallback_result(cache_token_key, quote_upper)
            if stale_result is not None:
                return stale_result
            raise DataSourceRateLimited(
                source=self.source_name,
                retry_after=self._rate_limit_state.backoff_seconds,
            )

        except TimeoutError as e:
            self._metrics.timeouts += 1
            return self._stale_or_raise_unavailable(
                cache_token_key,
                quote_upper,
                "Returning stale data for %s/%s (address endpoint timeout)",
                address_lower,
                f"Address endpoint timeout after {self._request_timeout}s with no cache",
                e,
            )

        except aiohttp.ClientError as e:
            self._metrics.errors += 1
            self._metrics.last_error = str(e)
            self._metrics.last_error_time = datetime.now(UTC)
            return self._stale_or_raise_unavailable(
                cache_token_key,
                quote_upper,
                f"Returning stale data for %s/%s (address endpoint network error: {e})",
                address_lower,
                str(e),
                e,
            )

    async def _attempt_address_fetch(  # noqa: C901  (response-code branching)
        self,
        url: str,
        params: dict[str, str],
        address: Any,
        address_lower: str,
        platform: str,
        cache_token_key: str,
        quote_upper: str,
        attempt: int,
    ) -> "PriceResult | None | _RetrySentinel":
        """Run one HTTP attempt of the address-endpoint fetch.

        Returns:
          - ``PriceResult``: success or stale-cache fallback (on non-200).
          - ``None``: CoinGecko has no listing for this address (normal miss).
          - ``_RETRY`` sentinel: status 429, caller should retry.

        Raises ``DataSourceUnavailable`` for non-200 with no stale cache.
        ``DataSourceRateLimited`` is **not** raised here — the orchestrator in
        ``_try_fetch_by_address`` decides whether to retry or exhaust.
        """
        start_time = time.time()
        session = await self._get_session()
        async with session.get(url, params=params) as response:
            latency_ms = (time.time() - start_time) * 1000

            if response.status == 429:
                # Rate limit is a transient outage, not "unknown token".
                # Caller orchestrates retry vs. exhaust.
                self._rate_limit_state.record_rate_limit()
                self._metrics.rate_limits += 1
                logger.warning(
                    "Rate limited by CoinGecko on address endpoint for %s/%s (attempt %d)",
                    address_lower,
                    quote_upper,
                    attempt + 1,
                )
                return _RETRY

            if response.status != 200:
                # Other HTTP errors — also transient. Try stale cache, then
                # surface as DataSourceUnavailable so the aggregator can
                # fall over to another source cleanly.
                body = await response.text()
                error_msg = f"HTTP {response.status}: {body[:200]}"
                self._metrics.errors += 1
                self._metrics.last_error = error_msg
                self._metrics.last_error_time = datetime.now(UTC)
                logger.info(
                    "CoinGecko address endpoint returned HTTP %s for %s on %s",
                    response.status,
                    address_lower,
                    platform,
                )
                stale_result = self._stale_fallback_result(cache_token_key, quote_upper)
                if stale_result is not None:
                    return stale_result
                raise DataSourceUnavailable(
                    source=self.source_name,
                    reason=error_msg,
                )

            data = await response.json()
            self._rate_limit_state.record_success()

            # Response shape: {"0xabc...": {"usd": 1234.56}}
            # CoinGecko lowercases addresses in its responses.
            entry = data.get(address_lower) or data.get(address) or {}
            quote_lower = quote_upper.lower()
            raw_price = entry.get(quote_lower)
            if raw_price is None:
                # Token genuinely isn't listed — this is a normal miss,
                # not an outage. Return None so the caller's "unknown
                # token" path runs and surfaces a clean error.
                logger.info(
                    "CoinGecko address endpoint had no %s price for %s on %s",
                    quote_upper,
                    address_lower,
                    platform,
                )
                return None

            price = Decimal(str(raw_price))
            # Address-endpoint listings aren't hand-curated like the
            # CoinGecko IDs in our static registry — CoinGecko exposes a
            # price for any token with a listed pool, including thin
            # and spammy ones. Lower confidence matches what DexScreener
            # assigns to similarly "automatic" listings so the aggregator
            # doesn't treat a new low-liquidity token the same as ETH/USDC.
            result = PriceResult(
                price=price,
                source=self.source_name,
                timestamp=datetime.now(UTC),
                confidence=0.85,
                stale=False,
            )
            self._update_cache(cache_token_key, quote_upper, result, latency_ms)
            self._metrics.successful_requests += 1
            self._metrics.total_latency_ms += latency_ms
            logger.debug(
                "Priced %s on %s via CoinGecko address endpoint: %s (latency: %.2fms)",
                address_lower,
                platform,
                price,
                latency_ms,
            )
            return result

    async def _attempt_id_fetch(  # noqa: C901  (response-code branching)
        self,
        url: str,
        params: dict[str, str],
        token_id: str,
        token_upper: str,
        quote_upper: str,
        attempt: int,
    ) -> "PriceResult | _RetrySentinel":
        """Run one HTTP attempt of the `/simple/price` ID-keyed fetch.

        Returns:
          - ``PriceResult``: success or stale-cache fallback (on non-200).
          - ``_RETRY`` sentinel: status 429, caller should retry.

        Raises ``DataSourceUnavailable`` for non-200/null/missing-field with
        no stale cache. ``DataSourceRateLimited`` is **not** raised here —
        the orchestrator in ``get_price`` decides retry vs. exhaust.
        """
        start_time = time.time()
        session = await self._get_session()
        async with session.get(url, params=params) as response:
            latency_ms = (time.time() - start_time) * 1000

            if response.status == 429:
                self._rate_limit_state.record_rate_limit()
                self._metrics.rate_limits += 1
                logger.warning(
                    "Rate limited by CoinGecko for %s/%s (attempt %d)",
                    token_upper,
                    quote_upper,
                    attempt + 1,
                    extra={
                        "token": token_upper,
                        "quote": quote_upper,
                        "consecutive_429s": self._rate_limit_state.consecutive_429s,
                    },
                )
                return _RETRY

            if response.status != 200:
                error_msg = f"HTTP {response.status}: {await response.text()}"
                self._metrics.errors += 1
                self._metrics.last_error = error_msg
                self._metrics.last_error_time = datetime.now(UTC)
                logger.error(
                    "CoinGecko API error for %s/%s: %s",
                    token_upper,
                    quote_upper,
                    error_msg,
                )
                stale_result = self._stale_fallback_result(token_upper, quote_upper)
                if stale_result is not None:
                    logger.info(
                        "Returning stale data for %s/%s due to API error",
                        token_upper,
                        quote_upper,
                    )
                    return stale_result
                raise DataSourceUnavailable(
                    source=self.source_name,
                    reason=error_msg,
                )

            data = await response.json()
            self._rate_limit_state.record_success()

            # Response format: {"token_id": {"usd": 1234.56}}
            quote_lower = quote_upper.lower()
            raw_price = self._extract_id_price(data, token_id, quote_lower, quote_upper)
            price = Decimal(str(raw_price))

            result = PriceResult(
                price=price,
                source=self.source_name,
                timestamp=datetime.now(UTC),
                confidence=1.0,
                stale=False,
            )
            self._update_cache(token_upper, quote_upper, result, latency_ms)
            self._metrics.successful_requests += 1
            self._metrics.total_latency_ms += latency_ms
            logger.debug(
                "Fetched price for %s/%s: %s (latency: %.2fms)",
                token_upper,
                quote_upper,
                price,
                latency_ms,
            )
            return result

    def _extract_id_price(
        self,
        data: dict[str, Any],
        token_id: str,
        quote_lower: str,
        quote_upper: str,
    ) -> Any:
        """Validate `/simple/price` response shape and return the raw price.

        Raises ``DataSourceUnavailable`` if the token id is missing, the quote
        key is missing under it, or the price value is null. ``Decimal(str(None))``
        would raise ``InvalidOperation``, so the null-value guard surfaces a
        clean source-level error instead of a downstream decimal crash.
        """
        if token_id not in data:
            error_msg = f"Token {token_id} not in response"
            self._metrics.errors += 1
            self._metrics.last_error = error_msg
            self._metrics.last_error_time = datetime.now(UTC)
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=error_msg,
            )

        if quote_lower not in data[token_id]:
            error_msg = f"Quote {quote_upper} not in response for {token_id}"
            self._metrics.errors += 1
            self._metrics.last_error = error_msg
            self._metrics.last_error_time = datetime.now(UTC)
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=error_msg,
            )

        raw_price = data[token_id][quote_lower]
        if raw_price is None:
            error_msg = f"Price for {token_id}/{quote_upper} is null in response"
            self._metrics.errors += 1
            self._metrics.last_error = error_msg
            self._metrics.last_error_time = datetime.now(UTC)
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=error_msg,
            )

        return raw_price

    @property
    def source_name(self) -> str:
        """Return the unique name of this data source."""
        return "coingecko"

    @property
    def supported_tokens(self) -> list[str]:
        """Return list of supported tokens across all chains."""
        return sorted(GLOBAL_TOKEN_IDS.keys())

    @property
    def cache_ttl_seconds(self) -> int:
        """Return the cache TTL for this source."""
        return self._cache_ttl

    def get_health_metrics(self) -> dict[str, Any]:
        """Get current health metrics for observability."""
        return self._metrics.to_dict()

    async def health_check(self) -> bool:
        """Check if the data source is healthy and responding.

        Returns:
            True if source is healthy, False otherwise
        """
        try:
            await self.get_price("ETH", "USD")
            return True
        except Exception as e:
            logger.warning("Health check failed: %s", str(e))
            return False

    def clear_cache(self) -> None:
        """Clear the price cache."""
        self._cache.clear()
        logger.info("Cleared CoinGecko price cache")

    async def __aenter__(self) -> "CoinGeckoPriceSource":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()
