"""Binance price source for the gateway price aggregator.

Uses the Binance public ticker API (no API key required) to fetch token prices.
Very generous rate limits (1200 req/min) compared to CoinGecko free tier.

This is especially useful for chains without Chainlink feeds (e.g., Mantle)
where CoinGecko free tier rate-limits aggressively.
"""

import asyncio
import logging
import time
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation

import aiohttp

from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceUnavailable,
    PriceResult,
)
from almanak.gateway.utils.ssl_context import build_ssl_context

logger = logging.getLogger(__name__)

# Map token symbols to Binance trading pairs (quoted in USDT)
# Binance uses "ETHUSDT", "BTCUSDT", etc.
_TOKEN_TO_BINANCE_SYMBOL: dict[str, str] = {
    "ETH": "ETHUSDT",
    "WETH": "ETHUSDT",
    "BTC": "BTCUSDT",
    "WBTC": "BTCUSDT",
    "SOL": "SOLUSDT",
    "ARB": "ARBUSDT",
    "AVAX": "AVAXUSDT",
    "WAVAX": "AVAXUSDT",
    "MATIC": "MATICUSDT",
    "WMATIC": "MATICUSDT",
    "BNB": "BNBUSDT",
    "WBNB": "BNBUSDT",
    "LINK": "LINKUSDT",
    "UNI": "UNIUSDT",
    "AAVE": "AAVEUSDT",
    # Note: Mantle (MNT) is NOT listed on Binance spot.
    # MANTA and MANTRA are different tokens. MNT prices come from CoinGecko/on-chain.
    "OP": "OPUSDT",
    "GMX": "GMXUSDT",
    "PENDLE": "PENDLEUSDT",
    "LDO": "LDOUSDT",
    "S": "SUSDT",
    "DOGE": "DOGEUSDT",
    "FTM": "FTMUSDT",
    "CAKE": "CAKEUSDT",
    "JOE": "JOEUSDT",
    "OKB": "OKBUSDT",
    "WOKB": "OKBUSDT",
    "xETH": "ETHUSDT",
    "XETH": "ETHUSDT",
    "xBTC": "BTCUSDT",
    "XBTC": "BTCUSDT",
}

# Stablecoins that are always $1
_STABLECOINS = {"USDC", "USDT", "DAI", "USDC.E", "USDT.E", "USDBC", "BUSD", "USDE", "USDT0", "USDG", "GHO"}


# Quote currencies to try when dynamically resolving unknown tokens (VIB-645).
# Order matters: USDT is the most common, USDC and FDUSD are fallbacks.
_DYNAMIC_QUOTE_CANDIDATES = ("USDT", "USDC", "FDUSD")

# TTL for negative cache entries (tokens confirmed not on Binance).
_NEGATIVE_CACHE_TTL = 4 * 3600  # 4 hours


class BinancePriceSource(BasePriceSource):
    """Binance public API price source.

    Uses the /api/v3/ticker/price endpoint (no API key needed).
    Rate limit: 1200 req/min (very generous).

    For tokens not in the static symbol map, attempts dynamic resolution
    by probing {SYMBOL}USDT, {SYMBOL}USDC, {SYMBOL}FDUSD against the API.
    Successful lookups are cached; failures are negative-cached for 4h.
    """

    _API_BASE = "https://api.binance.com"

    def __init__(self, cache_ttl: int = 30, request_timeout: float = 5.0) -> None:
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._cache: dict[str, tuple[PriceResult, float]] = {}
        self._session: aiohttp.ClientSession | None = None
        self._session_loop: asyncio.AbstractEventLoop | None = None
        # Dynamic resolution caches (VIB-645)
        self._dynamic_symbol_cache: dict[str, str] = {}  # TOKEN -> resolved Binance symbol
        self._negative_cache: dict[str, float] = {}  # TOKEN -> timestamp of failed lookup
        logger.info("Initialized BinancePriceSource (cache_ttl=%ds)", cache_ttl)

    async def _get_session(self) -> aiohttp.ClientSession:
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
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
            self._session_loop = None

    async def get_price(self, token: str, quote: str = "USD", *, resolved_token: object | None = None) -> PriceResult:
        quote_upper = quote.upper()

        # Binance pairs are quoted in USDT. Accept "USD" and "USDT" as equivalent;
        # reject any other quote currency to avoid mislabeled prices.
        if quote_upper not in ("USD", "USDT"):
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Binance only supports USD/USDT quotes, got '{quote}'",
            )

        token_upper = token.upper()

        # Stablecoins
        if token_upper in _STABLECOINS:
            return PriceResult(
                price=Decimal("1.0"),
                source=self.source_name,
                timestamp=datetime.now(UTC),
                confidence=1.0,
            )

        # Check cache
        cache_key = f"{token_upper}/{quote}"
        if cache_key in self._cache:
            result, cached_at = self._cache[cache_key]
            if time.time() - cached_at < self._cache_ttl:
                return result

        # Look up Binance symbol: static map -> dynamic cache -> dynamic resolve
        binance_symbol = _TOKEN_TO_BINANCE_SYMBOL.get(token_upper)
        confidence = 1.0

        if not binance_symbol:
            binance_symbol = self._dynamic_symbol_cache.get(token_upper)
            if binance_symbol:
                confidence = 0.9  # dynamically resolved, slightly lower confidence

        if not binance_symbol:
            # Check negative cache before probing
            neg_ts = self._negative_cache.get(token_upper)
            if neg_ts and (time.time() - neg_ts) < _NEGATIVE_CACHE_TTL:
                raise DataSourceUnavailable(
                    source=self.source_name,
                    reason=f"Token '{token_upper}' not available on Binance (negative-cached)",
                )
            # Dynamic resolution: probe candidate pairs (VIB-645)
            binance_symbol = await self._resolve_binance_symbol(token_upper)
            if binance_symbol:
                confidence = 0.9
            else:
                raise DataSourceUnavailable(
                    source=self.source_name,
                    reason=f"Token '{token_upper}' not found on Binance (tried {', '.join(_DYNAMIC_QUOTE_CANDIDATES)})",
                )

        try:
            result = await self._fetch_price(binance_symbol, cache_key, confidence)
            return result
        except DataSourceUnavailable:
            raise
        except (aiohttp.ClientError, TimeoutError, InvalidOperation, ValueError, TypeError, KeyError) as e:
            # Evict dynamic cache on API error (the pair may have been delisted)
            if token_upper in self._dynamic_symbol_cache:
                del self._dynamic_symbol_cache[token_upper]
                logger.warning("Evicted dynamic Binance mapping for %s after API error: %s", token_upper, e)
            # Check for stale cache
            if cache_key in self._cache:
                stale_result, _ = self._cache[cache_key]
                return PriceResult(
                    price=stale_result.price,
                    source=self.source_name,
                    timestamp=stale_result.timestamp,
                    confidence=0.7,
                    stale=True,
                )
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Binance request failed: {e}",
            ) from e

    async def _fetch_price(self, binance_symbol: str, cache_key: str, confidence: float) -> PriceResult:
        """Fetch a price from Binance for a given trading pair symbol."""
        session = await self._get_session()
        url = f"{self._API_BASE}/api/v3/ticker/price?symbol={binance_symbol}"
        async with session.get(url) as resp:
            if resp.status in (429, 418) or resp.status >= 500:
                text = await resp.text()
                raise DataSourceUnavailable(
                    source=self.source_name,
                    reason=f"Binance API transient error {resp.status}: {text}",
                )
            if resp.status != 200:
                text = await resp.text()
                raise ValueError(f"Binance API returned {resp.status}: {text}")
            data = await resp.json()
            price_str = data.get("price")
            if price_str is None:
                raise ValueError("Binance response missing 'price' field")
            price = Decimal(str(price_str))

            result = PriceResult(
                price=price,
                source=self.source_name,
                timestamp=datetime.now(UTC),
                confidence=confidence,
            )
            self._cache[cache_key] = (result, time.time())
            return result

    async def _resolve_binance_symbol(self, token: str) -> str | None:
        """Try to find a valid Binance trading pair for an unknown token.

        Probes {TOKEN}USDT, {TOKEN}USDC, {TOKEN}FDUSD against the ticker API.
        Returns the first valid pair, or None if no pair exists.
        Successful lookups are cached in _dynamic_symbol_cache.
        """
        # Guard against non-alphanumeric tokens being injected into URLs
        if not token.isalnum():
            return None

        session = await self._get_session()
        saw_transient_error = False
        for quote in _DYNAMIC_QUOTE_CANDIDATES:
            candidate = f"{token}{quote}"
            url = f"{self._API_BASE}/api/v3/ticker/price?symbol={candidate}"
            try:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        if data.get("price"):
                            self._dynamic_symbol_cache[token] = candidate
                            logger.warning(
                                "Dynamically resolved Binance pair for %s -> %s (not in static map)",
                                token,
                                candidate,
                            )
                            return candidate
                    elif resp.status in (429, 418) or resp.status >= 500:
                        saw_transient_error = True
                        continue
            except (aiohttp.ClientError, TimeoutError):
                saw_transient_error = True
                continue
        # Only negative-cache if all probes got definitive 4xx (not-found) responses
        if not saw_transient_error:
            self._negative_cache[token] = time.time()
        else:
            logger.debug("Skipping negative cache for %s due to transient errors during probe", token)
        return None

    @property
    def source_name(self) -> str:
        return "binance"

    @property
    def supported_tokens(self) -> list[str]:
        return list(_TOKEN_TO_BINANCE_SYMBOL.keys()) + list(_STABLECOINS)

    @property
    def cache_ttl_seconds(self) -> int:
        return self._cache_ttl
