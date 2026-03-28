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
}

# Stablecoins that are always $1
_STABLECOINS = {"USDC", "USDT", "DAI", "USDC.E", "USDT.E", "USDBC", "BUSD", "USDE"}


class BinancePriceSource(BasePriceSource):
    """Binance public API price source.

    Uses the /api/v3/ticker/price endpoint (no API key needed).
    Rate limit: 1200 req/min (very generous).
    """

    _API_BASE = "https://api.binance.com"

    def __init__(self, cache_ttl: int = 30, request_timeout: float = 5.0) -> None:
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._cache: dict[str, tuple[PriceResult, float]] = {}
        self._session: aiohttp.ClientSession | None = None
        self._session_loop: asyncio.AbstractEventLoop | None = None
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
            self._session = aiohttp.ClientSession(timeout=timeout)
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

        # Look up Binance symbol
        binance_symbol = _TOKEN_TO_BINANCE_SYMBOL.get(token_upper)
        if not binance_symbol:
            raise DataSourceUnavailable(
                source=self.source_name,
                reason=f"Token '{token_upper}' not mapped to a Binance pair",
            )

        try:
            session = await self._get_session()
            url = f"{self._API_BASE}/api/v3/ticker/price?symbol={binance_symbol}"
            async with session.get(url) as resp:
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
                    confidence=1.0,
                )
                self._cache[cache_key] = (result, time.time())
                return result

        except (aiohttp.ClientError, TimeoutError, InvalidOperation, ValueError, TypeError, KeyError) as e:
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

    @property
    def source_name(self) -> str:
        return "binance"

    @property
    def supported_tokens(self) -> list[str]:
        return list(_TOKEN_TO_BINANCE_SYMBOL.keys()) + list(_STABLECOINS)

    @property
    def cache_ttl_seconds(self) -> int:
        return self._cache_ttl
