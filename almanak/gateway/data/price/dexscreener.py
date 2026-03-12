"""DexScreener Price Source.

Uses the DexScreener API to fetch real-time DEX prices for tokens.
Particularly useful for meme coins and long-tail tokens that may not
have Pyth feeds or CoinGecko listings.

No API key required. Rate limit: 300 requests/minute.
"""

import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

import aiohttp

from almanak.framework.data.interfaces import (
    BasePriceSource,
    DataSourceUnavailable,
    PriceResult,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.dexscreener.com"

# Well-known Solana token addresses for direct lookup (faster than search)
_SOLANA_TOKEN_ADDRESSES: dict[str, str] = {
    "SOL": "So11111111111111111111111111111111111111112",
    "WSOL": "So11111111111111111111111111111111111111112",
    "USDC": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
    "USDT": "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",
    "JUP": "JUPyiwrYJFskUPiHa7hkeR8VUtAeFoSYbKedZNsDvCN",
    "RAY": "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R",
    "BONK": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
    "WIF": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
    "JTO": "jtojtomepa8beP8AuQc6eXt5FriJwfFMwQx2v2f9mCL",
    "ORCA": "orcaEKTdK7LKz57vaAYr9QeNsVEPfiu6QeMU1kektZE",
    "MSOL": "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So",
    "JITOSOL": "J1toso1uCk3RLmjorhTtrVwY9HJ7X8V9yYac6Y7kGCPn",
    "PYTH": "HZ1JovNiVvGrGNiiYvEozEVgZ58xaU3RKwX8eACQBCt3",
}


@dataclass
class _CacheEntry:
    """Cache entry for a DexScreener price result."""

    result: PriceResult
    cached_at: float


class DexScreenerPriceSource(BasePriceSource):
    """Price source using DexScreener DEX pair data.

    Fetches prices from DexScreener's REST API by looking up the highest-
    liquidity pair for a token. Works for any token with an active DEX
    pair -- especially useful for meme coins and long-tail tokens.

    Args:
        chain_id: Default chain to search (e.g., "solana", "ethereum").
        cache_ttl: Cache TTL in seconds.
        request_timeout: HTTP request timeout in seconds.
        min_liquidity_usd: Minimum pool liquidity to trust the price.
    """

    def __init__(
        self,
        chain_id: str = "solana",
        cache_ttl: int = 30,
        request_timeout: float = 10.0,
        min_liquidity_usd: float = 10_000,
        stale_confidence: float = 0.6,
    ) -> None:
        self._chain_id = chain_id
        self._cache_ttl = cache_ttl
        self._request_timeout = request_timeout
        self._min_liquidity_usd = min_liquidity_usd
        self._stale_confidence = stale_confidence
        self._cache: dict[str, _CacheEntry] = {}
        self._session: aiohttp.ClientSession | None = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self._request_timeout),
            )
        return self._session

    async def close(self) -> None:
        """Close the HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    @property
    def source_name(self) -> str:
        return "dexscreener"

    @property
    def supported_tokens(self) -> list[str]:
        return list(_SOLANA_TOKEN_ADDRESSES.keys())

    @property
    def cache_ttl_seconds(self) -> int:
        return self._cache_ttl

    async def get_price(self, token: str, quote: str = "USD") -> PriceResult:
        """Fetch the current price for a token from DexScreener.

        Looks up the highest-liquidity pair for the token and returns
        the USD price from that pair.

        Args:
            token: Token symbol (e.g., "BONK", "WIF") or address.
            quote: Quote currency (only "USD" effectively supported).

        Returns:
            PriceResult with price and metadata.

        Raises:
            DataSourceUnavailable: If no pair found or API unreachable.
        """
        token_upper = token.upper()
        cache_key = f"{token_upper}/{quote}"

        # Check fresh cache
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        try:
            result = await self._fetch_price(token_upper)
            self._cache[cache_key] = _CacheEntry(result=result, cached_at=time.time())
            return result
        except DataSourceUnavailable:
            raise
        except Exception as e:
            # Try stale cache
            stale = self._get_stale_cached(cache_key)
            if stale is not None:
                logger.warning("DexScreener fetch failed for %s, using stale cache: %s", token_upper, e)
                return stale
            raise DataSourceUnavailable(
                source="dexscreener",
                reason=f"Fetch failed for {token_upper}: {e}",
            ) from e

    async def _fetch_price(self, token: str) -> PriceResult:
        """Fetch price for a single token from DexScreener."""
        session = await self._get_session()

        # Try direct address lookup first (faster, more precise)
        address = _SOLANA_TOKEN_ADDRESSES.get(token)
        if address:
            pairs = await self._fetch_token_pairs(session, self._chain_id, address)
        else:
            # Might be a raw address or unknown symbol -- try search
            pairs = await self._search_pairs(session, token)

        if not pairs:
            raise DataSourceUnavailable(
                source="dexscreener",
                reason=f"No pairs found for '{token}' on {self._chain_id}",
            )

        # Filter to our chain and pick highest-liquidity pair
        chain_pairs = [p for p in pairs if p.get("chainId") == self._chain_id]
        if not chain_pairs:
            chain_pairs = pairs  # Fall back to all chains

        best = self._pick_best_pair(chain_pairs)
        if best is None:
            raise DataSourceUnavailable(
                source="dexscreener",
                reason=f"No liquid pair for '{token}' (min ${self._min_liquidity_usd})",
            )

        price_str = best.get("priceUsd", "0")
        try:
            price = Decimal(str(price_str))
        except Exception as e:
            raise DataSourceUnavailable(
                source="dexscreener",
                reason=f"Invalid price '{price_str}' for {token}",
            ) from e

        if price <= 0:
            raise DataSourceUnavailable(
                source="dexscreener",
                reason=f"Zero/negative price for {token}",
            )

        confidence = self._calculate_confidence(best)

        return PriceResult(
            price=price,
            source="dexscreener",
            timestamp=datetime.now(UTC),
            confidence=confidence,
            stale=False,
        )

    async def _fetch_token_pairs(self, session: aiohttp.ClientSession, chain_id: str, address: str) -> list[dict]:
        """Fetch pairs for a token by address."""
        url = f"{BASE_URL}/token-pairs/v1/{chain_id}/{address}"
        async with session.get(url) as response:
            if response.status != 200:
                return []
            data = await response.json()
            return data if isinstance(data, list) else data.get("pairs", []) or []

    async def _search_pairs(self, session: aiohttp.ClientSession, query: str) -> list[dict]:
        """Search for pairs by token name/symbol/address."""
        url = f"{BASE_URL}/latest/dex/search"
        async with session.get(url, params={"q": query}) as response:
            if response.status != 200:
                return []
            data = await response.json()
            return data.get("pairs", []) or []

    def _pick_best_pair(self, pairs: list[dict]) -> dict | None:
        """Pick the best pair from a list, preferring high liquidity."""
        valid = []
        for p in pairs:
            liq = (p.get("liquidity") or {}).get("usd", 0)
            try:
                liq = float(liq) if liq else 0
            except (ValueError, TypeError):
                liq = 0
            if liq >= self._min_liquidity_usd and p.get("priceUsd"):
                valid.append((liq, p))

        if not valid:
            return None

        valid.sort(key=lambda x: x[0], reverse=True)
        return valid[0][1]

    def _calculate_confidence(self, pair: dict) -> float:
        """Calculate confidence score based on pair quality."""
        confidence = 0.85  # Base confidence for DEX prices (less reliable than oracles)

        liq = float((pair.get("liquidity") or {}).get("usd", 0) or 0)
        vol = float((pair.get("volume") or {}).get("h24", 0) or 0)

        # High liquidity boost
        if liq >= 1_000_000:
            confidence = 0.95
        elif liq >= 100_000:
            confidence = 0.9

        # Low volume penalty
        if vol < 10_000:
            confidence -= 0.1

        return max(0.3, min(1.0, confidence))

    def _get_cached(self, key: str) -> PriceResult | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        if time.time() - entry.cached_at < self._cache_ttl:
            return entry.result
        return None

    def _get_stale_cached(self, key: str) -> PriceResult | None:
        entry = self._cache.get(key)
        if entry is None:
            return None
        return PriceResult(
            price=entry.result.price,
            source="dexscreener",
            timestamp=entry.result.timestamp,
            confidence=self._stale_confidence,
            stale=True,
        )

    async def health_check(self) -> bool:
        """Check if DexScreener API is reachable."""
        try:
            await self.get_price("SOL", "USD")
            return True
        except Exception:
            return False


__all__ = ["DexScreenerPriceSource"]
