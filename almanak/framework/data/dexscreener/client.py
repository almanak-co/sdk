"""DexScreener API client.

Async HTTP client for the DexScreener REST API. Provides access to
DEX pair data, token search, boosted/trending tokens, and more.

Free tier (no API key required):
  - Pair endpoints: 300 requests/minute
  - Token profile/boost endpoints: 60 requests/minute

Example::

    async with DexScreenerClient() as client:
        pairs = await client.search_pairs("BONK")
        for pair in pairs:
            print(f"{pair.base_token.symbol}: ${pair.price_usd}")
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any
from urllib.parse import quote_plus

import aiohttp

from .models import BoostedToken, DexPair, parse_boosted_token, parse_pair

logger = logging.getLogger(__name__)

BASE_URL = "https://api.dexscreener.com"

# Rate limit windows (seconds per request)
_PAIR_RATE_LIMIT = 60 / 300  # 300 req/min -> 0.2s
_PROFILE_RATE_LIMIT = 60 / 60  # 60 req/min -> 1.0s


class DexScreenerError(Exception):
    """Base exception for DexScreener API errors."""


class DexScreenerRateLimited(DexScreenerError):
    """Raised when rate-limited by DexScreener."""


class DexScreenerClient:
    """Async client for the DexScreener REST API.

    Args:
        request_timeout: HTTP request timeout in seconds.
        cache_ttl: Default cache TTL in seconds.
    """

    def __init__(
        self,
        request_timeout: float = 10.0,
        cache_ttl: int = 30,
    ) -> None:
        self._request_timeout = request_timeout
        self._cache_ttl = cache_ttl
        self._session: aiohttp.ClientSession | None = None
        self._cache: dict[str, tuple[float, object]] = {}
        self._last_pair_request: float = 0.0
        self._last_profile_request: float = 0.0

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

    async def __aenter__(self) -> DexScreenerClient:
        return self

    async def __aexit__(self, *args) -> None:
        await self.close()

    # -----------------------------------------------------------------------
    # Core API methods
    # -----------------------------------------------------------------------

    async def search_pairs(self, query: str) -> list[DexPair]:
        """Search for trading pairs by token name, symbol, or address.

        Returns up to ~40 pairs across all chains. No pagination.

        Args:
            query: Search term (e.g., "BONK", "SOL/USDC", or a token address).
        """
        data = await self._get_pair_endpoint(f"/latest/dex/search?q={quote_plus(query)}")
        return [parse_pair(p) for p in data.get("pairs", []) or []]

    async def get_token_pairs(self, chain_id: str, token_address: str) -> list[DexPair]:
        """Get all trading pairs for a token on a specific chain.

        Returns up to 30 pairs where the token is base or quote.

        Args:
            chain_id: Chain identifier (e.g., "solana", "ethereum").
            token_address: Token contract address.
        """
        data = await self._get_pair_endpoint(f"/token-pairs/v1/{chain_id}/{token_address}")
        if isinstance(data, list):
            return [parse_pair(p) for p in data]
        return [parse_pair(p) for p in data.get("pairs", []) or []]

    async def get_tokens(self, chain_id: str, token_addresses: list[str]) -> list[DexPair]:
        """Batch lookup tokens by address on a specific chain.

        Args:
            chain_id: Chain identifier.
            token_addresses: Up to 30 token addresses.
        """
        if not token_addresses:
            return []
        addrs = ",".join(token_addresses[:30])
        data = await self._get_pair_endpoint(f"/tokens/v1/{chain_id}/{addrs}")
        if isinstance(data, list):
            return [parse_pair(p) for p in data]
        return [parse_pair(p) for p in data.get("pairs", []) or []]

    async def get_pair(self, chain_id: str, pair_address: str) -> DexPair | None:
        """Get a specific trading pair by chain and pair address.

        Args:
            chain_id: Chain identifier (e.g., "solana").
            pair_address: Pool/pair contract address.
        """
        data = await self._get_pair_endpoint(f"/latest/dex/pairs/{chain_id}/{pair_address}")
        pair_data = data.get("pair") or (data.get("pairs", []) or [None])[0]
        if pair_data:
            return parse_pair(pair_data)
        return None

    async def get_top_boosts(self) -> list[BoostedToken]:
        """Get tokens ranked by active boost count (most boosted first)."""
        data = await self._get_profile_endpoint("/token-boosts/top/v1")
        if isinstance(data, list):
            return [parse_boosted_token(t) for t in data]
        return []

    async def get_latest_boosts(self) -> list[BoostedToken]:
        """Get the most recently boosted tokens."""
        data = await self._get_profile_endpoint("/token-boosts/latest/v1")
        if isinstance(data, list):
            return [parse_boosted_token(t) for t in data]
        return []

    async def get_latest_profiles(self) -> list[dict]:
        """Get the most recently updated token profiles.

        Returns raw profile dicts (chainId, tokenAddress, icon, links, etc.).
        """
        data = await self._get_profile_endpoint("/token-profiles/latest/v1")
        if isinstance(data, list):
            return data
        return []

    # -----------------------------------------------------------------------
    # Convenience: get best pair for a token on Solana
    # -----------------------------------------------------------------------

    async def get_best_solana_pair(self, token_symbol_or_address: str) -> DexPair | None:
        """Find the highest-liquidity Solana pair for a token.

        Searches by symbol or address, filters to Solana, picks the pair
        with the most USD liquidity.

        Args:
            token_symbol_or_address: Token symbol (e.g., "BONK") or mint address.
        """
        pairs = await self.search_pairs(token_symbol_or_address)
        solana_pairs = [p for p in pairs if p.chain_id == "solana"]
        if not solana_pairs:
            return None
        return max(solana_pairs, key=lambda p: p.liquidity.usd)

    async def get_solana_meme_candidates(
        self,
        *,
        min_liquidity_usd: float = 50_000,
        min_volume_h24: float = 100_000,
        min_age_hours: float = 1.0,
        max_age_hours: float = 168.0,  # 7 days
        limit: int = 20,
    ) -> list[DexPair]:
        """Screen for tradeable Solana meme coin candidates.

        Combines boosted tokens + search to find meme coins that pass
        basic tradability filters (liquidity, volume, age).

        Args:
            min_liquidity_usd: Minimum pool liquidity in USD.
            min_volume_h24: Minimum 24h volume in USD.
            min_age_hours: Minimum pair age in hours (avoid rug-pulls).
            max_age_hours: Maximum pair age in hours.
            limit: Max results to return.

        Returns:
            List of DexPair sorted by 24h volume descending.
        """
        # Gather candidates from boosted tokens
        boosted = await self.get_top_boosts()
        solana_boosts = [b for b in boosted if b.chain_id == "solana"]

        # Look up pair data for boosted tokens
        candidates: list[DexPair] = []
        if solana_boosts:
            addresses = [b.token_address for b in solana_boosts[:15]]
            pairs = await self.get_tokens("solana", addresses)
            candidates.extend(pairs)

        # Also search for common meme keywords
        for keyword in ["meme solana", "BONK", "WIF", "PEPE solana"]:
            try:
                results = await self.search_pairs(keyword)
                candidates.extend(p for p in results if p.chain_id == "solana")
            except DexScreenerError as exc:
                logger.debug("DexScreener search failed for keyword '%s': %s", keyword, exc)
                continue

        # Deduplicate by pair address
        seen = set()
        unique: list[DexPair] = []
        for p in candidates:
            if p.pair_address not in seen:
                seen.add(p.pair_address)
                unique.append(p)

        # Filter by tradability criteria
        filtered = []
        for p in unique:
            if p.liquidity.usd < min_liquidity_usd:
                continue
            if p.volume.h24 < min_volume_h24:
                continue
            age = p.age_hours
            if age is not None and (age < min_age_hours or age > max_age_hours):
                continue
            if p.price_usd_float <= 0:
                continue
            # Skip stablecoins
            symbol = p.base_token.symbol.upper()
            if symbol in ("USDC", "USDT", "DAI", "USDS", "PYUSD"):
                continue
            filtered.append(p)

        # Sort by volume descending
        filtered.sort(key=lambda p: p.volume.h24, reverse=True)
        return filtered[:limit]

    # -----------------------------------------------------------------------
    # Internal HTTP methods with rate limiting + caching
    # -----------------------------------------------------------------------

    async def _get_pair_endpoint(self, path: str) -> Any:
        """GET a pair-category endpoint (300 req/min limit)."""
        return await self._request(path, rate_attr="_last_pair_request", rate_limit=_PAIR_RATE_LIMIT)

    async def _get_profile_endpoint(self, path: str) -> Any:
        """GET a profile/boost-category endpoint (60 req/min limit)."""
        return await self._request(path, rate_attr="_last_profile_request", rate_limit=_PROFILE_RATE_LIMIT)

    async def _request(self, path: str, *, rate_attr: str, rate_limit: float) -> Any:
        """Make a rate-limited, cached HTTP GET request."""
        # Check cache
        cached = self._cache.get(path)
        if cached is not None:
            cached_at, cached_data = cached
            if time.time() - cached_at < self._cache_ttl:
                return cached_data

        # Rate limit
        last = getattr(self, rate_attr)
        elapsed = time.time() - last
        if elapsed < rate_limit:
            await asyncio.sleep(rate_limit - elapsed)

        session = await self._get_session()
        url = f"{BASE_URL}{path}"

        try:
            async with session.get(url) as response:
                setattr(self, rate_attr, time.time())

                if response.status == 429:
                    raise DexScreenerRateLimited(f"Rate limited on {path}")

                if response.status != 200:
                    text = await response.text()
                    raise DexScreenerError(f"HTTP {response.status}: {text[:200]}")

                data = await response.json()
                self._cache[path] = (time.time(), data)
                return data
        except aiohttp.ClientError as e:
            # Return stale cache on network error
            if cached is not None:
                logger.warning("DexScreener request failed, using stale cache: %s", e)
                return cached[1]
            raise DexScreenerError(f"Request failed: {e}") from e
