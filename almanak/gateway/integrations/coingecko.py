"""CoinGecko integration for gateway.

Provides access to CoinGecko price and market data through the gateway:
- Token prices in various currencies
- Market data (market cap, volume, etc.)
- Token lists and search

Supports both free and pro API endpoints based on API key availability.
"""

import logging
import os
from typing import Any

from pydantic import BaseModel, Field

from almanak.gateway.integrations.base import BaseIntegration

logger = logging.getLogger(__name__)


# =============================================================================
# Pydantic Models for API Response Validation
# =============================================================================


class CoinGeckoHistoricalPrice(BaseModel):
    """Validated response for historical price data."""

    price_usd: str = "0"
    market_cap_usd: str = "0"
    volume_usd: str = "0"


class CoinGeckoMarketChartRange(BaseModel):
    """Validated response for market chart range data."""

    prices: list[list[float | int]] = Field(default_factory=list)
    market_caps: list[list[float | int]] = Field(default_factory=list)
    total_volumes: list[list[float | int]] = Field(default_factory=list)


class CoinGeckoIntegration(BaseIntegration):
    """CoinGecko price and market data integration.

    Provides access to CoinGecko API for price and market data.
    Rate limits depend on API tier:
    - Free: 10-50 requests per minute
    - Pro: 500 requests per minute

    Supported endpoints:
    - get_price: Get price for a single token
    - get_prices: Get prices for multiple tokens
    - get_markets: Get market data with rankings

    Example:
        integration = CoinGeckoIntegration(api_key="your-api-key")
        price = await integration.get_price("ethereum", vs_currencies=["usd", "eur"])
        markets = await integration.get_markets(vs_currency="usd", per_page=100)
    """

    name = "coingecko"
    rate_limit_requests = 50  # Conservative default for free tier
    default_cache_ttl = 30  # 30 second cache for price data

    # API endpoints
    _FREE_API_BASE = "https://api.coingecko.com/api/v3"
    _PRO_API_BASE = "https://pro-api.coingecko.com/api/v3"

    def __init__(
        self,
        api_key: str | None = None,
        request_timeout: float = 30.0,
    ):
        """Initialize CoinGecko integration.

        Args:
            api_key: Optional CoinGecko API key. Uses pro API if provided.
            request_timeout: HTTP request timeout in seconds
        """
        # Check for API key in environment
        api_key = api_key or os.environ.get("COINGECKO_API_KEY")

        # Select API base and rate limit based on API key
        if api_key:
            base_url = self._PRO_API_BASE
            self.rate_limit_requests = 500  # Pro tier limit
        else:
            base_url = self._FREE_API_BASE
            self.rate_limit_requests = 30  # Conservative free tier

        super().__init__(
            api_key=api_key,
            base_url=base_url,
            request_timeout=request_timeout,
        )

        logger.info(
            "Initialized CoinGecko integration (tier: %s)",
            "pro" if api_key else "free",
        )

    def _get_headers(self) -> dict[str, str]:
        """Get headers for CoinGecko API requests."""
        headers = super()._get_headers()
        if self._api_key:
            headers["x-cg-pro-api-key"] = self._api_key
        return headers

    async def health_check(self) -> bool:
        """Check if CoinGecko API is healthy.

        Returns:
            True if API is responding, False otherwise
        """
        try:
            await self._fetch("/ping")
            return True
        except Exception as e:
            logger.warning("CoinGecko health check failed: %s", e)
            return False

    async def get_price(
        self,
        token_id: str,
        vs_currencies: list[str] | None = None,
    ) -> dict[str, str]:
        """Get price for a single token.

        Args:
            token_id: CoinGecko token ID (e.g., "ethereum", "bitcoin")
            vs_currencies: Quote currencies (e.g., ["usd", "eur"]). Defaults to ["usd"]

        Returns:
            Dictionary mapping currency to price as string

        Raises:
            IntegrationError: On API errors
        """
        vs_currencies = vs_currencies or ["usd"]
        vs_str = ",".join([c.lower() for c in vs_currencies])

        # Check cache
        cache_key = f"price:{token_id}:{vs_str}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        data = await self._fetch(
            "/simple/price",
            params={
                "ids": token_id.lower(),
                "vs_currencies": vs_str,
            },
        )

        # Extract prices for the token
        token_data = data.get(token_id.lower(), {})
        prices = {k: str(v) for k, v in token_data.items()}

        # Update cache
        self._update_cache(cache_key, prices)

        return prices

    async def get_prices(
        self,
        token_ids: list[str],
        vs_currencies: list[str] | None = None,
    ) -> dict[str, dict[str, str]]:
        """Get prices for multiple tokens.

        Args:
            token_ids: List of CoinGecko token IDs
            vs_currencies: Quote currencies. Defaults to ["usd"]

        Returns:
            Dictionary mapping token_id to {currency: price}

        Raises:
            IntegrationError: On API errors
        """
        vs_currencies = vs_currencies or ["usd"]
        ids_str = ",".join([t.lower() for t in token_ids])
        vs_str = ",".join([c.lower() for c in vs_currencies])

        # Check cache
        cache_key = f"prices:{ids_str}:{vs_str}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        data = await self._fetch(
            "/simple/price",
            params={
                "ids": ids_str,
                "vs_currencies": vs_str,
            },
        )

        # Transform to string prices
        result = {}
        for token_id, token_data in data.items():
            result[token_id] = {k: str(v) for k, v in token_data.items()}

        # Update cache
        self._update_cache(cache_key, result)

        return result

    async def get_markets(
        self,
        vs_currency: str = "usd",
        ids: list[str] | None = None,
        order: str = "market_cap_desc",
        per_page: int = 100,
        page: int = 1,
    ) -> list[dict[str, Any]]:
        """Get market data with rankings.

        Args:
            vs_currency: Quote currency (e.g., "usd")
            ids: Optional list of token IDs to filter
            order: Sort order (market_cap_desc, volume_desc, etc.)
            per_page: Results per page (max 250)
            page: Page number

        Returns:
            List of market data dictionaries

        Raises:
            IntegrationError: On API errors
        """
        per_page = min(per_page, 250)

        # Build params
        params: dict[str, Any] = {
            "vs_currency": vs_currency.lower(),
            "order": order,
            "per_page": per_page,
            "page": page,
        }
        if ids:
            params["ids"] = ",".join([t.lower() for t in ids])

        # Check cache
        ids_str = params.get("ids", "all")
        cache_key = f"markets:{vs_currency}:{ids_str}:{order}:{per_page}:{page}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        data = await self._fetch("/coins/markets", params=params)

        # Transform to standardized format
        markets = []
        for m in data:
            markets.append(
                {
                    "id": m.get("id"),
                    "symbol": m.get("symbol"),
                    "name": m.get("name"),
                    "current_price": str(m.get("current_price", 0)),
                    "market_cap": str(m.get("market_cap", 0)),
                    "market_cap_rank": m.get("market_cap_rank"),
                    "total_volume": str(m.get("total_volume", 0)),
                    "high_24h": str(m.get("high_24h", 0)),
                    "low_24h": str(m.get("low_24h", 0)),
                    "price_change_24h": str(m.get("price_change_24h", 0)),
                    "price_change_percentage_24h": str(m.get("price_change_percentage_24h", 0)),
                    "last_updated": m.get("last_updated"),
                }
            )

        # Update cache
        self._update_cache(cache_key, markets, ttl=60)  # 1 minute cache for markets

        return markets

    async def get_token_info(self, token_id: str) -> dict[str, Any]:
        """Get detailed token information.

        Args:
            token_id: CoinGecko token ID

        Returns:
            Token info including description, links, etc.

        Raises:
            IntegrationError: On API errors
        """
        cache_key = f"token_info:{token_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        data = await self._fetch(f"/coins/{token_id.lower()}")

        # Update cache (long TTL - token info rarely changes)
        self._update_cache(cache_key, data, ttl=3600)

        return data

    async def search(self, query: str) -> list[dict[str, Any]]:
        """Search for tokens.

        Args:
            query: Search query

        Returns:
            List of matching tokens

        Raises:
            IntegrationError: On API errors
        """
        cache_key = f"search:{query}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        data = await self._fetch("/search", params={"query": query})

        # Extract coins from response
        coins = data.get("coins", [])

        # Update cache
        self._update_cache(cache_key, coins, ttl=300)

        return coins

    async def get_historical_price(
        self,
        token_id: str,
        date: str,
    ) -> dict[str, Any]:
        """Get historical price for a token at a specific date.

        Args:
            token_id: CoinGecko token ID (e.g., "ethereum", "bitcoin")
            date: Date in dd-mm-yyyy format

        Returns:
            Dictionary with price_usd, market_cap_usd, volume_usd

        Raises:
            IntegrationError: On API errors
        """
        cache_key = f"historical:{token_id}:{date}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        data = await self._fetch(
            f"/coins/{token_id.lower()}/history",
            params={"date": date, "localization": "false"},
        )

        if "market_data" in data:
            market_data = data["market_data"]
            result = CoinGeckoHistoricalPrice(
                price_usd=str(market_data.get("current_price", {}).get("usd", 0)),
                market_cap_usd=str(market_data.get("market_cap", {}).get("usd", 0)),
                volume_usd=str(market_data.get("total_volume", {}).get("usd", 0)),
            )
        else:
            result = CoinGeckoHistoricalPrice()

        result_dict = result.model_dump()

        # Cache for a long time since historical data doesn't change
        self._update_cache(cache_key, result_dict, ttl=3600)

        return result_dict

    async def get_market_chart_range(
        self,
        token_id: str,
        from_timestamp: int,
        to_timestamp: int,
        vs_currency: str = "usd",
    ) -> dict[str, Any]:
        """Get market chart data for a token over a time range.

        Args:
            token_id: CoinGecko token ID (e.g., "ethereum", "bitcoin")
            from_timestamp: Start timestamp in seconds
            to_timestamp: End timestamp in seconds
            vs_currency: Quote currency (default: "usd")

        Returns:
            Dictionary with prices, market_caps, and total_volumes lists.
            Each list contains [timestamp_ms, value] pairs.

        Raises:
            IntegrationError: On API errors
        """
        cache_key = f"market_chart:{token_id}:{from_timestamp}:{to_timestamp}:{vs_currency}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        data = await self._fetch(
            f"/coins/{token_id.lower()}/market_chart/range",
            params={
                "vs_currency": vs_currency.lower(),
                "from": str(from_timestamp),
                "to": str(to_timestamp),
            },
        )

        result = CoinGeckoMarketChartRange(
            prices=data.get("prices", []),
            market_caps=data.get("market_caps", []),
            total_volumes=data.get("total_volumes", []),
        )

        result_dict = result.model_dump()

        # Cache for moderate time - historical data but may need updates for recent ranges
        self._update_cache(cache_key, result_dict, ttl=300)

        return result_dict
