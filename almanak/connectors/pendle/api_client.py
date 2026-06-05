"""Pendle REST API client with rate limiting and caching.

Wraps the Pendle v3 API (https://api-v2.pendle.finance/core/) to provide:
- Market data (implied APY, PT price, liquidity)
- Swap quotes (exact input/output amounts with price impact)
- Asset metadata (PT, YT, SY addresses and expiry)

Rate limiting: 600ms between requests (Pendle API limit).
Caching: 15s TTL by default, configurable per instance.
"""

import logging
import threading
import time
from decimal import Decimal
from typing import Any

from .models import PendleMarketData, PendleSwapQuote

logger = logging.getLogger(__name__)

# Pendle API base URL
PENDLE_API_BASE = "https://api-v2.pendle.finance/core"

# Chain ID mapping consistent with almanak/core/enums.py
CHAIN_ID_MAP: dict[str, int] = {
    "ethereum": 1,
    "arbitrum": 42161,
    "optimism": 10,
    "base": 8453,
    "bsc": 56,
}


class PendleAPIError(Exception):
    """Raised when a Pendle API call fails."""


class PendleAPIClient:
    """REST client for the Pendle v3 API.

    Follows the DefiLlamaProvider pattern: rate limiting via threading.Lock,
    bounded TTL cache, and typed return values.

    Example:
        client = PendleAPIClient(chain="ethereum")
        market = client.get_market_data("0x...")
        print(f"Implied APY: {market.implied_apy}")

        quote = client.get_swap_quote(
            market="0x...",
            token_in="0x...",
            amount_in=10**18,
            swap_type="token_to_pt",
        )
        print(f"Estimated output: {quote.amount_out}")
    """

    def __init__(
        self,
        chain: str = "ethereum",
        cache_ttl_seconds: float = 15.0,
        max_cache_entries: int = 1000,
        api_key: str | None = None,
    ):
        """Initialize the Pendle API client.

        Args:
            chain: Target chain name (ethereum, arbitrum, etc.)
            cache_ttl_seconds: Cache TTL in seconds (default 15)
            max_cache_entries: Maximum cache entries (default 1000)
            api_key: Optional Pendle API key for higher rate limits
        """
        if chain not in CHAIN_ID_MAP:
            raise ValueError(f"Unsupported chain: {chain}. Supported: {list(CHAIN_ID_MAP.keys())}")

        self.chain = chain
        self.chain_id = CHAIN_ID_MAP[chain]
        self.cache_ttl = cache_ttl_seconds
        self.max_cache_entries = max_cache_entries
        self.api_key = api_key

        # Rate limiting: 600ms minimum between requests
        self._rate_lock = threading.Lock()
        self._last_request_time: float = 0.0
        self._min_interval: float = 0.6  # 600ms

        # TTL cache: key -> (value, expiry_time)
        self._cache: dict[str, tuple[Any, float]] = {}
        self._cache_lock = threading.Lock()

        # Health metrics
        self._request_count = 0
        self._error_count = 0

        logger.info(f"PendleAPIClient initialized: chain={chain}, chain_id={self.chain_id}")

    def get_market_data(self, market_address: str) -> PendleMarketData:
        """Fetch market data from Pendle API.

        Args:
            market_address: Market contract address

        Returns:
            PendleMarketData with implied APY, PT price, liquidity, etc.

        Raises:
            PendleAPIError: If the API call fails
        """
        cache_key = f"market:{self.chain_id}:{market_address.lower()}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        url = f"{PENDLE_API_BASE}/v1/{self.chain_id}/markets/{market_address}"
        data = self._request("GET", url)

        result = self._parse_market_data(data, market_address)
        self._set_cached(cache_key, result)
        return result

    def get_swap_quote(
        self,
        market: str,
        token_in: str,
        amount_in: int,
        swap_type: str,
        slippage_bps: int = 50,
    ) -> PendleSwapQuote:
        """Get a swap quote from the Pendle API.

        Args:
            market: Market contract address
            token_in: Input token address
            amount_in: Input amount in wei
            swap_type: One of "token_to_pt", "pt_to_token", "token_to_yt", "yt_to_token"
            slippage_bps: Slippage tolerance in basis points

        Returns:
            PendleSwapQuote with estimated output and price impact

        Raises:
            PendleAPIError: If the API call fails
        """
        # Swap quotes should not be cached (amount-dependent)
        endpoint_map = {
            "token_to_pt": "swap/pt/in",
            "pt_to_token": "swap/pt/out",
            "token_to_yt": "swap/yt/in",
            "yt_to_token": "swap/yt/out",
        }
        if swap_type not in endpoint_map:
            raise PendleAPIError(f"Invalid swap_type: {swap_type}. Must be one of {list(endpoint_map.keys())}")

        url = f"{PENDLE_API_BASE}/v2/{self.chain_id}/markets/{market}/{endpoint_map[swap_type]}"
        params = {
            "tokenIn": token_in,
            "amountIn": str(amount_in),
            "slippage": str(slippage_bps / 10000),
        }

        data = self._request("GET", url, params=params)
        return self._parse_swap_quote(data, market, token_in, amount_in)

    def get_market_list(self) -> list[PendleMarketData]:
        """Fetch list of all active markets on this chain.

        Returns:
            List of PendleMarketData for all active markets

        Raises:
            PendleAPIError: If the API call fails
        """
        cache_key = f"markets_list:{self.chain_id}"
        cached = self._get_cached(cache_key)
        if cached is not None:
            return cached

        url = f"{PENDLE_API_BASE}/v1/{self.chain_id}/markets"
        params = {"order_by": "liquidity:desc", "limit": 100}
        data = self._request("GET", url, params=params)

        results = []
        markets = data.get("results", []) if isinstance(data, dict) else data if isinstance(data, list) else []
        for market_data in markets:
            addr = market_data.get("address", market_data.get("market", ""))
            if addr:
                results.append(self._parse_market_data(market_data, addr))

        self._set_cached(cache_key, results)
        return results

    def get_pt_price(self, market_address: str) -> Decimal:
        """Get PT price in terms of the underlying asset.

        Args:
            market_address: Market contract address

        Returns:
            PT price as Decimal (e.g., 0.97 means PT trades at 3% discount)

        Raises:
            PendleAPIError: If the API call fails
        """
        market_data = self.get_market_data(market_address)
        if market_data.pt_price_in_asset <= 0:
            raise PendleAPIError(f"Invalid PT price for market {market_address}: {market_data.pt_price_in_asset}")
        return market_data.pt_price_in_asset

    def get_implied_apy(self, market_address: str) -> Decimal:
        """Get the implied APY for a Pendle market.

        Args:
            market_address: Market contract address

        Returns:
            Implied APY as Decimal (e.g., 0.05 = 5%)

        Raises:
            PendleAPIError: If the API call fails
        """
        market_data = self.get_market_data(market_address)
        return market_data.implied_apy

    # =========================================================================
    # Internal Methods
    # =========================================================================

    def _request(self, method: str, url: str, params: dict | None = None) -> Any:
        """Make a rate-limited HTTP request.

        Args:
            method: HTTP method
            url: Request URL
            params: Query parameters

        Returns:
            Parsed JSON response

        Raises:
            PendleAPIError: If the request fails
        """
        # Rate limiting
        with self._rate_lock:
            now = time.monotonic()
            elapsed = now - self._last_request_time
            if elapsed < self._min_interval:
                time.sleep(self._min_interval - elapsed)
            self._last_request_time = time.monotonic()

        self._request_count += 1

        try:
            import json
            import urllib.parse
            import urllib.request

            if params:
                query_string = urllib.parse.urlencode(params)
                url = f"{url}?{query_string}"

            req = urllib.request.Request(url, method=method)
            req.add_header("Accept", "application/json")
            if self.api_key:
                req.add_header("Authorization", f"Bearer {self.api_key}")

            with urllib.request.urlopen(req, timeout=10) as response:
                body = response.read().decode("utf-8")
                return json.loads(body)

        except Exception as e:
            self._error_count += 1
            logger.warning(f"Pendle API request failed: {url} -> {e}")
            raise PendleAPIError(f"API request failed: {e}") from e

    def _parse_market_data(self, data: dict[str, Any], market_address: str) -> PendleMarketData:
        """Parse API response into PendleMarketData."""
        # Handle nested "data" wrapper some endpoints use
        if "data" in data and isinstance(data["data"], dict):
            data = data["data"]

        # Extract token info -- API may return nested objects or plain address strings
        pt_address, pt_symbol, pt_decimals = self._extract_token_info(data, "pt", "ptAddress")
        yt_address, yt_symbol, yt_decimals = self._extract_token_info(data, "yt", "ytAddress")
        sy_address, _, _ = self._extract_token_info(data, "sy", "syAddress")

        # Underlying asset
        underlying_raw = data.get("underlyingAsset", data.get("accountingAsset", ""))
        underlying_address = ""
        underlying_symbol = ""
        if isinstance(underlying_raw, dict):
            underlying_address = str(underlying_raw.get("address", "")).lower()
            underlying_symbol = str(underlying_raw.get("symbol", ""))
        else:
            underlying_address = str(underlying_raw).lower()

        return PendleMarketData(
            market_address=market_address.lower(),
            chain_id=self.chain_id,
            pt_address=pt_address,
            pt_symbol=pt_symbol,
            pt_decimals=pt_decimals,
            yt_address=yt_address,
            yt_symbol=yt_symbol,
            yt_decimals=yt_decimals,
            sy_address=sy_address,
            underlying_address=underlying_address,
            underlying_symbol=underlying_symbol,
            expiry=int(data.get("expiry", 0)),
            implied_apy=Decimal(str(data.get("impliedApy", data.get("implied_apy", 0)))),
            underlying_apy=Decimal(str(data.get("underlyingApy", data.get("underlying_apy", 0)))),
            pt_price_in_asset=Decimal(str(data.get("ptDiscount", data.get("pt_price", 0)))),
            yt_price_in_asset=Decimal(str(data.get("ytDiscount", data.get("yt_price", 0)))),
            liquidity_usd=Decimal(str(data.get("liquidity", data.get("tvl", 0)))),
            volume_24h_usd=Decimal(str(data.get("tradingVolume", data.get("volume24h", 0)))),
            pt_discount=Decimal(str(data.get("ptDiscount", 0))),
            is_expired=bool(data.get("isExpired", False)),
        )

    @staticmethod
    def _extract_token_info(data: dict, key: str, fallback_key: str) -> tuple[str, str, int]:
        """Extract address, symbol, decimals from a token field.

        The Pendle API may return a nested object or a plain address string.
        Returns (address, symbol, decimals) with sensible defaults.

        Handles nullable/malformed API fields:
        - None address -> empty string (not "none")
        - None or non-numeric decimals -> 18 (ERC-20 default)
        """
        raw = data.get(key, data.get(fallback_key, ""))
        if isinstance(raw, dict):
            # Guard against None address becoming "none" string
            addr_raw = raw.get("address")
            address = str(addr_raw).lower() if addr_raw is not None else ""

            symbol = str(raw.get("symbol") or "")

            # Guard against None or non-numeric decimals
            decimals_raw = raw.get("decimals")
            try:
                decimals = int(decimals_raw) if decimals_raw is not None else 18
            except (ValueError, TypeError):
                logger.warning(f"Non-numeric decimals value for token '{key}': {decimals_raw!r}, defaulting to 18")
                decimals = 18

            return (address, symbol, decimals)

        # Plain value: guard against None becoming "none"
        address = str(raw).lower() if raw is not None else ""
        return (address, "", 18)

    def _parse_swap_quote(
        self,
        data: dict[str, Any],
        market: str,
        token_in: str,
        amount_in: int,
    ) -> PendleSwapQuote:
        """Parse API response into PendleSwapQuote."""
        if "data" in data and isinstance(data["data"], dict):
            data = data["data"]

        amount_out = int(data.get("amountOut", data.get("netOut", 0)))
        price_impact = data.get("priceImpact", data.get("priceImpactBps", 0))

        # Convert price impact to bps if it's a decimal fraction
        if isinstance(price_impact, float) and abs(price_impact) < 1:
            price_impact_bps = int(abs(price_impact) * 10000)
        else:
            price_impact_bps = int(abs(price_impact))

        # Calculate exchange rate
        exchange_rate = Decimal("0")
        if amount_in > 0 and amount_out > 0:
            exchange_rate = Decimal(str(amount_out)) / Decimal(str(amount_in))

        return PendleSwapQuote(
            market_address=market.lower(),
            token_in=token_in.lower(),
            token_out=data.get("tokenOut", "").lower(),
            amount_in=amount_in,
            amount_out=amount_out,
            price_impact_bps=price_impact_bps,
            exchange_rate=exchange_rate,
            source="api",
        )

    def _get_cached(self, key: str) -> Any | None:
        """Get value from cache if not expired."""
        with self._cache_lock:
            entry = self._cache.get(key)
            if entry is None:
                return None
            value, expiry = entry
            if time.monotonic() > expiry:
                del self._cache[key]
                return None
            return value

    def _set_cached(self, key: str, value: Any) -> None:
        """Set value in cache with TTL."""
        with self._cache_lock:
            # Evict oldest entries if at capacity
            if len(self._cache) >= self.max_cache_entries:
                oldest_key = min(self._cache, key=lambda k: self._cache[k][1])
                del self._cache[oldest_key]
            self._cache[key] = (value, time.monotonic() + self.cache_ttl)

    def clear_cache(self) -> None:
        """Clear all cached data."""
        with self._cache_lock:
            self._cache.clear()

    @property
    def health(self) -> dict[str, Any]:
        """Return health metrics."""
        return {
            "request_count": self._request_count,
            "error_count": self._error_count,
            "cache_size": len(self._cache),
            "chain": self.chain,
            "chain_id": self.chain_id,
        }
