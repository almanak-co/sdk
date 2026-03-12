"""Drift Data API Client.

REST client for the Drift public data API (data.api.drift.trade).
Provides market data, funding rates, oracle prices, and candles
without requiring authentication.

Reference: https://data.api.drift.trade
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .constants import DRIFT_DATA_API_BASE_URL, PERP_MARKETS, PRICE_PRECISION
from .exceptions import DriftAPIError
from .models import DriftMarket, FundingRate

logger = logging.getLogger(__name__)


class DriftDataClient:
    """Client for the Drift public data API.

    Provides read-only access to market data, funding rates,
    and oracle prices. No authentication required.

    Example:
        client = DriftDataClient()
        markets = client.get_perp_markets()
        oracle_prices = client.get_oracle_prices()
    """

    def __init__(
        self,
        base_url: str = DRIFT_DATA_API_BASE_URL,
        timeout: int = 30,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._setup_session()

    def _setup_session(self) -> None:
        """Set up requests session with retry logic."""
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        """Make an API request with error handling.

        Args:
            method: HTTP method ("GET")
            endpoint: API endpoint path
            params: Optional query parameters

        Returns:
            Parsed JSON response

        Raises:
            DriftAPIError: If the request fails
        """
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.request(
                method,
                url,
                params=params,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            raise DriftAPIError(
                message=str(e),
                status_code=e.response.status_code if e.response else 0,
                endpoint=endpoint,
            ) from e
        except requests.exceptions.RequestException as e:
            raise DriftAPIError(
                message=str(e),
                status_code=0,
                endpoint=endpoint,
            ) from e

    # =========================================================================
    # Market Data
    # =========================================================================

    def get_perp_markets(self) -> list[DriftMarket]:
        """Get all perpetual futures markets.

        Returns:
            List of DriftMarket with market info and stats
        """
        try:
            data = self._make_request("GET", "/stats/markets")
            markets = []
            if isinstance(data, list):
                for item in data:
                    if item.get("marketType") == "perp":
                        markets.append(DriftMarket.from_api_response(item))
            return markets
        except DriftAPIError:
            logger.warning("Failed to fetch markets from Drift Data API, using static list")
            return [DriftMarket(market_index=idx, symbol=symbol) for idx, symbol in PERP_MARKETS.items()]

    def get_oracle_prices(self) -> dict[int, Decimal]:
        """Get current oracle prices for all perp markets.

        Returns:
            Dict of market_index → oracle price in USD
        """
        try:
            data = self._make_request("GET", "/amm/oraclePrice")
            prices: dict[int, Decimal] = {}
            if isinstance(data, list):
                for item in data:
                    market_index = item.get("marketIndex", -1)
                    price_raw = item.get("oraclePrice", 0)
                    if market_index >= 0 and price_raw:
                        prices[market_index] = Decimal(str(price_raw)) / Decimal(str(PRICE_PRECISION))
            return prices
        except DriftAPIError as e:
            logger.warning(f"Failed to fetch oracle prices: {e}")
            return {}

    def get_oracle_price(self, market_index: int) -> Decimal | None:
        """Get oracle price for a specific market.

        Args:
            market_index: Perp market index

        Returns:
            Oracle price in USD, or None if not available
        """
        prices = self.get_oracle_prices()
        return prices.get(market_index)

    def get_funding_rates(self, market_index: int) -> list[FundingRate]:
        """Get historical funding rates for a market.

        Args:
            market_index: Perp market index

        Returns:
            List of FundingRate data points
        """
        symbol = PERP_MARKETS.get(market_index, f"UNKNOWN-{market_index}")
        try:
            data = self._make_request(
                "GET",
                f"/market/{symbol}/fundingRates",
            )
            rates = []
            if isinstance(data, list):
                for item in data:
                    rates.append(FundingRate.from_api_response(item))
            return rates
        except DriftAPIError as e:
            logger.warning(f"Failed to fetch funding rates for {symbol}: {e}")
            return []

    def get_market_info(self, market_index: int) -> DriftMarket | None:
        """Get detailed info for a specific perp market.

        Args:
            market_index: Perp market index

        Returns:
            DriftMarket or None if not found
        """
        markets = self.get_perp_markets()
        for market in markets:
            if market.market_index == market_index:
                return market
        return None
