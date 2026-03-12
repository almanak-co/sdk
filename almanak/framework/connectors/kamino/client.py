"""Kamino Finance Lending HTTP Client.

This module provides the KaminoClient class for interacting with the
Kamino Finance REST API. Kamino is the primary lending protocol on Solana,
providing Aave-style lending/borrowing with a REST API that returns
pre-built unsigned transactions.

No authentication is required.

Example:
    from almanak.framework.connectors.kamino import KaminoClient, KaminoConfig

    config = KaminoConfig(wallet_address="your-solana-wallet-pubkey")
    client = KaminoClient(config)

    # Get reserves for the main market
    reserves = client.get_reserves("7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF")

    # Build a deposit transaction
    tx = client.deposit(
        market="7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF",
        reserve="D6q6wuQSrifJKZYpR1M8R4YawnLDtDsMmWM1NbBmgJ59",
        amount="100.0",
    )
"""

import logging
from dataclasses import dataclass
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .exceptions import KaminoAPIError, KaminoConfigError
from .models import KaminoMarket, KaminoReserve, KaminoTransactionResponse

logger = logging.getLogger(__name__)

# Kamino main market address (most liquid, primary market)
KAMINO_MAIN_MARKET = "7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF"

# U64::MAX for "withdraw all" / "repay all"
U64_MAX = "18446744073709551615"


@dataclass
class KaminoConfig:
    """Configuration for Kamino client.

    Attributes:
        wallet_address: Solana wallet public key (Base58)
        base_url: Kamino API base URL
        timeout: Request timeout in seconds
        market: Default lending market address (defaults to main market)
    """

    wallet_address: str
    base_url: str = "https://api.kamino.finance"
    timeout: int = 30
    market: str = KAMINO_MAIN_MARKET

    def __post_init__(self) -> None:
        """Validate configuration."""
        if not self.wallet_address:
            raise KaminoConfigError(
                "wallet_address is required",
                parameter="wallet_address",
            )


class KaminoClient:
    """Client for interacting with the Kamino Finance REST API.

    This client provides methods for:
    - Listing lending markets and reserves
    - Building deposit, borrow, repay, and withdraw transactions

    All transaction endpoints return base64-encoded unsigned VersionedTransactions.
    No authentication is required.

    Example:
        config = KaminoConfig(wallet_address="your-solana-wallet-pubkey")
        client = KaminoClient(config)

        reserves = client.get_reserves()
        tx = client.deposit(reserve=reserves[0].address, amount="100.0")
    """

    def __init__(self, config: KaminoConfig) -> None:
        """Initialize the Kamino client.

        Args:
            config: Kamino client configuration
        """
        self.config = config
        self._setup_session()
        logger.info(f"KaminoClient initialized for wallet={config.wallet_address[:8]}...")

    def _setup_session(self) -> None:
        """Set up requests session with retry logic."""
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
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
        json_data: Any | None = None,
    ) -> Any:
        """Make a request to the Kamino API.

        Args:
            method: HTTP method (GET, POST)
            endpoint: API endpoint path
            params: Query parameters
            json_data: JSON body for POST requests

        Returns:
            Parsed JSON response

        Raises:
            KaminoAPIError: If the API request fails
        """
        url = f"{self.config.base_url}{endpoint}"

        logger.debug(f"Kamino API Request: {method} {endpoint}")

        try:
            response = self.session.request(
                method,
                url,
                params=params,
                json=json_data,
                timeout=self.config.timeout,
            )

            response.raise_for_status()
            return response.json()

        except requests.exceptions.HTTPError as e:
            error_data = None
            error_code = None
            try:
                error_data = response.json()
                error_code = error_data.get("code")
            except Exception:
                pass

            raise KaminoAPIError(
                message=f"API request failed: {e}",
                status_code=response.status_code,
                endpoint=endpoint,
                error_code=error_code,
                error_data=error_data,
            ) from e

        except requests.exceptions.RequestException as e:
            raise KaminoAPIError(
                message=f"Request failed: {e}",
                status_code=0,
                endpoint=endpoint,
            ) from e

    # =========================================================================
    # Market & Reserve queries
    # =========================================================================

    def get_markets(self) -> list[KaminoMarket]:
        """List all available lending markets.

        Returns:
            List of KaminoMarket objects

        Raises:
            KaminoAPIError: If the API request fails
        """
        response = self._make_request("GET", "/v2/kamino-market")
        return [KaminoMarket.from_api_response(m) for m in response]

    def get_reserves(self, market: str | None = None) -> list[KaminoReserve]:
        """List reserves (token pools) for a lending market.

        Args:
            market: Market address (defaults to config market)

        Returns:
            List of KaminoReserve objects with current metrics

        Raises:
            KaminoAPIError: If the API request fails
        """
        market_addr = market or self.config.market
        response = self._make_request("GET", f"/kamino-market/{market_addr}/reserves/metrics")
        return [KaminoReserve.from_api_response(r) for r in response]

    def find_reserve_by_token(self, token_symbol: str, market: str | None = None) -> KaminoReserve | None:
        """Find a reserve by token symbol.

        Args:
            token_symbol: Token symbol (e.g., "USDC", "SOL")
            market: Market address (defaults to config market)

        Returns:
            KaminoReserve if found, None otherwise
        """
        reserves = self.get_reserves(market)
        token_upper = token_symbol.upper()
        for reserve in reserves:
            if reserve.token_symbol.upper() == token_upper:
                return reserve
        return None

    # =========================================================================
    # Transaction builders
    # =========================================================================

    def deposit(
        self,
        reserve: str,
        amount: str,
        market: str | None = None,
        wallet: str | None = None,
    ) -> KaminoTransactionResponse:
        """Build a deposit (supply) transaction.

        Args:
            reserve: Reserve address to deposit into
            amount: Amount in token units (e.g., "100.5" for 100.5 USDC)
            market: Market address (defaults to config market)
            wallet: Wallet address (defaults to config wallet)

        Returns:
            KaminoTransactionResponse with base64-encoded unsigned transaction

        Raises:
            KaminoAPIError: If the API request fails
        """
        payload = {
            "wallet": wallet or self.config.wallet_address,
            "market": market or self.config.market,
            "reserve": reserve,
            "amount": amount,
        }

        response = self._make_request("POST", "/ktx/klend/deposit", json_data=payload)
        logger.info(f"Kamino deposit tx built: reserve={reserve[:8]}..., amount={amount}")
        return KaminoTransactionResponse.from_api_response(response, action="deposit")

    def borrow(
        self,
        reserve: str,
        amount: str,
        market: str | None = None,
        wallet: str | None = None,
    ) -> KaminoTransactionResponse:
        """Build a borrow transaction.

        Args:
            reserve: Reserve address to borrow from
            amount: Amount in token units (e.g., "50.0" for 50 USDC)
            market: Market address (defaults to config market)
            wallet: Wallet address (defaults to config wallet)

        Returns:
            KaminoTransactionResponse with base64-encoded unsigned transaction

        Raises:
            KaminoAPIError: If the API request fails
        """
        payload = {
            "wallet": wallet or self.config.wallet_address,
            "market": market or self.config.market,
            "reserve": reserve,
            "amount": amount,
        }

        response = self._make_request("POST", "/ktx/klend/borrow", json_data=payload)
        logger.info(f"Kamino borrow tx built: reserve={reserve[:8]}..., amount={amount}")
        return KaminoTransactionResponse.from_api_response(response, action="borrow")

    def repay(
        self,
        reserve: str,
        amount: str,
        market: str | None = None,
        wallet: str | None = None,
    ) -> KaminoTransactionResponse:
        """Build a repay transaction.

        Args:
            reserve: Reserve address to repay into
            amount: Amount in token units (e.g., "50.0" for 50 USDC)
            market: Market address (defaults to config market)
            wallet: Wallet address (defaults to config wallet)

        Returns:
            KaminoTransactionResponse with base64-encoded unsigned transaction

        Raises:
            KaminoAPIError: If the API request fails
        """
        payload = {
            "wallet": wallet or self.config.wallet_address,
            "market": market or self.config.market,
            "reserve": reserve,
            "amount": amount,
        }

        response = self._make_request("POST", "/ktx/klend/repay", json_data=payload)
        logger.info(f"Kamino repay tx built: reserve={reserve[:8]}..., amount={amount}")
        return KaminoTransactionResponse.from_api_response(response, action="repay")

    def withdraw(
        self,
        reserve: str,
        amount: str,
        market: str | None = None,
        wallet: str | None = None,
    ) -> KaminoTransactionResponse:
        """Build a withdraw transaction.

        Use amount=U64_MAX ("18446744073709551615") to withdraw all.

        Args:
            reserve: Reserve address to withdraw from
            amount: Amount in token units, or U64_MAX for withdraw-all
            market: Market address (defaults to config market)
            wallet: Wallet address (defaults to config wallet)

        Returns:
            KaminoTransactionResponse with base64-encoded unsigned transaction

        Raises:
            KaminoAPIError: If the API request fails
        """
        payload = {
            "wallet": wallet or self.config.wallet_address,
            "market": market or self.config.market,
            "reserve": reserve,
            "amount": amount,
        }

        response = self._make_request("POST", "/ktx/klend/withdraw", json_data=payload)
        logger.info(f"Kamino withdraw tx built: reserve={reserve[:8]}..., amount={amount}")
        return KaminoTransactionResponse.from_api_response(response, action="withdraw")

    @property
    def wallet_address(self) -> str:
        """Get the configured wallet address."""
        return self.config.wallet_address
