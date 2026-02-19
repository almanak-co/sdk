"""Polymarket CLOB API Client.

Provides access to the Polymarket Central Limit Order Book (CLOB) API:
- L1 Authentication (EIP-712) for credential creation
- L2 Authentication (HMAC-SHA256) for trading operations
- Market data fetching
- Order management

Example:
    from almanak.framework.connectors.polymarket import ClobClient, PolymarketConfig

    config = PolymarketConfig.from_env()
    client = ClobClient(config)

    # Create API credentials (first time)
    credentials = client.create_api_credentials()

    # Fetch market data
    markets = client.get_markets(MarketFilters(active=True))
    orderbook = client.get_orderbook(token_id="123...")
"""

import base64
import hashlib
import hmac
import random
import secrets
import time
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_DOWN, ROUND_FLOOR, Decimal
from typing import Any
from urllib.parse import urlencode

import httpx
import structlog
from eth_account import Account
from eth_account.messages import encode_typed_data

from .exceptions import (
    PolymarketAPIError,
    PolymarketAuthenticationError,
    PolymarketInvalidPriceError,
    PolymarketInvalidTickSizeError,
    PolymarketMinimumOrderError,
    PolymarketRateLimitError,
)
from .models import (
    CLOB_AUTH_DOMAIN,
    CLOB_AUTH_MESSAGE,
    CLOB_AUTH_TYPES,
    CTF_EXCHANGE_DOMAIN,
    ORDER_TYPES,
    ApiCredentials,
    BalanceAllowance,
    GammaMarket,
    HistoricalPrice,
    HistoricalTrade,
    LimitOrderParams,
    MarketFilters,
    MarketOrderParams,
    OpenOrder,
    OrderBook,
    OrderFilters,
    OrderResponse,
    OrderSide,
    OrderType,
    PolymarketConfig,
    Position,
    PositionFilters,
    PriceHistory,
    PriceHistoryInterval,
    SignedOrder,
    TokenPrice,
    Trade,
    TradeFilters,
    UnsignedOrder,
)

logger = structlog.get_logger(__name__)


class TokenBucketRateLimiter:
    """Token bucket rate limiter for API calls.

    Implements a token bucket algorithm that allows bursting while maintaining
    an average rate over time. Tokens are added to the bucket at a fixed rate,
    and each request consumes one token.

    The algorithm:
    1. Bucket starts full (capacity = rate_per_second tokens)
    2. Tokens are added at rate_per_second tokens per second
    3. Each request consumes 1 token
    4. If bucket is empty, the caller waits until a token is available

    Thread Safety:
        This implementation is NOT thread-safe. Use separate instances per thread
        or add locking if needed.

    Example:
        >>> limiter = TokenBucketRateLimiter(rate_per_second=30.0)
        >>> limiter.acquire()  # Blocks if rate limit exceeded
        >>> # Make API call
    """

    def __init__(self, rate_per_second: float, enabled: bool = True):
        """Initialize the rate limiter.

        Args:
            rate_per_second: Maximum average requests per second
            enabled: Whether rate limiting is active (can be disabled for testing)
        """
        self._rate = rate_per_second
        self._enabled = enabled
        self._capacity = rate_per_second  # Bucket can hold one second's worth of tokens
        self._tokens = rate_per_second  # Start with full bucket
        self._last_refill = time.time()

    @property
    def enabled(self) -> bool:
        """Whether rate limiting is enabled."""
        return self._enabled

    @enabled.setter
    def enabled(self, value: bool) -> None:
        """Enable or disable rate limiting."""
        self._enabled = value

    @property
    def rate(self) -> float:
        """Current rate limit (requests per second)."""
        return self._rate

    @property
    def available_tokens(self) -> float:
        """Number of tokens currently available (after refill)."""
        self._refill()
        return self._tokens

    def _refill(self) -> None:
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill

        # Add tokens based on elapsed time
        tokens_to_add = elapsed * self._rate
        self._tokens = min(self._capacity, self._tokens + tokens_to_add)
        self._last_refill = now

    def acquire(self, timeout: float | None = None) -> bool:
        """Acquire a token, blocking if necessary.

        Waits until a token is available, then consumes it. If the bucket is
        empty, sleeps until enough time has passed for a new token.

        Args:
            timeout: Maximum time to wait in seconds. None means wait forever.
                    If timeout expires before a token is available, returns False.

        Returns:
            True if token was acquired, False if timeout expired (only when timeout is set)

        Example:
            >>> if limiter.acquire(timeout=5.0):
            ...     make_api_call()
            ... else:
            ...     print("Timeout waiting for rate limit")
        """
        if not self._enabled:
            return True

        start_time = time.time()

        while True:
            self._refill()

            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True

            # Calculate wait time until next token
            # We need 1 token, we have self._tokens, so we need (1 - self._tokens) more
            # At rate _rate tokens/sec, wait time = (1 - tokens) / rate
            wait_time = (1.0 - self._tokens) / self._rate

            # Check timeout
            if timeout is not None:
                elapsed = time.time() - start_time
                remaining_timeout = timeout - elapsed
                if remaining_timeout <= 0:
                    return False
                wait_time = min(wait_time, remaining_timeout)

            if wait_time > 0:
                logger.debug(
                    "Rate limiter waiting",
                    wait_seconds=round(wait_time, 3),
                    available_tokens=round(self._tokens, 2),
                    rate=self._rate,
                )
                time.sleep(wait_time)

    def try_acquire(self) -> bool:
        """Try to acquire a token without blocking.

        Returns:
            True if token was acquired, False if bucket is empty

        Example:
            >>> if limiter.try_acquire():
            ...     make_api_call()
            ... else:
            ...     print("Rate limit would be exceeded, skipping")
        """
        if not self._enabled:
            return True

        self._refill()

        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    def reset(self) -> None:
        """Reset the rate limiter to full capacity.

        Useful for testing or after a period of inactivity.
        """
        self._tokens = self._capacity
        self._last_refill = time.time()


class ClobClient:
    """Client for Polymarket CLOB API.

    Handles both L1 (EIP-712) and L2 (HMAC) authentication and provides
    methods for market data and order management.

    Attributes:
        config: Polymarket configuration
        credentials: API credentials (may be None until created)

    Thread Safety:
        This class is NOT thread-safe. Use separate instances per thread.
    """

    def __init__(
        self,
        config: PolymarketConfig,
        http_client: httpx.Client | None = None,
        rate_limiter: TokenBucketRateLimiter | None = None,
    ):
        """Initialize CLOB client.

        Args:
            config: Polymarket configuration with wallet and keys
            http_client: Optional HTTP client for testing
            rate_limiter: Optional rate limiter for testing. If not provided,
                         creates one based on config settings.
        """
        self.config = config
        self.credentials = config.api_credentials
        self._http = http_client or httpx.Client(timeout=30.0)
        self._cache: dict[str, tuple[Any, float]] = {}

        # Initialize rate limiter
        if rate_limiter is not None:
            self._rate_limiter = rate_limiter
        else:
            self._rate_limiter = TokenBucketRateLimiter(
                rate_per_second=config.rate_limit_requests_per_second,
                enabled=config.rate_limit_enabled,
            )

        logger.info(
            "ClobClient initialized",
            wallet=config.wallet_address,
            has_credentials=self.credentials is not None,
            rate_limit_enabled=self._rate_limiter.enabled,
            rate_limit_rps=self._rate_limiter.rate,
        )

    @property
    def rate_limiter(self) -> TokenBucketRateLimiter:
        """Access the rate limiter for configuration or testing."""
        return self._rate_limiter

    def close(self) -> None:
        """Close HTTP client."""
        self._http.close()

    def __enter__(self) -> "ClobClient":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # =========================================================================
    # L1 Authentication (EIP-712)
    # =========================================================================

    def _build_l1_headers(self, nonce: int = 0) -> dict[str, str]:
        """Build L1 authentication headers using EIP-712 signing.

        Args:
            nonce: Nonce for the signature (default 0)

        Returns:
            Headers dict with POLY_ADDRESS, POLY_SIGNATURE, POLY_TIMESTAMP, POLY_NONCE
        """
        timestamp = str(int(time.time()))
        wallet = self.config.wallet_address

        # Build EIP-712 typed data
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                ],
                **CLOB_AUTH_TYPES,
            },
            "primaryType": "ClobAuth",
            "domain": CLOB_AUTH_DOMAIN,
            "message": {
                "address": wallet,
                "timestamp": timestamp,
                "nonce": nonce,
                "message": CLOB_AUTH_MESSAGE,
            },
        }

        # Sign the typed data
        private_key = self.config.private_key.get_secret_value()
        signable = encode_typed_data(full_message=typed_data)
        signed = Account.sign_message(signable, private_key)

        return {
            "POLY_ADDRESS": wallet,
            "POLY_SIGNATURE": signed.signature.hex(),
            "POLY_TIMESTAMP": timestamp,
            "POLY_NONCE": str(nonce),
        }

    def create_api_credentials(self) -> ApiCredentials:
        """Create new API credentials using L1 authentication.

        This signs an EIP-712 message to prove wallet ownership and
        creates new API credentials for L2 authentication.

        Returns:
            ApiCredentials with api_key, secret, and passphrase

        Raises:
            PolymarketAuthenticationError: If credential creation fails
        """
        url = f"{self.config.clob_base_url}/auth/api-key"
        headers = self._build_l1_headers()

        logger.info("Creating API credentials", wallet=self.config.wallet_address)

        try:
            response = self._http.post(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            self.credentials = ApiCredentials.from_dict(data)
            logger.info("API credentials created successfully")
            return self.credentials

        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to create API credentials",
                status_code=e.response.status_code,
                response=e.response.text,
            )
            raise PolymarketAuthenticationError(f"Failed to create API credentials: {e.response.text}") from e

    def derive_api_credentials(self) -> ApiCredentials:
        """Derive existing API credentials using L1 authentication.

        If credentials were previously created, this retrieves them
        using wallet signature.

        Returns:
            ApiCredentials with api_key, secret, and passphrase

        Raises:
            PolymarketAuthenticationError: If credential derivation fails
        """
        url = f"{self.config.clob_base_url}/auth/derive-api-key"
        headers = self._build_l1_headers()

        logger.info("Deriving API credentials", wallet=self.config.wallet_address)

        try:
            response = self._http.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()

            self.credentials = ApiCredentials.from_dict(data)
            logger.info("API credentials derived successfully")
            return self.credentials

        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to derive API credentials",
                status_code=e.response.status_code,
                response=e.response.text,
            )
            raise PolymarketAuthenticationError(f"Failed to derive API credentials: {e.response.text}") from e

    def get_or_create_credentials(self) -> ApiCredentials:
        """Get existing credentials or create new ones.

        First attempts to derive existing credentials. If that fails,
        creates new ones.

        Returns:
            ApiCredentials

        Raises:
            PolymarketAuthenticationError: If both operations fail
        """
        if self.credentials is not None:
            return self.credentials

        try:
            return self.derive_api_credentials()
        except PolymarketAuthenticationError:
            logger.info("No existing credentials, creating new ones")
            return self.create_api_credentials()

    # =========================================================================
    # L2 Authentication (HMAC-SHA256)
    # =========================================================================

    def _ensure_credentials(self) -> ApiCredentials:
        """Ensure we have API credentials, creating if needed."""
        if self.credentials is None:
            self.credentials = self.get_or_create_credentials()
        return self.credentials

    def _build_l2_signature(
        self,
        method: str,
        path: str,
        timestamp: str,
        body: str = "",
    ) -> str:
        """Build HMAC-SHA256 signature for L2 authentication.

        Args:
            method: HTTP method (GET, POST, DELETE)
            path: Request path (e.g., /order)
            timestamp: Unix timestamp string
            body: Request body (empty string for GET)

        Returns:
            Base64-encoded HMAC signature
        """
        credentials = self._ensure_credentials()
        secret = credentials.secret.get_secret_value()

        # Build message to sign
        message = f"{timestamp}{method}{path}{body}"

        # Compute HMAC-SHA256
        signature = hmac.new(
            base64.b64decode(secret),
            message.encode("utf-8"),
            hashlib.sha256,
        ).digest()

        return base64.b64encode(signature).decode("utf-8")

    def _build_l2_headers(
        self,
        method: str,
        path: str,
        body: str = "",
    ) -> dict[str, str]:
        """Build L2 authentication headers.

        Args:
            method: HTTP method
            path: Request path
            body: Request body

        Returns:
            Headers dict with all required L2 auth headers
        """
        credentials = self._ensure_credentials()
        timestamp = str(int(time.time()))

        signature = self._build_l2_signature(method, path, timestamp, body)

        return {
            "POLY_ADDRESS": self.config.wallet_address,
            "POLY_SIGNATURE": signature,
            "POLY_TIMESTAMP": timestamp,
            "POLY_API_KEY": credentials.api_key,
            "POLY_PASSPHRASE": credentials.passphrase.get_secret_value(),
        }

    # =========================================================================
    # HTTP Request Helpers
    # =========================================================================

    def _request(
        self,
        method: str,
        url: str,
        authenticated: bool = False,
        params: dict | None = None,
        json_body: dict | None = None,
        _retry_count: int = 0,
    ) -> Any:
        """Make HTTP request with optional authentication and retry on rate limit.

        Args:
            method: HTTP method
            url: Full URL
            authenticated: Whether to use L2 authentication
            params: Query parameters
            json_body: JSON body for POST requests
            _retry_count: Internal retry counter

        Returns:
            Parsed JSON response

        Raises:
            PolymarketAPIError: If request fails
            PolymarketRateLimitError: If rate limited after max retries
        """
        headers: dict[str, str] = {"Content-Type": "application/json"}

        # Build path for signature
        path = url.replace(self.config.clob_base_url, "")
        if params:
            path = f"{path}?{urlencode(params)}"

        body = ""
        if json_body:
            import json as json_module

            body = json_module.dumps(json_body, separators=(",", ":"))

        if authenticated:
            auth_headers = self._build_l2_headers(method, path, body)
            headers.update(auth_headers)

        # Apply rate limiting before making the request
        self._rate_limiter.acquire()

        try:
            response = self._http.request(
                method=method,
                url=url,
                params=params,
                content=body if json_body else None,
                headers=headers,
            )

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                retry_seconds = int(retry_after) if retry_after else None

                # Handle rate limiting with exponential backoff
                if _retry_count < self.config.max_retries:
                    delay = self._calculate_backoff_delay(_retry_count, retry_seconds)
                    logger.warning(
                        "Rate limited, retrying with backoff",
                        retry_count=_retry_count + 1,
                        max_retries=self.config.max_retries,
                        delay_seconds=delay,
                    )
                    time.sleep(delay)
                    return self._request(
                        method=method,
                        url=url,
                        authenticated=authenticated,
                        params=params,
                        json_body=json_body,
                        _retry_count=_retry_count + 1,
                    )
                else:
                    logger.error(
                        "Rate limit exceeded after max retries",
                        retry_count=_retry_count,
                        max_retries=self.config.max_retries,
                    )
                    raise PolymarketRateLimitError(retry_after=retry_seconds)

            response.raise_for_status()

            if response.content:
                return response.json()
            return None

        except httpx.HTTPStatusError as e:
            logger.error(
                "API request failed",
                method=method,
                url=url,
                status_code=e.response.status_code,
                response=e.response.text[:500],
            )
            raise PolymarketAPIError(
                f"Request failed: {e.response.text}",
                status_code=e.response.status_code,
            ) from e

    def _calculate_backoff_delay(self, retry_count: int, retry_after: int | None = None) -> float:
        """Calculate exponential backoff delay with jitter.

        Args:
            retry_count: Current retry attempt (0-indexed)
            retry_after: Optional server-specified delay

        Returns:
            Delay in seconds
        """
        # If server specifies retry-after, respect it (with some jitter)
        if retry_after is not None:
            return min(retry_after + random.uniform(0, 1), self.config.max_retry_delay)

        # Exponential backoff: base * 2^retry_count + jitter
        base_delay = self.config.base_retry_delay * (2**retry_count)
        jitter = random.uniform(0, self.config.base_retry_delay)
        delay = base_delay + jitter

        return min(delay, self.config.max_retry_delay)

    def _get(self, endpoint: str, params: dict | None = None, authenticated: bool = False) -> Any:
        """Make GET request."""
        url = f"{self.config.clob_base_url}{endpoint}"
        return self._request("GET", url, authenticated=authenticated, params=params)

    def _post(self, endpoint: str, json_body: dict | None = None, authenticated: bool = True) -> Any:
        """Make POST request (authenticated by default)."""
        url = f"{self.config.clob_base_url}{endpoint}"
        return self._request("POST", url, authenticated=authenticated, json_body=json_body)

    def _delete(self, endpoint: str, params: dict | None = None, authenticated: bool = True) -> Any:
        """Make DELETE request (authenticated by default)."""
        url = f"{self.config.clob_base_url}{endpoint}"
        return self._request("DELETE", url, authenticated=authenticated, params=params)

    def _get_gamma(self, endpoint: str, params: dict | None = None) -> Any:
        """Make GET request to Gamma API."""
        url = f"{self.config.gamma_base_url}{endpoint}"
        return self._request("GET", url, params=params)

    def _get_data_api(self, endpoint: str, params: dict | None = None) -> Any:
        """Make GET request to Data API."""
        url = f"{self.config.data_api_base_url}{endpoint}"
        return self._request("GET", url, params=params)

    # =========================================================================
    # Caching
    # =========================================================================

    def _get_cached(self, key: str) -> Any | None:
        """Get cached value if not expired."""
        if key in self._cache:
            value, expires_at = self._cache[key]
            if time.time() < expires_at:
                return value
            del self._cache[key]
        return None

    def _set_cached(self, key: str, value: Any, ttl: int | None = None) -> None:
        """Set cached value with TTL."""
        if ttl is None:
            ttl = self.config.cache_ttl_seconds
        self._cache[key] = (value, time.time() + ttl)

    # =========================================================================
    # Public Endpoints (No Auth)
    # =========================================================================

    def health_check(self) -> bool:
        """Check if CLOB API is healthy.

        Returns:
            True if API is responding
        """
        try:
            self._get("/")
            return True
        except Exception:
            return False

    def get_server_time(self) -> int:
        """Get server timestamp.

        Returns:
            Unix timestamp from server
        """
        data = self._get("/time")
        return int(data.get("time", time.time()))

    # =========================================================================
    # Market Data (Gamma API)
    # =========================================================================

    def get_markets(self, filters: MarketFilters | None = None) -> list[GammaMarket]:
        """Get list of markets from Gamma API.

        Args:
            filters: Optional filters for the query

        Returns:
            List of GammaMarket objects
        """
        params: dict[str, Any] = {}
        if filters:
            if filters.active is not None:
                params["active"] = str(filters.active).lower()
            if filters.closed is not None:
                params["closed"] = str(filters.closed).lower()
            if filters.slug:
                params["slug"] = filters.slug
            if filters.condition_ids:
                params["condition_ids"] = ",".join(filters.condition_ids)
            if filters.clob_token_ids:
                params["clob_token_ids"] = ",".join(filters.clob_token_ids)
            if filters.event_id:
                params["event_id"] = filters.event_id
            if filters.event_slug:
                params["event_slug"] = filters.event_slug
            if filters.tag:
                params["tag"] = filters.tag
            params["limit"] = filters.limit
            params["offset"] = filters.offset

        data = self._get_gamma("/markets", params=params)

        markets = []
        for item in data if isinstance(data, list) else []:
            try:
                markets.append(GammaMarket.from_api_response(item))
            except Exception as e:
                logger.warning("Failed to parse market", error=str(e), market_id=item.get("id"))

        return markets

    def get_market(self, market_id: str) -> GammaMarket:
        """Get single market by ID.

        Args:
            market_id: Market ID

        Returns:
            GammaMarket object

        Raises:
            PolymarketAPIError: If market not found
        """
        cache_key = f"market:{market_id}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        data = self._get_gamma(f"/markets/{market_id}")
        market = GammaMarket.from_api_response(data)
        self._set_cached(cache_key, market)
        return market

    def get_market_by_slug(self, slug: str) -> GammaMarket | None:
        """Get market by URL slug.

        Args:
            slug: Market URL slug

        Returns:
            GammaMarket or None if not found
        """
        markets = self.get_markets(MarketFilters(slug=slug, limit=1))
        return markets[0] if markets else None

    # =========================================================================
    # Market Data (CLOB API)
    # =========================================================================

    def get_orderbook(self, token_id: str) -> OrderBook:
        """Get orderbook for a token.

        Args:
            token_id: CLOB token ID (YES or NO)

        Returns:
            OrderBook with bids and asks
        """
        cache_key = f"orderbook:{token_id}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        data = self._get("/book", params={"token_id": token_id})
        orderbook = OrderBook.from_api_response(data)
        self._set_cached(cache_key, orderbook)
        return orderbook

    def get_price(self, token_id: str) -> TokenPrice:
        """Get price for a token.

        Args:
            token_id: CLOB token ID

        Returns:
            TokenPrice with bid, ask, and mid
        """
        cache_key = f"price:{token_id}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        data = self._get("/price", params={"token_id": token_id})
        price = TokenPrice.from_api_response(data)
        self._set_cached(cache_key, price)
        return price

    def get_midpoint(self, token_id: str) -> Decimal:
        """Get midpoint price for a token.

        Args:
            token_id: CLOB token ID

        Returns:
            Midpoint price
        """
        data = self._get("/midpoint", params={"token_id": token_id})
        return Decimal(str(data.get("mid", "0")))

    def get_tick_size(self, token_id: str) -> Decimal:
        """Get minimum tick size for a token.

        Args:
            token_id: CLOB token ID

        Returns:
            Minimum tick size
        """
        data = self._get("/tick-size", params={"token_id": token_id})
        return Decimal(str(data.get("minimum_tick_size", "0.01")))

    # =========================================================================
    # Authenticated Endpoints - Balance
    # =========================================================================

    def get_balance_allowance(self, asset_type: str = "COLLATERAL", token_id: str | None = None) -> BalanceAllowance:
        """Get balance and allowance.

        Args:
            asset_type: "COLLATERAL" for USDC or "CONDITIONAL" for position tokens
            token_id: Token ID (required for CONDITIONAL)

        Returns:
            BalanceAllowance with current balance and allowance
        """
        params: dict[str, str] = {"asset_type": asset_type}
        if token_id:
            params["token_id"] = token_id

        data = self._get("/balance-allowance", params=params, authenticated=True)

        return BalanceAllowance(
            balance=Decimal(str(data.get("balance", "0"))),
            allowance=Decimal(str(data.get("allowance", "0"))),
        )

    # =========================================================================
    # Order Building
    # =========================================================================

    # Polymarket uses 6 decimals for USDC (10^6) and position tokens
    TOKEN_DECIMALS = 6
    DECIMAL_SCALE = 10**TOKEN_DECIMALS

    # Price constraints
    MIN_PRICE = Decimal("0.01")
    MAX_PRICE = Decimal("0.99")

    def _generate_salt(self) -> int:
        """Generate a random salt for order uniqueness.

        Returns:
            Random 256-bit integer
        """
        return int(secrets.token_hex(32), 16)

    def _to_token_units(self, amount: Decimal) -> int:
        """Convert decimal amount to token units (6 decimals).

        Args:
            amount: Decimal amount (e.g., 100.50)

        Returns:
            Integer amount in token units (e.g., 100500000)
        """
        # Round down to avoid overspending
        scaled = amount * self.DECIMAL_SCALE
        return int(scaled.quantize(Decimal("1"), rounding=ROUND_DOWN))

    def _validate_price(self, price: Decimal) -> None:
        """Validate price is within allowed range.

        Args:
            price: Price to validate (0.01 to 0.99)

        Raises:
            PolymarketInvalidPriceError: If price is out of range
        """
        if not (self.MIN_PRICE <= price <= self.MAX_PRICE):
            raise PolymarketInvalidPriceError(
                price=str(price),
                min_price=str(self.MIN_PRICE),
                max_price=str(self.MAX_PRICE),
            )

    # Default minimum order size - used when market metadata is unavailable
    DEFAULT_MIN_ORDER_SIZE = Decimal("5")

    def _validate_size(
        self,
        size: Decimal,
        min_size: Decimal | None = None,
        market: GammaMarket | None = None,
    ) -> None:
        """Validate order size meets market-specific minimum.

        The minimum order size is determined in order of priority:
        1. Explicit min_size parameter (if provided)
        2. Market's order_min_size field (if market metadata provided)
        3. DEFAULT_MIN_ORDER_SIZE fallback (5 shares)

        Args:
            size: Order size in shares
            min_size: Explicit minimum order size (overrides market default)
            market: GammaMarket metadata containing order_min_size

        Raises:
            PolymarketMinimumOrderError: If size is below minimum with actual market minimum in error
        """
        # Determine the effective minimum size
        if min_size is not None:
            effective_min = min_size
        elif market is not None:
            effective_min = market.order_min_size
        else:
            effective_min = self.DEFAULT_MIN_ORDER_SIZE

        if size < effective_min:
            raise PolymarketMinimumOrderError(size=str(size), minimum=str(effective_min))

    # Default tick size used when market metadata is unavailable
    DEFAULT_TICK_SIZE = Decimal("0.01")

    def _validate_tick_size(
        self,
        price: Decimal,
        tick_size: Decimal | None = None,
        market: GammaMarket | None = None,
    ) -> None:
        """Validate price conforms to market tick size.

        Prices must be exact multiples of the tick size. This method checks
        that the price can be expressed as an integer number of ticks.

        Args:
            price: Order price to validate
            tick_size: Explicit tick size (overrides market default)
            market: GammaMarket metadata containing order_price_min_tick_size

        Raises:
            PolymarketInvalidTickSizeError: If price is not a valid tick multiple

        Example:
            >>> # With tick_size=0.01, valid prices are 0.01, 0.02, ..., 0.99
            >>> self._validate_tick_size(Decimal("0.65"), tick_size=Decimal("0.01"))  # OK
            >>> self._validate_tick_size(Decimal("0.655"), tick_size=Decimal("0.01"))  # Raises
        """
        # Determine effective tick size
        if tick_size is not None:
            effective_tick = tick_size
        elif market is not None:
            effective_tick = market.order_price_min_tick_size
        else:
            effective_tick = self.DEFAULT_TICK_SIZE

        # Check if price is a multiple of tick size
        # price / tick_size should be an integer (with small tolerance for floating point)
        if effective_tick <= 0:
            return  # Invalid tick size, skip validation

        # Use modulo to check if price is a valid multiple
        remainder = price % effective_tick

        # Allow small tolerance for decimal precision issues
        tolerance = effective_tick / Decimal("1000")
        is_valid = remainder < tolerance or (effective_tick - remainder) < tolerance

        if not is_valid:
            # Calculate nearest valid price for error message
            ticks = price / effective_tick
            nearest_ticks = round(ticks)
            nearest_valid = nearest_ticks * effective_tick

            raise PolymarketInvalidTickSizeError(
                price=str(price),
                tick_size=str(effective_tick),
                nearest_valid=str(nearest_valid),
            )

    def _round_to_tick_size(
        self,
        price: Decimal,
        tick_size: Decimal,
        side: str,
    ) -> Decimal:
        """Round price to valid tick size.

        Rounding direction depends on order side to ensure the resulting
        price is favorable or neutral (never worse) for the order:
        - BUY orders: round DOWN (floor) to avoid overpaying
        - SELL orders: round UP (ceiling) to avoid underselling

        Args:
            price: Price to round
            tick_size: Market tick size
            side: Order side ("BUY" or "SELL")

        Returns:
            Price rounded to nearest valid tick

        Example:
            >>> self._round_to_tick_size(Decimal("0.655"), Decimal("0.01"), "BUY")
            Decimal("0.65")
            >>> self._round_to_tick_size(Decimal("0.655"), Decimal("0.01"), "SELL")
            Decimal("0.66")
        """
        if tick_size <= 0:
            return price

        # Calculate number of ticks
        ticks = price / tick_size

        # Round based on side
        if side == "BUY":
            # Floor for buys - don't pay more than intended
            rounded_ticks = ticks.quantize(Decimal("1"), rounding=ROUND_FLOOR)
        else:
            # Ceiling for sells - don't receive less than intended
            rounded_ticks = ticks.quantize(Decimal("1"), rounding=ROUND_CEILING)

        rounded_price = rounded_ticks * tick_size

        # Clamp to valid price range
        rounded_price = max(self.MIN_PRICE, min(self.MAX_PRICE, rounded_price))

        return rounded_price

    def round_price_to_tick(
        self,
        price: Decimal,
        side: str,
        market: GammaMarket | None = None,
        tick_size: Decimal | None = None,
    ) -> Decimal:
        """Round price to valid tick size for the market.

        Public method for rounding prices before order submission.
        Use this when you want automatic rounding instead of validation errors.

        Args:
            price: Price to round
            side: Order side ("BUY" or "SELL")
            market: Optional GammaMarket for market-specific tick size
            tick_size: Optional explicit tick size (overrides market)

        Returns:
            Price rounded to nearest valid tick

        Example:
            >>> market = client.get_market(market_id)
            >>> rounded = client.round_price_to_tick(Decimal("0.655"), "BUY", market=market)
        """
        effective_tick = tick_size or (market.order_price_min_tick_size if market else self.DEFAULT_TICK_SIZE)
        return self._round_to_tick_size(price, effective_tick, side)

    def build_limit_order(
        self,
        params: LimitOrderParams,
        market: GammaMarket | None = None,
    ) -> UnsignedOrder:
        """Build an unsigned limit order.

        Limit orders specify exact price and size. They remain on the orderbook
        until filled, cancelled, or expired.

        For BUY orders:
            - You spend USDC (maker_amount = size * price)
            - You receive shares (taker_amount = size)

        For SELL orders:
            - You spend shares (maker_amount = size)
            - You receive USDC (taker_amount = size * price)

        Args:
            params: Limit order parameters
            market: Optional GammaMarket metadata for market-specific validation.
                   If provided, uses market.order_min_size for size validation
                   and market.order_price_min_tick_size for tick validation.

        Returns:
            UnsignedOrder ready for signing

        Raises:
            PolymarketInvalidPriceError: If price is out of range (0.01-0.99)
            PolymarketInvalidTickSizeError: If price is not a valid tick multiple
            PolymarketMinimumOrderError: If size is below market minimum

        Example:
            >>> params = LimitOrderParams(
            ...     token_id="123...",
            ...     side="BUY",
            ...     price=Decimal("0.65"),
            ...     size=Decimal("100"),
            ... )
            >>> order = client.build_limit_order(params)
            >>>
            >>> # With market metadata for proper minimum and tick validation
            >>> market = client.get_market(market_id)
            >>> order = client.build_limit_order(params, market=market)
        """
        # Validate inputs using market-specific minimum and tick size
        self._validate_price(params.price)
        self._validate_tick_size(params.price, market=market)
        self._validate_size(params.size, market=market)

        wallet = self.config.wallet_address
        sig_type = self.config.signature_type.value

        # Calculate amounts based on side
        # BUY: pay USDC, receive shares
        # SELL: pay shares, receive USDC
        if params.side == "BUY":
            # maker pays: size * price in USDC
            # taker receives: size in shares
            usdc_amount = params.size * params.price
            maker_amount = self._to_token_units(usdc_amount)
            taker_amount = self._to_token_units(params.size)
            side = OrderSide.BUY.value
        else:  # SELL
            # maker pays: size in shares
            # taker receives: size * price in USDC
            usdc_amount = params.size * params.price
            maker_amount = self._to_token_units(params.size)
            taker_amount = self._to_token_units(usdc_amount)
            side = OrderSide.SELL.value

        # Build the order struct
        return UnsignedOrder(
            salt=self._generate_salt(),
            maker=wallet,
            signer=wallet,  # For EOA, maker and signer are the same
            taker="0x0000000000000000000000000000000000000000",  # Public order
            token_id=int(params.token_id),
            maker_amount=maker_amount,
            taker_amount=taker_amount,
            expiration=params.expiration or 0,  # 0 = no expiry
            nonce=0,  # Used for on-chain cancellation
            fee_rate_bps=params.fee_rate_bps,
            side=side,
            signature_type=sig_type,
        )

    def build_market_order(
        self,
        params: MarketOrderParams,
        market: GammaMarket | None = None,
    ) -> UnsignedOrder:
        """Build an unsigned market order.

        Market orders execute immediately at the best available price.
        They should be submitted with IOC (Immediate or Cancel) order type.

        For BUY orders:
            - You specify USDC amount to spend
            - worst_price sets the maximum price per share

        For SELL orders:
            - You specify number of shares to sell
            - worst_price sets the minimum price per share

        Args:
            params: Market order parameters
            market: Optional GammaMarket metadata for market-specific validation.
                   If provided, uses market.order_min_size for size validation
                   and market.order_price_min_tick_size for tick validation.

        Returns:
            UnsignedOrder ready for signing

        Raises:
            PolymarketInvalidPriceError: If worst_price is out of range (0.01-0.99)
            PolymarketInvalidTickSizeError: If worst_price is not a valid tick multiple
            PolymarketMinimumOrderError: If amount is below market minimum

        Example:
            >>> params = MarketOrderParams(
            ...     token_id="123...",
            ...     side="BUY",
            ...     amount=Decimal("100"),  # USDC to spend
            ...     worst_price=Decimal("0.70"),  # Max price per share
            ... )
            >>> order = client.build_market_order(params)
            >>>
            >>> # With market metadata for proper minimum and tick validation
            >>> market = client.get_market(market_id)
            >>> order = client.build_market_order(params, market=market)
        """
        # Use worst_price or default to max/min depending on side
        if params.worst_price is not None:
            self._validate_price(params.worst_price)
            self._validate_tick_size(params.worst_price, market=market)
            price = params.worst_price
        else:
            # Default: aggressive price to ensure fill
            # Note: MAX_PRICE (0.99) and MIN_PRICE (0.01) are always valid ticks
            price = self.MAX_PRICE if params.side == "BUY" else self.MIN_PRICE

        wallet = self.config.wallet_address
        sig_type = self.config.signature_type.value

        if params.side == "BUY":
            # For market BUY: amount is USDC to spend
            # Calculate expected shares at worst price
            expected_shares = params.amount / price
            self._validate_size(expected_shares, market=market)

            maker_amount = self._to_token_units(params.amount)
            taker_amount = self._to_token_units(expected_shares)
            side = OrderSide.BUY.value
        else:  # SELL
            # For market SELL: amount is shares to sell
            self._validate_size(params.amount, market=market)

            expected_usdc = params.amount * price
            maker_amount = self._to_token_units(params.amount)
            taker_amount = self._to_token_units(expected_usdc)
            side = OrderSide.SELL.value

        return UnsignedOrder(
            salt=self._generate_salt(),
            maker=wallet,
            signer=wallet,
            taker="0x0000000000000000000000000000000000000000",
            token_id=int(params.token_id),
            maker_amount=maker_amount,
            taker_amount=taker_amount,
            expiration=0,  # Market orders should not expire
            nonce=0,
            fee_rate_bps=0,  # Standard fee
            side=side,
            signature_type=sig_type,
        )

    def sign_order(self, order: UnsignedOrder) -> SignedOrder:
        """Sign an order using EIP-712 typed data signing.

        This creates a cryptographic signature that proves the order
        was created by the wallet owner.

        Args:
            order: Unsigned order to sign

        Returns:
            SignedOrder with signature attached

        Example:
            >>> unsigned = client.build_limit_order(params)
            >>> signed = client.sign_order(unsigned)
            >>> response = client.submit_order(signed)
        """
        # Build EIP-712 typed data
        typed_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                **ORDER_TYPES,
            },
            "primaryType": "Order",
            "domain": CTF_EXCHANGE_DOMAIN,
            "message": order.to_struct(),
        }

        # Sign with private key
        private_key = self.config.private_key.get_secret_value()
        signable = encode_typed_data(full_message=typed_data)
        signed = Account.sign_message(signable, private_key)

        logger.debug(
            "Order signed",
            token_id=order.token_id,
            side="BUY" if order.side == 0 else "SELL",
            maker_amount=order.maker_amount,
            taker_amount=order.taker_amount,
        )

        return SignedOrder(order=order, signature=signed.signature.hex())

    def create_and_sign_limit_order(
        self,
        params: LimitOrderParams,
        market: GammaMarket | None = None,
    ) -> SignedOrder:
        """Build and sign a limit order in one call.

        Convenience method that combines build_limit_order and sign_order.

        Args:
            params: Limit order parameters
            market: Optional GammaMarket metadata for market-specific validation

        Returns:
            SignedOrder ready for submission
        """
        unsigned = self.build_limit_order(params, market=market)
        return self.sign_order(unsigned)

    def create_and_sign_market_order(
        self,
        params: MarketOrderParams,
        market: GammaMarket | None = None,
    ) -> SignedOrder:
        """Build and sign a market order in one call.

        Convenience method that combines build_market_order and sign_order.

        Args:
            params: Market order parameters
            market: Optional GammaMarket metadata for market-specific validation

        Returns:
            SignedOrder ready for submission
        """
        unsigned = self.build_market_order(params, market=market)
        return self.sign_order(unsigned)

    # =========================================================================
    # Authenticated Endpoints - Orders
    # =========================================================================

    def submit_order(self, order: SignedOrder, order_type: OrderType = OrderType.GTC) -> OrderResponse:
        """Submit a signed order.

        Args:
            order: Signed order to submit
            order_type: Order type (GTC, IOC, FOK)

        Returns:
            OrderResponse with order ID and status
        """
        payload = order.to_api_payload()
        payload["orderType"] = order_type.value

        logger.info(
            "Submitting order",
            token_id=order.order.token_id,
            side="BUY" if order.order.side == 0 else "SELL",
            order_type=order_type.value,
        )

        data = self._post("/order", json_body=payload)
        return OrderResponse.from_api_response(data)

    def submit_order_payload(self, payload: dict[str, Any]) -> OrderResponse:
        """Submit an order from a pre-built payload dict.

        This method is used by ClobActionHandler to submit orders from
        ActionBundle metadata where the payload is already prepared.

        Args:
            payload: Order payload dict containing 'order', 'signature', and 'orderType'

        Returns:
            OrderResponse with order ID and status
        """
        logger.info(
            "Submitting order from payload",
            token_id=payload.get("order", {}).get("tokenId"),
            order_type=payload.get("orderType", "GTC"),
        )

        data = self._post("/order", json_body=payload)
        return OrderResponse.from_api_response(data)

    def get_order(self, order_id: str) -> OpenOrder | None:
        """Get a single order by ID.

        Args:
            order_id: Order ID to retrieve

        Returns:
            OpenOrder if found, None otherwise
        """
        # The CLOB API doesn't have a single order endpoint, so we query open orders
        # and filter. For filled/cancelled orders, we query order history.
        try:
            # First check open orders
            data = self._get("/data/orders", params={"orderID": order_id}, authenticated=True)
            if isinstance(data, list) and len(data) > 0:
                item = data[0]
                return OpenOrder(
                    order_id=item.get("orderID", ""),
                    market=item.get("market", ""),
                    side=item.get("side", "BUY"),
                    price=Decimal(str(item.get("price", "0"))),
                    size=Decimal(str(item.get("size", "0"))),
                    filled_size=Decimal(str(item.get("filledSize", "0"))),
                    created_at=datetime.fromisoformat(item["createdAt"].replace("Z", "+00:00"))
                    if item.get("createdAt")
                    else None,
                    expiration=item.get("expiration"),
                )
            return None
        except Exception as e:
            logger.warning("Failed to get order", order_id=order_id, error=str(e))
            return None

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an order by ID.

        Args:
            order_id: Order ID to cancel

        Returns:
            True if cancelled successfully
        """
        logger.info("Cancelling order", order_id=order_id)
        self._delete("/order", params={"orderID": order_id})
        return True

    def cancel_orders(self, order_ids: list[str]) -> bool:
        """Cancel multiple orders.

        Args:
            order_ids: List of order IDs to cancel

        Returns:
            True if cancelled successfully
        """
        logger.info("Cancelling orders", count=len(order_ids))
        self._delete("/orders", params={"orderIDs": ",".join(order_ids)})
        return True

    def cancel_all_orders(self) -> bool:
        """Cancel all open orders.

        Returns:
            True if cancelled successfully
        """
        logger.info("Cancelling all orders")
        self._delete("/cancel-all")
        return True

    def get_open_orders(self, filters: OrderFilters | None = None) -> list[OpenOrder]:
        """Get open orders.

        Args:
            filters: Optional filters

        Returns:
            List of open orders
        """
        params: dict[str, Any] = {}
        if filters:
            if filters.market:
                params["market"] = filters.market
            params["limit"] = filters.limit

        data = self._get("/data/orders", params=params, authenticated=True)

        orders = []
        for item in data if isinstance(data, list) else []:
            try:
                orders.append(
                    OpenOrder(
                        order_id=item.get("orderID", ""),
                        market=item.get("market", ""),
                        side=item.get("side", "BUY"),
                        price=Decimal(str(item.get("price", "0"))),
                        size=Decimal(str(item.get("size", "0"))),
                        filled_size=Decimal(str(item.get("filledSize", "0"))),
                        created_at=datetime.fromisoformat(item["createdAt"].replace("Z", "+00:00"))
                        if item.get("createdAt")
                        else None,
                        expiration=item.get("expiration"),
                    )
                )
            except Exception as e:
                logger.warning("Failed to parse order", error=str(e))

        return orders

    def get_trades(self, filters: TradeFilters | None = None) -> list[Trade]:
        """Get trade history.

        Args:
            filters: Optional filters

        Returns:
            List of trades
        """
        params: dict[str, Any] = {}
        if filters:
            if filters.market:
                params["market"] = filters.market
            if filters.after:
                params["after"] = filters.after.isoformat()
            if filters.before:
                params["before"] = filters.before.isoformat()
            params["limit"] = filters.limit

        data = self._get("/data/trades", params=params, authenticated=True)

        from .models import TradeStatus

        trades = []
        for item in data if isinstance(data, list) else []:
            try:
                trades.append(
                    Trade(
                        id=item.get("id", ""),
                        market_id=item.get("market", ""),
                        token_id=item.get("tokenId", ""),
                        side=item.get("side", "BUY"),
                        price=Decimal(str(item.get("price", "0"))),
                        size=Decimal(str(item.get("size", "0"))),
                        fee=Decimal(str(item.get("fee", "0"))),
                        timestamp=datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))
                        if item.get("timestamp")
                        else datetime.now(UTC),
                        status=TradeStatus(item.get("status", "CONFIRMED")),
                    )
                )
            except Exception as e:
                logger.warning("Failed to parse trade", error=str(e))

        return trades

    # =========================================================================
    # Positions (Data API)
    # =========================================================================

    def get_positions(self, wallet: str | None = None, filters: PositionFilters | None = None) -> list[Position]:
        """Get positions for a wallet.

        Queries the Polymarket Data API to retrieve all open prediction
        market positions for the specified wallet.

        Args:
            wallet: Wallet address to query. Defaults to config wallet if not specified.
            filters: Optional filters for market or outcome

        Returns:
            List of Position objects with size, prices, and PnL data

        Example:
            >>> positions = client.get_positions()
            >>> for pos in positions:
            ...     print(f"{pos.outcome}: {pos.size} shares at {pos.avg_price}")
        """
        if wallet is None:
            wallet = self.config.wallet_address

        params: dict[str, Any] = {"user": wallet}
        if filters:
            if filters.market:
                params["market"] = filters.market
            if filters.outcome:
                params["outcome"] = filters.outcome

        data = self._get_data_api("/positions", params=params)

        positions = []
        for item in data if isinstance(data, list) else []:
            try:
                # Determine outcome from token ID position in market
                from typing import Literal

                outcome: Literal["YES", "NO"] = "YES" if item.get("outcome") in ["Yes", "YES", "yes"] else "NO"

                # Parse position data
                size = Decimal(str(item.get("size", "0")))
                avg_price = Decimal(str(item.get("avgPrice", "0")))
                current_price = Decimal(str(item.get("currentPrice", "0")))

                # Calculate unrealized PnL: (current_price - avg_price) * size
                unrealized_pnl = (current_price - avg_price) * size

                positions.append(
                    Position(
                        market_id=item.get("market", ""),
                        condition_id=item.get("conditionId", ""),
                        token_id=item.get("tokenId", ""),
                        outcome=outcome,
                        size=size,
                        avg_price=avg_price,
                        current_price=current_price,
                        unrealized_pnl=unrealized_pnl,
                        realized_pnl=Decimal(str(item.get("realizedPnl", "0"))),
                    )
                )
            except Exception as e:
                logger.warning("Failed to parse position", error=str(e))

        return positions

    # =========================================================================
    # Historical Data
    # =========================================================================

    def get_price_history(
        self,
        token_id: str,
        interval: str | PriceHistoryInterval | None = None,
        start_ts: int | None = None,
        end_ts: int | None = None,
        fidelity: int | None = None,
    ) -> PriceHistory:
        """Get historical price data for a token.

        Fetches time-series price data from the CLOB API. Can query by
        predefined interval or custom time range.

        Args:
            token_id: CLOB token ID (YES or NO outcome)
            interval: Predefined interval (1m, 1h, 6h, 1d, 1w, max).
                Mutually exclusive with start_ts/end_ts.
            start_ts: Unix timestamp for start of range (UTC).
                Requires end_ts. Mutually exclusive with interval.
            end_ts: Unix timestamp for end of range (UTC).
                Requires start_ts. Mutually exclusive with interval.
            fidelity: Data resolution in minutes (e.g., 1, 5, 15, 60).
                Optional for both modes.

        Returns:
            PriceHistory with list of timestamped prices

        Raises:
            PolymarketAPIError: If request fails
            ValueError: If both interval and start_ts/end_ts are provided

        Example:
            >>> # Get last 24 hours
            >>> history = client.get_price_history(token_id, interval="1d")
            >>> print(f"Open: {history.open_price}, Close: {history.close_price}")
            >>>
            >>> # Get custom range
            >>> history = client.get_price_history(
            ...     token_id,
            ...     start_ts=1700000000,
            ...     end_ts=1700100000,
            ...     fidelity=5,  # 5-minute resolution
            ... )
        """
        # Validate parameters
        if interval and (start_ts or end_ts):
            raise ValueError("Cannot specify both interval and start_ts/end_ts")
        if (start_ts is None) != (end_ts is None):
            raise ValueError("start_ts and end_ts must be specified together")

        params: dict[str, str | int] = {"market": token_id}

        if interval:
            interval_value = interval.value if isinstance(interval, PriceHistoryInterval) else interval
            params["interval"] = interval_value
        elif start_ts and end_ts:
            params["startTs"] = start_ts
            params["endTs"] = end_ts

        if fidelity:
            params["fidelity"] = fidelity

        # Cache key includes all parameters
        cache_key = f"price_history:{token_id}:{interval}:{start_ts}:{end_ts}:{fidelity}"

        # Use longer TTL for historical data (older data is more stable)
        historical_ttl = 60  # 1 minute for historical data
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        data = self._get("/prices-history", params=params)

        # Parse response
        history_data = data.get("history", [])
        prices = [HistoricalPrice.from_api_response(p) for p in history_data]

        result = PriceHistory(
            token_id=token_id,
            interval=interval.value if isinstance(interval, PriceHistoryInterval) else (interval or "custom"),
            prices=prices,
            start_time=prices[0].timestamp if prices else None,
            end_time=prices[-1].timestamp if prices else None,
        )

        self._set_cached(cache_key, result, ttl=historical_ttl)

        logger.debug(
            "Fetched price history",
            token_id=token_id,
            interval=interval,
            points=len(prices),
        )

        return result

    def get_trade_tape(
        self,
        token_id: str | None = None,
        limit: int = 100,
    ) -> list[HistoricalTrade]:
        """Get recent executed trades (trade tape).

        Fetches the most recent trades for analysis. Can be filtered by
        token ID for market-specific trades.

        Args:
            token_id: Optional CLOB token ID to filter trades
            limit: Maximum number of trades to return (default 100, max 500)

        Returns:
            List of HistoricalTrade objects, newest first

        Raises:
            PolymarketAPIError: If request fails

        Example:
            >>> # Get recent trades for YES token
            >>> trades = client.get_trade_tape(token_id="123...", limit=50)
            >>> for trade in trades:
            ...     print(f"{trade.side} {trade.size} @ {trade.price}")
        """
        params: dict[str, str | int] = {"limit": min(limit, 500)}
        if token_id:
            params["market"] = token_id

        # Get trades from authenticated endpoint
        data = self._get("/data/trades", params=params, authenticated=True)

        trades = []
        for item in data if isinstance(data, list) else []:
            try:
                trades.append(HistoricalTrade.from_api_response(item))
            except Exception as e:
                logger.warning("Failed to parse trade", error=str(e))

        logger.debug(
            "Fetched trade tape",
            token_id=token_id,
            count=len(trades),
        )

        return trades


__all__ = ["ClobClient", "TokenBucketRateLimiter"]
