"""Polymarket configuration and data models.

This module provides Pydantic models for:
- API credentials and configuration
- Market data structures
- Order structures
- Position and trade data

Model Type Usage:
    This file uses three model types with specific purposes:

    1. Enum: For enumerated constants with fixed values
       - SignatureType, OrderSide, OrderType, OrderStatus, TradeStatus, PriceHistoryInterval
       - Used when you have a closed set of known values

    2. Pydantic BaseModel: For external API models requiring validation
       - ApiCredentials, PolymarketConfig, GammaMarket, OrderBook, OrderResponse, etc.
       - Used for data crossing system boundaries (API responses, configuration)
       - Provides automatic validation, type coercion, and serialization

    3. @dataclass: For internal data structures without validation overhead
       - LimitOrderParams, MarketOrderParams, UnsignedOrder, SignedOrder, HistoricalPrice, etc.
       - Used for trusted internal data structures and method parameters
       - Minimal overhead, good for performance-sensitive code

    See blueprints/18-model-type-selection.md for detailed guidelines on when to use each type.
"""

import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum, StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field, SecretStr, field_validator

# =============================================================================
# Constants
# =============================================================================

# CLOB API base URLs
CLOB_BASE_URL = "https://clob.polymarket.com"
GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
DATA_API_BASE_URL = "https://data-api.polymarket.com"

# Contract addresses (Polygon Mainnet)
CTF_EXCHANGE = "0x4bFb41d5B3570DeFd03C39a9A4D8dE6Bd8B8982E"
NEG_RISK_EXCHANGE = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
USDC_POLYGON = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"

# Chain ID
POLYGON_CHAIN_ID = 137


# =============================================================================
# Enums
# =============================================================================


class SignatureType(int, Enum):
    """Signature types for Polymarket authentication."""

    EOA = 0  # Standard wallet (MetaMask, private key)
    POLY_PROXY = 1  # Email/Magic wallet proxy
    POLY_GNOSIS_SAFE = 2  # Gnosis Safe multisig


class OrderSide(int, Enum):
    """Order side."""

    BUY = 0
    SELL = 1


class OrderType(StrEnum):
    """Order time-in-force types."""

    GTC = "GTC"  # Good Till Cancelled
    IOC = "IOC"  # Immediate or Cancel
    FOK = "FOK"  # Fill or Kill


class OrderStatus(StrEnum):
    """Order status values."""

    LIVE = "LIVE"
    MATCHED = "MATCHED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"


class TradeStatus(StrEnum):
    """Trade status values."""

    MATCHED = "MATCHED"
    MINED = "MINED"
    CONFIRMED = "CONFIRMED"


# =============================================================================
# Credentials
# =============================================================================


class ApiCredentials(BaseModel):
    """Polymarket API credentials from L1 authentication.

    These credentials are obtained by signing an EIP-712 message
    and calling the create/derive API key endpoint.

    Credential Handling Limitations:
        The current implementation has the following limitations that users should be aware of:

        1. **Storage**: Credentials are stored in environment variables only. There is no
           integration with secret management systems (e.g., AWS Secrets Manager, HashiCorp Vault).
           Users are responsible for secure credential storage in their deployment environment.

        2. **No Automatic Rotation**: API credentials do not auto-rotate. The Polymarket CLOB API
           credentials have an expiration (typically 7 days from creation). Users must manually
           regenerate credentials before expiration by calling `ClobClient.create_api_credentials()`
           or `ClobClient.derive_api_credentials()` and updating environment variables.

        3. **No Expiration Monitoring**: The connector does not monitor credential expiration.
           If credentials expire, API calls will fail with authentication errors. Users should
           implement their own monitoring or credential refresh workflow.

        4. **Manual Renewal Process**:
           - Generate new credentials using L1 authentication (EIP-712 signature)
           - Update environment variables: POLYMARKET_API_KEY, POLYMARKET_SECRET, POLYMARKET_PASSPHRASE
           - Restart the application or reload configuration

    Environment Variables:
        - POLYMARKET_API_KEY: API key for L2 HMAC authentication (required for trading)
        - POLYMARKET_SECRET: Base64-encoded HMAC secret (required for trading)
        - POLYMARKET_PASSPHRASE: Passphrase for API requests (required for trading)

    Example:
        # Load from environment
        creds = ApiCredentials.from_env()

        # Or create manually
        creds = ApiCredentials(
            api_key="your-api-key",
            secret=SecretStr("your-base64-secret"),
            passphrase=SecretStr("your-passphrase"),
        )
    """

    api_key: str = Field(description="API key for L2 authentication")
    secret: SecretStr = Field(description="HMAC secret for signing requests (base64-encoded)")
    passphrase: SecretStr = Field(description="Passphrase for requests")

    @classmethod
    def from_dict(cls, data: dict) -> "ApiCredentials":
        """Create from API response."""
        return cls(
            api_key=data["apiKey"],
            secret=SecretStr(data["secret"]),
            passphrase=SecretStr(data["passphrase"]),
        )

    @classmethod
    def from_env(
        cls,
        api_key_env: str = "POLYMARKET_API_KEY",
        secret_env: str = "POLYMARKET_SECRET",
        passphrase_env: str = "POLYMARKET_PASSPHRASE",
    ) -> "ApiCredentials":
        """Load credentials from environment variables."""
        api_key = os.environ.get(api_key_env)
        secret = os.environ.get(secret_env)
        passphrase = os.environ.get(passphrase_env)

        if not api_key:
            raise ValueError(f"Environment variable {api_key_env} not set")
        if not secret:
            raise ValueError(f"Environment variable {secret_env} not set")
        if not passphrase:
            raise ValueError(f"Environment variable {passphrase_env} not set")

        return cls(
            api_key=api_key,
            secret=SecretStr(secret),
            passphrase=SecretStr(passphrase),
        )


class PolymarketConfig(BaseModel):
    """Configuration for Polymarket connector.

    This configuration supports both L1 (EIP-712 signing) and L2 (HMAC API) authentication
    for Polymarket's hybrid CLOB + on-chain architecture.

    Credential Management:
        Polymarket uses a two-tier authentication system:

        **L1 Authentication (Wallet Signing)**:
            - Used for: Creating/deriving API credentials, signing on-chain transactions
            - Requires: wallet_address and private_key
            - No expiration: Wallet keys don't expire

        **L2 Authentication (HMAC API)**:
            - Used for: Orderbook operations (place/cancel orders, fetch positions)
            - Requires: api_credentials (api_key, secret, passphrase)
            - Expiration: Credentials typically expire after 7 days
            - Storage: Environment variables only (no secret manager integration)

        **Current Limitations**:
            1. Credentials stored in environment variables only
            2. No automatic credential rotation or renewal
            3. No expiration monitoring - API calls will fail silently when expired
            4. Manual renewal required before expiration

        **Renewal Process**:
            When credentials expire, you must:
            1. Use ClobClient with a valid wallet to call create_api_credentials()
               or derive_api_credentials()
            2. Update environment variables with new credentials
            3. Restart the application or reload configuration

    Configurable URLs:
        All API URLs are configurable to support proxies, test environments, or alternative endpoints:

        - clob_base_url: CLOB API for orderbook, prices, and trading
          (default: https://clob.polymarket.com)
        - gamma_base_url: Gamma Markets API for market metadata
          (default: https://gamma-api.polymarket.com)
        - data_api_base_url: Data API for positions and user data
          (default: https://data-api.polymarket.com)

    Environment Variables:
        Required for wallet operations:
            - POLYMARKET_WALLET_ADDRESS: Wallet address for signing and transactions
            - POLYMARKET_PRIVATE_KEY: Private key for EIP-712 signing (0x prefixed hex)

        Required for trading operations (L2 auth):
            - POLYMARKET_API_KEY: API key from credential creation/derivation
            - POLYMARKET_SECRET: Base64-encoded HMAC secret
            - POLYMARKET_PASSPHRASE: Passphrase from credential creation/derivation

        Optional:
            - POLYGON_RPC_URL: RPC endpoint for Polygon (for on-chain operations)
            - POLYMARKET_CLOB_URL: Override clob_base_url
            - POLYMARKET_GAMMA_URL: Override gamma_base_url
            - POLYMARKET_DATA_API_URL: Override data_api_base_url

    Example:
        # Basic configuration (wallet only, for read operations and credential creation)
        config = PolymarketConfig(
            wallet_address="0x...",
            private_key=SecretStr("0x..."),
        )

        # Full configuration with API credentials (for trading)
        config = PolymarketConfig(
            wallet_address="0x...",
            private_key=SecretStr("0x..."),
            api_credentials=ApiCredentials(
                api_key="your-api-key",
                secret=SecretStr("your-base64-secret"),
                passphrase=SecretStr("your-passphrase"),
            ),
        )

        # Load from environment
        config = PolymarketConfig.from_env()
        creds = ApiCredentials.from_env()
        config.api_credentials = creds

        # With custom URLs (for proxies or testing)
        config = PolymarketConfig(
            wallet_address="0x...",
            private_key=SecretStr("0x..."),
            data_api_base_url="https://my-proxy.example.com/data",
        )
    """

    chain: str = Field(default="polygon", description="Chain identifier")
    wallet_address: str = Field(description="User wallet address")
    private_key: SecretStr = Field(description="Private key for signing")
    signature_type: SignatureType = Field(default=SignatureType.EOA, description="Signature type for authentication")
    funder_address: str | None = Field(default=None, description="Funder address for proxy wallets")
    rpc_url: str | None = Field(default=None, description="RPC endpoint for Polygon")
    clob_base_url: str = Field(default=CLOB_BASE_URL, description="CLOB API base URL")
    gamma_base_url: str = Field(default=GAMMA_BASE_URL, description="Gamma Markets API base URL")
    data_api_base_url: str = Field(
        default=DATA_API_BASE_URL, description="Data API base URL for positions and user data"
    )
    api_credentials: ApiCredentials | None = Field(default=None, description="Pre-existing API credentials")

    # Cache settings
    cache_ttl_seconds: int = Field(default=5, ge=1, description="Cache TTL for market data")

    # Rate limiting
    rate_limit_requests_per_second: float = Field(default=30.0, ge=1.0, description="Max requests per second")
    rate_limit_enabled: bool = Field(default=True, description="Enable proactive rate limiting (disable for testing)")

    # Retry settings for rate limiting
    max_retries: int = Field(default=3, ge=0, le=10, description="Max retries on rate limit")
    base_retry_delay: float = Field(
        default=1.0, ge=0.1, le=10.0, description="Base delay for exponential backoff (seconds)"
    )
    max_retry_delay: float = Field(
        default=30.0, ge=1.0, le=120.0, description="Max delay for exponential backoff (seconds)"
    )

    # Gas pricing configuration
    max_priority_fee_gwei: float | None = Field(
        default=None,
        ge=0.1,
        le=500.0,
        description="Max priority fee in gwei for EIP-1559 transactions. If None, uses network defaults.",
    )
    max_fee_multiplier: float = Field(
        default=2.0,
        ge=1.0,
        le=10.0,
        description="Multiplier for base fee to calculate maxFeePerGas (e.g., 2.0 = baseFee * 2)",
    )
    use_legacy_gas: bool = Field(
        default=False,
        description="Use legacy gas pricing (gasPrice) instead of EIP-1559 (maxFeePerGas/maxPriorityFeePerGas)",
    )

    @field_validator("wallet_address", mode="before")
    @classmethod
    def checksum_wallet(cls, v: str) -> str:
        """Ensure wallet address is checksummed."""
        from web3 import Web3

        return Web3.to_checksum_address(v)

    @classmethod
    def from_env(
        cls,
        wallet_env: str = "POLYMARKET_WALLET_ADDRESS",
        private_key_env: str = "POLYMARKET_PRIVATE_KEY",
        rpc_env: str = "POLYGON_RPC_URL",
        clob_url_env: str = "POLYMARKET_CLOB_URL",
        gamma_url_env: str = "POLYMARKET_GAMMA_URL",
        data_api_url_env: str = "POLYMARKET_DATA_API_URL",
    ) -> "PolymarketConfig":
        """Load configuration from environment variables.

        Required Environment Variables:
            - POLYMARKET_WALLET_ADDRESS: Wallet address
            - POLYMARKET_PRIVATE_KEY: Private key for signing

        Optional Environment Variables:
            - POLYGON_RPC_URL: RPC endpoint for Polygon
            - POLYMARKET_CLOB_URL: Override CLOB API base URL
            - POLYMARKET_GAMMA_URL: Override Gamma Markets API base URL
            - POLYMARKET_DATA_API_URL: Override Data API base URL
        """
        wallet = os.environ.get(wallet_env)
        private_key = os.environ.get(private_key_env)
        rpc_url = os.environ.get(rpc_env)
        clob_url = os.environ.get(clob_url_env)
        gamma_url = os.environ.get(gamma_url_env)
        data_api_url = os.environ.get(data_api_url_env)

        if not wallet:
            raise ValueError(f"Environment variable {wallet_env} not set")
        if not private_key:
            raise ValueError(f"Environment variable {private_key_env} not set")

        # Build config with optional URL overrides
        config_kwargs: dict[str, Any] = {
            "wallet_address": wallet,
            "private_key": SecretStr(private_key),
            "rpc_url": rpc_url,
        }
        if clob_url:
            config_kwargs["clob_base_url"] = clob_url
        if gamma_url:
            config_kwargs["gamma_base_url"] = gamma_url
        if data_api_url:
            config_kwargs["data_api_base_url"] = data_api_url

        return cls(**config_kwargs)


# =============================================================================
# EIP-712 Domains and Types
# =============================================================================


# CLOB Authentication Domain
CLOB_AUTH_DOMAIN = {
    "name": "ClobAuthDomain",
    "version": "1",
    "chainId": POLYGON_CHAIN_ID,
}

CLOB_AUTH_TYPES = {
    "ClobAuth": [
        {"name": "address", "type": "address"},
        {"name": "timestamp", "type": "string"},
        {"name": "nonce", "type": "uint256"},
        {"name": "message", "type": "string"},
    ]
}

CLOB_AUTH_MESSAGE = "This message attests that I control the given wallet"

# CTF Exchange Order Domain
CTF_EXCHANGE_DOMAIN = {
    "name": "Polymarket CTF Exchange",
    "version": "1",
    "chainId": POLYGON_CHAIN_ID,
    "verifyingContract": CTF_EXCHANGE,
}

ORDER_TYPES = {
    "Order": [
        {"name": "salt", "type": "uint256"},
        {"name": "maker", "type": "address"},
        {"name": "signer", "type": "address"},
        {"name": "taker", "type": "address"},
        {"name": "tokenId", "type": "uint256"},
        {"name": "makerAmount", "type": "uint256"},
        {"name": "takerAmount", "type": "uint256"},
        {"name": "expiration", "type": "uint256"},
        {"name": "nonce", "type": "uint256"},
        {"name": "feeRateBps", "type": "uint256"},
        {"name": "side", "type": "uint8"},
        {"name": "signatureType", "type": "uint8"},
    ]
}


# =============================================================================
# Market Data Models
# =============================================================================


class GammaMarket(BaseModel):
    """Market data from Gamma Markets API."""

    id: str = Field(description="Internal market ID")
    condition_id: str = Field(description="CTF condition ID (0x...)")
    question: str = Field(description="Market question text")
    slug: str = Field(description="URL slug")
    outcomes: list[str] = Field(description="Outcome names (e.g., ['Yes', 'No'])")
    outcome_prices: list[Decimal] = Field(description="Current prices for each outcome")
    clob_token_ids: list[str] = Field(description="CLOB token IDs for YES and NO")
    volume: Decimal = Field(description="Total volume in USDC")
    volume_24hr: Decimal = Field(default=Decimal("0"), description="24h volume")
    liquidity: Decimal = Field(description="Current liquidity")
    end_date: datetime | None = Field(default=None, description="Resolution deadline")
    active: bool = Field(description="Accepting orders")
    closed: bool = Field(description="Market resolved")
    enable_order_book: bool = Field(description="CLOB enabled")
    order_price_min_tick_size: Decimal = Field(default=Decimal("0.01"), description="Minimum tick size")
    order_min_size: Decimal = Field(default=Decimal("5"), description="Minimum order size")
    best_bid: Decimal | None = Field(default=None, description="Current best bid")
    best_ask: Decimal | None = Field(default=None, description="Current best ask")
    last_trade_price: Decimal | None = Field(default=None, description="Last execution price")
    # Event and category fields for correlation analysis
    event_id: str | None = Field(default=None, description="Parent event ID")
    event_slug: str | None = Field(default=None, description="Parent event slug")
    group_slug: str | None = Field(default=None, description="Market group slug")
    tags: list[str] = Field(default_factory=list, description="Market tags/categories")

    @classmethod
    def from_api_response(cls, data: dict) -> "GammaMarket":
        """Create from Gamma API response."""
        import json

        # Parse JSON string fields
        outcomes = json.loads(data.get("outcomes", '["Yes", "No"]'))
        outcome_prices = [Decimal(p) for p in json.loads(data.get("outcomePrices", '["0.5", "0.5"]'))]
        clob_token_ids = json.loads(data.get("clobTokenIds", "[]"))

        # Parse end date
        end_date = None
        if data.get("endDate"):
            try:
                end_date = datetime.fromisoformat(data["endDate"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass

        # Parse tags - may be a JSON string or list
        tags_data = data.get("tags", [])
        if isinstance(tags_data, str):
            try:
                tags = json.loads(tags_data)
            except (json.JSONDecodeError, ValueError):
                tags = []
        else:
            tags = tags_data if tags_data else []

        return cls(
            id=data["id"],
            condition_id=data.get("conditionId", ""),
            question=data.get("question", ""),
            slug=data.get("slug", ""),
            outcomes=outcomes,
            outcome_prices=outcome_prices,
            clob_token_ids=clob_token_ids,
            volume=Decimal(str(data.get("volume", "0"))),
            volume_24hr=Decimal(str(data.get("volume24hr", "0"))),
            liquidity=Decimal(str(data.get("liquidity", "0"))),
            end_date=end_date,
            active=data.get("active", False),
            closed=data.get("closed", False),
            enable_order_book=data.get("enableOrderBook", False),
            order_price_min_tick_size=Decimal(str(data.get("orderPriceMinTickSize", "0.01"))),
            order_min_size=Decimal(str(data.get("orderMinSize", "5"))),
            best_bid=Decimal(str(data["bestBid"])) if data.get("bestBid") else None,
            best_ask=Decimal(str(data["bestAsk"])) if data.get("bestAsk") else None,
            last_trade_price=(Decimal(str(data["lastTradePrice"])) if data.get("lastTradePrice") else None),
            event_id=data.get("eventId") or data.get("event_id"),
            event_slug=data.get("eventSlug") or data.get("event_slug"),
            group_slug=data.get("groupItemSlug") or data.get("group_slug"),
            tags=tags,
        )

    @property
    def yes_token_id(self) -> str | None:
        """Get YES token ID."""
        return self.clob_token_ids[0] if len(self.clob_token_ids) > 0 else None

    @property
    def no_token_id(self) -> str | None:
        """Get NO token ID."""
        return self.clob_token_ids[1] if len(self.clob_token_ids) > 1 else None

    @property
    def yes_price(self) -> Decimal:
        """Get YES price."""
        return self.outcome_prices[0] if len(self.outcome_prices) > 0 else Decimal("0")

    @property
    def no_price(self) -> Decimal:
        """Get NO price."""
        return self.outcome_prices[1] if len(self.outcome_prices) > 1 else Decimal("0")


class PriceLevel(BaseModel):
    """Single price level in orderbook."""

    price: Decimal = Field(description="Price level")
    size: Decimal = Field(description="Total size at this level")


class OrderBook(BaseModel):
    """Orderbook for a token."""

    market: str = Field(description="Token ID")
    asset_id: str = Field(description="Asset ID (same as token ID)")
    bids: list[PriceLevel] = Field(default_factory=list, description="Bid levels")
    asks: list[PriceLevel] = Field(default_factory=list, description="Ask levels")
    hash: str = Field(default="", description="Orderbook hash")
    timestamp: datetime | None = Field(default=None, description="Timestamp")

    @classmethod
    def from_api_response(cls, data: dict) -> "OrderBook":
        """Create from CLOB API response."""
        bids = [PriceLevel(price=Decimal(b["price"]), size=Decimal(b["size"])) for b in data.get("bids", [])]
        asks = [PriceLevel(price=Decimal(a["price"]), size=Decimal(a["size"])) for a in data.get("asks", [])]

        return cls(
            market=data.get("market", ""),
            asset_id=data.get("asset_id", ""),
            bids=bids,
            asks=asks,
            hash=data.get("hash", ""),
        )

    @property
    def best_bid(self) -> Decimal | None:
        """Get best bid price."""
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        """Get best ask price."""
        return self.asks[0].price if self.asks else None

    @property
    def spread(self) -> Decimal | None:
        """Get bid-ask spread."""
        if self.best_bid is not None and self.best_ask is not None:
            return self.best_ask - self.best_bid
        return None


class TokenPrice(BaseModel):
    """Price information for a token."""

    bid: Decimal = Field(description="Best bid price")
    ask: Decimal = Field(description="Best ask price")
    mid: Decimal = Field(description="Mid price")

    @classmethod
    def from_api_response(cls, data: dict) -> "TokenPrice":
        """Create from CLOB API response."""
        return cls(
            bid=Decimal(str(data.get("bid", "0"))),
            ask=Decimal(str(data.get("ask", "0"))),
            mid=Decimal(str(data.get("mid", "0"))),
        )


# =============================================================================
# Order Models
# =============================================================================


@dataclass
class LimitOrderParams:
    """Parameters for building a limit order."""

    token_id: str
    side: Literal["BUY", "SELL"]
    price: Decimal
    size: Decimal
    expiration: int | None = None
    fee_rate_bps: int = 0


@dataclass
class MarketOrderParams:
    """Parameters for building a market order."""

    token_id: str
    side: Literal["BUY", "SELL"]
    amount: Decimal
    worst_price: Decimal | None = None


@dataclass
class UnsignedOrder:
    """Unsigned order ready for signing."""

    salt: int
    maker: str
    signer: str
    taker: str
    token_id: int
    maker_amount: int
    taker_amount: int
    expiration: int
    nonce: int
    fee_rate_bps: int
    side: int
    signature_type: int

    def to_struct(self) -> dict:
        """Convert to struct for EIP-712 signing."""
        return {
            "salt": self.salt,
            "maker": self.maker,
            "signer": self.signer,
            "taker": self.taker,
            "tokenId": self.token_id,
            "makerAmount": self.maker_amount,
            "takerAmount": self.taker_amount,
            "expiration": self.expiration,
            "nonce": self.nonce,
            "feeRateBps": self.fee_rate_bps,
            "side": self.side,
            "signatureType": self.signature_type,
        }


@dataclass
class SignedOrder:
    """Signed order ready for submission."""

    order: UnsignedOrder
    signature: str

    def to_api_payload(self) -> dict:
        """Convert to API submission payload."""
        return {
            "order": {
                "salt": self.order.salt,
                "maker": self.order.maker,
                "signer": self.order.signer,
                "taker": self.order.taker,
                "tokenId": str(self.order.token_id),
                "makerAmount": str(self.order.maker_amount),
                "takerAmount": str(self.order.taker_amount),
                "expiration": str(self.order.expiration),
                "nonce": str(self.order.nonce),
                "feeRateBps": str(self.order.fee_rate_bps),
                "side": self.order.side,
                "signatureType": self.order.signature_type,
            },
            "signature": self.signature,
        }


class OrderResponse(BaseModel):
    """Response from order submission."""

    order_id: str = Field(description="Order ID")
    status: OrderStatus = Field(description="Order status")
    market: str = Field(description="Token ID")
    side: str = Field(description="BUY or SELL")
    price: Decimal = Field(description="Order price")
    size: Decimal = Field(description="Order size")
    filled_size: Decimal = Field(default=Decimal("0"), description="Filled amount")
    created_at: datetime | None = Field(default=None, description="Creation time")

    @classmethod
    def from_api_response(cls, data: dict) -> "OrderResponse":
        """Create from CLOB API response."""
        created_at = None
        if data.get("createdAt"):
            try:
                created_at = datetime.fromisoformat(data["createdAt"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass

        return cls(
            order_id=data.get("orderID", ""),
            status=OrderStatus(data.get("status", "LIVE")),
            market=data.get("market", ""),
            side=data.get("side", "BUY"),
            price=Decimal(str(data.get("price", "0"))),
            size=Decimal(str(data.get("size", "0"))),
            filled_size=Decimal(str(data.get("filledSize", "0"))),
            created_at=created_at,
        )


class OpenOrder(BaseModel):
    """Open order information."""

    order_id: str = Field(description="Order ID")
    market: str = Field(description="Token ID")
    side: str = Field(description="BUY or SELL")
    price: Decimal = Field(description="Order price")
    size: Decimal = Field(description="Order size")
    filled_size: Decimal = Field(default=Decimal("0"), description="Filled amount")
    created_at: datetime | None = Field(default=None, description="Creation time")
    expiration: int | None = Field(default=None, description="Expiration timestamp")


# =============================================================================
# Position and Trade Models
# =============================================================================


class Position(BaseModel):
    """Position information."""

    market_id: str = Field(description="Market ID")
    condition_id: str = Field(description="CTF condition ID")
    token_id: str = Field(description="Token ID")
    outcome: Literal["YES", "NO"] = Field(description="Outcome type")
    size: Decimal = Field(description="Number of shares")
    avg_price: Decimal = Field(description="Average entry price")
    current_price: Decimal = Field(default=Decimal("0"), description="Current price")
    unrealized_pnl: Decimal = Field(default=Decimal("0"), description="Unrealized PnL")
    realized_pnl: Decimal = Field(default=Decimal("0"), description="Realized PnL")


class Trade(BaseModel):
    """Trade information."""

    id: str = Field(description="Trade ID")
    market_id: str = Field(description="Market ID")
    token_id: str = Field(description="Token ID")
    side: str = Field(description="BUY or SELL")
    price: Decimal = Field(description="Execution price")
    size: Decimal = Field(description="Trade size")
    fee: Decimal = Field(default=Decimal("0"), description="Fee paid")
    timestamp: datetime = Field(description="Trade timestamp")
    status: TradeStatus = Field(description="Trade status")


class BalanceAllowance(BaseModel):
    """Balance and allowance information."""

    balance: Decimal = Field(description="Current balance")
    allowance: Decimal = Field(description="Current allowance")


# =============================================================================
# Filter Models
# =============================================================================


class MarketFilters(BaseModel):
    """Filters for market queries."""

    active: bool | None = None
    closed: bool | None = None
    slug: str | None = None
    condition_ids: list[str] | None = None
    clob_token_ids: list[str] | None = None
    event_id: str | None = None
    event_slug: str | None = None
    tag: str | None = None
    limit: int = Field(default=100, ge=1, le=500)
    offset: int = Field(default=0, ge=0)


class OrderFilters(BaseModel):
    """Filters for order queries."""

    market: str | None = None
    status: OrderStatus | None = None
    limit: int = Field(default=100, ge=1, le=500)


class TradeFilters(BaseModel):
    """Filters for trade queries."""

    market: str | None = None
    after: datetime | None = None
    before: datetime | None = None
    limit: int = Field(default=100, ge=1, le=500)


class PositionFilters(BaseModel):
    """Filters for position queries."""

    market: str | None = None
    outcome: Literal["YES", "NO"] | None = None


# =============================================================================
# Historical Data Models
# =============================================================================


class PriceHistoryInterval(StrEnum):
    """Supported intervals for price history queries."""

    ONE_MINUTE = "1m"
    ONE_HOUR = "1h"
    SIX_HOURS = "6h"
    ONE_DAY = "1d"
    ONE_WEEK = "1w"
    MAX = "max"


@dataclass
class HistoricalPrice:
    """Single historical price point.

    Represents a price at a specific timestamp from the CLOB API.
    Price values represent probability (0.0 to 1.0).
    """

    timestamp: datetime
    price: Decimal

    @classmethod
    def from_api_response(cls, data: dict) -> "HistoricalPrice":
        """Create from API response."""
        from datetime import UTC

        return cls(
            timestamp=datetime.fromtimestamp(data["t"], tz=UTC),
            price=Decimal(str(data["p"])),
        )


@dataclass
class PriceHistory:
    """Historical price data for a token.

    Contains a time series of prices for OHLC-style analysis.
    """

    token_id: str
    interval: str
    prices: list[HistoricalPrice]
    start_time: datetime | None = None
    end_time: datetime | None = None

    @property
    def open_price(self) -> Decimal | None:
        """Get opening price (first price in the series)."""
        return self.prices[0].price if self.prices else None

    @property
    def close_price(self) -> Decimal | None:
        """Get closing price (last price in the series)."""
        return self.prices[-1].price if self.prices else None

    @property
    def high_price(self) -> Decimal | None:
        """Get highest price in the series."""
        return max(p.price for p in self.prices) if self.prices else None

    @property
    def low_price(self) -> Decimal | None:
        """Get lowest price in the series."""
        return min(p.price for p in self.prices) if self.prices else None


@dataclass
class HistoricalTrade:
    """Historical trade from the market.

    Represents a single executed trade from the trade tape.
    """

    id: str
    token_id: str
    side: Literal["BUY", "SELL"]
    price: Decimal
    size: Decimal
    timestamp: datetime
    maker: str | None = None
    taker: str | None = None

    @classmethod
    def from_api_response(cls, data: dict) -> "HistoricalTrade":
        """Create from API response."""
        from datetime import UTC

        # Parse timestamp - handle both ISO format and Unix timestamp
        timestamp = data.get("timestamp") or data.get("createdAt")
        if isinstance(timestamp, int | float):
            ts = datetime.fromtimestamp(timestamp, tz=UTC)
        elif isinstance(timestamp, str):
            ts = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        else:
            ts = datetime.now(UTC)

        return cls(
            id=data.get("id", "") or data.get("tradeId", ""),
            token_id=data.get("tokenId", "") or data.get("market", ""),
            side=data.get("side", "BUY"),
            price=Decimal(str(data.get("price", "0"))),
            size=Decimal(str(data.get("size", "0"))),
            timestamp=ts,
            maker=data.get("maker"),
            taker=data.get("taker"),
        )


__all__ = [
    # Constants
    "CLOB_BASE_URL",
    "GAMMA_BASE_URL",
    "DATA_API_BASE_URL",
    "CTF_EXCHANGE",
    "NEG_RISK_EXCHANGE",
    "CONDITIONAL_TOKENS",
    "NEG_RISK_ADAPTER",
    "USDC_POLYGON",
    "POLYGON_CHAIN_ID",
    # EIP-712
    "CLOB_AUTH_DOMAIN",
    "CLOB_AUTH_TYPES",
    "CLOB_AUTH_MESSAGE",
    "CTF_EXCHANGE_DOMAIN",
    "ORDER_TYPES",
    # Enums
    "SignatureType",
    "OrderSide",
    "OrderType",
    "OrderStatus",
    "TradeStatus",
    # Credentials & Config
    "ApiCredentials",
    "PolymarketConfig",
    # Market Data
    "GammaMarket",
    "PriceLevel",
    "OrderBook",
    "TokenPrice",
    # Orders
    "LimitOrderParams",
    "MarketOrderParams",
    "UnsignedOrder",
    "SignedOrder",
    "OrderResponse",
    "OpenOrder",
    # Positions & Trades
    "Position",
    "Trade",
    "BalanceAllowance",
    # Filters
    "MarketFilters",
    "OrderFilters",
    "TradeFilters",
    "PositionFilters",
    # Historical Data
    "PriceHistoryInterval",
    "HistoricalPrice",
    "PriceHistory",
    "HistoricalTrade",
]
