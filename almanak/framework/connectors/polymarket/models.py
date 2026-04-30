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

    General guideline: use Enum for fixed constants, Pydantic BaseModel for external API
    boundaries requiring validation, and @dataclass for trusted internal data structures.
"""

import logging
import os
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum, StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator

logger = logging.getLogger(__name__)

# =============================================================================
# Constants
# =============================================================================

# CLOB API base URLs.
#
# V2 is served at the canonical ``clob.polymarket.com`` host. The pre-cutover
# preview host ``clob-v2.polymarket.com`` now 301-redirects here, but we point
# directly at the canonical host because httpx (used by ``ClobClient``) does
# not follow redirects by default — pointing at ``clob-v2`` would surface as
# 301 errors on every request. Override via ``POLYMARKET_CLOB_URL`` env var
# or ``PolymarketConfig.clob_base_url``.
CLOB_BASE_URL = "https://clob.polymarket.com"
GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
DATA_API_BASE_URL = "https://data-api.polymarket.com"

# Contract addresses (Polygon Mainnet) — V2 (April 2026 cutover)
CTF_EXCHANGE_V2 = "0xE111180000d2663C0091e4f400237545B87B996B"
NEG_RISK_EXCHANGE_V2 = "0xe2222d279d744050d28e00520010520000310F59"
NEG_RISK_ADAPTER = "0xd91E80cF2E7be2e162c6513ceD06f1dD0dA35296"
CONDITIONAL_TOKENS = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

# V2 collateral pivot — pUSD via Onramp/Offramp wrappers
PUSD = "0xC011a7E12a19f7B1f670d46F03B03f3342E82DFB"
COLLATERAL_ONRAMP = "0x93070a847efEf7F70739046A929D47a521F5B8ee"
COLLATERAL_OFFRAMP = "0x2957922Eb93258b93368531d39fAcCA3B4dC5854"

# Source assets (ramp inputs). Both can be wrapped to pUSD via the Onramp.
USDCE_POLYGON = "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"  # bridged USDC.e (transitional)
USDC_NATIVE_POLYGON = "0x3c499c542cEF5E3811e1192ce70d8cC03d5c3359"  # native Circle USDC (future)

# Chain ID
POLYGON_CHAIN_ID = 137

# Default value for V2 bytes32 fields (metadata, builder) when no attribution is set.
BYTES32_ZERO = "0x" + "00" * 32


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
    GTD = "GTD"  # Good Till Date — V2 GTD; matcher refuses after `expiration` ts
    IOC = "IOC"  # Immediate or Cancel
    FOK = "FOK"  # Fill or Kill


class OrderStatus(StrEnum):
    """Order status values.

    VIB-3218: Polymarket's POST /order response can return ``delayed`` (matching
    engine still processing) and ``unmatched`` (IOC / FOK failed to match any
    liquidity) in addition to the historical four. ``FAILED`` / ``REJECTED``
    cover adapter-side and API-side rejections. Missing these from the enum
    caused ``OrderResponse.from_api_response`` to silently coerce them to
    ``LIVE``, turning a rejected order into a "healthy resting order" and
    defeating downstream failure detection.
    """

    LIVE = "LIVE"
    MATCHED = "MATCHED"
    CANCELLED = "CANCELLED"
    EXPIRED = "EXPIRED"
    DELAYED = "DELAYED"
    UNMATCHED = "UNMATCHED"
    FAILED = "FAILED"
    REJECTED = "REJECTED"


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
    wallet_address: str = Field(description="User wallet address (or trading EOA in remote-signing mode)")
    # Local-signing path. Optional when the remote signer service is configured —
    # the platform mode keeps the trading EOA's private key in the Almanak Signer
    # Service GCS bucket and never ships it to the gateway.
    private_key: SecretStr | None = Field(
        default=None, description="Private key for local signing (None when using remote signer)"
    )
    signature_type: SignatureType = Field(default=SignatureType.EOA, description="Signature type for authentication")
    funder_address: str | None = Field(default=None, description="Funder address for proxy wallets")
    # Remote-signing path (Almanak platform). When set, signing is delegated to the
    # Almanak Signer Service via POST {signer_service_url}/sign/hash with a JWT
    # bearer token. wallet_address must be the EOA whose key the service holds
    # (the trading EOA from the platform's polymarket_zodiac wallet entry).
    signer_service_url: str | None = Field(
        default=None,
        description="Almanak Signer Service base URL. When set, signing is delegated to /sign/hash.",
    )
    signer_service_jwt: SecretStr | None = Field(
        default=None,
        description="JWT for the Almanak Signer Service. Required when signer_service_url is set.",
    )
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

    # V2 builder attribution. Default zero bytes (no attribution). Register
    # with Polymarket to receive a builder code for fee-share / branding.
    builder_code: str = Field(
        default=BYTES32_ZERO,
        description="V2 builder attribution code (bytes32 hex). Defaults to zero (no attribution).",
    )

    @field_validator("wallet_address", mode="before")
    @classmethod
    def checksum_wallet(cls, v: str) -> str:
        """Ensure wallet address is checksummed."""
        from web3 import Web3

        return Web3.to_checksum_address(v)

    @model_validator(mode="after")
    def _require_signing_capability(self) -> "PolymarketConfig":
        """Either a local private key OR a complete remote-signer config must be present.

        Remote-signer mode requires both ``signer_service_url`` and
        ``signer_service_jwt``. ``wallet_address`` is the trading EOA in remote
        mode (the address whose key the Signer Service holds).
        """
        has_local_key = self.private_key is not None
        has_remote_signer = bool(self.signer_service_url) and bool(self.signer_service_jwt)
        if not has_local_key and not has_remote_signer:
            raise ValueError(
                "PolymarketConfig requires either private_key (local signing) or "
                "signer_service_url + signer_service_jwt (remote signing via Almanak Signer Service)"
            )
        if self.signer_service_url and not self.signer_service_jwt:
            raise ValueError("PolymarketConfig.signer_service_jwt is required when signer_service_url is set")
        if self.signer_service_jwt and not self.signer_service_url:
            raise ValueError("PolymarketConfig.signer_service_url is required when signer_service_jwt is set")
        return self

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

# CTF Exchange Order Domain — V2 (verifyingContract is per-order; build via helper).
CTF_EXCHANGE_V2_DOMAIN_NAME = "Polymarket CTF Exchange"
CTF_EXCHANGE_V2_DOMAIN_VERSION = "2"


def build_ctf_exchange_domain(exchange_address: str) -> dict:
    """Build a per-order EIP-712 domain.

    V2 orders may target either the regular CTF Exchange or the NegRisk
    CTF Exchange — the verifyingContract differs per market, so we build
    the domain dict per call rather than holding it as a module constant.
    """
    return {
        "name": CTF_EXCHANGE_V2_DOMAIN_NAME,
        "version": CTF_EXCHANGE_V2_DOMAIN_VERSION,
        "chainId": POLYGON_CHAIN_ID,
        "verifyingContract": exchange_address,
    }


ORDER_TYPES = {
    "Order": [
        {"name": "salt", "type": "uint256"},
        {"name": "maker", "type": "address"},
        {"name": "signer", "type": "address"},
        {"name": "tokenId", "type": "uint256"},
        {"name": "makerAmount", "type": "uint256"},
        {"name": "takerAmount", "type": "uint256"},
        {"name": "side", "type": "uint8"},
        {"name": "signatureType", "type": "uint8"},
        {"name": "timestamp", "type": "uint256"},
        {"name": "metadata", "type": "bytes32"},
        {"name": "builder", "type": "bytes32"},
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
    maker_base_fee_bps: int = Field(
        default=0,
        description=(
            "Market's maker fee in basis points. V2: informational only — fees are "
            "operator-set at match time and not signed into the order. Surfaced for "
            "UX / accounting (use ``getClobMarketInfo()`` for the live operator-side "
            "fee parameters)."
        ),
    )
    taker_base_fee_bps: int = Field(
        default=0,
        description="Market's taker fee in basis points (informational; V2 fees are dynamic).",
    )
    best_bid: Decimal | None = Field(default=None, description="Current best bid")
    best_ask: Decimal | None = Field(default=None, description="Current best ask")
    last_trade_price: Decimal | None = Field(default=None, description="Last execution price")
    # Event and category fields for correlation analysis
    event_id: str | None = Field(default=None, description="Parent event ID")
    event_slug: str | None = Field(default=None, description="Parent event slug")
    group_slug: str | None = Field(default=None, description="Market group slug")
    tags: list[str] = Field(default_factory=list, description="Market tags/categories")
    # V2 routing — neg-risk markets sign against NEG_RISK_EXCHANGE_V2; binary
    # YES/NO markets sign against CTF_EXCHANGE_V2. Driven by the API response.
    neg_risk: bool = Field(default=False, description="True if market trades on the NegRisk CTF Exchange V2")

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
            maker_base_fee_bps=int(data.get("makerBaseFee") or 0),
            taker_base_fee_bps=int(data.get("takerBaseFee") or 0),
            best_bid=Decimal(str(data["bestBid"])) if data.get("bestBid") else None,
            best_ask=Decimal(str(data["bestAsk"])) if data.get("bestAsk") else None,
            last_trade_price=(Decimal(str(data["lastTradePrice"])) if data.get("lastTradePrice") else None),
            event_id=data.get("eventId") or data.get("event_id"),
            event_slug=data.get("eventSlug") or data.get("event_slug"),
            group_slug=data.get("groupItemSlug") or data.get("group_slug"),
            tags=tags,
            neg_risk=bool(data.get("negRisk") or data.get("neg_risk") or False),
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
        """Best bid (highest price someone is willing to BUY at).

        Polymarket's CLOB returns the orderbook in depth-walk order — bids
        ascend from worst to best, so the best bid sits at the END of the
        list, not the start. Cross-checked against ``GET /price?side=BUY``
        which returns the same value as ``bids[-1].price`` on a live market.
        """
        return self.bids[-1].price if self.bids else None

    @property
    def best_ask(self) -> Decimal | None:
        """Best ask (lowest price someone is willing to SELL at).

        Polymarket's CLOB returns the orderbook in depth-walk order — asks
        descend from worst to best, so the best ask sits at the END of the
        list. Cross-checked against ``GET /price?side=SELL``.
        """
        return self.asks[-1].price if self.asks else None

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


@dataclass
class SimplifiedMarket:
    """Lightweight market summary from CLOB ``/simplified-markets``.

    Mirrors the proto ``PolymarketSimplifiedMarket`` shape. Used by the
    paginated discovery endpoint that returns only the fields needed to
    sweep open markets cheaply (no question text, prices, or fees) — the
    full ``GammaMarket`` is fetched separately when a caller drills in.
    """

    condition_id: str
    tokens: list[str]
    min_incentive_size: Decimal
    max_incentive_spread: Decimal
    active: bool
    closed: bool


# =============================================================================
# Order Models
# =============================================================================


@dataclass
class LimitOrderParams:
    """Parameters for building a V2 limit order.

    V2 changes vs V1:
    - Drop on-chain ``expiration`` (V2 orders carry no on-chain expiration;
      GTD time is API-level and lives on the order envelope, not the signed struct).
    - Drop ``fee_rate_bps`` (operator-set in V2; never signed by the maker).
    """

    token_id: str
    side: Literal["BUY", "SELL"]
    price: Decimal
    size: Decimal
    # API-level GTD timestamp (Unix seconds). 0 = no expiration. Routed to the
    # `expiration` field of the V2 order envelope (NOT the signed struct).
    expiration: int = 0


@dataclass
class MarketOrderParams:
    """Parameters for building a market order."""

    token_id: str
    side: Literal["BUY", "SELL"]
    amount: Decimal
    worst_price: Decimal | None = None


@dataclass
class UnsignedOrder:
    """Unsigned V2 order ready for EIP-712 signing.

    Field set matches the V2 ``Order`` struct (11 fields signed). The
    ``exchange_address`` is the V2 verifyingContract for this order — it
    differs between regular CTF Exchange V2 and NegRisk CTF Exchange V2,
    routed by ``GammaMarket.neg_risk``.

    ``api_expiration`` is the GTD timestamp included in the wire envelope but
    NOT in the signed struct — V2's matcher enforces freshness off-chain.
    """

    salt: int
    maker: str
    signer: str
    token_id: int
    maker_amount: int
    taker_amount: int
    side: int
    signature_type: int
    timestamp: int
    metadata: str
    builder: str
    # Routing — verifyingContract for the EIP-712 domain.
    exchange_address: str
    # Wire-only (NOT signed). API-level GTD; 0 = no expiration.
    api_expiration: int = 0

    def to_struct(self) -> dict:
        """Convert to struct for EIP-712 signing.

        Returns the 11-field V2 Order struct. ``exchange_address`` and
        ``api_expiration`` are intentionally excluded — they belong on the
        domain / wire envelope, not the signed payload.
        """
        return {
            "salt": self.salt,
            "maker": self.maker,
            "signer": self.signer,
            "tokenId": self.token_id,
            "makerAmount": self.maker_amount,
            "takerAmount": self.taker_amount,
            "side": self.side,
            "signatureType": self.signature_type,
            "timestamp": self.timestamp,
            "metadata": self.metadata,
            "builder": self.builder,
        }


@dataclass
class SignedOrder:
    """Signed V2 order ready for submission to /order."""

    order: UnsignedOrder
    signature: str

    def to_api_payload(self, owner: str, order_type: str = "GTC") -> dict:
        """Convert to Polymarket V2 CLOB `/order` submission payload.

        Args:
            owner: Polymarket API key (UUID) that owns the credential used
                to authenticate the request. Required by the API matcher.
            order_type: One of "GTC", "GTD", "FOK", "FAK".

        Wire shape matches py-clob-client-v2's ``order_to_json_v2``: signature
        is inside the ``order`` object with `0x` prefix, ``side`` is a string
        ("BUY"/"SELL"), and ``timestamp`` / ``metadata`` / ``builder`` are
        included alongside the API-level ``expiration``.
        """
        signature = self.signature if self.signature.startswith("0x") else f"0x{self.signature}"
        side_str = "BUY" if self.order.side == OrderSide.BUY.value else "SELL"
        return {
            "order": {
                "salt": self.order.salt,
                "maker": self.order.maker,
                "signer": self.order.signer,
                "tokenId": str(self.order.token_id),
                "makerAmount": str(self.order.maker_amount),
                "takerAmount": str(self.order.taker_amount),
                "side": side_str,
                "expiration": str(self.order.api_expiration),
                "signatureType": self.order.signature_type,
                "timestamp": str(self.order.timestamp),
                "metadata": self.order.metadata,
                "builder": self.order.builder,
                "signature": signature,
            },
            "owner": owner,
            "orderType": order_type,
        }


@dataclass(frozen=True)
class SetupTxInfo:
    """Per-tx record of a Polymarket V2 on-chain setup transaction (VIB-3710).

    The gateway's ``_ensure_wallet_ready`` may submit up to 5 ERC-20 approvals
    plus 1 source-asset → pUSD wrap on the first BUY for a wallet. These cost
    real MATIC and structurally attribute to the position whose first BUY
    triggered them. The strategy-side ``OrderResponse`` carries one
    ``SetupTxInfo`` per setup tx so the downstream prediction handler can
    fold the gas spend into the position's loaded cost basis.

    Attributes:
        tx_hash: 0x-prefixed Polygon tx hash.
        description: Human-readable label produced by the CTF SDK
            (e.g. ``"Approve pUSD → CTF V2 exchange"``).
        gas_used: Receipt ``gasUsed`` (units of gas consumed).
        gas_price_wei: Effective gas price in wei (post-London EIP-1559 takes
            ``effectiveGasPrice``; pre-London / Anvil falls back to the
            tx-level ``gasPrice`` / ``maxFeePerGas``). Decimal-encoded as a
            string to preserve precision through proto / JSON.
        total_cost_wei: ``gas_used * gas_price_wei`` — the wallet's MATIC
            outflow on this tx, in wei. Decimal-encoded as a string.
    """

    tx_hash: str
    description: str
    gas_used: int
    gas_price_wei: str
    total_cost_wei: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "tx_hash": self.tx_hash,
            "description": self.description,
            "gas_used": self.gas_used,
            "gas_price_wei": self.gas_price_wei,
            "total_cost_wei": self.total_cost_wei,
        }


class OrderResponse(BaseModel):
    """Response from order submission.

    The POST /order API returns ``status``, ``filledSize`` (immediate fills at
    submission time) and, when available, ``avgPrice`` (volume-weighted fill
    price). These fields are the basis for distinguishing "order accepted"
    from "order filled" in downstream execution (VIB-3218).

    VIB-3710: also carries (a) ``setup_txs`` — the on-chain approval / wrap
    transactions the gateway submitted before this order, populated only when
    ``_ensure_wallet_ready`` actually had work to do; and (b) ``fee_pusd`` —
    the operator-set fee charged at match time, which is NOT part of the
    signed order payload and so cannot be derived strategy-side without the
    gateway surfacing it explicitly.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    order_id: str = Field(description="Order ID")
    status: OrderStatus = Field(description="Order status")
    market: str = Field(description="Token ID")
    side: str = Field(description="BUY or SELL")
    price: Decimal = Field(description="Order price")
    size: Decimal = Field(description="Order size")
    filled_size: Decimal = Field(default=Decimal("0"), description="Filled amount")
    avg_fill_price: Decimal | None = Field(
        default=None,
        description=(
            "Volume-weighted average fill price if any portion of the order "
            "filled at submission time. None if there were no immediate fills."
        ),
    )
    created_at: datetime | None = Field(default=None, description="Creation time")
    setup_txs: list[SetupTxInfo] = Field(
        default_factory=list,
        description=(
            "On-chain setup transactions (approvals + source-asset → pUSD wrap) "
            "submitted by the gateway before this order. Empty when allowances "
            "were already in place AND no wrap was needed. Populated exactly "
            "once per order (VIB-3710)."
        ),
    )
    fee_pusd: Decimal | None = Field(
        default=None,
        description=(
            "pUSD operator fee charged at match time, in human units (6 dp). "
            "None when the order did not match (no fee charged) or when the "
            "CLOB response did not carry a fee field. Distinct from any "
            "signed-order fee — V2 fees are operator-set, not part of the "
            "EIP-712 payload (VIB-3710)."
        ),
    )

    @classmethod
    def from_api_response(cls, data: dict) -> "OrderResponse":
        """Create from CLOB API response.

        VIB-3218: Polymarket POST /order returns statuses the original enum
        did not cover (``delayed`` / ``unmatched``); the ``OrderStatus`` enum
        has been extended to include them. A truly-unknown status (new API
        value, typo) falls back to ``FAILED`` with a warning -- the safest
        default for a money-critical path is to force caller attention, not
        to silently pretend the order is resting on the book.
        """
        created_at = None
        if data.get("createdAt"):
            try:
                created_at = datetime.fromisoformat(data["createdAt"].replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                pass

        # CLOB returns lowercase status strings ("live", "matched", "delayed",
        # "unmatched", …); our enum is uppercase. Normalize.
        raw_status = str(data.get("status", "LIVE")).upper()
        try:
            status = OrderStatus(raw_status)
        except ValueError:
            logger.warning(
                "Unknown Polymarket order status %r; treating as FAILED",
                raw_status,
            )
            status = OrderStatus.FAILED

        avg_fill_price_raw = data.get("avgPrice") or data.get("avg_price")
        avg_fill_price: Decimal | None = None
        if avg_fill_price_raw is not None:
            try:
                value = Decimal(str(avg_fill_price_raw))
                if value > 0:
                    avg_fill_price = value
            except (ValueError, ArithmeticError):
                avg_fill_price = None

        # VIB-3710: optional setup_txs / fee_pusd carried by the gateway-routed
        # response shape (these never appear in raw CLOB JSON — the gateway
        # synthesises them server-side and re-shapes the dict before this
        # parser runs). Tolerant of missing fields so direct CLOB callers
        # still work unchanged.
        setup_txs_raw = data.get("setup_txs") or []
        setup_txs: list[SetupTxInfo] = []
        for entry in setup_txs_raw:
            try:
                setup_txs.append(
                    SetupTxInfo(
                        tx_hash=str(entry.get("tx_hash", "")),
                        description=str(entry.get("description", "")),
                        gas_used=int(entry.get("gas_used", 0) or 0),
                        gas_price_wei=str(entry.get("gas_price_wei", "0")),
                        total_cost_wei=str(entry.get("total_cost_wei", "0")),
                    )
                )
            except (TypeError, ValueError, AttributeError):
                # Malformed entry — drop it rather than crash the whole order
                # response; under-attribution beats losing the response.
                continue

        fee_pusd_raw = data.get("fee_pusd")
        if fee_pusd_raw in (None, ""):
            fee_pusd_raw = data.get("fee")  # raw CLOB JSON fallback for direct callers
        fee_pusd: Decimal | None = None
        if fee_pusd_raw not in (None, ""):
            try:
                value = Decimal(str(fee_pusd_raw))
                if value >= 0:
                    fee_pusd = value
            except (ValueError, ArithmeticError):
                fee_pusd = None

        return cls(
            # Gamma responses alternate between `orderID` and `orderId`.
            order_id=data.get("orderID") or data.get("orderId", ""),
            status=status,
            market=data.get("market", ""),
            side=data.get("side", "BUY"),
            price=Decimal(str(data.get("price", "0"))),
            size=Decimal(str(data.get("size", "0"))),
            filled_size=Decimal(str(data.get("filledSize", "0"))),
            avg_fill_price=avg_fill_price,
            created_at=created_at,
            setup_txs=setup_txs,
            fee_pusd=fee_pusd,
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
    market_question: str = Field(default="", description="Market question text (for UX/reconciliation)")


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
    # Constants — V2 contract addresses
    "CLOB_BASE_URL",
    "GAMMA_BASE_URL",
    "DATA_API_BASE_URL",
    "CTF_EXCHANGE_V2",
    "NEG_RISK_EXCHANGE_V2",
    "CONDITIONAL_TOKENS",
    "NEG_RISK_ADAPTER",
    "PUSD",
    "COLLATERAL_ONRAMP",
    "COLLATERAL_OFFRAMP",
    "USDCE_POLYGON",
    "USDC_NATIVE_POLYGON",
    "POLYGON_CHAIN_ID",
    "BYTES32_ZERO",
    # EIP-712
    "CLOB_AUTH_DOMAIN",
    "CLOB_AUTH_TYPES",
    "CLOB_AUTH_MESSAGE",
    "CTF_EXCHANGE_V2_DOMAIN_NAME",
    "CTF_EXCHANGE_V2_DOMAIN_VERSION",
    "build_ctf_exchange_domain",
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
    "SimplifiedMarket",
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
