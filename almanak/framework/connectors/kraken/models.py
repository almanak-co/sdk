"""Kraken configuration and data models.

This module provides Pydantic models for:
- Kraken API credentials
- Connector configuration
- Market information
- CEX-specific idempotency keys
"""

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field, SecretStr, field_validator

# =============================================================================
# Enums
# =============================================================================


class KrakenOrderStatus(StrEnum):
    """Kraken order status values."""

    PENDING = "pending"
    OPEN = "open"
    CLOSED = "closed"
    CANCELED = "canceled"
    EXPIRED = "expired"


class KrakenWithdrawStatus(StrEnum):
    """Kraken withdrawal status values."""

    INITIAL = "Initial"
    PENDING = "Pending"
    SETTLED = "Settled"
    SUCCESS = "Success"
    FAILURE = "Failure"


class KrakenDepositStatus(StrEnum):
    """Kraken deposit status values."""

    PENDING = "Pending"
    SETTLED = "Settled"
    SUCCESS = "Success"
    FAILURE = "Failure"


class CEXOperationType(StrEnum):
    """Type of CEX operation."""

    SWAP = "swap"
    WITHDRAW = "withdraw"
    DEPOSIT = "deposit"


# =============================================================================
# Credentials
# =============================================================================


class KrakenCredentials(BaseModel):
    """Kraken API credentials.

    Security notes:
    - Credentials are stored using SecretStr to prevent accidental logging
    - Use from_env() to load from environment variables
    - Never commit credentials to version control
    """

    api_key: SecretStr = Field(description="Kraken API key")
    api_secret: SecretStr = Field(description="Kraken API secret")

    @classmethod
    def from_env(
        cls,
        key_env: str = "KRAKEN_API_KEY",
        secret_env: str = "KRAKEN_API_SECRET",
    ) -> "KrakenCredentials":
        """Load credentials from environment variables.

        Args:
            key_env: Environment variable name for API key
            secret_env: Environment variable name for API secret

        Returns:
            KrakenCredentials instance

        Raises:
            ValueError: If environment variables are not set
        """
        api_key = os.environ.get(key_env)
        api_secret = os.environ.get(secret_env)

        if not api_key:
            raise ValueError(f"Environment variable {key_env} not set")
        if not api_secret:
            raise ValueError(f"Environment variable {secret_env} not set")

        return cls(api_key=SecretStr(api_key), api_secret=SecretStr(api_secret))

    def model_post_init(self, __context: Any) -> None:
        """Validate that credentials are not empty."""
        if not self.api_key.get_secret_value():
            raise ValueError("API key cannot be empty")
        if not self.api_secret.get_secret_value():
            raise ValueError("API secret cannot be empty")


# =============================================================================
# Configuration
# =============================================================================


class KrakenConfig(BaseModel):
    """Configuration for Kraken connector.

    Example:
        config = KrakenConfig(
            credentials=KrakenCredentials.from_env(),
            default_slippage_bps=50,
        )
    """

    credentials: KrakenCredentials | None = Field(
        default=None,
        description="API credentials. If not provided, will be loaded from env.",
    )

    # Trading settings
    default_slippage_bps: int = Field(
        default=50,
        ge=0,
        le=1000,
        description="Default slippage tolerance in basis points (0.5% = 50 bps)",
    )

    # Timeout settings
    order_timeout_seconds: int = Field(
        default=300,
        ge=30,
        description="Maximum time to wait for order completion",
    )
    withdrawal_timeout_seconds: int = Field(
        default=3600,
        ge=300,
        description="Maximum time to wait for withdrawal completion",
    )
    deposit_timeout_seconds: int = Field(
        default=3600,
        ge=300,
        description="Maximum time to wait for deposit confirmation",
    )

    # Polling settings
    poll_interval_seconds: float = Field(
        default=2.0,
        ge=0.5,
        description="Initial polling interval",
    )
    poll_backoff_factor: float = Field(
        default=1.5,
        ge=1.0,
        le=3.0,
        description="Exponential backoff factor for polling",
    )
    poll_max_interval_seconds: float = Field(
        default=30.0,
        ge=5.0,
        description="Maximum polling interval",
    )

    # Safety settings
    require_withdrawal_whitelist: bool = Field(
        default=True,
        description="Require withdrawal address to be whitelisted on Kraken",
    )

    def get_credentials(self) -> KrakenCredentials:
        """Get credentials, loading from env if not configured."""
        if self.credentials is not None:
            return self.credentials
        return KrakenCredentials.from_env()


class CEXRiskConfig(BaseModel):
    """CEX-specific risk parameters.

    Used by RiskGuard to validate CEX operations.
    """

    max_order_size_usd: Decimal = Field(
        default=Decimal("50000"),
        ge=0,
        description="Maximum single order size in USD",
    )
    max_daily_withdrawal_usd: Decimal = Field(
        default=Decimal("100000"),
        ge=0,
        description="Maximum daily withdrawal amount in USD",
    )
    max_outstanding_orders: int = Field(
        default=5,
        ge=1,
        description="Maximum number of pending orders",
    )
    allowed_withdrawal_chains: list[str] = Field(
        default_factory=lambda: ["arbitrum", "optimism", "ethereum"],
        description="Chains allowed for withdrawal",
    )
    require_withdrawal_whitelist: bool = Field(
        default=True,
        description="Require address to be whitelisted before withdrawal",
    )

    @field_validator("allowed_withdrawal_chains", mode="before")
    @classmethod
    def lowercase_chains(cls, v: list[str]) -> list[str]:
        """Normalize chain names to lowercase."""
        return [c.lower() for c in v]


# =============================================================================
# Market Info
# =============================================================================


class KrakenMarketInfo(BaseModel):
    """Information about a Kraken trading pair.

    Contains precision, minimum sizes, and fee information
    needed for order validation.
    """

    pair: str = Field(description="Trading pair symbol (e.g., 'ETHUSD')")
    base_asset: str = Field(description="Base asset symbol")
    quote_asset: str = Field(description="Quote asset symbol")

    # Precision
    pair_decimals: int = Field(description="Price decimal precision")
    lot_decimals: int = Field(description="Volume decimal precision")

    # Minimums
    ordermin: Decimal = Field(description="Minimum order size in base asset")
    costmin: Decimal = Field(description="Minimum order cost in quote asset")

    # Fees (percentage)
    taker_fee: Decimal = Field(description="Taker fee percentage")
    maker_fee: Decimal = Field(description="Maker fee percentage")

    @classmethod
    def from_kraken_response(cls, pair: str, data: dict) -> "KrakenMarketInfo":
        """Create from Kraken API response.

        Args:
            pair: Trading pair name
            data: Response from get_asset_pairs API

        Returns:
            KrakenMarketInfo instance
        """
        # Extract fees (first tier)
        fees = data.get("fees", [[0, 0.26]])
        fees_maker = data.get("fees_maker", [[0, 0.16]])

        return cls(
            pair=pair,
            base_asset=data.get("base", ""),
            quote_asset=data.get("quote", ""),
            pair_decimals=data.get("pair_decimals", 5),
            lot_decimals=data.get("lot_decimals", 8),
            ordermin=Decimal(str(data.get("ordermin", "0.0001"))),
            costmin=Decimal(str(data.get("costmin", "0.5"))),
            taker_fee=Decimal(str(fees[0][1])),
            maker_fee=Decimal(str(fees_maker[0][1])),
        )

    def get_min_order_base(self, decimals: int) -> int:
        """Get minimum order size in base asset wei units.

        Args:
            decimals: Token decimals

        Returns:
            Minimum order in wei
        """
        return int(self.ordermin * Decimal(10) ** decimals)

    def get_min_cost_quote(self, decimals: int) -> int:
        """Get minimum order cost in quote asset wei units.

        Args:
            decimals: Token decimals

        Returns:
            Minimum cost in wei
        """
        return int(self.costmin * Decimal(10) ** decimals)


# =============================================================================
# Idempotency Keys
# =============================================================================


@dataclass
class CEXIdempotencyKey:
    """Tracks CEX operation for idempotency and crash recovery.

    This is persisted in ExecutionSession to enable:
    - Resuming pending operations after restart
    - Avoiding duplicate orders with the same userref
    - Tracking withdrawal refids for status polling

    Attributes:
        action_id: Unique identifier for the action in the bundle
        exchange: Exchange name (e.g., "kraken")
        operation_type: Type of operation (swap, withdraw, deposit)
        userref: Client order ID for swaps (int32, set at compile time)
        refid: Kraken withdrawal reference (set after API call)
        order_id: Kraken order/transaction ID (txid)
        status: Current status of the operation
        created_at: When the operation was initiated
        last_poll: Last time status was polled
    """

    action_id: str
    exchange: str
    operation_type: CEXOperationType
    userref: int | None = None
    refid: str | None = None
    order_id: str | None = None
    status: str = "pending"
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_poll: datetime | None = None

    def to_dict(self) -> dict:
        """Serialize for state persistence."""
        return {
            "action_id": self.action_id,
            "exchange": self.exchange,
            "operation_type": self.operation_type.value,
            "userref": self.userref,
            "refid": self.refid,
            "order_id": self.order_id,
            "status": self.status,
            "created_at": self.created_at.isoformat(),
            "last_poll": self.last_poll.isoformat() if self.last_poll else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "CEXIdempotencyKey":
        """Deserialize from state."""
        return cls(
            action_id=data["action_id"],
            exchange=data["exchange"],
            operation_type=CEXOperationType(data["operation_type"]),
            userref=data.get("userref"),
            refid=data.get("refid"),
            order_id=data.get("order_id"),
            status=data.get("status", "pending"),
            created_at=datetime.fromisoformat(data["created_at"]),
            last_poll=(datetime.fromisoformat(data["last_poll"]) if data.get("last_poll") else None),
        )


# =============================================================================
# Balance Info
# =============================================================================


class KrakenBalance(BaseModel):
    """Balance information for an asset on Kraken."""

    asset: str = Field(description="Asset symbol (Kraken format)")
    total: Decimal = Field(description="Total balance")
    available: Decimal = Field(description="Available balance (not held)")
    held: Decimal = Field(default=Decimal("0"), description="Balance held in orders")

    @classmethod
    def from_kraken_response(cls, asset: str, data: dict) -> "KrakenBalance":
        """Create from Kraken balance response."""
        total = Decimal(str(data.get("balance", "0")))
        held = Decimal(str(data.get("hold_trade", "0")))
        return cls(
            asset=asset,
            total=total,
            available=total - held,
            held=held,
        )


__all__ = [
    # Enums
    "KrakenOrderStatus",
    "KrakenWithdrawStatus",
    "KrakenDepositStatus",
    "CEXOperationType",
    # Credentials & Config
    "KrakenCredentials",
    "KrakenConfig",
    "CEXRiskConfig",
    # Market Info
    "KrakenMarketInfo",
    # Idempotency
    "CEXIdempotencyKey",
    # Balance
    "KrakenBalance",
]
