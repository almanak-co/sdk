"""Drift Protocol Data Models.

Dataclasses for Drift configuration, market data, order parameters,
and on-chain account state.
"""

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from .constants import (
    DIRECTION_LONG,
    MARKET_TYPE_PERP,
    ORDER_TYPE_MARKET,
    POST_ONLY_NONE,
    TRIGGER_CONDITION_ABOVE,
)
from .exceptions import DriftConfigError


@dataclass
class DriftConfig:
    """Configuration for Drift adapter.

    Attributes:
        wallet_address: Solana public key (Base58)
        rpc_url: Solana RPC endpoint URL
        sub_account_id: Drift sub-account ID (default 0)
        data_api_base_url: Drift Data API base URL
        timeout: Request timeout in seconds
    """

    wallet_address: str
    rpc_url: str = ""
    sub_account_id: int = 0
    data_api_base_url: str = "https://data.api.drift.trade"
    timeout: int = 30

    def __post_init__(self) -> None:
        if not self.wallet_address:
            raise DriftConfigError(
                "wallet_address is required",
                parameter="wallet_address",
            )


@dataclass
class DriftMarket:
    """A Drift perpetual futures market.

    Attributes:
        market_index: On-chain market index
        symbol: Market symbol (e.g., "SOL-PERP")
        base_asset_symbol: Base asset symbol (e.g., "SOL")
        oracle_price: Current oracle price in USD
        funding_rate: Current funding rate (hourly)
        funding_rate_24h: 24-hour average funding rate
        open_interest: Total open interest in base units
        volume_24h: 24-hour volume in USD
        mark_price: Current mark price
    """

    market_index: int
    symbol: str = ""
    base_asset_symbol: str = ""
    oracle_price: Decimal = Decimal("0")
    funding_rate: Decimal = Decimal("0")
    funding_rate_24h: Decimal = Decimal("0")
    open_interest: Decimal = Decimal("0")
    volume_24h: Decimal = Decimal("0")
    mark_price: Decimal = Decimal("0")

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "DriftMarket":
        """Create from Drift Data API market response."""
        return cls(
            market_index=data.get("marketIndex", 0),
            symbol=data.get("symbol", ""),
            base_asset_symbol=data.get("baseAssetSymbol", ""),
            oracle_price=Decimal(str(data.get("oraclePrice", "0"))),
            funding_rate=Decimal(str(data.get("lastFundingRate", "0"))),
            funding_rate_24h=Decimal(str(data.get("fundingRate24h", "0"))),
            open_interest=Decimal(str(data.get("openInterest", "0"))),
            volume_24h=Decimal(str(data.get("volume24h", "0"))),
            mark_price=Decimal(str(data.get("markPrice", "0"))),
        )


@dataclass
class OrderParams:
    """Parameters for a Drift perpetual order.

    Maps to the on-chain OrderParams struct that gets Borsh-encoded
    into instruction data.

    Attributes:
        order_type: 0=Market, 1=Limit, 2=TriggerMarket, 3=TriggerLimit, 4=Oracle
        market_type: 0=Perp, 1=Spot
        direction: 0=Long, 1=Short
        user_order_id: User-assigned order ID (u8, 0-255)
        base_asset_amount: Position size in base precision (1e9)
        price: Limit price in price precision (1e6), 0 for market orders
        market_index: Market index (u16)
        reduce_only: Whether order can only reduce position
        post_only: PostOnlyParam enum (0=None, 1=MustPostOnly, 2=TryPostOnly, 3=Slide)
        bit_flags: u8 bit field — bit 0: ImmediateOrCancel, bit 1: UpdateHighLeverageMode
        max_ts: Optional max timestamp (unix seconds)
        trigger_price: Optional trigger price for stop/take-profit orders
        trigger_condition: 0=Above, 1=Below
        oracle_price_offset: Optional offset from oracle price (i32, in 1e6)
        auction_duration: Optional auction duration in slots
        auction_start_price: Optional auction start price
        auction_end_price: Optional auction end price
    """

    order_type: int = ORDER_TYPE_MARKET
    market_type: int = MARKET_TYPE_PERP
    direction: int = DIRECTION_LONG
    user_order_id: int = 0
    base_asset_amount: int = 0
    price: int = 0
    market_index: int = 0
    reduce_only: bool = False
    post_only: int = POST_ONLY_NONE
    # On-chain field: bit_flags (u8). Bit 0 = ImmediateOrCancel, Bit 1 = UpdateHighLeverageMode.
    # Set bit_flags=1 for IOC, bit_flags=2 for high leverage mode, bit_flags=3 for both.
    bit_flags: int = 0
    max_ts: int | None = None
    trigger_price: int | None = None
    trigger_condition: int = TRIGGER_CONDITION_ABOVE
    oracle_price_offset: int | None = None
    auction_duration: int | None = None
    auction_start_price: int | None = None
    auction_end_price: int | None = None


@dataclass
class DriftPerpPosition:
    """A user's perpetual position on Drift.

    Parsed from on-chain User account data.

    Attributes:
        market_index: Perp market index
        base_asset_amount: Signed base amount (positive=long, negative=short)
        quote_asset_amount: Quote amount (accumulated PnL)
        last_cumulative_funding_rate: Last seen funding rate
        open_orders: Number of open orders for this market
    """

    market_index: int = 0
    base_asset_amount: int = 0
    quote_asset_amount: int = 0
    last_cumulative_funding_rate: int = 0
    open_orders: int = 0

    @property
    def is_active(self) -> bool:
        """Whether this position slot has an active position."""
        return self.base_asset_amount != 0

    @property
    def is_long(self) -> bool:
        """Whether the position is long."""
        return self.base_asset_amount > 0


@dataclass
class DriftSpotPosition:
    """A user's spot position on Drift.

    Parsed from on-chain User account data.

    Attributes:
        market_index: Spot market index
        scaled_balance: Scaled balance (deposit or borrow)
        balance_type: 0=Deposit, 1=Borrow
        open_orders: Number of open orders
    """

    market_index: int = 0
    scaled_balance: int = 0
    balance_type: int = 0
    open_orders: int = 0

    @property
    def is_active(self) -> bool:
        """Whether this position slot has an active balance."""
        return self.scaled_balance != 0


@dataclass
class DriftUserAccount:
    """Parsed Drift User account state.

    Attributes:
        authority: Wallet public key that owns this account
        sub_account_id: Sub-account identifier
        perp_positions: List of perp position slots
        spot_positions: List of spot position slots
        exists: Whether the account exists on-chain
    """

    authority: str = ""
    sub_account_id: int = 0
    perp_positions: list[DriftPerpPosition] = field(default_factory=list)
    spot_positions: list[DriftSpotPosition] = field(default_factory=list)
    exists: bool = True

    @property
    def active_perp_market_indexes(self) -> list[int]:
        """Get market indexes of active perp positions."""
        return [p.market_index for p in self.perp_positions if p.is_active]

    @property
    def active_spot_market_indexes(self) -> list[int]:
        """Get market indexes of active spot positions."""
        return [p.market_index for p in self.spot_positions if p.is_active]


@dataclass
class FundingRate:
    """Funding rate data point.

    Attributes:
        timestamp: Unix timestamp
        funding_rate: Hourly funding rate as decimal
        market_index: Market index
    """

    timestamp: int = 0
    funding_rate: Decimal = Decimal("0")
    market_index: int = 0

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "FundingRate":
        """Create from Drift Data API response."""
        return cls(
            timestamp=data.get("ts", 0),
            funding_rate=Decimal(str(data.get("fundingRate", "0"))),
            market_index=data.get("marketIndex", 0),
        )
