"""GMX v2 Protocol Adapter.

This module provides the GMXv2Adapter class for interacting with GMX v2
perpetuals protocol on Arbitrum and Avalanche.

GMX v2 Architecture:
- ExchangeRouter: Main entry point for order creation
- OrderHandler: Processes and executes orders
- PositionHandler: Manages position state
- DataStore: Central storage for all protocol data
- Reader: View functions for reading protocol state

Key Concepts:
- Market: Trading pair (e.g., ETH/USD) with an index token and collateral tokens
- Position: Open leveraged long or short position
- Order: Pending market or limit order
- Collateral: Token used as margin for positions
"""

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any

from almanak.framework.data.tokens.exceptions import TokenResolutionError

if TYPE_CHECKING:
    from almanak.framework.data.tokens.resolver import TokenResolver as TokenResolverType

logger = logging.getLogger(__name__)


def _normalize_datetime_to_utc(dt: datetime) -> datetime:
    """Normalize a datetime to UTC timezone.

    If the datetime is naive (no tzinfo), assume it's UTC and add UTC timezone.
    If it already has a timezone, convert it to UTC.

    Args:
        dt: Datetime that may or may not have timezone info.

    Returns:
        Datetime with UTC timezone.
    """
    if dt.tzinfo is None:
        # Naive datetime - assume UTC
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


# =============================================================================
# Constants
# =============================================================================

# GMX v2 contract addresses per chain (updated Mar 2026)
# Source: https://github.com/gmx-io/gmx-interface/blob/master/sdk/src/configs/contracts.ts
#
# Note on GMX V2 architecture:
# - Position state is stored in DataStore, not a separate PositionHandler
# - Orders are processed by internal keepers, not a public PositionHandler
# - Read positions via Reader contract (SyntheticsReader)
GMX_V2_ADDRESSES: dict[str, dict[str, str]] = {
    "arbitrum": {
        "exchange_router": "0x1C3fa76e6E1088bCE750f23a5BFcffa1efEF6A41",
        "order_handler": "0x63492B775e30a9E6b4b4761c12605EB9d071d5e9",
        "data_store": "0xFD70de6b91282D8017aA4E741e9Ae325CAb992d8",
        "reader": "0xf60becbba223EEA9495Da3f606753867eC10d139",
        "synthetics_reader": "0x470fbC46bcC0f16532691Df360A07d8Bf5ee0789",
        "order_vault": "0x31eF83a530Fde1B38EE9A18093A333D8Bbbc40D5",
        "deposit_vault": "0xF89e77e8Dc11691C9e8757e84aaFbCD8A67d7A55",
        "withdrawal_vault": "0x0628D46b5D145f183AdB6Ef1f2c97eD1C4701C55",
        "router": "0x7452c558d45f8afC8c83dAe62C3f8A5BE19c71f6",
        "event_emitter": "0xC8ee91A54287DB53897056e12D9819156D3822Fb",
    },
    "avalanche": {
        "exchange_router": "0x8f550E53DFe96C055D5Bdb267c21F268fCAF63B2",
        "order_handler": "0x823b558B4bC0a2C4974a0d8D7885AA1102D15dEC",
        "data_store": "0x2F0b22339414ADeD7D5F06f9D604c7fF5b2fe3f6",
        "reader": "0x2eFEE1950ededC65De687b40Fd30a7B5f4544aBd",
        "synthetics_reader": "0x62Cb8740E6986B29dC671B2EB596676f60590A5B",
        "order_vault": "0xEE7d43517A62fa0aC642e22Eb93a93f82D0D3df6",
        "deposit_vault": "0x90c670825d0C62ede1c5ee9571d6d9a17A722DFF",
        "withdrawal_vault": "0xf5F30B10141E1F63FC11eD772931A8294a591996",
        "router": "0x820F5FfC5b525cD4d88Cd91aCf2c28F16530Cc68",
        "event_emitter": "0xDb17B211c34240B014ab6d61d4A31FA0C0e20c26",
    },
}

# GMX v2 markets (index token -> market address)
GMX_V2_MARKETS: dict[str, dict[str, str]] = {
    "arbitrum": {
        "ETH/USD": "0x70d95587d40A2caf56bd97485aB3Eec10Bee6336",  # ETH market
        "BTC/USD": "0x47c031236e19d024b42f8AE6780E44A573170703",  # BTC market
        "LINK/USD": "0x7f1fa204bb700853D36994DA19F830b6Ad18455C",
        "ARB/USD": "0xC25cEf6061Cf5dE5eb761b50E4743c1F5D7E5407",
        "SOL/USD": "0x09400D9DB990D5ed3f35D7be61DfAEB900Af03C9",
        "UNI/USD": "0xc7Abb2C5f3BF3CEB389dF0Ebb3cfe90EcE8a1BaA",
        "DOGE/USD": "0x6853EA96FF216fAb11D2d930CE3C508556A4bdc4",
        "LTC/USD": "0xD9535bB5f58A1a75032416F2dFe7880C30575a41",
        "XRP/USD": "0x0CCB4fAa6f1F1B30911619f1184082aB4E25813c",
        "ATOM/USD": "0x248C35760068cE009a13076D573ed3497A47bCD4",
        "NEAR/USD": "0x63Dc80EE90F26363B3FCD609007CC9e14c8991BE",
        "AAVE/USD": "0x1CbBa6346F110c8A5ea739ef2d1eb182990e4EB2",
        "AVAX/USD": "0xB7e69749E3d2EDd90ea59A4932EFEa2D41E245d7",
        "OP/USD": "0xB56E5E2eB50cf5383342914b0C85Fe62DbD861C8",
        "GMX/USD": "0x55391D178Ce46e7AC8eaAEa50A72D1A5a8A622Da",
    },
    "avalanche": {
        "AVAX/USD": "0xD996ff47A1F763E1e55415BC4437c59292D1F415",
        "ETH/USD": "0xB7e69749E3d2EDd90ea59A4932EFEa2D41E245d7",
        "BTC/USD": "0xFb02132333A79C8B5Bd0b64E3AbccA5f7fAf2937",
        "SOL/USD": "0x91ccF2053d79e16bEE6B8C4b9f8E67bA64669B98",
        "LTC/USD": "0x7E0d5Dc8c0c4f04C37568A5E3C2B29cA6c54A8E7",
    },
}


# Default execution fee (in native token)
DEFAULT_EXECUTION_FEE: dict[str, int] = {
    "arbitrum": int(0.002 * 10**18),  # 0.002 ETH (GMX requires ~0.0016+ as of 2026)
    "avalanche": int(0.02 * 10**18),  # 0.02 AVAX
}

# Gas estimates for GMX v2 operations
GMX_V2_GAS_ESTIMATES: dict[str, int] = {
    "create_increase_order": 800000,
    "create_decrease_order": 600000,
    "cancel_order": 200000,
    "claim_funding_fees": 300000,
    "claim_collateral": 200000,
}

# Function selectors for GMX v2 ExchangeRouter
GMX_CREATE_ORDER_SELECTOR = "0x5e2c576b"  # createOrder((address,...))
GMX_UPDATE_ORDER_SELECTOR = "0xfec7303e"  # updateOrder(bytes32,uint256,uint256,uint256)
GMX_CANCEL_ORDER_SELECTOR = "0xd42a7b9e"  # cancelOrder(bytes32)
GMX_CLAIM_FUNDING_FEES_SELECTOR = "0xd294f093"  # claimFundingFees(address[],address[],address)

# Order types
ORDER_TYPE_MARKET_INCREASE = 0
ORDER_TYPE_LIMIT_INCREASE = 1
ORDER_TYPE_MARKET_DECREASE = 2
ORDER_TYPE_LIMIT_DECREASE = 3
ORDER_TYPE_STOP_LOSS_DECREASE = 4
ORDER_TYPE_LIQUIDATION = 5


# =============================================================================
# Enums
# =============================================================================


class GMXv2OrderType(Enum):
    """GMX v2 order types."""

    MARKET_INCREASE = "MARKET_INCREASE"
    LIMIT_INCREASE = "LIMIT_INCREASE"
    MARKET_DECREASE = "MARKET_DECREASE"
    LIMIT_DECREASE = "LIMIT_DECREASE"
    STOP_LOSS_DECREASE = "STOP_LOSS_DECREASE"
    LIQUIDATION = "LIQUIDATION"

    def to_int(self) -> int:
        """Convert to GMX v2 order type integer."""
        mapping = {
            GMXv2OrderType.MARKET_INCREASE: ORDER_TYPE_MARKET_INCREASE,
            GMXv2OrderType.LIMIT_INCREASE: ORDER_TYPE_LIMIT_INCREASE,
            GMXv2OrderType.MARKET_DECREASE: ORDER_TYPE_MARKET_DECREASE,
            GMXv2OrderType.LIMIT_DECREASE: ORDER_TYPE_LIMIT_DECREASE,
            GMXv2OrderType.STOP_LOSS_DECREASE: ORDER_TYPE_STOP_LOSS_DECREASE,
            GMXv2OrderType.LIQUIDATION: ORDER_TYPE_LIQUIDATION,
        }
        return mapping[self]


class GMXv2PositionSide(Enum):
    """Position side (long/short)."""

    LONG = "LONG"
    SHORT = "SHORT"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class GMXv2Config:
    """Configuration for GMXv2Adapter.

    Attributes:
        chain: Target blockchain (arbitrum or avalanche)
        wallet_address: Address executing transactions
        default_slippage_bps: Default slippage tolerance in basis points (default 50 = 0.5%)
        execution_fee: Execution fee in native token wei (auto-set per chain)
        referral_code: Optional referral code for fee discounts
    """

    chain: str
    wallet_address: str
    default_slippage_bps: int = 50
    execution_fee: int | None = None
    referral_code: bytes = b"\x00" * 32

    def __post_init__(self) -> None:
        """Validate configuration and set defaults."""
        if self.chain not in GMX_V2_ADDRESSES:
            raise ValueError(f"Unsupported chain: {self.chain}. Supported: {list(GMX_V2_ADDRESSES.keys())}")

        if self.execution_fee is None:
            self.execution_fee = DEFAULT_EXECUTION_FEE.get(self.chain, DEFAULT_EXECUTION_FEE["arbitrum"])

        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError("Slippage must be between 0 and 10000 basis points")

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "chain": self.chain,
            "wallet_address": self.wallet_address,
            "default_slippage_bps": self.default_slippage_bps,
            "execution_fee": self.execution_fee,
            "referral_code": self.referral_code.hex(),
        }


@dataclass
class GMXv2Position:
    """Represents an open GMX v2 position.

    Attributes:
        position_key: Unique identifier for the position
        market: Market address
        collateral_token: Token used as collateral
        size_in_usd: Position size in USD (30 decimals)
        size_in_tokens: Position size in index tokens (token decimals)
        collateral_amount: Collateral amount in token decimals
        entry_price: Average entry price (30 decimals)
        is_long: True for long, False for short
        realized_pnl: Realized PnL (30 decimals)
        unrealized_pnl: Unrealized PnL (30 decimals)
        leverage: Current leverage (size / collateral)
        liquidation_price: Price at which position gets liquidated
        funding_fee_amount: Accumulated funding fees
        borrowing_fee_amount: Accumulated borrowing fees
        last_updated: Timestamp of last update
    """

    position_key: str
    market: str
    collateral_token: str
    size_in_usd: Decimal
    size_in_tokens: Decimal
    collateral_amount: Decimal
    entry_price: Decimal
    is_long: bool
    realized_pnl: Decimal = Decimal("0")
    unrealized_pnl: Decimal = Decimal("0")
    leverage: Decimal = Decimal("1")
    liquidation_price: Decimal | None = None
    funding_fee_amount: Decimal = Decimal("0")
    borrowing_fee_amount: Decimal = Decimal("0")
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def side(self) -> GMXv2PositionSide:
        """Get position side."""
        return GMXv2PositionSide.LONG if self.is_long else GMXv2PositionSide.SHORT

    @property
    def total_fees(self) -> Decimal:
        """Get total accumulated fees."""
        return self.funding_fee_amount + self.borrowing_fee_amount

    @property
    def net_pnl(self) -> Decimal:
        """Get net PnL after fees."""
        return self.unrealized_pnl - self.total_fees

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "position_key": self.position_key,
            "market": self.market,
            "collateral_token": self.collateral_token,
            "size_in_usd": str(self.size_in_usd),
            "size_in_tokens": str(self.size_in_tokens),
            "collateral_amount": str(self.collateral_amount),
            "entry_price": str(self.entry_price),
            "is_long": self.is_long,
            "side": self.side.value,
            "realized_pnl": str(self.realized_pnl),
            "unrealized_pnl": str(self.unrealized_pnl),
            "leverage": str(self.leverage),
            "liquidation_price": str(self.liquidation_price) if self.liquidation_price else None,
            "funding_fee_amount": str(self.funding_fee_amount),
            "borrowing_fee_amount": str(self.borrowing_fee_amount),
            "total_fees": str(self.total_fees),
            "net_pnl": str(self.net_pnl),
            "last_updated": self.last_updated.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GMXv2Position":
        """Create from dictionary."""
        # Parse and normalize last_updated to UTC
        if "last_updated" in data:
            last_updated = _normalize_datetime_to_utc(datetime.fromisoformat(data["last_updated"]))
        else:
            last_updated = datetime.now(UTC)

        return cls(
            position_key=data["position_key"],
            market=data["market"],
            collateral_token=data["collateral_token"],
            size_in_usd=Decimal(data["size_in_usd"]),
            size_in_tokens=Decimal(data["size_in_tokens"]),
            collateral_amount=Decimal(data["collateral_amount"]),
            entry_price=Decimal(data["entry_price"]),
            is_long=data["is_long"],
            realized_pnl=Decimal(data.get("realized_pnl", "0")),
            unrealized_pnl=Decimal(data.get("unrealized_pnl", "0")),
            leverage=Decimal(data.get("leverage", "1")),
            liquidation_price=(Decimal(data["liquidation_price"]) if data.get("liquidation_price") else None),
            funding_fee_amount=Decimal(data.get("funding_fee_amount", "0")),
            borrowing_fee_amount=Decimal(data.get("borrowing_fee_amount", "0")),
            last_updated=last_updated,
        )


@dataclass
class GMXv2Order:
    """Represents a GMX v2 order.

    Attributes:
        order_key: Unique identifier for the order
        market: Market address
        initial_collateral_token: Collateral token for the order
        order_type: Type of order
        is_long: Position direction
        size_delta_usd: Size change in USD (30 decimals)
        initial_collateral_delta_amount: Collateral amount change
        trigger_price: Trigger price for limit/stop orders
        acceptable_price: Maximum/minimum acceptable execution price
        execution_fee: Fee paid to keeper
        callback_gas_limit: Gas limit for callback execution
        is_frozen: Whether order is frozen
        created_at: Order creation timestamp
        updated_at: Last update timestamp
    """

    order_key: str
    market: str
    initial_collateral_token: str
    order_type: GMXv2OrderType
    is_long: bool
    size_delta_usd: Decimal
    initial_collateral_delta_amount: Decimal
    trigger_price: Decimal | None = None
    acceptable_price: Decimal | None = None
    execution_fee: int = 0
    callback_gas_limit: int = 0
    is_frozen: bool = False
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def is_increase(self) -> bool:
        """Check if order increases position size."""
        return self.order_type in (
            GMXv2OrderType.MARKET_INCREASE,
            GMXv2OrderType.LIMIT_INCREASE,
        )

    @property
    def is_decrease(self) -> bool:
        """Check if order decreases position size."""
        return self.order_type in (
            GMXv2OrderType.MARKET_DECREASE,
            GMXv2OrderType.LIMIT_DECREASE,
            GMXv2OrderType.STOP_LOSS_DECREASE,
        )

    @property
    def is_market_order(self) -> bool:
        """Check if order is a market order."""
        return self.order_type in (
            GMXv2OrderType.MARKET_INCREASE,
            GMXv2OrderType.MARKET_DECREASE,
        )

    @property
    def is_limit_order(self) -> bool:
        """Check if order is a limit order."""
        return self.order_type in (
            GMXv2OrderType.LIMIT_INCREASE,
            GMXv2OrderType.LIMIT_DECREASE,
            GMXv2OrderType.STOP_LOSS_DECREASE,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "order_key": self.order_key,
            "market": self.market,
            "initial_collateral_token": self.initial_collateral_token,
            "order_type": self.order_type.value,
            "is_long": self.is_long,
            "size_delta_usd": str(self.size_delta_usd),
            "initial_collateral_delta_amount": str(self.initial_collateral_delta_amount),
            "trigger_price": str(self.trigger_price) if self.trigger_price else None,
            "acceptable_price": str(self.acceptable_price) if self.acceptable_price else None,
            "execution_fee": self.execution_fee,
            "callback_gas_limit": self.callback_gas_limit,
            "is_frozen": self.is_frozen,
            "is_increase": self.is_increase,
            "is_decrease": self.is_decrease,
            "is_market_order": self.is_market_order,
            "is_limit_order": self.is_limit_order,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "GMXv2Order":
        """Create from dictionary."""
        # Parse and normalize timestamps to UTC
        if "created_at" in data:
            created_at = _normalize_datetime_to_utc(datetime.fromisoformat(data["created_at"]))
        else:
            created_at = datetime.now(UTC)

        if "updated_at" in data:
            updated_at = _normalize_datetime_to_utc(datetime.fromisoformat(data["updated_at"]))
        else:
            updated_at = datetime.now(UTC)

        return cls(
            order_key=data["order_key"],
            market=data["market"],
            initial_collateral_token=data["initial_collateral_token"],
            order_type=GMXv2OrderType(data["order_type"]),
            is_long=data["is_long"],
            size_delta_usd=Decimal(data["size_delta_usd"]),
            initial_collateral_delta_amount=Decimal(data["initial_collateral_delta_amount"]),
            trigger_price=(Decimal(data["trigger_price"]) if data.get("trigger_price") else None),
            acceptable_price=(Decimal(data["acceptable_price"]) if data.get("acceptable_price") else None),
            execution_fee=data.get("execution_fee", 0),
            callback_gas_limit=data.get("callback_gas_limit", 0),
            is_frozen=data.get("is_frozen", False),
            created_at=created_at,
            updated_at=updated_at,
        )


@dataclass
class OrderResult:
    """Result of creating an order.

    Attributes:
        success: Whether order creation succeeded
        order_key: Order key if successful
        tx_hash: Transaction hash
        order: Created order object
        error: Error message if failed
        gas_used: Gas used by transaction
    """

    success: bool
    order_key: str | None = None
    tx_hash: str | None = None
    order: GMXv2Order | None = None
    error: str | None = None
    gas_used: int = 0

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "order_key": self.order_key,
            "tx_hash": self.tx_hash,
            "order": self.order.to_dict() if self.order else None,
            "error": self.error,
            "gas_used": self.gas_used,
        }


@dataclass
class TransactionData:
    """Transaction data for execution.

    Attributes:
        to: Target contract address
        value: Native token value to send
        data: Encoded calldata
        gas_estimate: Estimated gas
        description: Human-readable description
    """

    to: str
    value: int
    data: str
    gas_estimate: int
    description: str

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "to": self.to,
            "value": str(self.value),
            "data": self.data,
            "gas_estimate": self.gas_estimate,
            "description": self.description,
        }


# =============================================================================
# GMX v2 Adapter
# =============================================================================


class GMXv2Adapter:
    """Adapter for GMX v2 perpetuals protocol.

    This adapter provides methods for:
    - Opening and closing positions
    - Increasing and decreasing position size
    - Managing limit orders and stop losses
    - Querying position and market data
    - Parsing transaction receipts

    Example:
        config = GMXv2Config(
            chain="arbitrum",
            wallet_address="0x...",
        )
        adapter = GMXv2Adapter(config)

        # Open a long position
        result = adapter.open_position(
            market="ETH/USD",
            collateral_token="USDC",
            collateral_amount=Decimal("1000"),
            size_delta_usd=Decimal("5000"),
            is_long=True,
        )

        # Check position
        position = adapter.get_position(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
        )

        # Close position
        result = adapter.close_position(
            market="ETH/USD",
            collateral_token="USDC",
            is_long=True,
            size_delta_usd=position.size_in_usd,
        )
    """

    def __init__(self, config: GMXv2Config, token_resolver: "TokenResolverType | None" = None) -> None:
        """Initialize the adapter.

        Args:
            config: GMX v2 adapter configuration
            token_resolver: Optional TokenResolver instance. If None, uses singleton.
        """
        self.config = config
        self.chain = config.chain
        self.wallet_address = config.wallet_address

        # Load contract addresses
        self.addresses = GMX_V2_ADDRESSES[self.chain]
        self.markets = GMX_V2_MARKETS.get(self.chain, {})

        # TokenResolver integration
        if token_resolver is not None:
            self._token_resolver = token_resolver
        else:
            from almanak.framework.data.tokens.resolver import get_token_resolver

            self._token_resolver = get_token_resolver()

        # Internal state tracking (in production, query from chain)
        self._positions: dict[str, GMXv2Position] = {}
        self._orders: dict[str, GMXv2Order] = {}

        logger.info(f"GMXv2Adapter initialized for chain={self.chain}, wallet={self.wallet_address[:10]}...")

    # =========================================================================
    # Position Management
    # =========================================================================

    def open_position(
        self,
        market: str,
        collateral_token: str,
        collateral_amount: Decimal,
        size_delta_usd: Decimal,
        is_long: bool,
        acceptable_price: Decimal | None = None,
        trigger_price: Decimal | None = None,
    ) -> OrderResult:
        """Open a new position or increase existing position.

        Args:
            market: Market identifier (e.g., "ETH/USD") or market address
            collateral_token: Token symbol or address for collateral
            collateral_amount: Amount of collateral in token decimals
            size_delta_usd: Position size in USD (will be scaled to 30 decimals)
            is_long: True for long, False for short
            acceptable_price: Maximum (long) or minimum (short) execution price
            trigger_price: Trigger price for limit orders

        Returns:
            OrderResult with order details
        """
        try:
            # Resolve market address
            market_address = self._resolve_market(market)
            if market_address is None:
                return OrderResult(
                    success=False,
                    error=f"Unknown market: {market}",
                )

            # Resolve collateral token
            collateral_address = self._resolve_token(collateral_token)
            if collateral_address is None:
                return OrderResult(
                    success=False,
                    error=f"Unknown collateral token: {collateral_token}",
                )

            # Pre-flight check: warn about execution fee requirement
            execution_fee = self.config.execution_fee or 0
            if execution_fee > 0:
                execution_fee_eth = Decimal(execution_fee) / Decimal(10**18)
                logger.warning(
                    f"GMX V2 order requires ~{execution_fee_eth:.4f} native token as keeper execution fee "
                    f"(on top of gas costs). Ensure wallet has sufficient native balance."
                )

            # Determine order type
            order_type = GMXv2OrderType.LIMIT_INCREASE if trigger_price else GMXv2OrderType.MARKET_INCREASE

            # Scale amounts (GMX v2 uses 30 decimals for USD values)
            size_delta_30 = size_delta_usd * Decimal(10**30)

            # Get collateral decimals
            collateral_decimals = self._get_token_decimals(collateral_token)
            collateral_wei = int(collateral_amount * Decimal(10**collateral_decimals))

            # Calculate acceptable price if not provided
            # For longs: we want to pay at most current price + slippage
            # For shorts: we want to receive at least current price - slippage
            if acceptable_price is None:
                # Use trigger price or max/min acceptable
                acceptable_price = trigger_price if trigger_price else (Decimal(10**30) if is_long else Decimal("0"))

            # Generate order key (in production, from contract event)
            import uuid

            order_key = f"0x{uuid.uuid4().hex}"

            # Create order object
            order = GMXv2Order(
                order_key=order_key,
                market=market_address,
                initial_collateral_token=collateral_address,
                order_type=order_type,
                is_long=is_long,
                size_delta_usd=size_delta_usd,
                initial_collateral_delta_amount=collateral_amount,
                trigger_price=trigger_price,
                acceptable_price=acceptable_price,
                execution_fee=self.config.execution_fee or 0,
            )

            # Build transaction
            tx_data = self._build_create_order_tx(
                market_address=market_address,
                collateral_token=collateral_address,
                collateral_amount=collateral_wei,
                size_delta_usd=int(size_delta_30),
                is_long=is_long,
                order_type=order_type.to_int(),
                acceptable_price=int(acceptable_price * Decimal(10**30)),
                trigger_price=int(trigger_price * Decimal(10**30)) if trigger_price else 0,
            )

            # Store order (in production, wait for event)
            self._orders[order_key] = order

            logger.info(f"Created {order_type.value} order: market={market}, size=${size_delta_usd}, is_long={is_long}")

            return OrderResult(
                success=True,
                order_key=order_key,
                order=order,
                gas_used=tx_data.gas_estimate,
            )

        except Exception as e:
            logger.exception(f"Failed to open position: {e}")
            return OrderResult(
                success=False,
                error=str(e),
            )

    def close_position(
        self,
        market: str,
        collateral_token: str,
        is_long: bool,
        size_delta_usd: Decimal | None = None,
        receive_token: str | None = None,
        acceptable_price: Decimal | None = None,
        trigger_price: Decimal | None = None,
    ) -> OrderResult:
        """Close a position or decrease position size.

        Args:
            market: Market identifier or address
            collateral_token: Token symbol or address for collateral
            is_long: Position direction
            size_delta_usd: Amount to close in USD (None = close entire position)
            receive_token: Token to receive (defaults to collateral_token)
            acceptable_price: Minimum (long) or maximum (short) execution price
            trigger_price: Trigger price for limit orders

        Returns:
            OrderResult with order details
        """
        try:
            # Resolve market address
            market_address = self._resolve_market(market)
            if market_address is None:
                return OrderResult(
                    success=False,
                    error=f"Unknown market: {market}",
                )

            # Resolve collateral token
            collateral_address = self._resolve_token(collateral_token)
            if collateral_address is None:
                return OrderResult(
                    success=False,
                    error=f"Unknown collateral token: {collateral_token}",
                )

            # Get position to determine size if not specified
            position_key = self._get_position_key(market_address, collateral_address, is_long)
            position = self._positions.get(position_key)

            if size_delta_usd is None:
                if position:
                    size_delta_usd = position.size_in_usd
                else:
                    return OrderResult(
                        success=False,
                        error="No size specified and no existing position found",
                    )

            # Determine order type
            order_type = GMXv2OrderType.LIMIT_DECREASE if trigger_price else GMXv2OrderType.MARKET_DECREASE

            # Scale amounts
            size_delta_30 = size_delta_usd * Decimal(10**30)

            # Calculate acceptable price if not provided
            # For closing longs: we want to receive at least current price - slippage
            # For closing shorts: we want to pay at most current price + slippage
            if acceptable_price is None:
                acceptable_price = trigger_price if trigger_price else (Decimal("0") if is_long else Decimal(10**30))

            # Generate order key
            import uuid

            order_key = f"0x{uuid.uuid4().hex}"

            # Resolve receive token
            receive_address = self._resolve_token(receive_token) if receive_token else collateral_address

            # Create order object
            order = GMXv2Order(
                order_key=order_key,
                market=market_address,
                initial_collateral_token=collateral_address,
                order_type=order_type,
                is_long=is_long,
                size_delta_usd=size_delta_usd,
                initial_collateral_delta_amount=Decimal("0"),  # Collateral returned on close
                trigger_price=trigger_price,
                acceptable_price=acceptable_price,
                execution_fee=self.config.execution_fee or 0,
            )

            # Build transaction
            tx_data = self._build_create_order_tx(
                market_address=market_address,
                collateral_token=receive_address or collateral_address,
                collateral_amount=0,  # No additional collateral for decrease
                size_delta_usd=int(size_delta_30),
                is_long=is_long,
                order_type=order_type.to_int(),
                acceptable_price=int(acceptable_price * Decimal(10**30)),
                trigger_price=int(trigger_price * Decimal(10**30)) if trigger_price else 0,
                is_decrease=True,
            )

            # Store order
            self._orders[order_key] = order

            logger.info(f"Created {order_type.value} order: market={market}, size=${size_delta_usd}, is_long={is_long}")

            return OrderResult(
                success=True,
                order_key=order_key,
                order=order,
                gas_used=tx_data.gas_estimate,
            )

        except Exception as e:
            logger.exception(f"Failed to close position: {e}")
            return OrderResult(
                success=False,
                error=str(e),
            )

    def increase_position(
        self,
        market: str,
        collateral_token: str,
        is_long: bool,
        collateral_delta: Decimal,
        size_delta_usd: Decimal,
        acceptable_price: Decimal | None = None,
        trigger_price: Decimal | None = None,
    ) -> OrderResult:
        """Increase an existing position.

        This is an alias for open_position with an existing position.

        Args:
            market: Market identifier or address
            collateral_token: Token symbol or address
            is_long: Position direction
            collateral_delta: Additional collateral to add
            size_delta_usd: Additional size in USD
            acceptable_price: Maximum (long) or minimum (short) execution price
            trigger_price: Trigger price for limit orders

        Returns:
            OrderResult with order details
        """
        return self.open_position(
            market=market,
            collateral_token=collateral_token,
            collateral_amount=collateral_delta,
            size_delta_usd=size_delta_usd,
            is_long=is_long,
            acceptable_price=acceptable_price,
            trigger_price=trigger_price,
        )

    def decrease_position(
        self,
        market: str,
        collateral_token: str,
        is_long: bool,
        size_delta_usd: Decimal,
        collateral_delta: Decimal = Decimal("0"),
        receive_token: str | None = None,
        acceptable_price: Decimal | None = None,
        trigger_price: Decimal | None = None,
    ) -> OrderResult:
        """Decrease an existing position.

        This is similar to close_position but for partial closes.

        Args:
            market: Market identifier or address
            collateral_token: Token symbol or address
            is_long: Position direction
            size_delta_usd: Size to reduce in USD
            collateral_delta: Collateral to withdraw
            receive_token: Token to receive
            acceptable_price: Minimum (long) or maximum (short) execution price
            trigger_price: Trigger price for limit orders

        Returns:
            OrderResult with order details
        """
        return self.close_position(
            market=market,
            collateral_token=collateral_token,
            is_long=is_long,
            size_delta_usd=size_delta_usd,
            receive_token=receive_token,
            acceptable_price=acceptable_price,
            trigger_price=trigger_price,
        )

    def get_position(
        self,
        market: str,
        collateral_token: str,
        is_long: bool,
    ) -> GMXv2Position | None:
        """Get position details.

        Args:
            market: Market identifier or address
            collateral_token: Token symbol or address
            is_long: Position direction

        Returns:
            Position details or None if not found
        """
        market_address = self._resolve_market(market)
        collateral_address = self._resolve_token(collateral_token)

        if market_address is None or collateral_address is None:
            return None

        position_key = self._get_position_key(market_address, collateral_address, is_long)
        return self._positions.get(position_key)

    def get_all_positions(self) -> list[GMXv2Position]:
        """Get all open positions.

        Returns:
            List of all open positions
        """
        return list(self._positions.values())

    # =========================================================================
    # Order Management
    # =========================================================================

    def cancel_order(self, order_key: str) -> OrderResult:
        """Cancel a pending order.

        Args:
            order_key: Order key to cancel

        Returns:
            OrderResult indicating success/failure
        """
        try:
            order = self._orders.get(order_key)
            if order is None:
                return OrderResult(
                    success=False,
                    error=f"Order not found: {order_key}",
                )

            # Build cancel transaction
            tx_data = self._build_cancel_order_tx(order_key)

            # Remove from tracking
            del self._orders[order_key]

            logger.info(f"Cancelled order: {order_key}")

            return OrderResult(
                success=True,
                order_key=order_key,
                gas_used=tx_data.gas_estimate,
            )

        except Exception as e:
            logger.exception(f"Failed to cancel order: {e}")
            return OrderResult(
                success=False,
                error=str(e),
            )

    def get_order(self, order_key: str) -> GMXv2Order | None:
        """Get order details.

        Args:
            order_key: Order key to look up

        Returns:
            Order details or None if not found
        """
        return self._orders.get(order_key)

    def get_all_orders(self) -> list[GMXv2Order]:
        """Get all pending orders.

        Returns:
            List of all pending orders
        """
        return list(self._orders.values())

    # =========================================================================
    # Transaction Building
    # =========================================================================

    def _build_create_order_tx(
        self,
        market_address: str,
        collateral_token: str,
        collateral_amount: int,
        size_delta_usd: int,
        is_long: bool,
        order_type: int,
        acceptable_price: int,
        trigger_price: int = 0,
        is_decrease: bool = False,
    ) -> TransactionData:
        """Build transaction data for creating an order.

        GMX v2 ExchangeRouter.createOrder takes a CreateOrderParams struct:
        struct CreateOrderParams {
            address receiver;
            address callbackContract;
            address uiFeeReceiver;
            address market;
            address initialCollateralToken;
            address[] swapPath;
            uint256 sizeDeltaUsd;
            uint256 initialCollateralDeltaAmount;
            uint256 triggerPrice;
            uint256 acceptablePrice;
            uint256 executionFee;
            uint256 callbackGasLimit;
            uint256 minOutputAmount;
            OrderType orderType;
            DecreasePositionSwapType decreasePositionSwapType;
            bool isLong;
            bool shouldUnwrapNativeToken;
            bytes32 referralCode;
        }
        """
        # Build CreateOrderParams struct encoding
        # This is a simplified encoding - production would use proper ABI encoding

        # Addresses (padded to 32 bytes each)
        receiver = self._pad_address(self.wallet_address)
        callback_contract = self._pad_address("0x" + "0" * 40)  # No callback
        ui_fee_receiver = self._pad_address("0x" + "0" * 40)
        market = self._pad_address(market_address)
        initial_collateral = self._pad_address(collateral_token)

        # Values (padded to 32 bytes each)
        size_delta = self._pad_uint256(size_delta_usd)
        collateral_delta = self._pad_uint256(collateral_amount)
        trigger = self._pad_uint256(trigger_price)
        acceptable = self._pad_uint256(acceptable_price)
        execution_fee = self._pad_uint256(self.config.execution_fee or 0)
        callback_gas = self._pad_uint256(0)
        min_output = self._pad_uint256(0)
        order_type_padded = self._pad_uint256(order_type)
        decrease_swap_type = self._pad_uint256(0)  # NoSwap
        is_long_padded = self._pad_uint256(1 if is_long else 0)
        unwrap_native = self._pad_uint256(0)  # Don't unwrap
        referral = self.config.referral_code.hex().zfill(64)

        # Simplified calldata (actual encoding is more complex with dynamic arrays)
        calldata = (
            GMX_CREATE_ORDER_SELECTOR
            + receiver
            + callback_contract
            + ui_fee_receiver
            + market
            + initial_collateral
            # swapPath would go here as dynamic array offset
            + size_delta
            + collateral_delta
            + trigger
            + acceptable
            + execution_fee
            + callback_gas
            + min_output
            + order_type_padded
            + decrease_swap_type
            + is_long_padded
            + unwrap_native
            + referral
        )

        # Determine gas estimate
        gas_estimate = (
            GMX_V2_GAS_ESTIMATES["create_decrease_order"]
            if is_decrease
            else GMX_V2_GAS_ESTIMATES["create_increase_order"]
        )

        action = "decrease" if is_decrease else "increase"
        side = "long" if is_long else "short"

        return TransactionData(
            to=self.addresses["exchange_router"],
            value=self.config.execution_fee or 0,
            data=calldata,
            gas_estimate=gas_estimate,
            description=f"Create GMX v2 {action} {side} order",
        )

    def _build_cancel_order_tx(self, order_key: str) -> TransactionData:
        """Build transaction data for canceling an order."""
        # cancelOrder(bytes32 key)
        key_padded = order_key.replace("0x", "").zfill(64)
        calldata = GMX_CANCEL_ORDER_SELECTOR + key_padded

        return TransactionData(
            to=self.addresses["exchange_router"],
            value=0,
            data=calldata,
            gas_estimate=GMX_V2_GAS_ESTIMATES["cancel_order"],
            description=f"Cancel GMX v2 order {order_key[:10]}...",
        )

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _resolve_market(self, market: str) -> str | None:
        """Resolve market identifier to address."""
        # Check if already an address
        if market.startswith("0x") and len(market) == 42:
            return market

        # Look up by symbol
        return self.markets.get(market)

    def _resolve_token(self, token: str) -> str:
        """Resolve token identifier to address using TokenResolver."""
        if token.startswith("0x") and len(token) == 42:
            return token
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.address
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[GMXV2Adapter] Cannot resolve token: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_token_decimals(self, token: str) -> int:
        """Get token decimals using TokenResolver."""
        try:
            resolved = self._token_resolver.resolve(token, self.chain)
            return resolved.decimals
        except TokenResolutionError as e:
            raise TokenResolutionError(
                token=token,
                chain=str(self.chain),
                reason=f"[GMXV2Adapter] Cannot determine decimals: {e.reason}",
                suggestions=e.suggestions,
            ) from e

    def _get_position_key(self, market: str, collateral_token: str, is_long: bool) -> str:
        """Generate position key for internal tracking."""
        return f"{self.wallet_address}:{market}:{collateral_token}:{is_long}"

    @staticmethod
    def _pad_address(addr: str) -> str:
        """Pad address to 32 bytes."""
        return addr.lower().replace("0x", "").zfill(64)

    @staticmethod
    def _pad_uint256(value: int) -> str:
        """Pad uint256 to 32 bytes."""
        return hex(value)[2:].zfill(64)

    # =========================================================================
    # State Management (for testing/simulation)
    # =========================================================================

    def set_position(self, position: GMXv2Position) -> None:
        """Set a position for testing.

        Args:
            position: Position to set
        """
        position_key = self._get_position_key(position.market, position.collateral_token, position.is_long)
        self._positions[position_key] = position

    def clear_positions(self) -> None:
        """Clear all positions."""
        self._positions.clear()

    def clear_orders(self) -> None:
        """Clear all orders."""
        self._orders.clear()

    def clear_all(self) -> None:
        """Clear all state."""
        self.clear_positions()
        self.clear_orders()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "GMXv2Adapter",
    "GMXv2Config",
    "GMXv2Position",
    "GMXv2Order",
    "GMXv2OrderType",
    "GMXv2PositionSide",
    "OrderResult",
    "TransactionData",
    "GMX_V2_ADDRESSES",
    "GMX_V2_MARKETS",
    "DEFAULT_EXECUTION_FEE",
    "GMX_V2_GAS_ESTIMATES",
]
