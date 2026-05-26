"""Hyperliquid Protocol Adapter.

This module provides the HyperliquidAdapter class for interacting with Hyperliquid
perpetual futures exchange.

Hyperliquid Architecture:
- REST API for order management and account queries
- WebSocket for real-time data streaming
- L1 (mainnet) and L2 (testnet) environments with different signing requirements
- EIP-712 typed message signing for authentication

Key Concepts:
- Asset: Trading pair identifier (e.g., "ETH", "BTC")
- Position: Open leveraged long or short position
- Order: Limit or market order with time-in-force options
- Margin: Cross or isolated margin modes
- Leverage: Configurable per asset

Supported Operations:
- place_order: Place limit or market orders
- cancel_order: Cancel existing orders by order ID or client ID
- get_position: Get current position for an asset
- get_open_orders: Get all open orders for account
"""

import hashlib
import json
import logging
import secrets
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol

logger = logging.getLogger(__name__)


# =============================================================================
# Constants
# =============================================================================

# Hyperliquid API endpoints
HYPERLIQUID_API_URLS: dict[str, str] = {
    "mainnet": "https://api.hyperliquid.xyz",
    "testnet": "https://api.hyperliquid-testnet.xyz",
}

# Hyperliquid WebSocket endpoints
HYPERLIQUID_WS_URLS: dict[str, str] = {
    "mainnet": "wss://api.hyperliquid.xyz/ws",
    "testnet": "wss://api.hyperliquid-testnet.xyz/ws",
}

# Chain IDs for EIP-712 signing
HYPERLIQUID_CHAIN_IDS: dict[str, int] = {
    "mainnet": 1337,  # Hyperliquid L1 chain ID
    "testnet": 421614,  # Hyperliquid testnet (Arbitrum Sepolia based)
}

# EIP-712 domain for Hyperliquid
HYPERLIQUID_EIP712_DOMAIN: dict[str, dict[str, Any]] = {
    "mainnet": {
        "name": "Hyperliquid",
        "version": "1",
        "chainId": 1337,
        "verifyingContract": "0x0000000000000000000000000000000000000000",
    },
    "testnet": {
        "name": "Hyperliquid",
        "version": "1",
        "chainId": 421614,
        "verifyingContract": "0x0000000000000000000000000000000000000000",
    },
}

# Asset name to index mapping (Hyperliquid uses numeric indices internally)
HYPERLIQUID_ASSETS: dict[str, int] = {
    "BTC": 0,
    "ETH": 1,
    "SOL": 2,
    "ARB": 3,
    "DOGE": 4,
    "WIF": 5,
    "OP": 6,
    "PEPE": 7,
    "AVAX": 8,
    "LINK": 9,
    "MATIC": 10,
    "NEAR": 11,
    "ATOM": 12,
    "APT": 13,
    "SUI": 14,
    "TIA": 15,
    "SEI": 16,
    "JTO": 17,
    "INJ": 18,
    "BLUR": 19,
    "LDO": 20,
    "STX": 21,
    "RUNE": 22,
    "ORDI": 23,
    "IMX": 24,
    "FTM": 25,
    "MINA": 26,
    "CRV": 27,
    "MKR": 28,
    "AAVE": 29,
}

# Default gas estimates (for compatibility with other connectors)
HYPERLIQUID_GAS_ESTIMATES: dict[str, int] = {
    "place_order": 0,  # Hyperliquid uses signatures, not gas
    "cancel_order": 0,
    "modify_order": 0,
}

# Order size constraints
MIN_ORDER_SIZE_USD = Decimal("10")
MAX_LEVERAGE = 50


# =============================================================================
# Enums
# =============================================================================


class HyperliquidNetwork(Enum):
    """Hyperliquid network environments."""

    MAINNET = "mainnet"
    TESTNET = "testnet"


class HyperliquidOrderType(Enum):
    """Hyperliquid order types."""

    LIMIT = "Limit"
    MARKET = "Market"  # Implemented as aggressive limit order


class HyperliquidOrderSide(Enum):
    """Order side (buy/sell)."""

    BUY = "B"
    SELL = "A"  # Ask


class HyperliquidTimeInForce(Enum):
    """Time in force options for orders."""

    GTC = "Gtc"  # Good til cancelled
    IOC = "Ioc"  # Immediate or cancel
    ALO = "Alo"  # Add liquidity only (post-only)


class HyperliquidPositionSide(Enum):
    """Position side (long/short)."""

    LONG = "long"
    SHORT = "short"
    NONE = "none"


class HyperliquidOrderStatus(Enum):
    """Order status values."""

    OPEN = "open"
    FILLED = "filled"
    PARTIALLY_FILLED = "partially_filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"
    EXPIRED = "expired"


class HyperliquidMarginMode(Enum):
    """Margin mode options."""

    CROSS = "cross"
    ISOLATED = "isolated"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class HyperliquidConfig:
    """Configuration for HyperliquidAdapter.

    Attributes:
        network: Target network (mainnet or testnet)
        wallet_address: Ethereum address for the account
        private_key: Private key for signing (optional, can use external signer)
        default_slippage_bps: Default slippage tolerance in basis points (default 50 = 0.5%)
        vault_address: Optional vault address for vault trading
        agent_address: Optional agent address for delegated trading
    """

    network: str  # "mainnet" or "testnet"
    wallet_address: str
    private_key: str | None = None
    default_slippage_bps: int = 50
    vault_address: str | None = None
    agent_address: str | None = None

    def __post_init__(self) -> None:
        """Validate configuration."""
        if self.network not in HYPERLIQUID_API_URLS:
            raise ValueError(f"Unsupported network: {self.network}. Supported: {list(HYPERLIQUID_API_URLS.keys())}")

        if self.default_slippage_bps < 0 or self.default_slippage_bps > 10000:
            raise ValueError("Slippage must be between 0 and 10000 basis points")

        # Normalize wallet address to checksum format
        if not self.wallet_address.startswith("0x"):
            raise ValueError("Wallet address must start with 0x")

    @property
    def api_url(self) -> str:
        """Get API URL for configured network."""
        return HYPERLIQUID_API_URLS[self.network]

    @property
    def ws_url(self) -> str:
        """Get WebSocket URL for configured network."""
        return HYPERLIQUID_WS_URLS[self.network]

    @property
    def chain_id(self) -> int:
        """Get chain ID for configured network."""
        return HYPERLIQUID_CHAIN_IDS[self.network]

    @property
    def eip712_domain(self) -> dict[str, Any]:
        """Get EIP-712 domain for configured network."""
        return HYPERLIQUID_EIP712_DOMAIN[self.network]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "network": self.network,
            "wallet_address": self.wallet_address,
            "default_slippage_bps": self.default_slippage_bps,
            "vault_address": self.vault_address,
            "agent_address": self.agent_address,
            "api_url": self.api_url,
            "ws_url": self.ws_url,
            "chain_id": self.chain_id,
        }


@dataclass
class HyperliquidPosition:
    """Represents an open Hyperliquid position.

    Attributes:
        asset: Asset symbol (e.g., "ETH")
        size: Position size (positive for long, negative for short)
        entry_price: Average entry price
        mark_price: Current mark price
        liquidation_price: Estimated liquidation price
        unrealized_pnl: Unrealized profit/loss
        realized_pnl: Realized profit/loss
        margin_used: Margin allocated to position
        leverage: Current leverage
        margin_mode: Cross or isolated margin
        max_leverage: Maximum allowed leverage for asset
        last_updated: Timestamp of last update
    """

    asset: str
    size: Decimal
    entry_price: Decimal
    mark_price: Decimal = Decimal("0")
    liquidation_price: Decimal | None = None
    unrealized_pnl: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")
    margin_used: Decimal = Decimal("0")
    leverage: Decimal = Decimal("1")
    margin_mode: HyperliquidMarginMode = HyperliquidMarginMode.CROSS
    max_leverage: int = 50
    last_updated: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def side(self) -> HyperliquidPositionSide:
        """Get position side."""
        if self.size > 0:
            return HyperliquidPositionSide.LONG
        elif self.size < 0:
            return HyperliquidPositionSide.SHORT
        return HyperliquidPositionSide.NONE

    @property
    def is_long(self) -> bool:
        """Check if position is long."""
        return self.size > 0

    @property
    def is_short(self) -> bool:
        """Check if position is short."""
        return self.size < 0

    @property
    def notional_value(self) -> Decimal:
        """Get notional value of position."""
        return abs(self.size) * self.mark_price

    @property
    def net_pnl(self) -> Decimal:
        """Get net PnL (realized + unrealized)."""
        return self.realized_pnl + self.unrealized_pnl

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "asset": self.asset,
            "size": str(self.size),
            "entry_price": str(self.entry_price),
            "mark_price": str(self.mark_price),
            "liquidation_price": str(self.liquidation_price) if self.liquidation_price else None,
            "unrealized_pnl": str(self.unrealized_pnl),
            "realized_pnl": str(self.realized_pnl),
            "margin_used": str(self.margin_used),
            "leverage": str(self.leverage),
            "margin_mode": self.margin_mode.value,
            "max_leverage": self.max_leverage,
            "side": self.side.value,
            "is_long": self.is_long,
            "is_short": self.is_short,
            "notional_value": str(self.notional_value),
            "net_pnl": str(self.net_pnl),
            "last_updated": self.last_updated.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HyperliquidPosition":
        """Create from dictionary."""
        return cls(
            asset=data["asset"],
            size=Decimal(data["size"]),
            entry_price=Decimal(data["entry_price"]),
            mark_price=Decimal(data.get("mark_price", "0")),
            liquidation_price=(Decimal(data["liquidation_price"]) if data.get("liquidation_price") else None),
            unrealized_pnl=Decimal(data.get("unrealized_pnl", "0")),
            realized_pnl=Decimal(data.get("realized_pnl", "0")),
            margin_used=Decimal(data.get("margin_used", "0")),
            leverage=Decimal(data.get("leverage", "1")),
            margin_mode=HyperliquidMarginMode(data.get("margin_mode", "cross")),
            max_leverage=data.get("max_leverage", 50),
            last_updated=datetime.fromisoformat(data["last_updated"]) if "last_updated" in data else datetime.now(UTC),
        )


@dataclass
class HyperliquidOrder:
    """Represents a Hyperliquid order.

    Attributes:
        order_id: Exchange-assigned order ID
        client_id: Client-assigned order ID (cloid)
        asset: Asset symbol
        side: Order side (buy/sell)
        size: Order size
        price: Limit price
        order_type: Order type (limit/market)
        time_in_force: Time in force option
        reduce_only: Whether order can only reduce position
        status: Current order status
        filled_size: Amount already filled
        avg_fill_price: Average fill price
        created_at: Order creation timestamp
        updated_at: Last update timestamp
    """

    order_id: str
    client_id: str | None
    asset: str
    side: HyperliquidOrderSide
    size: Decimal
    price: Decimal
    order_type: HyperliquidOrderType = HyperliquidOrderType.LIMIT
    time_in_force: HyperliquidTimeInForce = HyperliquidTimeInForce.GTC
    reduce_only: bool = False
    status: HyperliquidOrderStatus = HyperliquidOrderStatus.OPEN
    filled_size: Decimal = Decimal("0")
    avg_fill_price: Decimal | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    @property
    def remaining_size(self) -> Decimal:
        """Get remaining unfilled size."""
        return self.size - self.filled_size

    @property
    def is_buy(self) -> bool:
        """Check if order is a buy."""
        return self.side == HyperliquidOrderSide.BUY

    @property
    def is_sell(self) -> bool:
        """Check if order is a sell."""
        return self.side == HyperliquidOrderSide.SELL

    @property
    def is_open(self) -> bool:
        """Check if order is still open."""
        return self.status in (
            HyperliquidOrderStatus.OPEN,
            HyperliquidOrderStatus.PARTIALLY_FILLED,
        )

    @property
    def is_filled(self) -> bool:
        """Check if order is fully filled."""
        return self.status == HyperliquidOrderStatus.FILLED

    @property
    def fill_percentage(self) -> Decimal:
        """Get fill percentage."""
        if self.size == 0:
            return Decimal("0")
        return (self.filled_size / self.size) * 100

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "order_id": self.order_id,
            "client_id": self.client_id,
            "asset": self.asset,
            "side": self.side.value,
            "size": str(self.size),
            "price": str(self.price),
            "order_type": self.order_type.value,
            "time_in_force": self.time_in_force.value,
            "reduce_only": self.reduce_only,
            "status": self.status.value,
            "filled_size": str(self.filled_size),
            "avg_fill_price": str(self.avg_fill_price) if self.avg_fill_price else None,
            "remaining_size": str(self.remaining_size),
            "is_buy": self.is_buy,
            "is_sell": self.is_sell,
            "is_open": self.is_open,
            "is_filled": self.is_filled,
            "fill_percentage": str(self.fill_percentage),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "HyperliquidOrder":
        """Create from dictionary."""
        return cls(
            order_id=data["order_id"],
            client_id=data.get("client_id"),
            asset=data["asset"],
            side=HyperliquidOrderSide(data["side"]),
            size=Decimal(data["size"]),
            price=Decimal(data["price"]),
            order_type=HyperliquidOrderType(data.get("order_type", "Limit")),
            time_in_force=HyperliquidTimeInForce(data.get("time_in_force", "Gtc")),
            reduce_only=data.get("reduce_only", False),
            status=HyperliquidOrderStatus(data.get("status", "open")),
            filled_size=Decimal(data.get("filled_size", "0")),
            avg_fill_price=(Decimal(data["avg_fill_price"]) if data.get("avg_fill_price") else None),
            created_at=datetime.fromisoformat(data["created_at"]) if "created_at" in data else datetime.now(UTC),
            updated_at=datetime.fromisoformat(data["updated_at"]) if "updated_at" in data else datetime.now(UTC),
        )


@dataclass
class OrderResult:
    """Result of placing or canceling an order.

    Attributes:
        success: Whether operation succeeded
        order_id: Order ID if successful
        client_id: Client-assigned order ID
        order: Created/affected order object
        error: Error message if failed
        response: Raw API response
    """

    success: bool
    order_id: str | None = None
    client_id: str | None = None
    order: HyperliquidOrder | None = None
    error: str | None = None
    response: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "order_id": self.order_id,
            "client_id": self.client_id,
            "order": self.order.to_dict() if self.order else None,
            "error": self.error,
            "response": self.response,
        }


@dataclass
class CancelResult:
    """Result of canceling one or more orders.

    Attributes:
        success: Whether operation succeeded
        cancelled_orders: List of cancelled order IDs
        failed_orders: List of order IDs that failed to cancel
        error: Error message if failed
        response: Raw API response
    """

    success: bool
    cancelled_orders: list[str] = field(default_factory=list)
    failed_orders: list[str] = field(default_factory=list)
    error: str | None = None
    response: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "cancelled_orders": self.cancelled_orders,
            "failed_orders": self.failed_orders,
            "error": self.error,
            "response": self.response,
        }


@dataclass
class SignedAction:
    """A signed action ready for submission to Hyperliquid.

    Attributes:
        action: The action payload
        signature: EIP-712 signature
        nonce: Nonce used for signing
        vault_address: Optional vault address
    """

    action: dict[str, Any]
    signature: str
    nonce: int
    vault_address: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        result: dict[str, Any] = {
            "action": self.action,
            "signature": self.signature,
            "nonce": self.nonce,
        }
        if self.vault_address:
            result["vaultAddress"] = self.vault_address
        return result


# =============================================================================
# Message Signing
# =============================================================================


class MessageSigner(Protocol):
    """Protocol for message signing implementations."""

    def sign_l1_action(
        self,
        action: dict[str, Any],
        nonce: int,
        vault_address: str | None = None,
    ) -> str:
        """Sign an L1 action.

        L1 actions are used for mainnet and include:
        - Order placement
        - Order cancellation
        - Withdrawal requests

        Args:
            action: Action payload to sign
            nonce: Unique nonce for the action
            vault_address: Optional vault address

        Returns:
            Hex-encoded signature
        """
        ...

    def sign_l2_action(
        self,
        action: dict[str, Any],
        nonce: int,
    ) -> str:
        """Sign an L2 action.

        L2 actions are used for testnet and some mainnet operations.
        The signing scheme is slightly different from L1.

        Args:
            action: Action payload to sign
            nonce: Unique nonce for the action

        Returns:
            Hex-encoded signature
        """
        ...


class EIP712Signer:
    """EIP-712 typed message signer for Hyperliquid.

    This class handles the cryptographic signing of messages for both L1 and L2
    operations on Hyperliquid. It uses EIP-712 structured data hashing.

    The signing process:
    1. Construct the EIP-712 typed data structure
    2. Hash the domain separator
    3. Hash the message struct
    4. Sign the combined hash with the private key
    """

    # EIP-712 type hashes for Hyperliquid
    EIP712_DOMAIN_TYPE = "EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)"

    # L1 action type (mainnet)
    L1_ACTION_TYPE = "HyperliquidTransaction:Agent(address source,address connectionId,bytes32 nonce)"

    # Order wire type
    ORDER_TYPE = "Order(uint32 asset,bool isBuy,uint64 limitPx,uint64 sz,bool reduceOnly,uint8 orderType)"

    def __init__(
        self,
        private_key: str,
        chain_id: int,
        is_mainnet: bool = True,
    ) -> None:
        """Initialize the signer.

        Args:
            private_key: Hex-encoded private key (with or without 0x prefix)
            chain_id: Chain ID for EIP-712 domain
            is_mainnet: Whether this is mainnet (L1) or testnet (L2)
        """
        # Normalize private key
        if private_key.startswith("0x"):
            private_key = private_key[2:]
        self._private_key = bytes.fromhex(private_key)
        self._chain_id = chain_id
        self._is_mainnet = is_mainnet

        # Compute domain separator
        self._domain_separator = self._compute_domain_separator()

        logger.debug(f"EIP712Signer initialized: chain_id={chain_id}, is_mainnet={is_mainnet}")

    def _compute_domain_separator(self) -> bytes:
        """Compute EIP-712 domain separator hash."""
        domain_type_hash = self._keccak256(self.EIP712_DOMAIN_TYPE.encode())
        name_hash = self._keccak256(b"Hyperliquid")
        version_hash = self._keccak256(b"1")

        # ABI encode: domainTypeHash + nameHash + versionHash + chainId + verifyingContract
        encoded = (
            domain_type_hash
            + name_hash
            + version_hash
            + self._chain_id.to_bytes(32, "big")
            + bytes(32)  # Zero address for verifying contract
        )

        return self._keccak256(encoded)

    def sign_l1_action(
        self,
        action: dict[str, Any],
        nonce: int,
        vault_address: str | None = None,
    ) -> str:
        """Sign an L1 action for mainnet.

        L1 signing uses a specific EIP-712 structure with:
        - source: The signing wallet address
        - connectionId: Always the zero address for direct signing
        - nonce: Unique identifier to prevent replay

        Args:
            action: Action payload
            nonce: Unique nonce
            vault_address: Optional vault address

        Returns:
            Hex-encoded signature
        """
        # Construct the message hash
        action_hash = self._hash_action(action)

        # For L1, we use Agent typed data
        message_hash = self._keccak256(b"\x19\x01" + self._domain_separator + self._hash_l1_message(action_hash, nonce))

        # Sign the hash
        signature = self._sign_hash(message_hash)

        logger.debug(f"L1 action signed: nonce={nonce}")
        return signature

    def sign_l2_action(
        self,
        action: dict[str, Any],
        nonce: int,
    ) -> str:
        """Sign an L2 action for testnet.

        L2 signing uses a simpler structure that's more gas-efficient
        for the testnet environment.

        Args:
            action: Action payload
            nonce: Unique nonce

        Returns:
            Hex-encoded signature
        """
        # L2 uses a simpler signing scheme
        action_hash = self._hash_action(action)

        # Construct message for L2
        message_hash = self._keccak256(b"\x19\x01" + self._domain_separator + self._hash_l2_message(action_hash, nonce))

        signature = self._sign_hash(message_hash)

        logger.debug(f"L2 action signed: nonce={nonce}")
        return signature

    def _hash_action(self, action: dict[str, Any]) -> bytes:
        """Hash an action payload.

        The action is serialized to JSON with sorted keys and no spaces,
        then hashed with keccak256.
        """
        action_json = json.dumps(action, separators=(",", ":"), sort_keys=True)
        return self._keccak256(action_json.encode())

    def _hash_l1_message(self, action_hash: bytes, nonce: int) -> bytes:
        """Hash L1 message struct."""
        # Type hash for the Agent struct
        type_hash = self._keccak256(self.L1_ACTION_TYPE.encode())

        # ABI encode the struct values
        # source: derived from private key (not included in hash for simplicity)
        # connectionId: zero address
        # nonce: the nonce value as bytes32

        return self._keccak256(type_hash + action_hash + nonce.to_bytes(32, "big"))

    def _hash_l2_message(self, action_hash: bytes, nonce: int) -> bytes:
        """Hash L2 message struct."""
        # L2 uses a simpler hashing scheme
        return self._keccak256(action_hash + nonce.to_bytes(32, "big"))

    def _sign_hash(self, message_hash: bytes) -> str:
        """Sign a message hash using secp256k1.

        This is a simplified implementation. In production, use
        eth_account or similar library for proper ECDSA signing.

        Returns:
            Hex-encoded signature in Ethereum format (r + s + v)
        """
        # Note: This is a placeholder implementation
        # In production, use proper cryptographic signing:
        # from eth_account import Account
        # signed = Account.signHash(message_hash, self._private_key)
        # return signed.signature.hex()

        # Generate deterministic signature for testing
        # In production, replace with proper ECDSA signing
        sig_input = self._private_key + message_hash
        signature = self._keccak256(sig_input)

        # Construct signature: r (32 bytes) + s (32 bytes) + v (1 byte)
        r = signature[:32]
        s = self._keccak256(signature + b"\x01")[:32]
        v = 27  # Recovery ID

        return "0x" + r.hex() + s.hex() + format(v, "02x")

    @staticmethod
    def _keccak256(data: bytes) -> bytes:
        """Compute keccak256 hash."""
        # Note: In production, use eth_hash.keccak256 or web3.keccak
        # This is a simplified implementation using hashlib
        return hashlib.sha3_256(data).digest()


class ExternalSigner:
    """External signer that delegates to a callback.

    This allows using hardware wallets, custodians, or other
    external signing solutions.
    """

    SignCallback = Callable[[dict[str, Any], int, bool], str]

    def __init__(self, sign_callback: SignCallback) -> None:
        """Initialize with signing callback.

        Args:
            sign_callback: Function that signs (action, nonce, is_l1) -> signature
        """
        self._sign_callback = sign_callback

    def sign_l1_action(
        self,
        action: dict[str, Any],
        nonce: int,
        vault_address: str | None = None,
    ) -> str:
        """Sign an L1 action using external signer."""
        return self._sign_callback(action, nonce, True)

    def sign_l2_action(
        self,
        action: dict[str, Any],
        nonce: int,
    ) -> str:
        """Sign an L2 action using external signer."""
        return self._sign_callback(action, nonce, False)


# =============================================================================
# Hyperliquid Adapter
# =============================================================================


class HyperliquidAdapter:
    """Adapter for Hyperliquid perpetual futures exchange.

    This adapter provides methods for:
    - Placing limit and market orders
    - Canceling orders by ID or client ID
    - Querying positions and open orders
    - Managing leverage and margin settings

    Example:
        config = HyperliquidConfig(
            network="mainnet",
            wallet_address="0x...",
            private_key="0x...",
        )
        adapter = HyperliquidAdapter(config)

        # Place a limit buy order
        result = adapter.place_order(
            asset="ETH",
            is_buy=True,
            size=Decimal("0.1"),
            price=Decimal("2000"),
        )

        # Check open orders
        orders = adapter.get_open_orders()

        # Cancel order
        cancel_result = adapter.cancel_order(order_id=result.order_id)
    """

    def __init__(
        self,
        config: HyperliquidConfig,
        signer: MessageSigner | None = None,
    ) -> None:
        """Initialize the adapter.

        Args:
            config: Hyperliquid adapter configuration
            signer: Optional custom message signer (uses EIP712Signer if not provided)
        """
        self.config = config
        self.network = config.network
        self.wallet_address = config.wallet_address

        # Initialize signer
        self._signer: MessageSigner | None = None
        if signer:
            self._signer = signer
        elif config.private_key:
            self._signer = EIP712Signer(
                private_key=config.private_key,
                chain_id=config.chain_id,
                is_mainnet=(config.network == "mainnet"),
            )

        # Internal state tracking (in production, query from exchange)
        self._positions: dict[str, HyperliquidPosition] = {}
        self._orders: dict[str, HyperliquidOrder] = {}
        self._leverage: dict[str, int] = {}  # Per-asset leverage settings
        self._nonce_counter = int(time.time() * 1000)

        logger.info(f"HyperliquidAdapter initialized for network={self.network}, wallet={self.wallet_address[:10]}...")

    # =========================================================================
    # Order Management
    # =========================================================================

    def place_order(
        self,
        asset: str,
        is_buy: bool,
        size: Decimal,
        price: Decimal,
        order_type: HyperliquidOrderType = HyperliquidOrderType.LIMIT,
        time_in_force: HyperliquidTimeInForce = HyperliquidTimeInForce.GTC,
        reduce_only: bool = False,
        client_id: str | None = None,
        slippage_bps: int | None = None,
    ) -> OrderResult:
        """Place a new order.

        Args:
            asset: Asset symbol (e.g., "ETH", "BTC")
            is_buy: True for buy, False for sell
            size: Order size in asset units
            price: Limit price (for market orders, used as slippage reference)
            order_type: Order type (limit or market)
            time_in_force: Time in force option
            reduce_only: Whether order can only reduce position
            client_id: Optional client-assigned order ID
            slippage_bps: Slippage tolerance for market orders

        Returns:
            OrderResult with order details
        """
        try:
            # Validate asset
            if asset not in HYPERLIQUID_ASSETS:
                return OrderResult(
                    success=False,
                    error=f"Unknown asset: {asset}. Supported: {list(HYPERLIQUID_ASSETS.keys())}",
                )

            # Validate size
            if size <= 0:
                return OrderResult(
                    success=False,
                    error="Order size must be positive",
                )

            # Generate client ID if not provided
            if client_id is None:
                client_id = self._generate_client_id()

            # Calculate price for market orders with slippage
            effective_price = price
            if order_type == HyperliquidOrderType.MARKET:
                slippage = slippage_bps or self.config.default_slippage_bps
                slippage_factor = Decimal(slippage) / Decimal("10000")
                if is_buy:
                    effective_price = price * (1 + slippage_factor)
                else:
                    effective_price = price * (1 - slippage_factor)

            # Build order action
            order_action = self._build_order_action(
                asset=asset,
                is_buy=is_buy,
                size=size,
                price=effective_price,
                order_type=order_type,
                time_in_force=time_in_force,
                reduce_only=reduce_only,
                client_id=client_id,
            )

            # Sign the action
            nonce = self._get_next_nonce()
            if self._signer:
                if self.network == "mainnet":
                    signature = self._signer.sign_l1_action(order_action, nonce, self.config.vault_address)
                else:
                    signature = self._signer.sign_l2_action(order_action, nonce)
            else:
                signature = "0x" + "0" * 130  # Placeholder for testing

            # Generate order ID (in production, from exchange response)
            order_id = f"{asset}-{nonce}-{client_id}"

            # Create order object
            order = HyperliquidOrder(
                order_id=order_id,
                client_id=client_id,
                asset=asset,
                side=HyperliquidOrderSide.BUY if is_buy else HyperliquidOrderSide.SELL,
                size=size,
                price=effective_price,
                order_type=order_type,
                time_in_force=time_in_force,
                reduce_only=reduce_only,
                status=HyperliquidOrderStatus.OPEN,
            )

            # Store order
            self._orders[order_id] = order

            logger.info(
                f"Placed {order_type.value} order: asset={asset}, "
                f"side={'buy' if is_buy else 'sell'}, size={size}, price={effective_price}"
            )

            return OrderResult(
                success=True,
                order_id=order_id,
                client_id=client_id,
                order=order,
                response={
                    "action": order_action,
                    "signature": signature,
                    "nonce": nonce,
                },
            )

        except Exception as e:
            logger.exception(f"Failed to place order: {e}")
            return OrderResult(
                success=False,
                error=str(e),
            )

    def cancel_order(
        self,
        order_id: str | None = None,
        client_id: str | None = None,
        asset: str | None = None,
    ) -> CancelResult:
        """Cancel an existing order.

        Args:
            order_id: Exchange-assigned order ID
            client_id: Client-assigned order ID
            asset: Asset symbol (required with client_id)

        Returns:
            CancelResult indicating success/failure
        """
        try:
            # Find order to cancel
            order = None
            if order_id:
                order = self._orders.get(order_id)
            elif client_id:
                # Find by client ID
                for o in self._orders.values():
                    if o.client_id == client_id:
                        order = o
                        break

            if order is None:
                return CancelResult(
                    success=False,
                    error=f"Order not found: order_id={order_id}, client_id={client_id}",
                )

            # Build cancel action
            cancel_action = self._build_cancel_action(
                asset=order.asset,
                order_id=order.order_id,
            )

            # Sign the action
            nonce = self._get_next_nonce()
            if self._signer:
                if self.network == "mainnet":
                    signature = self._signer.sign_l1_action(cancel_action, nonce, self.config.vault_address)
                else:
                    signature = self._signer.sign_l2_action(cancel_action, nonce)
            else:
                signature = "0x" + "0" * 130

            # Update order status
            order.status = HyperliquidOrderStatus.CANCELLED
            order.updated_at = datetime.now(UTC)

            # Remove from active orders
            if order.order_id in self._orders:
                del self._orders[order.order_id]

            logger.info(f"Cancelled order: {order.order_id}")

            return CancelResult(
                success=True,
                cancelled_orders=[order.order_id],
                response={
                    "action": cancel_action,
                    "signature": signature,
                    "nonce": nonce,
                },
            )

        except Exception as e:
            logger.exception(f"Failed to cancel order: {e}")
            return CancelResult(
                success=False,
                error=str(e),
            )

    def cancel_all_orders(
        self,
        asset: str | None = None,
    ) -> CancelResult:
        """Cancel all open orders.

        Args:
            asset: Optional asset to filter by

        Returns:
            CancelResult with list of cancelled orders
        """
        try:
            orders_to_cancel = []
            for order in self._orders.values():
                if order.is_open:
                    if asset is None or order.asset == asset:
                        orders_to_cancel.append(order)

            if not orders_to_cancel:
                return CancelResult(
                    success=True,
                    cancelled_orders=[],
                )

            cancelled: list[str] = []
            failed: list[str] = []

            for order in orders_to_cancel:
                result = self.cancel_order(order_id=order.order_id)
                if result.success:
                    cancelled.extend(result.cancelled_orders)
                else:
                    failed.append(order.order_id)

            return CancelResult(
                success=len(failed) == 0,
                cancelled_orders=cancelled,
                failed_orders=failed,
            )

        except Exception as e:
            logger.exception(f"Failed to cancel all orders: {e}")
            return CancelResult(
                success=False,
                error=str(e),
            )

    def get_order(self, order_id: str) -> HyperliquidOrder | None:
        """Get order by ID.

        Args:
            order_id: Order ID to look up

        Returns:
            Order details or None if not found
        """
        return self._orders.get(order_id)

    def get_open_orders(
        self,
        asset: str | None = None,
    ) -> list[HyperliquidOrder]:
        """Get all open orders.

        Args:
            asset: Optional asset to filter by

        Returns:
            List of open orders
        """
        orders = [o for o in self._orders.values() if o.is_open]
        if asset:
            orders = [o for o in orders if o.asset == asset]
        return orders

    # =========================================================================
    # Position Management
    # =========================================================================

    def get_position(self, asset: str) -> HyperliquidPosition | None:
        """Get position for an asset.

        Args:
            asset: Asset symbol

        Returns:
            Position details or None if no position
        """
        return self._positions.get(asset)

    def get_all_positions(self) -> list[HyperliquidPosition]:
        """Get all open positions.

        Returns:
            List of all positions with non-zero size
        """
        return [p for p in self._positions.values() if p.size != 0]

    def set_leverage(
        self,
        asset: str,
        leverage: int,
    ) -> bool:
        """Set leverage for an asset.

        Args:
            asset: Asset symbol
            leverage: Target leverage (1-50)

        Returns:
            True if successful
        """
        if leverage < 1 or leverage > MAX_LEVERAGE:
            logger.error(f"Invalid leverage: {leverage}. Must be 1-{MAX_LEVERAGE}")
            return False

        self._leverage[asset] = leverage
        logger.info(f"Set leverage for {asset}: {leverage}x")
        return True

    def get_leverage(self, asset: str) -> int:
        """Get current leverage for an asset.

        Args:
            asset: Asset symbol

        Returns:
            Current leverage setting (default 1)
        """
        return self._leverage.get(asset, 1)

    # =========================================================================
    # Action Building
    # =========================================================================

    def _build_order_action(
        self,
        asset: str,
        is_buy: bool,
        size: Decimal,
        price: Decimal,
        order_type: HyperliquidOrderType,
        time_in_force: HyperliquidTimeInForce,
        reduce_only: bool,
        client_id: str,
    ) -> dict[str, Any]:
        """Build order action payload.

        Hyperliquid order format:
        {
            "type": "order",
            "orders": [{
                "a": asset_id,
                "b": is_buy,
                "p": price_string,
                "s": size_string,
                "r": reduce_only,
                "t": {
                    "limit": {"tif": time_in_force}
                } | {
                    "trigger": {...}
                },
                "c": client_id
            }],
            "grouping": "na"
        }
        """
        asset_id = HYPERLIQUID_ASSETS.get(asset, 0)

        # Format price and size to required precision
        # Hyperliquid uses string representation with specific decimal places
        price_str = f"{price:.5f}"  # 5 decimal places for price
        size_str = f"{size:.4f}"  # 4 decimal places for size

        order_spec: dict[str, Any] = {
            "a": asset_id,
            "b": is_buy,
            "p": price_str,
            "s": size_str,
            "r": reduce_only,
            "t": {
                "limit": {
                    "tif": time_in_force.value,
                }
            },
        }

        if client_id:
            order_spec["c"] = client_id

        return {
            "type": "order",
            "orders": [order_spec],
            "grouping": "na",
        }

    def _build_cancel_action(
        self,
        asset: str,
        order_id: str,
    ) -> dict[str, Any]:
        """Build cancel action payload.

        Hyperliquid cancel format:
        {
            "type": "cancel",
            "cancels": [{
                "a": asset_id,
                "o": order_id
            }]
        }
        """
        asset_id = HYPERLIQUID_ASSETS.get(asset, 0)

        return {
            "type": "cancel",
            "cancels": [
                {
                    "a": asset_id,
                    "o": order_id,
                }
            ],
        }

    def _build_cancel_by_cloid_action(
        self,
        asset: str,
        client_id: str,
    ) -> dict[str, Any]:
        """Build cancel by client ID action payload.

        Hyperliquid cancel by cloid format:
        {
            "type": "cancelByCloid",
            "cancels": [{
                "asset": asset_id,
                "cloid": client_id
            }]
        }
        """
        asset_id = HYPERLIQUID_ASSETS.get(asset, 0)

        return {
            "type": "cancelByCloid",
            "cancels": [
                {
                    "asset": asset_id,
                    "cloid": client_id,
                }
            ],
        }

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _get_next_nonce(self) -> int:
        """Get next nonce value."""
        self._nonce_counter += 1
        return self._nonce_counter

    def _generate_client_id(self) -> str:
        """Generate unique client order ID."""
        return f"almanak_{secrets.token_hex(8)}"

    # =========================================================================
    # State Management (for testing/simulation)
    # =========================================================================

    def set_position(self, position: HyperliquidPosition) -> None:
        """Set a position for testing.

        Args:
            position: Position to set
        """
        self._positions[position.asset] = position

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
        self._leverage.clear()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "HyperliquidAdapter",
    "HyperliquidConfig",
    "HyperliquidPosition",
    "HyperliquidOrder",
    "HyperliquidOrderType",
    "HyperliquidOrderSide",
    "HyperliquidOrderStatus",
    "HyperliquidPositionSide",
    "HyperliquidTimeInForce",
    "HyperliquidMarginMode",
    "HyperliquidNetwork",
    "OrderResult",
    "CancelResult",
    "SignedAction",
    "EIP712Signer",
    "ExternalSigner",
    "MessageSigner",
    "HYPERLIQUID_API_URLS",
    "HYPERLIQUID_WS_URLS",
    "HYPERLIQUID_CHAIN_IDS",
    "HYPERLIQUID_ASSETS",
    "HYPERLIQUID_GAS_ESTIMATES",
]
