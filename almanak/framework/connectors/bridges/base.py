"""Bridge Adapter Abstract Base Class.

This module provides the abstract base class for bridge protocol integrations,
defining a standard interface for cross-chain asset transfers.

Bridge adapters handle:
- Quote retrieval (fees, estimated time, routes)
- Transaction building for deposits
- Status tracking of in-flight transfers
- Completion time estimation

Supported patterns:
- Lock-and-mint bridges (e.g., native bridges)
- Liquidity bridges (e.g., Across, Stargate)
- Message-passing bridges (e.g., LayerZero)

Example:
    class MyBridgeAdapter(BridgeAdapter):
        @property
        def name(self) -> str:
            return "MyBridge"

        def get_quote(self, token, amount, from_chain, to_chain, max_slippage):
            # Implementation
            pass
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any

# =============================================================================
# Exceptions
# =============================================================================


class BridgeError(Exception):
    """Base exception for bridge-related errors."""

    pass


class BridgeQuoteError(BridgeError):
    """Error when retrieving a bridge quote."""

    pass


class BridgeTransactionError(BridgeError):
    """Error when building or submitting a bridge transaction."""

    pass


class BridgeStatusError(BridgeError):
    """Error when checking bridge transfer status."""

    pass


# =============================================================================
# Enums
# =============================================================================


class BridgeStatusEnum(Enum):
    """Status of a bridge transfer.

    States:
        PENDING: Transfer initiated but not yet detected on source chain
        DEPOSITED: Deposit confirmed on source chain
        IN_FLIGHT: Transfer in progress (relaying/bridging)
        FILLED: Destination chain credit detected, awaiting confirmations
        COMPLETED: Transfer fully completed and confirmed
        FAILED: Transfer failed (may need manual intervention)
        EXPIRED: Quote expired before execution
        REFUNDED: Transfer refunded on source chain
    """

    PENDING = "pending"
    DEPOSITED = "deposited"
    IN_FLIGHT = "in_flight"
    FILLED = "filled"
    COMPLETED = "completed"
    FAILED = "failed"
    EXPIRED = "expired"
    REFUNDED = "refunded"


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class BridgeRoute:
    """Represents a bridge route between two chains.

    Attributes:
        from_chain: Source chain identifier
        to_chain: Destination chain identifier
        tokens: List of tokens supported on this route
        min_amount: Minimum transfer amount (in token units)
        max_amount: Maximum transfer amount (in token units)
        estimated_time_seconds: Typical completion time
        is_active: Whether route is currently active
    """

    from_chain: str
    to_chain: str
    tokens: list[str] = field(default_factory=list)
    min_amount: Decimal = Decimal("0")
    max_amount: Decimal = Decimal("0")  # 0 = unlimited
    estimated_time_seconds: int = 300  # 5 minutes default
    is_active: bool = True

    def supports_token(self, token: str) -> bool:
        """Check if route supports a specific token.

        Args:
            token: Token symbol to check

        Returns:
            True if token is supported on this route
        """
        return token.upper() in [t.upper() for t in self.tokens]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "from_chain": self.from_chain,
            "to_chain": self.to_chain,
            "tokens": self.tokens,
            "min_amount": str(self.min_amount),
            "max_amount": str(self.max_amount),
            "estimated_time_seconds": self.estimated_time_seconds,
            "is_active": self.is_active,
        }


@dataclass
class BridgeQuote:
    """Quote for a bridge transfer.

    Contains all information needed to execute a bridge transfer,
    including fees, timing, and the resulting amount on destination.

    Attributes:
        bridge_name: Name of the bridge providing this quote
        token: Token being bridged
        input_amount: Amount being sent from source chain
        output_amount: Expected amount on destination (after fees)
        from_chain: Source chain identifier
        to_chain: Destination chain identifier
        fee_amount: Total fee in token units
        fee_usd: Total fee in USD (if available)
        gas_fee_amount: Gas fee portion (in native token)
        relayer_fee_amount: Relayer/protocol fee portion (in bridged token)
        estimated_time_seconds: Estimated completion time in seconds
        quote_timestamp: When quote was generated
        expires_at: When quote expires
        slippage_tolerance: Maximum slippage as decimal (e.g., 0.005 = 0.5%)
        route_data: Bridge-specific route information
        quote_id: Bridge-specific quote identifier (if any)
    """

    bridge_name: str
    token: str
    input_amount: Decimal
    output_amount: Decimal
    from_chain: str
    to_chain: str
    fee_amount: Decimal
    fee_usd: Decimal | None = None
    gas_fee_amount: Decimal = Decimal("0")
    relayer_fee_amount: Decimal = Decimal("0")
    estimated_time_seconds: int = 300
    quote_timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime | None = None
    slippage_tolerance: Decimal = Decimal("0.005")  # 0.5% default
    route_data: dict[str, Any] = field(default_factory=dict)
    quote_id: str | None = None

    def __post_init__(self) -> None:
        """Set default expiration if not provided."""
        if self.expires_at is None:
            # Default 5 minute expiration
            self.expires_at = self.quote_timestamp + timedelta(minutes=5)

    @property
    def fee_percentage(self) -> Decimal:
        """Get fee as percentage of input amount."""
        if self.input_amount <= 0:
            return Decimal("0")
        return (self.fee_amount / self.input_amount) * 100

    @property
    def is_expired(self) -> bool:
        """Check if quote has expired."""
        if self.expires_at is None:
            return False
        return datetime.now(UTC) > self.expires_at

    @property
    def time_until_expiry(self) -> timedelta | None:
        """Get time until quote expires."""
        if self.expires_at is None:
            return None
        return self.expires_at - datetime.now(UTC)

    @property
    def estimated_completion_time(self) -> datetime:
        """Get estimated completion timestamp."""
        return datetime.now(UTC) + timedelta(seconds=self.estimated_time_seconds)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "bridge_name": self.bridge_name,
            "token": self.token,
            "input_amount": str(self.input_amount),
            "output_amount": str(self.output_amount),
            "from_chain": self.from_chain,
            "to_chain": self.to_chain,
            "fee_amount": str(self.fee_amount),
            "fee_usd": str(self.fee_usd) if self.fee_usd else None,
            "fee_percentage": str(self.fee_percentage),
            "gas_fee_amount": str(self.gas_fee_amount),
            "relayer_fee_amount": str(self.relayer_fee_amount),
            "estimated_time_seconds": self.estimated_time_seconds,
            "quote_timestamp": self.quote_timestamp.isoformat(),
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "is_expired": self.is_expired,
            "slippage_tolerance": str(self.slippage_tolerance),
            "route_data": self.route_data,
            "quote_id": self.quote_id,
        }


@dataclass
class BridgeStatus:
    """Status of a bridge transfer.

    Tracks the progress of an in-flight bridge transfer including
    source and destination chain transaction details.

    Attributes:
        bridge_name: Name of the bridge
        bridge_deposit_id: Bridge-specific deposit identifier
        status: Current status of the transfer
        from_chain: Source chain identifier
        to_chain: Destination chain identifier
        token: Token being bridged
        input_amount: Amount sent from source chain
        output_amount: Amount received on destination (if known)
        source_tx_hash: Transaction hash on source chain
        destination_tx_hash: Transaction hash on destination chain (if complete)
        deposited_at: When deposit was confirmed on source
        filled_at: When fill was detected on destination
        completed_at: When transfer was fully completed
        error_message: Error details if failed
        relay_id: Relayer-specific identifier (if applicable)
        fill_deadline: Deadline for fill (for optimistic bridges)
    """

    bridge_name: str
    bridge_deposit_id: str
    status: BridgeStatusEnum
    from_chain: str
    to_chain: str
    token: str
    input_amount: Decimal
    output_amount: Decimal | None = None
    source_tx_hash: str | None = None
    destination_tx_hash: str | None = None
    deposited_at: datetime | None = None
    filled_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None
    relay_id: str | None = None
    fill_deadline: datetime | None = None

    @property
    def is_complete(self) -> bool:
        """Check if transfer is complete (success or failure)."""
        return self.status in (
            BridgeStatusEnum.COMPLETED,
            BridgeStatusEnum.FAILED,
            BridgeStatusEnum.EXPIRED,
            BridgeStatusEnum.REFUNDED,
        )

    @property
    def is_success(self) -> bool:
        """Check if transfer completed successfully."""
        return self.status == BridgeStatusEnum.COMPLETED

    @property
    def is_pending(self) -> bool:
        """Check if transfer is still in progress."""
        return self.status in (
            BridgeStatusEnum.PENDING,
            BridgeStatusEnum.DEPOSITED,
            BridgeStatusEnum.IN_FLIGHT,
            BridgeStatusEnum.FILLED,
        )

    @property
    def elapsed_time(self) -> timedelta | None:
        """Get elapsed time since deposit."""
        if self.deposited_at is None:
            return None
        end_time = self.completed_at or datetime.now(UTC)
        return end_time - self.deposited_at

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "bridge_name": self.bridge_name,
            "bridge_deposit_id": self.bridge_deposit_id,
            "status": self.status.value,
            "from_chain": self.from_chain,
            "to_chain": self.to_chain,
            "token": self.token,
            "input_amount": str(self.input_amount),
            "output_amount": str(self.output_amount) if self.output_amount else None,
            "source_tx_hash": self.source_tx_hash,
            "destination_tx_hash": self.destination_tx_hash,
            "deposited_at": self.deposited_at.isoformat() if self.deposited_at else None,
            "filled_at": self.filled_at.isoformat() if self.filled_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "error_message": self.error_message,
            "relay_id": self.relay_id,
            "fill_deadline": self.fill_deadline.isoformat() if self.fill_deadline else None,
            "is_complete": self.is_complete,
            "is_success": self.is_success,
            "elapsed_time_seconds": self.elapsed_time.total_seconds() if self.elapsed_time else None,
        }


# =============================================================================
# Abstract Base Class
# =============================================================================


class BridgeAdapter(ABC):
    """Abstract base class for bridge protocol adapters.

    All bridge adapters must implement this interface to provide
    a consistent API for cross-chain asset transfers.

    Bridge adapters handle:
    1. Quote retrieval - Get fee and time estimates for a transfer
    2. Transaction building - Build the deposit transaction
    3. Status tracking - Poll for transfer completion
    4. Time estimation - Estimate completion times for routes

    Example implementation:
        class AcrossBridgeAdapter(BridgeAdapter):
            @property
            def name(self) -> str:
                return "Across"

            @property
            def supported_tokens(self) -> list[str]:
                return ["ETH", "USDC", "WBTC"]

            def get_quote(self, token, amount, from_chain, to_chain, max_slippage):
                # Call Across API for quote
                pass

            def build_deposit_tx(self, quote, recipient):
                # Build deposit transaction
                pass
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the bridge adapter name.

        Returns:
            Human-readable name of the bridge (e.g., "Across", "Stargate")
        """
        pass

    @property
    @abstractmethod
    def supported_tokens(self) -> list[str]:
        """Get list of supported tokens.

        Returns:
            List of token symbols supported by this bridge
            (e.g., ["ETH", "USDC", "WBTC"])
        """
        pass

    @property
    @abstractmethod
    def supported_routes(self) -> list[BridgeRoute]:
        """Get list of supported bridge routes.

        Returns:
            List of BridgeRoute objects describing supported
            chain-to-chain routes with their tokens and limits
        """
        pass

    @abstractmethod
    def get_quote(
        self,
        token: str,
        amount: Decimal,
        from_chain: str,
        to_chain: str,
        max_slippage: Decimal = Decimal("0.005"),
    ) -> BridgeQuote:
        """Get a quote for bridging tokens.

        Retrieves fee and timing information for a potential bridge transfer.
        The quote contains all information needed to execute the transfer.

        Args:
            token: Token symbol to bridge (e.g., "ETH", "USDC")
            amount: Amount to bridge in token units
            from_chain: Source chain identifier (e.g., "arbitrum", "optimism")
            to_chain: Destination chain identifier
            max_slippage: Maximum slippage tolerance as decimal
                          (e.g., 0.005 = 0.5%, 0.01 = 1%)

        Returns:
            BridgeQuote with fee, timing, and route information

        Raises:
            BridgeQuoteError: If quote cannot be retrieved (unsupported route,
                              amount out of range, API error, etc.)
        """
        pass

    @abstractmethod
    def build_deposit_tx(
        self,
        quote: BridgeQuote,
        recipient: str,
    ) -> dict[str, Any]:
        """Build the deposit transaction for a bridge transfer.

        Creates the transaction data needed to initiate the bridge transfer
        on the source chain.

        Args:
            quote: BridgeQuote from get_quote()
            recipient: Address to receive tokens on destination chain

        Returns:
            Transaction data dict with:
                - to: Contract address to call
                - value: ETH value to send (for native transfers)
                - data: Encoded calldata

        Raises:
            BridgeTransactionError: If transaction cannot be built
                                   (quote expired, invalid recipient, etc.)
        """
        pass

    @abstractmethod
    def check_status(
        self,
        bridge_deposit_id: str,
    ) -> BridgeStatus:
        """Check the status of a bridge transfer.

        Polls the bridge for the current status of an in-flight transfer.

        Args:
            bridge_deposit_id: Bridge-specific deposit identifier
                               (returned from deposit transaction or
                               derived from source tx hash)

        Returns:
            BridgeStatus with current transfer status and transaction details

        Raises:
            BridgeStatusError: If status cannot be retrieved
                              (unknown deposit ID, API error, etc.)
        """
        pass

    @abstractmethod
    def estimate_completion_time(
        self,
        from_chain: str,
        to_chain: str,
    ) -> int:
        """Estimate completion time for a route.

        Returns the typical completion time in seconds for a bridge
        transfer between two chains.

        Args:
            from_chain: Source chain identifier
            to_chain: Destination chain identifier

        Returns:
            Estimated completion time in seconds

        Raises:
            BridgeError: If route is not supported
        """
        pass

    # =========================================================================
    # Helper Methods (optional implementation)
    # =========================================================================

    def supports_token(self, token: str) -> bool:
        """Check if bridge supports a token.

        Args:
            token: Token symbol to check

        Returns:
            True if token is supported
        """
        return token.upper() in [t.upper() for t in self.supported_tokens]

    def supports_route(self, from_chain: str, to_chain: str) -> bool:
        """Check if bridge supports a route.

        Args:
            from_chain: Source chain identifier
            to_chain: Destination chain identifier

        Returns:
            True if route is supported
        """
        for route in self.supported_routes:
            if (
                route.from_chain.lower() == from_chain.lower()
                and route.to_chain.lower() == to_chain.lower()
                and route.is_active
            ):
                return True
        return False

    def get_route(self, from_chain: str, to_chain: str) -> BridgeRoute | None:
        """Get route information.

        Args:
            from_chain: Source chain identifier
            to_chain: Destination chain identifier

        Returns:
            BridgeRoute if found, None otherwise
        """
        for route in self.supported_routes:
            if route.from_chain.lower() == from_chain.lower() and route.to_chain.lower() == to_chain.lower():
                return route
        return None

    def validate_transfer(
        self,
        token: str,
        amount: Decimal,
        from_chain: str,
        to_chain: str,
    ) -> tuple[bool, str | None]:
        """Validate a transfer before getting a quote.

        Args:
            token: Token symbol
            amount: Amount to bridge
            from_chain: Source chain
            to_chain: Destination chain

        Returns:
            Tuple of (is_valid, error_message)
        """
        # Check token support
        if not self.supports_token(token):
            return False, f"Token {token} not supported by {self.name}"

        # Check route support
        route = self.get_route(from_chain, to_chain)
        if route is None:
            return False, f"Route {from_chain} -> {to_chain} not supported by {self.name}"

        if not route.is_active:
            return False, f"Route {from_chain} -> {to_chain} is currently inactive"

        # Check token on route
        if not route.supports_token(token):
            return False, f"Token {token} not supported on route {from_chain} -> {to_chain}"

        # Check amount limits
        if route.min_amount > 0 and amount < route.min_amount:
            return False, f"Amount {amount} below minimum {route.min_amount}"

        if route.max_amount > 0 and amount > route.max_amount:
            return False, f"Amount {amount} above maximum {route.max_amount}"

        return True, None


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Abstract base class
    "BridgeAdapter",
    # Data classes
    "BridgeQuote",
    "BridgeStatus",
    "BridgeRoute",
    # Enums
    "BridgeStatusEnum",
    # Exceptions
    "BridgeError",
    "BridgeQuoteError",
    "BridgeTransactionError",
    "BridgeStatusError",
]
