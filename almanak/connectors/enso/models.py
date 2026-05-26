"""Enso SDK Data Models.

This module defines data classes for Enso API requests and responses.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class RoutingStrategy(StrEnum):
    """Enso routing strategies."""

    ENSO_WALLET = "ensowallet"
    ROUTER = "router"
    DELEGATE = "delegate"


@dataclass
class RouteParams:
    """Parameters for getting a swap route from Enso.

    Attributes:
        from_address: Address executing the transaction
        token_in: Input token address
        token_out: Output token address
        amount_in: Input amount in wei (as int)
        chain_id: Source blockchain chain ID
        slippage_bps: Slippage tolerance in basis points (e.g., 50 = 0.5%)
        routing_strategy: Routing strategy to use
        receiver: Address to receive output tokens (defaults to from_address)
        max_price_impact_bps: Maximum allowed price impact in basis points
        destination_chain_id: Target chain ID for cross-chain swaps (None for same-chain)
        refund_receiver: Address to receive refunds if cross-chain fails (required for cross-chain)
    """

    from_address: str
    token_in: str
    token_out: str
    amount_in: int
    chain_id: int
    slippage_bps: int = 50  # Default 0.5% slippage
    routing_strategy: RoutingStrategy = RoutingStrategy.ROUTER
    receiver: str | None = None
    max_price_impact_bps: int | None = None
    destination_chain_id: int | None = None
    refund_receiver: str | None = None

    @property
    def is_cross_chain(self) -> bool:
        """Check if this is a cross-chain route."""
        return self.destination_chain_id is not None and self.destination_chain_id != self.chain_id

    def to_api_format(self) -> dict[str, Any]:
        """Convert parameters to Enso API format."""
        params = {
            "fromAddress": self.from_address,
            "tokenIn": [self.token_in],
            "tokenOut": [self.token_out],
            "amountIn": [str(self.amount_in)],
            "chainId": self.chain_id,
            "slippage": str(self.slippage_bps),
        }

        if self.routing_strategy:
            params["routingStrategy"] = self.routing_strategy.value

        if self.receiver:
            params["receiver"] = self.receiver

        # Cross-chain parameters
        if self.destination_chain_id is not None:
            params["destinationChainId"] = self.destination_chain_id
            # refundReceiver is required for cross-chain operations
            refund_addr = self.refund_receiver or self.from_address
            params["refundReceiver"] = refund_addr

        return params


@dataclass
class Transaction:
    """Transaction data from Enso API response.

    Attributes:
        data: Encoded calldata
        to: Target contract address
        from_address: Sender address
        value: Native token value to send (in wei)
    """

    data: str
    to: str
    from_address: str
    value: str

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "Transaction":
        """Create Transaction from API response."""
        return cls(
            data=data.get("data", ""),
            to=data.get("to", ""),
            from_address=data.get("from", ""),
            value=data.get("value", "0"),
        )


@dataclass
class Hop:
    """A single hop in a swap route.

    Attributes:
        token_in: Input token addresses
        token_out: Output token addresses
        protocol: Protocol used for this hop
        action: Action performed
        primary: Primary address (pool/contract)
    """

    token_in: list[str] = field(default_factory=list)
    token_out: list[str] = field(default_factory=list)
    protocol: str = ""
    action: str = ""
    primary: str = ""

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "Hop":
        """Create Hop from API response."""
        return cls(
            token_in=data.get("tokenIn", data.get("token_in", [])),
            token_out=data.get("tokenOut", data.get("token_out", [])),
            protocol=data.get("protocol", ""),
            action=data.get("action", ""),
            primary=data.get("primary", ""),
        )


@dataclass
class RouteTransaction:
    """Route transaction response from Enso API.

    Attributes:
        gas: Estimated gas for the transaction
        tx: Transaction data
        amount_out: Expected output amount (as dict with token -> amount)
        price_impact: Price impact in basis points
        route: List of hops in the route
        fee_amount: Fee amounts
        created_at: Timestamp when route was created
        chain_id: Source chain ID for this route
        destination_chain_id: Destination chain ID for cross-chain routes (None for same-chain)
        bridge_fee: Bridge fee for cross-chain routes (in native token wei)
        estimated_time: Estimated completion time in seconds for cross-chain routes
    """

    gas: str
    tx: Transaction
    amount_out: dict[str, Any]
    price_impact: float | None = None
    route: list[Hop] = field(default_factory=list)
    fee_amount: list[str] = field(default_factory=list)
    created_at: int | None = None
    chain_id: int | None = None
    destination_chain_id: int | None = None
    bridge_fee: str | None = None
    estimated_time: int | None = None

    @property
    def is_cross_chain(self) -> bool:
        """Check if this is a cross-chain route."""
        return self.destination_chain_id is not None and self.destination_chain_id != self.chain_id

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "RouteTransaction":
        """Create RouteTransaction from API response."""
        tx_data = data.get("tx", {})
        tx = Transaction.from_api_response(tx_data)

        route_data = data.get("route", [])
        route = [Hop.from_api_response(hop) for hop in route_data]

        return cls(
            gas=data.get("gas", "0"),
            tx=tx,
            amount_out=data.get("amountOut", {}),
            price_impact=data.get("priceImpact"),
            route=route,
            fee_amount=data.get("feeAmount", []),
            created_at=data.get("createdAt"),
            chain_id=data.get("chainId"),
            destination_chain_id=data.get("destinationChainId"),
            bridge_fee=data.get("bridgeFee"),
            estimated_time=data.get("estimatedTime"),
        )

    def get_amount_out_wei(self, token_address: str | None = None) -> int:
        """Get the output amount in wei.

        Args:
            token_address: Specific token address to get amount for.
                If None, returns the first/only amount.

        Returns:
            Output amount in wei as integer
        """
        if isinstance(self.amount_out, dict):
            if token_address:
                amount = self.amount_out.get(token_address.lower())
                if amount:
                    return int(amount)
            # Return first value if no specific token or single output
            for value in self.amount_out.values():
                return int(value)
        elif isinstance(self.amount_out, str | int):
            return int(self.amount_out)
        return 0

    def get_price_impact_percentage(self) -> float | None:
        """Get price impact as a percentage.

        Returns:
            Price impact as a percentage (e.g., 3.0 for 3%) or None if not available.
        """
        if self.price_impact is None:
            return None
        return self.price_impact / 100


@dataclass
class Quote:
    """Quote response from Enso API.

    Attributes:
        amount_out: Expected output amount
        gas: Estimated gas
        price_impact: Price impact in basis points
        route: List of hops in the route
        chain_id: Chain ID for this quote
    """

    amount_out: str
    gas: str | None = None
    price_impact: float | None = None
    route: list[Hop] | None = None
    chain_id: int | None = None

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "Quote":
        """Create Quote from API response."""
        route_data = data.get("route", [])
        route = [Hop.from_api_response(hop) for hop in route_data] if route_data else None

        return cls(
            amount_out=data.get("amountOut", "0"),
            gas=data.get("gas"),
            price_impact=data.get("priceImpact"),
            route=route,
            chain_id=data.get("chainId"),
        )

    def get_price_impact_percentage(self) -> float | None:
        """Get price impact as a percentage."""
        if self.price_impact is None:
            return None
        return self.price_impact / 100


@dataclass
class BundleAction:
    """A single action in an Enso bundle.

    Used for composing multiple DeFi operations in a single transaction.

    Attributes:
        protocol: Protocol slug (e.g., "morpho-markets-v1", "aave-v3")
        action: Action type (e.g., "deposit", "borrow", "repay", "redeem")
        args: Action-specific arguments
    """

    protocol: str
    action: str
    args: dict[str, Any] = field(default_factory=dict)

    def to_api_format(self) -> dict[str, Any]:
        """Convert to Enso API bundle format."""
        return {
            "protocol": self.protocol,
            "action": self.action,
            "args": self.args,
        }
