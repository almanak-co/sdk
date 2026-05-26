"""LiFi SDK Data Models.

This module defines data classes for LiFi API requests and responses.
Uses @dataclass for internal data structures per project conventions.
"""

from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any


class LiFiOrderStrategy(StrEnum):
    """LiFi route ordering strategies."""

    FASTEST = "FASTEST"
    CHEAPEST = "CHEAPEST"
    SAFEST = "SAFEST"
    RECOMMENDED = "RECOMMENDED"


class LiFiTransferStatus(StrEnum):
    """LiFi cross-chain transfer status values."""

    NOT_FOUND = "NOT_FOUND"
    PENDING = "PENDING"
    DONE = "DONE"
    FAILED = "FAILED"


class LiFiTransferSubstatus(StrEnum):
    """LiFi cross-chain transfer substatus values."""

    COMPLETED = "COMPLETED"
    PARTIAL = "PARTIAL"
    REFUNDED = "REFUNDED"
    NOT_PROCESSABLE_REFUND_NEEDED = "NOT_PROCESSABLE_REFUND_NEEDED"
    UNKNOWN = "UNKNOWN"


class LiFiStepType(StrEnum):
    """LiFi step types."""

    SWAP = "swap"
    CROSS = "cross"
    LIFI = "lifi"
    PROTOCOL = "protocol"


# ============================================================================
# Token & Action Models
# ============================================================================


@dataclass
class LiFiToken:
    """Token information from LiFi API.

    Attributes:
        address: Token contract address
        chain_id: Chain ID where token resides
        symbol: Token symbol (e.g., "USDC")
        decimals: Token decimals
        name: Token name
        price_usd: Token price in USD (if available)
    """

    address: str
    chain_id: int
    symbol: str
    decimals: int
    name: str = ""
    price_usd: float | None = None

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "LiFiToken":
        """Create LiFiToken from API response.

        Note: decimals defaults to 18 here for display purposes only.
        The adapter uses TokenResolver for all amount calculations, so
        this default does NOT affect financial math.
        """
        # LiFi API returns priceUSD as a string, coerce to float
        raw_price = data.get("priceUSD")
        price_usd = None
        if raw_price is not None:
            try:
                price_usd = float(raw_price)
            except (ValueError, TypeError):
                price_usd = None

        return cls(
            address=data.get("address", ""),
            chain_id=data.get("chainId", 0),
            symbol=data.get("symbol", ""),
            decimals=data.get("decimals", 18),
            name=data.get("name", ""),
            price_usd=price_usd,
        )


@dataclass
class LiFiAction:
    """Action details within a LiFi step.

    Attributes:
        from_chain_id: Source chain ID
        to_chain_id: Destination chain ID
        from_token: Source token info
        to_token: Destination token info
        from_amount: Amount being sent (in wei/smallest unit)
        from_address: Sender address
        to_address: Receiver address
        slippage: Slippage tolerance (0-1)
    """

    from_chain_id: int
    to_chain_id: int
    from_token: LiFiToken
    to_token: LiFiToken
    from_amount: str
    from_address: str = ""
    to_address: str = ""
    slippage: float = 0.005

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "LiFiAction":
        """Create LiFiAction from API response."""
        return cls(
            from_chain_id=data.get("fromChainId", 0),
            to_chain_id=data.get("toChainId", 0),
            from_token=LiFiToken.from_api_response(data.get("fromToken", {})),
            to_token=LiFiToken.from_api_response(data.get("toToken", {})),
            from_amount=data.get("fromAmount", "0"),
            from_address=data.get("fromAddress", ""),
            to_address=data.get("toAddress", ""),
            slippage=data.get("slippage", 0.005),
        )


# ============================================================================
# Estimate & Cost Models
# ============================================================================


@dataclass
class LiFiGasCost:
    """Gas cost information.

    Attributes:
        type: Cost type (e.g., "SUM")
        estimate: Estimated gas units
        limit: Gas limit
        amount: Cost amount in native token (wei)
        amount_usd: Cost in USD
        token: Native token info
    """

    type: str = ""
    estimate: str = "0"
    limit: str = "0"
    amount: str = "0"
    amount_usd: str = "0"
    token: LiFiToken | None = None

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "LiFiGasCost":
        """Create from API response."""
        token_data = data.get("token")
        return cls(
            type=data.get("type", ""),
            estimate=data.get("estimate", "0"),
            limit=data.get("limit", "0"),
            amount=data.get("amount", "0"),
            amount_usd=data.get("amountUSD", "0"),
            token=LiFiToken.from_api_response(token_data) if token_data else None,
        )


@dataclass
class LiFiFeeCost:
    """Fee cost information.

    Attributes:
        name: Fee name (e.g., "Bridge Fee")
        description: Fee description
        percentage: Fee as percentage
        amount: Fee amount in token units
        amount_usd: Fee in USD
        token: Token for fee
        included: Whether fee is included in the amount
    """

    name: str = ""
    description: str = ""
    percentage: str = "0"
    amount: str = "0"
    amount_usd: str = "0"
    token: LiFiToken | None = None
    included: bool = True

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "LiFiFeeCost":
        """Create from API response."""
        token_data = data.get("token")
        return cls(
            name=data.get("name", ""),
            description=data.get("description", ""),
            percentage=data.get("percentage", "0"),
            amount=data.get("amount", "0"),
            amount_usd=data.get("amountUSD", "0"),
            token=LiFiToken.from_api_response(token_data) if token_data else None,
            included=data.get("included", True),
        )


@dataclass
class LiFiEstimate:
    """Estimate information for a LiFi step.

    Attributes:
        from_amount: Input amount (in smallest unit)
        to_amount: Expected output amount
        to_amount_min: Guaranteed minimum output (with slippage)
        approval_address: Contract address to approve tokens to
        execution_duration: Estimated execution time in seconds
        fee_costs: List of fee costs
        gas_costs: List of gas costs
    """

    from_amount: str = "0"
    to_amount: str = "0"
    to_amount_min: str = "0"
    approval_address: str = ""
    execution_duration: int = 0
    fee_costs: list[LiFiFeeCost] = field(default_factory=list)
    gas_costs: list[LiFiGasCost] = field(default_factory=list)

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "LiFiEstimate":
        """Create from API response."""
        return cls(
            from_amount=data.get("fromAmount", "0"),
            to_amount=data.get("toAmount", "0"),
            to_amount_min=data.get("toAmountMin", "0"),
            approval_address=data.get("approvalAddress", ""),
            execution_duration=data.get("executionDuration", 0),
            fee_costs=[LiFiFeeCost.from_api_response(f) for f in data.get("feeCosts", [])],
            gas_costs=[LiFiGasCost.from_api_response(g) for g in data.get("gasCosts", [])],
        )

    @property
    def total_gas_estimate(self) -> int:
        """Get total gas estimate across all gas costs."""
        total = 0
        for cost in self.gas_costs:
            try:
                total += int(cost.estimate)
            except (ValueError, TypeError):
                pass
        return total

    @property
    def total_fee_usd(self) -> float:
        """Get total fees in USD."""
        total = 0.0
        for cost in self.fee_costs:
            try:
                total += float(cost.amount_usd)
            except (ValueError, TypeError):
                pass
        return total


# ============================================================================
# Transaction Request
# ============================================================================


@dataclass
class LiFiTransactionRequest:
    """Transaction request data from LiFi API.

    This contains the actual calldata to execute on-chain.

    Attributes:
        from_address: Sender address
        to: Target contract address (LiFi Diamond)
        chain_id: Chain ID for the transaction
        data: Encoded calldata
        value: Native token value to send (in wei)
        gas_price: Suggested gas price
        gas_limit: Suggested gas limit
    """

    from_address: str = ""
    to: str = ""
    chain_id: int = 0
    data: str = ""
    value: str = "0"
    gas_price: str = "0"
    gas_limit: str = "0"

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "LiFiTransactionRequest":
        """Create from API response."""
        return cls(
            from_address=data.get("from", ""),
            to=data.get("to", ""),
            chain_id=data.get("chainId", 0),
            data=data.get("data", ""),
            value=data.get("value", "0"),
            gas_price=data.get("gasPrice", "0"),
            gas_limit=data.get("gasLimit", "0"),
        )


# ============================================================================
# Step (Quote Response)
# ============================================================================


@dataclass
class LiFiStep:
    """A step in a LiFi route (the main quote response).

    This is the top-level response from the /v1/quote endpoint.

    Attributes:
        id: Unique step identifier
        type: Step type (swap, cross, lifi, protocol)
        tool: Bridge/DEX tool used (e.g., "across", "1inch")
        action: Action details (chains, tokens, amounts)
        estimate: Estimate details (amounts, fees, duration)
        transaction_request: Transaction to execute on-chain
        included_steps: Sub-steps for multi-hop routes
        integrator: Integrator identifier
    """

    id: str = ""
    type: str = ""
    tool: str = ""
    action: LiFiAction | None = None
    estimate: LiFiEstimate | None = None
    transaction_request: LiFiTransactionRequest | None = None
    included_steps: list["LiFiStep"] = field(default_factory=list)
    integrator: str = ""

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "LiFiStep":
        """Create LiFiStep from API response."""
        action_data = data.get("action")
        estimate_data = data.get("estimate")
        tx_data = data.get("transactionRequest")

        included = []
        for step_data in data.get("includedSteps", []):
            included.append(LiFiStep.from_api_response(step_data))

        return cls(
            id=data.get("id", ""),
            type=data.get("type", ""),
            tool=data.get("tool", ""),
            action=LiFiAction.from_api_response(action_data) if action_data else None,
            estimate=LiFiEstimate.from_api_response(estimate_data) if estimate_data else None,
            transaction_request=LiFiTransactionRequest.from_api_response(tx_data) if tx_data else None,
            included_steps=included,
            integrator=data.get("integrator", ""),
        )

    @property
    def is_cross_chain(self) -> bool:
        """Check if this is a cross-chain step."""
        if self.action:
            return self.action.from_chain_id != self.action.to_chain_id
        return self.type == "cross" or self.type == "lifi"

    def get_to_amount(self) -> int:
        """Get expected output amount as integer."""
        if self.estimate:
            try:
                return int(self.estimate.to_amount)
            except (ValueError, TypeError):
                pass
        return 0

    def get_to_amount_min(self) -> int:
        """Get guaranteed minimum output amount as integer."""
        if self.estimate:
            try:
                return int(self.estimate.to_amount_min)
            except (ValueError, TypeError):
                pass
        return 0


# ============================================================================
# Status Response
# ============================================================================


@dataclass
class LiFiStatusResponse:
    """Status response for a cross-chain transfer.

    Attributes:
        transaction_id: LiFi transaction ID
        sending_tx_hash: Source chain transaction hash
        receiving_tx_hash: Destination chain transaction hash (when complete)
        bridge_name: Bridge used for the transfer
        from_chain_id: Source chain ID
        to_chain_id: Destination chain ID
        status: Overall status (NOT_FOUND, PENDING, DONE, FAILED)
        substatus: Detailed substatus (COMPLETED, PARTIAL, REFUNDED)
        substatus_message: Human-readable substatus message
    """

    transaction_id: str = ""
    sending_tx_hash: str = ""
    receiving_tx_hash: str = ""
    bridge_name: str = ""
    from_chain_id: int = 0
    to_chain_id: int = 0
    status: str = ""
    substatus: str = ""
    substatus_message: str = ""

    @classmethod
    def from_api_response(cls, data: dict[str, Any]) -> "LiFiStatusResponse":
        """Create from API response."""
        sending = data.get("sending", {})
        receiving = data.get("receiving", {})

        return cls(
            transaction_id=data.get("transactionId", ""),
            sending_tx_hash=sending.get("txHash", data.get("sendingTxHash", "")),
            receiving_tx_hash=receiving.get("txHash", data.get("receivingTxHash", "")),
            bridge_name=data.get("bridge", data.get("tool", "")),
            from_chain_id=data.get("fromChainId", sending.get("chainId", 0)),
            to_chain_id=data.get("toChainId", receiving.get("chainId", 0)),
            status=data.get("status", ""),
            substatus=data.get("substatus", ""),
            substatus_message=data.get("substatusMessage", ""),
        )

    @property
    def is_complete(self) -> bool:
        """Check if the transfer is complete."""
        return self.status == LiFiTransferStatus.DONE

    @property
    def is_failed(self) -> bool:
        """Check if the transfer failed."""
        return self.status == LiFiTransferStatus.FAILED

    @property
    def is_pending(self) -> bool:
        """Check if the transfer is still pending."""
        return self.status == LiFiTransferStatus.PENDING
