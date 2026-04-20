"""Bridge Intent for cross-chain asset transfers.

This module provides the BridgeIntent dataclass for expressing cross-chain
bridge transfers as first-class intents.

Example:
    from almanak.framework.intents import Intent

    # Bridge 1000 USDC from Base to Arbitrum
    intent = Intent.bridge(
        token="USDC",
        amount=Decimal("1000"),
        from_chain="base",
        to_chain="arbitrum",
    )

    # Bridge all ETH from previous step (in a sequence)
    intent = Intent.bridge(
        token="ETH",
        amount="all",  # Use output from previous step
        from_chain="optimism",
        to_chain="arbitrum",
    )
"""

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

from pydantic import Field, field_validator, model_validator

from almanak.framework.models.base import (
    AlmanakImmutableModel,
    SafeDecimal,
    default_intent_id,
    default_timestamp,
)

if TYPE_CHECKING:
    from .vocabulary import IntentType
from almanak.framework.models.base import (
    ChainedAmount as PydanticChainedAmount,
)

# =============================================================================
# Exceptions
# =============================================================================


class InvalidBridgeError(ValueError):
    """Raised when a bridge intent has invalid parameters.

    Attributes:
        message: Description of the error
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


class BridgeChainError(ValueError):
    """Raised when a bridge intent specifies invalid chains.

    Attributes:
        from_chain: The source chain
        to_chain: The destination chain
        configured_chains: The list of chains configured for the strategy
    """

    def __init__(
        self,
        from_chain: str,
        to_chain: str,
        configured_chains: Sequence[str],
        reason: str = "not configured",
    ) -> None:
        self.from_chain = from_chain
        self.to_chain = to_chain
        self.configured_chains = list(configured_chains)
        self.reason = reason
        chains_str = ", ".join(sorted(self.configured_chains)) if self.configured_chains else "(none)"
        super().__init__(f"Bridge from '{from_chain}' to '{to_chain}' {reason}. Configured chains: {chains_str}")


class BridgeTokenError(ValueError):
    """Raised when a bridge intent specifies an unsupported token.

    Attributes:
        token: The unsupported token
        from_chain: The source chain
        to_chain: The destination chain
    """

    def __init__(self, token: str, from_chain: str, to_chain: str) -> None:
        self.token = token
        self.from_chain = from_chain
        self.to_chain = to_chain
        super().__init__(f"Token '{token}' is not supported for bridging from '{from_chain}' to '{to_chain}'")


# =============================================================================
# Intent Type Extension
# =============================================================================


class BridgeIntentType(Enum):
    """Type for bridge intents."""

    BRIDGE = "BRIDGE"


# =============================================================================
# Bridge Intent Data Class
# =============================================================================


# Type for amount: either a Decimal value or 'all' to use previous step's output
BridgeAmount = Decimal | Literal["all"]


class BridgeIntent(AlmanakImmutableModel):
    """Intent to bridge tokens from one chain to another.

    BridgeIntent represents a cross-chain asset transfer. It can be used
    standalone or as part of an IntentSequence for complex multi-step
    operations.

    When amount="all", the bridge will use the entire output from the
    previous step in a sequence. This is useful for chaining operations
    like swap -> bridge -> supply.

    Attributes:
        token: Token symbol to bridge (e.g., "ETH", "USDC", "WBTC")
        amount: Amount to bridge (Decimal or "all" for chained amounts)
        from_chain: Source chain identifier (e.g., "base", "arbitrum")
        to_chain: Destination chain identifier (e.g., "arbitrum", "optimism")
        max_slippage: Maximum acceptable slippage (e.g., 0.005 = 0.5%)
        preferred_bridge: Optional preferred bridge adapter name
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Example:
        # Bridge 1000 USDC from Base to Arbitrum with 0.5% max slippage
        intent = BridgeIntent(
            token="USDC",
            amount=Decimal("1000"),
            from_chain="base",
            to_chain="arbitrum",
            max_slippage=Decimal("0.005"),
        )

        # Bridge all ETH from previous step output
        intent = BridgeIntent(
            token="ETH",
            amount="all",
            from_chain="optimism",
            to_chain="arbitrum",
        )
    """

    token: str
    amount: PydanticChainedAmount
    from_chain: str
    to_chain: str
    max_slippage: SafeDecimal = Field(default=Decimal("0.005"))
    preferred_bridge: str | None = None
    destination_address: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @field_validator("destination_address", mode="before")
    @classmethod
    def validate_destination_address(cls, v: str | None) -> str | None:
        """Validate destination_address is non-empty when provided."""
        if v is None:
            return None
        if not isinstance(v, str) or not v.strip():
            raise InvalidBridgeError("destination_address must be a non-empty string when provided")
        return v.strip()

    @field_validator("token", mode="before")
    @classmethod
    def validate_token(cls, v: str) -> str:
        """Validate token is non-empty."""
        if not v or not v.strip():
            raise InvalidBridgeError("token must be a non-empty string")
        return v

    @field_validator("from_chain", "to_chain", mode="before")
    @classmethod
    def normalize_chain(cls, v: str) -> str:
        """Validate and normalize chain names to lowercase."""
        if not v or not v.strip():
            raise InvalidBridgeError("chain must be a non-empty string")
        return v.lower()

    @model_validator(mode="after")
    def validate_bridge_intent(self) -> "BridgeIntent":
        """Validate bridge intent parameters."""
        # Same chain validation
        if self.from_chain == self.to_chain:
            raise InvalidBridgeError(f"from_chain and to_chain must be different, got '{self.from_chain}'")

        # Validate amount if it's a Decimal
        if isinstance(self.amount, Decimal) and self.amount <= 0:
            raise InvalidBridgeError("amount must be positive")

        # Validate slippage
        if self.max_slippage < 0 or self.max_slippage > 1:
            raise InvalidBridgeError("max_slippage must be between 0 and 1")

        return self

    @property
    def intent_type(self) -> "IntentType":
        """Return the type of this intent."""
        # Lazy import to avoid circular import at module import time.
        from .vocabulary import IntentType

        return IntentType.BRIDGE

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.amount == "all"

    @property
    def chain(self) -> str:
        """Return the source chain for compatibility with Intent.get_chain().

        For bridge intents, the 'chain' property returns the source chain
        (from_chain) since that's where the transaction originates.
        """
        return self.from_chain

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        intent_type = self.intent_type
        data = self.model_dump(mode="json")
        data["type"] = intent_type.value
        # Preserve "all" as string (not serialized to string by model_dump)
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "BridgeIntent":
        """Deserialize a dictionary to a BridgeIntent."""
        # Remove type field which is not part of the model
        clean_data = {k: v for k, v in data.items() if k != "type"}

        # Parse datetime if it's a string
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])

        return cls.model_validate(clean_data)

    @staticmethod
    def validate_chains(
        from_chain: str,
        to_chain: str,
        configured_chains: Sequence[str],
    ) -> None:
        """Validate that both chains are configured for the strategy.

        Args:
            from_chain: Source chain identifier
            to_chain: Destination chain identifier
            configured_chains: List of chains configured for the strategy

        Raises:
            BridgeChainError: If either chain is not configured
        """
        if not configured_chains:
            raise BridgeChainError(from_chain, to_chain, configured_chains, "no chains configured")

        # Normalize to lowercase for comparison
        normalized_chains = [c.lower() for c in configured_chains]

        if from_chain.lower() not in normalized_chains:
            raise BridgeChainError(
                from_chain,
                to_chain,
                configured_chains,
                f"source chain '{from_chain}' not configured",
            )

        if to_chain.lower() not in normalized_chains:
            raise BridgeChainError(
                from_chain,
                to_chain,
                configured_chains,
                f"destination chain '{to_chain}' not configured",
            )


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "BridgeIntent",
    "BridgeIntentType",
    "BridgeAmount",
    "InvalidBridgeError",
    "BridgeChainError",
    "BridgeTokenError",
]
