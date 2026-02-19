"""Ensure Balance Intent for automatic cross-chain balance management.

This module provides the EnsureBalanceIntent dataclass that allows strategy
developers to express "ensure I have at least X tokens on chain Y" so the
system handles sourcing automatically.

Example:
    from almanak.framework.intents import Intent

    # Ensure at least 1000 USDC on Arbitrum
    intent = Intent.ensure_balance(
        token="USDC",
        min_amount=Decimal("1000"),
        target_chain="arbitrum",
    )

    # The system will:
    # 1. Check current balance on arbitrum
    # 2. If >= 1000, return HoldIntent (no action needed)
    # 3. If < 1000, find a source chain with sufficient balance
    # 4. Generate a BridgeIntent to move tokens to arbitrum
"""

from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Union

from pydantic import Field, field_validator, model_validator

from almanak.framework.models.base import (
    AlmanakImmutableModel,
    SafeDecimal,
    default_intent_id,
    default_timestamp,
)

if TYPE_CHECKING:
    from .bridge import BridgeIntent
    from .vocabulary import HoldIntent


# =============================================================================
# Exceptions
# =============================================================================


class InsufficientBalanceError(ValueError):
    """Raised when no single chain has sufficient balance for ensure_balance.

    Attributes:
        token: The token that was requested
        min_amount: The minimum amount required
        target_chain: The target chain
        available_balances: Dict of {chain: balance} showing available balances
    """

    def __init__(
        self,
        token: str,
        min_amount: Decimal,
        target_chain: str,
        available_balances: dict[str, Decimal],
    ) -> None:
        self.token = token
        self.min_amount = min_amount
        self.target_chain = target_chain
        self.available_balances = available_balances

        # Format available balances
        balances_str = ", ".join(f"{chain}: {amount}" for chain, amount in sorted(available_balances.items()))
        if not balances_str:
            balances_str = "(no balances found)"

        super().__init__(
            f"No single chain has sufficient {token} balance to ensure {min_amount} "
            f"on {target_chain}. Available balances: {balances_str}"
        )


class InvalidEnsureBalanceError(ValueError):
    """Raised when ensure_balance intent has invalid parameters.

    Attributes:
        message: Description of the error
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)


# =============================================================================
# Intent Type Extension
# =============================================================================


class EnsureBalanceIntentType(Enum):
    """Type for ensure balance intents."""

    ENSURE_BALANCE = "ENSURE_BALANCE"


# =============================================================================
# Ensure Balance Intent Data Class
# =============================================================================


class EnsureBalanceIntent(AlmanakImmutableModel):
    """Intent to ensure a minimum token balance on a target chain.

    EnsureBalanceIntent is a high-level intent that expresses the goal of having
    at least a certain amount of tokens on a specific chain. The framework will
    automatically determine how to achieve this goal:

    1. Check current balance on target chain
    2. If balance >= min_amount: Return HoldIntent (no action needed)
    3. If balance < min_amount: Find a source chain with sufficient balance
    4. Generate a BridgeIntent to transfer the needed tokens

    This simplifies strategy development by abstracting away the complexity of
    cross-chain balance management.

    Attributes:
        token: Token symbol to ensure (e.g., "ETH", "USDC", "WBTC")
        min_amount: Minimum amount required on target chain
        target_chain: Chain where the balance is needed (e.g., "arbitrum", "base")
        max_slippage: Maximum acceptable slippage for bridging (e.g., 0.005 = 0.5%)
        preferred_bridge: Optional preferred bridge adapter name for transfer
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Example:
        # Ensure at least 1000 USDC on Arbitrum before opening a position
        intent = EnsureBalanceIntent(
            token="USDC",
            min_amount=Decimal("1000"),
            target_chain="arbitrum",
        )

        # Ensure at least 2 ETH on Base with specific slippage
        intent = EnsureBalanceIntent(
            token="ETH",
            min_amount=Decimal("2"),
            target_chain="base",
            max_slippage=Decimal("0.01"),  # 1% max slippage
        )
    """

    token: str
    min_amount: SafeDecimal
    target_chain: str
    max_slippage: SafeDecimal = Field(default=Decimal("0.005"))
    preferred_bridge: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @field_validator("token", mode="before")
    @classmethod
    def validate_token(cls, v: str) -> str:
        """Validate token is non-empty."""
        if not v or not v.strip():
            raise InvalidEnsureBalanceError("token must be a non-empty string")
        return v

    @field_validator("target_chain", mode="before")
    @classmethod
    def normalize_chain(cls, v: str) -> str:
        """Validate and normalize chain name to lowercase."""
        if not v or not v.strip():
            raise InvalidEnsureBalanceError("target_chain must be a non-empty string")
        return v.lower()

    @model_validator(mode="after")
    def validate_ensure_balance_intent(self) -> "EnsureBalanceIntent":
        """Validate ensure balance intent parameters."""
        # Validate amount
        if self.min_amount <= 0:
            raise InvalidEnsureBalanceError("min_amount must be positive")

        # Validate slippage
        if self.max_slippage < 0 or self.max_slippage > 1:
            raise InvalidEnsureBalanceError("max_slippage must be between 0 and 1")

        return self

    @property
    def intent_type(self) -> EnsureBalanceIntentType:
        """Return the type of this intent."""
        return EnsureBalanceIntentType.ENSURE_BALANCE

    @property
    def chain(self) -> str:
        """Return the target chain for compatibility with Intent.get_chain().

        For ensure_balance intents, the 'chain' property returns the target chain
        since that's where the balance needs to be ensured.
        """
        return self.target_chain

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = EnsureBalanceIntentType.ENSURE_BALANCE.value
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "EnsureBalanceIntent":
        """Deserialize a dictionary to an EnsureBalanceIntent."""
        # Remove type field which is not part of the model
        clean_data = {k: v for k, v in data.items() if k != "type"}

        # Parse datetime if it's a string
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])

        return cls.model_validate(clean_data)

    @staticmethod
    def validate_target_chain(
        target_chain: str,
        configured_chains: Sequence[str],
    ) -> None:
        """Validate that the target chain is configured for the strategy.

        Args:
            target_chain: Target chain identifier
            configured_chains: List of chains configured for the strategy

        Raises:
            InvalidEnsureBalanceError: If target chain is not configured
        """
        if not configured_chains:
            raise InvalidEnsureBalanceError(
                f"No chains configured for strategy, cannot ensure balance on '{target_chain}'"
            )

        # Normalize to lowercase for comparison
        normalized_chains = [c.lower() for c in configured_chains]

        if target_chain.lower() not in normalized_chains:
            chains_str = ", ".join(sorted(configured_chains))
            raise InvalidEnsureBalanceError(
                f"Target chain '{target_chain}' is not configured for this strategy. Configured chains: {chains_str}"
            )

    def resolve(
        self,
        target_balance: Decimal,
        chain_balances: dict[str, Decimal],
    ) -> Union["HoldIntent", "BridgeIntent"]:
        """Resolve this ensure_balance intent to a concrete intent.

        This method determines the appropriate action based on current balances:
        1. If target chain has sufficient balance -> HoldIntent
        2. If a source chain has sufficient balance -> BridgeIntent
        3. If no single chain has sufficient balance -> raise InsufficientBalanceError

        Args:
            target_balance: Current balance of the token on the target chain
            chain_balances: Dict of {chain: balance} for all configured chains
                           (excluding the target chain)

        Returns:
            HoldIntent if no action needed, BridgeIntent if transfer required

        Raises:
            InsufficientBalanceError: If no single chain has sufficient balance
        """
        # Import here to avoid circular imports
        from .bridge import BridgeIntent
        from .vocabulary import HoldIntent

        # Check if target already has sufficient balance
        if target_balance >= self.min_amount:
            return HoldIntent(
                reason=f"Sufficient {self.token} balance on {self.target_chain}: {target_balance} >= {self.min_amount}"
            )

        # Calculate how much we need to bridge
        amount_needed = self.min_amount - target_balance

        # Find a source chain with sufficient balance
        best_source_chain: str | None = None
        best_source_balance: Decimal = Decimal("0")

        for chain, balance in chain_balances.items():
            # Skip target chain
            if chain.lower() == self.target_chain:
                continue
            # Check if this chain has enough balance
            if balance >= amount_needed:
                # Prefer chain with higher balance for safety margin
                if balance > best_source_balance:
                    best_source_chain = chain
                    best_source_balance = balance

        # If no single chain has sufficient balance, raise error
        if best_source_chain is None:
            # Include target chain balance in the error for clarity
            all_balances = {**chain_balances, self.target_chain: target_balance}
            raise InsufficientBalanceError(
                token=self.token,
                min_amount=self.min_amount,
                target_chain=self.target_chain,
                available_balances=all_balances,
            )

        # Generate bridge intent from best source chain
        return BridgeIntent(
            token=self.token,
            amount=amount_needed,
            from_chain=best_source_chain,
            to_chain=self.target_chain,
            max_slippage=self.max_slippage,
            preferred_bridge=self.preferred_bridge,
        )


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    "EnsureBalanceIntent",
    "EnsureBalanceIntentType",
    "InsufficientBalanceError",
    "InvalidEnsureBalanceError",
]
