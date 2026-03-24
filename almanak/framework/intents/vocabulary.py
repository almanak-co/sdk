"""Intent vocabulary for expressing high-level trading actions.

This module defines the Intent class and its factory methods for creating
structured, serializable trading intents. Intents are the high-level
expression of what a strategy wants to do, which can then be compiled
into ActionBundles for execution.

Intent Types:
    - SWAP: Exchange one token for another
    - LP_OPEN: Open a liquidity position
    - LP_CLOSE: Close a liquidity position
    - BORROW: Borrow tokens from a lending protocol
    - REPAY: Repay borrowed tokens
    - BRIDGE: Bridge tokens between chains
    - HOLD: No action (wait)

Each intent type has its own dataclass with specific parameters, and the
Intent class provides factory methods for creating them ergonomically.
"""

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Literal, Union

from pydantic import Field, model_validator

from almanak.framework.models.base import (
    AlmanakImmutableModel,
    OptionalChainedAmount,
    OptionalSafeDecimal,
    SafeDecimal,
    default_intent_id,
    default_timestamp,
)
from almanak.framework.models.base import (
    ChainedAmount as PydanticChainedAmount,
)
from almanak.framework.services.prediction_monitor import PredictionExitConditions

# =============================================================================
# Exceptions
# =============================================================================


class InvalidChainError(ValueError):
    """Raised when an intent specifies a chain not configured for the strategy.

    Attributes:
        chain: The invalid chain that was specified
        configured_chains: The list of chains configured for the strategy
    """

    def __init__(self, chain: str, configured_chains: Sequence[str]) -> None:
        self.chain = chain
        self.configured_chains = list(configured_chains)
        chains_str = ", ".join(sorted(self.configured_chains)) if self.configured_chains else "(none)"
        super().__init__(f"Chain '{chain}' is not configured for this strategy. Configured chains: {chains_str}")


class InvalidSequenceError(ValueError):
    """Raised when an intent sequence is invalid.

    Attributes:
        message: Description of the error
    """

    def __init__(self, message: str) -> None:
        super().__init__(message)


class InvalidAmountError(ValueError):
    """Raised when amount='all' is used incorrectly.

    The 'all' amount is only valid when chaining outputs from a previous step.
    Using amount='all' on the first step of a sequence or on a standalone intent
    is invalid because there is no previous step output to reference.

    Attributes:
        intent_type: Type of intent with invalid amount
        reason: Explanation of why the amount is invalid
    """

    def __init__(self, intent_type: str, reason: str) -> None:
        self.intent_type = intent_type
        self.reason = reason
        super().__init__(f"Invalid amount='all' for {intent_type}: {reason}")


class InvalidProtocolParameterError(ValueError):
    """Raised when a protocol-specific parameter is invalid or not supported.

    Protocol-specific parameters are validated against the protocol's capabilities.
    For example, Aave supports 'variable' interest rate mode, while
    other protocols may not support interest rate mode selection at all.

    Attributes:
        protocol: The protocol that doesn't support the parameter
        parameter: The parameter name that is invalid
        value: The value that was provided
        reason: Explanation of why the parameter is invalid
    """

    def __init__(self, protocol: str, parameter: str, value: Any, reason: str) -> None:
        self.protocol = protocol
        self.parameter = parameter
        self.value = value
        self.reason = reason
        super().__init__(f"Invalid protocol parameter for '{protocol}': {parameter}={value!r}. {reason}")


class ProtocolRequiredError(ValueError):
    """Raised when protocol parameter is required but not provided.

    When a chain has multiple protocols configured that support the same operation,
    the protocol parameter must be explicitly specified to avoid ambiguity.

    Attributes:
        operation: The operation being performed (e.g., "borrow", "supply")
        available_protocols: List of protocols that support this operation on the chain
    """

    def __init__(self, operation: str, available_protocols: list[str]) -> None:
        self.operation = operation
        self.available_protocols = available_protocols
        protocols_str = ", ".join(sorted(available_protocols))
        super().__init__(
            f"Protocol must be specified for '{operation}' operation. Available protocols: {protocols_str}"
        )


# =============================================================================
# Enums
# =============================================================================


# =============================================================================
# Type Aliases
# =============================================================================

# Amount type that supports chained outputs from previous steps
# When amount="all", the intent will use the actual received amount from the
# previous step in a sequence (post-slippage, post-fees).
ChainedAmount = Decimal | Literal["all"]

# Interest rate mode type for lending protocols like Aave
# - 'variable': Interest rate fluctuates based on supply/demand
# Note: 'stable' rate was deprecated on Aave V3 and Spark (most assets disabled)
InterestRateMode = Literal["variable"]


# =============================================================================
# Protocol Capabilities
# =============================================================================

# Protocol capabilities for validation
# Maps protocol names to their supported features/parameters
PROTOCOL_CAPABILITIES: dict[str, dict[str, Any]] = {
    "aave_v3": {
        "supports_interest_rate_mode": True,
        "interest_rate_modes": ["variable"],  # stable rate deprecated on Aave V3 (most assets disabled)
        "supports_collateral_toggle": True,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
    "morpho": {
        "supports_interest_rate_mode": False,
        "supports_collateral_toggle": False,
        "requires_market_id": True,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
    "morpho_blue": {
        "supports_interest_rate_mode": False,
        "supports_collateral_toggle": False,
        "requires_market_id": True,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
    "spark": {
        "supports_interest_rate_mode": True,
        "interest_rate_modes": ["variable"],  # stable rate deprecated on Spark (most assets disabled)
        "supports_collateral_toggle": True,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
    "compound_v3": {
        "supports_interest_rate_mode": False,
        "supports_collateral_toggle": True,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
    "benqi": {
        "supports_interest_rate_mode": False,
        "supports_collateral_toggle": True,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
    "gmx_v2": {
        "supports_leverage": True,
        "max_leverage": Decimal("100"),
        "min_leverage": Decimal("1.1"),
        "operations": ["perp_open", "perp_close"],
    },
    "hyperliquid": {
        "supports_leverage": True,
        "max_leverage": Decimal("50"),
        "min_leverage": Decimal("1"),
        "operations": ["perp_open", "perp_close"],
    },
    "drift": {
        "supports_leverage": True,
        "max_leverage": Decimal("20"),
        "min_leverage": Decimal("1"),
        "operations": ["perp_open", "perp_close"],
    },
    "uniswap_v3": {
        "operations": ["swap", "lp_open", "lp_close"],
    },
    "enso": {
        "operations": ["swap"],
    },
    "polymarket": {
        "operations": ["prediction_buy", "prediction_sell", "prediction_redeem"],
        "min_price": Decimal("0.01"),
        "max_price": Decimal("0.99"),
        "order_types": ["market", "limit"],
        "time_in_force": ["GTC", "IOC", "FOK"],
        "collateral_token": "USDC",
    },
    "pendle": {
        "operations": ["swap", "lp_open", "lp_close", "withdraw"],
        "supports_pt_yt": True,
        "supports_maturity": True,
    },
    "metamorpho": {
        "operations": ["vault_deposit", "vault_redeem"],
        "supports_erc4626": True,
    },
    "kamino": {
        "supports_interest_rate_mode": False,
        "supports_collateral_toggle": False,
        "operations": ["supply", "withdraw", "borrow", "repay"],
    },
    "raydium_clmm": {
        "type": "clmm",
        "operations": ["lp_open", "lp_close"],
    },
    "meteora_dlmm": {
        "type": "dlmm",
        "operations": ["lp_open", "lp_close"],
    },
    "orca_whirlpools": {
        "type": "clmm",
        "operations": ["lp_open", "lp_close"],
    },
}


class IntentType(Enum):
    """Types of intents that strategies can express."""

    SWAP = "SWAP"
    LP_OPEN = "LP_OPEN"
    LP_CLOSE = "LP_CLOSE"
    BORROW = "BORROW"
    REPAY = "REPAY"
    SUPPLY = "SUPPLY"
    WITHDRAW = "WITHDRAW"
    PERP_OPEN = "PERP_OPEN"
    PERP_CLOSE = "PERP_CLOSE"
    BRIDGE = "BRIDGE"
    ENSURE_BALANCE = "ENSURE_BALANCE"
    FLASH_LOAN = "FLASH_LOAN"
    STAKE = "STAKE"
    UNSTAKE = "UNSTAKE"
    HOLD = "HOLD"
    # Prediction market intents
    PREDICTION_BUY = "PREDICTION_BUY"
    PREDICTION_SELL = "PREDICTION_SELL"
    PREDICTION_REDEEM = "PREDICTION_REDEEM"
    # Vault intents (MetaMorpho ERC-4626)
    VAULT_DEPOSIT = "VAULT_DEPOSIT"
    VAULT_REDEEM = "VAULT_REDEEM"
    VAULT_REALLOCATE = "VAULT_REALLOCATE"  # Phase 2
    VAULT_MANAGE = "VAULT_MANAGE"  # Phase 4
    # LP fee collection (without removing liquidity)
    LP_COLLECT_FEES = "LP_COLLECT_FEES"
    # Native token wrap/unwrap (ETH↔WETH, MATIC↔WMATIC, etc.)
    WRAP_NATIVE = "WRAP_NATIVE"
    UNWRAP_NATIVE = "UNWRAP_NATIVE"


# =============================================================================
# Intent Data Classes
# =============================================================================


class SwapIntent(AlmanakImmutableModel):
    """Intent to swap one token for another.

    Attributes:
        from_token: Symbol or address of the token to swap from
        to_token: Symbol or address of the token to swap to
        amount_usd: Amount to swap in USD terms (mutually exclusive with amount)
        amount: Amount to swap in token terms, or "all" to use output from previous step
        max_slippage: Maximum acceptable slippage (e.g., 0.005 = 0.5%)
        max_price_impact: Maximum acceptable price impact vs oracle price (e.g., 0.50 = 50%).
            If the on-chain quoter returns an amount deviating more than this from the oracle
            estimate, compilation fails. Defaults to None (uses compiler config default of 30%).
        protocol: Preferred protocol for the swap (e.g., "uniswap_v3", "enso")
        chain: Source chain for execution (defaults to strategy's primary chain)
        destination_chain: Destination chain for cross-chain swaps (None for same-chain)
        priority_fee_level: Solana priority fee level for Jupiter swaps.
            Valid values: "low", "medium", "high", "veryHigh". Defaults to "veryHigh".
        priority_fee_max_lamports: Maximum priority fee in lamports for Jupiter swaps.
            Defaults to 1_000_000 (0.001 SOL).
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        When amount="all", the swap will use the entire output from the previous
        step in a sequence. This is useful for chaining operations like:
        bridge -> swap -> supply. The actual amount is resolved at execution time.

        For cross-chain swaps, set destination_chain to the target chain name.
        Cross-chain swaps require protocol="enso" as Enso handles the bridging.

    Example:
        # Same-chain swap
        Intent.swap("USDC", "WETH", amount_usd=1000, chain="arbitrum")

        # Cross-chain swap: Base USDC -> Arbitrum WETH
        Intent.swap("USDC", "WETH", amount_usd=1000,
                    chain="base", destination_chain="arbitrum", protocol="enso")
    """

    from_token: str
    to_token: str
    amount_usd: OptionalSafeDecimal = None
    amount: OptionalChainedAmount = None
    max_slippage: SafeDecimal = Field(default=Decimal("0.005"))
    max_price_impact: OptionalSafeDecimal = Field(
        default=None,
        description="Maximum acceptable price impact vs oracle price (e.g., 0.50 = 50%). "
        "Compilation fails if quoter/oracle deviation exceeds this. "
        "Defaults to None (uses compiler config default of 30%).",
    )
    protocol: str | None = None
    chain: str | None = None
    destination_chain: str | None = None
    priority_fee_level: str | None = Field(
        default=None,
        description="Solana priority fee level for Jupiter swaps: 'low', 'medium', 'high', 'veryHigh'. "
        "Defaults to 'veryHigh' when None.",
    )
    priority_fee_max_lamports: int | None = Field(
        default=None,
        description="Maximum priority fee in lamports for Jupiter swaps. Defaults to 1_000_000 when None.",
    )
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_swap_intent(self) -> "SwapIntent":
        """Validate that either amount_usd or amount is provided."""
        if self.amount_usd is None and self.amount is None:
            raise ValueError("Either amount_usd or amount must be provided")
        if self.amount_usd is not None and self.amount is not None:
            raise ValueError("Only one of amount_usd or amount should be provided")
        if self.amount_usd is not None and self.amount_usd <= 0:
            raise ValueError("amount_usd must be positive")
        # Validate amount - either positive Decimal or "all"
        if self.amount is not None:
            if isinstance(self.amount, Decimal) and self.amount <= 0:
                raise ValueError("amount must be positive")
            elif not isinstance(self.amount, Decimal) and self.amount != "all":
                raise ValueError("amount must be a positive Decimal or 'all'")
        if self.max_slippage < 0 or self.max_slippage > 1:
            raise ValueError("max_slippage must be between 0 and 1")
        if self.max_price_impact is not None and (self.max_price_impact <= 0 or self.max_price_impact > 1):
            raise ValueError("max_price_impact must be between 0 (exclusive) and 1 (inclusive)")
        # Cross-chain swaps require an aggregator protocol (Enso or LiFi)
        if self.is_cross_chain and self.protocol and self.protocol.lower() not in ("enso", "lifi"):
            raise ValueError("Cross-chain swaps require protocol='enso' or protocol='lifi'")
        return self

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.amount == "all"

    @property
    def is_cross_chain(self) -> bool:
        """Check if this is a cross-chain swap."""
        return self.destination_chain is not None and self.chain is not None and self.destination_chain != self.chain

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.SWAP

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary.

        Backward compatible with existing serialization format.
        """
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        # Handle amount - preserve "all" as string (model_dump should do this)
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "SwapIntent":
        """Deserialize a dictionary to a SwapIntent.

        Backward compatible with existing serialization format.
        """
        # Remove "type" field as it's not part of the model
        clean_data = {k: v for k, v in data.items() if k != "type"}

        # Handle created_at string -> datetime
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])

        return cls.model_validate(clean_data)


class LPOpenIntent(AlmanakImmutableModel):
    """Intent to open a liquidity position.

    Attributes:
        pool: Pool address or identifier
        amount0: Amount of token0 to provide
        amount1: Amount of token1 to provide
        range_lower: Lower price bound for concentrated liquidity
        range_upper: Upper price bound for concentrated liquidity
        protocol: LP protocol (e.g., "uniswap_v3", "camelot")
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        protocol_params: Optional protocol-specific parameters (e.g., {"bin_range": 10} for TraderJoe V2)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created
    """

    pool: str
    amount0: SafeDecimal
    amount1: SafeDecimal
    range_lower: SafeDecimal
    range_upper: SafeDecimal
    protocol: str = "uniswap_v3"
    chain: str | None = None
    protocol_params: dict[str, Any] | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_lp_open_intent(self) -> "LPOpenIntent":
        """Validate LP open parameters."""
        if self.amount0 < 0:
            raise ValueError("amount0 must be non-negative")
        if self.amount1 < 0:
            raise ValueError("amount1 must be non-negative")
        if self.amount0 == 0 and self.amount1 == 0:
            raise ValueError("At least one amount must be positive")
        if self.range_lower >= self.range_upper:
            raise ValueError("range_lower must be less than range_upper")
        if self.range_lower <= 0:
            raise ValueError("range_lower must be positive")
        if self.protocol_params is not None:
            if not isinstance(self.protocol_params, dict):
                raise ValueError("protocol_params must be a dict")
            if "bin_range" in self.protocol_params:
                br = self.protocol_params["bin_range"]
                if isinstance(br, bool) or not isinstance(br, int) or br < 1 or br > 100:
                    raise ValueError(f"protocol_params.bin_range must be an integer between 1 and 100, got {br}")
        return self

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.LP_OPEN

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "LPOpenIntent":
        """Deserialize a dictionary to an LPOpenIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class LPCloseIntent(AlmanakImmutableModel):
    """Intent to close a liquidity position.

    Attributes:
        position_id: Identifier of the position to close (e.g., NFT token ID)
        pool: Pool address (optional, for validation)
        collect_fees: Whether to collect accumulated fees
        protocol: LP protocol (e.g., "uniswap_v3", "camelot")
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created
    """

    position_id: str
    pool: str | None = None
    collect_fees: bool = True
    protocol: str = "uniswap_v3"
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.LP_CLOSE

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "LPCloseIntent":
        """Deserialize a dictionary to an LPCloseIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class CollectFeesIntent(AlmanakImmutableModel):
    """Intent to collect accumulated fees from an LP position without closing it.

    This is useful for fee harvesting and auto-compounding strategies that want
    to claim earned fees while keeping their liquidity position open.

    Attributes:
        pool: Pool identifier (format: TOKEN_X/TOKEN_Y/BIN_STEP for TraderJoe V2)
        protocol: LP protocol (e.g., "traderjoe_v2")
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Example:
        # Collect fees from a TraderJoe V2 LP position
        intent = Intent.collect_fees(
            pool="WAVAX/USDC/20",
            protocol="traderjoe_v2",
        )
    """

    pool: str
    protocol: str = "traderjoe_v2"
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_collect_fees_intent(self) -> "CollectFeesIntent":
        """Validate the collect fees intent."""
        if not self.pool:
            raise ValueError("pool is required for collect fees intent")
        return self

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.LP_COLLECT_FEES

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "CollectFeesIntent":
        """Deserialize a dictionary to a CollectFeesIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class BorrowIntent(AlmanakImmutableModel):
    """Intent to borrow tokens from a lending protocol.

    Attributes:
        protocol: Lending protocol (e.g., "aave_v3", "morpho")
        collateral_token: Token to use as collateral
        collateral_amount: Amount of collateral to supply, or "all" for previous step output
        borrow_token: Token to borrow
        borrow_amount: Amount to borrow
        interest_rate_mode: Interest rate mode for protocols that support it (Aave: 'variable')
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        When collateral_amount="all", the borrow will use the entire output from
        the previous step in a sequence as collateral.

        The interest_rate_mode parameter is protocol-specific:
        - Aave V3: Supports 'variable' (default). Stable rate is deprecated.
        - Morpho: Does not support rate mode selection (parameter is rejected)
        - Compound V3: Does not support rate mode selection (parameter is rejected)

        The market_id parameter is required for protocols with isolated markets:
        - Morpho Blue: Required - identifies the specific lending market
        - Aave V3: Not used - uses unified pool
    """

    protocol: str
    collateral_token: str
    collateral_amount: PydanticChainedAmount
    borrow_token: str
    borrow_amount: SafeDecimal
    interest_rate_mode: InterestRateMode | None = None
    market_id: str | None = None
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_borrow_intent(self) -> "BorrowIntent":
        """Validate borrow parameters."""
        if isinstance(self.collateral_amount, Decimal) and self.collateral_amount < 0:
            raise ValueError("collateral_amount must be non-negative")
        elif not isinstance(self.collateral_amount, Decimal) and self.collateral_amount != "all":
            raise ValueError("collateral_amount must be a non-negative Decimal or 'all'")
        if self.borrow_amount <= 0:
            raise ValueError("borrow_amount must be positive")
        # Validate interest_rate_mode against protocol capabilities
        self._validate_protocol_params()
        return self

    def _validate_protocol_params(self) -> None:
        """Validate protocol-specific parameters."""
        protocol_lower = self.protocol.lower()
        capabilities = PROTOCOL_CAPABILITIES.get(protocol_lower, {})

        # Validate market_id for protocols that require it
        if capabilities.get("requires_market_id", False):
            if not self.market_id:
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="market_id",
                    value=self.market_id,
                    reason=f"Protocol '{self.protocol}' requires market_id for isolated lending markets",
                )

        # Validate interest_rate_mode if provided
        if self.interest_rate_mode is not None:
            if not capabilities.get("supports_interest_rate_mode", False):
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="interest_rate_mode",
                    value=self.interest_rate_mode,
                    reason=f"Protocol '{self.protocol}' does not support interest rate mode selection",
                )
            valid_modes = capabilities.get("interest_rate_modes", [])
            if self.interest_rate_mode not in valid_modes:
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="interest_rate_mode",
                    value=self.interest_rate_mode,
                    reason=f"Valid modes for '{self.protocol}': {', '.join(valid_modes)}",
                )

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.collateral_amount == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.BORROW

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        # Preserve "all" literal
        if self.collateral_amount == "all":
            data["collateral_amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "BorrowIntent":
        """Deserialize a dictionary to a BorrowIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class RepayIntent(AlmanakImmutableModel):
    """Intent to repay borrowed tokens.

    Attributes:
        protocol: Lending protocol (e.g., "aave_v3", "morpho")
        token: Token to repay
        amount: Amount to repay, or "all" to use output from previous step
        repay_full: If True, repay the full outstanding debt
        interest_rate_mode: Interest rate mode for protocols that support it (Aave: 'variable')
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        When amount="all", the repay will use the entire output from the previous
        step in a sequence.

        The interest_rate_mode parameter is protocol-specific:
        - Aave V3: Supports 'variable' (default). Stable rate is deprecated. Must match
          the rate mode used when borrowing.
        - Morpho: Does not support rate mode selection (parameter is rejected)
        - Compound V3: Does not support rate mode selection (parameter is rejected)

        The market_id parameter is required for protocols with isolated markets:
        - Morpho Blue: Required - identifies the specific lending market
        - Aave V3: Not used - uses unified pool
    """

    protocol: str
    token: str
    amount: PydanticChainedAmount
    repay_full: bool = False
    interest_rate_mode: InterestRateMode | None = None
    market_id: str | None = None
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_repay_intent(self) -> "RepayIntent":
        """Validate repay parameters."""
        if not self.repay_full:
            if isinstance(self.amount, Decimal) and self.amount <= 0:
                raise ValueError("amount must be positive when not repaying full")
            elif not isinstance(self.amount, Decimal) and self.amount != "all":
                raise ValueError("amount must be a positive Decimal or 'all' when not repaying full")
        # Validate protocol-specific parameters
        self._validate_protocol_params()
        return self

    def _validate_protocol_params(self) -> None:
        """Validate protocol-specific parameters."""
        protocol_lower = self.protocol.lower()
        capabilities = PROTOCOL_CAPABILITIES.get(protocol_lower, {})

        # Validate market_id for protocols that require it
        if capabilities.get("requires_market_id", False):
            if not self.market_id:
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="market_id",
                    value=self.market_id,
                    reason=f"Protocol '{self.protocol}' requires market_id for isolated lending markets",
                )

        # Validate interest_rate_mode if provided
        if self.interest_rate_mode is not None:
            if not capabilities.get("supports_interest_rate_mode", False):
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="interest_rate_mode",
                    value=self.interest_rate_mode,
                    reason=f"Protocol '{self.protocol}' does not support interest rate mode selection",
                )
            valid_modes = capabilities.get("interest_rate_modes", [])
            if self.interest_rate_mode not in valid_modes:
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="interest_rate_mode",
                    value=self.interest_rate_mode,
                    reason=f"Valid modes for '{self.protocol}': {', '.join(valid_modes)}",
                )

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.amount == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.REPAY

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "RepayIntent":
        """Deserialize a dictionary to a RepayIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class SupplyIntent(AlmanakImmutableModel):
    """Intent to supply tokens to a lending protocol.

    Attributes:
        protocol: Lending protocol (e.g., "aave_v3")
        token: Token to supply
        amount: Amount to supply, or "all" to use output from previous step
        use_as_collateral: Whether to enable the asset as collateral (default True).
            Also known as 'enable_as_collateral' - this is an Aave-specific parameter
            that controls whether the supplied asset can be used as collateral for borrowing.
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        When amount="all", the supply will use the entire output from the previous
        step in a sequence. This is useful for chaining operations like:
        swap -> supply or bridge -> supply.

        The use_as_collateral parameter is protocol-specific:
        - Aave V3: Supports collateral toggle (default True)
        - Compound V3: Supports collateral toggle
        - Morpho: Does not support collateral toggle (all supplied assets are collateral)

        The market_id parameter is required for protocols with isolated markets:
        - Morpho Blue: Required - identifies the specific lending market
        - Aave V3: Not used - uses unified pool
    """

    protocol: str
    token: str
    amount: PydanticChainedAmount
    use_as_collateral: bool = True
    market_id: str | None = None
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_supply_intent(self) -> "SupplyIntent":
        """Validate supply parameters."""
        if isinstance(self.amount, Decimal) and self.amount <= 0:
            raise ValueError("amount must be positive")
        elif not isinstance(self.amount, Decimal) and self.amount != "all":
            raise ValueError("amount must be a positive Decimal or 'all'")
        # Validate protocol-specific parameters
        self._validate_protocol_params()
        return self

    def _validate_protocol_params(self) -> None:
        """Validate protocol-specific parameters."""
        protocol_lower = self.protocol.lower()
        capabilities = PROTOCOL_CAPABILITIES.get(protocol_lower, {})

        # Validate market_id for protocols that require it
        if capabilities.get("requires_market_id", False):
            if not self.market_id:
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="market_id",
                    value=self.market_id,
                    reason=f"Protocol '{self.protocol}' requires market_id for isolated lending markets",
                )

        # Validate use_as_collateral if explicitly set to False
        # (setting to True is always safe, but setting to False on a protocol
        # that doesn't support it would be confusing)
        if not self.use_as_collateral:
            if not capabilities.get("supports_collateral_toggle", False):
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="use_as_collateral",
                    value=self.use_as_collateral,
                    reason=f"Protocol '{self.protocol}' does not support disabling collateral. All supplied assets are automatically used as collateral.",
                )

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.amount == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.SUPPLY

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "SupplyIntent":
        """Deserialize a dictionary to a SupplyIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class WithdrawIntent(AlmanakImmutableModel):
    """Intent to withdraw tokens from a lending protocol.

    Attributes:
        protocol: Lending protocol (e.g., "aave_v3")
        token: Token to withdraw
        amount: Amount to withdraw, or "all" to use output from previous step
        withdraw_all: If True, withdraw all available balance
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        When amount="all", the withdraw will use the entire output from the previous
        step in a sequence. This is different from withdraw_all which withdraws
        all available balance from the protocol.

        The market_id parameter is required for protocols with isolated markets:
        - Morpho Blue: Required - identifies the specific lending market
        - Aave V3: Not used - uses unified pool
    """

    protocol: str
    token: str
    amount: PydanticChainedAmount
    withdraw_all: bool = False
    market_id: str | None = None
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_withdraw_intent(self) -> "WithdrawIntent":
        """Validate withdraw parameters."""
        if not self.withdraw_all:
            if isinstance(self.amount, Decimal) and self.amount <= 0:
                raise ValueError("amount must be positive when not withdrawing all")
            elif not isinstance(self.amount, Decimal) and self.amount != "all":
                raise ValueError("amount must be a positive Decimal or 'all' when not withdrawing all")
        # Validate protocol-specific parameters
        self._validate_protocol_params()
        return self

    def _validate_protocol_params(self) -> None:
        """Validate protocol-specific parameters."""
        protocol_lower = self.protocol.lower()
        capabilities = PROTOCOL_CAPABILITIES.get(protocol_lower, {})

        # Validate market_id for protocols that require it
        if capabilities.get("requires_market_id", False):
            if not self.market_id:
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="market_id",
                    value=self.market_id,
                    reason=f"Protocol '{self.protocol}' requires market_id for isolated lending markets",
                )

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.amount == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.WITHDRAW

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "WithdrawIntent":
        """Deserialize a dictionary to a WithdrawIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class PerpOpenIntent(AlmanakImmutableModel):
    """Intent to open a perpetual futures position.

    Attributes:
        market: Market identifier (e.g., "ETH/USD") or market address
        collateral_token: Token symbol or address for collateral
        collateral_amount: Amount of collateral in token terms, or "all" for previous step output
        size_usd: Position size in USD terms
        is_long: True for long position, False for short
        leverage: Target leverage for the position (protocol-specific limits apply)
        max_slippage: Maximum acceptable slippage (e.g., 0.01 = 1%)
        protocol: Perpetuals protocol (default "gmx_v2")
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        When collateral_amount="all", the perp open will use the entire output from
        the previous step in a sequence. This is useful for chaining operations like:
        swap -> perp_open.

        The leverage parameter is validated against protocol-specific limits:
        - GMX V2: Supports leverage from 1.1x to 100x
        - Hyperliquid: Supports leverage from 1x to 50x
    """

    market: str
    collateral_token: str
    collateral_amount: PydanticChainedAmount
    size_usd: SafeDecimal
    is_long: bool = True
    leverage: SafeDecimal = Field(default=Decimal("1"))
    max_slippage: SafeDecimal = Field(default=Decimal("0.01"))
    protocol: str = "gmx_v2"
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_perp_open_intent(self) -> "PerpOpenIntent":
        """Validate perp open parameters."""
        if isinstance(self.collateral_amount, Decimal) and self.collateral_amount <= 0:
            raise ValueError("collateral_amount must be positive")
        elif not isinstance(self.collateral_amount, Decimal) and self.collateral_amount != "all":
            raise ValueError("collateral_amount must be a positive Decimal or 'all'")
        if self.size_usd <= 0:
            raise ValueError("size_usd must be positive")
        if self.max_slippage < 0 or self.max_slippage > 1:
            raise ValueError("max_slippage must be between 0 and 1")
        if self.leverage < 1:
            raise ValueError("leverage must be >= 1")
        # Validate leverage against protocol capabilities
        self._validate_protocol_params()
        return self

    def _validate_protocol_params(self) -> None:
        """Validate protocol-specific parameters."""
        protocol_lower = self.protocol.lower()
        capabilities = PROTOCOL_CAPABILITIES.get(protocol_lower, {})

        # Validate leverage if the protocol supports it
        if capabilities.get("supports_leverage", False):
            min_leverage = capabilities.get("min_leverage", Decimal("1"))
            max_leverage = capabilities.get("max_leverage", Decimal("100"))

            if self.leverage < min_leverage:
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="leverage",
                    value=self.leverage,
                    reason=f"Leverage must be at least {min_leverage}x for {self.protocol}",
                )
            if self.leverage > max_leverage:
                raise InvalidProtocolParameterError(
                    protocol=self.protocol,
                    parameter="leverage",
                    value=self.leverage,
                    reason=f"Leverage cannot exceed {max_leverage}x for {self.protocol}",
                )

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.collateral_amount == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PERP_OPEN

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.collateral_amount == "all":
            data["collateral_amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PerpOpenIntent":
        """Deserialize a dictionary to a PerpOpenIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class PerpCloseIntent(AlmanakImmutableModel):
    """Intent to close a perpetual futures position.

    Attributes:
        market: Market identifier (e.g., "ETH/USD") or market address
        collateral_token: Token symbol or address for collateral
        is_long: Position direction
        size_usd: Amount to close in USD (None = close full position)
        max_slippage: Maximum acceptable slippage (e.g., 0.01 = 1%)
        protocol: Perpetuals protocol (default "gmx_v2")
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created
    """

    market: str
    collateral_token: str
    is_long: bool
    size_usd: OptionalSafeDecimal = None
    max_slippage: SafeDecimal = Field(default=Decimal("0.01"))
    protocol: str = "gmx_v2"
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_perp_close_intent(self) -> "PerpCloseIntent":
        """Validate perp close parameters."""
        if self.size_usd is not None and self.size_usd <= 0:
            raise ValueError("size_usd must be positive if specified")
        if self.max_slippage < 0 or self.max_slippage > 1:
            raise ValueError("max_slippage must be between 0 and 1")
        return self

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PERP_CLOSE

    @property
    def close_full_position(self) -> bool:
        """Check if this intent is to close the full position."""
        return self.size_usd is None

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PerpCloseIntent":
        """Deserialize a dictionary to a PerpCloseIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class HoldIntent(AlmanakImmutableModel):
    """Intent to take no action (wait).

    This is useful when a strategy explicitly decides not to act,
    as opposed to returning None which might indicate an error.

    Attributes:
        reason: Optional reason for holding (for logging/debugging)
        reason_code: Optional structured reason code for alerting/filtering
            (e.g., "INSUFFICIENT_BALANCE", "RSI_NEUTRAL", "PRICE_BELOW_THRESHOLD")
        reason_details: Optional structured details for the hold reason
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created
    """

    reason: str | None = None
    reason_code: str | None = None
    reason_details: dict[str, Any] | None = None
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.HOLD

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "HoldIntent":
        """Deserialize a dictionary to a HoldIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


# Forward declaration for FlashLoanIntent callback_intents type
FlashLoanCallbackIntent = Union[
    "CollectFeesIntent",
    "SwapIntent",
    "LPOpenIntent",
    "LPCloseIntent",
    "BorrowIntent",
    "RepayIntent",
    "SupplyIntent",
    "WithdrawIntent",
    "PerpOpenIntent",
    "PerpCloseIntent",
]


class FlashLoanIntent(AlmanakImmutableModel):
    """Intent to execute a flash loan with nested callback operations.

    A flash loan allows borrowing assets without collateral, provided the
    borrowed amount plus fees is repaid within the same transaction. This
    enables atomic arbitrage and other capital-efficient strategies.

    Attributes:
        provider: Flash loan provider ("aave", "balancer", or "auto" for automatic selection)
        token: Token to borrow via flash loan
        amount: Amount to borrow
        callback_intents: List of intents to execute with the borrowed funds.
                         These must return sufficient funds to repay the loan plus fees.
        chain: Target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Example:
        # Flash loan arbitrage: borrow USDC, swap through two DEXs
        Intent.flash_loan(
            provider="aave",
            token="USDC",
            amount=Decimal("100000"),
            callback_intents=[
                Intent.swap("USDC", "WETH", amount=Decimal("100000"), protocol="uniswap_v3"),
                Intent.swap("WETH", "USDC", amount="all", protocol="curve"),
            ],
            chain="ethereum"
        )

    Note:
        The callback_intents are executed atomically within the flash loan transaction.
        The final intent in callback_intents should return the borrowed token with
        sufficient amount to cover the loan amount plus provider fees.

        Provider fees:
        - Aave: 0.09% (9 bps)
        - Balancer: 0% (but limited liquidity)
    """

    model_config = {"arbitrary_types_allowed": True}

    provider: Literal["aave", "balancer", "morpho", "auto"]
    token: str
    amount: SafeDecimal
    callback_intents: list[FlashLoanCallbackIntent]
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_flash_loan_intent(self) -> "FlashLoanIntent":
        """Validate flash loan parameters."""
        if self.amount <= 0:
            raise ValueError("Flash loan amount must be positive")
        if not self.callback_intents:
            raise ValueError("Flash loan must have at least one callback intent")
        if self.provider not in ("aave", "balancer", "morpho", "auto"):
            raise ValueError(
                f"Invalid flash loan provider: {self.provider}. Must be 'aave', 'balancer', 'morpho', or 'auto'"
            )
        return self

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.FLASH_LOAN

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        return {
            "type": self.intent_type.value,
            "intent_id": self.intent_id,
            "created_at": self.created_at.isoformat(),
            "provider": self.provider,
            "token": self.token,
            "amount": str(self.amount),
            "callback_intents": [intent.serialize() for intent in self.callback_intents],
            "chain": self.chain,
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "FlashLoanIntent":
        """Deserialize a dictionary to a FlashLoanIntent."""
        callback_intents = []
        for intent_data in data.get("callback_intents", []):
            callback_intents.append(cls._deserialize_callback_intent(intent_data))

        clean_data = {
            "provider": data["provider"],
            "token": data["token"],
            "amount": data["amount"],
            "callback_intents": callback_intents,
            "chain": data.get("chain"),
            "intent_id": data.get("intent_id", str(uuid.uuid4())),
            "created_at": datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(UTC),
        }
        return cls.model_validate(clean_data)

    @staticmethod
    def _deserialize_callback_intent(data: dict[str, Any]) -> FlashLoanCallbackIntent:
        """Deserialize a callback intent (excludes FlashLoanIntent to prevent nesting)."""
        intent_type: str = data.get("type", "")
        deserializers: dict[str, type[FlashLoanCallbackIntent]] = {
            IntentType.SWAP.value: SwapIntent,
            IntentType.LP_OPEN.value: LPOpenIntent,
            IntentType.LP_CLOSE.value: LPCloseIntent,
            IntentType.LP_COLLECT_FEES.value: CollectFeesIntent,
            IntentType.BORROW.value: BorrowIntent,
            IntentType.REPAY.value: RepayIntent,
            IntentType.SUPPLY.value: SupplyIntent,
            IntentType.WITHDRAW.value: WithdrawIntent,
            IntentType.PERP_OPEN.value: PerpOpenIntent,
            IntentType.PERP_CLOSE.value: PerpCloseIntent,
        }
        deserializer = deserializers.get(intent_type)
        if deserializer is None:
            raise ValueError(f"Invalid callback intent type for flash loan: {intent_type}")
        return deserializer.deserialize(data)


class StakeIntent(AlmanakImmutableModel):
    """Intent to stake tokens with a liquid staking protocol.

    StakeIntent represents staking tokens (like ETH) with a liquid staking protocol
    (like Lido or Ethena) to receive a liquid staking derivative (like stETH or sUSDe).

    Attributes:
        protocol: Staking protocol (e.g., "lido", "ethena")
        token_in: Token to stake (e.g., "ETH" for Lido, "USDe" for Ethena)
        amount: Amount to stake, or "all" to use output from previous step
        receive_wrapped: Whether to receive the wrapped version (e.g., wstETH instead of stETH).
            Default is True for better DeFi composability.
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        When amount="all", the stake will use the entire output from the previous
        step in a sequence. This is useful for chaining operations like:
        swap -> stake or bridge -> stake.

        Protocol-specific behavior:
        - Lido: Stakes ETH, receives stETH (rebasing) or wstETH (non-rebasing)
        - Ethena: Stakes USDe, receives sUSDe (ERC4626 vault)

    Example:
        # Stake 1 ETH with Lido, receive wstETH (wrapped, non-rebasing)
        intent = Intent.stake(
            protocol="lido",
            token_in="ETH",
            amount=Decimal("1"),
            receive_wrapped=True,  # Get wstETH
            chain="ethereum",
        )

        # Stake USDe with Ethena, receive sUSDe
        intent = Intent.stake(
            protocol="ethena",
            token_in="USDe",
            amount=Decimal("10000"),
            chain="ethereum",
        )

        # Stake all ETH from previous step
        intent = Intent.stake(
            protocol="lido",
            token_in="ETH",
            amount="all",
            chain="ethereum",
        )
    """

    protocol: str
    token_in: str
    amount: PydanticChainedAmount
    receive_wrapped: bool = True
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_stake_intent(self) -> "StakeIntent":
        """Validate stake parameters."""
        if isinstance(self.amount, Decimal) and self.amount <= 0:
            raise ValueError("amount must be positive")
        elif not isinstance(self.amount, Decimal) and self.amount != "all":
            raise ValueError("amount must be a positive Decimal or 'all'")
        return self

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.amount == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.STAKE

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "StakeIntent":
        """Deserialize a dictionary to a StakeIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class UnstakeIntent(AlmanakImmutableModel):
    """Intent to unstake/withdraw tokens from a liquid staking protocol.

    UnstakeIntent represents withdrawing staked tokens from a liquid staking protocol
    (like Lido or Ethena) to receive back the underlying tokens.

    Attributes:
        protocol: Staking protocol (e.g., "lido", "ethena")
        token_in: Staked token to unstake (e.g., "wstETH" for Lido, "sUSDe" for Ethena)
        amount: Amount to unstake, or "all" to use output from previous step
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        When amount="all", the unstake will use the entire output from the previous
        step in a sequence. This is useful for chaining operations.

        Protocol-specific behavior:
        - Lido: Unwrap wstETH to stETH, or request withdrawal from stETH
        - Ethena: Initiates cooldown on sUSDe (unstaking has a cooldown period)

    Example:
        # Unstake 1 wstETH with Lido
        intent = Intent.unstake(
            protocol="lido",
            token_in="wstETH",
            amount=Decimal("1"),
            chain="ethereum",
        )

        # Unstake sUSDe with Ethena (starts cooldown)
        intent = Intent.unstake(
            protocol="ethena",
            token_in="sUSDe",
            amount=Decimal("10000"),
            chain="ethereum",
        )

        # Unstake all tokens from previous step
        intent = Intent.unstake(
            protocol="lido",
            token_in="wstETH",
            amount="all",
            chain="ethereum",
        )
    """

    protocol: str
    token_in: str
    amount: PydanticChainedAmount
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)
    protocol_params: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_unstake_intent(self) -> "UnstakeIntent":
        """Validate unstake parameters."""
        if isinstance(self.amount, Decimal) and self.amount <= 0:
            raise ValueError("amount must be positive")
        elif not isinstance(self.amount, Decimal) and self.amount != "all":
            raise ValueError("amount must be a positive Decimal or 'all'")
        return self

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.amount == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.UNSTAKE

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "UnstakeIntent":
        """Deserialize a dictionary to an UnstakeIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


# =============================================================================
# Prediction Market Intents
# =============================================================================

# Type aliases for prediction markets
PredictionOutcome = Literal["YES", "NO"]
PredictionOrderType = Literal["market", "limit"]
PredictionTimeInForce = Literal["GTC", "IOC", "FOK"]
# PredictionShareAmount uses PydanticChainedAmount for proper string->Decimal coercion
PredictionShareAmount = PydanticChainedAmount


class PredictionBuyIntent(AlmanakImmutableModel):
    """Intent to buy shares in a prediction market.

    This intent is used to buy outcome tokens (YES or NO) on Polymarket or
    similar prediction market platforms.

    Attributes:
        market_id: Polymarket market ID or slug (e.g., "will-bitcoin-exceed-100000")
        outcome: Which outcome to buy ("YES" or "NO")
        amount_usd: USDC amount to spend (mutually exclusive with shares)
        shares: Number of shares to buy (mutually exclusive with amount_usd)
        max_price: Maximum price per share (0.01-0.99) for limit orders
        order_type: Order type ("market" or "limit")
        time_in_force: How long order remains active ("GTC", "IOC", "FOK")
        expiration_hours: Hours until order expires (None = no expiry)
        protocol: Protocol to use (defaults to "polymarket")
        chain: Target chain (defaults to "polygon" for Polymarket)
        exit_conditions: Optional exit conditions for automatic position monitoring
            (stop-loss, take-profit, trailing stop, pre-resolution exit)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        - Prices represent implied probability (0.65 = 65% chance of YES)
        - Market orders use aggressive pricing for immediate execution
        - Limit orders rest in the orderbook until matched or cancelled
        - GTC (Good Till Cancelled) orders remain until filled or cancelled
        - IOC (Immediate or Cancel) fills what it can immediately, cancels rest
        - FOK (Fill or Kill) must fill entirely or is cancelled

    Example:
        # Buy $100 worth of YES shares at market price
        intent = Intent.prediction_buy(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            amount_usd=Decimal("100"),
        )

        # Buy 50 YES shares with limit order at max price of $0.65
        intent = Intent.prediction_buy(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            shares=Decimal("50"),
            max_price=Decimal("0.65"),
            order_type="limit",
        )
    """

    market_id: str
    outcome: PredictionOutcome
    amount_usd: OptionalSafeDecimal = None
    shares: OptionalSafeDecimal = None
    max_price: OptionalSafeDecimal = None
    order_type: PredictionOrderType = "market"
    time_in_force: PredictionTimeInForce = "GTC"
    expiration_hours: int | None = None
    protocol: str = "polymarket"
    chain: str | None = None
    exit_conditions: PredictionExitConditions | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_prediction_buy_intent(self) -> "PredictionBuyIntent":
        """Validate prediction buy parameters."""
        # Validate amount specification
        if self.amount_usd is None and self.shares is None:
            raise ValueError("Must specify either amount_usd or shares")
        if self.amount_usd is not None and self.shares is not None:
            raise ValueError("Cannot specify both amount_usd and shares")
        if self.amount_usd is not None and self.amount_usd <= 0:
            raise ValueError("amount_usd must be positive")
        if self.shares is not None and self.shares <= 0:
            raise ValueError("shares must be positive")

        # Validate max_price (0.01-0.99 for prediction markets)
        if self.max_price is not None:
            if self.max_price < Decimal("0.01") or self.max_price > Decimal("0.99"):
                raise ValueError("max_price must be between 0.01 and 0.99")

        # Limit orders require max_price
        if self.order_type == "limit" and self.max_price is None:
            raise ValueError("Limit orders require max_price to be specified")

        # Validate expiration_hours
        if self.expiration_hours is not None and self.expiration_hours <= 0:
            raise ValueError("expiration_hours must be positive")

        return self

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PREDICTION_BUY

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        # Serialize exit_conditions using its to_dict() method
        if self.exit_conditions is not None:
            data["exit_conditions"] = self.exit_conditions.to_dict()
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PredictionBuyIntent":
        """Deserialize a dictionary to a PredictionBuyIntent."""
        from almanak.framework.services.prediction_monitor import PredictionExitConditions

        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        # Deserialize exit_conditions from dict
        if "exit_conditions" in clean_data and clean_data["exit_conditions"] is not None:
            ec_data = clean_data["exit_conditions"]
            clean_data["exit_conditions"] = PredictionExitConditions(
                stop_loss_price=Decimal(ec_data["stop_loss_price"]) if ec_data.get("stop_loss_price") else None,
                take_profit_price=Decimal(ec_data["take_profit_price"]) if ec_data.get("take_profit_price") else None,
                exit_before_resolution_hours=ec_data.get("exit_before_resolution_hours"),
                trailing_stop_pct=Decimal(ec_data["trailing_stop_pct"]) if ec_data.get("trailing_stop_pct") else None,
                max_spread_pct=Decimal(ec_data["max_spread_pct"]) if ec_data.get("max_spread_pct") else None,
                min_liquidity_usd=Decimal(ec_data["min_liquidity_usd"]) if ec_data.get("min_liquidity_usd") else None,
            )
        return cls.model_validate(clean_data)


class PredictionSellIntent(AlmanakImmutableModel):
    """Intent to sell shares in a prediction market.

    This intent is used to sell outcome tokens (YES or NO) on Polymarket or
    similar prediction market platforms.

    Attributes:
        market_id: Polymarket market ID or slug
        outcome: Which outcome to sell ("YES" or "NO")
        shares: Number of shares to sell, or "all" to sell entire position
        min_price: Minimum price per share (0.01-0.99) for limit orders
        order_type: Order type ("market" or "limit")
        time_in_force: How long order remains active ("GTC", "IOC", "FOK")
        protocol: Protocol to use (defaults to "polymarket")
        chain: Target chain (defaults to "polygon" for Polymarket)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        - Use shares="all" to sell your entire position
        - Market orders execute immediately at best available price
        - Limit orders only execute at min_price or better

    Example:
        # Sell all YES shares at market price
        intent = Intent.prediction_sell(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            shares="all",
        )

        # Sell 25 NO shares with limit order at min $0.40
        intent = Intent.prediction_sell(
            market_id="will-bitcoin-exceed-100000",
            outcome="NO",
            shares=Decimal("25"),
            min_price=Decimal("0.40"),
            order_type="limit",
        )
    """

    market_id: str
    outcome: PredictionOutcome
    shares: PredictionShareAmount
    min_price: OptionalSafeDecimal = None
    order_type: PredictionOrderType = "market"
    time_in_force: PredictionTimeInForce = "GTC"
    protocol: str = "polymarket"
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_prediction_sell_intent(self) -> "PredictionSellIntent":
        """Validate prediction sell parameters."""
        # Validate shares
        if isinstance(self.shares, Decimal) and self.shares <= 0:
            raise ValueError("shares must be positive")
        elif not isinstance(self.shares, Decimal) and self.shares != "all":
            raise ValueError("shares must be a positive Decimal or 'all'")

        # Validate min_price (0.01-0.99 for prediction markets)
        if self.min_price is not None:
            if self.min_price < Decimal("0.01") or self.min_price > Decimal("0.99"):
                raise ValueError("min_price must be between 0.01 and 0.99")

        # Limit orders require min_price
        if self.order_type == "limit" and self.min_price is None:
            raise ValueError("Limit orders require min_price to be specified")

        return self

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.shares == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PREDICTION_SELL

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        # Preserve "all" literal
        if self.shares == "all":
            data["shares"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PredictionSellIntent":
        """Deserialize a dictionary to a PredictionSellIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class PredictionRedeemIntent(AlmanakImmutableModel):
    """Intent to redeem winning prediction market positions.

    This intent is used to redeem outcome tokens after a market has resolved.
    Winning tokens can be redeemed for $1 each (in USDC).

    Attributes:
        market_id: Polymarket market ID or slug
        outcome: Which outcome to redeem ("YES", "NO", or None for both)
        shares: Number of shares to redeem, or "all" (default)
        protocol: Protocol to use (defaults to "polymarket")
        chain: Target chain (defaults to "polygon" for Polymarket)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Note:
        - Redemption is only possible after the market has resolved
        - Winning positions redeem for $1 per share
        - Losing positions are worthless
        - Use outcome=None to redeem all winning positions

    Example:
        # Redeem all winning positions from a market
        intent = Intent.prediction_redeem(
            market_id="will-bitcoin-exceed-100000",
        )

        # Redeem only YES shares (if YES won)
        intent = Intent.prediction_redeem(
            market_id="will-bitcoin-exceed-100000",
            outcome="YES",
            shares="all",
        )
    """

    market_id: str
    outcome: PredictionOutcome | None = None
    shares: PredictionShareAmount = "all"
    protocol: str = "polymarket"
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_prediction_redeem_intent(self) -> "PredictionRedeemIntent":
        """Validate prediction redeem parameters."""
        # Validate shares
        if isinstance(self.shares, Decimal) and self.shares <= 0:
            raise ValueError("shares must be positive")
        elif not isinstance(self.shares, Decimal) and self.shares != "all":
            raise ValueError("shares must be a positive Decimal or 'all'")

        return self

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.shares == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.PREDICTION_REDEEM

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        # Preserve "all" literal
        if self.shares == "all":
            data["shares"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "PredictionRedeemIntent":
        """Deserialize a dictionary to a PredictionRedeemIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


# =============================================================================
# Vault Intent Data Classes (MetaMorpho ERC-4626)
# =============================================================================


class VaultDepositIntent(AlmanakImmutableModel):
    """Intent to deposit assets into a MetaMorpho ERC-4626 vault.

    Attributes:
        protocol: Vault protocol (must be "metamorpho")
        vault_address: MetaMorpho vault contract address
        amount: Amount of underlying assets to deposit (in token units), or "all"
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Example:
        intent = Intent.vault_deposit(
            protocol="metamorpho",
            vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
            amount=Decimal("1000"),
            chain="ethereum",
        )
    """

    protocol: str
    vault_address: str
    amount: PydanticChainedAmount
    deposit_token: str | None = None
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_vault_deposit_intent(self) -> "VaultDepositIntent":
        """Validate vault deposit parameters."""
        if isinstance(self.amount, Decimal) and self.amount <= 0:
            raise ValueError("amount must be positive")
        elif not isinstance(self.amount, Decimal) and self.amount != "all":
            raise ValueError("amount must be a positive Decimal or 'all'")
        if not self.vault_address.startswith("0x") or len(self.vault_address) != 42:
            raise ValueError(f"Invalid vault_address: {self.vault_address}. Must be 0x-prefixed 40 hex chars.")
        if self.protocol.lower() != "metamorpho":
            raise ValueError(f"Invalid protocol: {self.protocol}. Must be 'metamorpho'.")
        return self

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.amount == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.VAULT_DEPOSIT

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "VaultDepositIntent":
        """Deserialize a dictionary to a VaultDepositIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class VaultRedeemIntent(AlmanakImmutableModel):
    """Intent to redeem shares from a MetaMorpho ERC-4626 vault.

    Attributes:
        protocol: Vault protocol (must be "metamorpho")
        vault_address: MetaMorpho vault contract address
        shares: Number of vault shares to redeem, or "all" to redeem all
        chain: Optional target chain for execution (defaults to strategy's primary chain)
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Example:
        # Redeem specific amount of shares
        intent = Intent.vault_redeem(
            protocol="metamorpho",
            vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
            shares=Decimal("1000"),
            chain="ethereum",
        )

        # Redeem all shares
        intent = Intent.vault_redeem(
            protocol="metamorpho",
            vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
            shares="all",
            chain="ethereum",
        )
    """

    protocol: str
    vault_address: str
    shares: PydanticChainedAmount
    deposit_token: str | None = None
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_vault_redeem_intent(self) -> "VaultRedeemIntent":
        """Validate vault redeem parameters."""
        if isinstance(self.shares, Decimal) and self.shares <= 0:
            raise ValueError("shares must be positive")
        elif not isinstance(self.shares, Decimal) and self.shares != "all":
            raise ValueError("shares must be a positive Decimal or 'all'")
        if not self.vault_address.startswith("0x") or len(self.vault_address) != 42:
            raise ValueError(f"Invalid vault_address: {self.vault_address}. Must be 0x-prefixed 40 hex chars.")
        if self.protocol.lower() != "metamorpho":
            raise ValueError(f"Invalid protocol: {self.protocol}. Must be 'metamorpho'.")
        return self

    @property
    def is_chained_amount(self) -> bool:
        """Check if this intent uses a chained amount from previous step."""
        return self.shares == "all"

    @property
    def intent_type(self) -> IntentType:
        """Return the type of this intent."""
        return IntentType.VAULT_REDEEM

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.shares == "all":
            data["shares"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "VaultRedeemIntent":
        """Deserialize a dictionary to a VaultRedeemIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class WrapNativeIntent(AlmanakImmutableModel):
    """Intent to wrap native tokens (e.g. ETH -> WETH, MATIC -> WMATIC).

    Calls the wrapped token's ``deposit()`` function with ``msg.value`` to convert
    native currency to its wrapped ERC-20 equivalent.

    Attributes:
        token: Wrapped token symbol to receive (e.g. "WETH", "WMATIC", "WAVAX")
        amount: Amount of native token to wrap in token units (Decimal or "all")
        chain: Target chain for execution
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Example:
        intent = WrapNativeIntent(
            token="WETH",
            amount=Decimal("0.5"),
            chain="arbitrum",
        )
    """

    token: str
    amount: PydanticChainedAmount
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_wrap_intent(self) -> "WrapNativeIntent":
        """Validate wrap parameters."""
        if isinstance(self.amount, Decimal) and self.amount <= 0:
            raise ValueError("amount must be positive")
        elif not isinstance(self.amount, Decimal) and self.amount != "all":
            raise ValueError("amount must be a positive Decimal or 'all'")
        return self

    @property
    def is_chained_amount(self) -> bool:
        """Return True when amount depends on a prior step's output."""
        return self.amount == "all"

    @property
    def intent_type(self) -> IntentType:
        return IntentType.WRAP_NATIVE

    def serialize(self) -> dict[str, Any]:
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "WrapNativeIntent":
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


class UnwrapNativeIntent(AlmanakImmutableModel):
    """Intent to unwrap a wrapped native token (e.g. WETH -> ETH).

    Calls the wrapped token's ``withdraw(uint256)`` function to convert
    wrapped native tokens back to the chain's native currency.

    Attributes:
        token: Wrapped token symbol (e.g. "WETH", "WMATIC", "WAVAX")
        amount: Amount to unwrap in token units (Decimal or "all")
        chain: Target chain for execution
        intent_id: Unique identifier for this intent
        created_at: Timestamp when the intent was created

    Example:
        intent = UnwrapNativeIntent(
            token="WETH",
            amount=Decimal("0.5"),
            chain="arbitrum",
        )
    """

    token: str
    amount: PydanticChainedAmount
    chain: str | None = None
    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_unwrap_intent(self) -> "UnwrapNativeIntent":
        """Validate unwrap parameters."""
        if isinstance(self.amount, Decimal) and self.amount <= 0:
            raise ValueError("amount must be positive")
        elif not isinstance(self.amount, Decimal) and self.amount != "all":
            raise ValueError("amount must be a positive Decimal or 'all'")
        return self

    @property
    def is_chained_amount(self) -> bool:
        """Return True when amount depends on a prior step's output."""
        return self.amount == "all"

    @property
    def intent_type(self) -> IntentType:
        return IntentType.UNWRAP_NATIVE

    def serialize(self) -> dict[str, Any]:
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "UnwrapNativeIntent":
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)


# =============================================================================
# Union Type for All Intents
# =============================================================================

# Note: BridgeIntent is defined in .bridge module to avoid circular imports
# AnyIntent includes all base intents; BridgeIntent is handled dynamically
# in deserialize() and can be accessed via Intent.bridge() factory method
AnyIntent = (
    SwapIntent
    | LPOpenIntent
    | LPCloseIntent
    | CollectFeesIntent
    | BorrowIntent
    | RepayIntent
    | SupplyIntent
    | WithdrawIntent
    | PerpOpenIntent
    | PerpCloseIntent
    | FlashLoanIntent
    | StakeIntent
    | UnstakeIntent
    | HoldIntent
    | PredictionBuyIntent
    | PredictionSellIntent
    | PredictionRedeemIntent
    | VaultDepositIntent
    | VaultRedeemIntent
    | WrapNativeIntent
    | UnwrapNativeIntent
)


# =============================================================================
# Intent Sequence for Dependent Actions
# =============================================================================


@dataclass
class IntentSequence:
    """A sequence of intents that must execute in order (dependent actions).

    IntentSequence wraps a list of intents that have dependencies between them
    and must execute sequentially. This is used when the output of one intent
    feeds into the input of the next (e.g., swap output -> bridge input).

    Intents that are NOT in a sequence can execute in parallel if they are
    independent (e.g., two swaps on different chains).

    Attributes:
        intents: List of intents to execute in order
        sequence_id: Unique identifier for this sequence
        created_at: Timestamp when the sequence was created
        description: Optional description of the sequence purpose

    Example:
        # Create a sequence of dependent actions
        sequence = Intent.sequence([
            Intent.swap("USDC", "ETH", amount=Decimal("1000"), chain="base"),
            Intent.bridge(token="ETH", amount="all", from_chain="base", to_chain="arbitrum"),
            Intent.supply(protocol="aave_v3", token="WETH", amount="all", chain="arbitrum"),
        ])

        # Return from decide() - will execute sequentially
        return sequence
    """

    intents: list[AnyIntent]
    sequence_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    description: str | None = None

    def __post_init__(self) -> None:
        """Validate the sequence."""
        if not self.intents:
            raise InvalidSequenceError("IntentSequence must contain at least one intent")
        if len(self.intents) < 1:
            raise InvalidSequenceError("IntentSequence must contain at least one intent")

    def __len__(self) -> int:
        """Return the number of intents in the sequence."""
        return len(self.intents)

    def __iter__(self):
        """Iterate over intents in the sequence."""
        return iter(self.intents)

    def __getitem__(self, index: int) -> AnyIntent:
        """Get intent at index."""
        return self.intents[index]

    @property
    def first(self) -> AnyIntent:
        """Get the first intent in the sequence."""
        return self.intents[0]

    @property
    def last(self) -> AnyIntent:
        """Get the last intent in the sequence."""
        return self.intents[-1]

    def serialize(self) -> dict[str, Any]:
        """Serialize the sequence to a dictionary."""
        return {
            "type": "SEQUENCE",
            "sequence_id": self.sequence_id,
            "created_at": self.created_at.isoformat(),
            "description": self.description,
            "intents": [intent.serialize() for intent in self.intents],
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "IntentSequence":
        """Deserialize a dictionary to an IntentSequence.

        Note: This requires the Intent.deserialize function to be available,
        which creates a circular dependency. The actual deserialization is
        done in the Intent class.
        """
        from .vocabulary import Intent  # Import here to avoid circular import

        intents = [Intent.deserialize(intent_data) for intent_data in data["intents"]]
        return cls(
            intents=intents,
            sequence_id=data.get("sequence_id", str(uuid.uuid4())),
            created_at=datetime.fromisoformat(data["created_at"]) if data.get("created_at") else datetime.now(UTC),
            description=data.get("description"),
        )


# Type for decide() return value: single intent, sequence, or list of parallel intents
DecideResult = AnyIntent | IntentSequence | list[AnyIntent | IntentSequence] | None


# =============================================================================
# Intent Factory Class
# =============================================================================


class Intent:
    """Factory class for creating intents with a fluent API.

    This class provides static factory methods for creating intents,
    making strategy code more readable and ergonomic.

    Example:
        # Instead of:
        intent = SwapIntent(from_token="USDC", to_token="ETH", amount_usd=Decimal("1000"))

        # You can write:
        intent = Intent.swap(from_token="USDC", to_token="ETH", amount_usd=Decimal("1000"))
    """

    @staticmethod
    def swap(
        from_token: str,
        to_token: str,
        amount_usd: Decimal | None = None,
        amount: ChainedAmount | None = None,
        max_slippage: Decimal = Decimal("0.005"),
        max_price_impact: Decimal | None = None,
        protocol: str | None = None,
        chain: str | None = None,
        destination_chain: str | None = None,
    ) -> SwapIntent:
        """Create a swap intent.

        Args:
            from_token: Symbol or address of the token to swap from
            to_token: Symbol or address of the token to swap to
            amount_usd: Amount to swap in USD terms
            amount: Amount to swap in token terms, or "all" to use previous step output
            max_slippage: Maximum acceptable slippage (default 0.5%)
            max_price_impact: Maximum acceptable price impact vs oracle price (e.g., 0.50 = 50%).
                Compilation fails if quoter/oracle deviation exceeds this.
                Defaults to None (uses compiler config default of 30%).
            protocol: Preferred protocol for the swap
            chain: Source chain for execution (defaults to strategy's primary chain)
            destination_chain: Destination chain for cross-chain swaps (None for same-chain)

        Returns:
            SwapIntent: The created swap intent

        Example:
            # Swap $1000 worth of USDC to ETH
            intent = Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))

            # Swap 0.5 ETH to USDC on Base
            intent = Intent.swap("ETH", "USDC", amount=Decimal("0.5"), chain="base")

            # Swap all ETH from previous step output
            intent = Intent.swap("ETH", "USDC", amount="all", chain="base")

            # Cross-chain swap: Base USDC -> Arbitrum WETH via Enso
            intent = Intent.swap("USDC", "WETH", amount_usd=Decimal("1000"),
                                 chain="base", destination_chain="arbitrum", protocol="enso")
        """
        return SwapIntent(
            from_token=from_token,
            to_token=to_token,
            amount_usd=amount_usd,
            amount=amount,
            max_slippage=max_slippage,
            max_price_impact=max_price_impact,
            protocol=protocol,
            chain=chain,
            destination_chain=destination_chain,
        )

    @staticmethod
    def lp_open(
        pool: str,
        amount0: Decimal,
        amount1: Decimal,
        range_lower: Decimal,
        range_upper: Decimal,
        protocol: str = "uniswap_v3",
        chain: str | None = None,
        protocol_params: dict[str, Any] | None = None,
    ) -> LPOpenIntent:
        """Create an LP open intent.

        Args:
            pool: Pool address or identifier
            amount0: Amount of token0 to provide
            amount1: Amount of token1 to provide
            range_lower: Lower price bound for concentrated liquidity
            range_upper: Upper price bound for concentrated liquidity
            protocol: LP protocol (default "uniswap_v3")
            chain: Target chain for execution (defaults to strategy's primary chain)
            protocol_params: Optional protocol-specific parameters (e.g., {"bin_range": 10} for TraderJoe V2)

        Returns:
            LPOpenIntent: The created LP open intent

        Example:
            # Open an ETH/USDC LP position around the current price
            intent = Intent.lp_open(
                pool="0x8ad...",
                amount0=Decimal("1"),  # 1 ETH
                amount1=Decimal("2000"),  # 2000 USDC
                range_lower=Decimal("1800"),
                range_upper=Decimal("2200"),
            )
        """
        return LPOpenIntent(
            pool=pool,
            amount0=amount0,
            amount1=amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol=protocol,
            chain=chain,
            protocol_params=protocol_params,
        )

    @staticmethod
    def lp_close(
        position_id: str,
        pool: str | None = None,
        collect_fees: bool = True,
        protocol: str = "uniswap_v3",
        chain: str | None = None,
    ) -> LPCloseIntent:
        """Create an LP close intent.

        Args:
            position_id: Identifier of the position to close
            pool: Pool address (optional, for validation)
            collect_fees: Whether to collect accumulated fees (default True)
            protocol: LP protocol (default "uniswap_v3")
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            LPCloseIntent: The created LP close intent

        Example:
            # Close an LP position and collect fees
            intent = Intent.lp_close(position_id="12345")

            # Close without collecting fees
            intent = Intent.lp_close(position_id="12345", collect_fees=False)
        """
        return LPCloseIntent(
            position_id=position_id,
            pool=pool,
            collect_fees=collect_fees,
            protocol=protocol,
            chain=chain,
        )

    @staticmethod
    def collect_fees(
        pool: str,
        protocol: str = "traderjoe_v2",
        chain: str | None = None,
    ) -> CollectFeesIntent:
        """Create a collect fees intent to harvest LP fees without closing the position.

        Args:
            pool: Pool identifier (e.g., "WAVAX/USDC/20" for TraderJoe V2)
            protocol: LP protocol (default "traderjoe_v2")
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            CollectFeesIntent: The created collect fees intent

        Example:
            # Collect fees from a TraderJoe V2 WAVAX/USDC LP position
            intent = Intent.collect_fees(pool="WAVAX/USDC/20", protocol="traderjoe_v2")
        """
        return CollectFeesIntent(
            pool=pool,
            protocol=protocol,
            chain=chain,
        )

    @staticmethod
    def borrow(
        protocol: str,
        collateral_token: str,
        collateral_amount: ChainedAmount,
        borrow_token: str,
        borrow_amount: Decimal,
        interest_rate_mode: InterestRateMode | None = None,
        market_id: str | None = None,
        chain: str | None = None,
    ) -> BorrowIntent:
        """Create a borrow intent.

        Args:
            protocol: Lending protocol (e.g., "aave_v3", "morpho_blue")
            collateral_token: Token to use as collateral
            collateral_amount: Amount of collateral to supply, or "all" for previous step output
            borrow_token: Token to borrow
            borrow_amount: Amount to borrow
            interest_rate_mode: Interest rate mode for Aave ('variable' only, stable is deprecated).
                Only applies to protocols that support rate mode selection.
                For Aave V3, defaults to 'variable' if not specified.
            market_id: Market identifier for isolated lending protocols (e.g., Morpho Blue).
                Required for morpho/morpho_blue, ignored for aave_v3.
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            BorrowIntent: The created borrow intent

        Example:
            # Supply ETH as collateral and borrow USDC on Arbitrum with variable rate
            intent = Intent.borrow(
                protocol="aave_v3",
                collateral_token="ETH",
                collateral_amount=Decimal("1"),
                borrow_token="USDC",
                borrow_amount=Decimal("1500"),
                interest_rate_mode="variable",
                chain="arbitrum",
            )

            # Borrow on Morpho Blue (requires market_id)
            intent = Intent.borrow(
                protocol="morpho_blue",
                collateral_token="wstETH",
                collateral_amount=Decimal("0"),  # Already supplied
                borrow_token="USDC",
                borrow_amount=Decimal("1500"),
                market_id="0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
                chain="ethereum",
            )
        """
        return BorrowIntent(
            protocol=protocol,
            collateral_token=collateral_token,
            collateral_amount=collateral_amount,
            borrow_token=borrow_token,
            borrow_amount=borrow_amount,
            interest_rate_mode=interest_rate_mode,
            market_id=market_id,
            chain=chain,
        )

    @staticmethod
    def repay(
        protocol: str,
        token: str,
        amount: ChainedAmount,
        repay_full: bool = False,
        interest_rate_mode: InterestRateMode | None = None,
        market_id: str | None = None,
        chain: str | None = None,
    ) -> RepayIntent:
        """Create a repay intent.

        Args:
            protocol: Lending protocol (e.g., "aave_v3", "morpho_blue")
            token: Token to repay
            amount: Amount to repay, or "all" to use previous step output
            repay_full: If True, repay the full outstanding debt
            interest_rate_mode: Interest rate mode for protocols that support it.
                Aave V3: 'variable' (default). Stable rate is deprecated. Must match
                the rate mode used when borrowing.
            market_id: Market identifier for isolated lending protocols (e.g., Morpho Blue).
                Required for morpho/morpho_blue, ignored for aave_v3.
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            RepayIntent: The created repay intent

        Example:
            # Repay 500 USDC on Aave (variable rate)
            intent = Intent.repay(
                protocol="aave_v3",
                token="USDC",
                amount=Decimal("500"),
                interest_rate_mode="variable",
            )

            # Repay full debt on Morpho Blue
            intent = Intent.repay(
                protocol="morpho_blue",
                token="USDC",
                amount=Decimal("0"),  # Ignored when repay_full=True
                repay_full=True,
                market_id="0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
            )
        """
        return RepayIntent(
            protocol=protocol,
            token=token,
            amount=amount,
            repay_full=repay_full,
            interest_rate_mode=interest_rate_mode,
            market_id=market_id,
            chain=chain,
        )

    @staticmethod
    def supply(
        protocol: str,
        token: str,
        amount: ChainedAmount,
        use_as_collateral: bool = True,
        market_id: str | None = None,
        chain: str | None = None,
    ) -> SupplyIntent:
        """Create a supply intent.

        Args:
            protocol: Lending protocol (e.g., "aave_v3", "morpho_blue")
            token: Token to supply
            amount: Amount to supply, or "all" to use previous step output
            use_as_collateral: Whether to enable as collateral (default True)
            market_id: Market identifier for isolated lending protocols (e.g., Morpho Blue).
                Required for morpho/morpho_blue, ignored for aave_v3.
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            SupplyIntent: The created supply intent

        Example:
            # Supply 1 ETH to Aave V3 on Arbitrum
            intent = Intent.supply(
                protocol="aave_v3",
                token="WETH",
                amount=Decimal("1"),
                chain="arbitrum",
            )

            # Supply wstETH to Morpho Blue market
            intent = Intent.supply(
                protocol="morpho_blue",
                token="wstETH",
                amount=Decimal("1"),
                market_id="0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
                chain="ethereum",
            )
        """
        return SupplyIntent(
            protocol=protocol,
            token=token,
            amount=amount,
            use_as_collateral=use_as_collateral,
            market_id=market_id,
            chain=chain,
        )

    @staticmethod
    def withdraw(
        protocol: str,
        token: str,
        amount: ChainedAmount,
        withdraw_all: bool = False,
        market_id: str | None = None,
        chain: str | None = None,
    ) -> WithdrawIntent:
        """Create a withdraw intent.

        Args:
            protocol: Lending protocol (e.g., "aave_v3", "morpho_blue")
            token: Token to withdraw
            amount: Amount to withdraw, or "all" to use previous step output
            withdraw_all: If True, withdraw all available balance
            market_id: Market identifier for isolated lending protocols (e.g., Morpho Blue).
                Required for morpho/morpho_blue, ignored for aave_v3.
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            WithdrawIntent: The created withdraw intent

        Example:
            # Withdraw 0.5 ETH from Aave V3
            intent = Intent.withdraw(
                protocol="aave_v3",
                token="WETH",
                amount=Decimal("0.5"),
            )

            # Withdraw all collateral from Morpho Blue
            intent = Intent.withdraw(
                protocol="morpho_blue",
                token="wstETH",
                amount=Decimal("0"),  # Ignored when withdraw_all=True
                withdraw_all=True,
                market_id="0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
            )
        """
        return WithdrawIntent(
            protocol=protocol,
            token=token,
            amount=amount,
            withdraw_all=withdraw_all,
            market_id=market_id,
            chain=chain,
        )

    @staticmethod
    def perp_open(
        market: str,
        collateral_token: str,
        collateral_amount: ChainedAmount,
        size_usd: Decimal,
        is_long: bool = True,
        leverage: Decimal = Decimal("1"),
        max_slippage: Decimal = Decimal("0.01"),
        protocol: str = "gmx_v2",
        chain: str | None = None,
    ) -> PerpOpenIntent:
        """Create a perpetual position open intent.

        Args:
            market: Market identifier (e.g., "ETH/USD") or market address
            collateral_token: Token symbol or address for collateral
            collateral_amount: Amount of collateral in token terms, or "all" for previous step output
            size_usd: Position size in USD terms
            is_long: True for long, False for short (default True)
            leverage: Target leverage (default 1x)
            max_slippage: Maximum acceptable slippage (default 1%)
            protocol: Perpetuals protocol (default "gmx_v2")
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            PerpOpenIntent: The created perp open intent

        Example:
            # Open a 5x long ETH position with 0.1 ETH collateral on Arbitrum
            intent = Intent.perp_open(
                market="ETH/USD",
                collateral_token="WETH",
                collateral_amount=Decimal("0.1"),
                size_usd=Decimal("1750"),  # ~5x at $3500 ETH
                is_long=True,
                leverage=Decimal("5"),
                chain="arbitrum",
            )

            # Use all collateral from previous step
            intent = Intent.perp_open(
                market="ETH/USD",
                collateral_token="WETH",
                collateral_amount="all",  # Use previous step output
                size_usd=Decimal("1750"),
                is_long=True,
                chain="arbitrum",
            )
        """
        return PerpOpenIntent(
            market=market,
            collateral_token=collateral_token,
            collateral_amount=collateral_amount,
            size_usd=size_usd,
            is_long=is_long,
            leverage=leverage,
            max_slippage=max_slippage,
            protocol=protocol,
            chain=chain,
        )

    @staticmethod
    def perp_close(
        market: str,
        collateral_token: str,
        is_long: bool,
        size_usd: Decimal | None = None,
        max_slippage: Decimal = Decimal("0.01"),
        protocol: str = "gmx_v2",
        chain: str | None = None,
    ) -> PerpCloseIntent:
        """Create a perpetual position close intent.

        Args:
            market: Market identifier (e.g., "ETH/USD") or market address
            collateral_token: Token symbol or address for collateral
            is_long: Position direction
            size_usd: Amount to close in USD (None = close full position)
            max_slippage: Maximum acceptable slippage (default 1%)
            protocol: Perpetuals protocol (default "gmx_v2")
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            PerpCloseIntent: The created perp close intent

        Example:
            # Close entire long ETH position
            intent = Intent.perp_close(
                market="ETH/USD",
                collateral_token="WETH",
                is_long=True,
            )

            # Close $500 of position
            intent = Intent.perp_close(
                market="ETH/USD",
                collateral_token="WETH",
                is_long=True,
                size_usd=Decimal("500"),
            )
        """
        return PerpCloseIntent(
            market=market,
            collateral_token=collateral_token,
            is_long=is_long,
            size_usd=size_usd,
            max_slippage=max_slippage,
            protocol=protocol,
            chain=chain,
        )

    @staticmethod
    def bridge(
        token: str,
        amount: Decimal | Literal["all"],
        from_chain: str,
        to_chain: str,
        max_slippage: Decimal = Decimal("0.005"),
        preferred_bridge: str | None = None,
        destination_address: str | None = None,
    ) -> Any:
        """Create a bridge intent for cross-chain asset transfer.

        Bridge intents represent cross-chain token transfers. They can be used
        standalone or as part of an IntentSequence for complex multi-step
        operations like swap -> bridge -> supply.

        When amount="all", the bridge will use the entire output from the
        previous step in a sequence. This is useful for chaining operations.

        Args:
            token: Token symbol to bridge (e.g., "ETH", "USDC", "WBTC")
            amount: Amount to bridge (Decimal) or "all" to use previous step's output
            from_chain: Source chain identifier (e.g., "base", "arbitrum")
            to_chain: Destination chain identifier (e.g., "arbitrum", "optimism")
            max_slippage: Maximum acceptable slippage (default 0.5%)
            preferred_bridge: Optional preferred bridge adapter name (e.g., "across", "stargate")
            destination_address: Optional recipient address on the destination chain.
                If None, the compiler resolves it from chain_wallets (multi-wallet mode)
                or uses the source wallet address (single-wallet mode).

        Returns:
            BridgeIntent: The created bridge intent

        Example:
            # Bridge 1000 USDC from Base to Arbitrum
            intent = Intent.bridge(
                token="USDC",
                amount=Decimal("1000"),
                from_chain="base",
                to_chain="arbitrum",
            )

            # Bridge all ETH from previous step (in a sequence)
            sequence = Intent.sequence([
                Intent.swap("USDC", "ETH", amount=Decimal("1000"), chain="base"),
                Intent.bridge(
                    token="ETH",
                    amount="all",  # Use output from swap
                    from_chain="base",
                    to_chain="arbitrum",
                ),
                Intent.supply(protocol="aave_v3", token="WETH", amount="all", chain="arbitrum"),
            ])

            # Bridge with preferred bridge
            intent = Intent.bridge(
                token="USDC",
                amount=Decimal("5000"),
                from_chain="arbitrum",
                to_chain="optimism",
                preferred_bridge="across",  # Prefer Across for fast finality
            )
        """
        # Import here to avoid circular import
        from .bridge import BridgeIntent

        return BridgeIntent(
            token=token,
            amount=amount,
            from_chain=from_chain,
            to_chain=to_chain,
            max_slippage=max_slippage,
            preferred_bridge=preferred_bridge,
            destination_address=destination_address,
        )

    @staticmethod
    def flash_loan(
        provider: Literal["aave", "balancer", "morpho", "auto"],
        token: str,
        amount: Decimal,
        callback_intents: list[FlashLoanCallbackIntent],
        chain: str | None = None,
    ) -> FlashLoanIntent:
        """Create a flash loan intent with callback operations.

        A flash loan allows borrowing assets without collateral, provided the
        borrowed amount plus fees is repaid within the same transaction.

        Args:
            provider: Flash loan provider ("aave", "balancer", or "auto")
                     - "aave": 0.09% fee, high liquidity
                     - "balancer": 0% fee, lower liquidity
                     - "auto": Automatically select based on availability and fees
            token: Token to borrow via flash loan
            amount: Amount to borrow
            callback_intents: List of intents to execute with borrowed funds.
                            Must return sufficient funds to repay loan + fees.
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            FlashLoanIntent: The created flash loan intent

        Example:
            # Flash loan arbitrage: borrow USDC, swap through two DEXs
            intent = Intent.flash_loan(
                provider="aave",
                token="USDC",
                amount=Decimal("100000"),
                callback_intents=[
                    Intent.swap("USDC", "WETH", amount=Decimal("100000"), protocol="uniswap_v3"),
                    Intent.swap("WETH", "USDC", amount="all", protocol="curve"),
                ],
                chain="ethereum"
            )
        """
        return FlashLoanIntent(
            provider=provider,
            token=token,
            amount=amount,
            callback_intents=callback_intents,
            chain=chain,
        )

    @staticmethod
    def hold(
        reason: str | None = None,
        chain: str | None = None,
        reason_code: str | None = None,
        reason_details: dict[str, Any] | None = None,
    ) -> HoldIntent:
        """Create a hold intent (no action).

        Args:
            reason: Optional reason for holding (for logging/debugging)
            chain: Target chain for execution (defaults to strategy's primary chain)
            reason_code: Optional structured reason code for alerting/filtering
                (e.g., "INSUFFICIENT_BALANCE", "RSI_NEUTRAL")
            reason_details: Optional structured details for the hold reason

        Returns:
            HoldIntent: The created hold intent

        Example:
            # Hold with no reason
            intent = Intent.hold()

            # Hold with a reason for logging
            intent = Intent.hold(reason="RSI in neutral zone, waiting for signal")

            # Hold with structured reason for alerting
            intent = Intent.hold(
                reason="RSI neutral",
                reason_code="RSI_NEUTRAL",
                reason_details={"rsi": 52.3, "oversold": 30, "overbought": 70},
            )
        """
        return HoldIntent(reason=reason, chain=chain, reason_code=reason_code, reason_details=reason_details)

    @staticmethod
    def stake(
        protocol: str,
        token_in: str,
        amount: ChainedAmount,
        receive_wrapped: bool = True,
        chain: str | None = None,
    ) -> StakeIntent:
        """Create a stake intent for liquid staking protocols.

        Args:
            protocol: Staking protocol (e.g., "lido", "ethena")
            token_in: Token to stake (e.g., "ETH" for Lido, "USDe" for Ethena)
            amount: Amount to stake, or "all" to use previous step output
            receive_wrapped: Whether to receive wrapped version (default True).
                For Lido: True = wstETH (non-rebasing), False = stETH (rebasing)
                For Ethena: Always receives sUSDe regardless of this flag
            chain: Target chain for execution (defaults to strategy's primary chain)

        Returns:
            StakeIntent: The created stake intent

        Example:
            # Stake 1 ETH with Lido on Ethereum, receive wstETH
            intent = Intent.stake(
                protocol="lido",
                token_in="ETH",
                amount=Decimal("1"),
                receive_wrapped=True,
                chain="ethereum",
            )

            # Stake USDe with Ethena
            intent = Intent.stake(
                protocol="ethena",
                token_in="USDe",
                amount=Decimal("10000"),
                chain="ethereum",
            )

            # Stake all ETH from previous step in a sequence
            intent = Intent.stake(
                protocol="lido",
                token_in="ETH",
                amount="all",
                chain="ethereum",
            )
        """
        return StakeIntent(
            protocol=protocol,
            token_in=token_in,
            amount=amount,
            receive_wrapped=receive_wrapped,
            chain=chain,
        )

    @staticmethod
    def unstake(
        protocol: str,
        token_in: str,
        amount: ChainedAmount,
        chain: str | None = None,
        protocol_params: "dict[str, Any] | None" = None,
    ) -> UnstakeIntent:
        """Create an unstake intent for withdrawing from liquid staking protocols.

        Args:
            protocol: Staking protocol (e.g., "lido", "ethena")
            token_in: Staked token to unstake (e.g., "wstETH" for Lido, "sUSDe" for Ethena)
            amount: Amount to unstake, or "all" to use previous step output
            chain: Target chain for execution (defaults to strategy's primary chain)
            protocol_params: Optional protocol-specific parameters (e.g., {"phase": "cooldown"} for Ethena)

        Returns:
            UnstakeIntent: The created unstake intent

        Example:
            # Unstake 1 wstETH with Lido on Ethereum
            intent = Intent.unstake(
                protocol="lido",
                token_in="wstETH",
                amount=Decimal("1"),
                chain="ethereum",
            )

            # Unstake sUSDe with Ethena (initiates cooldown)
            intent = Intent.unstake(
                protocol="ethena",
                token_in="sUSDe",
                amount=Decimal("10000"),
                chain="ethereum",
            )

            # Unstake all tokens from previous step in a sequence
            intent = Intent.unstake(
                protocol="lido",
                token_in="wstETH",
                amount="all",
                chain="ethereum",
            )
        """
        return UnstakeIntent(
            protocol=protocol,
            token_in=token_in,
            amount=amount,
            chain=chain,
            protocol_params=protocol_params,
        )

    @staticmethod
    def ensure_balance(
        token: str,
        min_amount: Decimal,
        target_chain: str,
        max_slippage: Decimal = Decimal("0.005"),
        preferred_bridge: str | None = None,
    ) -> Any:
        """Create an ensure_balance intent for automatic cross-chain balance management.

        EnsureBalanceIntent expresses the goal of having at least a certain amount
        of tokens on a specific chain. When resolved (via resolve() method), the
        system will automatically determine the appropriate action:

        1. If target chain has sufficient balance -> HoldIntent (no action)
        2. If another chain has sufficient balance -> BridgeIntent (transfer)
        3. If no single chain has enough -> InsufficientBalanceError

        This simplifies strategy development by abstracting away the complexity of
        cross-chain balance management.

        Args:
            token: Token symbol to ensure (e.g., "ETH", "USDC", "WBTC")
            min_amount: Minimum amount required on target chain
            target_chain: Chain where the balance is needed (e.g., "arbitrum", "base")
            max_slippage: Maximum acceptable slippage for bridging (default 0.5%)
            preferred_bridge: Optional preferred bridge adapter name for transfer

        Returns:
            EnsureBalanceIntent: The created ensure_balance intent

        Example:
            # Ensure at least 1000 USDC on Arbitrum before opening a position
            intent = Intent.ensure_balance(
                token="USDC",
                min_amount=Decimal("1000"),
                target_chain="arbitrum",
            )

            # Ensure at least 2 ETH on Base with custom slippage
            intent = Intent.ensure_balance(
                token="ETH",
                min_amount=Decimal("2"),
                target_chain="base",
                max_slippage=Decimal("0.01"),  # 1% max slippage
                preferred_bridge="across",  # Prefer Across bridge
            )

            # Using ensure_balance in a strategy
            def decide(self, market: MultiChainMarketSnapshot) -> DecideResult:
                # First ensure we have enough USDC on Arbitrum
                ensure_intent = Intent.ensure_balance(
                    token="USDC",
                    min_amount=Decimal("5000"),
                    target_chain="arbitrum",
                )

                # Resolve to concrete intent based on current balances
                target_balance = market.balance("USDC", chain="arbitrum").balance
                chain_balances = {
                    chain: market.balance("USDC", chain=chain).balance
                    for chain in market.chains
                    if chain != "arbitrum"
                }
                resolved_intent = ensure_intent.resolve(target_balance, chain_balances)

                # If resolved to HoldIntent, we can proceed with other actions
                # If resolved to BridgeIntent, execute the bridge first
                return resolved_intent
        """
        # Import here to avoid circular import
        from .ensure_balance import EnsureBalanceIntent

        return EnsureBalanceIntent(
            token=token,
            min_amount=min_amount,
            target_chain=target_chain,
            max_slippage=max_slippage,
            preferred_bridge=preferred_bridge,
        )

    @staticmethod
    def prediction_buy(
        market_id: str,
        outcome: Literal["YES", "NO"],
        amount_usd: Decimal | None = None,
        shares: Decimal | None = None,
        max_price: Decimal | None = None,
        order_type: Literal["market", "limit"] = "market",
        time_in_force: Literal["GTC", "IOC", "FOK"] = "GTC",
        expiration_hours: int | None = None,
        protocol: str = "polymarket",
        chain: str | None = None,
        exit_conditions: PredictionExitConditions | None = None,
    ) -> PredictionBuyIntent:
        """Create a prediction buy intent for purchasing outcome shares.

        Buy outcome tokens (YES or NO) on a prediction market like Polymarket.
        Prices represent implied probability (e.g., 0.65 = 65% chance).

        Args:
            market_id: Polymarket market ID or slug (e.g., "will-bitcoin-exceed-100000")
            outcome: Which outcome to buy ("YES" or "NO")
            amount_usd: USDC amount to spend (mutually exclusive with shares)
            shares: Number of shares to buy (mutually exclusive with amount_usd)
            max_price: Maximum price per share (0.01-0.99) for limit orders
            order_type: Order type ("market" or "limit", default "market")
            time_in_force: How long order remains active ("GTC", "IOC", "FOK")
            expiration_hours: Hours until order expires (None = no expiry)
            protocol: Protocol to use (defaults to "polymarket")
            chain: Target chain (defaults to "polygon" for Polymarket)
            exit_conditions: Optional exit conditions for automatic position monitoring
                (stop-loss, take-profit, trailing stop, pre-resolution exit)

        Returns:
            PredictionBuyIntent: The created prediction buy intent

        Example:
            # Buy $100 worth of YES shares at market price
            intent = Intent.prediction_buy(
                market_id="will-bitcoin-exceed-100000",
                outcome="YES",
                amount_usd=Decimal("100"),
            )

            # Buy 50 YES shares with limit order at max price of $0.65
            intent = Intent.prediction_buy(
                market_id="will-bitcoin-exceed-100000",
                outcome="YES",
                shares=Decimal("50"),
                max_price=Decimal("0.65"),
                order_type="limit",
            )

            # Buy NO shares with IOC (immediate or cancel)
            intent = Intent.prediction_buy(
                market_id="will-bitcoin-exceed-100000",
                outcome="NO",
                amount_usd=Decimal("200"),
                time_in_force="IOC",
            )
        """
        return PredictionBuyIntent(
            market_id=market_id,
            outcome=outcome,
            amount_usd=amount_usd,
            shares=shares,
            max_price=max_price,
            order_type=order_type,
            time_in_force=time_in_force,
            expiration_hours=expiration_hours,
            protocol=protocol,
            chain=chain,
            exit_conditions=exit_conditions,
        )

    @staticmethod
    def prediction_sell(
        market_id: str,
        outcome: Literal["YES", "NO"],
        shares: Decimal | Literal["all"],
        min_price: Decimal | None = None,
        order_type: Literal["market", "limit"] = "market",
        time_in_force: Literal["GTC", "IOC", "FOK"] = "GTC",
        protocol: str = "polymarket",
        chain: str | None = None,
    ) -> PredictionSellIntent:
        """Create a prediction sell intent for selling outcome shares.

        Sell outcome tokens (YES or NO) on a prediction market like Polymarket.
        Use shares="all" to sell your entire position.

        Args:
            market_id: Polymarket market ID or slug
            outcome: Which outcome to sell ("YES" or "NO")
            shares: Number of shares to sell, or "all" to sell entire position
            min_price: Minimum price per share (0.01-0.99) for limit orders
            order_type: Order type ("market" or "limit", default "market")
            time_in_force: How long order remains active ("GTC", "IOC", "FOK")
            protocol: Protocol to use (defaults to "polymarket")
            chain: Target chain (defaults to "polygon" for Polymarket)

        Returns:
            PredictionSellIntent: The created prediction sell intent

        Example:
            # Sell all YES shares at market price
            intent = Intent.prediction_sell(
                market_id="will-bitcoin-exceed-100000",
                outcome="YES",
                shares="all",
            )

            # Sell 25 NO shares with limit order at min $0.40
            intent = Intent.prediction_sell(
                market_id="will-bitcoin-exceed-100000",
                outcome="NO",
                shares=Decimal("25"),
                min_price=Decimal("0.40"),
                order_type="limit",
            )
        """
        return PredictionSellIntent(
            market_id=market_id,
            outcome=outcome,
            shares=shares,
            min_price=min_price,
            order_type=order_type,
            time_in_force=time_in_force,
            protocol=protocol,
            chain=chain,
        )

    @staticmethod
    def prediction_redeem(
        market_id: str,
        outcome: Literal["YES", "NO"] | None = None,
        shares: Decimal | Literal["all"] = "all",
        protocol: str = "polymarket",
        chain: str | None = None,
    ) -> PredictionRedeemIntent:
        """Create a prediction redeem intent for redeeming winning positions.

        Redeem winning outcome tokens after a market has resolved. Winning
        positions redeem for $1 per share in USDC.

        Args:
            market_id: Polymarket market ID or slug
            outcome: Which outcome to redeem ("YES", "NO", or None for both)
            shares: Number of shares to redeem, or "all" (default)
            protocol: Protocol to use (defaults to "polymarket")
            chain: Target chain (defaults to "polygon" for Polymarket)

        Returns:
            PredictionRedeemIntent: The created prediction redeem intent

        Note:
            Redemption is only possible after the market has resolved.
            Losing positions are worthless and cannot be redeemed.

        Example:
            # Redeem all winning positions from a resolved market
            intent = Intent.prediction_redeem(
                market_id="will-bitcoin-exceed-100000",
            )

            # Redeem only YES shares (if YES won)
            intent = Intent.prediction_redeem(
                market_id="will-bitcoin-exceed-100000",
                outcome="YES",
            )

            # Redeem specific number of shares
            intent = Intent.prediction_redeem(
                market_id="will-bitcoin-exceed-100000",
                outcome="YES",
                shares=Decimal("50"),
            )
        """
        return PredictionRedeemIntent(
            market_id=market_id,
            outcome=outcome,
            shares=shares,
            protocol=protocol,
            chain=chain,
        )

    @staticmethod
    def vault_deposit(
        protocol: str,
        vault_address: str,
        amount: ChainedAmount,
        deposit_token: str | None = None,
        chain: str | None = None,
    ) -> VaultDepositIntent:
        """Create a vault deposit intent for MetaMorpho ERC-4626 vaults.

        Deposits underlying assets into a MetaMorpho vault in exchange for
        vault shares. The vault manages allocation across Morpho Blue markets.

        Args:
            protocol: Vault protocol (must be "metamorpho")
            vault_address: MetaMorpho vault contract address
            amount: Amount of underlying assets to deposit, or "all"
            deposit_token: Underlying token symbol (e.g. "USDC") for backtesting
            chain: Target chain (defaults to strategy's primary chain)

        Returns:
            VaultDepositIntent: The created vault deposit intent

        Example:
            # Deposit 1000 USDC into Steakhouse vault
            intent = Intent.vault_deposit(
                protocol="metamorpho",
                vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
                amount=Decimal("1000"),
                deposit_token="USDC",
                chain="ethereum",
            )
        """
        return VaultDepositIntent(
            protocol=protocol,
            vault_address=vault_address,
            amount=amount,
            deposit_token=deposit_token,
            chain=chain,
        )

    @staticmethod
    def vault_redeem(
        protocol: str,
        vault_address: str,
        shares: ChainedAmount,
        deposit_token: str | None = None,
        chain: str | None = None,
    ) -> VaultRedeemIntent:
        """Create a vault redeem intent for MetaMorpho ERC-4626 vaults.

        Redeems vault shares to receive underlying assets. No approval needed
        since the user is redeeming their own shares.

        Args:
            protocol: Vault protocol (must be "metamorpho")
            vault_address: MetaMorpho vault contract address
            shares: Number of shares to redeem, or "all" to redeem all
            deposit_token: Underlying token symbol (e.g. "USDC") for backtesting
            chain: Target chain (defaults to strategy's primary chain)

        Returns:
            VaultRedeemIntent: The created vault redeem intent

        Example:
            # Redeem all shares from Steakhouse vault
            intent = Intent.vault_redeem(
                protocol="metamorpho",
                vault_address="0xBEEF01735c132Ada46AA9aA4c54623cAA92A64CB",
                shares="all",
                deposit_token="USDC",
                chain="ethereum",
            )
        """
        return VaultRedeemIntent(
            protocol=protocol,
            vault_address=vault_address,
            shares=shares,
            deposit_token=deposit_token,
            chain=chain,
        )

    @staticmethod
    def sequence(
        intents: list[AnyIntent],
        description: str | None = None,
    ) -> IntentSequence:
        """Create an intent sequence for dependent actions that must execute in order.

        Use this when you have a series of intents where each step depends on the
        previous step's output. For example:
        - Swap USDC -> ETH, then bridge ETH to another chain
        - Bridge tokens, then supply to lending protocol

        The intents in a sequence will always execute sequentially. If any step
        fails, subsequent steps will not execute.

        Args:
            intents: List of intents to execute in order
            description: Optional description of what this sequence accomplishes

        Returns:
            IntentSequence: The created intent sequence

        Raises:
            InvalidSequenceError: If the intents list is empty

        Example:
            # Create a sequence: swap -> bridge -> supply
            return Intent.sequence([
                Intent.swap("USDC", "ETH", amount=Decimal("1000"), chain="base"),
                Intent.supply(protocol="aave_v3", token="WETH", amount=Decimal("0.5"), chain="arbitrum"),
            ], description="Move funds from Base to Arbitrum and deposit")

            # In decide(), return multiple sequences for parallel execution
            return [
                Intent.sequence([swap1, supply1]),  # Execute as sequence
                Intent.sequence([swap2, supply2]),  # Execute in parallel with above
            ]
        """
        return IntentSequence(intents=intents, description=description)

    @staticmethod
    def serialize(intent: AnyIntent) -> dict[str, Any]:
        """Serialize any intent to a dictionary.

        Args:
            intent: The intent to serialize

        Returns:
            dict: The serialized intent
        """
        return intent.serialize()

    @staticmethod
    def deserialize(data: dict[str, Any]) -> Any:
        """Deserialize a dictionary to the appropriate intent type.

        Args:
            data: The serialized intent data

        Returns:
            The deserialized intent (AnyIntent or BridgeIntent)

        Raises:
            ValueError: If the intent type is unknown
        """
        intent_type = data.get("type")
        if intent_type is None:
            raise ValueError("Missing 'type' field in intent data")

        # Handle BridgeIntent separately to avoid circular import
        if intent_type == IntentType.BRIDGE.value:
            from .bridge import BridgeIntent

            return BridgeIntent.deserialize(data)

        # Handle EnsureBalanceIntent separately to avoid circular import
        if intent_type == IntentType.ENSURE_BALANCE.value:
            from .ensure_balance import EnsureBalanceIntent

            return EnsureBalanceIntent.deserialize(data)

        deserializers: dict[str, type[AnyIntent]] = {
            IntentType.SWAP.value: SwapIntent,
            IntentType.LP_OPEN.value: LPOpenIntent,
            IntentType.LP_CLOSE.value: LPCloseIntent,
            IntentType.LP_COLLECT_FEES.value: CollectFeesIntent,
            IntentType.BORROW.value: BorrowIntent,
            IntentType.REPAY.value: RepayIntent,
            IntentType.SUPPLY.value: SupplyIntent,
            IntentType.WITHDRAW.value: WithdrawIntent,
            IntentType.PERP_OPEN.value: PerpOpenIntent,
            IntentType.PERP_CLOSE.value: PerpCloseIntent,
            IntentType.FLASH_LOAN.value: FlashLoanIntent,
            IntentType.STAKE.value: StakeIntent,
            IntentType.UNSTAKE.value: UnstakeIntent,
            IntentType.HOLD.value: HoldIntent,
            IntentType.PREDICTION_BUY.value: PredictionBuyIntent,
            IntentType.PREDICTION_SELL.value: PredictionSellIntent,
            IntentType.PREDICTION_REDEEM.value: PredictionRedeemIntent,
            IntentType.VAULT_DEPOSIT.value: VaultDepositIntent,
            IntentType.VAULT_REDEEM.value: VaultRedeemIntent,
            IntentType.WRAP_NATIVE.value: WrapNativeIntent,
            IntentType.UNWRAP_NATIVE.value: UnwrapNativeIntent,
        }

        deserializer = deserializers.get(intent_type)
        if deserializer is None:
            raise ValueError(f"Unknown intent type: {intent_type}")

        return deserializer.deserialize(data)

    @staticmethod
    def get_type(intent: AnyIntent) -> IntentType:
        """Get the type of an intent.

        Args:
            intent: The intent to get the type of

        Returns:
            IntentType: The type of the intent
        """
        return intent.intent_type

    @staticmethod
    def validate_chain(
        intent: AnyIntent,
        configured_chains: Sequence[str],
        default_chain: str | None = None,
    ) -> str:
        """Validate and resolve the chain for an intent.

        Validates that the intent's chain (if specified) is in the list of
        configured chains. If no chain is specified on the intent, returns
        the default chain.

        Args:
            intent: The intent to validate
            configured_chains: List of chains configured for the strategy
            default_chain: Default chain to use if intent has no chain specified.
                          If None, uses the first configured chain.

        Returns:
            str: The resolved chain name (lowercase)

        Raises:
            InvalidChainError: If the intent's chain is not in configured_chains
            ValueError: If no default chain can be determined

        Example:
            # Validate an intent against strategy's configured chains
            resolved_chain = Intent.validate_chain(
                intent,
                configured_chains=["arbitrum", "optimism"],
                default_chain="arbitrum",
            )
        """
        if not configured_chains:
            raise ValueError("No chains configured for strategy")

        # Normalize configured chains to lowercase
        normalized_chains = [c.lower() for c in configured_chains]

        # Get chain from intent (all intent types have chain attribute now)
        intent_chain = getattr(intent, "chain", None)

        if intent_chain is not None:
            # Validate the specified chain
            chain_lower = intent_chain.lower()
            if chain_lower not in normalized_chains:
                raise InvalidChainError(intent_chain, configured_chains)
            return chain_lower

        # No chain specified - use default
        if default_chain is not None:
            default_lower = default_chain.lower()
            if default_lower not in normalized_chains:
                raise InvalidChainError(default_chain, configured_chains)
            return default_lower

        # Fall back to first configured chain
        return normalized_chains[0]

    @staticmethod
    def get_chain(intent: AnyIntent) -> str | None:
        """Get the chain specified on an intent.

        Args:
            intent: The intent to get the chain from

        Returns:
            Optional[str]: The chain name if specified, None otherwise
        """
        return getattr(intent, "chain", None)

    @staticmethod
    def is_sequence(item: AnyIntent | IntentSequence) -> bool:
        """Check if an item is an IntentSequence.

        Args:
            item: Intent or IntentSequence to check

        Returns:
            bool: True if item is an IntentSequence
        """
        return isinstance(item, IntentSequence)

    @staticmethod
    def normalize_decide_result(
        result: DecideResult,
    ) -> list[AnyIntent | IntentSequence]:
        """Normalize a decide() result to a list of items to execute.

        This helper converts any valid decide() return value into a normalized
        list that the executor can process:
        - None -> empty list (no action)
        - Single intent -> list with one intent
        - IntentSequence -> list with one sequence
        - List -> returned as-is

        Args:
            result: The return value from decide()

        Returns:
            List of intents and/or sequences to execute.
            Items in the list can execute in parallel.
            Intents within a sequence execute sequentially.
        """
        if result is None:
            return []
        if isinstance(result, IntentSequence):
            return [result]
        if isinstance(result, list):
            return result
        # Single intent
        return [result]

    @staticmethod
    def count_intents(result: DecideResult) -> int:
        """Count the total number of intents in a decide() result.

        Args:
            result: The return value from decide()

        Returns:
            Total number of intents (counting all intents within sequences)
        """
        if result is None:
            return 0

        items = Intent.normalize_decide_result(result)
        total = 0
        for item in items:
            if isinstance(item, IntentSequence):
                total += len(item.intents)
            else:
                total += 1
        return total

    @staticmethod
    def serialize_result(result: DecideResult) -> dict[str, Any] | None:
        """Serialize a decide() result to a dictionary.

        Args:
            result: The return value from decide()

        Returns:
            Serialized result, or None if result was None
        """
        if result is None:
            return None

        if isinstance(result, IntentSequence):
            return result.serialize()

        if isinstance(result, list):
            return {
                "type": "PARALLEL",
                "items": [
                    item.serialize() if isinstance(item, IntentSequence) else Intent.serialize(item) for item in result
                ],
            }

        # Single intent
        return Intent.serialize(result)

    @staticmethod
    def deserialize_result(data: dict[str, Any] | None) -> DecideResult:
        """Deserialize a decide() result from a dictionary.

        Args:
            data: Serialized result data

        Returns:
            Deserialized DecideResult
        """
        if data is None:
            return None

        result_type = data.get("type")

        if result_type == "SEQUENCE":
            return IntentSequence.deserialize(data)

        if result_type == "PARALLEL":
            items: list[AnyIntent | IntentSequence] = []
            for item_data in data.get("items", []):
                if item_data.get("type") == "SEQUENCE":
                    items.append(IntentSequence.deserialize(item_data))
                else:
                    items.append(Intent.deserialize(item_data))
            return items

        # Single intent
        return Intent.deserialize(data)

    @staticmethod
    def has_chained_amount(intent: AnyIntent) -> bool:
        """Check if an intent uses a chained amount from a previous step.

        An intent has a chained amount when its amount field is set to "all",
        meaning it should use the actual received amount from the previous
        step in a sequence (post-slippage, post-fees).

        Args:
            intent: The intent to check

        Returns:
            True if the intent uses amount="all", False otherwise
        """
        return getattr(intent, "is_chained_amount", False)

    @staticmethod
    def validate_chained_amounts(sequence: IntentSequence) -> None:
        """Validate that chained amounts are used correctly in a sequence.

        Validates that:
        1. amount="all" is NOT used on the first step of a sequence
        2. The sequence has proper dependencies for amount resolution

        Args:
            sequence: The intent sequence to validate

        Raises:
            InvalidAmountError: If amount="all" is used on the first step
        """
        if not sequence.intents:
            return

        first_intent = sequence.intents[0]
        if Intent.has_chained_amount(first_intent):
            intent_type = first_intent.intent_type.value if hasattr(first_intent, "intent_type") else "Unknown"
            raise InvalidAmountError(
                intent_type=intent_type,
                reason="amount='all' cannot be used on the first step of a sequence because there is no previous step output to reference",
            )

    @staticmethod
    def get_amount_field(intent: AnyIntent) -> ChainedAmount | None:
        """Get the amount field value from an intent for chaining purposes.

        This returns the amount that flows to the next step in a sequence.
        Different intents output different amounts:
        - SwapIntent: amount (token output) or amount_usd
        - SupplyIntent: amount (what was supplied)
        - RepayIntent: amount (what was repaid)
        - WithdrawIntent: amount (what was withdrawn)
        - BorrowIntent: borrow_amount (NOT collateral_amount - this is what's borrowed)
        - PerpOpenIntent: collateral_amount (what was deposited)
        - BridgeIntent: amount (what was bridged)

        Args:
            intent: The intent to get the amount from

        Returns:
            The amount value (Decimal or "all"), or None if not applicable
        """
        # For BorrowIntent, the output is the borrow_amount (what was borrowed)
        # NOT the collateral_amount (which may be 0 if already supplied)
        if hasattr(intent, "borrow_amount"):
            borrow_amount = intent.borrow_amount
            if borrow_amount is not None:
                return borrow_amount
        # Check standard amount field first (prefer non-None value)
        if hasattr(intent, "amount"):
            amount = intent.amount
            if amount is not None:
                return amount
        # Check amount_usd as fallback (for SwapIntent using USD amounts)
        if hasattr(intent, "amount_usd"):
            amount_usd = intent.amount_usd
            if amount_usd is not None:
                return amount_usd
        # Check collateral_amount for perp intents
        if hasattr(intent, "collateral_amount"):
            return intent.collateral_amount
        return None

    @staticmethod
    def set_resolved_amount(intent: AnyIntent, resolved_amount: Decimal) -> AnyIntent:
        """Create a copy of an intent with the amount resolved from "all" to a concrete value.

        This is used at execution time to resolve amount="all" to the actual
        received amount from the previous step.

        Args:
            intent: The intent to update
            resolved_amount: The concrete amount to use

        Returns:
            A new intent instance with the resolved amount

        Note:
            This creates a new intent instance; it does not mutate the original.
        """
        # Get the serialized form
        data = intent.serialize()

        # Update the appropriate amount field
        if "amount" in data and data["amount"] == "all":
            data["amount"] = str(resolved_amount)
        elif "collateral_amount" in data and data["collateral_amount"] == "all":
            data["collateral_amount"] = str(resolved_amount)

        # Deserialize back to an intent
        return Intent.deserialize(data)
