"""Advanced and utility intent classes.

Intent classes for flash loans, staking/unstaking, vault operations,
and native token wrap/unwrap.
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

from pydantic import Field, model_validator

from almanak.framework.models.base import (
    AlmanakImmutableModel,  # noqa: F401  -- re-exported for backward compatibility
    SafeDecimal,
    default_intent_id,
    default_timestamp,
)
from almanak.framework.models.base import (
    ChainedAmount as PydanticChainedAmount,
)

from .base import BaseIntent
from .lending_intents import BorrowIntent, RepayIntent, SupplyIntent, WithdrawIntent
from .perp_intents import PerpCloseIntent, PerpOpenIntent
from .vocabulary import (
    CollectFeesIntent,
    IntentType,
    LPCloseIntent,
    LPOpenIntent,
    SwapIntent,
)

# Union of all intent types that can appear as flash loan callback operations.
# FlashLoanIntent itself is excluded to prevent nesting.
FlashLoanCallbackIntent = (
    CollectFeesIntent
    | SwapIntent
    | LPOpenIntent
    | LPCloseIntent
    | BorrowIntent
    | RepayIntent
    | SupplyIntent
    | WithdrawIntent
    | PerpOpenIntent
    | PerpCloseIntent
)


def _validate_vault_protocol(protocol: str) -> None:
    """Validate ``protocol`` against the vault adapter registry.

    Lazy import keeps the intents module free of compile-time dependencies on
    connector packages.
    """
    from almanak.connectors._strategy_base.vaults import supported_vault_protocols

    supported = supported_vault_protocols()
    if protocol.lower() not in supported:
        raise ValueError(f"Invalid vault protocol: {protocol!r}. Supported: {sorted(supported)}")


class FlashLoanIntent(BaseIntent):
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
        """Serialize the intent to a dictionary.

        FlashLoanIntent ships a hand-rolled dict literal here (rather than
        delegating to ``model_dump``) because the nested ``callback_intents``
        list needs each element's per-class ``serialize()`` to dispatch
        correctly. The reserved ``registry_handle`` field (VIB-4192) is
        emitted unconditionally (with value ``None`` when unset) for schema
        stability — see UAT card §D1.S2 / D2.M1 for the per-class
        defaulted-None round-trip contract.
        """
        return {
            "type": self.intent_type.value,
            "intent_id": self.intent_id,
            "created_at": self.created_at.isoformat(),
            "provider": self.provider,
            "token": self.token,
            "amount": str(self.amount),
            "callback_intents": [intent.serialize() for intent in self.callback_intents],
            "chain": self.chain,
            "registry_handle": self.registry_handle,
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
            # registry_handle (VIB-4192) is reserved on BaseIntent; surface
            # the deserialized value when present so round-trip preserves it.
            "registry_handle": data.get("registry_handle"),
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


class StakeIntent(BaseIntent):
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


class UnstakeIntent(BaseIntent):
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


class VaultDepositIntent(BaseIntent):
    """Intent to deposit assets into an ERC-4626 vault.

    Supports any vault protocol registered with
    :mod:`almanak.connectors._strategy_base.vaults` (e.g. ``metamorpho``; future:
    ``beefy``, ``yearn_v3``). The ``protocol`` field is the dispatch key.

    Attributes:
        protocol: Registered vault protocol name (case-insensitive)
        vault_address: ERC-4626 vault contract address
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
        _validate_vault_protocol(self.protocol)
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


class VaultRedeemIntent(BaseIntent):
    """Intent to redeem shares from an ERC-4626 vault.

    Supports any vault protocol registered with
    :mod:`almanak.connectors._strategy_base.vaults`.

    Attributes:
        protocol: Registered vault protocol name (case-insensitive)
        vault_address: ERC-4626 vault contract address
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
        _validate_vault_protocol(self.protocol)
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


class WrapNativeIntent(BaseIntent):
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


class UnwrapNativeIntent(BaseIntent):
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
