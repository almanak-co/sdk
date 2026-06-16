"""Lending protocol intent classes.

Intent classes for lending protocol operations: borrow, repay, supply, and withdraw.
These intents support protocols like Aave V3, Morpho Blue, Compound V3, etc.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

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
from .intent_errors import BundledCollateralBorrowError, InvalidProtocolParameterError
from .vocabulary import (
    IntentType,
    InterestRateMode,
)


def _capabilities_for(protocol_lower: str) -> dict[str, Any]:
    """Return the capability dict for ``protocol_lower`` via the connector registry.

    Resolved lazily because the registry imports connector packages whose own
    ``__init__.py`` modules pull symbols from this package -- pulling the
    registry at module-import time would create a circular import on cold
    boot. The lookup is cheap once the registry's aggregated view is built
    (built on first call, cached for the life of the process).
    """
    # Local import: see docstring.
    from almanak.connectors._strategy_base.capabilities_registry import get_protocol_capabilities

    return get_protocol_capabilities(protocol_lower)


class BorrowIntent(BaseIntent):
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
        # Fail-closed: reject a bundled collateralized borrow. A nonzero
        # collateral_amount (or the chained "all" form, which resolves to a
        # positive supply at execution time) supplies AND borrows on-chain in
        # one action, but accounting writes one event per intent -- the SUPPLY
        # event and supply FIFO lot are silently dropped. Only the standalone
        # form (collateral_amount == Decimal("0")) is accounting-correct.
        # See docs/internal/bundled-collateral-borrow-migration.md (and
        # docs/internal/FollowUp-13June15.md §D1 for the original root cause).
        self._reject_bundled_collateral()
        # Validate interest_rate_mode against protocol capabilities
        self._validate_protocol_params()
        return self

    def _reject_bundled_collateral(self) -> None:
        """Reject a bundled collateralized borrow (``collateral_amount > 0`` or ``"all"``).

        Allowed: ``collateral_amount == Decimal("0")`` (the standalone-borrow
        form -- ``collateral_token`` is metadata only and nothing is supplied).
        Rejected: any positive ``Decimal`` or the chained ``"all"`` literal,
        both of which bundle an on-chain supply that the accounting layer cannot
        record as a distinct SUPPLY event. Raises
        :class:`BundledCollateralBorrowError`.

        Protocols whose native open is an *atomic* supply+borrow (e.g. a Fluid
        vault ``operate()`` that mints an NFT-CDP and supplies + borrows in one
        call) opt out by declaring ``supports_bundled_collateral_borrow`` in
        their connector capabilities -- for them the bundled shape is correct
        and the accounting split is owned by the receipt parser, not by
        intent-level decomposition.
        """
        is_positive_decimal = isinstance(self.collateral_amount, Decimal) and self.collateral_amount > 0
        is_chained_all = not isinstance(self.collateral_amount, Decimal) and self.collateral_amount == "all"
        if not (is_positive_decimal or is_chained_all):
            return
        capabilities = _capabilities_for(self.protocol.lower())
        if capabilities.get("supports_bundled_collateral_borrow", False):
            return
        raise BundledCollateralBorrowError(
            protocol=self.protocol,
            collateral_token=self.collateral_token,
            collateral_amount=self.collateral_amount,
            borrow_token=self.borrow_token,
        )

    def _validate_protocol_params(self) -> None:
        """Validate protocol-specific parameters."""
        protocol_lower = self.protocol.lower()
        capabilities = _capabilities_for(protocol_lower)

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

    @classmethod
    def for_permission_discovery(cls, **fields: Any) -> "BorrowIntent":
        """Build a synthetic BORROW intent for permission discovery, bypassing the bundled-collateral guard.

        Permission discovery (``build_discovery_vectors`` in connector
        ``permission_hints.py``) intentionally emits a *bundled* borrow
        (``collateral_amount > 0``) so the compiler enumerates BOTH the
        collateral-supply approval and the borrow selector on the Safe
        manifest. These synthetic intents are never executed and never reach
        the accounting ledger, so the 1:1 ledger→event invariant the
        :meth:`validate_borrow_intent` guard protects does not apply.

        This uses ``model_construct`` (Pydantic's trusted-data constructor) to
        skip the model validator while still applying field defaults
        (``intent_id``, ``created_at``). Do NOT use this for any
        strategy-emitted or executable borrow — those must go through
        :meth:`Intent.borrow` / normal construction so the guard fires.
        """
        return cls.model_construct(**fields)

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


class RepayIntent(BaseIntent):
    """Intent to repay borrowed tokens.

    Attributes:
        protocol: Lending protocol (e.g., "aave_v3", "morpho")
        token: Token to repay
        amount: Amount to repay, or "all" to use output from previous step.
            Defaults to Decimal("0") when repay_full=True (ignored by the protocol in that case).
            Required when repay_full=False.
        repay_full: If True, repay the full outstanding debt (sends MAX_UINT256 to the protocol).
            When True, amount is ignored and may be omitted via Intent.repay().
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
        capabilities = _capabilities_for(protocol_lower)

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


class SupplyIntent(BaseIntent):
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
        capabilities = _capabilities_for(protocol_lower)

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


class WithdrawIntent(BaseIntent):
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
    is_collateral: bool = True
    """For Morpho Blue: True withdraws collateral, False withdraws loan token.
    Other protocols ignore this field. Defaults to True for backward compat."""
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
        capabilities = _capabilities_for(protocol_lower)

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


class DeleverageIntent(BaseIntent):
    """Intent to perform an emergency deleverage (forced repay with risk-event context).

    Structurally identical to a RepayIntent at the protocol level — the on-chain
    action is a full or partial repay. The distinction is that a DeleverageIntent
    is emitted by risk-management logic (health-factor guards) rather than normal
    strategy operation. The extra fields make the event distinguishable in accounting
    and dashboards without requiring protocol-level changes.

    Attributes:
        protocol: Lending protocol (e.g., "aave_v3", "morpho_blue")
        token: Token to repay
        amount: Amount to repay, or "all" to use output from previous step.
            Defaults to Decimal("0") when repay_full=True (ignored by the protocol).
        repay_full: If True, repay the full outstanding debt (sends MAX_UINT256).
        interest_rate_mode: Interest rate mode for protocols that support it.
        market_id: Market identifier for isolated lending protocols (e.g., Morpho Blue).
        chain: Target chain for execution.
        trigger_reason: Human-readable description of why the deleverage was triggered
            (e.g., "HF 1.08 < emergency_threshold 1.2: full deleverage").
        observed_hf: Health factor observed at the time the deleverage was triggered.
            None if the health factor could not be read before the trigger.
        target_hf: The desired health factor after the deleverage completes.
            None if not specified by the calling strategy.
        intent_id: Unique identifier for this intent.
        created_at: Timestamp when the intent was created.

    Note:
        The compiler routes DELEVERAGE to the same on-chain path as REPAY.  The
        event_type in accounting will be LendingEventType.DELEVERAGE (not REPAY),
        and the trigger context is preserved in the accounting event's notes field.
    """

    protocol: str
    token: str
    amount: PydanticChainedAmount
    repay_full: bool = False
    interest_rate_mode: InterestRateMode | None = None
    market_id: str | None = None
    chain: str | None = None

    # Risk-event context — the fields that distinguish a deleverage from a repay.
    trigger_reason: str = ""
    observed_hf: Decimal | None = None
    target_hf: Decimal | None = None

    intent_id: str = Field(default_factory=default_intent_id)
    created_at: datetime = Field(default_factory=default_timestamp)

    @model_validator(mode="after")
    def validate_deleverage_intent(self) -> "DeleverageIntent":
        """Validate deleverage parameters."""
        if not self.repay_full:
            if isinstance(self.amount, Decimal) and self.amount <= 0:
                raise ValueError("amount must be positive when not repaying full")
            elif not isinstance(self.amount, Decimal) and self.amount != "all":
                raise ValueError("amount must be a positive Decimal or 'all' when not repaying full")
        if self.observed_hf is not None and self.observed_hf < Decimal("0"):
            raise ValueError("observed_hf must be non-negative")
        if self.target_hf is not None and self.target_hf <= Decimal("0"):
            raise ValueError("target_hf must be positive")
        # Apply the same protocol-param validation as RepayIntent so inputs such
        # as protocol="morpho_blue" without market_id are rejected early.
        self._validate_protocol_params()
        return self

    def _validate_protocol_params(self) -> None:
        """Validate protocol-specific parameters (mirrors RepayIntent)."""
        protocol_lower = self.protocol.lower()
        capabilities = _capabilities_for(protocol_lower)

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
        return IntentType.DELEVERAGE

    def serialize(self) -> dict[str, Any]:
        """Serialize the intent to a dictionary."""
        data = self.model_dump(mode="json")
        data["type"] = self.intent_type.value
        if self.amount == "all":
            data["amount"] = "all"
        return data

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> "DeleverageIntent":
        """Deserialize a dictionary to a DeleverageIntent."""
        clean_data = {k: v for k, v in data.items() if k != "type"}
        if "created_at" in clean_data and isinstance(clean_data["created_at"], str):
            clean_data["created_at"] = datetime.fromisoformat(clean_data["created_at"])
        return cls.model_validate(clean_data)
