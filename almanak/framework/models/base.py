"""Base Pydantic v2 models for the Almanak Strategy Framework.

This module provides the foundation for all Pydantic models in the framework:

- AlmanakBaseModel: Base model with serialization settings
- AlmanakImmutableModel: Frozen model for intents and configs
- AlmanakMutableModel: Mutable model for state objects
- SafeDecimal: Decimal validator that rejects floats for precision safety
- ChainedAmount: Union[Decimal, Literal["all"]] for intent chaining

Design Philosophy:
    - UX First: Accept int, str, Decimal for amounts (just reject float)
    - Safety Always: Strict mode, forbid extra fields, explicit serialization
    - Backward Compatible: serialize() and deserialize() methods for existing code
"""

import uuid
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    field_serializer,
)
from pydantic.functional_validators import BeforeValidator

# =============================================================================
# Decimal Safety Validators
# =============================================================================


def validate_decimal_safe(v: Any) -> Decimal:
    """Validate and convert to Decimal safely.

    Accepts: int, str, Decimal
    Rejects: float (with clear error message)

    This prevents precision loss from float -> Decimal coercion.
    For example, Decimal(0.1) != Decimal("0.1") due to float representation.

    Args:
        v: Value to convert to Decimal

    Returns:
        Decimal representation of the value

    Raises:
        ValueError: If value is a float or cannot be converted
    """
    if isinstance(v, float):
        raise ValueError(
            f"Float values are not allowed for Decimal fields to prevent precision loss. "
            f"Use Decimal('{v}') or pass as string '{v}' instead."
        )
    if isinstance(v, Decimal):
        return v
    if isinstance(v, int):
        return Decimal(str(v))
    if isinstance(v, str):
        try:
            return Decimal(v)
        except InvalidOperation as e:
            raise ValueError(f"Cannot convert '{v}' to Decimal - invalid number format") from e
    raise ValueError(f"Expected int, str, or Decimal, got {type(v).__name__}")


def validate_optional_decimal_safe(v: Any) -> Decimal | None:
    """Validate optional Decimal field - allows None, validates others."""
    if v is None:
        return None
    return validate_decimal_safe(v)


def validate_chained_amount(v: Any) -> Decimal | Literal["all"]:
    """Validate ChainedAmount: Decimal or literal "all".

    Used for intent amounts that can either be:
    - A specific Decimal value
    - "all" to use the output from the previous step in a sequence

    The "all" pattern enables chaining like:
        Intent.swap("USDC", "ETH", amount_usd=Decimal("1000"))
        Intent.bridge("ETH", amount="all", ...)  # Uses ETH from swap

    Args:
        v: Value to validate - Decimal, int, str, or literal "all"

    Returns:
        Decimal or literal "all"

    Raises:
        ValueError: If value is invalid
    """
    if v == "all":
        return "all"
    return validate_decimal_safe(v)


def validate_optional_chained_amount(v: Any) -> Decimal | Literal["all"] | None:
    """Validate optional ChainedAmount - allows None."""
    if v is None:
        return None
    return validate_chained_amount(v)


# =============================================================================
# Type Aliases with Validators
# =============================================================================

# Safe Decimal that rejects float input
SafeDecimal = Annotated[Decimal, BeforeValidator(validate_decimal_safe)]

# Optional Safe Decimal
OptionalSafeDecimal = Annotated[Decimal | None, BeforeValidator(validate_optional_decimal_safe)]

# Chained amount: Decimal or "all" for intent sequences
ChainedAmount = Annotated[
    Decimal | Literal["all"],
    BeforeValidator(validate_chained_amount),
]

# Optional chained amount
OptionalChainedAmount = Annotated[
    Decimal | Literal["all"] | None,
    BeforeValidator(validate_optional_chained_amount),
]


# =============================================================================
# Default Field Factories
# =============================================================================


def default_intent_id() -> str:
    """Generate a new UUID for intent_id field."""
    return str(uuid.uuid4())


def default_timestamp() -> datetime:
    """Generate current UTC timestamp for created_at field."""
    return datetime.now(UTC)


# =============================================================================
# Base Models
# =============================================================================


class AlmanakBaseModel(BaseModel):
    """Base model for all Almanak Pydantic models.

    Features:
    - Strict mode: Explicit type handling (no silent coercion beyond our validators)
    - Forbid extra fields: Catch typos in field names early
    - Validate on assignment: Mutations are validated
    - Decimal serialization: Decimals serialize as strings for JSON safety

    All models in the Almanak framework should inherit from this base
    or one of its subclasses (AlmanakImmutableModel, AlmanakMutableModel).
    """

    model_config = ConfigDict(
        # Strict mode - rely on our explicit validators
        strict=True,
        # Forbid extra fields - catch typos
        extra="forbid",
        # Validate when fields are assigned
        validate_assignment=True,
        # Allow population by field name or alias
        populate_by_name=True,
        # Use enum values in serialization
        use_enum_values=True,
    )

    @field_serializer("*", when_used="always")
    def serialize_special_types(self, v: Any) -> Any:
        """Serialize Decimal and datetime to JSON-safe formats.

        - Decimal -> str (preserves precision)
        - datetime -> ISO format string
        """
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, datetime):
            return v.isoformat()
        return v


class AlmanakImmutableModel(AlmanakBaseModel):
    """Immutable base model for intents and configurations.

    Once created, instances cannot be modified. This is important for:
    - Intents: Should not change after creation (audit trail)
    - Configs: Changes should create new instances (CAS semantics)

    Attempting to modify a field after creation raises an error.
    """

    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        validate_assignment=False,  # Frozen, so no assignment validation needed
        frozen=True,  # Make immutable
        populate_by_name=True,
        use_enum_values=True,
    )

    @field_serializer("*", when_used="always")
    def serialize_special_types(self, v: Any) -> Any:
        """Serialize Decimal and datetime to JSON-safe formats."""
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, datetime):
            return v.isoformat()
        return v


class AlmanakMutableModel(AlmanakBaseModel):
    """Mutable base model for state objects.

    Used for objects that need to be modified after creation:
    - State objects that track progress
    - Accumulator patterns
    - Temporary working objects

    Mutations are validated to ensure consistency.
    """

    model_config = ConfigDict(
        strict=True,
        extra="forbid",
        validate_assignment=True,  # Validate mutations
        frozen=False,  # Allow mutation
        populate_by_name=True,
        use_enum_values=True,
    )

    @field_serializer("*", when_used="always")
    def serialize_special_types(self, v: Any) -> Any:
        """Serialize Decimal and datetime to JSON-safe formats."""
        if isinstance(v, Decimal):
            return str(v)
        if isinstance(v, datetime):
            return v.isoformat()
        return v


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Validators
    "validate_decimal_safe",
    "validate_optional_decimal_safe",
    "validate_chained_amount",
    "validate_optional_chained_amount",
    # Type aliases
    "SafeDecimal",
    "OptionalSafeDecimal",
    "ChainedAmount",
    "OptionalChainedAmount",
    # Default factories
    "default_intent_id",
    "default_timestamp",
    # Base models
    "AlmanakBaseModel",
    "AlmanakImmutableModel",
    "AlmanakMutableModel",
]
