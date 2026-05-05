"""Tests for base Pydantic models and validators.

These tests verify:
1. Decimal safety - floats are rejected to prevent precision loss
2. ChainedAmount - "all" literal and Decimal both work
3. Base model behavior - serialization, immutability, etc.
4. UX preservation - int, str, Decimal all accepted for amounts
"""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

from almanak.framework.models.base import (
    AlmanakBaseModel,
    AlmanakImmutableModel,
    AlmanakMutableModel,
    ChainedAmount,
    OptionalSafeDecimal,
    SafeDecimal,
    default_intent_id,
    default_timestamp,
    validate_chained_amount,
    validate_decimal_safe,
    validate_optional_chained_amount,
    validate_optional_decimal_safe,
)


class TestValidateDecimalSafe:
    """Test the validate_decimal_safe function."""

    def test_accepts_decimal(self):
        """Decimal values pass through unchanged."""
        result = validate_decimal_safe(Decimal("1.5"))
        assert result == Decimal("1.5")
        assert isinstance(result, Decimal)

    def test_accepts_int(self):
        """Integer values are converted to Decimal."""
        result = validate_decimal_safe(1000)
        assert result == Decimal("1000")
        assert isinstance(result, Decimal)

    def test_accepts_zero(self):
        """Zero is a valid value."""
        assert validate_decimal_safe(0) == Decimal("0")
        assert validate_decimal_safe("0") == Decimal("0")

    def test_accepts_negative(self):
        """Negative values are valid (validation of sign is caller's job)."""
        assert validate_decimal_safe(-100) == Decimal("-100")
        assert validate_decimal_safe("-100") == Decimal("-100")

    def test_accepts_string(self):
        """String numbers are converted to Decimal."""
        result = validate_decimal_safe("1.234567890123456789")
        assert result == Decimal("1.234567890123456789")
        assert isinstance(result, Decimal)

    def test_accepts_string_with_precision(self):
        """High precision strings preserve all digits."""
        precise = "0.123456789012345678901234567890"
        result = validate_decimal_safe(precise)
        assert str(result) == precise

    def test_rejects_float(self):
        """Float values are rejected with clear error."""
        with pytest.raises(ValueError, match="Float values are not allowed"):
            validate_decimal_safe(1.5)

    def test_rejects_float_zero(self):
        """Even 0.0 is rejected - use integer 0 instead."""
        with pytest.raises(ValueError, match="Float values are not allowed"):
            validate_decimal_safe(0.0)

    def test_rejects_float_with_precision_warning(self):
        """The error message explains the precision issue."""
        with pytest.raises(ValueError, match="precision loss"):
            validate_decimal_safe(0.1)

    def test_rejects_invalid_string(self):
        """Non-numeric strings are rejected."""
        with pytest.raises(ValueError, match="Cannot convert"):
            validate_decimal_safe("not_a_number")

    def test_rejects_empty_string(self):
        """Empty strings are rejected."""
        with pytest.raises(ValueError, match="Cannot convert"):
            validate_decimal_safe("")

    def test_rejects_none(self):
        """None is rejected (use Optional variant)."""
        with pytest.raises(ValueError):
            validate_decimal_safe(None)

    def test_rejects_list(self):
        """Lists are rejected with type error."""
        with pytest.raises(ValueError, match="Expected int, str, or Decimal"):
            validate_decimal_safe([1, 2, 3])


class TestValidateOptionalDecimalSafe:
    """Test the validate_optional_decimal_safe function."""

    def test_accepts_none(self):
        """None passes through as None."""
        assert validate_optional_decimal_safe(None) is None

    def test_accepts_decimal(self):
        """Decimal values work as expected."""
        result = validate_optional_decimal_safe(Decimal("100"))
        assert result == Decimal("100")

    def test_accepts_int(self):
        """Integer values are converted."""
        assert validate_optional_decimal_safe(1000) == Decimal("1000")

    def test_rejects_float(self):
        """Floats are still rejected."""
        with pytest.raises(ValueError, match="Float values are not allowed"):
            validate_optional_decimal_safe(1.5)


class TestValidateChainedAmount:
    """Test the validate_chained_amount function."""

    def test_accepts_all_literal(self):
        """The literal 'all' is accepted."""
        result = validate_chained_amount("all")
        assert result == "all"

    def test_accepts_decimal(self):
        """Decimal values are accepted."""
        result = validate_chained_amount(Decimal("100"))
        assert result == Decimal("100")

    def test_accepts_int(self):
        """Integer values are converted to Decimal."""
        result = validate_chained_amount(1000)
        assert result == Decimal("1000")

    def test_accepts_string_number(self):
        """Numeric strings are converted to Decimal."""
        result = validate_chained_amount("100.5")
        assert result == Decimal("100.5")

    def test_rejects_float(self):
        """Float values are rejected."""
        with pytest.raises(ValueError, match="Float values are not allowed"):
            validate_chained_amount(1.5)

    def test_rejects_invalid_string(self):
        """Non-'all' non-numeric strings are rejected."""
        with pytest.raises(ValueError):
            validate_chained_amount("some")


class TestValidateOptionalChainedAmount:
    """Test the validate_optional_chained_amount function."""

    def test_accepts_none(self):
        """None passes through."""
        assert validate_optional_chained_amount(None) is None

    def test_accepts_all(self):
        """'all' literal works."""
        assert validate_optional_chained_amount("all") == "all"

    def test_accepts_decimal(self):
        """Decimal values work."""
        assert validate_optional_chained_amount(Decimal("50")) == Decimal("50")


class TestSafeDecimalType:
    """Test SafeDecimal as a Pydantic field type."""

    def test_in_model_accepts_int(self):
        """Model with SafeDecimal field accepts int."""

        class TestModel(AlmanakBaseModel):
            amount: SafeDecimal

        model = TestModel(amount=1000)
        assert model.amount == Decimal("1000")

    def test_in_model_accepts_string(self):
        """Model with SafeDecimal field accepts string."""

        class TestModel(AlmanakBaseModel):
            amount: SafeDecimal

        model = TestModel(amount="123.456")
        assert model.amount == Decimal("123.456")

    def test_in_model_accepts_decimal(self):
        """Model with SafeDecimal field accepts Decimal."""

        class TestModel(AlmanakBaseModel):
            amount: SafeDecimal

        model = TestModel(amount=Decimal("999"))
        assert model.amount == Decimal("999")

    def test_in_model_rejects_float(self):
        """Model with SafeDecimal field rejects float."""

        class TestModel(AlmanakBaseModel):
            amount: SafeDecimal

        with pytest.raises(ValidationError) as exc_info:
            TestModel(amount=1.5)

        # Check the error message contains our custom message
        errors = exc_info.value.errors()
        assert len(errors) == 1
        assert "Float values are not allowed" in str(errors[0]["msg"])


class TestChainedAmountType:
    """Test ChainedAmount as a Pydantic field type."""

    def test_in_model_accepts_all(self):
        """Model with ChainedAmount field accepts 'all'."""

        class TestModel(AlmanakBaseModel):
            amount: ChainedAmount

        model = TestModel(amount="all")
        assert model.amount == "all"

    def test_in_model_accepts_decimal(self):
        """Model with ChainedAmount field accepts Decimal."""

        class TestModel(AlmanakBaseModel):
            amount: ChainedAmount

        model = TestModel(amount=Decimal("500"))
        assert model.amount == Decimal("500")

    def test_in_model_accepts_int(self):
        """Model with ChainedAmount field accepts int."""

        class TestModel(AlmanakBaseModel):
            amount: ChainedAmount

        model = TestModel(amount=100)
        assert model.amount == Decimal("100")


class TestAlmanakBaseModel:
    """Test the AlmanakBaseModel base class."""

    def test_decimal_serialization(self):
        """Decimals are serialized as strings."""

        class TestModel(AlmanakBaseModel):
            amount: SafeDecimal

        model = TestModel(amount=Decimal("123.456"))
        data = model.model_dump()
        assert data["amount"] == "123.456"
        assert isinstance(data["amount"], str)

    def test_datetime_serialization(self):
        """Datetimes are serialized as ISO format."""

        class TestModel(AlmanakBaseModel):
            timestamp: datetime

        ts = datetime(2024, 1, 15, 12, 30, 45)
        model = TestModel(timestamp=ts)
        data = model.model_dump()
        assert data["timestamp"] == "2024-01-15T12:30:45"

    def test_rejects_extra_fields(self):
        """Extra fields are rejected."""

        class TestModel(AlmanakBaseModel):
            name: str

        with pytest.raises(ValidationError) as exc_info:
            TestModel(name="test", extra_field="oops")

        errors = exc_info.value.errors()
        assert any("extra" in str(e).lower() for e in errors)

    def test_json_roundtrip(self):
        """Model can be serialized to JSON and back."""

        class TestModel(AlmanakBaseModel):
            amount: SafeDecimal
            name: str

        original = TestModel(amount=Decimal("100.5"), name="test")
        json_str = original.model_dump_json()
        restored = TestModel.model_validate_json(json_str)

        assert restored.amount == original.amount
        assert restored.name == original.name


class TestAlmanakImmutableModel:
    """Test the AlmanakImmutableModel (frozen) base class."""

    def test_is_frozen(self):
        """Frozen models cannot be modified after creation."""

        class TestModel(AlmanakImmutableModel):
            value: int

        model = TestModel(value=42)

        with pytest.raises(ValidationError):
            model.value = 100

    def test_serialization_works(self):
        """Frozen models can still be serialized."""

        class TestModel(AlmanakImmutableModel):
            amount: SafeDecimal

        model = TestModel(amount=Decimal("50"))
        data = model.model_dump()
        assert data["amount"] == "50"


class TestAlmanakMutableModel:
    """Test the AlmanakMutableModel base class."""

    def test_allows_mutation(self):
        """Mutable models can be modified."""

        class TestModel(AlmanakMutableModel):
            value: int

        model = TestModel(value=42)
        model.value = 100
        assert model.value == 100

    def test_validates_on_mutation(self):
        """Mutations are validated."""

        class TestModel(AlmanakMutableModel):
            amount: SafeDecimal

        model = TestModel(amount=Decimal("100"))

        # Valid mutation
        model.amount = Decimal("200")
        assert model.amount == Decimal("200")

        # Int is converted
        model.amount = 300
        assert model.amount == Decimal("300")


class TestDefaultFactories:
    """Test default field factory functions."""

    def test_default_intent_id_is_uuid(self):
        """default_intent_id generates valid UUIDs."""
        id1 = default_intent_id()
        id2 = default_intent_id()

        # Should be strings
        assert isinstance(id1, str)
        assert isinstance(id2, str)

        # Should be unique
        assert id1 != id2

        # Should be valid UUID format (36 chars with hyphens)
        assert len(id1) == 36
        assert id1.count("-") == 4

    def test_default_timestamp_is_now(self):
        """default_timestamp returns current time."""
        before = datetime.now(UTC)
        ts = default_timestamp()
        after = datetime.now(UTC)

        assert before <= ts <= after
        assert isinstance(ts, datetime)


class TestUXPreservation:
    """Tests that verify user-facing API is preserved.

    These tests ensure that strategy authors can continue to use
    intuitive syntax like amount=1000 instead of amount=Decimal("1000").
    """

    def test_int_amount_accepted(self):
        """Strategy authors can use int for amounts."""

        class SwapIntentMock(AlmanakImmutableModel):
            from_token: str
            to_token: str
            amount_usd: OptionalSafeDecimal = None

        # This should work - common UX pattern
        intent = SwapIntentMock(from_token="USDC", to_token="ETH", amount_usd=1000)
        assert intent.amount_usd == Decimal("1000")

    def test_string_amount_accepted(self):
        """Strategy authors can use string for amounts."""

        class SwapIntentMock(AlmanakImmutableModel):
            from_token: str
            to_token: str
            amount_usd: OptionalSafeDecimal = None

        intent = SwapIntentMock(from_token="USDC", to_token="ETH", amount_usd="1000.50")
        assert intent.amount_usd == Decimal("1000.50")

    def test_decimal_amount_accepted(self):
        """Strategy authors can use Decimal for amounts."""

        class SwapIntentMock(AlmanakImmutableModel):
            from_token: str
            to_token: str
            amount_usd: OptionalSafeDecimal = None

        intent = SwapIntentMock(from_token="USDC", to_token="ETH", amount_usd=Decimal("1000"))
        assert intent.amount_usd == Decimal("1000")

    def test_float_amount_rejected_with_clear_error(self):
        """Float amounts are rejected with a helpful error message."""

        class SwapIntentMock(AlmanakImmutableModel):
            from_token: str
            to_token: str
            amount_usd: OptionalSafeDecimal = None

        with pytest.raises(ValidationError) as exc_info:
            SwapIntentMock(from_token="USDC", to_token="ETH", amount_usd=1000.0)

        # The error should explain the issue and suggest a fix
        error_str = str(exc_info.value)
        assert "Float" in error_str or "float" in error_str

    def test_chained_all_works(self):
        """The 'all' chained amount pattern works."""

        class BridgeIntentMock(AlmanakImmutableModel):
            token: str
            amount: ChainedAmount

        intent = BridgeIntentMock(token="ETH", amount="all")
        assert intent.amount == "all"

        # Serialization preserves "all"
        data = intent.model_dump()
        assert data["amount"] == "all"
