"""Unit tests for token resolution exceptions.

This module tests the token resolution exception classes, covering:
- TokenResolutionError base class fields and formatting
- TokenNotFoundError for unknown tokens
- TokenResolutionTimeoutError for on-chain timeouts
- InvalidTokenAddressError for malformed addresses
- AmbiguousTokenError for multiple token matches
- Inheritance hierarchy
- Exception message formatting
"""

import pytest

from almanak.framework.data.tokens.exceptions import (
    AmbiguousTokenError,
    InvalidTokenAddressError,
    TokenNotFoundError,
    TokenResolutionError,
    TokenResolutionTimeoutError,
)


class TestTokenResolutionError:
    """Tests for TokenResolutionError base class."""

    def test_basic_fields(self):
        """Test exception stores all basic fields."""
        error = TokenResolutionError(
            token="USDC",
            chain="arbitrum",
            reason="Test reason",
            suggestions=["Try this", "Or that"],
        )
        assert error.token == "USDC"
        assert error.chain == "arbitrum"
        assert error.reason == "Test reason"
        assert error.suggestions == ["Try this", "Or that"]

    def test_message_format_with_suggestions(self):
        """Test exception message includes suggestions."""
        error = TokenResolutionError(
            token="UNKNOWN",
            chain="ethereum",
            reason="Token not found",
            suggestions=["Check spelling", "Use address"],
        )
        message = str(error)
        assert "Cannot resolve token 'UNKNOWN' on ethereum" in message
        assert "Token not found" in message
        assert "Suggestions:" in message
        assert "Check spelling" in message
        assert "Use address" in message

    def test_message_format_without_suggestions(self):
        """Test exception message without suggestions."""
        error = TokenResolutionError(
            token="USDC",
            chain="base",
            reason="Chain not supported",
        )
        message = str(error)
        assert "Cannot resolve token 'USDC' on base: Chain not supported" in message
        assert "Suggestions:" not in message

    def test_empty_suggestions_list(self):
        """Test exception with empty suggestions list."""
        error = TokenResolutionError(
            token="WETH",
            chain="optimism",
            reason="Gateway unavailable",
            suggestions=[],
        )
        assert error.suggestions == []
        assert "Suggestions:" not in str(error)

    def test_repr(self):
        """Test repr contains all fields."""
        error = TokenResolutionError(
            token="DAI",
            chain="polygon",
            reason="Test error",
            suggestions=["Suggestion 1"],
        )
        repr_str = repr(error)
        assert "TokenResolutionError" in repr_str
        assert "token='DAI'" in repr_str
        assert "chain='polygon'" in repr_str
        assert "reason='Test error'" in repr_str
        assert "suggestions=['Suggestion 1']" in repr_str

    def test_is_exception_subclass(self):
        """Test TokenResolutionError is an Exception subclass."""
        error = TokenResolutionError(
            token="TEST",
            chain="ethereum",
            reason="Test",
        )
        assert isinstance(error, Exception)

    def test_can_be_raised_and_caught(self):
        """Test exception can be raised and caught."""
        with pytest.raises(TokenResolutionError) as exc_info:
            raise TokenResolutionError(
                token="TEST",
                chain="ethereum",
                reason="Test error",
            )
        assert exc_info.value.token == "TEST"


class TestTokenNotFoundError:
    """Tests for TokenNotFoundError subclass."""

    def test_inherits_from_token_resolution_error(self):
        """Test TokenNotFoundError inherits from TokenResolutionError."""
        error = TokenNotFoundError(
            token="UNKNOWN",
            chain="arbitrum",
        )
        assert isinstance(error, TokenResolutionError)
        assert isinstance(error, Exception)

    def test_default_reason(self):
        """Test default reason message."""
        error = TokenNotFoundError(
            token="NOTFOUND",
            chain="ethereum",
        )
        assert error.reason == "Token not found in any registry"

    def test_custom_reason(self):
        """Test custom reason overrides default."""
        error = TokenNotFoundError(
            token="CUSTOM",
            chain="base",
            reason="Custom reason provided",
        )
        assert error.reason == "Custom reason provided"

    def test_default_suggestions_added(self):
        """Test default suggestions are added."""
        error = TokenNotFoundError(
            token="TEST",
            chain="optimism",
        )
        suggestions = error.suggestions
        assert "Check the token symbol spelling" in suggestions
        assert "Use the full contract address if available" in suggestions
        assert "Verify the token exists on this chain" in suggestions

    def test_custom_suggestions_preserved(self):
        """Test custom suggestions are preserved with defaults."""
        error = TokenNotFoundError(
            token="TEST",
            chain="polygon",
            suggestions=["Custom suggestion"],
        )
        suggestions = error.suggestions
        assert "Custom suggestion" in suggestions
        # Defaults should also be present
        assert "Check the token symbol spelling" in suggestions

    def test_no_duplicate_suggestions(self):
        """Test suggestions aren't duplicated if provided."""
        error = TokenNotFoundError(
            token="TEST",
            chain="ethereum",
            suggestions=["Check the token symbol spelling"],  # Same as default
        )
        # Should only appear once
        count = error.suggestions.count("Check the token symbol spelling")
        assert count == 1

    def test_message_format(self):
        """Test message format includes token and chain."""
        error = TokenNotFoundError(
            token="UNKNOWNTOKEN",
            chain="arbitrum",
        )
        message = str(error)
        assert "UNKNOWNTOKEN" in message
        assert "arbitrum" in message
        assert "not found" in message.lower()

    def test_can_catch_as_base_class(self):
        """Test TokenNotFoundError can be caught as TokenResolutionError."""
        with pytest.raises(TokenResolutionError):
            raise TokenNotFoundError(
                token="TEST",
                chain="ethereum",
            )


class TestTokenResolutionTimeoutError:
    """Tests for TokenResolutionTimeoutError subclass."""

    def test_inherits_from_token_resolution_error(self):
        """Test TokenResolutionTimeoutError inherits from TokenResolutionError."""
        error = TokenResolutionTimeoutError(
            token="0x1234567890abcdef1234567890abcdef12345678",
            chain="ethereum",
            timeout_seconds=10.0,
        )
        assert isinstance(error, TokenResolutionError)
        assert isinstance(error, Exception)

    def test_timeout_seconds_field(self):
        """Test timeout_seconds field is stored."""
        error = TokenResolutionTimeoutError(
            token="0xabcd",
            chain="arbitrum",
            timeout_seconds=5.5,
        )
        assert error.timeout_seconds == 5.5

    def test_auto_generated_reason(self):
        """Test reason is auto-generated from timeout."""
        error = TokenResolutionTimeoutError(
            token="0xtest",
            chain="base",
            timeout_seconds=10.0,
        )
        assert "10.0 seconds" in error.reason

    def test_custom_reason_overrides_auto(self):
        """Test custom reason overrides auto-generated."""
        error = TokenResolutionTimeoutError(
            token="0xtest",
            chain="optimism",
            timeout_seconds=10.0,
            reason="Custom timeout reason",
        )
        assert error.reason == "Custom timeout reason"

    def test_default_suggestions(self):
        """Test default suggestions are added."""
        error = TokenResolutionTimeoutError(
            token="0xtest",
            chain="polygon",
            timeout_seconds=10.0,
        )
        suggestions = error.suggestions
        assert any("RPC endpoint" in s for s in suggestions)
        assert any("timeout" in s.lower() for s in suggestions)

    def test_message_includes_timeout(self):
        """Test message includes timeout information."""
        error = TokenResolutionTimeoutError(
            token="0x1234",
            chain="ethereum",
            timeout_seconds=15.0,
        )
        message = str(error)
        assert "15.0" in message
        assert "timeout" in message.lower()


class TestInvalidTokenAddressError:
    """Tests for InvalidTokenAddressError subclass."""

    def test_inherits_from_token_resolution_error(self):
        """Test InvalidTokenAddressError inherits from TokenResolutionError."""
        error = InvalidTokenAddressError(
            token="0xinvalid",
            chain="ethereum",
        )
        assert isinstance(error, TokenResolutionError)
        assert isinstance(error, Exception)

    def test_default_reason(self):
        """Test default reason message."""
        error = InvalidTokenAddressError(
            token="invalid_address",
            chain="arbitrum",
        )
        assert error.reason == "Invalid token address format"

    def test_custom_reason(self):
        """Test custom reason overrides default."""
        error = InvalidTokenAddressError(
            token="bad_addr",
            chain="base",
            reason="Address checksum failed",
        )
        assert error.reason == "Address checksum failed"

    def test_default_suggestions(self):
        """Test default suggestions are provided."""
        error = InvalidTokenAddressError(
            token="0xshort",
            chain="optimism",
        )
        suggestions = error.suggestions
        assert any("0x" in s for s in suggestions)
        assert any("42 characters" in s for s in suggestions)
        assert any("hexadecimal" in s.lower() for s in suggestions)

    def test_message_format(self):
        """Test message includes invalid address."""
        error = InvalidTokenAddressError(
            token="not_an_address",
            chain="ethereum",
            reason="Missing 0x prefix",
        )
        message = str(error)
        assert "not_an_address" in message
        assert "Missing 0x prefix" in message


class TestAmbiguousTokenError:
    """Tests for AmbiguousTokenError subclass."""

    def test_inherits_from_token_resolution_error(self):
        """Test AmbiguousTokenError inherits from TokenResolutionError."""
        error = AmbiguousTokenError(
            token="USDC",
            chain="arbitrum",
        )
        assert isinstance(error, TokenResolutionError)
        assert isinstance(error, Exception)

    def test_matching_addresses_field(self):
        """Test matching_addresses field is stored."""
        addresses = [
            "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
            "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
        ]
        error = AmbiguousTokenError(
            token="USDC",
            chain="arbitrum",
            matching_addresses=addresses,
        )
        assert error.matching_addresses == addresses

    def test_empty_matching_addresses(self):
        """Test empty matching_addresses defaults to empty list."""
        error = AmbiguousTokenError(
            token="USDC",
            chain="base",
        )
        assert error.matching_addresses == []

    def test_default_reason(self):
        """Test default reason message."""
        error = AmbiguousTokenError(
            token="USDC",
            chain="arbitrum",
        )
        assert error.reason == "Multiple tokens match the identifier"

    def test_custom_reason(self):
        """Test custom reason overrides default."""
        error = AmbiguousTokenError(
            token="USDC",
            chain="arbitrum",
            reason="Native and bridged USDC both exist",
        )
        assert error.reason == "Native and bridged USDC both exist"

    def test_default_suggestions(self):
        """Test default suggestions are provided."""
        error = AmbiguousTokenError(
            token="USDC",
            chain="arbitrum",
        )
        suggestions = error.suggestions
        assert any("contract address" in s for s in suggestions)
        assert any("variant" in s for s in suggestions)

    def test_custom_suggestions_with_addresses(self):
        """Test custom suggestions with specific addresses."""
        error = AmbiguousTokenError(
            token="USDC",
            chain="arbitrum",
            matching_addresses=[
                "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
                "0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8",
            ],
            suggestions=[
                "Use 'USDC' for native: 0xaf88...",
                "Use 'USDC.e' for bridged: 0xFF97...",
            ],
        )
        assert "Use 'USDC' for native: 0xaf88..." in error.suggestions

    def test_message_format(self):
        """Test message includes ambiguous token."""
        error = AmbiguousTokenError(
            token="USDC",
            chain="arbitrum",
            reason="Multiple USDC variants found",
        )
        message = str(error)
        assert "USDC" in message
        assert "arbitrum" in message
        assert "Multiple USDC variants found" in message


class TestExceptionHierarchy:
    """Tests for exception inheritance hierarchy."""

    def test_all_exceptions_inherit_from_base(self):
        """Test all token exceptions inherit from TokenResolutionError."""
        exceptions = [
            TokenNotFoundError("T", "c"),
            TokenResolutionTimeoutError("T", "c", 10.0),
            InvalidTokenAddressError("T", "c"),
            AmbiguousTokenError("T", "c"),
        ]
        for exc in exceptions:
            assert isinstance(exc, TokenResolutionError)

    def test_all_exceptions_inherit_from_exception(self):
        """Test all token exceptions inherit from Exception."""
        exceptions = [
            TokenResolutionError("T", "c", "r"),
            TokenNotFoundError("T", "c"),
            TokenResolutionTimeoutError("T", "c", 10.0),
            InvalidTokenAddressError("T", "c"),
            AmbiguousTokenError("T", "c"),
        ]
        for exc in exceptions:
            assert isinstance(exc, Exception)

    def test_catch_all_with_base_class(self):
        """Test all subclasses can be caught with base class."""
        exceptions_to_raise = [
            TokenNotFoundError("T", "c"),
            TokenResolutionTimeoutError("T", "c", 10.0),
            InvalidTokenAddressError("T", "c"),
            AmbiguousTokenError("T", "c"),
        ]
        for exc in exceptions_to_raise:
            with pytest.raises(TokenResolutionError):
                raise exc

    def test_specific_exceptions_have_repr(self):
        """Test all exceptions have proper repr."""
        exceptions = [
            TokenResolutionError("T", "c", "r", ["s"]),
            TokenNotFoundError("T", "c"),
            TokenResolutionTimeoutError("T", "c", 10.0),
            InvalidTokenAddressError("T", "c"),
            AmbiguousTokenError("T", "c"),
        ]
        for exc in exceptions:
            repr_str = repr(exc)
            # All should include class name
            assert exc.__class__.__name__ in repr_str
            # All should include token and chain
            assert "token=" in repr_str
            assert "chain=" in repr_str
