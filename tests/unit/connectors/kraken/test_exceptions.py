"""Tests for Kraken exceptions."""

import pytest

from almanak.framework.connectors.kraken.exceptions import (
    KrakenAPIError,
    KrakenAuthenticationError,
    KrakenChainNotSupportedError,
    KrakenDepositError,
    KrakenError,
    KrakenInsufficientFundsError,
    KrakenMinimumOrderError,
    KrakenOrderCancelledError,
    KrakenOrderError,
    KrakenOrderNotFoundError,
    KrakenRateLimitError,
    KrakenTimeoutError,
    KrakenUnknownAssetError,
    KrakenUnknownPairError,
    KrakenWithdrawalAddressNotWhitelistedError,
    KrakenWithdrawalError,
    KrakenWithdrawalLimitExceededError,
)


class TestExceptionHierarchy:
    """Tests for exception inheritance."""

    def test_all_exceptions_inherit_from_kraken_error(self):
        """All Kraken exceptions should inherit from KrakenError."""
        exceptions = [
            KrakenAuthenticationError("test"),
            KrakenRateLimitError("test"),
            KrakenInsufficientFundsError("test", "ETH", "1.0", "0.5"),
            KrakenMinimumOrderError("test", "ETHUSD", "0.001", "0.01"),
            KrakenUnknownAssetError("XYZ"),
            KrakenUnknownPairError("XYZUSD"),
            KrakenWithdrawalError("test"),
            KrakenDepositError("test"),
            KrakenOrderError("test"),
            KrakenChainNotSupportedError("solana", "withdrawal"),
            KrakenTimeoutError("swap", 300),
            KrakenAPIError(["error1", "error2"]),
        ]

        for exc in exceptions:
            assert isinstance(exc, KrakenError)

    def test_withdrawal_exceptions_inherit_from_withdrawal_error(self):
        """Withdrawal exceptions should inherit from KrakenWithdrawalError."""
        exc1 = KrakenWithdrawalAddressNotWhitelistedError("0x...", "ETH", "arbitrum")
        exc2 = KrakenWithdrawalLimitExceededError("test", "100000", "50000")

        assert isinstance(exc1, KrakenWithdrawalError)
        assert isinstance(exc2, KrakenWithdrawalError)

    def test_order_exceptions_inherit_from_order_error(self):
        """Order exceptions should inherit from KrakenOrderError."""
        exc1 = KrakenOrderNotFoundError("ORDER123")
        exc2 = KrakenOrderCancelledError("ORDER123")

        assert isinstance(exc1, KrakenOrderError)
        assert isinstance(exc2, KrakenOrderError)


class TestKrakenRateLimitError:
    """Tests for KrakenRateLimitError."""

    def test_with_retry_after(self):
        """Should store retry_after value."""
        exc = KrakenRateLimitError("Rate limited", retry_after=60)
        assert str(exc) == "Rate limited"
        assert exc.retry_after == 60

    def test_without_retry_after(self):
        """Should work without retry_after."""
        exc = KrakenRateLimitError("Rate limited")
        assert exc.retry_after is None


class TestKrakenInsufficientFundsError:
    """Tests for KrakenInsufficientFundsError."""

    def test_stores_balance_info(self):
        """Should store balance information."""
        exc = KrakenInsufficientFundsError(
            "Insufficient balance",
            asset="ETH",
            requested="1.5",
            available="0.5",
        )
        assert exc.asset == "ETH"
        assert exc.requested == "1.5"
        assert exc.available == "0.5"


class TestKrakenMinimumOrderError:
    """Tests for KrakenMinimumOrderError."""

    def test_stores_order_info(self):
        """Should store order size information."""
        exc = KrakenMinimumOrderError(
            "Order too small",
            pair="ETHUSD",
            amount="0.001",
            minimum="0.01",
        )
        assert exc.pair == "ETHUSD"
        assert exc.amount == "0.001"
        assert exc.minimum == "0.01"


class TestKrakenUnknownAssetError:
    """Tests for KrakenUnknownAssetError."""

    def test_creates_helpful_message(self):
        """Should create a helpful error message."""
        exc = KrakenUnknownAssetError("INVALIDTOKEN")
        assert "INVALIDTOKEN" in str(exc)
        assert exc.asset == "INVALIDTOKEN"


class TestKrakenUnknownPairError:
    """Tests for KrakenUnknownPairError."""

    def test_creates_helpful_message(self):
        """Should create a helpful error message."""
        exc = KrakenUnknownPairError("INVALIDPAIR")
        assert "INVALIDPAIR" in str(exc)
        assert exc.pair == "INVALIDPAIR"


class TestKrakenWithdrawalAddressNotWhitelistedError:
    """Tests for KrakenWithdrawalAddressNotWhitelistedError."""

    def test_creates_detailed_message(self):
        """Should create detailed error message with address info."""
        exc = KrakenWithdrawalAddressNotWhitelistedError(
            "0x1234567890abcdef",
            "ETH",
            "arbitrum",
        )
        assert "0x1234567890abcdef" in str(exc)
        assert "ETH" in str(exc)
        assert "arbitrum" in str(exc)
        assert exc.address == "0x1234567890abcdef"
        assert exc.asset == "ETH"
        assert exc.chain == "arbitrum"


class TestKrakenWithdrawalLimitExceededError:
    """Tests for KrakenWithdrawalLimitExceededError."""

    def test_stores_limit_info(self):
        """Should store limit information."""
        exc = KrakenWithdrawalLimitExceededError(
            "Limit exceeded",
            amount="100000",
            limit="50000",
        )
        assert exc.amount == "100000"
        assert exc.limit == "50000"


class TestKrakenOrderNotFoundError:
    """Tests for KrakenOrderNotFoundError."""

    def test_with_order_id_only(self):
        """Should work with just order ID."""
        exc = KrakenOrderNotFoundError("ORDER123")
        assert "ORDER123" in str(exc)
        assert exc.order_id == "ORDER123"
        assert exc.userref is None

    def test_with_userref(self):
        """Should include userref in message."""
        exc = KrakenOrderNotFoundError("ORDER123", userref=12345678)
        assert "ORDER123" in str(exc)
        assert "12345678" in str(exc)
        assert exc.userref == 12345678


class TestKrakenOrderCancelledError:
    """Tests for KrakenOrderCancelledError."""

    def test_without_reason(self):
        """Should work without reason."""
        exc = KrakenOrderCancelledError("ORDER123")
        assert "ORDER123" in str(exc)
        assert exc.reason is None

    def test_with_reason(self):
        """Should include reason in message."""
        exc = KrakenOrderCancelledError("ORDER123", reason="User requested")
        assert "ORDER123" in str(exc)
        assert "User requested" in str(exc)
        assert exc.reason == "User requested"


class TestKrakenChainNotSupportedError:
    """Tests for KrakenChainNotSupportedError."""

    def test_creates_helpful_message(self):
        """Should create helpful error message."""
        exc = KrakenChainNotSupportedError("solana", "withdrawal")
        assert "solana" in str(exc)
        assert "withdrawal" in str(exc)
        assert exc.chain == "solana"
        assert exc.operation == "withdrawal"


class TestKrakenTimeoutError:
    """Tests for KrakenTimeoutError."""

    def test_without_identifier(self):
        """Should work without identifier."""
        exc = KrakenTimeoutError("swap", 300)
        assert "swap" in str(exc)
        assert "300" in str(exc)
        assert exc.operation == "swap"
        assert exc.timeout_seconds == 300
        assert exc.identifier is None

    def test_with_identifier(self):
        """Should include identifier in message."""
        exc = KrakenTimeoutError("swap", 300, identifier="ORDER123")
        assert "ORDER123" in str(exc)
        assert exc.identifier == "ORDER123"


class TestKrakenAPIError:
    """Tests for KrakenAPIError."""

    def test_single_error(self):
        """Should format single error."""
        exc = KrakenAPIError(["EOrder:Insufficient funds"])
        assert "EOrder:Insufficient funds" in str(exc)
        assert exc.errors == ["EOrder:Insufficient funds"]

    def test_multiple_errors(self):
        """Should join multiple errors with semicolon."""
        exc = KrakenAPIError(["Error 1", "Error 2"])
        assert "Error 1; Error 2" == str(exc)

    def test_empty_errors(self):
        """Should handle empty error list."""
        exc = KrakenAPIError([])
        assert "Unknown" in str(exc)
        assert exc.errors == []


class TestExceptionCatching:
    """Tests for exception catching patterns."""

    def test_catch_specific_then_general(self):
        """Should be catchable in specific then general order."""

        def raise_insufficient_funds():
            raise KrakenInsufficientFundsError("test", "ETH", "1.0", "0.5")

        # Catch specific first
        with pytest.raises(KrakenInsufficientFundsError):
            raise_insufficient_funds()

        # Can also catch with general exception
        with pytest.raises(KrakenError):
            raise_insufficient_funds()

    def test_catch_withdrawal_hierarchy(self):
        """Should catch withdrawal errors at different levels."""

        def raise_whitelist_error():
            raise KrakenWithdrawalAddressNotWhitelistedError("0x...", "ETH", "arb")

        # Most specific
        with pytest.raises(KrakenWithdrawalAddressNotWhitelistedError):
            raise_whitelist_error()

        # Parent class
        with pytest.raises(KrakenWithdrawalError):
            raise_whitelist_error()

        # Base class
        with pytest.raises(KrakenError):
            raise_whitelist_error()
