"""Kraken-specific exceptions.

This module defines custom exceptions for Kraken CEX operations.
These exceptions provide clear error messages and can be caught
specifically to handle different failure modes.
"""


class KrakenError(Exception):
    """Base exception for all Kraken-related errors."""

    pass


class KrakenAuthenticationError(KrakenError):
    """Authentication failed with Kraken API.

    Raised when:
    - API key is invalid or expired
    - API secret is incorrect
    - Insufficient permissions for the requested operation
    """

    pass


class KrakenRateLimitError(KrakenError):
    """Rate limit exceeded on Kraken API.

    Kraken enforces strict rate limits. This error includes
    information about when to retry.
    """

    def __init__(self, message: str, retry_after: int | None = None) -> None:
        super().__init__(message)
        self.retry_after = retry_after


class KrakenInsufficientFundsError(KrakenError):
    """Insufficient balance for the requested operation.

    Raised when trying to trade or withdraw more than available balance.
    """

    def __init__(
        self,
        message: str,
        asset: str,
        requested: str,
        available: str,
    ) -> None:
        super().__init__(message)
        self.asset = asset
        self.requested = requested
        self.available = available


class KrakenMinimumOrderError(KrakenError):
    """Order amount is below Kraken's minimum.

    Different trading pairs have different minimum order sizes.
    """

    def __init__(
        self,
        message: str,
        pair: str,
        amount: str,
        minimum: str,
    ) -> None:
        super().__init__(message)
        self.pair = pair
        self.amount = amount
        self.minimum = minimum


class KrakenUnknownAssetError(KrakenError):
    """Asset is not supported or not found on Kraken."""

    def __init__(self, asset: str) -> None:
        super().__init__(f"Unknown or unsupported asset on Kraken: {asset}")
        self.asset = asset


class KrakenUnknownPairError(KrakenError):
    """Trading pair does not exist on Kraken."""

    def __init__(self, pair: str) -> None:
        super().__init__(f"Trading pair not found on Kraken: {pair}")
        self.pair = pair


class KrakenWithdrawalError(KrakenError):
    """Withdrawal operation failed."""

    pass


class KrakenWithdrawalAddressNotWhitelistedError(KrakenWithdrawalError):
    """Withdrawal address is not whitelisted on the Kraken account.

    For security, Kraken requires withdrawal addresses to be
    pre-approved in the account settings.
    """

    def __init__(self, address: str, asset: str, chain: str) -> None:
        super().__init__(
            f"Address {address} not whitelisted for {asset} on {chain}. "
            f"Please add this address in Kraken account settings."
        )
        self.address = address
        self.asset = asset
        self.chain = chain


class KrakenWithdrawalLimitExceededError(KrakenWithdrawalError):
    """Withdrawal exceeds daily or account limits."""

    def __init__(
        self,
        message: str,
        amount: str,
        limit: str,
    ) -> None:
        super().__init__(message)
        self.amount = amount
        self.limit = limit


class KrakenDepositError(KrakenError):
    """Deposit operation failed or not found."""

    pass


class KrakenOrderError(KrakenError):
    """Order placement or query failed."""

    pass


class KrakenOrderNotFoundError(KrakenOrderError):
    """Order with given ID not found."""

    def __init__(self, order_id: str, userref: int | None = None) -> None:
        msg = f"Order not found: {order_id}"
        if userref is not None:
            msg += f" (userref: {userref})"
        super().__init__(msg)
        self.order_id = order_id
        self.userref = userref


class KrakenOrderCancelledError(KrakenOrderError):
    """Order was cancelled before completion."""

    def __init__(
        self,
        order_id: str,
        reason: str | None = None,
    ) -> None:
        msg = f"Order {order_id} was cancelled"
        if reason:
            msg += f": {reason}"
        super().__init__(msg)
        self.order_id = order_id
        self.reason = reason


class KrakenChainNotSupportedError(KrakenError):
    """Chain is not supported for the requested operation."""

    def __init__(self, chain: str, operation: str) -> None:
        super().__init__(f"Chain {chain} not supported for {operation} on Kraken")
        self.chain = chain
        self.operation = operation


class KrakenTimeoutError(KrakenError):
    """Operation timed out waiting for completion."""

    def __init__(
        self,
        operation: str,
        timeout_seconds: int,
        identifier: str | None = None,
    ) -> None:
        msg = f"Kraken {operation} timed out after {timeout_seconds}s"
        if identifier:
            msg += f" (id: {identifier})"
        super().__init__(msg)
        self.operation = operation
        self.timeout_seconds = timeout_seconds
        self.identifier = identifier


class KrakenAPIError(KrakenError):
    """Generic Kraken API error with error codes.

    Wraps errors returned directly from the Kraken API.
    """

    def __init__(self, errors: list[str]) -> None:
        message = "; ".join(errors) if errors else "Unknown Kraken API error"
        super().__init__(message)
        self.errors = errors


__all__ = [
    "KrakenError",
    "KrakenAuthenticationError",
    "KrakenRateLimitError",
    "KrakenInsufficientFundsError",
    "KrakenMinimumOrderError",
    "KrakenUnknownAssetError",
    "KrakenUnknownPairError",
    "KrakenWithdrawalError",
    "KrakenWithdrawalAddressNotWhitelistedError",
    "KrakenWithdrawalLimitExceededError",
    "KrakenDepositError",
    "KrakenOrderError",
    "KrakenOrderNotFoundError",
    "KrakenOrderCancelledError",
    "KrakenChainNotSupportedError",
    "KrakenTimeoutError",
    "KrakenAPIError",
]
