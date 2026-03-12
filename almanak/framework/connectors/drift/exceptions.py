"""Drift Protocol Exception Classes.

Custom exceptions for the Drift connector, providing detailed error
information for debugging and error handling.
"""

from typing import Any


class DriftError(Exception):
    """Base exception class for all Drift connector errors."""

    pass


class DriftAPIError(DriftError):
    """Exception raised for errors in the Drift Data API response.

    Attributes:
        message: Error message
        status_code: HTTP status code of the response
        endpoint: The API endpoint that was called
        error_code: Drift-specific error code
    """

    def __init__(
        self,
        message: str,
        status_code: int,
        endpoint: str | None = None,
        error_code: str | None = None,
    ):
        self.message = message
        self.status_code = status_code
        self.endpoint = endpoint
        self.error_code = error_code
        super().__init__(self.message)

    def __str__(self) -> str:
        error_msg = f"Drift API Error ({self.status_code}): {self.message}"
        if self.endpoint:
            error_msg += f"\nEndpoint: {self.endpoint}"
        if self.error_code:
            error_msg += f"\nCode: {self.error_code}"
        return error_msg


class DriftValidationError(DriftError):
    """Exception raised for validation errors.

    Attributes:
        message: Error message
        field: Name of the field that failed validation
        value: The invalid value
    """

    def __init__(
        self,
        message: str,
        field: str | None = None,
        value: Any | None = None,
    ):
        self.message = message
        self.field = field
        self.value = value
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.field and self.value:
            return f"Drift Validation Error: {self.message} (Field: {self.field}, Value: {self.value})"
        elif self.field:
            return f"Drift Validation Error: {self.message} (Field: {self.field})"
        return f"Drift Validation Error: {self.message}"


class DriftConfigError(DriftError):
    """Exception raised for configuration errors.

    Attributes:
        message: Error message
        parameter: Name of the configuration parameter that caused the error
    """

    def __init__(self, message: str, parameter: str | None = None):
        self.message = message
        self.parameter = parameter
        super().__init__(self.message)

    def __str__(self) -> str:
        if self.parameter:
            return f"Drift Config Error: {self.message} (Parameter: {self.parameter})"
        return f"Drift Config Error: {self.message}"


class DriftAccountNotFoundError(DriftError):
    """Exception raised when a Drift account is not found on-chain.

    Attributes:
        message: Error message
        account_type: Type of account (e.g., "User", "PerpMarket")
        address: The address that was looked up
    """

    def __init__(self, message: str, account_type: str = "", address: str = ""):
        self.message = message
        self.account_type = account_type
        self.address = address
        super().__init__(self.message)


class DriftMarketError(DriftError):
    """Exception raised for market-related errors.

    Attributes:
        message: Error message
        market: Market identifier
    """

    def __init__(self, message: str, market: str = ""):
        self.message = message
        self.market = market
        super().__init__(self.message)
