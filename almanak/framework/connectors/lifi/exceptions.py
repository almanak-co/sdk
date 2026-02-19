"""LiFi SDK Exception Classes.

This module defines custom exceptions for the LiFi connector, providing
detailed error information for debugging and error handling.
"""

from typing import Any


class LiFiError(Exception):
    """Base exception class for all LiFi connector errors."""

    pass


class LiFiAPIError(LiFiError):
    """Exception raised for errors in the LiFi API response.

    Attributes:
        message: Error message
        status_code: HTTP status code of the response
        endpoint: The API endpoint that was called
        error_type: Classified error type
        error_data: Parsed error data from the response
    """

    def __init__(
        self,
        message: str,
        status_code: int,
        endpoint: str | None = None,
        error_data: dict | None = None,
    ):
        self.message = message
        self.status_code = status_code
        self.endpoint = endpoint
        self.error_data = error_data
        self.error_type = self._classify_error()

        super().__init__(self.message)

    def _classify_error(self) -> str:
        """Classify the error based on status code."""
        if self.status_code == 429:
            return "RATE_LIMIT"
        elif 500 <= self.status_code < 600:
            return "SERVER_ERROR"
        elif self.status_code == 400:
            return "VALIDATION_ERROR"
        elif self.status_code == 401:
            return "AUTHENTICATION_ERROR"
        elif self.status_code == 404:
            return "NOT_FOUND"
        elif 400 <= self.status_code < 500:
            return "CLIENT_ERROR"
        else:
            return "UNKNOWN_ERROR"

    def __str__(self) -> str:
        error_msg = f"LiFi API Error ({self.status_code}): {self.message}"
        if self.endpoint:
            error_msg += f"\nEndpoint: {self.endpoint}"
        if self.error_type:
            error_msg += f"\nError Type: {self.error_type}"
        if self.error_data:
            error_msg += f"\nDetails: {self.error_data}"
        return error_msg


class LiFiConfigError(LiFiError):
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
            return f"Configuration Error: {self.message} (Parameter: {self.parameter})"
        return f"Configuration Error: {self.message}"


class LiFiValidationError(LiFiError):
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
            return f"Validation Error: {self.message} (Field: {self.field}, Value: {self.value})"
        elif self.field:
            return f"Validation Error: {self.message} (Field: {self.field})"
        return f"Validation Error: {self.message}"


class LiFiRouteNotFoundError(LiFiError):
    """Raised when LiFi cannot find a route for the requested transfer."""

    pass


class LiFiTransferFailedError(LiFiError):
    """Raised when a LiFi cross-chain transfer fails or is refunded.

    Attributes:
        message: Error message
        tx_hash: Source chain transaction hash
        status: LiFi status value (FAILED, etc.)
        substatus: LiFi substatus (REFUNDED, PARTIAL, etc.)
    """

    def __init__(
        self,
        message: str,
        tx_hash: str | None = None,
        status: str | None = None,
        substatus: str | None = None,
    ):
        self.message = message
        self.tx_hash = tx_hash
        self.status = status
        self.substatus = substatus
        super().__init__(self.message)

    def __str__(self) -> str:
        error_msg = f"Transfer Failed: {self.message}"
        if self.tx_hash:
            error_msg += f"\nTX Hash: {self.tx_hash}"
        if self.status:
            error_msg += f"\nStatus: {self.status}"
        if self.substatus:
            error_msg += f"\nSubstatus: {self.substatus}"
        return error_msg
