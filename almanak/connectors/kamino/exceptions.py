"""Kamino Finance Lending Exception Classes.

Custom exceptions for the Kamino connector, providing detailed error
information for debugging and error handling.
"""

from typing import Any


class KaminoError(Exception):
    """Base exception class for all Kamino connector errors."""

    pass


class KaminoAPIError(KaminoError):
    """Exception raised for errors in the Kamino API response.

    Attributes:
        message: Error message
        status_code: HTTP status code of the response
        endpoint: The API endpoint that was called
        error_code: Kamino-specific error code (e.g., KLEND_RESERVE_NOT_FOUND)
        error_data: Parsed error data from the response
    """

    def __init__(
        self,
        message: str,
        status_code: int,
        endpoint: str | None = None,
        error_code: str | None = None,
        error_data: dict | None = None,
    ):
        self.message = message
        self.status_code = status_code
        self.endpoint = endpoint
        self.error_code = error_code
        self.error_data = error_data
        super().__init__(self.message)

    def __str__(self) -> str:
        error_msg = f"Kamino API Error ({self.status_code}): {self.message}"
        if self.endpoint:
            error_msg += f"\nEndpoint: {self.endpoint}"
        if self.error_code:
            error_msg += f"\nCode: {self.error_code}"
        if self.error_data:
            error_msg += f"\nDetails: {self.error_data}"
        return error_msg


class KaminoValidationError(KaminoError):
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
            return f"Kamino Validation Error: {self.message} (Field: {self.field}, Value: {self.value})"
        elif self.field:
            return f"Kamino Validation Error: {self.message} (Field: {self.field})"
        return f"Kamino Validation Error: {self.message}"


class KaminoConfigError(KaminoError):
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
            return f"Kamino Config Error: {self.message} (Parameter: {self.parameter})"
        return f"Kamino Config Error: {self.message}"
