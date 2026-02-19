"""Enso SDK Exception Classes.

This module defines custom exceptions for the Enso SDK, providing
detailed error information for debugging and error handling.
"""

from typing import Any


class EnsoError(Exception):
    """Base exception class for all Enso SDK errors."""

    pass


class EnsoAPIError(EnsoError):
    """Exception raised for errors in the API response.

    Attributes:
        message: Error message
        status_code: HTTP status code of the response
        endpoint: The API endpoint that was called
        error_type: Classified error type (e.g., SERVER_ERROR, RATE_LIMIT)
        api_error_message: The specific error message from the API response
        error_data: Parsed error data from the response, if available
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
        self.api_error_message = self._extract_api_message()

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
        elif self.status_code == 403:
            return "AUTHORIZATION_ERROR"
        elif 400 <= self.status_code < 500:
            return "CLIENT_ERROR"
        else:
            return "UNKNOWN_ERROR"

    def _extract_api_message(self) -> str | None:
        """Extract error message from API response."""
        if not self.error_data or not isinstance(self.error_data, dict):
            return None

        # Handle different error formats from the API
        if "error" in self.error_data:
            error = self.error_data["error"]
            if isinstance(error, str):
                return error
            elif isinstance(error, dict) and "message" in error:
                return error["message"]
        elif "message" in self.error_data:
            return self.error_data["message"]
        elif "errorMessage" in self.error_data:
            return self.error_data["errorMessage"]

        return None

    def __str__(self) -> str:
        error_msg = f"API Error ({self.status_code}): {self.message}"
        if self.api_error_message:
            error_msg += f"\nAPI Message: {self.api_error_message}"
        if self.endpoint:
            error_msg += f"\nEndpoint: {self.endpoint}"
        if self.error_type:
            error_msg += f"\nError Type: {self.error_type}"
        if self.error_data and self.api_error_message is None:
            error_msg += f"\nDetails: {self.error_data}"
        return error_msg


class EnsoValidationError(EnsoError):
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


class EnsoConfigError(EnsoError):
    """Exception raised for SDK configuration errors.

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


class EnsoTokenError(EnsoError):
    """Exception raised for token-related errors.

    Attributes:
        message: Error message
        token_address: Address of the token that caused the error
        chain_id: Chain ID where the error occurred
    """

    def __init__(
        self,
        message: str,
        token_address: str | None = None,
        chain_id: int | None = None,
    ):
        self.message = message
        self.token_address = token_address
        self.chain_id = chain_id
        super().__init__(self.message)

    def __str__(self) -> str:
        error_msg = f"Token Error: {self.message}"
        if self.token_address:
            error_msg += f"\nToken Address: {self.token_address}"
        if self.chain_id:
            error_msg += f"\nChain ID: {self.chain_id}"
        return error_msg


class PriceImpactExceedsThresholdError(EnsoError):
    """Raised when route price impact exceeds maximum threshold.

    Attributes:
        message: Error message
        price_impact_bps: Actual price impact in basis points
        threshold_bps: Maximum allowed price impact in basis points
    """

    def __init__(
        self,
        message: str,
        price_impact_bps: float | None = None,
        threshold_bps: int | None = None,
    ):
        self.message = message
        self.price_impact_bps = price_impact_bps
        self.threshold_bps = threshold_bps
        super().__init__(self.message)

    def __str__(self) -> str:
        error_msg = f"Price Impact Error: {self.message}"
        if self.price_impact_bps is not None:
            error_msg += f"\nActual Price Impact: {self.price_impact_bps}bp ({self.price_impact_bps / 100:.2f}%)"
        if self.threshold_bps is not None:
            error_msg += f"\nThreshold: {self.threshold_bps}bp ({self.threshold_bps / 100:.2f}%)"
        return error_msg


class EnsoTransactionError(EnsoError):
    """Exception raised for blockchain transaction errors.

    Attributes:
        message: Error message
        tx_hash: Transaction hash if available
        error_data: Additional error data
    """

    def __init__(
        self,
        message: str,
        tx_hash: str | None = None,
        error_data: dict | None = None,
    ):
        self.message = message
        self.tx_hash = tx_hash
        self.error_data = error_data
        super().__init__(self.message)

    def __str__(self) -> str:
        error_msg = f"Transaction Error: {self.message}"
        if self.tx_hash:
            error_msg += f"\nTransaction Hash: {self.tx_hash}"
        if self.error_data:
            error_msg += f"\nDetails: {self.error_data}"
        return error_msg
