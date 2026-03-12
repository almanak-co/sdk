"""Jupiter DEX Aggregator Exception Classes.

Custom exceptions for the Jupiter connector, providing detailed error
information for debugging and error handling.
"""

from typing import Any


class JupiterError(Exception):
    """Base exception class for all Jupiter connector errors."""

    pass


class JupiterAPIError(JupiterError):
    """Exception raised for errors in the Jupiter API response.

    Attributes:
        message: Error message
        status_code: HTTP status code of the response
        endpoint: The API endpoint that was called
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

        super().__init__(self.message)

    def __str__(self) -> str:
        error_msg = f"Jupiter API Error ({self.status_code}): {self.message}"
        if self.endpoint:
            error_msg += f"\nEndpoint: {self.endpoint}"
        if self.error_data:
            error_msg += f"\nDetails: {self.error_data}"
        return error_msg


class JupiterValidationError(JupiterError):
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
            return f"Jupiter Validation Error: {self.message} (Field: {self.field}, Value: {self.value})"
        elif self.field:
            return f"Jupiter Validation Error: {self.message} (Field: {self.field})"
        return f"Jupiter Validation Error: {self.message}"


class JupiterConfigError(JupiterError):
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
            return f"Jupiter Config Error: {self.message} (Parameter: {self.parameter})"
        return f"Jupiter Config Error: {self.message}"


class JupiterPriceImpactError(JupiterError):
    """Raised when swap price impact exceeds maximum threshold.

    Attributes:
        message: Error message
        price_impact_pct: Actual price impact as percentage
        threshold_pct: Maximum allowed price impact as percentage
    """

    def __init__(
        self,
        message: str,
        price_impact_pct: float | None = None,
        threshold_pct: float | None = None,
    ):
        self.message = message
        self.price_impact_pct = price_impact_pct
        self.threshold_pct = threshold_pct
        super().__init__(self.message)

    def __str__(self) -> str:
        error_msg = f"Jupiter Price Impact Error: {self.message}"
        if self.price_impact_pct is not None:
            error_msg += f"\nActual Price Impact: {self.price_impact_pct:.4f}%"
        if self.threshold_pct is not None:
            error_msg += f"\nThreshold: {self.threshold_pct:.4f}%"
        return error_msg
