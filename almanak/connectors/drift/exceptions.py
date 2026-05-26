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


class DriftInstructionFallbackError(DriftError):
    """Raised when the on-chain Drift program rejects an instruction with
    Anchor error 101 (``InstructionFallbackNotFound``).

    Anchor error 101 fires when the program cannot match an instruction's
    8-byte discriminator to any registered handler in its IDL — meaning the
    SDK's serialized instruction data is for an older or differently-versioned
    Drift program than what's deployed.

    The SDK cannot self-heal this — only Almanak shipping a refreshed
    discriminator + arg-encoding bundle (or migrating to the upstream
    ``drift-protocol-v2-py`` SDK) can resolve it. Raising a typed error with a
    stable prefix lets the framework state machine classify it as
    ``COMPILATION_PERMANENT`` and stop the retry storm immediately.

    Attributes:
        instruction_name: The Drift instruction the SDK was attempting to send
            (``"place_perp_order"``, ``"initialize_user"``, etc.).
        program_id: The on-chain program ID the instruction targeted.
        layout_version: ``DRIFT_LAYOUT_VERSION`` value at the time of failure.

    Strategies can match on the stable error-message prefix
    (``"Drift instruction not recognized by on-chain program"``) to emit a
    clean ``Intent.hold(...)``.
    """

    ERROR_PREFIX = "Drift instruction not recognized by on-chain program"

    def __init__(
        self,
        *,
        instruction_name: str,
        program_id: str,
        layout_version: str,
    ) -> None:
        self.instruction_name = instruction_name
        self.program_id = program_id
        self.layout_version = layout_version
        super().__init__(
            f"{self.ERROR_PREFIX}: instruction='{instruction_name}', "
            f"program={program_id[:8]}..., sdk_layout_version={layout_version}. "
            f"This is Anchor error 101 (InstructionFallbackNotFound) — the "
            f"SDK's instruction discriminator does not match any handler in "
            f"the deployed program's IDL. Drift likely upgraded; refresh the "
            f"connector against the current IDL or migrate to the upstream "
            f"drift-protocol-v2-py SDK."
        )


class DriftDiscriminatorMismatchError(DriftError):
    """Raised at SDK construction when a hardcoded discriminator does not
    match the canonical Anchor encoding ``sha256("global:<name>")[:8]``.

    Protects against silent edits to ``constants.py`` that would brick the
    connector — every discriminator constant must round-trip through the
    Anchor formula. Caught at import time so CI fails before any tx is
    submitted.

    Attributes:
        instruction_name: The Anchor instruction name (e.g.
            ``"place_perp_order"``).
        expected: 8-byte discriminator computed from the Anchor formula.
        actual: 8-byte discriminator stored in ``constants.py``.
    """

    def __init__(
        self,
        instruction_name: str,
        expected: bytes,
        actual: bytes,
    ) -> None:
        self.instruction_name = instruction_name
        self.expected = expected
        self.actual = actual
        super().__init__(
            f"Drift discriminator mismatch for '{instruction_name}': "
            f"expected sha256('global:{instruction_name}')[:8] = {expected.hex()}, "
            f"got {actual.hex()}. Edit constants.py to match the canonical "
            f"Anchor encoding."
        )
