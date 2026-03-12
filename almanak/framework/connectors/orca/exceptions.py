"""Orca Whirlpools exceptions."""


class OrcaError(Exception):
    """Base exception for Orca operations."""


class OrcaAPIError(OrcaError):
    """Error communicating with the Orca API."""

    def __init__(
        self,
        message: str,
        status_code: int = 0,
        endpoint: str = "",
    ) -> None:
        self.status_code = status_code
        self.endpoint = endpoint
        super().__init__(message)


class OrcaConfigError(OrcaError):
    """Invalid Orca configuration."""

    def __init__(self, message: str, parameter: str = "") -> None:
        self.parameter = parameter
        super().__init__(message)


class OrcaPoolError(OrcaError):
    """Error with pool state or operations."""
