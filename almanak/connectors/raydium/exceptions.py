"""Raydium CLMM exceptions."""


class RaydiumError(Exception):
    """Base exception for Raydium operations."""


class RaydiumAPIError(RaydiumError):
    """Error communicating with the Raydium API."""

    def __init__(
        self,
        message: str,
        status_code: int = 0,
        endpoint: str = "",
    ) -> None:
        self.status_code = status_code
        self.endpoint = endpoint
        super().__init__(message)


class RaydiumConfigError(RaydiumError):
    """Invalid Raydium configuration."""

    def __init__(self, message: str, parameter: str = "") -> None:
        self.parameter = parameter
        super().__init__(message)


class RaydiumPoolError(RaydiumError):
    """Error with pool state or operations."""


class RaydiumTickError(RaydiumError):
    """Error with tick calculations."""
