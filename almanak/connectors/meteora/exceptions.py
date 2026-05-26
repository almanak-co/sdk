"""Meteora DLMM exceptions."""


class MeteoraError(Exception):
    """Base exception for Meteora operations."""


class MeteoraAPIError(MeteoraError):
    """Error communicating with the Meteora DLMM API."""

    def __init__(
        self,
        message: str,
        status_code: int = 0,
        endpoint: str = "",
    ) -> None:
        self.status_code = status_code
        self.endpoint = endpoint
        super().__init__(message)


class MeteoraPoolError(MeteoraError):
    """Error with pool state or operations."""


class MeteoraPositionError(MeteoraError):
    """Error with position state or operations."""
