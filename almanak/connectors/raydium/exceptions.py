"""Raydium CLMM exceptions."""

from almanak.connectors._strategy_base.solana_clmm_math import SolanaCLMMTickError


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


# Tick maths moved to the shared Solana CLMM foundation
# (almanak.connectors._strategy_base.solana_clmm_math), which raises
# SolanaCLMMTickError. RaydiumTickError is retained as a public back-compat
# alias of that foundation error so existing `except RaydiumTickError` /
# `pytest.raises(RaydiumTickError)` callers keep working.
RaydiumTickError = SolanaCLMMTickError
