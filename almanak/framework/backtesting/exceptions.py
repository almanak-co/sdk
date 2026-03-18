"""Exceptions for backtesting module.

This module defines custom exceptions for backtesting operations,
including historical data unavailability and validation errors.

Example:
    from almanak.framework.backtesting.exceptions import HistoricalDataUnavailableError

    if strict_mode and volume_data is None:
        raise HistoricalDataUnavailableError(
            data_type="volume",
            pool_address="0x1234...",
            timestamp=datetime.now(),
            message="Historical volume unavailable for pool",
        )
"""

from datetime import datetime


class BacktestError(Exception):
    """Base exception for backtesting errors."""

    pass


class HistoricalDataUnavailableError(BacktestError):
    """Raised when historical data is unavailable in strict mode.

    This exception is raised by adapters when BacktestDataConfig.strict_historical_mode
    is True and required historical data cannot be fetched. In non-strict mode,
    adapters fall back to heuristic values with warnings instead.

    Attributes:
        data_type: Type of data that was unavailable (e.g., "volume", "funding", "apy", "liquidity")
        identifier: Resource identifier (e.g., pool address, market, token symbol)
        timestamp: The timestamp for which data was requested
        message: Human-readable error message
        chain: Optional chain identifier
        protocol: Optional protocol identifier

    Example:
        raise HistoricalDataUnavailableError(
            data_type="volume",
            identifier="0x88e6A0c2dDD26FEEb64F039a2c41296FcB3f5640",
            timestamp=datetime(2024, 1, 15, 12, 0, 0),
            message="No volume data available from Uniswap V3 subgraph for USDC/ETH pool",
            chain="ethereum",
            protocol="uniswap_v3",
        )
    """

    def __init__(
        self,
        data_type: str,
        identifier: str,
        timestamp: datetime,
        message: str,
        chain: str | None = None,
        protocol: str | None = None,
    ) -> None:
        """Initialize the exception.

        Args:
            data_type: Type of data that was unavailable
            identifier: Resource identifier (pool address, market, token symbol)
            timestamp: The timestamp for which data was requested
            message: Human-readable error message
            chain: Optional chain identifier
            protocol: Optional protocol identifier
        """
        self.data_type = data_type
        self.identifier = identifier
        self.timestamp = timestamp
        self.chain = chain
        self.protocol = protocol

        # Build detailed message
        parts = [message]
        parts.append(f"Data type: {data_type}")
        parts.append(f"Identifier: {identifier}")
        parts.append(f"Timestamp: {timestamp.isoformat()}")
        if chain:
            parts.append(f"Chain: {chain}")
        if protocol:
            parts.append(f"Protocol: {protocol}")

        full_message = " | ".join(parts)
        super().__init__(full_message)
        self.message = message

    def __repr__(self) -> str:
        """Return a detailed representation of the exception."""
        return (
            f"HistoricalDataUnavailableError("
            f"data_type={self.data_type!r}, "
            f"identifier={self.identifier!r}, "
            f"timestamp={self.timestamp!r}, "
            f"chain={self.chain!r}, "
            f"protocol={self.protocol!r})"
        )


__all__ = [
    "BacktestError",
    "HistoricalDataUnavailableError",
]
