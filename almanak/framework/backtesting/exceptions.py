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


class NoAcceptableDataSourceError(BacktestError):
    """Raised when a required data source is unavailable and no safe fallback exists.

    Unlike :class:`HistoricalDataUnavailableError` (which signals a missing
    *historical* data point at a specific timestamp), this exception signals that
    the adapter has *no acceptable way* to obtain a number it needs and refuses to
    fabricate one. It is raised when:

    - historical volume could not be fetched (gateway DEX-volume lane
      unreachable / lookup failed / ``use_historical_volume=False``), AND
    - the caller did not provide explicit inputs (pool liquidity + volume), AND
    - the caller did not explicitly opt in to the heuristic fallback.

    The message must tell the user exactly what to provide to proceed. We fail loud
    here rather than silently substitute a fabricated number, because a wrong
    financial estimate is worse than a clear error.

    Attributes:
        data_type: Type of data that was unavailable (e.g., "volume").
        identifier: Resource identifier (e.g., pool address, position id).
        remediation: Concrete, actionable guidance on how to make the call succeed.

    Example:
        raise NoAcceptableDataSourceError(
            data_type="volume",
            identifier="WETH/USDC",
            remediation=(
                "Provide one of: use_historical_volume=True with a pool "
                "address on the position and a reachable gateway DEX-volume "
                "lane; an explicit volume_provider; or set "
                "allow_volume_fallback=True to accept the rough heuristic."
            ),
        )
    """

    def __init__(
        self,
        data_type: str,
        identifier: str,
        remediation: str,
        message: str | None = None,
    ) -> None:
        """Initialize the exception.

        Args:
            data_type: Type of data that was unavailable.
            identifier: Resource identifier (pool address, position id, token pair).
            remediation: Concrete guidance on how to make the call succeed.
            message: Optional override for the leading message. Defaults to a
                standard "no acceptable data source" sentence.
        """
        self.data_type = data_type
        self.identifier = identifier
        self.remediation = remediation
        lead = message or (
            f"No acceptable {data_type} data source for '{identifier}' and refusing to fabricate a value"
        )
        self.message = lead
        full_message = f"{lead}. To proceed: {remediation}"
        super().__init__(full_message)

    def __repr__(self) -> str:
        """Return a detailed representation of the exception."""
        return (
            f"NoAcceptableDataSourceError("
            f"data_type={self.data_type!r}, "
            f"identifier={self.identifier!r}, "
            f"remediation={self.remediation!r})"
        )


# Deprecated alias, remove after one release.
DataSourceUnavailableError = NoAcceptableDataSourceError


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


class UnsupportedIntentError(BacktestError):
    """Raised when a strategy emits an intent type the PnL engine cannot simulate.

    The engine refuses to run past an unsupported intent. Recording it as a
    costed no-op (the pre-2026-07 behaviour) silently diverged the backtest
    from live behaviour: the trade record existed, execution costs were
    charged, but no token moved and no position changed. A backtest that
    quietly skips part of the strategy certifies numbers the strategy never
    earned, so unsupported intents are a fatal, run-stopping error — never a
    warning.

    Attributes:
        intent_label: Human-readable description of the offending intent
            (declared intent type plus the intent's class name).
    """

    def __init__(
        self,
        intent_label: str,
        supported: tuple[str, ...],
        hint: str | None = None,
    ) -> None:
        """Initialize the exception.

        Args:
            intent_label: Description of the offending intent.
            supported: The intent types the engine simulates end to end.
            hint: Optional extra remediation detail (e.g. the multi-intent case).
        """
        self.intent_label = intent_label
        message = (
            f"Unsupported intent in PnL backtest: {intent_label}. "
            f"The engine simulates: {', '.join(supported)}. "
            "Refusing to continue — a silently skipped intent would diverge the "
            "backtest from live behaviour. Remove the intent from the strategy "
            "or validate this path on the paper trader instead."
        )
        if hint:
            message = f"{message} {hint}"
        super().__init__(message)

    def __repr__(self) -> str:
        """Return a detailed representation of the exception."""
        return f"UnsupportedIntentError(intent_label={self.intent_label!r})"


__all__ = [
    "BacktestError",
    "NoAcceptableDataSourceError",
    "DataSourceUnavailableError",
    "HistoricalDataUnavailableError",
    "UnsupportedIntentError",
]
