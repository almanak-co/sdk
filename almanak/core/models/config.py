"""Configuration models for the Almanak SDK.

These models were previously in almanak.strategy.models and have been moved
to core for use across the SDK.
"""

from enum import StrEnum

from pydantic import BaseModel, Field


class GasConfig(BaseModel):
    """Configuration for gas price validation and retry logic."""

    allow_retry_time: int = Field(
        default=0,
        description="Time window in granularity units to retry when gas is too high",
    )
    buffer: float = Field(
        default=0.0,
        description="Buffer multiplier above median gas price (e.g., 0.2 = 20% above median)",
    )
    lookback: int = Field(
        default=0,
        description="Lookback period in granularity units for calculating median gas",
    )
    max_gas_price: int | None = Field(
        default=None,
        description="Maximum gas price ceiling in wei",
    )
    allow_transaction_after_retries: bool = Field(
        default=True,
        description="Whether to allow transaction after retry window expires",
    )
    exceptions: list[str] | None = Field(
        default=None,
        description="List of flow contexts that bypass gas validation",
    )
    auto_allow_below_price: int | None = Field(
        default=None,
        description="Auto-allow transactions below this gas price in wei",
    )


class VaultVersion(StrEnum):
    """Supported vault contract versions."""

    V0_0_0 = "0.0.0"
    V0_1_0 = "0.1.0"
    V0_2_0 = "0.2.0"
    V0_3_0 = "0.3.0"
    # V0_4_0 was skipped: the on-chain contracts jumped from 0.3.0 to 0.5.0
    V0_5_0 = "0.5.0"
    V1_0_0 = "1.0.0"


class Token(BaseModel):
    """Represents a token."""

    address: str
    symbol: str
    decimals: int
    name: str | None = None
    chain_id: int | None = None
    chain: str | None = None  # Chain name for CoinGecko API compatibility


class Pool(BaseModel):
    """Represents a liquidity pool."""

    address: str
    token0: Token
    token1: Token
    fee: int | None = None
    protocol: str | None = None
    chain: str | None = None  # Chain name for CoinGecko API compatibility


def convert_time_window(
    value: int | float,
    from_granularity: str,
    to_granularity: str,
) -> int | float:
    """Convert a time value from one granularity to another.

    Args:
        value: The time value to convert
        from_granularity: Source granularity (e.g., "1h", "15m", "1d")
        to_granularity: Target granularity (e.g., "1h", "15m", "1s")

    Returns:
        Converted time value in target granularity
    """
    # Granularity to seconds mapping
    granularity_seconds = {
        "1s": 1,
        "1m": 60,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "2h": 7200,
        "4h": 14400,
        "6h": 21600,
        "12h": 43200,
        "1d": 86400,
        "1w": 604800,
    }

    from_seconds = granularity_seconds.get(from_granularity)
    to_seconds = granularity_seconds.get(to_granularity)

    if from_seconds is None:
        raise ValueError(f"Unknown granularity: {from_granularity}")
    if to_seconds is None:
        raise ValueError(f"Unknown granularity: {to_granularity}")

    # Convert: value * from_seconds / to_seconds
    return value * from_seconds / to_seconds
