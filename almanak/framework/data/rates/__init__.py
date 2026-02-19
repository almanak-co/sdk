"""Lending Rate Monitor Module.

This module provides real-time lending rate monitoring across multiple DeFi protocols
including Aave V3, Morpho Blue, and Compound V3, plus historical rate data for
backtesting rate arbitrage and carry strategies.

Key Features:
    - Fetch supply/borrow APY from multiple lending protocols
    - Cross-protocol rate comparison
    - Historical lending and funding rate data (The Graph, DeFi Llama, Hyperliquid)
    - Configurable refresh intervals
    - Caching to minimize RPC calls

Example:
    from almanak.framework.data.rates import RateMonitor, LendingRate, RateSide

    # Create rate monitor
    monitor = RateMonitor(chain="ethereum")

    # Get rate for specific protocol
    rate = await monitor.get_lending_rate(
        protocol="aave_v3",
        token="USDC",
        side=RateSide.SUPPLY
    )
    print(f"Aave USDC Supply APY: {rate.apy_percent:.2f}%")

    # Get best rate across protocols
    best = await monitor.get_best_lending_rate(
        token="USDC",
        side=RateSide.SUPPLY
    )
    print(f"Best rate: {best.protocol} at {best.apy_percent:.2f}%")

    # Historical lending rates
    from almanak.framework.data.rates import RateHistoryReader
    reader = RateHistoryReader()
    envelope = reader.get_lending_rate_history("aave_v3", "USDC", "arbitrum", days=90)
"""

from .history import (
    FundingRateSnapshot,
    LendingRateSnapshot,
    RateHistoryReader,
)
from .monitor import (
    PROTOCOL_CHAINS,
    # Constants
    SUPPORTED_PROTOCOLS,
    SUPPORTED_TOKENS,
    BestRateResult,
    # Data classes
    LendingRate,
    LendingRateResult,
    Protocol,
    ProtocolNotSupportedError,
    ProtocolRates,
    # Main service
    RateMonitor,
    # Exceptions
    RateMonitorError,
    # Enums
    RateSide,
    RateUnavailableError,
    TokenNotSupportedError,
)

__all__ = [
    # Main service
    "RateMonitor",
    # History reader
    "RateHistoryReader",
    # Data classes
    "LendingRate",
    "LendingRateResult",
    "BestRateResult",
    "ProtocolRates",
    "LendingRateSnapshot",
    "FundingRateSnapshot",
    # Enums
    "RateSide",
    "Protocol",
    # Exceptions
    "RateMonitorError",
    "RateUnavailableError",
    "ProtocolNotSupportedError",
    "TokenNotSupportedError",
    # Constants
    "SUPPORTED_PROTOCOLS",
    "PROTOCOL_CHAINS",
    "SUPPORTED_TOKENS",
]
