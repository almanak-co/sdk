"""Multi-chain market snapshot and related types.

This module contains classes for multi-chain strategy data access including
MultiChainMarketSnapshot, chain health tracking, error types, and provider
type aliases.

These were extracted from intent_strategy.py for maintainability. All symbols
remain importable from almanak.framework.strategies.intent_strategy.
"""

import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Literal

from .strategy_models import TokenBalance

logger = logging.getLogger(__name__)

_DSU_BOILERPLATE_RE = re.compile(r"data source '[^']*' unavailable:\s*")


class ChainNotConfiguredError(Exception):
    """Raised when accessing data for a chain not configured for the strategy.

    Attributes:
        chain: The chain that was requested
        configured_chains: List of chains that are configured
    """

    def __init__(self, chain: str, configured_chains: list[str]) -> None:
        self.chain = chain
        self.configured_chains = configured_chains
        super().__init__(f"Chain '{chain}' is not configured for this strategy. Configured chains: {configured_chains}")


# =============================================================================
# Multi-Chain Market Snapshot Types
# =============================================================================

# Type for chain-aware price oracle function
# (token, quote, chain) -> price
MultiChainPriceOracle = Callable[[str, str, str], Decimal]

# Type for chain-aware balance provider function
# (token, chain) -> TokenBalance
MultiChainBalanceProvider = Callable[[str, str], TokenBalance]


# =============================================================================
# Chain Health Status
# =============================================================================


class ChainHealthStatus(Enum):
    """Status of a chain's data health.

    Attributes:
        HEALTHY: Chain data is fresh and available
        DEGRADED: Chain data is stale but still usable (between threshold and 2x threshold)
        UNAVAILABLE: Chain data could not be fetched
        STALE: Chain data is too old to be trusted (beyond staleness threshold)
    """

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"
    STALE = "stale"


@dataclass
class ChainHealth:
    """Health status and staleness information for a single chain.

    This dataclass provides detailed information about the health of market data
    for a specific chain, including when data was last fetched, staleness metrics,
    and any error information.

    Attributes:
        chain: Chain name (e.g., "arbitrum", "optimism")
        status: Current health status of the chain
        last_updated: When the chain's data was last successfully fetched
        staleness_seconds: How old the data is in seconds (None if unavailable)
        stale_threshold_seconds: The threshold used to determine staleness
        error: Error message if data fetch failed
        is_stale: Whether the data is considered stale
        is_available: Whether the data is available for use

    Example:
        health = ChainHealth(
            chain="arbitrum",
            status=ChainHealthStatus.HEALTHY,
            last_updated=datetime.now(timezone.utc),
            staleness_seconds=5.2,
            stale_threshold_seconds=30.0,
        )

        if health.is_stale:
            logger.warning(f"Chain {health.chain} data is stale")
    """

    chain: str
    status: ChainHealthStatus
    last_updated: datetime | None = None
    staleness_seconds: float | None = None
    stale_threshold_seconds: float = 30.0
    error: str | None = None

    @property
    def is_stale(self) -> bool:
        """Check if the chain data is stale.

        Returns:
            True if staleness exceeds threshold or data is unavailable
        """
        if self.status == ChainHealthStatus.UNAVAILABLE:
            return True
        if self.status == ChainHealthStatus.STALE:
            return True
        if self.staleness_seconds is not None:
            return self.staleness_seconds > self.stale_threshold_seconds
        return False

    @property
    def is_available(self) -> bool:
        """Check if the chain data is available for use.

        Returns:
            True if data is healthy or degraded (but still usable)
        """
        return self.status in (ChainHealthStatus.HEALTHY, ChainHealthStatus.DEGRADED)

    @property
    def is_healthy(self) -> bool:
        """Check if the chain data is fully healthy.

        Returns:
            True if status is HEALTHY
        """
        return self.status == ChainHealthStatus.HEALTHY

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary representation.

        Returns:
            Dictionary with health information
        """
        return {
            "chain": self.chain,
            "status": self.status.value,
            "last_updated": self.last_updated.isoformat() if self.last_updated else None,
            "staleness_seconds": self.staleness_seconds,
            "stale_threshold_seconds": self.stale_threshold_seconds,
            "error": self.error,
            "is_stale": self.is_stale,
            "is_available": self.is_available,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ChainHealth":
        """Create ChainHealth from dictionary.

        Args:
            data: Dictionary with health information

        Returns:
            ChainHealth instance
        """
        last_updated = None
        if data.get("last_updated"):
            last_updated = datetime.fromisoformat(data["last_updated"])

        return cls(
            chain=data["chain"],
            status=ChainHealthStatus(data["status"]),
            last_updated=last_updated,
            staleness_seconds=data.get("staleness_seconds"),
            stale_threshold_seconds=data.get("stale_threshold_seconds", 30.0),
            error=data.get("error"),
        )


class StaleDataError(Exception):
    """Raised when market data is stale and fail_closed policy is active.

    This exception is raised during snapshot validation when one or more chains
    have stale or unavailable data and the data freshness policy is set to
    'fail_closed'.

    Attributes:
        stale_chains: List of chains with stale data
        chain_health: Dictionary mapping chain names to their health status
    """

    def __init__(
        self,
        stale_chains: list[str],
        chain_health: dict[str, "ChainHealth"],
    ) -> None:
        self.stale_chains = stale_chains
        self.chain_health = chain_health

        chain_details = []
        for chain in stale_chains:
            health = chain_health.get(chain)
            if health:
                if health.error:
                    chain_details.append(f"{chain}: {health.error}")
                elif health.staleness_seconds is not None:
                    chain_details.append(
                        f"{chain}: {health.staleness_seconds:.1f}s old (threshold: {health.stale_threshold_seconds}s)"
                    )
                else:
                    chain_details.append(f"{chain}: unavailable")
            else:
                chain_details.append(f"{chain}: unknown")

        super().__init__(f"Data is stale for chains: {', '.join(stale_chains)}. Details: {'; '.join(chain_details)}")


# Type alias for data freshness policy
DataFreshnessPolicy = Literal["fail_closed", "fail_open"]


# Type for Aave health factor provider function
# (chain) -> health_factor or None if no position
AaveHealthFactorProvider = Callable[[str], Decimal | None]

# Type for Aave available borrow provider function
# (token, chain) -> available_borrow_amount or None
AaveAvailableBorrowProvider = Callable[[str, str], Decimal | None]

# Type for GMX available liquidity provider function
# (market, chain) -> available_liquidity or None
GmxAvailableLiquidityProvider = Callable[[str, str], Decimal | None]

# Type for GMX funding rate provider function
# (market, chain) -> funding_rate or None
GmxFundingRateProvider = Callable[[str, str], Decimal | None]


# =============================================================================
# Multi-Chain Market Snapshot
# =============================================================================


# =============================================================================
# VIB-4062 — Multi-Chain Market Snapshot
#
# The standalone ``MultiChainMarketSnapshot`` class is removed. The canonical
# ``MarketSnapshot`` class supports multi-chain via the keyword-only ``chain=``
# argument and the ``chains`` property (PRD §4.2). ``MultiChainMarketSnapshot``
# remains importable as a TypeAlias for backward compat with tests / strategies
# that prefer the explicit name.
# =============================================================================
from ..market import MultiChainMarketSnapshot  # noqa: F401
