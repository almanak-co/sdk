"""Multi-chain market snapshot and related types.

This module contains classes for multi-chain strategy data access including
MultiChainMarketSnapshot, chain health tracking, error types, and provider
type aliases.

These were extracted from intent_strategy.py for maintainability. All symbols
remain importable from almanak.framework.strategies.intent_strategy.
"""

import concurrent.futures
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Literal

from .strategy_models import PriceData, TokenBalance

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


class MultiChainMarketSnapshot:
    """Multi-chain market data access for cross-chain strategy decisions.

    MultiChainMarketSnapshot extends MarketSnapshot to provide data from multiple
    blockchain networks for cross-chain strategy decision-making. It supports:

    - Chain-specific prices via market.price('ETH', chain='arbitrum')
    - Chain-specific balances via market.balance('USDC', chain='optimism')
    - Cross-chain portfolio aggregation via market.total_portfolio_usd()
    - Parallel data fetching from all chains for performance

    The snapshot validates that all chain requests are for configured chains,
    raising ChainNotConfiguredError for invalid chain requests.

    Example:
        def decide(self, market: MultiChainMarketSnapshot) -> Optional[Intent]:
            # Get chain-specific prices
            arb_eth_price = market.price("ETH", chain="arbitrum")
            opt_eth_price = market.price("ETH", chain="optimism")

            # Get chain-specific balances
            usdc_on_optimism = market.balance("USDC", chain="optimism")

            # Get all configured chains
            for chain in market.chains:
                print(f"Balance on {chain}: {market.balance('ETH', chain=chain)}")

            # Aggregate portfolio value
            total_value = market.total_portfolio_usd()

            return Intent.hold()
    """

    # Default timeout for parallel fetching (2 seconds as per spec)
    DEFAULT_FETCH_TIMEOUT: float = 2.0

    def __init__(
        self,
        chains: list[str],
        wallet_address: str,
        price_oracle: MultiChainPriceOracle | None = None,
        balance_provider: MultiChainBalanceProvider | None = None,
        timestamp: datetime | None = None,
        fetch_timeout: float | None = None,
        # Protocol health metric providers
        aave_health_factor_provider: AaveHealthFactorProvider | None = None,
        aave_available_borrow_provider: AaveAvailableBorrowProvider | None = None,
        gmx_available_liquidity_provider: GmxAvailableLiquidityProvider | None = None,
        gmx_funding_rate_provider: GmxFundingRateProvider | None = None,
        # Data freshness settings
        data_freshness_policy: DataFreshnessPolicy = "fail_closed",
        stale_data_threshold_seconds: float = 30.0,
    ) -> None:
        """Initialize multi-chain market snapshot.

        Args:
            chains: List of configured chain names (e.g., ["arbitrum", "optimism", "base"])
            wallet_address: Wallet address for balance queries (same across EVM chains)
            price_oracle: Function to fetch prices (token, quote, chain) -> price
            balance_provider: Function to fetch balances (token, chain) -> TokenBalance
            timestamp: Snapshot timestamp (defaults to now)
            fetch_timeout: Timeout for parallel fetching in seconds (default 2.0)
            aave_health_factor_provider: Function to fetch Aave health factor (chain) -> factor
            aave_available_borrow_provider: Function to fetch Aave available borrow (token, chain) -> amount
            gmx_available_liquidity_provider: Function to fetch GMX liquidity (market, chain) -> amount
            gmx_funding_rate_provider: Function to fetch GMX funding rate (market, chain) -> rate
            data_freshness_policy: How to handle stale data - 'fail_closed' (default) errors on stale,
                'fail_open' excludes stale chains and continues
            stale_data_threshold_seconds: Data older than this is considered stale (default 30s)
        """
        if not chains:
            raise ValueError("At least one chain must be configured")

        self._chains = [c.lower() for c in chains]
        self._wallet_address = wallet_address
        self._price_oracle = price_oracle
        self._balance_provider = balance_provider
        self._timestamp = timestamp or datetime.now(UTC)
        self._fetch_timeout = fetch_timeout or self.DEFAULT_FETCH_TIMEOUT

        # Data freshness settings
        self._data_freshness_policy: DataFreshnessPolicy = data_freshness_policy
        self._stale_data_threshold_seconds = stale_data_threshold_seconds

        # Protocol health metric providers
        self._aave_health_factor_provider = aave_health_factor_provider
        self._aave_available_borrow_provider = aave_available_borrow_provider
        self._gmx_available_liquidity_provider = gmx_available_liquidity_provider
        self._gmx_funding_rate_provider = gmx_funding_rate_provider

        # Per-chain caches: {chain: {token: data}}
        self._price_cache: dict[str, dict[str, PriceData]] = {c: {} for c in self._chains}
        self._balance_cache: dict[str, dict[str, TokenBalance]] = {c: {} for c in self._chains}

        # Critical data failures observed while strategies queried this snapshot.
        # Mirrors the same field on MarketSnapshot so the runner can call the
        # has/classify/summarize API uniformly across single- and multi-chain snapshots.
        self._critical_data_failures: dict[tuple[str, str], str] = {}

        # Per-chain protocol metrics caches: {chain: data}
        self._aave_health_factor_cache: dict[str, Decimal | None] = {}
        self._aave_available_borrow_cache: dict[str, dict[str, Decimal | None]] = {c: {} for c in self._chains}
        self._gmx_available_liquidity_cache: dict[str, dict[str, Decimal | None]] = {c: {} for c in self._chains}
        self._gmx_funding_rate_cache: dict[str, dict[str, Decimal | None]] = {c: {} for c in self._chains}

        # Pre-populated data (can be set directly): {chain: {token: data}}
        self._prices: dict[str, dict[str, Decimal]] = {c: {} for c in self._chains}
        self._balances: dict[str, dict[str, TokenBalance]] = {c: {} for c in self._chains}

        # Chain health tracking: {chain: ChainHealth}
        self._chain_health: dict[str, ChainHealth] = {}
        self._chain_last_updated: dict[str, datetime] = {}

        # Initialize chain health for all chains as unknown
        for chain in self._chains:
            self._chain_health[chain] = ChainHealth(
                chain=chain,
                status=ChainHealthStatus.HEALTHY,  # Start healthy, update on fetch
                stale_threshold_seconds=stale_data_threshold_seconds,
            )

    @property
    def chains(self) -> list[str]:
        """Get the list of configured chains.

        Returns:
            List of chain names configured for this snapshot
        """
        return list(self._chains)

    @property
    def wallet_address(self) -> str:
        """Get the wallet address."""
        return self._wallet_address

    @property
    def timestamp(self) -> datetime:
        """Get the snapshot timestamp."""
        return self._timestamp

    @property
    def chain_health(self) -> dict[str, ChainHealth]:
        """Get health status for all configured chains.

        Returns:
            Dictionary mapping chain names to their ChainHealth status.
            Use this to check staleness and availability before making decisions.

        Example:
            health = market.chain_health
            for chain, status in health.items():
                if status.is_stale:
                    logger.warning(f"Chain {chain} has stale data")
        """
        return dict(self._chain_health)

    @property
    def data_freshness_policy(self) -> DataFreshnessPolicy:
        """Get the data freshness policy for this snapshot.

        Returns:
            'fail_closed' or 'fail_open'
        """
        return self._data_freshness_policy

    @property
    def stale_data_threshold_seconds(self) -> float:
        """Get the staleness threshold in seconds.

        Returns:
            Number of seconds after which data is considered stale
        """
        return self._stale_data_threshold_seconds

    @property
    def healthy_chains(self) -> list[str]:
        """Get list of chains with healthy (non-stale) data.

        This is useful when using fail_open policy to know which chains
        have usable data.

        Returns:
            List of chain names with healthy data
        """
        return [chain for chain, health in self._chain_health.items() if health.is_available]

    @property
    def stale_chains(self) -> list[str]:
        """Get list of chains with stale or unavailable data.

        Returns:
            List of chain names with stale data
        """
        return [chain for chain, health in self._chain_health.items() if health.is_stale]

    @property
    def all_chains_healthy(self) -> bool:
        """Check if all chains have healthy data.

        Returns:
            True if no chains have stale data
        """
        return len(self.stale_chains) == 0

    def _validate_chain(self, chain: str) -> str:
        """Validate that a chain is configured and return normalized name.

        Args:
            chain: Chain name to validate

        Returns:
            Normalized (lowercase) chain name

        Raises:
            ChainNotConfiguredError: If chain is not configured
        """
        chain_lower = chain.lower()
        if chain_lower not in self._chains:
            raise ChainNotConfiguredError(chain, self._chains)
        return chain_lower

    def _update_chain_health(
        self,
        chain: str,
        success: bool,
        error: str | None = None,
    ) -> None:
        """Update health status for a chain after a data fetch.

        This method should be called after fetching data from a chain to
        update the chain's health status and last updated timestamp.

        Args:
            chain: Chain name
            success: Whether the fetch was successful
            error: Error message if fetch failed
        """
        chain_lower = chain.lower()
        now = datetime.now(UTC)

        if success:
            self._chain_last_updated[chain_lower] = now
            staleness = 0.0

            # Determine status based on staleness
            status = ChainHealthStatus.HEALTHY

            self._chain_health[chain_lower] = ChainHealth(
                chain=chain_lower,
                status=status,
                last_updated=now,
                staleness_seconds=staleness,
                stale_threshold_seconds=self._stale_data_threshold_seconds,
                error=None,
            )
        else:
            # Fetch failed - mark as unavailable
            self._chain_health[chain_lower] = ChainHealth(
                chain=chain_lower,
                status=ChainHealthStatus.UNAVAILABLE,
                last_updated=self._chain_last_updated.get(chain_lower),
                staleness_seconds=None,
                stale_threshold_seconds=self._stale_data_threshold_seconds,
                error=error,
            )
            logger.warning(f"Chain {chain_lower} data fetch failed: {error}")

    def _recalculate_chain_staleness(self) -> None:
        """Recalculate staleness for all chains based on current time.

        This method updates the staleness_seconds and status for each chain
        based on when data was last successfully fetched.
        """
        now = datetime.now(UTC)

        for chain in self._chains:
            if chain in self._chain_last_updated:
                last_updated = self._chain_last_updated[chain]
                staleness = (now - last_updated).total_seconds()

                # Determine status based on staleness
                if staleness <= self._stale_data_threshold_seconds:
                    status = ChainHealthStatus.HEALTHY
                elif staleness <= self._stale_data_threshold_seconds * 2:
                    status = ChainHealthStatus.DEGRADED
                    logger.warning(
                        f"Chain {chain} data is degraded "
                        f"({staleness:.1f}s old, threshold: {self._stale_data_threshold_seconds}s)"
                    )
                else:
                    status = ChainHealthStatus.STALE
                    logger.warning(
                        f"Chain {chain} data is stale "
                        f"({staleness:.1f}s old, threshold: {self._stale_data_threshold_seconds}s)"
                    )

                self._chain_health[chain] = ChainHealth(
                    chain=chain,
                    status=status,
                    last_updated=last_updated,
                    staleness_seconds=staleness,
                    stale_threshold_seconds=self._stale_data_threshold_seconds,
                    error=self._chain_health[chain].error if chain in self._chain_health else None,
                )

    def validate_freshness(self) -> None:
        """Validate data freshness according to the configured policy.

        This method should be called before using the snapshot for decisions.
        It recalculates staleness and applies the freshness policy:

        - fail_closed: Raises StaleDataError if ANY chain has stale data
        - fail_open: Logs warnings for stale chains but doesn't raise

        Raises:
            StaleDataError: If data is stale and policy is 'fail_closed'

        Example:
            try:
                market.validate_freshness()
                # Safe to proceed with decision
                intent = strategy.decide(market)
            except StaleDataError as e:
                logger.error(f"Cannot proceed: {e}")
                # Handle stale data situation
        """
        # Update staleness calculations
        self._recalculate_chain_staleness()

        stale = self.stale_chains

        if not stale:
            logger.debug("All chain data is fresh")
            return

        if self._data_freshness_policy == "fail_closed":
            # Log which chains are stale before raising
            for chain in stale:
                health = self._chain_health.get(chain)
                if health:
                    logger.error(
                        f"Chain {chain} data is stale/unavailable - "
                        f"status: {health.status.value}, "
                        f"staleness: {health.staleness_seconds}s, "
                        f"error: {health.error}"
                    )
            raise StaleDataError(stale, self._chain_health)

        else:  # fail_open
            # Log warnings but don't raise
            for chain in stale:
                health = self._chain_health.get(chain)
                if health:
                    logger.warning(
                        f"Chain {chain} excluded due to stale/unavailable data - "
                        f"status: {health.status.value}, "
                        f"staleness: {health.staleness_seconds}s, "
                        f"error: {health.error}"
                    )
            logger.info(
                f"Proceeding with fail_open policy. Healthy chains: {self.healthy_chains}, Stale chains: {stale}"
            )

    def set_chain_health(self, chain: str, health: ChainHealth) -> None:
        """Manually set health status for a chain.

        This is useful for pre-populating health information when creating
        snapshots programmatically.

        Args:
            chain: Chain name
            health: ChainHealth instance to set

        Raises:
            ChainNotConfiguredError: If chain is not configured
        """
        chain_lower = self._validate_chain(chain)
        self._chain_health[chain_lower] = health
        if health.last_updated:
            self._chain_last_updated[chain_lower] = health.last_updated

    def price(self, token: str, chain: str, quote: str = "USD") -> Decimal:
        """Get the price of a token on a specific chain.

        Args:
            token: Token symbol (e.g., "ETH", "WBTC")
            chain: Chain name (e.g., "arbitrum", "optimism")
            quote: Quote currency (default "USD")

        Returns:
            Token price in quote currency

        Raises:
            ChainNotConfiguredError: If chain is not configured
            ValueError: If price cannot be determined
        """
        chain_lower = self._validate_chain(chain)
        cache_key = f"{token}/{quote}"

        # Check pre-populated prices first
        if token in self._prices.get(chain_lower, {}):
            return self._prices[chain_lower][token]

        # Check cache
        chain_cache = self._price_cache.get(chain_lower, {})
        if cache_key in chain_cache:
            return chain_cache[cache_key].price

        # Use oracle if available
        if self._price_oracle:
            try:
                price_value = self._price_oracle(token, quote, chain_lower)
                # VIB-3889: stamp inferred source on the cached entry.
                from almanak.framework.strategies.intent_strategy import _infer_oracle_source

                self._price_cache[chain_lower][cache_key] = PriceData(
                    price=price_value, source=_infer_oracle_source(self._price_oracle)
                )
                self._critical_data_failures.pop(("price", f"{cache_key}@{chain_lower}"), None)
                return price_value
            except Exception as e:
                self._critical_data_failures.setdefault(("price", f"{cache_key}@{chain_lower}"), str(e))
                logger.warning(f"Price oracle failed for {token}/{quote} on {chain_lower}: {e}")

        self._critical_data_failures.setdefault(
            ("price", f"{cache_key}@{chain_lower}"),
            f"Cannot determine price for {token}/{quote} on {chain}",
        )
        raise ValueError(f"Cannot determine price for {token}/{quote} on {chain}")

    def balance(self, token: str, chain: str) -> TokenBalance:
        """Get wallet balance for a token on a specific chain.

        Args:
            token: Token symbol
            chain: Chain name (e.g., "arbitrum", "optimism")

        Returns:
            TokenBalance with current balance on the specified chain

        Raises:
            ChainNotConfiguredError: If chain is not configured
            ValueError: If balance cannot be determined
        """
        chain_lower = self._validate_chain(chain)

        # Check pre-populated balances first
        if token in self._balances.get(chain_lower, {}):
            return self._balances[chain_lower][token]

        # Check cache
        chain_cache = self._balance_cache.get(chain_lower, {})
        if token in chain_cache:
            return chain_cache[token]

        # Use provider if available
        if self._balance_provider:
            try:
                balance_data = self._balance_provider(token, chain_lower)
                self._balance_cache[chain_lower][token] = balance_data
                self._critical_data_failures.pop(("balance", f"{token}@{chain_lower}"), None)
                return balance_data
            except Exception as e:
                self._critical_data_failures.setdefault(("balance", f"{token}@{chain_lower}"), str(e))
                logger.warning(f"Balance provider failed for {token} on {chain_lower}: {e}")

        self._critical_data_failures.setdefault(
            ("balance", f"{token}@{chain_lower}"),
            f"Cannot determine balance for {token} on {chain}",
        )
        raise ValueError(f"Cannot determine balance for {token} on {chain}")

    def balance_usd(self, token: str, chain: str) -> Decimal:
        """Get wallet balance in USD terms for a token on a specific chain.

        Args:
            token: Token symbol
            chain: Chain name

        Returns:
            Balance in USD on the specified chain
        """
        return self.balance(token, chain).balance_usd

    def price_difference(
        self,
        token: str,
        chain_a: str,
        chain_b: str,
        quote: str = "USD",
    ) -> Decimal | None:
        """Calculate price difference (spread) between two chains.

        This method is useful for cross-chain arbitrage detection by comparing
        the price of a token on two different chains.

        Args:
            token: Token symbol (e.g., "ETH", "WBTC")
            chain_a: First chain name (e.g., "arbitrum")
            chain_b: Second chain name (e.g., "optimism")
            quote: Quote currency (default "USD")

        Returns:
            Price spread as a decimal representing the percentage difference.
            Positive value means chain_a price is higher than chain_b.
            For example: 0.005 means chain_a is 0.5% higher than chain_b.
            Returns None if price is unavailable on either chain.

        Raises:
            ChainNotConfiguredError: If either chain is not configured

        Example:
            # Check for arbitrage opportunity
            spread = market.price_difference("ETH", chain_a="arbitrum", chain_b="optimism")
            if spread is not None and spread > Decimal("0.005"):  # 0.5% spread
                # Arbitrage: buy on optimism, sell on arbitrum
                pass
        """
        # Validate both chains are configured (raises ChainNotConfiguredError if not)
        self._validate_chain(chain_a)
        self._validate_chain(chain_b)

        try:
            price_a = self.price(token, chain_a, quote)
        except ValueError:
            logger.debug(f"Price unavailable for {token} on {chain_a}")
            return None

        try:
            price_b = self.price(token, chain_b, quote)
        except ValueError:
            logger.debug(f"Price unavailable for {token} on {chain_b}")
            return None

        # Avoid division by zero
        if price_b == Decimal("0"):
            logger.warning(f"Price for {token} on {chain_b} is zero, cannot calculate spread")
            return None

        # Calculate spread: (price_a - price_b) / price_b
        # Positive means chain_a is more expensive
        spread = (price_a - price_b) / price_b
        return spread

    # =========================================================================
    # Protocol Health Metrics - Aave
    # =========================================================================

    def aave_health_factor(self, chain: str) -> Decimal | None:
        """Get Aave health factor for the wallet on a specific chain.

        The health factor represents the safety of the user's position in Aave.
        - Health factor > 1: Position is safe
        - Health factor <= 1: Position can be liquidated

        Args:
            chain: Chain name (e.g., "arbitrum", "optimism")

        Returns:
            Aave health factor as Decimal, or None if:
            - No Aave position exists on this chain
            - Health factor cannot be determined
            - Provider returned None (graceful handling)

        Raises:
            ChainNotConfiguredError: If chain is not configured

        Example:
            health = market.aave_health_factor(chain='arbitrum')
            if health is not None and health < Decimal('1.5'):
                # Position is getting risky, consider repaying
                pass
        """
        chain_lower = self._validate_chain(chain)

        # Check cache
        if chain_lower in self._aave_health_factor_cache:
            return self._aave_health_factor_cache[chain_lower]

        # Use provider if available
        if self._aave_health_factor_provider:
            try:
                health_factor = self._aave_health_factor_provider(chain_lower)
                self._aave_health_factor_cache[chain_lower] = health_factor
                return health_factor
            except Exception as e:
                logger.debug(f"Aave health factor provider failed for {chain_lower}: {e}")
                # Cache None to avoid repeated failed calls
                self._aave_health_factor_cache[chain_lower] = None
                return None

        # No provider - return None (missing metrics return None, not error)
        return None

    def aave_available_borrow(self, token: str, chain: str) -> Decimal | None:
        """Get maximum available borrow amount for a token on Aave.

        This returns the maximum amount of the specified token that can be
        borrowed based on the user's collateral and current borrow utilization.

        Args:
            token: Token symbol to check borrow capacity for (e.g., "USDC", "ETH")
            chain: Chain name (e.g., "arbitrum", "optimism")

        Returns:
            Maximum borrowable amount as Decimal, or None if:
            - No Aave position exists on this chain
            - Token is not available for borrowing
            - Available borrow cannot be determined
            - Provider returned None (graceful handling)

        Raises:
            ChainNotConfiguredError: If chain is not configured

        Example:
            available = market.aave_available_borrow(token='USDC', chain='arbitrum')
            if available is not None and available > Decimal('1000'):
                # Can borrow more USDC
                return Intent.borrow(token='USDC', amount=Decimal('500'), chain='arbitrum')
        """
        chain_lower = self._validate_chain(chain)
        cache_key = token.upper()

        # Check cache
        chain_cache = self._aave_available_borrow_cache.get(chain_lower, {})
        if cache_key in chain_cache:
            return chain_cache[cache_key]

        # Use provider if available
        if self._aave_available_borrow_provider:
            try:
                available = self._aave_available_borrow_provider(token, chain_lower)
                self._aave_available_borrow_cache[chain_lower][cache_key] = available
                return available
            except Exception as e:
                logger.debug(f"Aave available borrow provider failed for {token} on {chain_lower}: {e}")
                # Cache None to avoid repeated failed calls
                self._aave_available_borrow_cache[chain_lower][cache_key] = None
                return None

        # No provider - return None (missing metrics return None, not error)
        return None

    # =========================================================================
    # Protocol Health Metrics - GMX
    # =========================================================================

    def gmx_available_liquidity(self, market: str, chain: str) -> Decimal | None:
        """Get available liquidity for a GMX market.

        This returns the available liquidity in the GMX market pool that can
        be used for opening new positions. Important for determining if a
        position size can be supported.

        Args:
            market: Market identifier (e.g., "ETH/USD", "BTC/USD")
            chain: Chain name (e.g., "arbitrum")

        Returns:
            Available liquidity in USD as Decimal, or None if:
            - Market does not exist on this chain
            - Liquidity cannot be determined
            - Provider returned None (graceful handling)

        Raises:
            ChainNotConfiguredError: If chain is not configured

        Example:
            liquidity = market.gmx_available_liquidity(market='ETH/USD', chain='arbitrum')
            if liquidity is not None and liquidity > Decimal('100000'):
                # Sufficient liquidity for position
                return Intent.perp_open(
                    market='ETH/USD',
                    direction='long',
                    size_usd=Decimal('10000'),
                    chain='arbitrum'
                )
        """
        chain_lower = self._validate_chain(chain)
        cache_key = market.upper()

        # Check cache
        chain_cache = self._gmx_available_liquidity_cache.get(chain_lower, {})
        if cache_key in chain_cache:
            return chain_cache[cache_key]

        # Use provider if available
        if self._gmx_available_liquidity_provider:
            try:
                liquidity = self._gmx_available_liquidity_provider(market, chain_lower)
                self._gmx_available_liquidity_cache[chain_lower][cache_key] = liquidity
                return liquidity
            except Exception as e:
                logger.debug(f"GMX available liquidity provider failed for {market} on {chain_lower}: {e}")
                # Cache None to avoid repeated failed calls
                self._gmx_available_liquidity_cache[chain_lower][cache_key] = None
                return None

        # No provider - return None (missing metrics return None, not error)
        return None

    def gmx_funding_rate(self, market: str, chain: str) -> Decimal | None:
        """Get current funding rate for a GMX market.

        The funding rate is the periodic payment between long and short
        positions. Positive rate means longs pay shorts, negative means
        shorts pay longs.

        Args:
            market: Market identifier (e.g., "ETH/USD", "BTC/USD")
            chain: Chain name (e.g., "arbitrum")

        Returns:
            Current funding rate as Decimal (per hour), or None if:
            - Market does not exist on this chain
            - Funding rate cannot be determined
            - Provider returned None (graceful handling)

            The rate is expressed as a decimal per hour.
            For example, 0.0001 means 0.01% per hour.

        Raises:
            ChainNotConfiguredError: If chain is not configured

        Example:
            rate = market.gmx_funding_rate(market='ETH/USD', chain='arbitrum')
            if rate is not None and rate > Decimal('0.0005'):
                # High positive funding - expensive to hold longs
                pass
        """
        chain_lower = self._validate_chain(chain)
        cache_key = market.upper()

        # Check cache
        chain_cache = self._gmx_funding_rate_cache.get(chain_lower, {})
        if cache_key in chain_cache:
            return chain_cache[cache_key]

        # Use provider if available
        if self._gmx_funding_rate_provider:
            try:
                rate = self._gmx_funding_rate_provider(market, chain_lower)
                self._gmx_funding_rate_cache[chain_lower][cache_key] = rate
                return rate
            except Exception as e:
                logger.debug(f"GMX funding rate provider failed for {market} on {chain_lower}: {e}")
                # Cache None to avoid repeated failed calls
                self._gmx_funding_rate_cache[chain_lower][cache_key] = None
                return None

        # No provider - return None (missing metrics return None, not error)
        return None

    # =========================================================================
    # Protocol Metrics - Parallel Fetching
    # =========================================================================

    def fetch_all_protocol_metrics_parallel(
        self,
        aave_chains: list[str] | None = None,
        aave_borrow_tokens: list[str] | None = None,
        gmx_markets: list[str] | None = None,
        gmx_chains: list[str] | None = None,
    ) -> dict[str, Any]:
        """Fetch protocol health metrics in parallel with other market data.

        This method fetches Aave and GMX metrics concurrently from all specified
        chains within the configured timeout (default 2 seconds).

        Args:
            aave_chains: Chains to fetch Aave health factors from (default: all chains)
            aave_borrow_tokens: Tokens to fetch Aave available borrow for (default: none)
            gmx_markets: GMX markets to fetch liquidity/funding for (default: none)
            gmx_chains: Chains to fetch GMX metrics from (default: all chains)

        Returns:
            Dictionary with fetched metrics:
            {
                'aave_health_factors': {chain: factor_or_none},
                'aave_available_borrow': {chain: {token: amount_or_none}},
                'gmx_available_liquidity': {chain: {market: amount_or_none}},
                'gmx_funding_rates': {chain: {market: rate_or_none}}
            }

        Example:
            # Fetch all Aave and GMX metrics in parallel
            metrics = market.fetch_all_protocol_metrics_parallel(
                aave_chains=['arbitrum', 'optimism'],
                aave_borrow_tokens=['USDC', 'ETH'],
                gmx_markets=['ETH/USD', 'BTC/USD'],
                gmx_chains=['arbitrum']
            )
        """
        # Default to all chains if not specified
        aave_chains_to_fetch = [c.lower() for c in (aave_chains or self._chains)]
        gmx_chains_to_fetch = [c.lower() for c in (gmx_chains or self._chains)]
        aave_tokens = aave_borrow_tokens or []
        gmx_mkts = gmx_markets or []

        results: dict[str, Any] = {
            "aave_health_factors": {},
            "aave_available_borrow": {c: {} for c in aave_chains_to_fetch},
            "gmx_available_liquidity": {c: {} for c in gmx_chains_to_fetch},
            "gmx_funding_rates": {c: {} for c in gmx_chains_to_fetch},
        }

        # Build task list
        tasks: list[tuple[str, str, str, str]] = []  # (type, chain, key, unused)

        # Aave health factor tasks
        if self._aave_health_factor_provider:
            for chain in aave_chains_to_fetch:
                if chain in self._chains:
                    tasks.append(("aave_health", chain, "", ""))

        # Aave available borrow tasks
        if self._aave_available_borrow_provider:
            for chain in aave_chains_to_fetch:
                if chain in self._chains:
                    for token in aave_tokens:
                        tasks.append(("aave_borrow", chain, token, ""))

        # GMX available liquidity tasks
        if self._gmx_available_liquidity_provider:
            for chain in gmx_chains_to_fetch:
                if chain in self._chains:
                    for market in gmx_mkts:
                        tasks.append(("gmx_liquidity", chain, market, ""))

        # GMX funding rate tasks
        if self._gmx_funding_rate_provider:
            for chain in gmx_chains_to_fetch:
                if chain in self._chains:
                    for market in gmx_mkts:
                        tasks.append(("gmx_funding", chain, market, ""))

        if not tasks:
            logger.debug("No protocol metrics tasks to fetch (no providers or empty task list)")
            return results

        def fetch_metric(task_type: str, chain: str, key: str) -> tuple[str, str, str, Decimal | None]:
            """Fetch a single metric and return (type, chain, key, value)."""
            try:
                if task_type == "aave_health":
                    value = self._aave_health_factor_provider(chain)  # type: ignore
                    return (task_type, chain, "", value)
                elif task_type == "aave_borrow":
                    value = self._aave_available_borrow_provider(key, chain)  # type: ignore
                    return (task_type, chain, key, value)
                elif task_type == "gmx_liquidity":
                    value = self._gmx_available_liquidity_provider(key, chain)  # type: ignore
                    return (task_type, chain, key, value)
                elif task_type == "gmx_funding":
                    value = self._gmx_funding_rate_provider(key, chain)  # type: ignore
                    return (task_type, chain, key, value)
                else:
                    return (task_type, chain, key, None)
            except Exception as e:
                logger.debug(f"Failed to fetch {task_type} for {key or 'health'} on {chain}: {e}")
                return (task_type, chain, key, None)

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            futures = {executor.submit(fetch_metric, t[0], t[1], t[2]): t for t in tasks}

            # Wait for completion with timeout
            done, not_done = concurrent.futures.wait(
                futures,
                timeout=self._fetch_timeout,
                return_when=concurrent.futures.ALL_COMPLETED,
            )

            # Process completed futures
            for future in done:
                try:
                    task_type, chain, key, value = future.result()
                    if task_type == "aave_health":
                        results["aave_health_factors"][chain] = value
                        self._aave_health_factor_cache[chain] = value
                    elif task_type == "aave_borrow":
                        results["aave_available_borrow"][chain][key] = value
                        self._aave_available_borrow_cache[chain][key.upper()] = value
                    elif task_type == "gmx_liquidity":
                        results["gmx_available_liquidity"][chain][key] = value
                        self._gmx_available_liquidity_cache[chain][key.upper()] = value
                    elif task_type == "gmx_funding":
                        results["gmx_funding_rates"][chain][key] = value
                        self._gmx_funding_rate_cache[chain][key.upper()] = value
                except Exception as e:
                    logger.debug(f"Future result error: {e}")

            # Log if any timed out
            if not_done:
                logger.warning(
                    f"Protocol metrics fetch timed out after {self._fetch_timeout}s, "
                    f"{len(not_done)} requests incomplete"
                )

        return results

    def total_portfolio_usd(self) -> Decimal:
        """Calculate total portfolio value in USD across all chains.

        Aggregates all known balances across all configured chains.

        Returns:
            Total portfolio value in USD
        """
        total = Decimal("0")

        # Sum pre-populated balances
        for _chain, balances in self._balances.items():
            for _token, balance in balances.items():
                total += balance.balance_usd

        # Sum cached balances (only those not already in pre-populated)
        for chain, cache in self._balance_cache.items():
            for token, balance in cache.items():
                # Skip if already counted in pre-populated
                if token not in self._balances.get(chain, {}):
                    total += balance.balance_usd

        return total

    def set_price(self, token: str, chain: str, price_value: Decimal) -> None:
        """Pre-populate price for a token on a specific chain.

        Args:
            token: Token symbol
            chain: Chain name
            price_value: Price value in USD

        Raises:
            ChainNotConfiguredError: If chain is not configured
        """
        chain_lower = self._validate_chain(chain)
        self._prices[chain_lower][token] = price_value

    def set_price_data(self, token: str, chain: str, price_data: "PriceData", quote: str = "USD") -> None:
        """Pre-populate enriched price data for a token on a specific chain.

        Unlike set_price() which only sets a scalar price, this sets the full
        PriceData object including change_24h_pct, high_24h, low_24h, etc.

        Args:
            token: Token symbol
            chain: Chain name
            price_data: PriceData with price, change_24h_pct, etc.
            quote: Quote currency (default "USD")

        Raises:
            ChainNotConfiguredError: If chain is not configured
        """
        chain_lower = self._validate_chain(chain)
        cache_key = f"{token}/{quote}"
        self._price_cache[chain_lower][cache_key] = price_data

    def set_balance(self, token: str, chain: str, balance_data: TokenBalance) -> None:
        """Pre-populate balance for a token on a specific chain.

        Args:
            token: Token symbol
            chain: Chain name
            balance_data: Balance data

        Raises:
            ChainNotConfiguredError: If chain is not configured
        """
        chain_lower = self._validate_chain(chain)
        self._balances[chain_lower][token] = balance_data

    def fetch_all_prices_parallel(
        self,
        tokens: list[str],
        quote: str = "USD",
    ) -> dict[str, dict[str, Decimal]]:
        """Fetch prices for multiple tokens across all chains in parallel.

        Uses concurrent execution to fetch prices from all chains within
        the configured timeout (default 2 seconds).

        Args:
            tokens: List of token symbols to fetch prices for
            quote: Quote currency (default "USD")

        Returns:
            Nested dict: {chain: {token: price}}
            Missing prices are omitted from the result.
        """
        if not self._price_oracle:
            logger.warning("No price oracle configured, cannot fetch prices")
            return {}

        results: dict[str, dict[str, Decimal]] = {c: {} for c in self._chains}

        def fetch_price(chain: str, token: str) -> tuple[str, str, Decimal | None]:
            """Fetch a single price and return (chain, token, price)."""
            try:
                price = self._price_oracle(token, quote, chain)  # type: ignore
                return (chain, token, price)
            except Exception as e:
                logger.debug(f"Failed to fetch {token}/{quote} on {chain}: {e}")
                return (chain, token, None)

        # Create all fetch tasks
        tasks = [(chain, token) for chain in self._chains for token in tokens]

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            futures = {executor.submit(fetch_price, chain, token): (chain, token) for chain, token in tasks}

            # Wait for completion with timeout
            done, not_done = concurrent.futures.wait(
                futures,
                timeout=self._fetch_timeout,
                return_when=concurrent.futures.ALL_COMPLETED,
            )

            # Process completed futures
            for future in done:
                try:
                    chain, token, price = future.result()
                    if price is not None:
                        results[chain][token] = price
                        # Update cache
                        cache_key = f"{token}/{quote}"
                        self._price_cache[chain][cache_key] = PriceData(price=price)
                except Exception as e:
                    logger.debug(f"Future result error: {e}")

            # Log if any timed out
            if not_done:
                logger.warning(
                    f"Price fetch timed out after {self._fetch_timeout}s, {len(not_done)} requests incomplete"
                )

        return results

    def fetch_all_balances_parallel(
        self,
        tokens: list[str],
    ) -> dict[str, dict[str, TokenBalance]]:
        """Fetch balances for multiple tokens across all chains in parallel.

        Uses concurrent execution to fetch balances from all chains within
        the configured timeout (default 2 seconds).

        Args:
            tokens: List of token symbols to fetch balances for

        Returns:
            Nested dict: {chain: {token: TokenBalance}}
            Missing balances are omitted from the result.
        """
        if not self._balance_provider:
            logger.warning("No balance provider configured, cannot fetch balances")
            return {}

        results: dict[str, dict[str, TokenBalance]] = {c: {} for c in self._chains}

        def fetch_balance(chain: str, token: str) -> tuple[str, str, TokenBalance | None]:
            """Fetch a single balance and return (chain, token, balance)."""
            try:
                balance = self._balance_provider(token, chain)  # type: ignore
                return (chain, token, balance)
            except Exception as e:
                logger.debug(f"Failed to fetch {token} balance on {chain}: {e}")
                return (chain, token, None)

        # Create all fetch tasks
        tasks = [(chain, token) for chain in self._chains for token in tokens]

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks)) as executor:
            futures = {executor.submit(fetch_balance, chain, token): (chain, token) for chain, token in tasks}

            # Wait for completion with timeout
            done, not_done = concurrent.futures.wait(
                futures,
                timeout=self._fetch_timeout,
                return_when=concurrent.futures.ALL_COMPLETED,
            )

            # Process completed futures
            for future in done:
                try:
                    chain, token, balance = future.result()
                    if balance is not None:
                        results[chain][token] = balance
                        # Update cache
                        self._balance_cache[chain][token] = balance
                except Exception as e:
                    logger.debug(f"Future result error: {e}")

            # Log if any timed out
            if not_done:
                logger.warning(
                    f"Balance fetch timed out after {self._fetch_timeout}s, {len(not_done)} requests incomplete"
                )

        return results

    def to_dict(self) -> dict[str, Any]:
        """Convert snapshot to dictionary."""
        return {
            "chains": self._chains,
            "wallet_address": self._wallet_address,
            "timestamp": self._timestamp.isoformat(),
            "data_freshness_policy": self._data_freshness_policy,
            "stale_data_threshold_seconds": self._stale_data_threshold_seconds,
            "chain_health": {chain: health.to_dict() for chain, health in self._chain_health.items()},
            "prices": {chain: {k: str(v) for k, v in prices.items()} for chain, prices in self._prices.items()},
            "balances": {
                chain: {
                    token: {
                        "symbol": balance.symbol,
                        "balance": str(balance.balance),
                        "balance_usd": str(balance.balance_usd),
                    }
                    for token, balance in balances.items()
                }
                for chain, balances in self._balances.items()
            },
        }

    # ------------------------------------------------------------------
    # Critical-data-failure tracking API
    # Mirrors MarketSnapshot so the runner can call these methods uniformly
    # across single-chain and multi-chain strategies without hasattr guards.
    # ------------------------------------------------------------------

    def has_critical_data_failures(self) -> bool:
        """Return True when this snapshot observed any critical data failures."""
        return bool(self._critical_data_failures)

    def critical_data_failure_count(self) -> int:
        """Number of currently tracked critical failures for this snapshot."""
        return len(self._critical_data_failures)

    def clear_critical_data_failures(self) -> None:
        """Clear all tracked critical data failures (called after price pre-warm)."""
        self._critical_data_failures.clear()

    def classify_critical_data_failures(self) -> str:
        """Classify observed failures as transient, permanent, or mixed."""
        if not self._critical_data_failures:
            return "none"

        transient_hints = (
            "timeout",
            "timed out",
            "temporarily unavailable",
            "rate limit",
            "429",
            "connection reset",
            "unavailable",
            "resource exhausted",
            "service unavailable",
            "statuscode.internal",
            "statuscode.unavailable",
            "statuscode.resource_exhausted",
            "statuscode.deadline_exceeded",
        )
        permanent_hints = (
            "cannot resolve token",
            "token '",
            "unknown token",
            "no chainlink feed",
            "not found",
            "unsupported",
            "invalid",
            "no pairs found",
            "symbol",
        )

        has_transient = False
        has_permanent = False
        for detail in self._critical_data_failures.values():
            lowered = detail.lower()
            stripped = _DSU_BOILERPLATE_RE.sub("", lowered)
            found_permanent = any(hint in stripped for hint in permanent_hints)
            found_transient = any(hint in stripped for hint in transient_hints)
            if found_permanent:
                has_permanent = True
            if found_transient:
                has_transient = True
            elif not found_permanent:
                # Unknown class: be conservative and treat as transient.
                has_transient = True

        if has_transient and has_permanent:
            return "mixed"
        if has_permanent:
            return "permanent"
        return "transient"

    def summarize_critical_data_failures(self, *, limit: int = 3) -> str:
        """Create a concise summary for logs/lifecycle error messages."""
        if not self._critical_data_failures:
            return ""

        chunks: list[str] = []
        for idx, ((source, key), detail) in enumerate(self._critical_data_failures.items()):
            if idx >= limit:
                break
            chunks.append(f"{source}({key}): {detail}")

        remaining = len(self._critical_data_failures) - len(chunks)
        if remaining > 0:
            chunks.append(f"... and {remaining} more")
        return "; ".join(chunks)


__all__ = [
    "ChainNotConfiguredError",
    "MultiChainPriceOracle",
    "MultiChainBalanceProvider",
    "ChainHealthStatus",
    "ChainHealth",
    "StaleDataError",
    "DataFreshnessPolicy",
    "AaveHealthFactorProvider",
    "AaveAvailableBorrowProvider",
    "GmxAvailableLiquidityProvider",
    "GmxFundingRateProvider",
    "MultiChainMarketSnapshot",
]
