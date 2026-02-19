"""Base interface for strategy-specific backtest adapters.

This module defines the abstract base class for all strategy backtest adapters.
Adapters provide strategy-specific logic for backtesting, allowing the backtesting
engine to accurately simulate different position types (LP, perps, lending, etc.).

Key Components:
    - StrategyBacktestAdapter: Abstract base class for all adapters
    - AdapterRegistry: Registry for adapter discovery and lookup
    - get_adapter: Convenience function for registry lookup

Each adapter implements four core methods:
    - execute_intent: Simulate execution of an intent
    - update_position: Update position state each tick (fees, funding, interest)
    - value_position: Calculate current position value in USD
    - should_rebalance: Determine if position needs rebalancing

Example:
    from almanak.framework.backtesting.adapters.base import (
        StrategyBacktestAdapter,
        get_adapter,
        register_adapter,
    )

    # Look up an adapter by strategy type
    adapter = get_adapter("lp")
    if adapter:
        fill = adapter.execute_intent(intent, portfolio, market_state)

    # Register a custom adapter
    @register_adapter("custom_strategy")
    class CustomAdapter(StrategyBacktestAdapter):
        def execute_intent(self, intent, portfolio, market_state) -> SimulatedFill:
            ...
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from almanak.framework.backtesting.config import BacktestDataConfig
    from almanak.framework.backtesting.pnl.data_provider import MarketState
    from almanak.framework.backtesting.pnl.portfolio import (
        SimulatedFill,
        SimulatedPortfolio,
        SimulatedPosition,
    )
    from almanak.framework.intents.vocabulary import Intent


@dataclass
class StrategyBacktestConfig:
    """Base configuration for strategy-specific backtest adapters.

    This dataclass provides common configuration fields that all strategy
    adapters can use. Subclasses can extend this to add strategy-specific
    configuration options while maintaining a consistent interface.

    The configuration controls which features are enabled during backtesting
    and provides extensibility through the extra_params field.

    Attributes:
        strategy_type: Identifier for the strategy type (e.g., "lp", "perp",
            "lending", "arbitrage"). Used for adapter lookup in the registry.
        fee_tracking_enabled: Whether to track and accrue protocol fees during
            backtesting. When True, LP fees, trading fees, and other protocol
            charges are calculated and applied to positions. Default True.
        position_tracking_enabled: Whether to track individual positions with
            full detail. When True, maintains complete position history including
            all state changes. Useful for detailed analysis but increases memory
            usage. Default True.
        reconcile_on_tick: Whether to reconcile position state on each tick.
            When True, validates tracked position state against expected values.
            Default False.
        extra_params: Additional adapter-specific parameters as key-value pairs.
            Provides extensibility for custom adapter configurations without
            requiring base class changes. Default empty dict.

    Example:
        # Create a basic config
        config = StrategyBacktestConfig(
            strategy_type="lp",
            fee_tracking_enabled=True,
            position_tracking_enabled=True,
        )

        # Create an extended config subclass
        @dataclass
        class LPBacktestConfig(StrategyBacktestConfig):
            il_calculation_method: str = "standard"
            rebalance_on_out_of_range: bool = True

        lp_config = LPBacktestConfig(
            strategy_type="lp",
            il_calculation_method="concentrated",
        )

    Note:
        Subclasses should call super().__post_init__() if they override
        __post_init__ to ensure proper initialization of base fields.
    """

    strategy_type: str
    """Identifier for the strategy type (e.g., "lp", "perp", "lending")."""

    fee_tracking_enabled: bool = True
    """Whether to track and accrue protocol fees during backtesting."""

    position_tracking_enabled: bool = True
    """Whether to track individual positions with full detail."""

    reconcile_on_tick: bool = False
    """Whether to reconcile position state on each tick."""

    extra_params: dict[str, Any] = field(default_factory=dict)
    """Additional adapter-specific parameters as key-value pairs."""

    strict_reproducibility: bool = False
    """Enforce strict reproducibility mode (default: False).

    When enabled, the adapter will raise ValueError instead of falling back to
    datetime.now() when simulation timestamp is missing. This ensures that
    backtests are fully reproducible by preventing any non-deterministic behavior.

    Use this mode when you need byte-identical results across multiple runs.
    When disabled, the adapter will use datetime.now() as fallback and log
    warnings instead of failing.
    """

    def __post_init__(self) -> None:
        """Validate configuration after initialization.

        Raises:
            ValueError: If strategy_type is empty or invalid.
        """
        if not self.strategy_type:
            msg = "strategy_type must be a non-empty string"
            raise ValueError(msg)
        if not isinstance(self.strategy_type, str):
            msg = f"strategy_type must be a string, got {type(self.strategy_type).__name__}"
            raise ValueError(msg)

    def to_dict(self) -> dict[str, Any]:
        """Serialize configuration to a dictionary.

        Returns:
            Dictionary representation of the configuration.
        """
        return {
            "strategy_type": self.strategy_type,
            "fee_tracking_enabled": self.fee_tracking_enabled,
            "position_tracking_enabled": self.position_tracking_enabled,
            "reconcile_on_tick": self.reconcile_on_tick,
            "extra_params": dict(self.extra_params),
            "strict_reproducibility": self.strict_reproducibility,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StrategyBacktestConfig":
        """Create configuration from a dictionary.

        Args:
            data: Dictionary with configuration values.

        Returns:
            New configuration instance.

        Note:
            Subclasses should override this method to handle their
            specific fields while calling super().from_dict() or
            constructing directly.
        """
        return cls(
            strategy_type=data["strategy_type"],
            fee_tracking_enabled=data.get("fee_tracking_enabled", True),
            position_tracking_enabled=data.get("position_tracking_enabled", True),
            reconcile_on_tick=data.get("reconcile_on_tick", False),
            extra_params=data.get("extra_params", {}),
            strict_reproducibility=data.get("strict_reproducibility", False),
        )

    def with_updates(self, **kwargs: Any) -> "StrategyBacktestConfig":
        """Create a new config with updated values.

        This method allows creating a modified copy of the configuration
        without mutating the original.

        Args:
            **kwargs: Field names and new values to update.

        Returns:
            New configuration instance with updated values.

        Example:
            base_config = StrategyBacktestConfig(strategy_type="lp")
            debug_config = base_config.with_updates(
                fee_tracking_enabled=False,
                reconcile_on_tick=True,
            )
        """
        current = self.to_dict()
        current.update(kwargs)
        return self.__class__.from_dict(current)


class StrategyBacktestAdapter(ABC):
    """Abstract base class for strategy-specific backtest adapters.

    Adapters encapsulate the custom backtesting behavior for different strategy
    types. While the core backtesting engine handles common functionality like
    portfolio tracking and metrics calculation, adapters handle strategy-specific
    logic such as:

    - LP strategies: Fee accrual, impermanent loss, range checking
    - Perp strategies: Funding payments, margin validation, liquidation
    - Lending strategies: Interest accrual, health factor tracking
    - Arbitrage strategies: Multi-hop execution, cumulative slippage

    Each adapter must implement four core methods that define how positions
    of that type behave during backtesting.

    Attributes:
        adapter_name: Unique identifier for this adapter (property)

    Example:
        class MyAdapter(StrategyBacktestAdapter):
            @property
            def adapter_name(self) -> str:
                return "my_strategy"

            def execute_intent(
                self,
                intent: Intent,
                portfolio: SimulatedPortfolio,
                market_state: MarketState,
            ) -> SimulatedFill | None:
                # Custom execution logic
                ...

            def update_position(
                self,
                position: SimulatedPosition,
                market_state: MarketState,
                elapsed_seconds: float,
            ) -> None:
                # Custom position update logic
                ...

            def value_position(
                self,
                position: SimulatedPosition,
                market_state: MarketState,
            ) -> Decimal:
                # Custom valuation logic
                return position.total_amount * market_state.get_price(position.primary_token)

            def should_rebalance(
                self,
                position: SimulatedPosition,
                market_state: MarketState,
            ) -> bool:
                # Custom rebalance trigger logic
                return False
    """

    @property
    @abstractmethod
    def adapter_name(self) -> str:
        """Return the unique name of this adapter.

        This should match the strategy type identifier used in the registry.

        Returns:
            Strategy type identifier string (e.g., "lp", "perp", "lending")
        """
        ...

    @abstractmethod
    def execute_intent(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill | None":
        """Simulate execution of an intent and return the resulting fill.

        This method handles the strategy-specific logic for executing intents.
        It should simulate realistic execution including:

        - Price impact and slippage calculation
        - Fee calculation based on protocol
        - Position opening/closing/modification
        - Gas cost estimation

        The method should NOT directly modify the portfolio state. Instead,
        it should return a SimulatedFill that describes the execution result.
        The backtesting engine will apply the fill to the portfolio.

        Args:
            intent: The intent to execute (SwapIntent, LPOpenIntent, etc.)
            portfolio: Current portfolio state (read-only for this method)
            market_state: Current market prices and data

        Returns:
            SimulatedFill describing the execution result, or None if the
            intent cannot be executed (e.g., insufficient balance).

        Note:
            If the adapter returns None, the backtesting engine will fall back
            to default execution logic or skip the intent entirely.
        """
        ...

    @abstractmethod
    def update_position(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        elapsed_seconds: float,
        timestamp: datetime | None = None,
    ) -> None:
        """Update position state based on time passage and market changes.

        This method is called during mark-to-market to update strategy-specific
        position fields. It handles time-based effects such as:

        - LP positions: Fee accrual based on volume and liquidity share
        - Perp positions: Funding payment application
        - Lending positions: Interest accrual (compound or simple)

        This method SHOULD modify the position in-place to update:
        - Accumulated fees/interest/funding
        - Health factors
        - Liquidation prices
        - Any other time-dependent fields

        Args:
            position: The position to update (modified in-place)
            market_state: Current market prices and data
            elapsed_seconds: Time elapsed since last update in seconds
            timestamp: Simulation timestamp for deterministic updates. If None,
                implementations may fall back to datetime.now() but this is
                discouraged as it breaks backtest reproducibility. Callers
                should always pass market_state.timestamp when available.

        Note:
            This method should handle the position type it was designed for.
            If called with an incompatible position type, it may do nothing
            or raise an error depending on implementation.
        """
        ...

    @abstractmethod
    def value_position(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        timestamp: datetime | None = None,
    ) -> Decimal:
        """Calculate the current USD value of a position.

        This method computes the total value of a position in USD, accounting
        for strategy-specific factors:

        - LP positions: Token amounts + accumulated fees - IL
        - Perp positions: Collateral +/- unrealized PnL +/- funding
        - Lending positions: Principal + accrued interest
        - Spot positions: Simple token amount * price

        The returned value should include ALL components that contribute to
        the position's equity value.

        Args:
            position: The position to value
            market_state: Current market prices and data
            timestamp: Simulation timestamp for deterministic valuation. If None,
                implementations should use market_state.timestamp when available.
                This parameter ensures valuations are tied to simulation time
                rather than wall clock time for reproducible backtests.

        Returns:
            Total position value in USD as a Decimal

        Note:
            For borrow positions (debt), this should return a positive value
            representing the debt amount. The portfolio calculates net value
            by subtracting borrows from supplies.
        """
        ...

    @abstractmethod
    def should_rebalance(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
    ) -> bool:
        """Determine if a position should be rebalanced.

        This method checks strategy-specific conditions that indicate a
        position needs adjustment:

        - LP positions: Price moved outside tick range
        - Perp positions: Leverage ratio too high/low
        - Lending positions: Health factor near liquidation threshold

        The backtesting engine uses this to trigger alerts or automatic
        rebalancing depending on configuration.

        Args:
            position: The position to check
            market_state: Current market prices and data

        Returns:
            True if the position should be rebalanced, False otherwise

        Note:
            This method should NOT perform the rebalancing itself - it only
            indicates whether rebalancing is needed. The actual rebalancing
            is handled by the strategy's decide() method or automatic
            risk management.
        """
        ...

    def to_dict(self) -> dict[str, Any]:
        """Serialize the adapter configuration to a dictionary.

        Subclasses should override this to include their specific
        configuration parameters.

        Returns:
            Dictionary with adapter configuration
        """
        return {
            "adapter_name": self.adapter_name,
        }


@dataclass
class AdapterMetadata:
    """Metadata for a registered adapter.

    Attributes:
        name: Strategy type identifier (e.g., "lp", "perp")
        adapter_class: The adapter class
        description: Human-readable description
        aliases: Alternative names that map to this adapter
    """

    name: str
    adapter_class: type[StrategyBacktestAdapter]
    description: str = ""
    aliases: list[str] | None = None


class AdapterRegistry:
    """Registry for adapter discovery and lookup.

    The registry maintains a mapping from strategy type names to adapter
    classes, allowing dynamic lookup and instantiation of adapters.

    Adapters are registered using the `register` method or the
    `register_adapter` decorator. They can then be looked up by
    strategy type using `get` or `get_adapter`.

    Example:
        # Register via method
        AdapterRegistry.register("my_strategy", MyAdapter)

        # Look up and instantiate
        adapter_class = AdapterRegistry.get("my_strategy")
        adapter = adapter_class()

        # Get all registered strategy types
        types = AdapterRegistry.list_strategy_types()
    """

    # Class-level registry storage
    _registry: dict[str, AdapterMetadata] = {}

    @classmethod
    def register(
        cls,
        name: str,
        adapter_class: type[StrategyBacktestAdapter],
        description: str = "",
        aliases: list[str] | None = None,
    ) -> None:
        """Register an adapter class for a strategy type.

        Args:
            name: Primary strategy type identifier (e.g., "lp", "perp")
            adapter_class: The adapter class to register
            description: Human-readable description of the adapter
            aliases: Additional strategy type names that map to this adapter
        """
        all_aliases = list(aliases) if aliases else []

        metadata = AdapterMetadata(
            name=name,
            adapter_class=adapter_class,
            description=description,
            aliases=all_aliases,
        )

        # Register under primary name
        cls._registry[name.lower()] = metadata

        # Register aliases
        for alias in all_aliases:
            cls._registry[alias.lower()] = metadata

    @classmethod
    def get(cls, strategy_type: str) -> type[StrategyBacktestAdapter] | None:
        """Get the adapter class for a strategy type.

        Args:
            strategy_type: Strategy type identifier (case-insensitive)

        Returns:
            Adapter class or None if not found
        """
        metadata = cls._registry.get(strategy_type.lower())
        if metadata:
            return metadata.adapter_class
        return None

    @classmethod
    def get_metadata(cls, strategy_type: str) -> AdapterMetadata | None:
        """Get metadata for a registered adapter.

        Args:
            strategy_type: Strategy type identifier (case-insensitive)

        Returns:
            AdapterMetadata or None if not found
        """
        return cls._registry.get(strategy_type.lower())

    @classmethod
    def list_strategy_types(cls) -> list[str]:
        """List all registered strategy type names.

        Returns:
            List of registered strategy type identifiers
        """
        # Return unique primary names (not aliases)
        seen = set()
        types = []
        for metadata in cls._registry.values():
            if metadata.name not in seen:
                seen.add(metadata.name)
                types.append(metadata.name)
        return sorted(types)

    @classmethod
    def list_all(cls) -> dict[str, AdapterMetadata]:
        """Get all registered adapters with their metadata.

        Returns:
            Dictionary mapping strategy types to metadata
        """
        # Return only primary names
        result = {}
        for metadata in cls._registry.values():
            if metadata.name not in result:
                result[metadata.name] = metadata
        return result

    @classmethod
    def clear(cls) -> None:
        """Clear all registered adapters.

        This is primarily useful for testing.
        """
        cls._registry.clear()


def register_adapter(
    name: str,
    description: str = "",
    aliases: list[str] | None = None,
) -> Any:
    """Decorator to register an adapter class.

    Args:
        name: Strategy type identifier (e.g., "lp", "perp")
        description: Human-readable description
        aliases: Additional strategy type names

    Returns:
        Class decorator

    Example:
        @register_adapter("my_strategy", description="My custom adapter")
        class MyAdapter(StrategyBacktestAdapter):
            ...
    """

    def decorator(cls: type[StrategyBacktestAdapter]) -> type[StrategyBacktestAdapter]:
        AdapterRegistry.register(name, cls, description, aliases)
        return cls

    return decorator


def get_adapter(strategy_type: str) -> StrategyBacktestAdapter | None:
    """Get an instantiated adapter for a strategy type.

    This is a convenience function that looks up the adapter class
    in the registry and instantiates it with default parameters.

    Args:
        strategy_type: Strategy type identifier (case-insensitive)

    Returns:
        Instantiated adapter or None if not found

    Example:
        adapter = get_adapter("lp")
        if adapter:
            fill = adapter.execute_intent(intent, portfolio, market_state)
    """
    adapter_class = AdapterRegistry.get(strategy_type)
    if adapter_class:
        return adapter_class()
    return None


def get_adapter_with_config(
    strategy_type: str,
    data_config: "BacktestDataConfig | None" = None,
) -> StrategyBacktestAdapter | None:
    """Get an instantiated adapter for a strategy type with data config.

    This function looks up the adapter class in the registry and instantiates
    it with the provided BacktestDataConfig for controlling historical data
    provider behavior.

    Args:
        strategy_type: Strategy type identifier (case-insensitive)
        data_config: BacktestDataConfig for historical data provider settings.
            If provided, will be passed to the adapter constructor.

    Returns:
        Instantiated adapter or None if not found

    Example:
        from almanak.framework.backtesting.config import BacktestDataConfig

        data_config = BacktestDataConfig(
            use_historical_volume=True,
            use_historical_funding=True,
        )
        adapter = get_adapter_with_config("lp", data_config=data_config)
        if adapter:
            fill = adapter.execute_intent(intent, portfolio, market_state)
    """
    adapter_class = AdapterRegistry.get(strategy_type)
    if adapter_class:
        # Try to pass data_config to the adapter constructor
        # All registered adapters (LP, Perp, Lending) accept data_config parameter
        try:
            return adapter_class(data_config=data_config)  # type: ignore[call-arg]
        except TypeError:
            # Fallback for adapters that don't accept data_config
            return adapter_class()
    return None


__all__ = [
    "AdapterMetadata",
    "AdapterRegistry",
    "StrategyBacktestAdapter",
    "StrategyBacktestConfig",
    "get_adapter",
    "get_adapter_with_config",
    "register_adapter",
]
