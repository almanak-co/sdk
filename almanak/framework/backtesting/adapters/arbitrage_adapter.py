"""Arbitrage backtest adapter for multi-step execution strategies.

This module provides the backtest adapter for arbitrage strategies, handling
multi-hop trades, cumulative slippage, and MEV cost simulation. It manages:

- Multi-step execution with configurable delays between hops
- Cumulative slippage across multiple swap hops
- MEV impact simulation based on trade size and tokens
- Execution sequence tracking for analysis

Key Features:
    - Configurable slippage model (multiplicative or additive)
    - MEV simulation integration for realistic cost estimation
    - Execution delay modeling between trade steps
    - Trade sequence tracking for post-execution analysis

Example:
    from almanak.framework.backtesting.adapters.arbitrage_adapter import (
        ArbitrageBacktestAdapter,
        ArbitrageBacktestConfig,
    )

    # Create config for arbitrage backtesting
    config = ArbitrageBacktestConfig(
        strategy_type="arbitrage",
        mev_simulation_enabled=True,
        cumulative_slippage_model="multiplicative",
        execution_delay_seconds=1.0,
    )

    # Get adapter instance
    adapter = ArbitrageBacktestAdapter(config)

    # Use in backtesting
    fill = adapter.execute_intent(intent, portfolio, market_state)
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal

from almanak.framework.backtesting.adapters.base import (
    StrategyBacktestAdapter,
    StrategyBacktestConfig,
    register_adapter,
)

if TYPE_CHECKING:
    from almanak.framework.backtesting.pnl.data_provider import MarketState
    from almanak.framework.backtesting.pnl.portfolio import (
        SimulatedFill,
        SimulatedPortfolio,
        SimulatedPosition,
    )
    from almanak.framework.intents.vocabulary import Intent

logger = logging.getLogger(__name__)


class CumulativeSlippageModel(StrEnum):
    """Model for calculating cumulative slippage across multiple hops.

    Attributes:
        MULTIPLICATIVE: Slippage compounds multiplicatively across hops.
            Final_slippage = 1 - (1 - s1) * (1 - s2) * ... * (1 - sn)
            More accurate for real-world scenarios where each hop's
            slippage reduces the available amount for the next hop.
        ADDITIVE: Slippage accumulates additively across hops.
            Final_slippage = s1 + s2 + ... + sn
            Simpler model, may overestimate total slippage for many hops.
    """

    MULTIPLICATIVE = "multiplicative"
    ADDITIVE = "additive"


@dataclass
class ExecutionStep:
    """A single step in a multi-hop arbitrage execution.

    Attributes:
        step_number: Sequential step number (1-indexed)
        token_in: Input token for this step
        token_out: Output token for this step
        amount_in: Amount of input token
        amount_out: Amount of output token after slippage/fees
        slippage_pct: Slippage incurred on this step (as decimal)
        fee_pct: Trading fee for this step (as decimal)
        mev_cost_usd: MEV cost incurred on this step
        execution_delay_seconds: Delay before this step was executed
        protocol: Protocol used for this step (e.g., "uniswap_v3")
        pool_address: Optional pool address for this step
    """

    step_number: int
    token_in: str
    token_out: str
    amount_in: Decimal
    amount_out: Decimal
    slippage_pct: Decimal
    fee_pct: Decimal
    mev_cost_usd: Decimal = Decimal("0")
    execution_delay_seconds: float = 0.0
    protocol: str = ""
    pool_address: str = ""

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "step_number": self.step_number,
            "token_in": self.token_in,
            "token_out": self.token_out,
            "amount_in": str(self.amount_in),
            "amount_out": str(self.amount_out),
            "slippage_pct": str(self.slippage_pct),
            "slippage_bps": float(self.slippage_pct * Decimal("10000")),
            "fee_pct": str(self.fee_pct),
            "fee_bps": float(self.fee_pct * Decimal("10000")),
            "mev_cost_usd": str(self.mev_cost_usd),
            "execution_delay_seconds": self.execution_delay_seconds,
            "protocol": self.protocol,
            "pool_address": self.pool_address,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionStep":
        """Deserialize from dictionary."""
        return cls(
            step_number=data["step_number"],
            token_in=data["token_in"],
            token_out=data["token_out"],
            amount_in=Decimal(data["amount_in"]),
            amount_out=Decimal(data["amount_out"]),
            slippage_pct=Decimal(data["slippage_pct"]),
            fee_pct=Decimal(data["fee_pct"]),
            mev_cost_usd=Decimal(data.get("mev_cost_usd", "0")),
            execution_delay_seconds=data.get("execution_delay_seconds", 0.0),
            protocol=data.get("protocol", ""),
            pool_address=data.get("pool_address", ""),
        )


@dataclass
class ArbitrageExecutionResult:
    """Result of a multi-hop arbitrage execution.

    Attributes:
        steps: List of execution steps in order
        total_slippage_pct: Total cumulative slippage across all hops
        total_fees_pct: Total fees across all hops
        total_mev_cost_usd: Total MEV cost across all hops
        total_execution_delay_seconds: Total delay for all steps
        initial_amount: Starting amount in initial token
        final_amount: Final amount after all hops
        profit_loss_pct: PnL as percentage of initial amount
        execution_model: Which slippage model was used
    """

    steps: list[ExecutionStep]
    total_slippage_pct: Decimal
    total_fees_pct: Decimal
    total_mev_cost_usd: Decimal
    total_execution_delay_seconds: float
    initial_amount: Decimal
    final_amount: Decimal
    profit_loss_pct: Decimal
    execution_model: CumulativeSlippageModel

    @property
    def num_hops(self) -> int:
        """Number of hops in this execution."""
        return len(self.steps)

    @property
    def is_profitable(self) -> bool:
        """Whether the arbitrage was profitable after all costs."""
        return self.profit_loss_pct > Decimal("0")

    def to_dict(self) -> dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "steps": [step.to_dict() for step in self.steps],
            "total_slippage_pct": str(self.total_slippage_pct),
            "total_slippage_bps": float(self.total_slippage_pct * Decimal("10000")),
            "total_fees_pct": str(self.total_fees_pct),
            "total_fees_bps": float(self.total_fees_pct * Decimal("10000")),
            "total_mev_cost_usd": str(self.total_mev_cost_usd),
            "total_execution_delay_seconds": self.total_execution_delay_seconds,
            "initial_amount": str(self.initial_amount),
            "final_amount": str(self.final_amount),
            "profit_loss_pct": str(self.profit_loss_pct),
            "num_hops": self.num_hops,
            "is_profitable": self.is_profitable,
            "execution_model": self.execution_model.value,
        }


@dataclass
class ArbitrageBacktestConfig(StrategyBacktestConfig):
    """Configuration for arbitrage-specific backtesting.

    This config extends the base StrategyBacktestConfig with arbitrage-specific
    options for controlling MEV simulation, slippage modeling, and execution
    delay behavior.

    Attributes:
        strategy_type: Must be "arbitrage" for arbitrage adapter (inherited)
        fee_tracking_enabled: Whether to track trading fees (inherited)
        position_tracking_enabled: Whether to track positions in detail (inherited)
        reconcile_on_tick: Whether to reconcile position state each tick (inherited)
        extra_params: Additional parameters (inherited)
        mev_simulation_enabled: Whether to simulate MEV costs for trades.
            When True, sandwich attack probability and MEV extraction costs
            are calculated for each hop based on trade size and token pair.
            Default False.
        cumulative_slippage_model: How slippage accumulates across hops.
            - "multiplicative": Slippage compounds (more realistic)
            - "additive": Slippage adds up (simpler)
            Default "multiplicative".
        execution_delay_seconds: Base delay between execution steps in seconds.
            Simulates the time required to execute each hop, which can affect
            price and liquidity between steps. Default 1.0 seconds.
        max_hops: Maximum number of hops allowed in a single arbitrage.
            Limits complexity and ensures reasonable execution. Default 5.
        base_slippage_per_hop_pct: Base slippage percentage per hop.
            Used when actual slippage cannot be calculated. Default 0.1%.
        mev_random_seed: Optional seed for reproducible MEV simulations.
            Default None (random).

    Example:
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            mev_simulation_enabled=True,
            cumulative_slippage_model="multiplicative",
            execution_delay_seconds=0.5,
            max_hops=4,
        )
    """

    mev_simulation_enabled: bool = False
    """Whether to simulate MEV costs for trades."""

    cumulative_slippage_model: Literal["multiplicative", "additive"] = "multiplicative"
    """How slippage accumulates across hops."""

    execution_delay_seconds: float = 1.0
    """Base delay between execution steps in seconds."""

    max_hops: int = 5
    """Maximum number of hops allowed in a single arbitrage."""

    base_slippage_per_hop_pct: Decimal = Decimal("0.001")  # 0.1%
    """Base slippage percentage per hop (as decimal)."""

    base_fee_per_hop_pct: Decimal = Decimal("0.003")  # 0.3%
    """Base trading fee percentage per hop (as decimal)."""

    mev_random_seed: int | None = None
    """Optional seed for reproducible MEV simulations."""

    def __post_init__(self) -> None:
        """Validate arbitrage-specific configuration.

        Raises:
            ValueError: If strategy_type is not "arbitrage" or other
                validation fails.
        """
        # Call parent validation
        super().__post_init__()

        # Validate strategy_type for arbitrage
        if self.strategy_type.lower() != "arbitrage":
            msg = f"ArbitrageBacktestConfig requires strategy_type='arbitrage', got '{self.strategy_type}'"
            raise ValueError(msg)

        # Validate cumulative_slippage_model
        valid_models = {"multiplicative", "additive"}
        if self.cumulative_slippage_model not in valid_models:
            msg = f"cumulative_slippage_model must be one of {valid_models}, got '{self.cumulative_slippage_model}'"
            raise ValueError(msg)

        # Validate numeric constraints
        if self.execution_delay_seconds < 0:
            msg = "execution_delay_seconds must be non-negative"
            raise ValueError(msg)

        if self.max_hops < 1:
            msg = "max_hops must be at least 1"
            raise ValueError(msg)

        if not (Decimal("0") <= self.base_slippage_per_hop_pct <= Decimal("1")):
            msg = "base_slippage_per_hop_pct must be between 0 and 1"
            raise ValueError(msg)

        if not (Decimal("0") <= self.base_fee_per_hop_pct <= Decimal("1")):
            msg = "base_fee_per_hop_pct must be between 0 and 1"
            raise ValueError(msg)

    def to_dict(self) -> dict[str, Any]:
        """Serialize configuration to a dictionary.

        Returns:
            Dictionary representation of the configuration.
        """
        base = super().to_dict()
        base.update(
            {
                "mev_simulation_enabled": self.mev_simulation_enabled,
                "cumulative_slippage_model": self.cumulative_slippage_model,
                "execution_delay_seconds": self.execution_delay_seconds,
                "max_hops": self.max_hops,
                "base_slippage_per_hop_pct": str(self.base_slippage_per_hop_pct),
                "base_fee_per_hop_pct": str(self.base_fee_per_hop_pct),
                "mev_random_seed": self.mev_random_seed,
            }
        )
        return base

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ArbitrageBacktestConfig":
        """Create configuration from a dictionary.

        Args:
            data: Dictionary with configuration values.

        Returns:
            New ArbitrageBacktestConfig instance.
        """
        return cls(
            strategy_type=data.get("strategy_type", "arbitrage"),
            fee_tracking_enabled=data.get("fee_tracking_enabled", True),
            position_tracking_enabled=data.get("position_tracking_enabled", True),
            reconcile_on_tick=data.get("reconcile_on_tick", False),
            extra_params=data.get("extra_params", {}),
            strict_reproducibility=data.get("strict_reproducibility", False),
            mev_simulation_enabled=data.get("mev_simulation_enabled", False),
            cumulative_slippage_model=data.get("cumulative_slippage_model", "multiplicative"),
            execution_delay_seconds=data.get("execution_delay_seconds", 1.0),
            max_hops=data.get("max_hops", 5),
            base_slippage_per_hop_pct=Decimal(str(data.get("base_slippage_per_hop_pct", "0.001"))),
            base_fee_per_hop_pct=Decimal(str(data.get("base_fee_per_hop_pct", "0.003"))),
            mev_random_seed=data.get("mev_random_seed"),
        )


@register_adapter(
    "arbitrage",
    description="Adapter for arbitrage strategies with multi-hop execution and MEV simulation",
    aliases=["arb", "mev", "flash_loan"],
)
class ArbitrageBacktestAdapter(StrategyBacktestAdapter):
    """Backtest adapter for arbitrage strategies.

    This adapter handles the simulation of multi-hop arbitrage trades during
    backtesting. It provides:

    - Multi-step execution simulation with delays
    - Cumulative slippage calculation (multiplicative or additive)
    - MEV cost simulation for vulnerable trades
    - Execution sequence tracking for analysis

    The adapter can be used with or without explicit configuration.
    When used without config, it uses sensible defaults.

    Attributes:
        config: Arbitrage-specific configuration (optional)

    Example:
        # With config
        config = ArbitrageBacktestConfig(
            strategy_type="arbitrage",
            mev_simulation_enabled=True,
            cumulative_slippage_model="multiplicative",
        )
        adapter = ArbitrageBacktestAdapter(config)

        # Without config (uses defaults)
        adapter = ArbitrageBacktestAdapter()

        # Calculate cumulative slippage for a multi-hop trade
        result = adapter.calculate_cumulative_slippage(
            hops=[
                ("USDC", "WETH", Decimal("0.003")),
                ("WETH", "ARB", Decimal("0.005")),
                ("ARB", "USDC", Decimal("0.004")),
            ],
            initial_amount=Decimal("10000"),
        )
    """

    def __init__(self, config: ArbitrageBacktestConfig | None = None) -> None:
        """Initialize the arbitrage backtest adapter.

        Args:
            config: Arbitrage-specific configuration. If None, uses default
                ArbitrageBacktestConfig with strategy_type="arbitrage".
        """
        self._config = config or ArbitrageBacktestConfig(strategy_type="arbitrage")
        self._mev_simulator: Any = None  # Lazy-loaded to avoid circular imports
        self._execution_history: list[ArbitrageExecutionResult] = []

    @property
    def adapter_name(self) -> str:
        """Return the unique name of this adapter.

        Returns:
            Strategy type identifier "arbitrage"
        """
        return "arbitrage"

    @property
    def config(self) -> ArbitrageBacktestConfig:
        """Get the adapter configuration.

        Returns:
            Arbitrage backtest configuration
        """
        return self._config

    @property
    def execution_history(self) -> list[ArbitrageExecutionResult]:
        """Get the execution history for this adapter.

        Returns:
            List of ArbitrageExecutionResult from previous executions
        """
        return self._execution_history

    def _get_mev_simulator(self) -> Any:
        """Lazy-load the MEV simulator to avoid circular imports.

        Returns:
            MEVSimulator instance
        """
        if self._mev_simulator is None and self._config.mev_simulation_enabled:
            from almanak.framework.backtesting.pnl.mev_simulator import (
                MEVSimulator,
                MEVSimulatorConfig,
            )

            mev_config = MEVSimulatorConfig(random_seed=self._config.mev_random_seed)
            self._mev_simulator = MEVSimulator(config=mev_config)
        return self._mev_simulator

    def execute_intent(
        self,
        intent: "Intent",
        portfolio: "SimulatedPortfolio",
        market_state: "MarketState",
    ) -> "SimulatedFill | None":
        """Simulate execution of an arbitrage-related intent.

        This method handles SWAP intents for arbitrage strategies, applying:
        - Multi-step execution tracking with configurable delays
        - MEV impact simulation based on trade size and token pair
        - Cumulative slippage calculation (multiplicative or additive)

        For single swaps, the method tracks the execution as a 1-hop trade.
        For multi-hop scenarios (detected via metadata), applies cumulative
        slippage across all hops.

        Args:
            intent: The intent to execute (typically SwapIntent)
            portfolio: Current portfolio state
            market_state: Current market prices and data

        Returns:
            SimulatedFill describing the execution result with MEV and
            slippage applied, or None for non-swap intents to use default
            execution logic.
        """
        from almanak.framework.backtesting.models import IntentType
        from almanak.framework.backtesting.pnl.portfolio import SimulatedFill
        from almanak.framework.intents.vocabulary import SwapIntent

        # Only handle SWAP intents - other intents use default execution
        if not isinstance(intent, SwapIntent):
            return None

        # Extract swap parameters
        token_in = intent.from_token
        token_out = intent.to_token

        # Get trade amount in USD
        if intent.amount_usd is not None:
            amount_usd = Decimal(str(intent.amount_usd))
        else:
            # amount is in tokens - need to convert to USD
            token_amount = (
                Decimal(str(intent.amount)) if intent.amount != "all" else portfolio.get_token_balance(token_in)
            )
            try:
                token_price = market_state.get_price(token_in)
                amount_usd = token_amount * (token_price if token_price else Decimal("1"))
            except KeyError:
                amount_usd = token_amount  # Assume stablecoin if price not found

        # Get token prices for execution
        try:
            price_in = market_state.get_price(token_in)
        except KeyError:
            price_in = Decimal("1")

        try:
            price_out = market_state.get_price(token_out)
        except KeyError:
            price_out = Decimal("1")

        if not price_in or price_in <= 0:
            price_in = Decimal("1")
        if not price_out or price_out <= 0:
            price_out = Decimal("1")

        # Calculate input amount in tokens
        amount_in_tokens = amount_usd / price_in

        # Check if this is a multi-hop swap (via route metadata)
        # Multi-hop routes are typically indicated in the protocol response or intent metadata
        route_hops: list[tuple[str, str, Decimal]] = []

        # Check for route info in intent - could be added by strategy for explicit arbitrage paths
        intent_metadata = getattr(intent, "metadata", {}) or {}
        if "route" in intent_metadata:
            route = intent_metadata["route"]
            for hop in route:
                hop_slippage = Decimal(str(hop.get("slippage", self._config.base_slippage_per_hop_pct)))
                route_hops.append((hop["token_in"], hop["token_out"], hop_slippage))
        else:
            # Single hop swap - create a 1-hop route
            route_hops = [(token_in, token_out, self._config.base_slippage_per_hop_pct)]

        # Calculate cumulative execution result with MEV
        execution_result = self.calculate_cumulative_slippage(
            hops=route_hops,
            initial_amount=amount_in_tokens,
            initial_amount_usd=amount_usd,
            market_state=market_state,
        )

        # Calculate final output amounts
        total_slippage_pct = execution_result.total_slippage_pct
        total_fees_pct = execution_result.total_fees_pct
        total_mev_cost_usd = execution_result.total_mev_cost_usd

        # Calculate expected and actual output
        # Use execution_result.final_amount which properly compounds slippage/fees per hop
        expected_amount_out = amount_in_tokens * (price_in / price_out)
        if execution_result.initial_amount > 0:
            effective_fill_ratio = execution_result.final_amount / execution_result.initial_amount
        else:
            effective_fill_ratio = Decimal("0")
        actual_amount_out = expected_amount_out * effective_fill_ratio

        # Calculate costs in USD
        fee_usd = amount_usd * total_fees_pct
        slippage_usd = amount_usd * total_slippage_pct

        # Apply max_slippage check from intent
        max_slippage = Decimal(str(intent.max_slippage))
        if total_slippage_pct > max_slippage:
            logger.warning(
                "Arbitrage swap slippage %.4f%% exceeds max_slippage %.4f%%",
                float(total_slippage_pct * 100),
                float(max_slippage * 100),
            )
            # Return failed fill if slippage too high
            return SimulatedFill(
                timestamp=market_state.timestamp,
                intent_type=IntentType.SWAP,
                protocol=intent.protocol or "arbitrage",
                tokens=[token_in, token_out],
                executed_price=price_out,
                amount_usd=amount_usd,
                fee_usd=Decimal("0"),
                slippage_usd=Decimal("0"),
                gas_cost_usd=Decimal("0"),
                tokens_in={},
                tokens_out={},
                success=False,
                metadata={
                    "failure_reason": "slippage_exceeded",
                    "actual_slippage_pct": str(total_slippage_pct),
                    "max_slippage_pct": str(max_slippage),
                    "num_hops": execution_result.num_hops,
                    "execution_model": execution_result.execution_model.value,
                },
            )

        # Log multi-step execution details
        if len(route_hops) > 1:
            logger.info(
                "Multi-hop arbitrage execution: %d hops, total_slippage=%.4f%%, fees=%.4f%%, MEV=$%.2f, delay=%.1fs",
                len(route_hops),
                float(total_slippage_pct * 100),
                float(total_fees_pct * 100),
                float(total_mev_cost_usd),
                execution_result.total_execution_delay_seconds,
            )
        else:
            logger.debug(
                "Single-hop arbitrage: %s->%s, slippage=%.4f%%, MEV=$%.2f",
                token_in,
                token_out,
                float(total_slippage_pct * 100),
                float(total_mev_cost_usd),
            )

        # Create successful fill with execution details
        return SimulatedFill(
            timestamp=market_state.timestamp,
            intent_type=IntentType.SWAP,
            protocol=intent.protocol or "arbitrage",
            tokens=[token_in, token_out],
            executed_price=price_out,
            amount_usd=amount_usd,
            fee_usd=fee_usd,
            slippage_usd=slippage_usd,
            gas_cost_usd=Decimal("0"),  # Gas handled separately by simulator
            tokens_in={token_in: amount_in_tokens},  # Tokens sent TO the pool
            tokens_out={token_out: actual_amount_out},  # Tokens received FROM the pool
            success=True,
            estimated_mev_cost_usd=total_mev_cost_usd if self._config.mev_simulation_enabled else None,
            metadata={
                "num_hops": execution_result.num_hops,
                "execution_model": execution_result.execution_model.value,
                "execution_delay_seconds": execution_result.total_execution_delay_seconds,
                "execution_steps": [step.to_dict() for step in execution_result.steps],
                "expected_amount_out": str(expected_amount_out),
                "actual_amount_out": str(actual_amount_out),
                "total_slippage_pct": str(total_slippage_pct),
                "total_fees_pct": str(total_fees_pct),
                "mev_enabled": self._config.mev_simulation_enabled,
            },
        )

    def update_position(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        elapsed_seconds: float,
        timestamp: datetime | None = None,
    ) -> None:
        """Update position state based on time passage.

        For arbitrage strategies, positions are typically short-lived
        (single block execution). This method primarily tracks spot
        positions created from arbitrage profits.

        Args:
            position: The position to update (modified in-place)
            market_state: Current market prices and data
            elapsed_seconds: Time elapsed since last update in seconds
            timestamp: Simulation timestamp for deterministic updates. If None,
                uses market_state.timestamp for reproducible backtests.

        Note:
            Arbitrage positions don't accrue fees or funding - they
            are simply valued at current market prices.
        """
        # Arbitrage creates spot positions, which don't have time-based updates
        # Just update the last_updated timestamp using simulation time
        # Prefer explicit timestamp > market_state.timestamp > datetime.now() (with warning)
        if timestamp is not None:
            update_time = timestamp
        elif hasattr(market_state, "timestamp") and market_state.timestamp is not None:
            update_time = market_state.timestamp
        else:
            if self._config.strict_reproducibility:
                msg = (
                    f"No simulation timestamp available for arbitrage position {position.position_id}. "
                    "In strict reproducibility mode, timestamp must be provided. "
                    "Either pass timestamp parameter or ensure market_state.timestamp is set."
                )
                raise ValueError(msg)
            logger.warning(
                "No simulation timestamp available for arbitrage position %s, "
                "falling back to datetime.now(). This breaks backtest reproducibility.",
                position.position_id,
            )
            update_time = datetime.now()
        position.last_updated = update_time

    def value_position(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
        timestamp: datetime | None = None,
    ) -> Decimal:
        """Calculate the current USD value of a position.

        For arbitrage strategies, positions are typically spot token
        holdings. The value is simply amount * price.

        Args:
            position: The position to value
            market_state: Current market prices and data
            timestamp: Simulation timestamp for deterministic valuation. If None,
                uses market_state.timestamp. Currently unused in arbitrage valuation
                but accepted for interface consistency.

        Returns:
            Total position value in USD as a Decimal
        """
        # Note: timestamp parameter accepted for interface consistency
        # Arbitrage valuation is based on current market prices, not time-dependent
        _ = timestamp
        total_value = Decimal("0")

        for token, amount in position.amounts.items():
            try:
                price = market_state.get_price(token)
                if price and price > 0:
                    total_value += amount * price
            except KeyError:
                # Token price not available, use entry price if available
                if position.entry_price and position.entry_price > 0:
                    total_value += amount * position.entry_price

        return total_value

    def should_rebalance(
        self,
        position: "SimulatedPosition",
        market_state: "MarketState",
    ) -> bool:
        """Determine if a position should be rebalanced.

        For arbitrage strategies, positions don't typically need
        rebalancing in the traditional sense. The strategy decides
        when to execute new arbitrage opportunities.

        Args:
            position: The position to check
            market_state: Current market prices and data

        Returns:
            Always False for arbitrage positions - the strategy handles
            when to execute arbitrages.
        """
        return False

    def calculate_cumulative_slippage(
        self,
        hops: list[tuple[str, str, Decimal]],
        initial_amount: Decimal,
        initial_amount_usd: Decimal | None = None,
        market_state: "MarketState | None" = None,
    ) -> ArbitrageExecutionResult:
        """Calculate cumulative slippage across multiple swap hops.

        This method simulates a multi-hop arbitrage execution, calculating
        the total slippage using either a multiplicative or additive model.

        Multiplicative model (more realistic):
            final_amount = initial * (1 - s1) * (1 - s2) * ... * (1 - sn)
            total_slippage = 1 - (1 - s1) * (1 - s2) * ... * (1 - sn)

        Additive model (simpler):
            total_slippage = s1 + s2 + ... + sn
            final_amount = initial * (1 - total_slippage)

        Args:
            hops: List of (token_in, token_out, slippage_pct) tuples for each hop.
                slippage_pct should be as a decimal (0.01 = 1%).
            initial_amount: Starting amount in the initial token
            initial_amount_usd: Starting amount in USD (for MEV calculation)
            market_state: Market state for price lookups (optional)

        Returns:
            ArbitrageExecutionResult with detailed breakdown of each step

        Example:
            result = adapter.calculate_cumulative_slippage(
                hops=[
                    ("USDC", "WETH", Decimal("0.003")),  # 0.3% slippage
                    ("WETH", "ARB", Decimal("0.005")),   # 0.5% slippage
                    ("ARB", "USDC", Decimal("0.004")),   # 0.4% slippage
                ],
                initial_amount=Decimal("10000"),
                initial_amount_usd=Decimal("10000"),
            )
        """
        if not hops or initial_amount <= 0:
            return ArbitrageExecutionResult(
                steps=[],
                total_slippage_pct=Decimal("0"),
                total_fees_pct=Decimal("0"),
                total_mev_cost_usd=Decimal("0"),
                total_execution_delay_seconds=0.0,
                initial_amount=initial_amount,
                final_amount=initial_amount,
                profit_loss_pct=Decimal("0"),
                execution_model=CumulativeSlippageModel(self._config.cumulative_slippage_model),
            )

        # Enforce max hops
        if len(hops) > self._config.max_hops:
            logger.warning(
                "Arbitrage has %d hops, exceeding max_hops=%d. Truncating.",
                len(hops),
                self._config.max_hops,
            )
            hops = hops[: self._config.max_hops]

        steps: list[ExecutionStep] = []
        current_amount = initial_amount
        total_fees_pct = Decimal("0")
        total_mev_cost_usd = Decimal("0")
        total_execution_delay = 0.0

        # Track cumulative slippage based on model
        if self._config.cumulative_slippage_model == "multiplicative":
            cumulative_retention = Decimal("1")
        else:
            cumulative_slippage = Decimal("0")

        for step_num, (token_in, token_out, slippage_pct) in enumerate(hops, 1):
            # Apply base slippage if none provided
            if slippage_pct <= 0:
                slippage_pct = self._config.base_slippage_per_hop_pct

            # Get fee for this hop
            fee_pct = self._config.base_fee_per_hop_pct

            # Simulate MEV cost if enabled
            mev_cost_usd = Decimal("0")
            if self._config.mev_simulation_enabled and initial_amount_usd:
                mev_simulator = self._get_mev_simulator()
                if mev_simulator:
                    # Estimate USD value of current step
                    step_pct = current_amount / initial_amount
                    step_usd = initial_amount_usd * step_pct

                    from almanak.framework.backtesting.models import IntentType

                    mev_result = mev_simulator.simulate_mev_cost(
                        trade_amount_usd=step_usd,
                        token_in=token_in,
                        token_out=token_out,
                        intent_type=IntentType.SWAP,
                    )
                    mev_cost_usd = mev_result.mev_cost_usd

                    # Add MEV-induced slippage to step slippage
                    if mev_result.additional_slippage_pct > 0:
                        slippage_pct = slippage_pct + mev_result.additional_slippage_pct

            # Calculate output amount after slippage and fees
            amount_after_slippage = current_amount * (Decimal("1") - slippage_pct)
            amount_after_fees = amount_after_slippage * (Decimal("1") - fee_pct)

            # Create execution step
            step = ExecutionStep(
                step_number=step_num,
                token_in=token_in,
                token_out=token_out,
                amount_in=current_amount,
                amount_out=amount_after_fees,
                slippage_pct=slippage_pct,
                fee_pct=fee_pct,
                mev_cost_usd=mev_cost_usd,
                execution_delay_seconds=self._config.execution_delay_seconds,
            )
            steps.append(step)

            # Update tracking
            current_amount = amount_after_fees
            total_fees_pct += fee_pct
            total_mev_cost_usd += mev_cost_usd
            total_execution_delay += self._config.execution_delay_seconds

            if self._config.cumulative_slippage_model == "multiplicative":
                cumulative_retention *= Decimal("1") - slippage_pct
            else:
                cumulative_slippage += slippage_pct

        # Calculate total slippage
        if self._config.cumulative_slippage_model == "multiplicative":
            total_slippage_pct = Decimal("1") - cumulative_retention
        else:
            total_slippage_pct = cumulative_slippage

        # Calculate profit/loss as percentage of initial
        profit_loss_pct = (current_amount - initial_amount) / initial_amount

        result = ArbitrageExecutionResult(
            steps=steps,
            total_slippage_pct=total_slippage_pct,
            total_fees_pct=total_fees_pct,
            total_mev_cost_usd=total_mev_cost_usd,
            total_execution_delay_seconds=total_execution_delay,
            initial_amount=initial_amount,
            final_amount=current_amount,
            profit_loss_pct=profit_loss_pct,
            execution_model=CumulativeSlippageModel(self._config.cumulative_slippage_model),
        )

        # Store in history
        self._execution_history.append(result)

        logger.debug(
            "Arbitrage execution: %d hops, slippage=%.4f%%, fees=%.4f%%, "
            "MEV=$%.2f, delay=%.1fs, initial=%s, final=%s, PnL=%.4f%%",
            len(steps),
            float(total_slippage_pct * 100),
            float(total_fees_pct * 100),
            float(total_mev_cost_usd),
            total_execution_delay,
            str(initial_amount),
            str(current_amount),
            float(profit_loss_pct * 100),
        )

        return result

    def simulate_mev_impact(
        self,
        trade_amount_usd: Decimal,
        token_in: str,
        token_out: str,
    ) -> tuple[Decimal, Decimal, bool]:
        """Simulate MEV impact for a single trade.

        This is a convenience method for simulating MEV on a single hop
        without executing a full multi-hop arbitrage.

        Args:
            trade_amount_usd: Trade size in USD
            token_in: Input token symbol
            token_out: Output token symbol

        Returns:
            Tuple of (mev_cost_usd, additional_slippage_pct, was_sandwiched)
        """
        if not self._config.mev_simulation_enabled:
            return Decimal("0"), Decimal("0"), False

        mev_simulator = self._get_mev_simulator()
        if mev_simulator is None:
            return Decimal("0"), Decimal("0"), False

        from almanak.framework.backtesting.models import IntentType

        result = mev_simulator.simulate_mev_cost(
            trade_amount_usd=trade_amount_usd,
            token_in=token_in,
            token_out=token_out,
            intent_type=IntentType.SWAP,
        )

        return (
            result.mev_cost_usd,
            result.additional_slippage_pct,
            result.is_sandwiched,
        )

    def clear_execution_history(self) -> None:
        """Clear the execution history."""
        self._execution_history.clear()

    def get_execution_stats(self) -> dict[str, Any]:
        """Get summary statistics for all executions.

        Returns:
            Dictionary with execution statistics
        """
        if not self._execution_history:
            return {
                "total_executions": 0,
                "profitable_executions": 0,
                "total_hops": 0,
                "avg_hops_per_execution": 0,
                "total_slippage_pct": "0",
                "total_mev_cost_usd": "0",
                "avg_slippage_per_execution_pct": "0",
                "avg_profit_loss_pct": "0",
            }

        total_executions = len(self._execution_history)
        profitable_executions = sum(1 for r in self._execution_history if r.is_profitable)
        total_hops = sum(r.num_hops for r in self._execution_history)
        total_slippage = sum(r.total_slippage_pct for r in self._execution_history)
        total_mev = sum(r.total_mev_cost_usd for r in self._execution_history)
        total_pnl = sum(r.profit_loss_pct for r in self._execution_history)

        return {
            "total_executions": total_executions,
            "profitable_executions": profitable_executions,
            "profitable_pct": f"{profitable_executions / total_executions * 100:.1f}%",
            "total_hops": total_hops,
            "avg_hops_per_execution": total_hops / total_executions,
            "total_slippage_pct": str(total_slippage),
            "total_mev_cost_usd": str(total_mev),
            "avg_slippage_per_execution_pct": str(total_slippage / total_executions),
            "avg_profit_loss_pct": str(total_pnl / total_executions),
        }

    def to_dict(self) -> dict[str, Any]:
        """Serialize the adapter configuration to a dictionary.

        Returns:
            Dictionary with adapter configuration
        """
        return {
            "adapter_name": self.adapter_name,
            "config": self._config.to_dict(),
            "execution_stats": self.get_execution_stats(),
        }


__all__ = [
    "ArbitrageBacktestAdapter",
    "ArbitrageBacktestConfig",
    "ArbitrageExecutionResult",
    "CumulativeSlippageModel",
    "ExecutionStep",
]
