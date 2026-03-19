"""
===============================================================================
TUTORIAL: Aerodrome LP Strategy - Solidly-Based AMM on Base
===============================================================================

This is a tutorial strategy demonstrating how to manage Aerodrome liquidity
positions on Base chain. It shows the basics of Solidly-fork LP management.

WHAT THIS STRATEGY DOES:
------------------------
1. Opens a liquidity position on Aerodrome (volatile or stable pool)
2. Provides liquidity in a 50/50 ratio for fungible LP tokens
3. Monitors the position value
4. Can close positions and withdraw liquidity

AERODROME EXPLAINED:
--------------------
Aerodrome is a Solidly-based AMM on Base with two pool types:

- Volatile Pools: x*y=k formula (0.3% fee) - For uncorrelated assets
- Stable Pools: x^3*y + y^3*x formula (0.05% fee) - For correlated assets

Key Concepts:
- Fungible LP Tokens: Unlike Uniswap V3, LP tokens are ERC-20 (like V2)
- Pool Types: Must specify `stable=True/False` for all operations
- No concentrated liquidity: All liquidity spread across full range
- Voting escrow (veAERO): Gauge voting for emissions (not covered here)

Benefits:
- Simple LP token model (no NFTs)
- Lower fees for stable pairs
- Deep liquidity from Optimism incentives
- Simple position management

USAGE:
------
    # Test on Anvil (local Base fork)
    almanak strat run -d aerodrome_lp --network anvil --once

    # Run once to open a position
    almanak strat run -d aerodrome_lp --once

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

# Timeline API for logging
from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event

# Intent is what your strategy returns - describes what action to take
from almanak.framework.intents import Intent

# Core strategy framework imports
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

# Logging utilities for user-friendly output
from almanak.framework.utils.log_formatters import format_token_amount_human

# Logger for debugging and monitoring
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


# =============================================================================
# CONFIGURATION CLASS
# =============================================================================


@dataclass
class AerodromeLPConfig:
    """Configuration for Aerodrome LP strategy.

    This dataclass properly loads all config fields from JSON.
    """

    pool: str = "WETH/USDC"
    stable: bool = False
    amount0: Decimal = field(default_factory=lambda: Decimal("0.001"))
    amount1: Decimal = field(default_factory=lambda: Decimal("3"))
    force_action: str = ""

    def __post_init__(self):
        """Convert string values to proper types."""
        if isinstance(self.amount0, str):
            self.amount0 = Decimal(self.amount0)
        if isinstance(self.amount1, str):
            self.amount1 = Decimal(self.amount1)
        if isinstance(self.stable, str):
            self.stable = self.stable.lower() in ("true", "1", "yes")

    def to_dict(self) -> dict:
        """Convert config to dictionary for serialization."""
        return {
            "pool": self.pool,
            "stable": self.stable,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "force_action": self.force_action,
        }


# =============================================================================
# STRATEGY METADATA (via decorator)
# =============================================================================


@almanak_strategy(
    # Unique identifier - used to run via CLI
    name="demo_aerodrome_lp",
    # Human-readable description
    description="Tutorial LP strategy - manages Aerodrome liquidity positions on Base",
    # Semantic versioning
    version="1.0.0",
    # Author
    author="Almanak",
    # Tags for categorization
    tags=["demo", "tutorial", "lp", "liquidity", "aerodrome", "base", "solidly"],
    # Supported blockchains (Aerodrome is only on Base)
    supported_chains=["base"],
    # Protocols this strategy interacts with
    supported_protocols=["aerodrome"],
    # Types of intents this strategy may return
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="base",
)
class AerodromeLPStrategy(IntentStrategy[AerodromeLPConfig]):
    """
    An Aerodrome LP strategy for educational purposes.

    This strategy demonstrates:
    - How to open Aerodrome liquidity positions
    - How to choose between volatile and stable pools
    - How to provide liquidity in a 50/50 ratio
    - How to close positions and collect tokens

    Configuration Parameters (from config.json):
    --------------------------------------------
    - pool: Pool identifier (e.g., "WETH/USDC")
    - stable: Pool type (True=stable, False=volatile)
    - amount0: Amount of token0 to provide (e.g., "0.001" WETH)
    - amount1: Amount of token1 to provide (e.g., "3" USDC)
    - force_action: Force "open" or "close" for testing

    Example Config:
    ---------------
    {
        "pool": "WETH/USDC",
        "stable": false,
        "amount0": "0.001",
        "amount1": "3",
        "force_action": "open"
    }
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """
        Initialize the LP strategy with configuration.

        The base class handles standard parameters:
        - self.config: Strategy configuration (AerodromeLPConfig)
        - self.chain: Blockchain to operate on
        - self.wallet_address: Wallet for transactions
        """
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Extract configuration from AerodromeLPConfig
        # =====================================================================

        # Pool configuration - Format: "TOKEN0/TOKEN1"
        self.pool = self.config.pool

        # Parse pool to extract token symbols
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"

        # Pool type: stable (correlated assets) or volatile (uncorrelated)
        self.stable = self.config.stable

        # Token amounts to provide
        self.amount0 = self.config.amount0  # Token0 (e.g., WETH)
        self.amount1 = self.config.amount1  # Token1 (e.g., USDC)

        # Force action for testing ("open" or "close")
        self.force_action = self.config.force_action.lower() if self.config.force_action else ""

        # Internal state - track if we have an LP position
        self._has_position: bool = False
        self._lp_token_balance: Decimal = Decimal("0")

        pool_type = "stable" if self.stable else "volatile"
        logger.info(
            f"AerodromeLPStrategy initialized: "
            f"pool={self.pool}, "
            f"type={pool_type}, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
        )

    def _has_tracked_position(self) -> bool:
        """State-first position marker used for teardown gating."""
        return self._has_position or self._lp_token_balance > 0

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make an LP decision based on market conditions.

        Decision Flow:
        --------------
        1. If force_action is set, execute that action
        2. If no position exists, open one
        3. If position exists, hold and monitor
        4. Otherwise, hold

        Parameters:
            market: MarketSnapshot containing prices, balances, etc.

        Returns:
            Intent: LP_OPEN, LP_CLOSE, or HOLD
        """
        # =================================================================
        # STEP 1: Get current market price
        # =================================================================

        try:
            token0_price_usd = market.price(self.token0_symbol)
            token1_price_usd = market.price(self.token1_symbol)
            current_price = token0_price_usd / token1_price_usd
            logger.debug(f"Current price: {current_price:.4f} {self.token1_symbol}/{self.token0_symbol}")
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get price: {e}")
            # Use a reasonable default for ETH/USDC testing
            current_price = Decimal("3000")  # ~$3000 per ETH

        # =================================================================
        # STEP 2: Handle forced actions (for testing)
        # =================================================================

        if self.force_action == "open":
            logger.info("Forced action: OPEN LP position")
            return self._create_open_intent()

        elif self.force_action == "close":
            # Note: We don't check _has_position here because:
            # 1. Position state is in-memory and lost on restart
            # 2. User explicitly wants to close - trust them
            # 3. The removeLiquidity call will fail gracefully if no position exists
            logger.info("Forced action: CLOSE LP position")
            return self._create_close_intent()

        # =================================================================
        # STEP 3: Check current position status
        # =================================================================

        if self._has_tracked_position():
            # We have a position - monitor it
            return Intent.hold(reason=f"Position exists in {self.pool} pool - monitoring")

        # =================================================================
        # STEP 4: No position - decide whether to open one
        # =================================================================

        # Check we have sufficient balance
        try:
            token0_bal = market.balance(self.token0_symbol)
            token1_bal = market.balance(self.token1_symbol)

            # TokenBalance has .balance attribute with the actual Decimal value
            if token0_bal.balance < self.amount0:
                return Intent.hold(
                    reason=f"Insufficient {self.token0_symbol}: {token0_bal.balance} < {self.amount0}"
                )
            if token1_bal.balance < self.amount1:
                return Intent.hold(
                    reason=f"Insufficient {self.token1_symbol}: {token1_bal.balance} < {self.amount1}"
                )
        except (ValueError, KeyError, AttributeError):
            logger.warning("Could not verify balances, proceeding anyway")

        # Open new position
        logger.info("No position found - opening new LP position")
        return self._create_open_intent()

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_open_intent(self) -> Intent:
        """
        Create an LP_OPEN intent to open a new Aerodrome position.

        For Aerodrome (Solidly-based):
        - Liquidity is spread across full price range
        - No tick/bin selection needed (unlike V3)
        - Pool type (stable/volatile) is encoded in pool string

        Pool format: "TOKEN0/TOKEN1/stable" or "TOKEN0/TOKEN1/volatile"

        Returns:
            LPOpenIntent ready for compilation
        """
        pool_type = "stable" if self.stable else "volatile"
        logger.info(
            f"💧 LP_OPEN: {format_token_amount_human(self.amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.amount1, self.token1_symbol)}, "
            f"pool_type={pool_type}"
        )

        # Encode pool type in pool string: "WETH/USDC/volatile" or "WETH/USDC/stable"
        pool_with_type = f"{self.pool}/{pool_type}"

        # Use LP_OPEN intent with aerodrome protocol
        # Range values are required by Intent but not used by Aerodrome (full range)
        # Using dummy values that pass validation
        return Intent.lp_open(
            pool=pool_with_type,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=Decimal("1"),  # Dummy - Aerodrome uses full range
            range_upper=Decimal("1000000"),  # Dummy - Aerodrome uses full range
            protocol="aerodrome",
        )

    def _create_close_intent(self) -> Intent:
        """
        Create an LP_CLOSE intent to close the existing position.

        For Aerodrome, closing a position:
        1. Removes all LP tokens from the pool
        2. Returns both tokens to the wallet
        3. Collects any accumulated fees

        Pool format: "TOKEN0/TOKEN1/stable" or "TOKEN0/TOKEN1/volatile"

        Returns:
            LPCloseIntent ready for compilation
        """
        pool_type = "stable" if self.stable else "volatile"
        pool_with_type = f"{self.pool}/{pool_type}"

        logger.info(f"💧 LP_CLOSE: {pool_with_type}")

        return Intent.lp_close(
            position_id=pool_with_type,  # Use pool with type as identifier
            pool=pool_with_type,
            collect_fees=True,
            protocol="aerodrome",
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """
        Called after an intent is executed.

        Parameters:
            intent: The intent that was executed
            success: Whether execution succeeded
            result: Execution result
        """
        if success and intent.intent_type.value == "LP_OPEN":
            logger.info("Aerodrome LP position opened successfully")
            self._has_position = True
            if self._lp_token_balance <= 0:
                self._lp_token_balance = Decimal("1")
            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_OPENED,
                    description=f"Aerodrome LP position opened on {self.pool}",
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool, "stable": self.stable},
                )
            )

        elif success and intent.intent_type.value == "LP_CLOSE":
            logger.info("Aerodrome LP position closed successfully")
            self._has_position = False
            self._lp_token_balance = Decimal("0")

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        """Persist minimal LP state so teardown can recover after process restart."""
        parent_get_state = getattr(super(), "get_persistent_state", None)
        state = parent_get_state() if callable(parent_get_state) else {}
        state["has_position"] = self._has_tracked_position()
        state["lp_token_balance"] = str(self._lp_token_balance)
        return state

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore persisted LP state."""
        parent_load_state = getattr(super(), "load_persistent_state", None)
        if callable(parent_load_state):
            parent_load_state(state)

        raw_has_position = state.get("has_position", False)
        if isinstance(raw_has_position, str):
            self._has_position = raw_has_position.strip().lower() in {"1", "true", "yes", "on"}
        else:
            self._has_position = bool(raw_has_position)

        raw_lp_balance = state.get("lp_token_balance", "0")
        try:
            self._lp_token_balance = Decimal(str(raw_lp_balance))
        except Exception:
            logger.warning("Invalid persisted lp_token_balance=%r; defaulting to 0", raw_lp_balance)
            self._lp_token_balance = Decimal("0")
        if self._lp_token_balance > 0:
            self._has_position = True

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """
        Get current strategy status for monitoring/dashboards.

        Returns:
            Dictionary with strategy status information
        """
        return {
            "strategy": "demo_aerodrome_lp",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pool": self.pool,
                "stable": self.stable,
                "amount0": str(self.amount0),
                "amount1": str(self.amount1),
            },
            "state": {
                "has_position": self._has_position,
                "lp_token_balance": str(self._lp_token_balance),
            },
        }

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get summary of open LP positions for teardown preview."""
        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        if self._has_tracked_position():
            # Calculate estimated value
            token0_price_usd = Decimal("3000")  # Default ETH price
            token1_price_usd = Decimal("1")  # Default USDC price

            estimated_value = self.amount0 * token0_price_usd + self.amount1 * token1_price_usd

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=f"aerodrome-lp-{self.pool}-{self.chain}",
                    chain=self.chain,
                    protocol="aerodrome",
                    value_usd=estimated_value,
                    details={
                        "asset": f"{self.token0_symbol}/{self.token1_symbol}",
                        "size": str(self._lp_token_balance or Decimal("1")),
                        "pool": self.pool,
                        "stable": self.stable,
                        "amount0": str(self.amount0),
                        "amount1": str(self.amount1),
                    },
                )
            )

        total_value = sum(p.value_usd for p in positions)

        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            total_value_usd=total_value,
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to close all LP positions."""

        intents: list[Intent] = []

        if self._has_tracked_position():
            pool_type = "stable" if self.stable else "volatile"
            pool_with_type = f"{self.pool}/{pool_type}"

            logger.info(f"Generating teardown intent for Aerodrome LP position (mode={mode.value})")

            intents.append(
                Intent.lp_close(
                    position_id=pool_with_type,
                    pool=pool_with_type,
                    collect_fees=True,
                    protocol="aerodrome",
                )
            )

        return intents


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("AerodromeLPStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {AerodromeLPStrategy.STRATEGY_NAME}")
    print(f"Version: {AerodromeLPStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {AerodromeLPStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {AerodromeLPStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {AerodromeLPStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {AerodromeLPStrategy.STRATEGY_METADATA.description}")
    print("\nTo test on Anvil:")
    print("  almanak strat run -d aerodrome_lp --network anvil --once")
