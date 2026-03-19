"""
===============================================================================
TUTORIAL: Uniswap V3 LP Strategy - Dynamic Liquidity Position Management
===============================================================================

This is a tutorial strategy demonstrating how to manage Uniswap V3 concentrated
liquidity positions. It shows the basics of LP position management in DeFi.

WHAT THIS STRATEGY DOES:
------------------------
1. Opens a concentrated liquidity position on Uniswap V3
2. Monitors if the position is still in range
3. When out of range: Closes the position and re-opens centered on current price
4. Collects fees when closing positions

CONCENTRATED LIQUIDITY EXPLAINED:
---------------------------------
Uniswap V3 introduced "concentrated liquidity" which allows LPs to provide
liquidity within a specific price range rather than across all prices:

- Traditional AMM: Liquidity spread from $0 to infinity (very capital inefficient)
- Uniswap V3: Liquidity concentrated in a specific range (e.g., $3000-$4000)

Benefits:
- Higher capital efficiency (up to 4000x vs V2 for tight ranges)
- More fees earned per dollar of capital
- Better slippage for traders

Risks:
- Position goes "out of range" if price moves outside your range
- When out of range, you hold 100% of one token (no fees earned)
- Impermanent loss can be higher with tighter ranges

PRICE RANGE CALCULATION:
------------------------
We calculate the range as a percentage around current price:
- range_width_pct = 0.20 means 20% total width (±10% from current price)
- If ETH = $3400:
  - range_lower = $3400 * 0.90 = $3060
  - range_upper = $3400 * 1.10 = $3740

USAGE:
------
    # Run once to open a position (first run)
    almanak strat run -d uniswap_lp --once

    # Run continuously to monitor and rebalance
    almanak strat run -d uniswap_lp --interval 60

    # Test on Anvil (local fork)
    almanak strat run -d uniswap_lp --network anvil --once

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

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
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

# Logger for debugging and monitoring
logger = logging.getLogger(__name__)


# =============================================================================
# STRATEGY CONFIGURATION
# =============================================================================


@dataclass
class UniswapLPConfig:
    """Configuration for Uniswap V3 LP strategy.

    This dataclass defines all configurable parameters for the LP strategy.
    The CLI will automatically load values from config.json into this class.

    Attributes:
        pool: Pool identifier in format "TOKEN0/TOKEN1/FEE" (e.g., "WETH/USDC/500")
        range_width_pct: Total width of price range as decimal (0.20 = 20%)
        amount0: Amount of token0 to provide (e.g., "0.001" WETH)
        amount1: Amount of token1 to provide (e.g., "0.1" USDC)
        force_action: Force specific action for testing ("open", "close", or "")
        position_id: NFT ID of position to close (when force_action="close")
    """

    # Pool configuration
    pool: str = "WETH/USDC/500"
    range_width_pct: Decimal = Decimal("0.20")

    # Token amounts
    amount0: Decimal = Decimal("0.001")
    amount1: Decimal = Decimal("0.1")

    # Testing/override options
    force_action: str = ""
    position_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert the configuration to a dictionary for serialization."""
        return {
            "pool": self.pool,
            "range_width_pct": str(self.range_width_pct),
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "force_action": self.force_action,
            "position_id": self.position_id,
        }

    def update(self, **kwargs: Any) -> Any:
        """Update configuration values.

        Returns a simple result object for compatibility with StrategyBase.
        """

        @dataclass
        class UpdateResult:
            success: bool = True
            updated_fields: list = field(default_factory=list)

        updated = []
        for k, v in kwargs.items():
            if hasattr(self, k):
                setattr(self, k, v)
                updated.append(k)
        return UpdateResult(success=True, updated_fields=updated)


# =============================================================================
# STRATEGY METADATA (via decorator)
# =============================================================================
#
# The @almanak_strategy decorator registers your strategy and defines:
# - How it can be found and run (name)
# - What chains and protocols it supports
# - What types of actions it can take (intent_types)


@almanak_strategy(
    # Unique identifier - used to run via CLI
    name="demo_uniswap_lp",
    # Human-readable description
    description="Tutorial LP strategy - manages Uniswap V3 concentrated liquidity positions",
    # Semantic versioning
    version="1.0.0",
    # Author
    author="Almanak",
    # Tags for categorization
    tags=["demo", "tutorial", "lp", "liquidity", "uniswap-v3", "arbitrum"],
    # Supported blockchains
    supported_chains=["arbitrum", "ethereum", "base", "optimism"],
    # Protocols this strategy interacts with
    supported_protocols=["uniswap_v3"],
    # Types of intents this strategy may return
    # LP_OPEN: Create new liquidity position
    # LP_CLOSE: Close existing position and collect fees
    # HOLD: No action needed
    intent_types=["LP_OPEN", "LP_CLOSE", "HOLD"],
    default_chain="arbitrum",
)
class UniswapLPStrategy(IntentStrategy[UniswapLPConfig]):
    """
    A Uniswap V3 LP strategy for educational purposes.

    This strategy demonstrates:
    - How to open concentrated liquidity positions
    - How to calculate price ranges
    - How to detect when positions are out of range
    - How to close positions and collect fees
    - How to rebalance by re-opening centered positions

    Configuration Parameters (from config.json):
    --------------------------------------------
    - pool: Pool identifier (e.g., "WETH/USDC.e/500")
    - range_width_pct: Total width of price range (0.20 = 20%)
    - amount0: Amount of token0 to provide (e.g., "0.1" WETH)
    - amount1: Amount of token1 to provide (e.g., "340" USDC)
    - force_action: Force "open" or "close" for testing
    - position_id: NFT ID of position to close (for force_action="close")

    Example Config:
    ---------------
    {
        "pool": "WETH/USDC.e/500",
        "range_width_pct": 0.20,
        "amount0": "0.1",
        "amount1": "340",
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
        - self.config: UniswapLPConfig instance with pool, amounts, etc.
        - self.chain: Blockchain to operate on
        - self.wallet_address: Wallet for transactions
        """
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Read configuration from UniswapLPConfig
        # =====================================================================
        # The config is now a proper dataclass with typed fields

        # Pool configuration
        # Format: "TOKEN0/TOKEN1/FEE" where FEE is in hundredths of a basis point
        # Common fee tiers: 100 (0.01%), 500 (0.05%), 3000 (0.3%), 10000 (1%)
        self.pool = self.config.pool

        # Parse pool to extract token symbols
        pool_parts = self.pool.split("/")
        self.token0_symbol = pool_parts[0] if len(pool_parts) > 0 else "WETH"
        self.token1_symbol = pool_parts[1] if len(pool_parts) > 1 else "USDC"
        self.fee_tier = int(pool_parts[2]) if len(pool_parts) > 2 else 500

        # Range width as percentage
        # 0.20 = 20% total width = ±10% from current price
        self.range_width_pct = Decimal(str(self.config.range_width_pct))

        # Token amounts to provide (ensure Decimal for arithmetic)
        self.amount0 = Decimal(str(self.config.amount0))  # Token0 (e.g., WETH)
        self.amount1 = Decimal(str(self.config.amount1))  # Token1 (e.g., USDC)

        # Force action for testing ("open" or "close")
        self.force_action = str(self.config.force_action).lower()

        # Position ID for closing
        self.position_id = self.config.position_id

        # Internal state
        self._current_position_id: str | None = None
        
        # Load position ID from persistent state if available
        self._load_position_from_state()

        logger.info(
            f"UniswapLPStrategy initialized: "
            f"pool={self.pool}, "
            f"range_width={self.range_width_pct * 100}%, "
            f"amounts={self.amount0} {self.token0_symbol} + {self.amount1} {self.token1_symbol}"
            + (f", position_id={self._current_position_id}" if self._current_position_id else "")
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make an LP decision based on market conditions.

        This method is called by the framework on each iteration with fresh
        market data.

        Decision Flow:
        --------------
        1. If force_action is set, execute that action
        2. If no position exists, open one
        3. If position is out of range, close and re-open
        4. Otherwise, hold

        Parameters:
            market: MarketSnapshot containing prices, balances, etc.

        Returns:
            Intent: LP_OPEN, LP_CLOSE, or HOLD
        """
        # =================================================================
        # STEP 1: Handle forced close before price lookup
        # =================================================================
        # LP_CLOSE does not need current_price; guarding here lets force-close
        # work even when the price feed is unavailable (e.g. Anvil smoke tests).

        if self.force_action == "close":
            position_id = self.position_id or self._current_position_id
            if not position_id:
                logger.warning("force_action=close but no position_id tracked")
                return Intent.hold(reason="Close requested but no position_id")
            logger.info(f"Forced action: CLOSE LP position {position_id}")
            return self._create_close_intent(position_id)

        # =================================================================
        # STEP 2: Get current market price
        # =================================================================
        # Price is expressed as token1 per token0
        # For WETH/USDC: price = USDC per WETH (e.g., 3400)

        try:
            token0_price_usd = market.price(self.token0_symbol)
            token1_price_usd = market.price(self.token1_symbol)
            if token0_price_usd == Decimal("0") or token1_price_usd == Decimal("0"):
                zero_token = self.token0_symbol if token0_price_usd == Decimal("0") else self.token1_symbol
                logger.warning(f"Token price is zero for {zero_token}")
                return Intent.hold(reason=f"Price data unavailable: {zero_token} price is zero")
            current_price = token0_price_usd / token1_price_usd
            logger.debug(f"Current price: {current_price:.2f} {self.token1_symbol}/{self.token0_symbol}")
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get price: {e}")
            return Intent.hold(reason=f"Price data unavailable: {e}")

        # =================================================================
        # STEP 3: Handle forced open (needs price)
        # =================================================================

        if self.force_action == "open":
            logger.info("Forced action: OPEN LP position")
            return self._create_open_intent(current_price)

        # =================================================================
        # STEP 4: Check current position status
        # =================================================================
        # In a real strategy, we would:
        # 1. Query the NFT position manager for our positions
        # 2. Check if current price is within position's range
        # 3. Decide whether to rebalance
        #
        # For this demo, we use simplified logic:

        # If we have a tracked position, check if it needs rebalancing
        if self._current_position_id:
            # In production, you would query the position's tick range
            # and compare to current price
            # Here we just hold and wait
            return Intent.hold(reason=f"Position {self._current_position_id} exists - monitoring")

        # =================================================================
        # STEP 5: No position - decide whether to open one
        # =================================================================

        # Check we have sufficient balance
        try:
            token0_balance_result = market.balance(self.token0_symbol)
            token1_balance_result = market.balance(self.token1_symbol)
            
            # Handle both TokenBalance and Decimal return types
            if hasattr(token0_balance_result, 'balance'):
                # TokenBalance object - extract the balance Decimal
                token0_balance = token0_balance_result.balance
                token1_balance = token1_balance_result.balance
            else:
                # Already a Decimal
                token0_balance = token0_balance_result
                token1_balance = token1_balance_result

            if token0_balance < self.amount0:
                return Intent.hold(
                    reason=f"Insufficient {self.token0_symbol}: {token0_balance} < {self.amount0}"
                )
            if token1_balance < self.amount1:
                return Intent.hold(
                    reason=f"Insufficient {self.token1_symbol}: {token1_balance} < {self.amount1}"
                )
        except (ValueError, KeyError):
            # Balance check failed, but we can still try to open
            logger.warning("Could not verify balances, proceeding anyway")

        # Open new position centered on current price
        logger.info("No position found - opening new LP position")
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description="No position found - opening new LP position",
                strategy_id=self.strategy_id,
                details={"action": "opening_new_position"},
            )
        )
        return self._create_open_intent(current_price)

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_open_intent(self, current_price: Decimal) -> Intent:
        """
        Create an LP_OPEN intent to open a new position.

        Calculates the price range centered on the current price using
        the configured range_width_pct.

        Parameters:
            current_price: Current price (token1 per token0)

        Returns:
            LPOpenIntent ready for compilation
        """
        # Calculate price range
        # range_width_pct = 0.20 means ±10% from current price
        half_width = self.range_width_pct / Decimal("2")
        range_lower = current_price * (Decimal("1") - half_width)
        range_upper = current_price * (Decimal("1") + half_width)

        logger.info(
            f"💧 LP_OPEN: {format_token_amount_human(self.amount0, self.token0_symbol)} + "
            f"{format_token_amount_human(self.amount1, self.token1_symbol)}, "
            f"range [{format_usd(range_lower)} - {format_usd(range_upper)}]"
        )

        return Intent.lp_open(
            pool=self.pool,
            amount0=self.amount0,
            amount1=self.amount1,
            range_lower=range_lower,
            range_upper=range_upper,
            protocol="uniswap_v3",
        )

    def _create_close_intent(self, position_id: str) -> Intent:
        """
        Create an LP_CLOSE intent to close an existing position.

        Closing a position:
        1. Removes all liquidity from the position
        2. Collects any accumulated fees
        3. Returns tokens to the wallet

        Parameters:
            position_id: NFT token ID of the position to close

        Returns:
            LPCloseIntent ready for compilation
        """
        logger.info(f"💧 LP_CLOSE: position={position_id}")

        return Intent.lp_close(
            position_id=position_id,
            pool=self.pool,
            collect_fees=True,  # Always collect fees when closing
            protocol="uniswap_v3",
        )

    # =========================================================================
    # OPTIONAL: LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """
        Called after an intent is executed.

        This hook allows you to:
        - Track position IDs after opening
        - Update internal state
        - Log execution results

        Parameters:
            intent: The intent that was executed
            success: Whether execution succeeded
            result: Execution result with enriched data (position_id, swap_amounts, etc.)
        """
        if success and intent.intent_type.value == "LP_OPEN":
            # Result Enrichment: position_id is automatically extracted by the framework
            # No manual parsing needed - data is attached directly to the result
            position_id = result.position_id if result else None

            if position_id:
                self._current_position_id = str(position_id)
                logger.info(f"LP position opened successfully: position_id={position_id}")

                # Save to state so it persists across runs
                # Note: Don't call save_state() here - runner saves automatically after callback
                self._save_position_to_state(position_id)
            else:
                logger.warning("LP position opened but could not extract position ID from receipt")

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.LP_OPEN,
                    description=f"LP position opened on {self.pool}" + (f" (ID: {position_id})" if position_id else ""),
                    strategy_id=self.strategy_id,
                    details={"pool": self.pool, "position_id": str(position_id) if position_id else None},
                )
            )
    
    def _load_position_from_state(self) -> None:
        """Load position ID from persistent state if available."""
        state = self.get_persistent_state()
        if state and "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])
            logger.info(f"Loaded position ID from state: {self._current_position_id}")
    def _save_position_to_state(self, position_id: int) -> None:
        """Save position ID to strategy state so it persists across runs.
        
        Args:
            position_id: Position NFT token ID
        """
        # Position ID will be saved via get_persistent_state() override
        # This method just updates the in-memory value
        self._current_position_id = str(position_id)
        logger.info(f"Updated position ID: {position_id}")
    
    def get_persistent_state(self) -> dict[str, Any]:
        """Get persistent state including position ID.
        
        Returns:
            Dictionary with strategy state including position tracking
        """
        state = super().get_persistent_state() if hasattr(super(), "get_persistent_state") else {}
        
        # Add position tracking to state
        if self._current_position_id:
            state["current_position_id"] = self._current_position_id
            # Keep position_opened_at if it exists, otherwise set it now
            if "position_opened_at" not in state:
                state["position_opened_at"] = datetime.now(UTC).isoformat()
        
        return state
    
    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persistent state including position ID.
        
        Args:
            state: Dictionary with strategy state
        """
        super().load_persistent_state(state) if hasattr(super(), "load_persistent_state") else None
        
        # Load position ID from state
        if "current_position_id" in state:
            self._current_position_id = str(state["current_position_id"])
            logger.info(f"Restored position ID from state: {self._current_position_id}")

    # =========================================================================
    # OPTIONAL: STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """
        Get current strategy status for monitoring/dashboards.

        Returns:
            Dictionary with strategy status information
        """
        return {
            "strategy": "demo_uniswap_lp",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "pool": self.pool,
                "range_width_pct": str(self.range_width_pct),
                "amount0": str(self.amount0),
                "amount1": str(self.amount1),
            },
            "state": {
                "current_position_id": self._current_position_id,
            },
        }

    # =========================================================================
    # TEARDOWN SUPPORT
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        """
        Get summary of open LP positions for teardown preview.

        Returns:
            TeardownPositionSummary with LP position details
        """
        from datetime import datetime

        from almanak.framework.teardown import (
            PositionInfo,
            PositionType,
            TeardownPositionSummary,
        )

        positions: list[PositionInfo] = []

        # Check if we have a tracked position
        position_id = self._current_position_id or self.position_id

        if position_id:
            # Calculate estimated value using live prices
            try:
                snapshot = self.create_market_snapshot()
                token0_price_usd = snapshot.price(self.token0_symbol)
                token1_price_usd = snapshot.price(self.token1_symbol)
            except Exception:  # noqa: BLE001
                logger.debug("Could not get live prices for LP value estimate, using fallback $0")
                token0_price_usd = Decimal("0")
                token1_price_usd = Decimal("0")

            estimated_value = self.amount0 * token0_price_usd + self.amount1 * token1_price_usd

            positions.append(
                PositionInfo(
                    position_type=PositionType.LP,
                    position_id=str(position_id),
                    chain=self.chain,
                    protocol="uniswap_v3",
                    value_usd=estimated_value,
                    details={
                        "pool": self.pool,
                        "fee_tier": self.fee_tier,
                        "amount0": str(self.amount0),
                        "amount1": str(self.amount1),
                        "token0": self.token0_symbol,
                        "token1": self.token1_symbol,
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "demo_uniswap_lp"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """
        Generate intents to close all LP positions.

        For Uniswap LP, teardown is straightforward:
        1. If position exists in state, generate LP_CLOSE intent
        2. Compiler handles non-existent/already-closed positions gracefully

        Args:
            mode: TeardownMode (SOFT or HARD) - affects urgency
            market: Optional market snapshot (unused for LP close)

        Returns:
            List of LP_CLOSE intents (empty if no position in state)
        """
        intents: list[Intent] = []

        position_id = self._current_position_id or self.position_id
        if not position_id:
            return intents

        logger.info(f"Generating teardown intent for LP position {position_id} (mode={mode.value})")

        intents.append(
            Intent.lp_close(
                position_id=position_id,
                pool=self.pool,
                collect_fees=True,
                protocol="uniswap_v3",
            )
        )

        return intents

    def on_teardown_started(self, mode: "TeardownMode") -> None:
        """
        Called when teardown begins.

        Args:
            mode: The teardown mode being used
        """
        from almanak.framework.teardown import TeardownMode

        position_id = self._current_position_id or self.position_id
        mode_name = "Graceful Shutdown" if mode == TeardownMode.SOFT else "Safe Emergency Exit"
        logger.info(f"[TEARDOWN] Starting {mode_name} for Uniswap LP strategy. Position: {position_id or 'None'}")

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        """
        Called when teardown completes.

        Args:
            success: Whether teardown completed successfully
            recovered_usd: Amount recovered in USD
        """
        if success:
            logger.info(f"[TEARDOWN] Uniswap LP teardown completed successfully. Recovered: ${recovered_usd:,.2f}")
            # Clear position tracking
            self._current_position_id = None
        else:
            logger.warning(f"[TEARDOWN] Uniswap LP teardown failed. Partial recovery: ${recovered_usd:,.2f}")


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("UniswapLPStrategy - Demo Strategy")
    print("=" * 60)
    print(f"\nStrategy Name: {UniswapLPStrategy.STRATEGY_NAME}")
    print(f"Version: {UniswapLPStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {UniswapLPStrategy.STRATEGY_METADATA.supported_chains}")
    print(f"Supported Protocols: {UniswapLPStrategy.STRATEGY_METADATA.supported_protocols}")
    print(f"Intent Types: {UniswapLPStrategy.STRATEGY_METADATA.intent_types}")
    print(f"\nDescription: {UniswapLPStrategy.STRATEGY_METADATA.description}")
    print("\nTo run this strategy:")
    print("  almanak strat run -d uniswap_lp --once")
    print("\nTo test on Anvil:")
    print("  almanak strat run -d uniswap_lp --network anvil --once")
