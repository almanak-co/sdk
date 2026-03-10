"""
===============================================================================
TUTORIAL: Morpho Blue Looping Strategy - Leveraged Yield Farming
===============================================================================

This tutorial strategy demonstrates how to create a leveraged yield position
using Morpho Blue's isolated lending markets.

WHAT IS LOOPING?
----------------
Looping (also called "recursive borrowing" or "leverage farming") is a DeFi
strategy that amplifies yield by repeatedly:
1. Supply collateral to earn yield
2. Borrow against it
3. Swap borrowed tokens back to collateral
4. Re-supply to increase exposure

Example with 3x leverage on wstETH:
- Start: 1 wstETH (~$3,400)
- Loop 1: Supply 1 wstETH, borrow $2,900 USDC, swap to 0.85 wstETH
- Loop 2: Supply 0.85 wstETH, borrow $2,465 USDC, swap to 0.72 wstETH
- Loop 3: Supply 0.72 wstETH, borrow $2,095 USDC, swap to 0.61 wstETH
- Final: Total ~3.18 wstETH exposure from 1 wstETH initial capital

This amplifies both gains AND losses:
- If wstETH goes up 10%: Your position gains ~30% (3x leverage)
- If wstETH goes down 10%: Your position loses ~30% (3x leverage)

WHY MORPHO BLUE?
----------------
Morpho Blue is ideal for looping because:
1. Isolated Markets: Each market has its own risk parameters
2. High LLTV: Markets like wstETH/USDC have 86% LLTV (allows more leverage)
3. No aTokens: Direct collateral tracking simplifies logic
4. Lower Fees: Morpho's unique design often has lower effective rates

RISKS:
------
- LIQUIDATION: The #1 risk. If collateral value drops, you get liquidated.
- HEALTH FACTOR: Must stay above 1.0 (this strategy maintains >1.5)
- CASCADING LIQUIDATIONS: High leverage = high liquidation risk
- SLIPPAGE: Each swap incurs slippage, reducing effective leverage
- GAS COSTS: Multiple transactions = higher gas costs
- SMART CONTRACT RISK: Both Morpho and swap protocol risks

HEALTH FACTOR EXPLAINED:
------------------------
Health Factor = (Collateral Value * LLTV) / Borrow Value

- HF > 1.0: Safe
- HF = 1.0: Liquidatable (partial or full liquidation)
- HF < 1.0: Being actively liquidated

For safety with leverage, maintain HF > 1.5 (this strategy targets 1.8)

USAGE:
------
    # Run once (execute one loop iteration)
    python -m almanak.cli.run --strategy demo_morpho_looping --once

    # Run continuously (monitor and rebalance)
    python -m almanak.cli.run --strategy demo_morpho_looping

    # Test on Anvil
    python strategies/demo/morpho_looping/run_anvil.py

===============================================================================
"""

# =============================================================================
# IMPORTS
# =============================================================================

import logging
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

# Logger for debugging
logger = logging.getLogger(__name__)


# =============================================================================
# STRATEGY METADATA
# =============================================================================


@almanak_strategy(
    # Unique identifier for CLI
    name="demo_morpho_looping",
    # Description
    description="Tutorial strategy - leveraged yield farming via recursive borrowing on Morpho Blue",
    # Version
    version="1.0.0",
    # Author
    author="Almanak",
    # Tags
    tags=["demo", "tutorial", "lending", "leverage", "looping", "morpho", "yield"],
    # Supported chains (Morpho Blue is on Ethereum and Base)
    supported_chains=["ethereum", "base"],
    # Protocols used
    supported_protocols=["morpho_blue", "uniswap_v3"],
    # Intent types this strategy may emit
    intent_types=["SUPPLY", "BORROW", "SWAP", "REPAY", "WITHDRAW", "HOLD"],
    default_chain="ethereum",
)
class MorphoLoopingStrategy(IntentStrategy):
    """
    Morpho Blue looping strategy for educational purposes.

    This strategy demonstrates:
    - How to create leveraged positions via recursive borrowing
    - How to manage health factor and liquidation risk
    - How to unwind leveraged positions safely

    Configuration Parameters (from config.json):
    --------------------------------------------
    - market_id: Morpho Blue market ID (required)
    - collateral_token: Token to use as collateral (e.g., "wstETH")
    - borrow_token: Token to borrow (e.g., "USDC")
    - initial_collateral: Initial collateral amount (default: "1.0")
    - target_loops: Number of loops to execute (default: 3)
    - target_ltv: Target LTV per loop (default: 0.75 = 75%)
    - min_health_factor: Minimum health factor to maintain (default: 1.5)
    - swap_slippage: Slippage tolerance for swaps (default: 0.005 = 0.5%)

    Example Config:
    ---------------
    {
        "market_id": "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
        "collateral_token": "wstETH",
        "borrow_token": "USDC",
        "initial_collateral": "1.0",
        "target_loops": 3,
        "target_ltv": 0.75,
        "min_health_factor": 1.5,
        "swap_slippage": 0.005
    }

    Running Notes:
    --------------
    This is a multi-phase strategy with an internal state machine
    (idle -> supplying -> borrowing -> swapping -> ... -> complete).
    Each call to decide() advances the state by one step.

    - Use ``--interval`` mode (not ``--once``) to complete the full looping
      lifecycle, since each iteration only advances one state transition.
    - Use ``--fresh`` flag when testing on Anvil to clear stale state from
      previous runs. Stale state causes the strategy to resume mid-loop on
      a fresh fork where no on-chain positions exist.

    Example::

        # Full lifecycle on Anvil (clears stale state, runs continuously)
        almanak strat run -d strategies/demo/morpho_looping --fresh --interval 15 --network anvil

        # Single step for debugging
        almanak strat run -d strategies/demo/morpho_looping --fresh --once --network anvil
    """

    # =========================================================================
    # INITIALIZATION
    # =========================================================================

    def __init__(self, *args, **kwargs):
        """
        Initialize the looping strategy.

        Extracts configuration and sets up internal state for tracking
        the looping process.
        """
        super().__init__(*args, **kwargs)

        # =====================================================================
        # Extract configuration
        # =====================================================================

        # Market configuration (required)
        # Default: wstETH/USDC market on Ethereum (86% LLTV)
        self.market_id = self.get_config(
            "market_id",
            "0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc",
        )

        # Token configuration
        self.collateral_token = self.get_config("collateral_token", "wstETH")
        self.borrow_token = self.get_config("borrow_token", "USDC")

        # Collateral amount
        self.initial_collateral = Decimal(str(self.get_config("initial_collateral", "1.0")))

        # Looping parameters
        self.target_loops = int(self.get_config("target_loops", 3))
        self.target_ltv = Decimal(str(self.get_config("target_ltv", "0.75")))  # 75% LTV per loop

        # Risk parameters
        self.min_health_factor = Decimal(str(self.get_config("min_health_factor", "1.5")))

        # Swap parameters
        self.swap_slippage = Decimal(str(self.get_config("swap_slippage", "0.005")))  # 0.5%

        # Force action for testing
        self.force_action = str(self.get_config("force_action", "")).lower()

        # =====================================================================
        # Internal state tracking
        # =====================================================================

        # Loop state machine: idle -> supplying -> supplied -> borrowing -> borrowed -> swapping -> swapped -> (repeat) -> complete
        self._loop_state = "idle"
        self._previous_stable_state = "idle"  # Revert target on intent failure
        self._current_loop = 0
        self._loops_completed = 0

        # Position tracking
        self._total_collateral = Decimal("0")
        self._total_borrowed = Decimal("0")
        self._pending_swap_amount = Decimal("0")

        # Health tracking
        self._current_health_factor = Decimal("0")

        logger.info(
            f"MorphoLoopingStrategy initialized: "
            f"market={self.market_id[:10]}..., "
            f"collateral={self.initial_collateral} {self.collateral_token}, "
            f"target_loops={self.target_loops}, "
            f"target_ltv={self.target_ltv * 100}%"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """
        Make a looping decision based on market conditions and current state.

        Decision Flow (State Machine):
        1. IDLE: Start first supply
        2. SUPPLYING: Wait for supply confirmation
        3. SUPPLIED: Borrow against collateral
        4. BORROWING: Wait for borrow confirmation
        5. BORROWED: Swap borrowed tokens to collateral
        6. SWAPPING: Wait for swap confirmation
        7. Check if more loops needed -> Go to SUPPLYING
        8. COMPLETE: All loops done, hold position

        Parameters:
            market: MarketSnapshot containing prices, balances, etc.

        Returns:
            Intent: SUPPLY_COLLATERAL, BORROW, SWAP, or HOLD
        """
        # =================================================================
        # STEP 1: Get current market prices
        # =================================================================

        try:
            collateral_price = market.price(self.collateral_token)
            borrow_price = market.price(self.borrow_token)
            logger.debug(
                f"Prices: {self.collateral_token}=${collateral_price:.2f}, {self.borrow_token}=${borrow_price:.2f}"
            )
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get prices: {e}")
            # Use reasonable defaults for testing
            collateral_price = Decimal("3400")  # wstETH ~= ETH price
            borrow_price = Decimal("1")  # USDC = $1

        # =================================================================
        # STEP 2: Handle forced actions (for testing)
        # =================================================================

        if self.force_action == "supply":
            logger.info("Forced action: SUPPLY collateral")
            return self._create_supply_intent(self.initial_collateral)

        elif self.force_action == "borrow":
            logger.info("Forced action: BORROW")
            # For force_action testing, assume initial_collateral was supplied
            # This allows testing borrow independently without needing internal state
            self._total_collateral = self.initial_collateral
            return self._create_borrow_intent(collateral_price, borrow_price)

        elif self.force_action == "swap":
            logger.info("Forced action: SWAP borrowed to collateral")
            return self._create_swap_intent(Decimal("1000"), borrow_price)

        elif self.force_action == "repay":
            logger.info("Forced action: REPAY borrowed amount")
            return self._create_repay_intent()

        # =================================================================
        # STEP 3: State machine logic
        # =================================================================

        # State: IDLE - Start the first supply
        if self._loop_state == "idle":
            return self._handle_idle_state(market)

        # State: SUPPLIED - Borrow against collateral
        elif self._loop_state == "supplied":
            return self._handle_supplied_state(collateral_price, borrow_price)

        # State: BORROWED - Swap borrowed tokens to collateral
        elif self._loop_state == "borrowed":
            return self._handle_borrowed_state(borrow_price)

        # State: SWAPPED - Check if more loops needed
        elif self._loop_state == "swapped":
            return self._handle_swapped_state(market)

        # State: COMPLETE - All loops done
        elif self._loop_state == "complete":
            return self._handle_complete_state(collateral_price, borrow_price)

        # Safety net: if we're in a transitional state (supplying, borrowing, swapping)
        # it means the previous intent failed and on_intent_executed didn't fire.
        # Revert to the last known stable state.
        else:
            if self._loop_state in ("supplying", "borrowing", "swapping"):
                revert_to = self._previous_stable_state
                logger.warning(
                    f"Stuck in transitional state '{self._loop_state}' — reverting to '{revert_to}'"
                )
                self._loop_state = revert_to
            return Intent.hold(reason=f"Waiting for state transition (current: {self._loop_state})")


    # =========================================================================
    # STATE HANDLERS
    # =========================================================================

    def _handle_idle_state(self, market: MarketSnapshot) -> Intent:
        """Handle IDLE state - start first supply."""
        try:
            collateral_balance = market.balance(self.collateral_token)
            balance_value = collateral_balance.balance if hasattr(collateral_balance, "balance") else collateral_balance

            if balance_value < self.initial_collateral:
                return Intent.hold(
                    reason=f"Insufficient {self.collateral_token}: {balance_value} < {self.initial_collateral}"
                )
        except (ValueError, KeyError):
            logger.warning("Could not verify balance, proceeding anyway")

        logger.info(f"State: IDLE -> SUPPLYING (loop {self._current_loop + 1}/{self.target_loops})")
        self._emit_state_change("idle", "supplying")
        self._previous_stable_state = self._loop_state
        self._loop_state = "supplying"
        return self._create_supply_intent(self.initial_collateral)

    def _handle_supplied_state(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """Handle SUPPLIED state - borrow against collateral."""
        logger.info(f"State: SUPPLIED -> BORROWING (loop {self._current_loop + 1}/{self.target_loops})")
        self._emit_state_change("supplied", "borrowing")
        self._previous_stable_state = self._loop_state
        self._loop_state = "borrowing"
        return self._create_borrow_intent(collateral_price, borrow_price)

    def _handle_borrowed_state(self, borrow_price: Decimal) -> Intent:
        """Handle BORROWED state - swap borrowed tokens to collateral."""
        if self._pending_swap_amount <= 0:
            logger.warning("No borrowed amount to swap")
            self._loop_state = "swapped"
            return Intent.hold(reason="No borrowed amount to swap")

        logger.info(f"State: BORROWED -> SWAPPING (loop {self._current_loop + 1}/{self.target_loops})")
        self._emit_state_change("borrowed", "swapping")
        self._previous_stable_state = self._loop_state
        self._loop_state = "swapping"
        return self._create_swap_intent(self._pending_swap_amount, borrow_price)

    def _handle_swapped_state(self, market: MarketSnapshot) -> Intent:
        """Handle SWAPPED state - check if more loops needed.

        Note: Loop counters (_loops_completed, _current_loop) are incremented
        in on_intent_executed(success=True) for SWAP, not here. This prevents
        double-counting if a subsequent supply fails and we revert to this state.
        """
        if self._current_loop < self.target_loops:
            # More loops needed - supply the swapped collateral
            logger.info(
                f"Loop {self._loops_completed} complete. Starting loop {self._current_loop + 1}/{self.target_loops}"
            )
            self._previous_stable_state = self._loop_state
            self._loop_state = "supplying"

            # The collateral to supply is what we got from the swap
            # For now, supply the pending amount (set by on_intent_executed)
            supply_amount = self._pending_swap_amount
            if supply_amount <= 0:
                # Estimate based on last borrow
                supply_amount = self._total_borrowed / Decimal("3400")  # Rough estimate

            return self._create_supply_intent(supply_amount)
        else:
            # All loops complete
            logger.info(f"All {self.target_loops} loops complete! Final leverage achieved.")
            self._loop_state = "complete"
            self._emit_state_change("swapped", "complete")
            return Intent.hold(reason=f"Looping complete - {self._loops_completed} loops executed")

    def _handle_complete_state(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """Handle COMPLETE state - monitor position."""
        # Calculate current health factor
        if self._total_borrowed > 0:
            collateral_value = self._total_collateral * collateral_price
            borrow_value = self._total_borrowed * borrow_price
            self._current_health_factor = (collateral_value * self.target_ltv) / borrow_value

            if self._current_health_factor < self.min_health_factor:
                logger.warning(f"Health factor low: {self._current_health_factor:.2f} < {self.min_health_factor}")
                # In a production strategy, you would add collateral or repay debt here

        return Intent.hold(
            reason=f"Position active - HF: {self._current_health_factor:.2f}, "
            f"Collateral: {self._total_collateral} {self.collateral_token}, "
            f"Borrowed: {self._total_borrowed} {self.borrow_token}"
        )

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_supply_intent(self, amount: Decimal) -> Intent:
        """
        Create a SUPPLY intent to deposit collateral into Morpho Blue.

        For Morpho Blue, supply is used as collateral. The market_id is passed
        in the protocol_params for the intent compiler.

        Parameters:
            amount: Amount of collateral to supply

        Returns:
            SupplyIntent ready for compilation
        """
        logger.info(f"SUPPLY intent: {format_token_amount_human(amount, self.collateral_token)} to Morpho Blue")

        # Use standard supply intent with market_id for Morpho Blue
        # For Morpho, supply_as_collateral is always true since that's how Morpho works
        return Intent.supply(
            protocol="morpho_blue",
            token=self.collateral_token,
            amount=amount,
            use_as_collateral=True,  # Morpho always uses supply as collateral
            market_id=self.market_id,  # Required for Morpho Blue isolated markets
            chain=self.chain,
        )

    def _create_borrow_intent(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """
        Create a BORROW intent to borrow against supplied collateral.

        Calculates safe borrow amount based on:
        - Current collateral value
        - Target LTV (staying below market LLTV for safety)
        - Existing borrows

        Parameters:
            collateral_price: Current price of collateral token
            borrow_price: Current price of borrow token

        Returns:
            BorrowIntent ready for compilation
        """
        # Calculate collateral value in USD
        collateral_value = self._total_collateral * collateral_price

        # Calculate safe borrow amount
        # We use target_ltv which should be below the market's LLTV (e.g., 75% vs 86%)
        max_borrow_value = collateral_value * self.target_ltv

        # Subtract existing borrows
        existing_borrow_value = self._total_borrowed * borrow_price
        available_borrow_value = max_borrow_value - existing_borrow_value

        if available_borrow_value <= 0:
            logger.warning("No additional borrowing capacity")
            return Intent.hold(reason="No additional borrowing capacity")

        # Convert to borrow token units (accounting for price)
        borrow_amount = available_borrow_value / borrow_price

        # Round down for safety
        borrow_amount = borrow_amount.quantize(Decimal("0.01"))

        logger.info(
            f"BORROW intent: "
            f"Collateral={format_usd(collateral_value)}, "
            f"LTV={self.target_ltv * 100:.0f}%, "
            f"Borrow={format_token_amount_human(borrow_amount, self.borrow_token)}"
        )

        # Store for swap step
        self._pending_swap_amount = borrow_amount

        return Intent.borrow(
            protocol="morpho_blue",
            collateral_token=self.collateral_token,
            collateral_amount=Decimal("0"),  # Already supplied
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            market_id=self.market_id,  # Required for Morpho Blue isolated markets
            chain=self.chain,
        )

    def _create_swap_intent(self, amount: Decimal, borrow_price: Decimal) -> Intent:
        """
        Create a SWAP intent to convert borrowed tokens back to collateral.

        This is the "loop" part - converting borrowed USDC back to wstETH
        so we can re-supply and amplify our position.

        Parameters:
            amount: Amount of borrow token to swap
            borrow_price: Current price of borrow token (for logging)

        Returns:
            SwapIntent ready for compilation
        """
        swap_value = amount * borrow_price

        logger.info(
            f"SWAP intent: "
            f"{format_token_amount_human(amount, self.borrow_token)} ({format_usd(swap_value)}) "
            f"-> {self.collateral_token}"
        )

        return Intent.swap(
            from_token=self.borrow_token,
            to_token=self.collateral_token,
            amount=amount,
            max_slippage=self.swap_slippage,
            chain=self.chain,
        )

    def _create_repay_intent(self, amount: Decimal | None = None) -> Intent:
        """
        Create a REPAY intent to repay borrowed tokens.

        Used for:
        - Partial deleveraging
        - Full position unwinding (teardown)
        - Testing repay functionality

        Parameters:
            amount: Amount to repay (None = repay full debt using repay_full=True)

        Returns:
            RepayIntent ready for compilation
        """
        # For testing, always use repay_full=True to avoid share-to-asset conversion issues
        # Morpho Blue's repay with MAX_UINT256 shares handles exact debt repayment
        if amount is None:
            logger.info(f"REPAY intent: repay_full=True (will repay exact debt amount)")
            return Intent.repay(
                protocol="morpho_blue",
                token=self.borrow_token,
                amount=Decimal("0"),  # Amount ignored when repay_full=True
                repay_full=True,
                market_id=self.market_id,
                chain=self.chain,
            )
        else:
            logger.info(f"REPAY intent: {format_token_amount_human(amount, self.borrow_token)}")
            return Intent.repay(
                protocol="morpho_blue",
                token=self.borrow_token,
                amount=amount,
                repay_full=False,  # Repay specific amount
                market_id=self.market_id,
                chain=self.chain,
            )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """
        Called after an intent is executed.

        Updates internal state to track looping progress.
        """
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY_COLLATERAL":
                self._loop_state = "supplied"
                # Extract amount from intent
                if hasattr(intent, "amount"):
                    amount = intent.amount if isinstance(intent.amount, Decimal) else Decimal("0")
                    self._total_collateral += amount
                logger.info(f"Supply successful - Total collateral: {self._total_collateral} {self.collateral_token}")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Supplied {self.collateral_token} to Morpho Blue",
                        strategy_id=self.strategy_id,
                        details={
                            "action": "supply_collateral",
                            "token": self.collateral_token,
                            "total_collateral": str(self._total_collateral),
                        },
                    )
                )

            elif intent_type == "BORROW":
                self._loop_state = "borrowed"
                # Extract borrow amount
                if hasattr(intent, "borrow_amount"):
                    amount = intent.borrow_amount if isinstance(intent.borrow_amount, Decimal) else Decimal("0")
                    self._total_borrowed += amount
                    self._pending_swap_amount = amount
                logger.info(f"Borrow successful - Total borrowed: {self._total_borrowed} {self.borrow_token}")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Borrowed {self.borrow_token} from Morpho Blue",
                        strategy_id=self.strategy_id,
                        details={
                            "action": "borrow",
                            "token": self.borrow_token,
                            "total_borrowed": str(self._total_borrowed),
                        },
                    )
                )

            elif intent_type == "SWAP":
                self._loop_state = "swapped"
                # Increment loop counters here (not in _handle_swapped_state) to prevent
                # double-counting if a subsequent supply fails and reverts to "swapped"
                self._loops_completed += 1
                self._current_loop += 1
                # The swap result should contain the output amount
                # For now, estimate based on prices
                logger.info(f"Swap successful - Loop {self._current_loop} swap complete")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Swapped {self.borrow_token} to {self.collateral_token}",
                        strategy_id=self.strategy_id,
                        details={
                            "action": "swap",
                            "from_token": self.borrow_token,
                            "to_token": self.collateral_token,
                            "loop": self._current_loop + 1,
                        },
                    )
                )

        else:
            # On failure, revert to previous stable state so decide() can retry
            # (staying in the transitional state would permanently stuck the strategy)
            revert_to = self._previous_stable_state
            logger.warning(
                f"{intent_type} failed in state '{self._loop_state}' — reverting to '{revert_to}'"
            )
            self._loop_state = revert_to

    def _emit_state_change(self, old_state: str, new_state: str) -> None:
        """Emit a state change event to the timeline."""
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"State: {old_state.upper()} -> {new_state.upper()}",
                strategy_id=self.strategy_id,
                details={
                    "old_state": old_state,
                    "new_state": new_state,
                    "loop": self._current_loop + 1,
                    "total_loops": self.target_loops,
                },
            )
        )

    # =========================================================================
    # STATUS REPORTING
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "demo_morpho_looping",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "market_id": self.market_id[:20] + "...",
                "collateral_token": self.collateral_token,
                "borrow_token": self.borrow_token,
                "initial_collateral": str(self.initial_collateral),
                "target_loops": self.target_loops,
                "target_ltv": str(self.target_ltv),
                "min_health_factor": str(self.min_health_factor),
            },
            "state": {
                "loop_state": self._loop_state,
                "current_loop": self._current_loop,
                "loops_completed": self._loops_completed,
                "total_collateral": str(self._total_collateral),
                "total_borrowed": str(self._total_borrowed),
                "health_factor": str(self._current_health_factor),
            },
        }

    # =========================================================================
    # STATE PERSISTENCE
    # =========================================================================

    def get_persistent_state(self) -> dict[str, Any]:
        """Get state to persist for crash recovery."""
        return {
            "loop_state": self._loop_state,
            "previous_stable_state": self._previous_stable_state,
            "current_loop": self._current_loop,
            "loops_completed": self._loops_completed,
            "total_collateral": str(self._total_collateral),
            "total_borrowed": str(self._total_borrowed),
            "pending_swap_amount": str(self._pending_swap_amount),
            "current_health_factor": str(self._current_health_factor),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persisted state on startup."""
        if "loop_state" in state:
            self._loop_state = state["loop_state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "current_loop" in state:
            self._current_loop = int(state["current_loop"])
        if "loops_completed" in state:
            self._loops_completed = int(state["loops_completed"])
        if "total_collateral" in state:
            self._total_collateral = Decimal(str(state["total_collateral"]))
        if "total_borrowed" in state:
            self._total_borrowed = Decimal(str(state["total_borrowed"]))
        if "pending_swap_amount" in state:
            self._pending_swap_amount = Decimal(str(state["pending_swap_amount"]))
        if "current_health_factor" in state:
            self._current_health_factor = Decimal(str(state["current_health_factor"]))

        logger.info(
            f"Restored state: loop={self._current_loop}/{self.target_loops}, "
            f"state={self._loop_state}, HF={self._current_health_factor}"
        )

    # =========================================================================
    # TEARDOWN INTERFACE
    # =========================================================================

    def supports_teardown(self) -> bool:
        """This strategy supports the teardown system."""
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":  # noqa: F821
        """Get all open positions for teardown.

        Returns:
            TeardownPositionSummary with supply and borrow positions
        """
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []

        # Collateral position
        if self._total_collateral > 0:
            collateral_value = self._total_collateral * Decimal("3400")  # Estimate
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"morpho-collateral-{self.market_id[:16]}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=collateral_value,
                    details={
                        "market_id": self.market_id,
                        "asset": self.collateral_token,
                        "amount": str(self._total_collateral),
                    },
                )
            )

        # Borrow position
        if self._total_borrowed > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"morpho-borrow-{self.market_id[:16]}",
                    chain=self.chain,
                    protocol="morpho_blue",
                    value_usd=self._total_borrowed,
                    health_factor=self._current_health_factor,
                    details={
                        "market_id": self.market_id,
                        "asset": self.borrow_token,
                        "amount": str(self._total_borrowed),
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:  # noqa: F821
        """Generate intents to unwind the looped position.

        Teardown order (CRITICAL for safety):
        1. SWAP: Swap any remaining borrow tokens to repay
        2. REPAY: Repay all borrowed amount (frees collateral)
        3. WITHDRAW_COLLATERAL: Withdraw all collateral
        4. SWAP: Optionally swap collateral to stable

        Args:
            mode: TeardownMode.SOFT (graceful) or TeardownMode.HARD (emergency)

        Returns:
            List of intents in correct execution order
        """
        intents = []

        # Step 1: Repay all borrowed amount
        if self._total_borrowed > 0:
            intents.append(
                Intent.repay(
                    protocol="morpho_blue",
                    token=self.borrow_token,
                    amount=self._total_borrowed,
                    repay_full=True,
                    market_id=self.market_id,  # Required for Morpho Blue
                    chain=self.chain,
                )
            )

        # Step 2: Withdraw all collateral
        if self._total_collateral > 0:
            intents.append(
                Intent.withdraw(
                    protocol="morpho_blue",
                    token=self.collateral_token,
                    amount=self._total_collateral,
                    withdraw_all=True,
                    market_id=self.market_id,  # Required for Morpho Blue
                    chain=self.chain,
                )
            )

        # Step 3: Swap collateral to stable (USDC)
        if self._total_collateral > 0:
            intents.append(
                Intent.swap(
                    from_token=self.collateral_token,
                    to_token="USDC",
                    amount="all",
                    chain=self.chain,
                )
            )

        return intents

    def on_teardown_started(self, mode: "TeardownMode") -> None:  # noqa: F821
        """Called when teardown starts."""
        from almanak.framework.teardown import TeardownMode

        mode_name = "graceful" if mode == TeardownMode.SOFT else "emergency"
        logger.info(f"Teardown started in {mode_name} mode for Morpho Looping strategy")
        logger.info(
            f"Will repay {self._total_borrowed} {self.borrow_token} "
            f"and withdraw {self._total_collateral} {self.collateral_token}"
        )

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        """Called when teardown completes."""
        if success:
            logger.info(f"Teardown completed. Recovered ${recovered_usd:,.2f}")
            # Reset state
            self._loop_state = "idle"
            self._current_loop = 0
            self._loops_completed = 0
            self._total_collateral = Decimal("0")
            self._total_borrowed = Decimal("0")
            self._pending_swap_amount = Decimal("0")
            self._current_health_factor = Decimal("0")
        else:
            logger.error("Teardown failed - manual intervention may be required")


# =============================================================================
# TESTING
# =============================================================================

if __name__ == "__main__":
    print("=" * 70)
    print("MorphoLoopingStrategy - Leveraged Yield Farming Demo")
    print("=" * 70)
    print(f"\nStrategy Name: {MorphoLoopingStrategy.STRATEGY_NAME}")
    print(f"Version: {MorphoLoopingStrategy.STRATEGY_METADATA.version}")
    print(f"Supported Chains: {MorphoLoopingStrategy.SUPPORTED_CHAINS}")
    print(f"Supported Protocols: {MorphoLoopingStrategy.SUPPORTED_PROTOCOLS}")
    print(f"Intent Types: {MorphoLoopingStrategy.INTENT_TYPES}")
    print(f"\nDescription: {MorphoLoopingStrategy.STRATEGY_METADATA.description}")
    print("\nTo run this strategy:")
    print("  uv run almanak strat run --strategy demo_morpho_looping --once")
    print("\nTo test on Anvil:")
    print("  python strategies/demo/morpho_looping/run_anvil.py")
