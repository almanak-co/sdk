"""
Aave V3 Leveraged Yield Loop Strategy
======================================

Recursive leveraged yield strategy on Aave V3:
1. Supply wstETH as collateral
2. Borrow WETH against it (at safe LTV)
3. Swap WETH -> wstETH
4. Re-supply the new wstETH
5. Repeat N times

Net yield = (wstETH supply APY * leverage) - (WETH borrow APY * leverage)

This strategy exercises:
- Multi-protocol composability (Aave V3 + DEX swap)
- IntentSequence for multi-step flows (future improvement)
- State machine pattern for multi-iteration looping
- Aave V3 supply/borrow intents (less tested path)

Risks:
- Liquidation if wstETH/ETH peg breaks
- Each loop increases leverage and liquidation risk
- Gas costs for multi-step transactions
- Slippage on wstETH <-> WETH swaps

Source: VIB-123 (AGI - Strategist project)
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="aave_leverage_loop",
    description="Leveraged yield loop on Aave V3 - supply wstETH, borrow WETH, swap back, re-supply",
    version="1.0.0",
    author="YAInnick Loop",
    tags=["incubating", "lending", "leverage", "looping", "aave-v3", "yield"],
    supported_chains=["arbitrum", "ethereum"],
    supported_protocols=["aave_v3", "uniswap_v3"],
    intent_types=["SUPPLY", "BORROW", "SWAP", "REPAY", "WITHDRAW", "HOLD"],
)
class AaveLeverageLoopStrategy(IntentStrategy):
    """Aave V3 leveraged yield loop strategy.

    Uses a state machine to execute the looping sequence:
    idle -> supplying -> supplied -> borrowing -> borrowed -> swapping -> swapped -> (repeat or complete)

    Each call to decide() advances the state by one step. Use --interval mode
    (not --once) to complete the full looping lifecycle.

    Use --fresh flag when testing on Anvil to clear stale state from previous runs.

    Example::

        almanak strat run -d strategies/incubating/aave_leverage_loop --fresh --interval 15 --network anvil
        almanak strat run -d strategies/incubating/aave_leverage_loop --fresh --once --network anvil
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            if hasattr(self.config, "get"):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Token configuration
        self.collateral_token = get_config("collateral_token", "wstETH")
        self.borrow_token = get_config("borrow_token", "WETH")

        # Position sizing
        self.initial_collateral = Decimal(str(get_config("initial_collateral", "0.1")))

        # Looping parameters
        self.target_loops = int(get_config("target_loops", 3))
        self.target_ltv = Decimal(str(get_config("target_ltv", "0.70")))

        # Risk parameters
        self.min_health_factor = Decimal(str(get_config("min_health_factor", "1.5")))

        # Swap parameters
        self.swap_slippage = Decimal(str(get_config("swap_slippage", "0.01")))

        # Force action for testing
        self.force_action = str(get_config("force_action", "")).lower()

        # Internal state machine
        self._loop_state = "idle"
        self._current_loop = 0
        self._loops_completed = 0

        # Position tracking (in token units)
        self._total_collateral_supplied = Decimal("0")
        self._total_borrowed = Decimal("0")
        self._last_borrow_amount = Decimal("0")  # Amount from most recent borrow
        self._last_swap_output = Decimal("0")  # Estimated output from most recent swap

        # Health tracking
        self._current_health_factor = Decimal("0")

        logger.info(
            f"AaveLeverageLoop initialized: "
            f"collateral={self.initial_collateral} {self.collateral_token}, "
            f"borrow_token={self.borrow_token}, "
            f"target_loops={self.target_loops}, "
            f"target_ltv={self.target_ltv * 100}%"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """State machine for the leveraged loop.

        Each call advances one step:
        idle -> supply initial -> supplied -> borrow -> borrowed -> swap -> swapped
        -> supply loop N -> supplied -> borrow -> ... -> complete
        """
        try:
            # Get prices for calculations
            collateral_price = self._get_price(market, self.collateral_token, Decimal("3800"))
            borrow_price = self._get_price(market, self.borrow_token, Decimal("3400"))

            # Handle forced actions for testing
            if self.force_action:
                return self._handle_force_action(collateral_price, borrow_price)

            # State machine dispatch
            if self._loop_state == "idle":
                return self._handle_idle(market)
            elif self._loop_state == "supplied":
                return self._handle_supplied(collateral_price, borrow_price)
            elif self._loop_state == "borrowed":
                return self._handle_borrowed(collateral_price, borrow_price)
            elif self._loop_state == "swapped":
                return self._handle_swapped(market)
            elif self._loop_state == "complete":
                return self._handle_complete(collateral_price, borrow_price)
            else:
                return Intent.hold(reason=f"Waiting for state transition (state={self._loop_state})")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    # =========================================================================
    # STATE HANDLERS
    # =========================================================================

    def _handle_idle(self, market: MarketSnapshot) -> Intent:
        """IDLE -> Supply initial collateral to Aave V3."""
        # Verify we have enough collateral
        try:
            balance = market.balance(self.collateral_token)
            balance_value = balance.balance if hasattr(balance, "balance") else balance
            if balance_value < self.initial_collateral:
                return Intent.hold(
                    reason=f"Insufficient {self.collateral_token}: "
                    f"{balance_value} < {self.initial_collateral}"
                )
        except (ValueError, KeyError):
            logger.warning("Could not verify balance, proceeding anyway")

        logger.info(
            f"State: IDLE -> SUPPLYING "
            f"(initial {format_token_amount_human(self.initial_collateral, self.collateral_token)})"
        )
        self._emit_state_change("idle", "supplying")
        self._loop_state = "supplying"
        return self._create_supply_intent(self.initial_collateral)

    def _handle_supplied(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """SUPPLIED -> Borrow against collateral."""
        loop_num = self._current_loop + 1
        logger.info(f"State: SUPPLIED -> BORROWING (loop {loop_num}/{self.target_loops})")
        self._emit_state_change("supplied", "borrowing")
        self._loop_state = "borrowing"
        return self._create_borrow_intent(collateral_price, borrow_price)

    def _handle_borrowed(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """BORROWED -> Swap borrowed WETH to wstETH."""
        if self._last_borrow_amount <= 0:
            logger.warning("No borrow amount to swap, completing loop")
            self._loop_state = "swapped"
            return Intent.hold(reason="No borrow amount to swap")

        logger.info(
            f"State: BORROWED -> SWAPPING "
            f"({format_token_amount_human(self._last_borrow_amount, self.borrow_token)} -> {self.collateral_token})"
        )
        self._emit_state_change("borrowed", "swapping")
        self._loop_state = "swapping"
        return self._create_swap_intent(self._last_borrow_amount, borrow_price)

    def _handle_swapped(self, market: MarketSnapshot) -> Intent:
        """SWAPPED -> Check if more loops needed, re-supply if so."""
        self._loops_completed += 1
        self._current_loop += 1

        if self._current_loop < self.target_loops:
            # More loops needed -- supply the swap output as additional collateral
            supply_amount = self._last_swap_output
            if supply_amount <= 0:
                # Estimate: borrow amount / price ratio (wstETH is ~1.18x WETH)
                supply_amount = self._last_borrow_amount * Decimal("0.85")

            logger.info(
                f"Loop {self._loops_completed} complete. "
                f"Starting loop {self._current_loop + 1}/{self.target_loops}. "
                f"Re-supplying ~{format_token_amount_human(supply_amount, self.collateral_token)}"
            )
            self._loop_state = "supplying"
            return self._create_supply_intent(supply_amount)
        else:
            # All loops complete
            effective_leverage = self._calculate_leverage()
            logger.info(
                f"All {self.target_loops} loops complete. "
                f"Effective leverage: {effective_leverage:.2f}x. "
                f"Total collateral: {self._total_collateral_supplied} {self.collateral_token}, "
                f"Total borrowed: {self._total_borrowed} {self.borrow_token}"
            )
            self._loop_state = "complete"
            self._emit_state_change("swapped", "complete")
            return Intent.hold(
                reason=f"Looping complete - {self._loops_completed} loops, "
                f"~{effective_leverage:.1f}x leverage"
            )

    def _handle_complete(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """COMPLETE -> Monitor position health."""
        if self._total_borrowed > 0 and self._total_collateral_supplied > 0:
            # Estimate health factor
            # HF = (collateral_value * liquidation_threshold) / borrow_value
            # Aave wstETH liquidation threshold is ~83%
            liq_threshold = Decimal("0.83")
            collateral_value = self._total_collateral_supplied * collateral_price
            borrow_value = self._total_borrowed * borrow_price
            if borrow_value > 0:
                self._current_health_factor = (collateral_value * liq_threshold) / borrow_value
            else:
                self._current_health_factor = Decimal("999")

            if self._current_health_factor < self.min_health_factor:
                logger.warning(
                    f"Health factor LOW: {self._current_health_factor:.2f} < {self.min_health_factor}. "
                    f"Consider unwinding."
                )

        leverage = self._calculate_leverage()
        return Intent.hold(
            reason=f"Position active - HF: {self._current_health_factor:.2f}, "
            f"leverage: {leverage:.1f}x, "
            f"collateral: {self._total_collateral_supplied} {self.collateral_token}, "
            f"debt: {self._total_borrowed} {self.borrow_token}"
        )

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_supply_intent(self, amount: Decimal) -> Intent:
        """Create a SUPPLY intent for wstETH to Aave V3."""
        logger.info(f"SUPPLY: {format_token_amount_human(amount, self.collateral_token)} to Aave V3")
        return Intent.supply(
            protocol="aave_v3",
            token=self.collateral_token,
            amount=amount,
            use_as_collateral=True,
            chain=self.chain,
        )

    def _create_borrow_intent(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """Create a BORROW intent for WETH from Aave V3.

        Calculates safe borrow amount based on:
        - Total collateral value
        - Target LTV (e.g., 70%, well below Aave's 83% liquidation threshold for wstETH)
        - Existing borrows
        """
        collateral_value = self._total_collateral_supplied * collateral_price
        max_borrow_value = collateral_value * self.target_ltv
        existing_borrow_value = self._total_borrowed * borrow_price
        available_borrow_value = max_borrow_value - existing_borrow_value

        if available_borrow_value <= 0:
            logger.warning("No additional borrowing capacity available")
            return Intent.hold(reason="No additional borrowing capacity")

        borrow_amount = available_borrow_value / borrow_price
        # Round down for safety
        borrow_amount = borrow_amount.quantize(Decimal("0.0001"))

        logger.info(
            f"BORROW: {format_token_amount_human(borrow_amount, self.borrow_token)} "
            f"(collateral={format_usd(collateral_value)}, LTV={self.target_ltv * 100:.0f}%, "
            f"existing_debt={format_usd(existing_borrow_value)})"
        )

        self._last_borrow_amount = borrow_amount

        return Intent.borrow(
            protocol="aave_v3",
            collateral_token=self.collateral_token,
            collateral_amount=Decimal("0"),  # Already supplied
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            interest_rate_mode="variable",
            chain=self.chain,
        )

    def _create_swap_intent(self, amount: Decimal, borrow_price: Decimal) -> Intent:
        """Create a SWAP intent: WETH -> wstETH."""
        swap_value_usd = amount * borrow_price
        logger.info(
            f"SWAP: {format_token_amount_human(amount, self.borrow_token)} "
            f"({format_usd(swap_value_usd)}) -> {self.collateral_token}"
        )
        return Intent.swap(
            from_token=self.borrow_token,
            to_token=self.collateral_token,
            amount=amount,
            max_slippage=self.swap_slippage,
            chain=self.chain,
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Update internal state after intent execution."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._loop_state = "supplied"
                if hasattr(intent, "amount") and isinstance(intent.amount, Decimal):
                    self._total_collateral_supplied += intent.amount
                logger.info(
                    f"Supply OK - total collateral: "
                    f"{self._total_collateral_supplied} {self.collateral_token}"
                )
                self._emit_position_event("supply", self.collateral_token)

            elif intent_type == "BORROW":
                self._loop_state = "borrowed"
                if hasattr(intent, "borrow_amount") and isinstance(intent.borrow_amount, Decimal):
                    self._total_borrowed += intent.borrow_amount
                    self._last_borrow_amount = intent.borrow_amount
                logger.info(
                    f"Borrow OK - total debt: "
                    f"{self._total_borrowed} {self.borrow_token}"
                )
                self._emit_position_event("borrow", self.borrow_token)

            elif intent_type == "SWAP":
                self._loop_state = "swapped"
                # Estimate swap output (wstETH is ~1.18x WETH, minus slippage)
                estimated_output = self._last_borrow_amount * Decimal("0.84")
                if result and hasattr(result, "swap_amounts"):
                    try:
                        if result.swap_amounts and result.swap_amounts.amount_out:
                            estimated_output = result.swap_amounts.amount_out
                    except (AttributeError, TypeError):
                        pass
                self._last_swap_output = estimated_output
                logger.info(
                    f"Swap OK - loop {self._current_loop + 1} swap complete, "
                    f"estimated output: ~{estimated_output} {self.collateral_token}"
                )
                self._emit_position_event("swap", f"{self.borrow_token} -> {self.collateral_token}")

            elif intent_type == "REPAY":
                if hasattr(intent, "repay_full") and intent.repay_full:
                    self._total_borrowed = Decimal("0")
                elif hasattr(intent, "amount") and isinstance(intent.amount, Decimal):
                    self._total_borrowed = max(Decimal("0"), self._total_borrowed - intent.amount)
                logger.info(f"Repay OK - remaining debt: {self._total_borrowed} {self.borrow_token}")
                self._emit_position_event("repay", self.borrow_token)

            elif intent_type == "WITHDRAW":
                if hasattr(intent, "withdraw_all") and intent.withdraw_all:
                    self._total_collateral_supplied = Decimal("0")
                elif hasattr(intent, "amount") and isinstance(intent.amount, Decimal):
                    self._total_collateral_supplied = max(
                        Decimal("0"), self._total_collateral_supplied - intent.amount
                    )
                logger.info(
                    f"Withdraw OK - remaining collateral: "
                    f"{self._total_collateral_supplied} {self.collateral_token}"
                )
                self._emit_position_event("withdraw", self.collateral_token)
        else:
            logger.warning(f"{intent_type} FAILED - staying in current state for retry")

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_price(self, market: MarketSnapshot, token: str, fallback: Decimal) -> Decimal:
        """Get token price from market, with fallback for testing."""
        try:
            return market.price(token)
        except (ValueError, KeyError) as e:
            logger.debug(f"Could not get price for {token}: {e}, using fallback ${fallback}")
            return fallback

    def _calculate_leverage(self) -> Decimal:
        """Calculate effective leverage ratio.

        Leverage = total_collateral / initial_collateral
        For a 3-loop strategy at 70% LTV: ~1 + 0.7 + 0.49 + 0.34 = ~2.53x
        """
        if self.initial_collateral > 0 and self._total_collateral_supplied > 0:
            return self._total_collateral_supplied / self.initial_collateral
        return Decimal("1")

    def _handle_force_action(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """Handle forced actions for testing."""
        if self.force_action == "supply":
            return self._create_supply_intent(self.initial_collateral)
        elif self.force_action == "borrow":
            self._total_collateral_supplied = self.initial_collateral
            return self._create_borrow_intent(collateral_price, borrow_price)
        elif self.force_action == "swap":
            return self._create_swap_intent(Decimal("0.05"), borrow_price)
        elif self.force_action == "repay":
            return Intent.repay(
                protocol="aave_v3",
                token=self.borrow_token,
                amount=Decimal("0"),
                repay_full=True,
                chain=self.chain,
            )
        return Intent.hold(reason=f"Unknown force_action: {self.force_action}")

    def _emit_state_change(self, old_state: str, new_state: str) -> None:
        """Emit a state change timeline event."""
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

    def _emit_position_event(self, action: str, token: str) -> None:
        """Emit a position modified timeline event."""
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.POSITION_MODIFIED,
                description=f"{action.upper()} {token}",
                strategy_id=self.strategy_id,
                details={
                    "action": action,
                    "token": token,
                    "loop": self._current_loop + 1,
                    "total_collateral": str(self._total_collateral_supplied),
                    "total_borrowed": str(self._total_borrowed),
                },
            )
        )

    # =========================================================================
    # STATUS AND STATE PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "aave_leverage_loop",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
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
                "total_collateral": str(self._total_collateral_supplied),
                "total_borrowed": str(self._total_borrowed),
                "health_factor": str(self._current_health_factor),
                "leverage": str(self._calculate_leverage()),
            },
        }

    def get_persistent_state(self) -> dict[str, Any]:
        """Get state to persist for crash recovery."""
        return {
            "loop_state": self._loop_state,
            "current_loop": self._current_loop,
            "loops_completed": self._loops_completed,
            "total_collateral_supplied": str(self._total_collateral_supplied),
            "total_borrowed": str(self._total_borrowed),
            "last_borrow_amount": str(self._last_borrow_amount),
            "last_swap_output": str(self._last_swap_output),
            "current_health_factor": str(self._current_health_factor),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persisted state on startup."""
        if "loop_state" in state:
            self._loop_state = state["loop_state"]
        if "current_loop" in state:
            self._current_loop = int(state["current_loop"])
        if "loops_completed" in state:
            self._loops_completed = int(state["loops_completed"])
        if "total_collateral_supplied" in state:
            self._total_collateral_supplied = Decimal(str(state["total_collateral_supplied"]))
        if "total_borrowed" in state:
            self._total_borrowed = Decimal(str(state["total_borrowed"]))
        if "last_borrow_amount" in state:
            self._last_borrow_amount = Decimal(str(state["last_borrow_amount"]))
        if "last_swap_output" in state:
            self._last_swap_output = Decimal(str(state["last_swap_output"]))
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
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":  # noqa: F821
        """Get open positions for teardown."""
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []

        if self._total_collateral_supplied > 0:
            collateral_value = self._total_collateral_supplied * Decimal("3800")
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.collateral_token}-{self.chain}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=collateral_value,
                    details={
                        "asset": self.collateral_token,
                        "amount": str(self._total_collateral_supplied),
                    },
                )
            )

        if self._total_borrowed > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"aave-borrow-{self.borrow_token}-{self.chain}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._total_borrowed * Decimal("3400"),
                    health_factor=self._current_health_factor,
                    details={
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
        """Generate intents to unwind the leveraged position.

        Teardown order:
        1. Swap any wstETH proceeds to WETH (to repay debt)
        2. Repay all WETH debt
        3. Withdraw all wstETH collateral
        """
        intents = []

        # Step 1: Repay all WETH debt
        if self._total_borrowed > 0:
            intents.append(
                Intent.repay(
                    protocol="aave_v3",
                    token=self.borrow_token,
                    amount=Decimal("0"),
                    repay_full=True,
                    chain=self.chain,
                )
            )

        # Step 2: Withdraw all wstETH collateral
        if self._total_collateral_supplied > 0:
            intents.append(
                Intent.withdraw(
                    protocol="aave_v3",
                    token=self.collateral_token,
                    amount=Decimal("0"),
                    withdraw_all=True,
                    chain=self.chain,
                )
            )

        return intents

    def on_teardown_started(self, mode: "TeardownMode") -> None:  # noqa: F821
        from almanak.framework.teardown import TeardownMode

        mode_name = "graceful" if mode == TeardownMode.SOFT else "emergency"
        logger.info(
            f"Teardown ({mode_name}): "
            f"repay {self._total_borrowed} {self.borrow_token}, "
            f"withdraw {self._total_collateral_supplied} {self.collateral_token}"
        )

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        if success:
            logger.info(f"Teardown completed. Recovered ${recovered_usd:,.2f}")
            self._loop_state = "idle"
            self._current_loop = 0
            self._loops_completed = 0
            self._total_collateral_supplied = Decimal("0")
            self._total_borrowed = Decimal("0")
            self._last_borrow_amount = Decimal("0")
            self._last_swap_output = Decimal("0")
            self._current_health_factor = Decimal("0")
        else:
            logger.error("Teardown FAILED - manual intervention may be required")
