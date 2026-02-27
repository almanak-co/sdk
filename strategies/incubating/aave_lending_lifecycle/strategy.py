"""
Aave V3 Lending Lifecycle Strategy
===================================

Full lending lifecycle on Aave V3: supply collateral -> borrow -> repay.
First-ever test of RepayIntent in the YAInnick loop (14 iterations).

This strategy exercises:
- SupplyIntent (proven in demo_aave_borrow)
- BorrowIntent (partially tested in iteration 1, got stuck in state machine)
- RepayIntent (NEVER tested in yailoop -- this is the primary coverage gap)

Lifecycle mode runs all 3 steps across 3 iterations on the same Anvil fork.
Force mode allows testing each step independently.
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
    name="aave_lending_lifecycle",
    description="Aave V3 full lending lifecycle: supply -> borrow -> repay",
    version="1.0.0",
    author="YAInnick Loop",
    tags=["incubating", "lending", "lifecycle", "aave-v3", "repay"],
    supported_chains=["arbitrum"],
    supported_protocols=["aave_v3"],
    intent_types=["SUPPLY", "BORROW", "REPAY", "HOLD"],
)
class AaveLendingLifecycleStrategy(IntentStrategy):
    """Aave V3 lending lifecycle: supply -> borrow -> repay.

    State machine:
        idle -> supplying -> supplied -> borrowing -> borrowed -> repaying -> complete

    Config parameters:
        collateral_token: Token to supply as collateral (default: WETH)
        collateral_amount: Amount to supply (default: 0.01)
        borrow_token: Token to borrow (default: USDC)
        ltv_target: Target loan-to-value ratio (default: 0.3 = 30%)
        interest_rate_mode: "variable" or "stable" (default: variable)
        force_action: "supply", "borrow", "repay", or "lifecycle" for full cycle
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Collateral config
        self.collateral_token = get_config("collateral_token", "WETH")
        self.collateral_amount = Decimal(str(get_config("collateral_amount", "0.01")))

        # Borrow config
        self.borrow_token = get_config("borrow_token", "USDC")
        self.ltv_target = Decimal(str(get_config("ltv_target", "0.3")))

        # Interest rate mode
        self.interest_rate_mode = get_config("interest_rate_mode", "variable")

        # Force action for testing
        self.force_action = str(get_config("force_action", "")).lower()

        # State machine
        self._state = "idle"
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")
        self._previous_stable_state = "idle"

        logger.info(
            f"AaveLendingLifecycleStrategy initialized: "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"borrow={self.borrow_token}, LTV={self.ltv_target * 100:.0f}%, "
            f"force_action={self.force_action or 'none'}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make lending decision based on state machine and market data."""
        try:
            # Get prices for borrow calculation
            collateral_price, borrow_price = self._get_prices(market)

            # Handle force_action modes
            if self.force_action == "supply":
                logger.info("Forced action: SUPPLY collateral")
                return self._create_supply_intent()

            if self.force_action == "borrow":
                logger.info("Forced action: BORROW against collateral")
                return self._create_borrow_intent(collateral_price, borrow_price)

            if self.force_action == "repay":
                logger.info("Forced action: REPAY borrowed amount")
                return self._create_repay_intent()

            if self.force_action == "lifecycle":
                return self._lifecycle_step(market, collateral_price, borrow_price)

            # Default: single supply
            if self._state == "idle":
                return self._create_supply_intent()

            return Intent.hold(reason=f"State: {self._state}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _lifecycle_step(
        self,
        market: MarketSnapshot,
        collateral_price: Decimal,
        borrow_price: Decimal,
    ) -> Intent | None:
        """Execute the next step in the supply -> borrow -> repay lifecycle."""
        logger.info(f"Lifecycle mode: current state = {self._state}")

        if self._state == "idle":
            # Step 1: Supply collateral
            self._transition("supplying")
            return self._create_supply_intent()

        if self._state == "supplied":
            # Step 2: Borrow against collateral
            self._transition("borrowing")
            return self._create_borrow_intent(collateral_price, borrow_price)

        if self._state == "borrowed":
            # Step 3: Repay the borrow
            self._transition("repaying")
            return self._create_repay_intent()

        if self._state == "complete":
            return Intent.hold(reason="Lifecycle complete: supply -> borrow -> repay all succeeded")

        # Transitional states: wait for callback
        return Intent.hold(reason=f"Waiting for {self._state} to complete")

    def _transition(self, new_state: str) -> None:
        """Transition state machine with logging."""
        old_state = self._state
        self._previous_stable_state = old_state
        self._state = new_state
        logger.info(f"State transition: {old_state} -> {new_state}")
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"State: {old_state} -> {new_state}",
                strategy_id=self.strategy_id,
                details={"old_state": old_state, "new_state": new_state},
            )
        )

    def _get_prices(self, market: MarketSnapshot) -> tuple[Decimal, Decimal]:
        """Get collateral and borrow token prices."""
        try:
            collateral_price = market.price(self.collateral_token)
            borrow_price = market.price(self.borrow_token)
            logger.info(
                f"Prices: {self.collateral_token}=${collateral_price:.2f}, "
                f"{self.borrow_token}=${borrow_price:.2f}"
            )
            return collateral_price, borrow_price
        except (ValueError, KeyError) as e:
            logger.warning(f"Price fetch failed: {e}, using defaults")
            return Decimal("3400"), Decimal("1")

    # =========================================================================
    # Intent creation
    # =========================================================================

    def _create_supply_intent(self) -> Intent:
        """Create SUPPLY intent to deposit collateral into Aave V3."""
        logger.info(
            f"SUPPLY: {format_token_amount_human(self.collateral_amount, self.collateral_token)} to Aave V3"
        )
        return Intent.supply(
            protocol="aave_v3",
            token=self.collateral_token,
            amount=self.collateral_amount,
            use_as_collateral=True,
            chain=self.chain,
        )

    def _create_borrow_intent(
        self, collateral_price: Decimal, borrow_price: Decimal
    ) -> Intent:
        """Create BORROW intent against supplied collateral."""
        collateral_value = self.collateral_amount * collateral_price
        borrow_value = collateral_value * self.ltv_target
        borrow_amount = (borrow_value / borrow_price).quantize(Decimal("0.01"))

        logger.info(
            f"BORROW: collateral_value={format_usd(collateral_value)}, "
            f"LTV={self.ltv_target * 100:.0f}%, "
            f"borrow={format_token_amount_human(borrow_amount, self.borrow_token)}"
        )
        return Intent.borrow(
            protocol="aave_v3",
            collateral_token=self.collateral_token,
            collateral_amount=Decimal("0"),  # Already supplied
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            interest_rate_mode=self.interest_rate_mode,
            chain=self.chain,
        )

    def _create_repay_intent(self) -> Intent:
        """Create REPAY intent to repay the borrowed amount.

        Uses repay_full=True to repay the entire outstanding debt including
        any accrued interest. This is the safest approach for lifecycle testing.
        """
        logger.info(
            f"REPAY: repaying {format_token_amount_human(self._borrowed_amount, self.borrow_token)} "
            f"(repay_full=True) on Aave V3"
        )
        return Intent.repay(
            protocol="aave_v3",
            token=self.borrow_token,
            amount=self._borrowed_amount if self._borrowed_amount > 0 else Decimal("1"),
            repay_full=True,
            chain=self.chain,
        )

    # =========================================================================
    # Lifecycle hooks
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Handle execution results and advance state machine."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._state = "supplied"
                self._supplied_amount = self.collateral_amount
                logger.info(
                    f"SUPPLY succeeded: {self._supplied_amount} {self.collateral_token}. "
                    f"State -> supplied"
                )

            elif intent_type == "BORROW":
                self._state = "borrowed"
                if hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))
                logger.info(
                    f"BORROW succeeded: {self._borrowed_amount} {self.borrow_token}. "
                    f"State -> borrowed"
                )

            elif intent_type == "REPAY":
                self._state = "complete"
                self._borrowed_amount = Decimal("0")
                logger.info(
                    f"REPAY succeeded: debt cleared. State -> complete"
                )

        else:
            # On failure, revert to previous stable state
            logger.warning(
                f"{intent_type} FAILED. Reverting state: {self._state} -> {self._previous_stable_state}"
            )
            self._state = self._previous_stable_state

    # =========================================================================
    # Status and persistence
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "aave_lending_lifecycle",
            "chain": self.chain,
            "state": self._state,
            "supplied": f"{self._supplied_amount} {self.collateral_token}",
            "borrowed": f"{self._borrowed_amount} {self.borrow_token}",
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            self._state = state["state"]
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
        if "borrowed_amount" in state:
            self._borrowed_amount = Decimal(str(state["borrowed_amount"]))
        logger.info(f"Restored state: {self._state}, supplied={self._supplied_amount}, borrowed={self._borrowed_amount}")
