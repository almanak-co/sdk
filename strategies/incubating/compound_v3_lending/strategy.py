"""
Compound V3 Lending Lifecycle Strategy
=======================================

Kitchen Loop iteration 28 strategy. First test of the Compound V3 connector
through the intent system.

Tests two paths:
1. SUPPLY: Lend USDC to the USDC Comet market (earn interest)
2. BORROW: Supply WETH as collateral, borrow USDC from the USDC Comet market

Compound V3 (Comet) model:
- Each market has ONE base asset (e.g., USDC)
- supply(USDC) = lend USDC, earn interest
- supply_collateral(WETH) + borrow(USDC) = leveraged borrow
- Collateral assets are NOT rehypothecated (different from Aave V3)
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="compound_v3_lending",
    description="Compound V3 lending lifecycle - supply and borrow on Comet markets",
    version="1.0.0",
    author="Kitchen Loop",
    tags=["kitchenloop", "lending", "compound-v3"],
    supported_chains=["ethereum", "arbitrum"],
    supported_protocols=["compound_v3"],
    intent_types=["SUPPLY", "BORROW", "HOLD"],
)
class CompoundV3LendingStrategy(IntentStrategy):
    """Compound V3 lending lifecycle strategy.

    Exercises the Compound V3 connector through the intent system.
    Supports two modes via force_action config:
    - "supply": Lend USDC to earn interest
    - "borrow": Supply WETH collateral + borrow USDC
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Supply config (lending)
        self.supply_token = self.get_config("supply_token", "USDC")
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "1000")))

        # Borrow config (collateralized borrowing)
        self.collateral_token = self.get_config("collateral_token", "WETH")
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "0.5")))
        self.borrow_token = self.get_config("borrow_token", "USDC")
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.4")))

        # Compound V3 market (which Comet contract to use)
        self.market = self.get_config("market", "usdc")

        # Force action for testing
        self.force_action = str(self.get_config("force_action", "")).lower()

        # State tracking
        self._loop_state = "idle"
        self._previous_stable_state = "idle"
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")

        logger.info(
            f"CompoundV3LendingStrategy initialized: "
            f"market={self.market}, chain={self.chain}, "
            f"force_action={self.force_action or 'none'}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make a lending decision.

        Routes to supply or borrow based on force_action config.
        Without force_action, follows a state machine:
        idle -> supply collateral -> borrow -> complete.
        """
        try:
            # Get prices for calculations
            try:
                collateral_price = market.price(self.collateral_token)
                borrow_price = market.price(self.borrow_token)
                logger.info(
                    f"Prices: {self.collateral_token}=${collateral_price:.2f}, "
                    f"{self.borrow_token}=${borrow_price:.2f}"
                )
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get prices: {e}")
                collateral_price = Decimal("2500")
                borrow_price = Decimal("1")

            # Handle forced actions (for --once testing)
            if self.force_action == "supply":
                logger.info("Forced action: SUPPLY to Compound V3")
                return self._create_supply_intent()

            if self.force_action == "borrow":
                logger.info("Forced action: BORROW from Compound V3")
                return self._create_borrow_intent(collateral_price, borrow_price)

            # State machine logic for multi-iteration runs
            if self._loop_state == "idle":
                logger.info("State: IDLE -> Supplying collateral for borrow")
                self._previous_stable_state = self._loop_state
                self._loop_state = "borrowing"
                return self._create_borrow_intent(collateral_price, borrow_price)

            if self._loop_state == "borrowed":
                return Intent.hold(reason="Borrow position established - holding")

            if self._loop_state == "complete":
                return Intent.hold(reason="Lifecycle complete")

            # Stuck in transitional state -- revert
            if self._loop_state in ("borrowing", "supplying"):
                revert_to = self._previous_stable_state
                logger.warning(
                    f"Stuck in transitional state '{self._loop_state}' -- reverting to '{revert_to}'"
                )
                self._loop_state = revert_to
            return Intent.hold(reason=f"Waiting for state transition (current: {self._loop_state})")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")

    def _create_supply_intent(self) -> Intent:
        """Create a SUPPLY intent to lend base asset (USDC) to Compound V3.

        In Compound V3, supplying the base asset means lending it to earn interest.
        This is different from supplying collateral (which is done via BorrowIntent).
        """
        logger.info(
            f"SUPPLY intent: {format_token_amount_human(self.supply_amount, self.supply_token)} "
            f"to Compound V3 {self.market} market"
        )

        return Intent.supply(
            protocol="compound_v3",
            token=self.supply_token,
            amount=self.supply_amount,
            market_id=self.market,
            chain=self.chain,
        )

    def _create_borrow_intent(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """Create a BORROW intent to supply collateral and borrow from Compound V3.

        In Compound V3:
        1. Supply collateral asset (e.g., WETH) to the Comet contract
        2. Borrow the base asset (e.g., USDC) against it
        """
        collateral_value = self.collateral_amount * collateral_price
        borrow_amount = (collateral_value * self.ltv_target / borrow_price).quantize(Decimal("0.01"))

        logger.info(
            f"BORROW intent: collateral={format_token_amount_human(self.collateral_amount, self.collateral_token)} "
            f"(value={format_usd(collateral_value)}), "
            f"LTV={self.ltv_target * 100:.0f}%, "
            f"borrow={format_token_amount_human(borrow_amount, self.borrow_token)}"
        )

        return Intent.borrow(
            protocol="compound_v3",
            collateral_token=self.collateral_token,
            collateral_amount=self.collateral_amount,
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            market_id=self.market,
            chain=self.chain,
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track execution results for state machine."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._loop_state = "supplied"
                self._supplied_amount = self.supply_amount
                logger.info(f"SUPPLY successful: {self.supply_amount} {self.supply_token} to Compound V3")

            elif intent_type == "BORROW":
                self._loop_state = "borrowed"
                if hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))
                logger.info(
                    f"BORROW successful: collateral={self.collateral_amount} {self.collateral_token}, "
                    f"borrowed={self._borrowed_amount} {self.borrow_token}"
                )
        else:
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type} failed in state '{self._loop_state}' -- reverting to '{revert_to}'")
            self._loop_state = revert_to

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "compound_v3_lending",
            "chain": self.chain,
            "market": self.market,
            "state": self._loop_state,
            "supplied": str(self._supplied_amount),
            "borrowed": str(self._borrowed_amount),
        }

    def get_persistent_state(self) -> dict[str, Any]:
        """Get state to persist for crash recovery."""
        return {
            "loop_state": self._loop_state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persisted state on startup."""
        if "loop_state" in state:
            self._loop_state = state["loop_state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
        if "borrowed_amount" in state:
            self._borrowed_amount = Decimal(str(state["borrowed_amount"]))

    # Teardown interface
    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get open positions for teardown."""
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []

        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"compound-supply-{self.supply_token}-{self.chain}",
                    chain=self.chain,
                    protocol="compound_v3",
                    value_usd=self._supplied_amount,
                    details={"asset": self.supply_token, "market": self.market},
                )
            )

        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"compound-borrow-{self.borrow_token}-{self.chain}",
                    chain=self.chain,
                    protocol="compound_v3",
                    value_usd=self._borrowed_amount,
                    details={"asset": self.borrow_token, "market": self.market},
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to unwind positions.

        Order: repay borrow -> withdraw collateral -> withdraw supply
        """
        intents = []

        if self._borrowed_amount > 0:
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="compound_v3",
                    repay_full=True,
                )
            )

        if self._supplied_amount > 0:
            intents.append(
                Intent.withdraw(
                    token=self.supply_token,
                    amount=self._supplied_amount,
                    protocol="compound_v3",
                    withdraw_all=True,
                )
            )

        return intents
