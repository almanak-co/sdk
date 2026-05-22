"""
Spark Protocol Full Lending Lifecycle on Ethereum
===================================================

Demonstrates the complete Spark lending lifecycle on Ethereum:

  1. SUPPLY wstETH collateral to Spark
  2. BORROW DAI against wstETH (variable rate)
  3. REPAY DAI debt (repay_full=True)
  4. WITHDRAW wstETH collateral

Spark is a MakerDAO/Sky ecosystem fork of Aave V3, focused on DAI-centric
lending markets. This validates the full supply/borrow/repay/withdraw flow
with wstETH as collateral and DAI as the borrow asset.

USAGE:
------
    # Run full lifecycle on Anvil
    almanak strat run -d almanak/demo_strategies/spark_lending_lifecycle --network anvil --once

    # Supply only
    Edit config.json: "force_action": "supply"
    almanak strat run -d almanak/demo_strategies/spark_lending_lifecycle --network anvil --once
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="spark_lending_lifecycle",
    description="Spark full lending lifecycle on Ethereum: supply wstETH -> borrow DAI -> repay -> withdraw",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "lending", "lifecycle", "spark", "ethereum", "wstETH", "DAI"],
    supported_chains=["ethereum"],
    supported_protocols=["spark"],
    intent_types=["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"],
    default_chain="ethereum",
)
class SparkLendingLifecycleStrategy(IntentStrategy):
    """Spark lending full lifecycle on Ethereum: supply wstETH -> borrow DAI -> repay -> withdraw.

    State machine:
        idle -> supplying -> supplied -> borrowing -> borrowed
            -> repaying -> repaid -> withdrawing -> complete

    Config parameters:
        collateral_token: Token to supply as collateral (default: wstETH)
        collateral_amount: Amount to supply (default: 1)
        borrow_token: Token to borrow (default: DAI)
        ltv_target: Target loan-to-value ratio (default: 0.3 = 30%)
        borrow_amount_override: Fixed borrow amount (bypasses price lookup)
        force_action: "supply", "borrow", "repay", "withdraw", or "lifecycle"
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.collateral_token = str(self.get_config("collateral_token", "wstETH"))
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "1")))
        self.borrow_token = str(self.get_config("borrow_token", "DAI"))
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.3")))
        self.force_action = str(self.get_config("force_action", "")).lower()
        borrow_override = self.get_config("borrow_amount_override", "")
        self.borrow_amount_override = Decimal(str(borrow_override)) if borrow_override else None

        self._state = "idle"
        self._previous_stable_state = "idle"
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")

        logger.info(
            f"SparkLendingLifecycleStrategy initialized: "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"borrow={self.borrow_token}, LTV={self.ltv_target * 100:.0f}%, "
            f"force_action={self.force_action or 'none'}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make lending decision based on state machine and market data."""
        try:
            collateral_price, borrow_price = self._get_prices(market)

            if self.force_action == "supply":
                if self._state == "supplying":
                    return Intent.hold(reason="Waiting for supply to complete")
                self._transition("supplying")
                return self._create_supply_intent()
            if self.force_action == "borrow":
                if self._state == "borrowing":
                    return Intent.hold(reason="Waiting for borrow to complete")
                if (borrow_price <= 0 or collateral_price <= 0) and self.borrow_amount_override is None:
                    return Intent.hold(reason="Prices unavailable, cannot calculate borrow amount")
                self._transition("borrowing")
                return self._create_borrow_intent(collateral_price, borrow_price)
            if self.force_action == "repay":
                if self._state == "repaying":
                    return Intent.hold(reason="Waiting for repay to complete")
                self._transition("repaying")
                return self._create_repay_intent()
            if self.force_action == "withdraw":
                if self._state == "withdrawing":
                    return Intent.hold(reason="Waiting for withdraw to complete")
                self._transition("withdrawing")
                return self._create_withdraw_intent()
            if self.force_action == "lifecycle":
                return self._lifecycle_step(collateral_price, borrow_price)

            if self._state == "idle":
                self._transition("supplying")
                return self._create_supply_intent()

            return Intent.hold(reason=f"State: {self._state}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e}")

    def _lifecycle_step(
        self,
        collateral_price: Decimal,
        borrow_price: Decimal,
    ) -> Intent | None:
        """Execute the next step in the supply -> borrow -> repay -> withdraw lifecycle."""
        logger.info(f"Lifecycle mode: current state = {self._state}")

        if self._state == "idle":
            self._transition("supplying")
            return self._create_supply_intent()

        if self._state == "supplied":
            if (borrow_price <= 0 or collateral_price <= 0) and self.borrow_amount_override is None:
                logger.warning("Prices unavailable -- cannot calculate borrow amount. Holding.")
                return Intent.hold(reason="Prices unavailable, skipping borrow step")
            self._transition("borrowing")
            return self._create_borrow_intent(collateral_price, borrow_price)

        if self._state == "borrowed":
            self._transition("repaying")
            return self._create_repay_intent()

        if self._state == "repaid":
            self._transition("withdrawing")
            return self._create_withdraw_intent()

        if self._state == "complete":
            return Intent.hold(
                reason="Lifecycle complete: supply -> borrow -> repay -> withdraw all succeeded on Spark Ethereum"
            )

        if self._state in ("supplying", "borrowing", "repaying", "withdrawing"):
            return Intent.hold(reason=f"Waiting for {self._state} to complete")

        return Intent.hold(reason=f"Unknown state: {self._state}")

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _transition(self, new_state: str) -> None:
        old_state = self._state
        self._previous_stable_state = old_state
        self._state = new_state
        logger.info(f"State transition: {old_state} -> {new_state}")
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"State: {old_state} -> {new_state}",
                deployment_id=self.deployment_id,
                details={"old_state": old_state, "new_state": new_state},
            )
        )

    def _get_prices(self, market: MarketSnapshot) -> tuple[Decimal, Decimal]:
        try:
            collateral_price = market.price(self.collateral_token)
            borrow_price = market.price(self.borrow_token)
            logger.info(
                f"Prices: {self.collateral_token}=${collateral_price:.2f}, "
                f"{self.borrow_token}=${borrow_price:.2f}"
            )
            return collateral_price, borrow_price
        except (ValueError, KeyError) as e:
            logger.warning(f"Price fetch failed: {e}, returning zeros")
            return Decimal("0"), Decimal("0")

    def _create_supply_intent(self) -> Intent:
        logger.info(
            f"SUPPLY: {format_token_amount_human(self.collateral_amount, self.collateral_token)} to Spark on Ethereum"
        )
        return Intent.supply(
            protocol="spark",
            token=self.collateral_token,
            amount=self.collateral_amount,
            use_as_collateral=True,
            chain=self.chain,
        )

    def _create_borrow_intent(
        self, collateral_price: Decimal, borrow_price: Decimal
    ) -> Intent:
        collateral_value = self.collateral_amount * collateral_price
        if self.borrow_amount_override is not None:
            borrow_amount = self.borrow_amount_override
            logger.info(f"Using fixed borrow_amount_override: {borrow_amount} {self.borrow_token}")
        else:
            borrow_value = collateral_value * self.ltv_target
            borrow_amount = (borrow_value / borrow_price).quantize(Decimal("0.000001"), rounding=ROUND_DOWN)

        logger.info(
            f"BORROW: collateral_value={format_usd(collateral_value)}, "
            f"LTV={self.ltv_target * 100:.0f}%, "
            f"borrow={format_token_amount_human(borrow_amount, self.borrow_token)}"
        )
        return Intent.borrow(
            protocol="spark",
            collateral_token=self.collateral_token,
            collateral_amount=Decimal("0"),
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            interest_rate_mode="variable",
            chain=self.chain,
        )

    def _create_repay_intent(self) -> Intent:
        logger.info(
            f"REPAY: repaying {format_token_amount_human(self._borrowed_amount, self.borrow_token)} "
            f"(repay_full=True) on Spark Ethereum"
        )
        return Intent.repay(
            protocol="spark",
            token=self.borrow_token,
            amount=self._borrowed_amount,
            repay_full=True,
            chain=self.chain,
        )

    def _create_withdraw_intent(self) -> Intent:
        logger.info(
            f"WITHDRAW: withdrawing {format_token_amount_human(self._supplied_amount, self.collateral_token)} "
            f"(withdraw_all=True) from Spark Ethereum"
        )
        return Intent.withdraw(
            protocol="spark",
            token=self.collateral_token,
            amount=self._supplied_amount,
            withdraw_all=True,
            chain=self.chain,
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._state = "supplied"
                self._supplied_amount = self.collateral_amount
                logger.info(f"SUPPLY succeeded: {self._supplied_amount} {self.collateral_token}. State -> supplied")

            elif intent_type == "BORROW":
                self._state = "borrowed"
                if hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))
                logger.info(f"BORROW succeeded: {self._borrowed_amount} {self.borrow_token}. State -> borrowed")

            elif intent_type == "REPAY":
                self._state = "repaid"
                self._borrowed_amount = Decimal("0")
                logger.info("REPAY succeeded: debt cleared. State -> repaid")

            elif intent_type == "WITHDRAW":
                self._state = "complete"
                self._supplied_amount = Decimal("0")
                logger.info("WITHDRAW succeeded: collateral recovered. State -> complete")

            add_event(
                TimelineEvent(
                    timestamp=datetime.now(UTC),
                    event_type=TimelineEventType.POSITION_MODIFIED,
                    description=f"{intent_type} succeeded on Spark Ethereum",
                    deployment_id=self.deployment_id,
                    details={"action": intent_type.lower(), "state": self._state},
                )
            )

        else:
            logger.warning(
                f"{intent_type} FAILED. Reverting state: {self._state} -> {self._previous_stable_state}"
            )
            self._state = self._previous_stable_state

    # =========================================================================
    # STATUS & STATE PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "spark_lending_lifecycle",
            "chain": self.chain,
            "state": self._state,
            "supplied": f"{self._supplied_amount} {self.collateral_token}",
            "borrowed": f"{self._borrowed_amount} {self.borrow_token}",
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            self._state = state["state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
        if "borrowed_amount" in state:
            self._borrowed_amount = Decimal(str(state["borrowed_amount"]))
        logger.info(
            f"Restored state: {self._state}, "
            f"supplied={self._supplied_amount}, borrowed={self._borrowed_amount}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"spark-supply-{self.collateral_token}-{self.chain}",
                    chain=self.chain,
                    protocol="spark",
                    value_usd=Decimal("0"),
                    details={"asset": self.collateral_token, "amount": str(self._supplied_amount)},
                )
            )
        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"spark-borrow-{self.borrow_token}-{self.chain}",
                    chain=self.chain,
                    protocol="spark",
                    value_usd=Decimal("0"),
                    details={"asset": self.borrow_token, "amount": str(self._borrowed_amount)},
                )
            )
        return TeardownPositionSummary(
            deployment_id=self.deployment_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        intents = []
        if self._borrowed_amount > 0:
            intents.append(
                Intent.repay(
                    protocol="spark",
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    repay_full=True,
                    chain=self.chain,
                )
            )
        if self._supplied_amount > 0:
            intents.append(
                Intent.withdraw(
                    protocol="spark",
                    token=self.collateral_token,
                    amount=self._supplied_amount,
                    withdraw_all=True,
                    chain=self.chain,
                )
            )
        return intents
