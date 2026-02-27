"""
Aave V3 Supply on Base - Chain Portability Validation (VIB-320)

Kitchen Loop iteration 26 strategy. Supplies USDC to Aave V3 on Base,
holds the aUSDC position, and withdraws when triggered by force_action
or APY floor breach. Validates that the Aave V3 connector works on Base
(previously only tested on Arbitrum).
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)
from almanak.framework.utils.log_formatters import format_token_amount_human, format_usd

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import PositionInfo, TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="aave_supply_base",
    description="Supply USDC to Aave V3 on Base - chain portability validation",
    version="1.0.0",
    author="Kitchen Loop",
    tags=["incubating", "lending", "supply", "aave-v3", "base", "chain-portability"],
    supported_chains=["base"],
    supported_protocols=["aave_v3"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
)
class AaveSupplyBaseStrategy(IntentStrategy):
    """Supply USDC to Aave V3 on Base and withdraw on APY floor breach.

    State machine:
        idle -> supplying -> supplied -> withdrawing -> withdrawn -> idle

    Transition states (supplying, withdrawing) revert to previous stable
    state on intent failure, allowing the next decide() call to retry.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Config
        self.supply_token = self.get_config("supply_token", "USDC")
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "100")))
        # APY thresholds in percentage terms (e.g., 1.0 means 1.0%)
        # LendingRate.apy_percent returns values like 5.25 for 5.25%
        self.min_apy_pct = Decimal(str(self.get_config("min_apy_pct", "1.0")))
        self.re_entry_apy_pct = Decimal(str(self.get_config("re_entry_apy_pct", "3.0")))
        self.force_action = str(self.get_config("force_action", "")).lower()

        # Internal state
        self._loop_state = "idle"  # idle | supplying | supplied | withdrawing | withdrawn
        self._previous_stable_state = "idle"
        self._supplied_amount = Decimal("0")

        logger.info(
            f"AaveSupplyBaseStrategy initialized: "
            f"supply={self.supply_amount} {self.supply_token}, "
            f"min_apy={self.min_apy_pct:.1f}%, "
            f"re_entry_apy={self.re_entry_apy_pct:.1f}%, "
            f"force_action={self.force_action or '(none)'}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Decide whether to supply, withdraw, or hold."""
        try:
            # Get price for logging
            try:
                token_price = market.price(self.supply_token)
                logger.debug(f"Price: {self.supply_token}=${token_price:.4f}")
            except (ValueError, KeyError) as e:
                logger.warning(f"Could not get {self.supply_token} price: {e}")
                token_price = Decimal("1")  # Reasonable default for USDC

            # Handle forced actions (for testing)
            if self.force_action == "supply":
                logger.info("Forced action: SUPPLY")
                return self._create_supply_intent()
            elif self.force_action == "withdraw":
                logger.info("Forced action: WITHDRAW")
                return self._create_withdraw_intent()
            elif self.force_action == "lifecycle":
                # Full lifecycle: supply if idle, withdraw if supplied, done if withdrawn
                if self._loop_state == "idle":
                    logger.info("Lifecycle: SUPPLY (idle -> supplying)")
                    self._previous_stable_state = self._loop_state
                    self._loop_state = "supplying"
                    return self._create_supply_intent()
                elif self._loop_state == "supplied":
                    logger.info("Lifecycle: WITHDRAW (supplied -> withdrawing)")
                    self._previous_stable_state = self._loop_state
                    self._loop_state = "withdrawing"
                    return self._create_withdraw_intent()
                else:
                    return Intent.hold(reason=f"Lifecycle complete (state={self._loop_state})")

            # State machine logic
            if self._loop_state == "idle":
                return self._handle_idle(market)
            elif self._loop_state == "supplied":
                return self._handle_supplied(market)
            elif self._loop_state == "withdrawn":
                return self._handle_withdrawn(market)
            elif self._loop_state in ("supplying", "withdrawing"):
                # Stuck in transitional state -- revert to previous stable state
                revert_to = self._previous_stable_state
                logger.warning(
                    f"Stuck in transitional state '{self._loop_state}' -- reverting to '{revert_to}'"
                )
                self._loop_state = revert_to
                return Intent.hold(reason=f"Reverted from {self._loop_state} to {revert_to}")
            else:
                return Intent.hold(reason=f"Unknown state: {self._loop_state}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    def _handle_idle(self, market: MarketSnapshot) -> Intent:
        """Idle state: supply if we have enough tokens."""
        try:
            balance = market.balance(self.supply_token)
            balance_value = balance.balance if hasattr(balance, "balance") else balance
        except (ValueError, KeyError):
            balance_value = self.supply_amount  # Assume we have it on Anvil

        if balance_value < self.supply_amount:
            return Intent.hold(
                reason=f"Insufficient {self.supply_token}: {balance_value} < {self.supply_amount}"
            )

        logger.info("State: IDLE -> Supplying")
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description="State: idle -> supplying",
                strategy_id=self.strategy_id,
                details={"old_state": "idle", "new_state": "supplying"},
            )
        )
        self._previous_stable_state = self._loop_state
        self._loop_state = "supplying"
        return self._create_supply_intent()

    def _handle_supplied(self, market: MarketSnapshot) -> Intent:
        """Supplied state: check APY, withdraw if below floor."""
        # Try to read lending rate (API: lending_rate(protocol, token, side))
        try:
            rate = market.lending_rate("aave_v3", self.supply_token, "supply")
            apy_pct = rate.apy_percent  # e.g., 5.25 for 5.25%
            logger.info(f"Current supply APY for {self.supply_token}: {apy_pct:.2f}%")

            if apy_pct < self.min_apy_pct:
                logger.info(
                    f"APY {apy_pct:.2f}% < min {self.min_apy_pct:.1f}% -- withdrawing"
                )
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.STATE_CHANGE,
                        description=f"APY below floor ({apy_pct:.2f}% < {self.min_apy_pct:.1f}%)",
                        strategy_id=self.strategy_id,
                        details={
                            "current_apy_pct": str(apy_pct),
                            "min_apy_pct": str(self.min_apy_pct),
                        },
                    )
                )
                self._previous_stable_state = self._loop_state
                self._loop_state = "withdrawing"
                return self._create_withdraw_intent()
        except Exception as e:
            logger.warning(f"Could not read lending rate: {e} -- holding position")

        supply_value = self._supplied_amount * (market.price(self.supply_token) if self.supply_token != "USDC" else Decimal("1"))
        return Intent.hold(
            reason=f"Holding {format_token_amount_human(self._supplied_amount, self.supply_token)} "
            f"(~{format_usd(supply_value)}) in Aave V3 on Base"
        )

    def _handle_withdrawn(self, market: MarketSnapshot) -> Intent:
        """Withdrawn state: check if APY recovered above re-entry threshold."""
        try:
            rate = market.lending_rate("aave_v3", self.supply_token, "supply")
            apy_pct = rate.apy_percent
            logger.info(f"Post-withdraw APY check: {apy_pct:.2f}%")

            if apy_pct >= self.re_entry_apy_pct:
                logger.info(
                    f"APY recovered to {apy_pct:.2f}% >= {self.re_entry_apy_pct:.1f}% -- re-entering"
                )
                self._loop_state = "idle"
                return self._handle_idle(market)
        except Exception as e:
            logger.warning(f"Could not read lending rate: {e}")

        return Intent.hold(reason="Withdrawn -- waiting for APY recovery above re-entry threshold")

    # =========================================================================
    # INTENT CREATION HELPERS
    # =========================================================================

    def _create_supply_intent(self) -> Intent:
        """Create a SUPPLY intent for Aave V3."""
        logger.info(
            f"SUPPLY intent: {format_token_amount_human(self.supply_amount, self.supply_token)} "
            f"to Aave V3 on {self.chain}"
        )
        return Intent.supply(
            protocol="aave_v3",
            token=self.supply_token,
            amount=self.supply_amount,
            use_as_collateral=True,
            chain=self.chain,
        )

    def _create_withdraw_intent(self) -> Intent:
        """Create a WITHDRAW intent from Aave V3."""
        logger.info(
            f"WITHDRAW intent: {format_token_amount_human(self._supplied_amount, self.supply_token)} "
            f"from Aave V3 on {self.chain}"
        )
        return Intent.withdraw(
            protocol="aave_v3",
            token=self.supply_token,
            amount=self._supplied_amount,
            withdraw_all=True,
            chain=self.chain,
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Update state after intent execution."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._loop_state = "supplied"
                self._supplied_amount = self.supply_amount
                logger.info(
                    f"Supply successful: {self._supplied_amount} {self.supply_token} "
                    f"deposited to Aave V3 on {self.chain}"
                )
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Supplied {self._supplied_amount} {self.supply_token} to Aave V3",
                        strategy_id=self.strategy_id,
                        details={
                            "action": "supply",
                            "amount": str(self._supplied_amount),
                            "token": self.supply_token,
                            "chain": self.chain,
                        },
                    )
                )
            elif intent_type == "WITHDRAW":
                self._loop_state = "withdrawn"
                withdrawn_amount = self._supplied_amount
                self._supplied_amount = Decimal("0")
                logger.info(
                    f"Withdraw successful: {withdrawn_amount} {self.supply_token} "
                    f"withdrawn from Aave V3 on {self.chain}"
                )
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Withdrew {withdrawn_amount} {self.supply_token} from Aave V3",
                        strategy_id=self.strategy_id,
                        details={
                            "action": "withdraw",
                            "amount": str(withdrawn_amount),
                            "token": self.supply_token,
                            "chain": self.chain,
                        },
                    )
                )
        else:
            revert_to = self._previous_stable_state
            logger.warning(
                f"{intent_type} failed in state '{self._loop_state}' -- reverting to '{revert_to}'"
            )
            self._loop_state = revert_to

    # =========================================================================
    # STATUS & STATE PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "aave_supply_base",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "supply_token": self.supply_token,
                "supply_amount": str(self.supply_amount),
                "min_apy_pct": str(self.min_apy_pct),
                "re_entry_apy_pct": str(self.re_entry_apy_pct),
            },
            "state": {
                "loop_state": self._loop_state,
                "supplied_amount": str(self._supplied_amount),
            },
        }

    def get_persistent_state(self) -> dict[str, Any]:
        """Persist state for crash recovery."""
        return {
            "loop_state": self._loop_state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Restore state on startup."""
        if "loop_state" in state:
            self._loop_state = state["loop_state"]
            logger.info(f"Restored loop_state: {self._loop_state}")
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
            logger.info(f"Restored supplied_amount: {self._supplied_amount}")

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def _get_gateway_client(self) -> Any:
        """Get the gateway client for on-chain queries, if available."""
        compiler = getattr(self, "_compiler", None)
        if compiler is not None:
            client = getattr(compiler, "_gateway_client", None)
            if client is not None:
                return client
        return None

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get all open positions for teardown."""
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []

        if self._supplied_amount > 0:
            # USDC is ~$1, so value_usd ~ supplied_amount
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.supply_token}-{self.chain}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._supplied_amount,
                    details={
                        "asset": self.supply_token,
                        "amount": str(self._supplied_amount),
                    },
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to unwind the supply position."""
        if self._supplied_amount > 0:
            return [
                Intent.withdraw(
                    protocol="aave_v3",
                    token=self.supply_token,
                    amount=self._supplied_amount,
                    withdraw_all=True,
                    chain=self.chain,
                ),
            ]
        return []

    def on_teardown_started(self, mode: "TeardownMode") -> None:
        from almanak.framework.teardown import TeardownMode

        mode_name = "graceful" if mode == TeardownMode.SOFT else "emergency"
        logger.info(f"Teardown started ({mode_name}): will withdraw {self._supplied_amount} {self.supply_token}")

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        if success:
            logger.info(f"Teardown completed. Recovered ${recovered_usd:,.2f}")
            self._loop_state = "idle"
            self._supplied_amount = Decimal("0")
        else:
            logger.error("Teardown failed - manual intervention may be required")
