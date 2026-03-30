"""
BENQI Full Lending Lifecycle on Avalanche (Demo)
=================================================

Demo strategy showcasing the complete BENQI lending lifecycle on Avalanche.
Promoted from incubating after Kitchen Loop validation (iter 52, 97).

BENQI is a Compound V2 fork on Avalanche using qiToken architecture:
- Supply mints qiTokens (e.g., qiUSDC)
- Collateral enabled via Comptroller.enterMarkets()
- Borrow from qiToken markets against collateral
- Repay via repayBorrow(uint256) for ERC20 tokens
- Withdraw via redeemUnderlying

Lifecycle steps (one per iteration):
1. BORROW: Supply USDC collateral + enterMarkets + borrow USDT
2. REPAY: Repay the borrowed USDT (repay_full with MAX_UINT256 approve)
3. WITHDRAW: Withdraw USDC collateral (amount-based, not withdraw_all)
4. HOLD: Lifecycle complete

Coverage gaps filled:
- First BENQI connector test ever (connector exists, never exercised)
- First lending lifecycle on Avalanche
- Tests Compound V2 qiToken architecture (distinct from Aave V3 / Compound V3)
- Tests ERC20 borrow/repay (USDT via qiUSDT)
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
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
    name="benqi_lending_lifecycle",
    description="BENQI full lending lifecycle on Avalanche: borrow -> repay -> withdraw",
    version="1.0.0",
    author="Kitchen Loop",
    tags=["kitchenloop", "lending", "benqi", "lifecycle", "avalanche"],
    supported_chains=["avalanche"],
    default_chain="avalanche",
    supported_protocols=["benqi"],
    intent_types=["BORROW", "REPAY", "WITHDRAW", "HOLD"],
)
class BenqiLendingLifecycleStrategy(IntentStrategy):
    """BENQI full lending lifecycle strategy on Avalanche.

    Tests the complete borrow -> repay -> withdraw lifecycle through
    the intent system on the BENQI protocol (Compound V2 fork).
    Supply USDC collateral, borrow USDT, repay USDT, withdraw USDC.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Borrow config
        self.collateral_token = self.get_config("collateral_token", "USDC")
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "500")))
        self.borrow_token = self.get_config("borrow_token", "USDT")
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.2")))

        # State machine: idle -> borrowing -> borrowed -> repaying -> repaid -> withdrawing -> complete
        self._loop_state = "idle"
        self._previous_stable_state = "idle"
        self._collateral_supplied = Decimal("0")
        self._borrowed_amount = Decimal("0")

        logger.info(
            f"BenqiLendingLifecycleStrategy initialized: "
            f"chain={self.chain}, protocol=benqi, "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"borrow_token={self.borrow_token}, LTV={self.ltv_target * 100:.0f}%"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make a lending decision based on lifecycle state."""
        try:
            # State: IDLE -> BORROW (needs prices to calculate borrow amount)
            if self._loop_state == "idle":
                try:
                    collateral_price = market.price(self.collateral_token)
                    borrow_price = market.price(self.borrow_token)
                    logger.info(
                        f"Prices: {self.collateral_token}=${collateral_price:.2f}, "
                        f"{self.borrow_token}=${borrow_price:.2f}"
                    )
                except (ValueError, KeyError) as e:
                    logger.warning(f"Could not get prices: {e}, skipping this iteration")
                    return Intent.hold(reason="Price data unavailable, skipping this iteration")

                if collateral_price == Decimal("0") or borrow_price == Decimal("0"):
                    return Intent.hold(reason="Price data unavailable, skipping this iteration")

                logger.info("State: IDLE -> Supplying collateral and borrowing")
                self._previous_stable_state = self._loop_state
                self._loop_state = "borrowing"
                return self._create_borrow_intent(collateral_price, borrow_price)

            # State: BORROWED -> REPAY (repay the debt; no price needed)
            if self._loop_state == "borrowed":
                logger.info("State: BORROWED -> Repaying debt")
                self._previous_stable_state = self._loop_state
                self._loop_state = "repaying"
                return self._create_repay_intent()

            # State: REPAID -> WITHDRAW (reclaim collateral)
            if self._loop_state == "repaid":
                logger.info("State: REPAID -> Withdrawing collateral")
                self._previous_stable_state = self._loop_state
                self._loop_state = "withdrawing"
                return self._create_withdraw_intent()

            # State: COMPLETE -> HOLD
            if self._loop_state == "complete":
                return Intent.hold(reason="Full lifecycle complete: borrow -> repay -> withdraw")

            # Stuck in transitional state -- revert to last stable state
            if self._loop_state in ("borrowing", "repaying", "withdrawing"):
                revert_to = self._previous_stable_state
                logger.warning(
                    f"Stuck in transitional state '{self._loop_state}' -- reverting to '{revert_to}'"
                )
                self._loop_state = revert_to

            return Intent.hold(reason=f"Waiting for state transition (current: {self._loop_state})")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")

    def _create_borrow_intent(self, collateral_price: Decimal, borrow_price: Decimal) -> Intent:
        """Create a BORROW intent: supply collateral, enter markets, and borrow the configured token."""
        collateral_value = self.collateral_amount * collateral_price
        borrow_amount = (collateral_value * self.ltv_target / borrow_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

        logger.info(
            f"BORROW intent: collateral={format_token_amount_human(self.collateral_amount, self.collateral_token)} "
            f"(value={format_usd(collateral_value)}), "
            f"LTV={self.ltv_target * 100:.0f}%, "
            f"borrow={format_token_amount_human(borrow_amount, self.borrow_token)}"
        )

        return Intent.borrow(
            protocol="benqi",
            collateral_token=self.collateral_token,
            collateral_amount=self.collateral_amount,
            borrow_token=self.borrow_token,
            borrow_amount=borrow_amount,
            chain=self.chain,
        )

    def _create_repay_intent(self) -> Intent:
        """Create a REPAY intent: repay the configured borrow token."""
        logger.info(
            f"REPAY intent: repay {format_token_amount_human(self._borrowed_amount, self.borrow_token)} "
            f"to BENQI (repay_full with MAX_UINT256 approve)"
        )

        return Intent.repay(
            token=self.borrow_token,
            amount=self._borrowed_amount,
            protocol="benqi",
            repay_full=True,
            chain=self.chain,
        )

    def _create_withdraw_intent(self) -> Intent:
        """Create a WITHDRAW intent: reclaim USDC collateral."""
        logger.info(
            f"WITHDRAW intent: withdraw {format_token_amount_human(self._collateral_supplied, self.collateral_token)} "
            f"collateral from BENQI (amount-based)"
        )

        return Intent.withdraw(
            token=self.collateral_token,
            amount=self._collateral_supplied,
            protocol="benqi",
            withdraw_all=False,
            chain=self.chain,
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track execution results and advance the state machine."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "BORROW":
                self._loop_state = "borrowed"
                self._collateral_supplied = self.collateral_amount
                if hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))
                logger.info(
                    f"BORROW successful: collateral={self._collateral_supplied} {self.collateral_token}, "
                    f"borrowed={self._borrowed_amount} {self.borrow_token} -- state -> borrowed"
                )

            elif intent_type == "REPAY":
                self._loop_state = "repaid"
                self._borrowed_amount = Decimal("0")
                logger.info("REPAY successful: debt cleared -- state -> repaid")

            elif intent_type == "WITHDRAW":
                self._loop_state = "complete"
                self._collateral_supplied = Decimal("0")
                logger.info("WITHDRAW successful: collateral reclaimed -- state -> complete")

        else:
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type} failed in state '{self._loop_state}' -- reverting to '{revert_to}'")
            self._loop_state = revert_to

    def get_status(self) -> dict[str, Any]:
        """Get current strategy status."""
        return {
            "strategy": "benqi_lending_lifecycle",
            "chain": self.chain,
            "protocol": "benqi",
            "state": self._loop_state,
            "collateral_supplied": str(self._collateral_supplied),
            "borrowed": str(self._borrowed_amount),
        }

    def get_persistent_state(self) -> dict[str, Any]:
        """Get state to persist for crash recovery."""
        return {
            "loop_state": self._loop_state,
            "previous_stable_state": self._previous_stable_state,
            "collateral_supplied": str(self._collateral_supplied),
            "borrowed_amount": str(self._borrowed_amount),
        }

    _VALID_STATES = frozenset({"idle", "borrowing", "borrowed", "repaying", "repaid", "withdrawing", "complete"})

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        """Load persisted state on startup."""
        if "loop_state" in state and state["loop_state"] in self._VALID_STATES:
            self._loop_state = state["loop_state"]
        if "previous_stable_state" in state and state["previous_stable_state"] in self._VALID_STATES:
            self._previous_stable_state = state["previous_stable_state"]
        if "collateral_supplied" in state:
            try:
                self._collateral_supplied = Decimal(str(state["collateral_supplied"]))
            except Exception:
                logger.warning(f"Invalid collateral_supplied in persisted state: {state['collateral_supplied']!r}")
        if "borrowed_amount" in state:
            try:
                self._borrowed_amount = Decimal(str(state["borrowed_amount"]))
            except Exception:
                logger.warning(f"Invalid borrowed_amount in persisted state: {state['borrowed_amount']!r}")

    def supports_teardown(self) -> bool:
        return True

    # Teardown interface
    def get_open_positions(self) -> "TeardownPositionSummary":
        """Get open positions for teardown."""
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        # Fetch live prices for position valuation
        try:
            market = self.create_market_snapshot()
            collateral_price = Decimal(str(market.price(self.collateral_token)))
            borrow_price = Decimal(str(market.price(self.borrow_token)))
        except Exception:
            logger.warning("Unable to fetch live prices in teardown valuation")
            collateral_price = Decimal("0")
            borrow_price = Decimal("0")

        positions: list[PositionInfo] = []

        if self._collateral_supplied > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"benqi-collateral-{self.collateral_token}-{self.chain}",
                    chain=self.chain,
                    protocol="benqi",
                    value_usd=self._collateral_supplied * collateral_price,
                    details={"asset": self.collateral_token, "type": "collateral"},
                )
            )

        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"benqi-borrow-{self.borrow_token}-{self.chain}",
                    chain=self.chain,
                    protocol="benqi",
                    value_usd=self._borrowed_amount * borrow_price,
                    details={"asset": self.borrow_token},
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Generate intents to unwind positions: repay -> withdraw collateral."""
        intents = []

        if self._borrowed_amount > 0:
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="benqi",
                    repay_full=True,
                    chain=self.chain,
                )
            )

        if self._collateral_supplied > 0:
            intents.append(
                Intent.withdraw(
                    token=self.collateral_token,
                    amount=self._collateral_supplied,
                    protocol="benqi",
                    withdraw_all=False,
                    chain=self.chain,
                )
            )

        return intents
