"""Joe Lend (Banker Joe) Full Lending Lifecycle on Avalanche.

Joe Lend is Banker Joe's lending arm — a Compound V2 fork on Avalanche.
This strategy tests the complete jToken lifecycle with stable-to-stable lending.

Lifecycle steps (one per iteration, sequential):
1. BORROW: Supply USDC collateral + enterMarkets + borrow USDT
2. REPAY: Repay USDT debt
3. WITHDRAW: Withdraw USDC collateral
4. HOLD: Lifecycle complete

Uses USDC collateral and USDT borrow (both stablecoins) to avoid:
- WAVAX wrapping issues
- Price volatility affecting test reliability
- Native AVAX handling complexity

Joe Lend (Compound V2 fork) on Avalanche:
- jToken model: supply mints jTokens, borrow from jToken markets
- Collateral enabled via Joetroller.enterMarkets()
- Supported assets: AVAX, USDC, USDT, WETH.e, WBTC.e, DAI.e
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

# Stable states
IDLE = "idle"
BORROWED = "borrowed"
REPAID = "repaid"
COMPLETE = "complete"

# Transitional states
BORROWING = "borrowing"
REPAYING = "repaying"
WITHDRAWING = "withdrawing"

STABLE_STATES = {IDLE, BORROWED, REPAID, COMPLETE}
TRANSITIONAL_STATES = {BORROWING, REPAYING, WITHDRAWING}


@almanak_strategy(
    name="joelend_lending_lifecycle_avalanche",
    description="Joe Lend full lending lifecycle on Avalanche: supply USDC -> borrow USDT -> repay -> withdraw",
    version="1.0.0",
    author="Kitchen Loop",
    tags=["kitchenloop", "lending", "joelend", "avalanche", "lifecycle"],
    supported_chains=["avalanche"],
    supported_protocols=["joelend"],
    intent_types=["BORROW", "REPAY", "WITHDRAW", "HOLD"],
    default_chain="avalanche",
)
class JoeLendLendingLifecycleAvalancheStrategy(IntentStrategy):
    """Joe Lend (Banker Joe) full lending lifecycle on Avalanche.

    Supply USDC as collateral, borrow USDT, repay, withdraw.
    Tests the complete Joe Lend connector lifecycle with stable-to-stable lending.
    """

    def supports_teardown(self) -> bool:
        return True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.collateral_token = self.get_config("collateral_token", "USDC.e")
        self.collateral_amount = Decimal(str(self.get_config("collateral_amount", "100")))
        self.borrow_token = self.get_config("borrow_token", "USDT.e")
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.3")))

        borrow_override = self.get_config("borrow_amount_override", "")
        self.borrow_amount_override = Decimal(str(borrow_override)) if borrow_override else None

        # State machine
        self._loop_state = IDLE
        self._previous_stable_state = IDLE

        # Position tracking
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")

        logger.info(
            f"JoeLendLendingLifecycleAvalanche initialized: "
            f"collateral={self.collateral_amount} {self.collateral_token}, "
            f"borrow {self.borrow_token} LTV={self.ltv_target * 100}%"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        try:
            # Handle stuck transitional states
            if self._loop_state in TRANSITIONAL_STATES:
                stuck_state = self._loop_state
                revert_to = self._previous_stable_state
                logger.warning(f"Stuck in '{stuck_state}' -- reverting to '{revert_to}', holding this iteration")
                self._loop_state = revert_to
                return Intent.hold(reason=f"Recovered from stuck state '{stuck_state}', holding before retry")

            # Step 1: BORROW (supply collateral + enterMarkets + borrow)
            if self._loop_state == IDLE:
                if self.borrow_amount_override is not None:
                    borrow_amount = self.borrow_amount_override
                    logger.info(f"Using fixed borrow_amount_override: {borrow_amount} {self.borrow_token}")
                else:
                    try:
                        collateral_price = market.price(self.collateral_token)
                        borrow_price = market.price(self.borrow_token)
                    except (ValueError, KeyError) as e:
                        logger.warning(f"Price fetch failed: {e}")
                        return Intent.hold(reason=f"Price data unavailable: {e}")
                    collateral_value = self.collateral_amount * collateral_price
                    borrow_value = collateral_value * self.ltv_target
                    borrow_amount = (borrow_value / borrow_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

                if borrow_amount <= 0:
                    return Intent.hold(reason="Computed borrow amount is zero")

                logger.info(
                    f"Step 1: BORROW {format_token_amount_human(borrow_amount, self.borrow_token)} "
                    f"from Joe Lend (supply {self.collateral_amount} {self.collateral_token}, "
                    f"LTV={self.ltv_target * 100:.0f}%)"
                )
                self._transition(BORROWING)

                return Intent.borrow(
                    protocol="joelend",
                    collateral_token=self.collateral_token,
                    collateral_amount=self.collateral_amount,
                    borrow_token=self.borrow_token,
                    borrow_amount=borrow_amount,
                    chain=self.chain,
                )

            # Step 2: REPAY
            if self._loop_state == BORROWED:
                logger.info(
                    f"Step 2: REPAY {format_token_amount_human(self._borrowed_amount, self.borrow_token)} to Joe Lend"
                )
                self._transition(REPAYING)

                return Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="joelend",
                    repay_full=True,
                    chain=self.chain,
                )

            # Step 3: WITHDRAW
            if self._loop_state == REPAID:
                logger.info(
                    f"Step 3: WITHDRAW {format_token_amount_human(self._supplied_amount, self.collateral_token)} "
                    f"from Joe Lend"
                )
                self._transition(WITHDRAWING)

                return Intent.withdraw(
                    token=self.collateral_token,
                    amount=self._supplied_amount,
                    protocol="joelend",
                    withdraw_all=False,
                    chain=self.chain,
                )

            # Done
            if self._loop_state == COMPLETE:
                return Intent.hold(
                    reason="Full lending lifecycle complete: supply -> borrow -> repay -> withdraw"
                )

            return Intent.hold(reason=f"Unknown state: {self._loop_state}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {e!s}")

    def _transition(self, new_state: str) -> None:
        old = self._loop_state
        if old in STABLE_STATES:
            self._previous_stable_state = old
        self._loop_state = new_state
        logger.info(f"State transition: {old} -> {new_state}")

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        intent_type = getattr(intent, "intent_type", None)
        if intent_type is None:
            return
        intent_type_val = intent_type.value if hasattr(intent_type, "value") else str(intent_type)

        if success:
            if intent_type_val == "BORROW":
                self._loop_state = BORROWED
                self._previous_stable_state = BORROWED
                self._supplied_amount = self.collateral_amount
                if hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))
                logger.info(
                    f"BORROW succeeded: supplied {self._supplied_amount} {self.collateral_token}, "
                    f"borrowed {self._borrowed_amount} {self.borrow_token} from Joe Lend"
                )
                self._log_result_details("BORROW", result)

            elif intent_type_val == "REPAY":
                self._loop_state = REPAID
                self._previous_stable_state = REPAID
                self._borrowed_amount = Decimal("0")
                logger.info("REPAY succeeded -- debt cleared, state -> repaid")

            elif intent_type_val == "WITHDRAW":
                self._loop_state = COMPLETE
                self._previous_stable_state = COMPLETE
                self._supplied_amount = Decimal("0")
                logger.info("WITHDRAW succeeded -- collateral cleared, lifecycle complete")
        else:
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type_val} FAILED in state '{self._loop_state}' -- reverting to '{revert_to}'")
            self._loop_state = revert_to

    def _log_result_details(self, intent_type: str, result: Any) -> None:
        if result is None:
            return
        if hasattr(result, "extracted_data") and result.extracted_data:
            logger.info(f"  {intent_type} extracted_data: {result.extracted_data}")
        if hasattr(result, "transaction_results"):
            tx_results = result.transaction_results
            if tx_results:
                for i, tx_result in enumerate(tx_results):
                    tx_hash = getattr(tx_result, "tx_hash", "N/A")
                    gas_used = getattr(tx_result, "gas_used", "N/A")
                    logger.info(f"  {intent_type} TX {i + 1}: hash={tx_hash}, gas={gas_used}")

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._loop_state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._loop_state = state.get("state", IDLE)
        self._previous_stable_state = state.get("previous_stable_state", IDLE)
        self._supplied_amount = Decimal(str(state.get("supplied_amount", "0")))
        self._borrowed_amount = Decimal(str(state.get("borrowed_amount", "0")))
        logger.info(f"Restored state: {self._loop_state}")

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "joelend_lending_lifecycle_avalanche",
            "chain": self.chain,
            "state": self._loop_state,
            "supplied": f"{self._supplied_amount} {self.collateral_token}",
            "borrowed": f"{self._borrowed_amount} {self.borrow_token}",
        }

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        try:
            market = self.create_market_snapshot()
            collateral_price = Decimal(str(market.price(self.collateral_token)))
            borrow_price = Decimal(str(market.price(self.borrow_token)))
        except Exception:
            logger.warning("Unable to fetch live prices for teardown valuation")
            collateral_price = Decimal("0")
            borrow_price = Decimal("0")

        positions: list[PositionInfo] = []

        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"joelend-supply-{self.collateral_token}-avalanche",
                    chain=self.chain,
                    protocol="joelend",
                    value_usd=self._supplied_amount * collateral_price,
                    details={"asset": self.collateral_token, "type": "collateral"},
                )
            )

        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"joelend-borrow-{self.borrow_token}-avalanche",
                    chain=self.chain,
                    protocol="joelend",
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
        intents = []

        # 1. Repay debt
        if self._borrowed_amount > 0:
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="joelend",
                    repay_full=True,
                    chain=self.chain,
                )
            )

        # 2. Withdraw collateral
        if self._supplied_amount > 0:
            intents.append(
                Intent.withdraw(
                    token=self.collateral_token,
                    amount=self._supplied_amount,
                    protocol="joelend",
                    withdraw_all=False,
                    chain=self.chain,
                )
            )

        return intents
