"""Euler V2 Supply Lifecycle on Avalanche.

Euler V2 is a modular lending protocol using ERC-4626 vaults. Each vault
holds a single underlying asset. Cross-vault borrowing is coordinated
through the Ethereum Vault Connector (EVC).

This strategy tests the supply/withdraw lifecycle using a USDC vault.

Lifecycle steps (one per iteration, sequential):
1. SUPPLY: Deposit USDC into the eUSDC-19 vault
2. WITHDRAW: Withdraw USDC from the vault
3. HOLD: Lifecycle complete

Uses the eUSDC-19 vault (0x37ca03aD51B8ff79aAD35FadaCBA4CEDF0C3e74e) —
3.1M USDC TVL, 86% utilization, curated by Re7 Labs.

Euler V2 architecture:
- Individual ERC-4626 vaults per asset (NOT shared pools like Aave)
- EVC (Ethereum Vault Connector) for cross-vault collateral/borrow
- 247 vaults deployed on Avalanche via factory
- Standard deposit/withdraw operations (ERC-4626)
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary

# Stable states
IDLE = "idle"
SUPPLIED = "supplied"
COMPLETE = "complete"

# Transitional states
SUPPLYING = "supplying"
WITHDRAWING = "withdrawing"

STABLE_STATES = {IDLE, SUPPLIED, COMPLETE}
TRANSITIONAL_STATES = {SUPPLYING, WITHDRAWING}


@almanak_strategy(
    name="euler_v2_lending_lifecycle_avalanche",
    description="Euler V2 supply lifecycle on Avalanche: deposit USDC -> withdraw USDC",
    version="1.0.0",
    author="Kitchen Loop",
    tags=["kitchenloop", "lending", "euler_v2", "avalanche", "lifecycle"],
    supported_chains=["avalanche"],
    supported_protocols=["euler_v2"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
    default_chain="avalanche",
)
class EulerV2LendingLifecycleAvalancheStrategy(IntentStrategy):
    """Euler V2 supply lifecycle on Avalanche.

    Deposit USDC into the eUSDC-19 ERC-4626 vault, then withdraw.
    Tests the Euler V2 deposit/withdraw flow.
    """

    def supports_teardown(self) -> bool:
        return True

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.supply_token = self.get_config("supply_token", "USDC")
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "1000")))

        # State machine
        self._loop_state = IDLE
        self._previous_stable_state = IDLE

        # Position tracking
        self._supplied_amount = Decimal("0")

        logger.info(
            f"EulerV2LendingLifecycleAvalanche initialized: "
            f"supply={self.supply_amount} {self.supply_token}"
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

            # Step 1: SUPPLY (deposit into ERC-4626 vault)
            if self._loop_state == IDLE:
                logger.info(
                    f"Step 1: SUPPLY {format_token_amount_human(self.supply_amount, self.supply_token)} "
                    f"to Euler V2"
                )
                self._transition(SUPPLYING)

                return Intent.supply(
                    protocol="euler_v2",
                    token=self.supply_token,
                    amount=self.supply_amount,
                    chain=self.chain,
                )

            # Step 2: WITHDRAW (use withdraw_all to fully clear the ERC-4626 position)
            if self._loop_state == SUPPLIED:
                logger.info(
                    f"Step 2: WITHDRAW {format_token_amount_human(self._supplied_amount, self.supply_token)} "
                    f"from Euler V2"
                )
                self._transition(WITHDRAWING)

                return Intent.withdraw(
                    token=self.supply_token,
                    amount=self._supplied_amount,
                    protocol="euler_v2",
                    withdraw_all=True,
                    chain=self.chain,
                )

            # Done
            if self._loop_state == COMPLETE:
                return Intent.hold(
                    reason="Full supply lifecycle complete: deposit -> withdraw"
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
            if intent_type_val == "SUPPLY":
                self._loop_state = SUPPLIED
                self._previous_stable_state = SUPPLIED
                self._supplied_amount = self.supply_amount
                logger.info(
                    f"SUPPLY succeeded: deposited {self._supplied_amount} {self.supply_token} to Euler V2"
                )
                self._log_result_details("SUPPLY", result)

            elif intent_type_val == "WITHDRAW":
                self._loop_state = COMPLETE
                self._previous_stable_state = COMPLETE
                self._supplied_amount = Decimal("0")
                logger.info("WITHDRAW succeeded -- supply cleared, lifecycle complete")
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
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        self._loop_state = state.get("state", IDLE)
        self._previous_stable_state = state.get("previous_stable_state", IDLE)
        self._supplied_amount = Decimal(str(state.get("supplied_amount", "0")))
        logger.info(f"Restored state: {self._loop_state}")

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "euler_v2_lending_lifecycle_avalanche",
            "chain": self.chain,
            "state": self._loop_state,
            "supplied": f"{self._supplied_amount} {self.supply_token}",
        }

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        try:
            market = self.create_market_snapshot()
            supply_price = Decimal(str(market.price(self.supply_token)))
        except Exception:
            logger.warning("Unable to fetch live prices for teardown valuation")
            supply_price = Decimal("0")

        positions: list[PositionInfo] = []

        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"euler_v2-supply-{self.supply_token}-avalanche",
                    chain=self.chain,
                    protocol="euler_v2",
                    value_usd=self._supplied_amount * supply_price,
                    details={"asset": self.supply_token, "type": "deposit"},
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        intents = []

        if self._supplied_amount > 0:
            intents.append(
                Intent.withdraw(
                    token=self.supply_token,
                    amount=self._supplied_amount,
                    protocol="euler_v2",
                    withdraw_all=True,
                    chain=self.chain,
                )
            )

        return intents
