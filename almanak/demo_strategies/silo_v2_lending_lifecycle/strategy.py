"""Silo V2 Supply Lifecycle on Avalanche.

Silo V2 is an isolated lending protocol where each market consists of two
ERC-4626 vaults paired together. This strategy tests the supply lifecycle
using the WAVAX/USDC market.

Lifecycle steps (one per iteration, sequential):
1. SUPPLY: Deposit USDC into the USDC silo vault
2. HOLD: Supply deposited (WITHDRAW blocked — Anvil fork revert, see VIB-2726)

Uses the WAVAX/USDC market USDC silo (most liquid market pair on Silo V2 Avalanche).

Silo V2 architecture:
- Isolated pair markets: each market has exactly 2 vaults (silo0 + silo1)
- WAVAX/USDC market: silo0 = WAVAX vault, silo1 = USDC vault
- No enterMarkets needed — depositing into a silo is an ERC-4626 deposit
- Supported markets: WAVAX/USDC, sAVAX/WAVAX, BTC.b/WAVAX, and 35+ others

Known issues:
- WITHDRAW reverts on Anvil fork with empty 0x revert data (cross-contract
  SiloConfig calls fail to fetch storage from forked Avalanche state).
  Same class of issue as VIB-422 (TraderJoe V2 LP_CLOSE on Avalanche).
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.intents import Intent
from almanak.framework.market import MarketSnapshot
from almanak.framework.strategies import IntentStrategy, almanak_strategy
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
    name="silo_v2_lending_lifecycle_avalanche",
    description="Silo V2 supply lifecycle on Avalanche: deposit USDC (withdraw blocked on Anvil)",
    version="2.0.0",
    author="Kitchen Loop",
    tags=["kitchenloop", "lending", "silo_v2", "avalanche", "lifecycle"],
    supported_chains=["avalanche"],
    supported_protocols=["silo_v2"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
    default_chain="avalanche",
)
class SiloV2LendingLifecycleAvalancheStrategy(IntentStrategy):
    """Silo V2 supply lifecycle on Avalanche.

    Deposit USDC into the USDC silo vault, then withdraw.
    Tests the Silo V2 ERC-4626 deposit/withdraw flow.

    NOTE: WITHDRAW is currently blocked on Anvil forks — Silo V2's
    withdraw triggers cross-contract SiloConfig calls that revert
    with empty 0x data on forked Avalanche state.
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
            f"SiloV2LendingLifecycleAvalanche initialized: "
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

            # Step 1: SUPPLY (deposit into silo vault)
            if self._loop_state == IDLE:
                logger.info(
                    f"Step 1: SUPPLY {format_token_amount_human(self.supply_amount, self.supply_token)} "
                    f"to Silo V2"
                )
                self._transition(SUPPLYING)

                return Intent.supply(
                    protocol="silo_v2",
                    token=self.supply_token,
                    amount=self.supply_amount,
                    chain=self.chain,
                )

            # Step 2: HOLD (WITHDRAW blocked on Anvil — see docstring / VIB-2726)
            if self._loop_state == SUPPLIED:
                logger.info("Step 2: HOLD — WITHDRAW blocked on Anvil fork (VIB-2726)")
                self._transition(COMPLETE)
                return Intent.hold(
                    reason="Supply deposited. WITHDRAW blocked on Anvil fork (VIB-2726)"
                )

            # Done
            if self._loop_state == COMPLETE:
                return Intent.hold(
                    reason="Supply lifecycle complete: deposit -> hold (withdraw blocked)"
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
                    f"SUPPLY succeeded: deposited {self._supplied_amount} {self.supply_token} to Silo V2"
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
            "strategy": "silo_v2_lending_lifecycle_avalanche",
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
                    position_id=f"silo_v2-supply-{self.supply_token}-avalanche",
                    chain=self.chain,
                    protocol="silo_v2",
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
                    protocol="silo_v2",
                    withdraw_all=True,
                    chain=self.chain,
                )
            )

        return intents
