"""Aave V3 Looping Strategy on Mantle.

Leverages Aave V3 lending to amplify yield through recursive supply/borrow:

1. Supply WETH as collateral
2. Borrow USDC against it
3. Swap USDC -> WETH
4. Re-supply the WETH
5. Repeat up to max_loops times

This creates leveraged long ETH exposure. Profit if WETH appreciates faster
than the USDC borrow cost. Aave V3 Mantle collateral: WETH (80.5% LTV), WMNT (40% LTV).
Borrowable: USDC, USDT0, USDe, GHO.

Teardown unwinds in reverse: repay all debt, withdraw all collateral.

Usage:
    almanak strat run -d aave_loop_mantle --network anvil --once
"""

import logging
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import IntentStrategy, MarketSnapshot, almanak_strategy
from almanak.framework.utils.log_formatters import format_token_amount_human

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import PositionInfo, TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_aave_loop_mantle",
    description="Aave V3 looping strategy on Mantle - recursive supply/borrow for leveraged yield",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "mantle", "lending", "aave-v3", "loop", "leverage"],
    supported_chains=["mantle"],
    supported_protocols=["aave_v3"],
    intent_types=["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "SWAP", "HOLD"],
    default_chain="mantle",
)
class AaveLoopMantleStrategy(IntentStrategy):
    """Aave V3 leveraged looping on Mantle.

    State machine per loop iteration:
        idle -> supplying -> supplied -> borrowing -> borrowed -> swapping -> idle (next loop)

    After max_loops: state = complete, strategy holds.

    Configuration (config.json):
        supply_token: Token to supply (default: WETH) — must be an Aave V3 Mantle reserve
        borrow_token: Token to borrow (default: USDC) — must be an Aave V3 Mantle reserve
        initial_supply_amount: First supply amount (default: 0.01)
        ltv_target: Target LTV per loop (default: 0.4 = 40%)
        max_loops: Number of supply/borrow cycles (default: 3)
        min_health_factor: Safety floor (default: 1.5)
        max_slippage_bps: Swap slippage tolerance (default: 100 = 1%)
        interest_rate_mode: "variable" (stable rate deprecated on Aave V3) (default: variable)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.supply_token = self.get_config("supply_token", "WETH")
        self.borrow_token = self.get_config("borrow_token", "USDC")
        self.initial_supply_amount = Decimal(str(self.get_config("initial_supply_amount", "0.01")))
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.4")))
        self.max_loops = int(self.get_config("max_loops", 3))
        self.min_health_factor = Decimal(str(self.get_config("min_health_factor", "1.5")))
        self.max_slippage_bps = int(self.get_config("max_slippage_bps", 100))
        self.interest_rate_mode = self.get_config("interest_rate_mode", "variable")

        # State tracking
        self._state = "idle"  # idle, supplying, supplied, borrowing, borrowed, swapping, complete
        self._previous_stable_state = "idle"
        self._current_loop = 0
        self._total_supplied = Decimal("0")
        self._total_borrowed = Decimal("0")
        self._pending_supply_amount = Decimal("0")  # Amount to supply in current loop
        self._last_borrow_amount = Decimal("0")  # Last borrow amount for swap step

        logger.info(
            f"AaveLoopMantle initialized: "
            f"supply={self.initial_supply_amount} {self.supply_token}, "
            f"borrow={self.borrow_token}, "
            f"LTV={self.ltv_target * 100:.0f}%, "
            f"max_loops={self.max_loops}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Execute the next step in the looping state machine.

        Each iteration advances one step. The runner calls decide() repeatedly
        until the strategy returns HOLD (complete or waiting).
        """
        if self._state == "complete":
            return Intent.hold(
                reason=f"Loop complete: {self._current_loop} loops, "
                f"supplied={self._total_supplied} {self.supply_token}, "
                f"borrowed={self._total_borrowed} {self.borrow_token}"
            )

        # Get prices for borrow calculations
        try:
            supply_price = market.price(self.supply_token)
            borrow_price = market.price(self.borrow_token)
        except (ValueError, KeyError) as e:
            return Intent.hold(reason=f"Price data unavailable: {e}")

        max_slippage = Decimal(str(self.max_slippage_bps)) / Decimal("10000")

        # Step 1: Supply (first loop uses initial amount, subsequent loops use swapped amount)
        if self._state == "idle":
            if self._current_loop >= self.max_loops:
                self._state = "complete"
                logger.info(f"Max loops ({self.max_loops}) reached. Strategy complete.")
                return Intent.hold(reason=f"Looping complete after {self._current_loop} loops")

            # First loop uses initial amount; subsequent loops use all available from swap
            if self._current_loop == 0:
                supply_amount = self.initial_supply_amount
            else:
                supply_amount = Decimal("0")  # Will be set from wallet balance below

            # Use wallet balance (caps first loop, determines subsequent loops)
            try:
                balance = market.balance(self.supply_token)
                available = balance.balance if hasattr(balance, "balance") else balance
                if self._current_loop > 0:
                    supply_amount = available  # Use all available from previous swap
                elif available < supply_amount:
                    supply_amount = available
                if supply_amount <= 0:
                    return Intent.hold(reason=f"No {self.supply_token} available to supply")
            except (ValueError, KeyError):
                if supply_amount <= 0:
                    return Intent.hold(reason=f"Cannot determine {self.supply_token} balance for loop {self._current_loop + 1}")

            self._pending_supply_amount = supply_amount
            self._previous_stable_state = self._state
            self._state = "supplying"

            logger.info(
                f"[Loop {self._current_loop + 1}/{self.max_loops}] "
                f"SUPPLY {format_token_amount_human(supply_amount, self.supply_token)}"
            )
            return Intent.supply(
                protocol="aave_v3",
                token=self.supply_token,
                amount=supply_amount,
                use_as_collateral=True,
                chain=self.chain,
            )

        # Step 2: Borrow against the supplied collateral
        if self._state == "supplied":
            # Calculate borrow in USD terms: collateral_value * LTV / borrow_price
            collateral_value_usd = self._pending_supply_amount * supply_price
            borrow_value_usd = collateral_value_usd * self.ltv_target
            borrow_amount = (borrow_value_usd / borrow_price).quantize(Decimal("0.01"))

            if borrow_amount <= 0:
                self._state = "complete"
                return Intent.hold(reason="Borrow amount too small, stopping loop")

            self._previous_stable_state = self._state
            self._state = "borrowing"

            logger.info(
                f"[Loop {self._current_loop + 1}/{self.max_loops}] "
                f"BORROW {format_token_amount_human(borrow_amount, self.borrow_token)} "
                f"(LTV {self.ltv_target * 100:.0f}%)"
            )
            return Intent.borrow(
                protocol="aave_v3",
                collateral_token=self.supply_token,
                collateral_amount=Decimal("0"),  # Already supplied
                borrow_token=self.borrow_token,
                borrow_amount=borrow_amount,
                interest_rate_mode=self.interest_rate_mode,
                chain=self.chain,
            )

        # Step 3: Swap borrowed token back to supply token
        if self._state == "borrowed":
            self._previous_stable_state = self._state
            self._state = "swapping"

            # Use the last borrow amount (tracked in on_intent_executed)
            swap_amount = self._last_borrow_amount

            logger.info(
                f"[Loop {self._current_loop + 1}/{self.max_loops}] "
                f"SWAP {format_token_amount_human(swap_amount, self.borrow_token)} -> {self.supply_token}"
            )
            return Intent.swap(
                from_token=self.borrow_token,
                to_token=self.supply_token,
                amount=swap_amount,
                max_slippage=max_slippage,
            )

        # Safety: revert stuck transitional states
        if self._state in ("supplying", "borrowing", "swapping"):
            revert_to = self._previous_stable_state
            logger.warning(f"Stuck in '{self._state}', reverting to '{revert_to}'")
            self._state = revert_to

        return Intent.hold(reason=f"Waiting (state={self._state}, loop={self._current_loop})")

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._state = "supplied"
                self._total_supplied += self._pending_supply_amount
                logger.info(f"Supply successful. Total supplied: {self._total_supplied} {self.supply_token}")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Loop {self._current_loop + 1}: Supplied {self._pending_supply_amount} {self.supply_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "supply", "loop": self._current_loop + 1, "amount": str(self._pending_supply_amount)},
                    )
                )

            elif intent_type == "BORROW":
                self._state = "borrowed"
                borrow_amount = Decimal(str(intent.borrow_amount)) if hasattr(intent, "borrow_amount") else Decimal("0")
                self._last_borrow_amount = borrow_amount
                self._total_borrowed += borrow_amount
                logger.info(f"Borrow successful. Total borrowed: {self._total_borrowed} {self.borrow_token}")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Loop {self._current_loop + 1}: Borrowed {borrow_amount} {self.borrow_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "borrow", "loop": self._current_loop + 1, "amount": str(borrow_amount)},
                    )
                )

            elif intent_type == "SWAP":
                # Swap complete — advance to next loop
                self._current_loop += 1
                # Reset pending amount — decide() will use wallet balance for next supply
                self._pending_supply_amount = Decimal("0")
                self._state = "idle"  # Ready for next loop iteration
                logger.info(
                    f"Swap successful. Loop {self._current_loop} complete. "
                    f"Next supply: {self._pending_supply_amount} {self.supply_token}"
                )
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Loop {self._current_loop}: Swapped {self.borrow_token} -> {self.supply_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "swap", "loop": self._current_loop},
                    )
                )

            elif intent_type == "REPAY":
                self._total_borrowed = Decimal("0")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Repaid all {self.borrow_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "repay"},
                    )
                )

            elif intent_type == "WITHDRAW":
                self._total_supplied = Decimal("0")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Withdrew all {self.supply_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "withdraw"},
                    )
                )
        else:
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type} failed in state '{self._state}', reverting to '{revert_to}'")
            self._state = revert_to

    # -- Status & Persistence --

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_aave_loop_mantle",
            "chain": self.chain,
            "state": self._state,
            "current_loop": self._current_loop,
            "max_loops": self.max_loops,
            "total_supplied": str(self._total_supplied),
            "total_borrowed": str(self._total_borrowed),
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "current_loop": self._current_loop,
            "total_supplied": str(self._total_supplied),
            "total_borrowed": str(self._total_borrowed),
            "pending_supply_amount": str(self._pending_supply_amount),
            "last_borrow_amount": str(self._last_borrow_amount),
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            self._state = state["state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "current_loop" in state:
            self._current_loop = int(state["current_loop"])
        if "total_supplied" in state:
            self._total_supplied = Decimal(str(state["total_supplied"]))
        if "total_borrowed" in state:
            self._total_borrowed = Decimal(str(state["total_borrowed"]))
        if "pending_supply_amount" in state:
            self._pending_supply_amount = Decimal(str(state["pending_supply_amount"]))
        if "last_borrow_amount" in state:
            self._last_borrow_amount = Decimal(str(state["last_borrow_amount"]))

    # -- Teardown --

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list["PositionInfo"] = []

        if self._total_supplied > 0:
            # Best-effort price lookup; fall back to zero if market data unavailable
            supply_value_usd = Decimal("0")
            try:
                market = self.create_market_snapshot()
                supply_price = market.price(self.supply_token)
                supply_value_usd = self._total_supplied * supply_price
            except Exception:
                pass
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.supply_token}-mantle",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=supply_value_usd,
                    details={"asset": self.supply_token, "amount": str(self._total_supplied)},
                )
            )

        if self._total_borrowed > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"aave-borrow-{self.borrow_token}-mantle",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._total_borrowed,  # borrow_token is stablecoin (default USDC)
                    details={"asset": self.borrow_token, "amount": str(self._total_borrowed)},
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Unwind the loop: get borrow_token, repay debt, then withdraw collateral.

        After a completed loop the borrowed asset has been swapped back into
        supply_token and re-supplied. The wallet holds supply_token (from the
        last swap) but not the borrow_token needed for repayment. Step 0 swaps
        the wallet's supply_token into borrow_token so the repay can succeed.
        """
        from almanak.framework.teardown import TeardownMode

        intents = []
        max_slippage = Decimal("0.03") if mode == TeardownMode.HARD else Decimal("0.01")

        # Step 0: Swap wallet's supply_token -> borrow_token to fund repayment.
        # After looping, the wallet holds supply_token (last swap output) but no
        # borrow_token. Swap it all so repay_full=True can source the full debt.
        if self._total_borrowed > 0 and self._current_loop > 0:
            intents.append(
                Intent.swap(
                    from_token=self.supply_token,
                    to_token=self.borrow_token,
                    amount="all",
                    max_slippage=max_slippage,
                )
            )

        # Step 1: Repay all borrowed borrow_token (e.g. USDC)
        if self._total_borrowed > 0:
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    amount=self._total_borrowed,
                    protocol="aave_v3",
                    repay_full=True,
                )
            )

        # Step 2: Withdraw all supplied supply_token (e.g. WETH)
        if self._total_supplied > 0:
            intents.append(
                Intent.withdraw(
                    token=self.supply_token,
                    amount="all",
                    protocol="aave_v3",
                    withdraw_all=True,
                )
            )

        return intents
