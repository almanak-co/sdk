"""
===============================================================================
DEMO: Aave V3 Paper Trade Lending — Supply/Borrow Cycles on Arbitrum
===============================================================================

This demo strategy is the vehicle for testing the paper trading engine
(``almanak strat backtest paper``) with lending intents on Arbitrum.

Unlike the PnL backtest lending demo (which uses simulated prices), this
strategy runs on Anvil forks with real Aave V3 transactions. It exercises
lending-specific paper trading gaps:

- Interest accrual tracking across ticks
- Health factor monitoring during borrow cycles
- Supply/borrow/repay lifecycle with real on-chain state

PURPOSE:
--------
1. Validate the paper trading pipeline with lending intents:
   - Anvil fork management on Arbitrum
   - Aave V3 supply/borrow/repay execution
   - PnL journal entries for lending positions
   - Equity curve with interest-bearing positions
2. First paper trading test on Arbitrum (prior tests only on Base).
3. First paper trading test with lending (prior tests only LP-based).

USAGE:
------
    # Paper trade for 5 ticks at 60-second intervals
    almanak strat backtest paper start \\
        -s demo_aave_paper_lending \\
        --chain arbitrum \\
        --max-ticks 5 \\
        --tick-interval 60 \\
        --foreground

    # Or run directly on Anvil (single iteration)
    almanak strat run -d strategies/demo/aave_paper_lending \\
        --network anvil --once

STRATEGY LOGIC:
---------------
Each tick:
  1. If idle -> supply WETH as collateral
  2. If supplied and price dropped -> borrow USDC (leverage on dip)
  3. If borrowed and price risen -> repay USDC (de-leverage on recovery)
  4. After max borrow cycles -> hold (preserve capital)
  5. Otherwise -> hold

The configurable thresholds and cycle limit create predictable open/close
patterns that generate PnL journal entries for the paper trader.
===============================================================================
"""

import logging
from datetime import UTC, datetime
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_aave_paper_lending",
    description="Paper trading demo — Aave V3 supply/borrow cycles on Arbitrum",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "paper-trading", "lending", "aave-v3", "arbitrum", "backtesting"],
    supported_chains=["arbitrum"],
    supported_protocols=["aave_v3"],
    intent_types=["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"],
)
class AavePaperLendingStrategy(IntentStrategy):
    """Aave V3 lending strategy for paper trading on Arbitrum.

    Executes supply/borrow/repay cycles on Anvil forks to validate the paper
    trading engine's handling of lending positions, interest accrual, and
    health factor tracking.

    Configuration (config.json):
        supply_token: Token to supply as collateral (default: "WETH")
        borrow_token: Token to borrow (default: "USDC")
        supply_amount: Amount of supply_token to deposit (default: "0.01")
        ltv_target: Target LTV ratio for borrows (default: 0.4 = 40%)
        price_drop_threshold: Price drop % to trigger borrow (default: 0.02 = 2%)
        price_rise_threshold: Price rise % to trigger repay (default: 0.03 = 3%)
        max_borrow_cycles: Max borrow/repay cycles before stopping (default: 3)

    Paper Trading Notes:
        - Lower thresholds than PnL backtest demo for faster cycling on Anvil
        - max_borrow_cycles prevents runaway borrows in long sessions
        - Use ``almanak strat backtest paper start`` for multi-tick sessions
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.supply_token = self.get_config("supply_token", "WETH")
        self.borrow_token = self.get_config("borrow_token", "USDC")
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "0.01")))
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.4")))
        self.price_drop_threshold = Decimal(str(self.get_config("price_drop_threshold", "0.02")))
        self.price_rise_threshold = Decimal(str(self.get_config("price_rise_threshold", "0.03")))
        self.max_borrow_cycles = int(self.get_config("max_borrow_cycles", 3))

        # State machine: idle -> supplied -> borrowed -> supplied (cycle)
        self._state = "idle"
        self._previous_stable_state = "idle"
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")
        self._reference_price: Decimal | None = None
        self._previous_reference_price: Decimal | None = None
        self._borrow_cycles = 0
        self._tick_count = 0

        logger.info(
            f"AavePaperLending initialized: "
            f"supply={self.supply_amount} {self.supply_token}, "
            f"borrow_token={self.borrow_token}, "
            f"LTV={self.ltv_target * 100}%, "
            f"drop={self.price_drop_threshold * 100}%, "
            f"rise={self.price_rise_threshold * 100}%, "
            f"max_cycles={self.max_borrow_cycles}"
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make lending decisions based on price movement.

        Decision flow:
        1. idle -> supply collateral (first tick)
        2. supplied + price dropped -> borrow (if cycles remaining)
        3. borrowed + price risen -> repay
        4. max cycles reached -> hold
        """
        self._tick_count += 1

        try:
            supply_price = market.price(self.supply_token)
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get {self.supply_token} price: {e}")
            return Intent.hold(reason=f"Price unavailable for {self.supply_token}: {e}")

        # Step 1: Supply collateral if idle
        if self._state == "idle":
            self._reference_price = supply_price
            self._previous_stable_state = self._state
            self._state = "supplying"
            logger.info(
                f"[tick {self._tick_count}] SUPPLY {self.supply_amount} "
                f"{self.supply_token} at ${supply_price:.2f}"
            )
            return Intent.supply(
                protocol="aave_v3",
                token=self.supply_token,
                amount=self.supply_amount,
                use_as_collateral=True,
                chain=self.chain,
            )

        # Step 2: Borrow on price drop (if cycles remaining)
        if self._state == "supplied" and self._reference_price is not None and self._reference_price > 0:
            if self._borrow_cycles >= self.max_borrow_cycles:
                return Intent.hold(
                    reason=f"Max borrow cycles ({self.max_borrow_cycles}) reached, "
                    f"holding supplied position"
                )

            price_change = (supply_price - self._reference_price) / self._reference_price

            if price_change <= -self.price_drop_threshold:
                try:
                    borrow_price = market.price(self.borrow_token)
                except (ValueError, KeyError) as e:
                    logger.warning(f"Could not get {self.borrow_token} price: {e}")
                    return Intent.hold(reason=f"Price unavailable for {self.borrow_token}: {e}")

                if borrow_price <= 0:
                    return Intent.hold(reason=f"Borrow token price is zero or negative: {borrow_price}")

                collateral_value = self._supplied_amount * supply_price
                borrow_value = collateral_value * self.ltv_target
                borrow_amount = (borrow_value / borrow_price).quantize(
                    Decimal("0.01"), rounding=ROUND_DOWN
                )

                if borrow_amount > 0:
                    self._previous_stable_state = self._state
                    self._previous_reference_price = self._reference_price
                    self._state = "borrowing"
                    self._reference_price = supply_price
                    logger.info(
                        f"[tick {self._tick_count}] BORROW {borrow_amount} "
                        f"{self.borrow_token} (price drop {price_change * 100:.1f}%, "
                        f"cycle {self._borrow_cycles + 1}/{self.max_borrow_cycles})"
                    )
                    return Intent.borrow(
                        protocol="aave_v3",
                        collateral_token=self.supply_token,
                        collateral_amount=Decimal("0"),
                        borrow_token=self.borrow_token,
                        borrow_amount=borrow_amount,
                        interest_rate_mode="variable",
                        chain=self.chain,
                    )

        # Step 3: Repay on price rise
        if self._state == "borrowed" and self._reference_price is not None and self._reference_price > 0:
            price_change = (supply_price - self._reference_price) / self._reference_price

            if price_change >= self.price_rise_threshold:
                self._previous_stable_state = self._state
                self._previous_reference_price = self._reference_price
                self._state = "repaying"
                self._reference_price = supply_price
                logger.info(
                    f"[tick {self._tick_count}] REPAY {self._borrowed_amount} "
                    f"{self.borrow_token} (price rise {price_change * 100:.1f}%)"
                )
                return Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="aave_v3",
                    repay_full=True,
                    chain=self.chain,
                )

        # Safety: revert stuck transitional states
        if self._state in ("supplying", "borrowing", "repaying"):
            revert_to = self._previous_stable_state
            logger.warning(f"Stuck in '{self._state}', reverting to '{revert_to}'")
            self._state = revert_to
            if self._previous_reference_price is not None:
                self._reference_price = self._previous_reference_price

        return Intent.hold(
            reason=f"Holding (state={self._state}, tick={self._tick_count}, "
            f"price=${supply_price:.2f}, cycles={self._borrow_cycles})"
        )

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track state transitions from execution results."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._state = "supplied"
                self._supplied_amount = self.supply_amount
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Supplied {self.supply_amount} {self.supply_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "supply", "amount": str(self.supply_amount)},
                    )
                )
            elif intent_type == "BORROW":
                self._state = "borrowed"
                self._borrow_cycles += 1
                self._borrowed_amount = Decimal(str(intent.borrow_amount))
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=(
                            f"Borrowed {self._borrowed_amount} {self.borrow_token} "
                            f"(cycle {self._borrow_cycles})"
                        ),
                        strategy_id=self.strategy_id,
                        details={
                            "action": "borrow",
                            "amount": str(self._borrowed_amount),
                            "cycle": self._borrow_cycles,
                        },
                    )
                )
            elif intent_type == "REPAY":
                self._state = "supplied"
                self._borrowed_amount = Decimal("0")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Repaid {self.borrow_token} (cycle {self._borrow_cycles})",
                        strategy_id=self.strategy_id,
                        details={"action": "repay", "cycle": self._borrow_cycles},
                    )
                )
            elif intent_type == "WITHDRAW":
                self._state = "idle"
                self._supplied_amount = Decimal("0")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Withdrew {self.supply_token} (teardown)",
                        strategy_id=self.strategy_id,
                        details={"action": "withdraw"},
                    )
                )
        else:
            revert_to = self._previous_stable_state
            logger.warning(f"{intent_type} failed, reverting to '{revert_to}'")
            self._state = revert_to
            if self._previous_reference_price is not None:
                self._reference_price = self._previous_reference_price

    # =========================================================================
    # STATUS & PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_aave_paper_lending",
            "chain": self.chain,
            "state": self._state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
            "borrow_cycles": self._borrow_cycles,
            "tick_count": self._tick_count,
            "reference_price": str(self._reference_price) if self._reference_price is not None else None,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
            "reference_price": str(self._reference_price) if self._reference_price is not None else None,
            "previous_reference_price": str(self._previous_reference_price) if self._previous_reference_price is not None else None,
            "borrow_cycles": self._borrow_cycles,
            "tick_count": self._tick_count,
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
        if state.get("reference_price") is not None:
            self._reference_price = Decimal(str(state["reference_price"]))
        if state.get("previous_reference_price") is not None:
            self._previous_reference_price = Decimal(str(state["previous_reference_price"]))
        if "borrow_cycles" in state:
            self._borrow_cycles = int(state["borrow_cycles"])
        if "tick_count" in state:
            self._tick_count = int(state["tick_count"])
        logger.info(
            f"Restored state: {self._state}, cycles={self._borrow_cycles}, "
            f"ticks={self._tick_count}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        """Return open positions based on in-memory state.

        Note: This paper-trading demo uses cached state rather than querying
        on-chain Aave positions. Production strategies MUST query on-chain
        state (e.g., via aToken/debtToken balanceOf) to avoid stale data.
        """
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []
        snapshot = None
        try:
            snapshot = self.create_market_snapshot()
        except Exception as e:  # noqa: BLE001
            logger.warning(f"Failed to create market snapshot for get_open_positions: {e}")

        if self._supplied_amount > 0:
            supply_price = Decimal("0")
            if snapshot:
                try:
                    supply_price = snapshot.price(self.supply_token)
                except (ValueError, KeyError) as e:
                    logger.warning(f"Could not get price for {self.supply_token} during teardown: {e}")
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.supply_token}-{self.chain}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._supplied_amount * supply_price,
                    details={"asset": self.supply_token, "amount": str(self._supplied_amount)},
                )
            )

        if self._borrowed_amount > 0:
            borrow_price = Decimal("1")  # Conservative fallback for stablecoins
            if snapshot:
                try:
                    borrow_price = snapshot.price(self.borrow_token)
                except (ValueError, KeyError) as e:
                    logger.warning(f"Could not get price for {self.borrow_token}, assuming 1.0: {e}")
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"aave-borrow-{self.borrow_token}-{self.chain}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=self._borrowed_amount * borrow_price,
                    details={"asset": self.borrow_token, "amount": str(self._borrowed_amount)},
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        intents = []

        if self._borrowed_amount > 0:
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="aave_v3",
                    repay_full=True,
                    chain=self.chain,
                )
            )

        if self._supplied_amount > 0:
            intents.append(
                Intent.withdraw(
                    token=self.supply_token,
                    amount=self._supplied_amount,
                    protocol="aave_v3",
                    withdraw_all=True,
                    chain=self.chain,
                )
            )

        return intents
