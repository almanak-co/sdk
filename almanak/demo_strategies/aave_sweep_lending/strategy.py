"""
===============================================================================
DEMO: Aave V3 Sweep Lending — Rate-Threshold-Based Lending on Arbitrum
===============================================================================

This demo strategy is the vehicle for testing the parameter sweep engine
(``almanak strat backtest sweep``) with lending-specific parameters.

All existing parameter sweeps are for LP/swap strategies (Aerodrome, TraderJoe,
Uniswap RSI). This is the first sweep for a lending protocol, testing whether
the sweep engine handles supply/borrow decision boundaries correctly.

PURPOSE:
--------
1. Validate the parameter sweep pipeline with lending intents:
   - Grid search over supply_rate_threshold (when to supply collateral)
   - Grid search over borrow_rate_threshold (when to borrow)
   - LTV target variation (capital efficiency optimization)
2. First parameter sweep for a lending protocol on Arbitrum.
3. Tests sweep engine with rate-based thresholds vs price-based thresholds.

USAGE:
------
    # Sweep supply/borrow rate thresholds (4x4 grid = 16 combinations)
    almanak strat backtest sweep -s demo_aave_sweep_lending \
        --start 2024-01-01 --end 2024-06-01 \
        --param "supply_rate_threshold:2.0,4.0,6.0,8.0" \
        --param "borrow_rate_threshold:3.0,5.0,7.0,10.0"

    # Sweep LTV target
    almanak strat backtest sweep -s demo_aave_sweep_lending \
        --start 2024-01-01 --end 2024-06-01 \
        --param "ltv_target:0.2,0.3,0.4,0.5"

    # Full grid: rates + LTV (32 combinations)
    almanak strat backtest sweep -s demo_aave_sweep_lending \
        --start 2024-01-01 --end 2024-06-01 \
        --param "supply_rate_threshold:3.0,5.0,7.0" \
        --param "borrow_rate_threshold:4.0,6.0,8.0" \
        --param "ltv_target:0.3,0.4" \
        --parallel 4

SWEEPABLE PARAMETERS:
---------------------
    supply_rate_threshold: Price volatility % to gate re-supply after a full cycle
    borrow_rate_threshold: Price volatility % ceiling for borrowing (stable market proxy)
    ltv_target: Target loan-to-value ratio (0.0-1.0)
    supply_amount: Amount of supply_token to deposit
    max_borrow_cycles: Max supply/borrow cycles before stopping

STRATEGY LOGIC:
---------------
Each tick:
  1. If idle and supply rate > supply_rate_threshold -> supply WETH
  2. If supplied and borrow rate < borrow_rate_threshold -> borrow USDC
  3. If borrowed and borrow rate > borrow_rate_threshold * 1.5 -> repay
  4. Track cycle count against max_borrow_cycles
  5. Otherwise -> hold
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
    name="demo_aave_sweep_lending",
    description="Parameter sweep demo — Aave V3 lending with sweepable rate thresholds on Arbitrum",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "sweep", "lending", "aave-v3", "arbitrum", "backtesting"],
    supported_chains=["arbitrum"],
    default_chain="arbitrum",
    supported_protocols=["aave_v3"],
    intent_types=["SUPPLY", "BORROW", "REPAY", "WITHDRAW", "HOLD"],
)
class AaveSweepLendingStrategy(IntentStrategy):
    """Aave V3 lending strategy with sweep-optimizable rate thresholds.

    All configuration parameters can be overridden by the sweep engine via
    ``--param "name:val1,val2,val3"`` on the CLI.

    Configuration (config.json):
        supply_token: Token to supply as collateral (default: "WETH")
        borrow_token: Token to borrow (default: "USDC")
        supply_amount: Amount of supply_token to deposit (sweepable, default: "0.5")
        supply_rate_threshold: Supply rate % to trigger supply (sweepable, default: 4.0)
        borrow_rate_threshold: Borrow rate % to trigger borrow (sweepable, default: 6.0)
        ltv_target: Target LTV for borrows (sweepable, default: 0.4)
        max_borrow_cycles: Max supply/borrow cycles (default: 5)
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Token configuration
        self.supply_token = str(self.get_config("supply_token", "WETH"))
        self.borrow_token = str(self.get_config("borrow_token", "USDC"))

        # Sweepable parameters
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "0.5")))
        self.supply_rate_threshold = Decimal(str(self.get_config("supply_rate_threshold", "4.0")))
        self.borrow_rate_threshold = Decimal(str(self.get_config("borrow_rate_threshold", "6.0")))
        self.ltv_target = Decimal(str(self.get_config("ltv_target", "0.4")))
        self.max_borrow_cycles = int(self.get_config("max_borrow_cycles", 5))

        # Validate
        if self.supply_rate_threshold < 0:
            raise ValueError(f"supply_rate_threshold must be >= 0, got {self.supply_rate_threshold}")
        if self.borrow_rate_threshold < 0:
            raise ValueError(f"borrow_rate_threshold must be >= 0, got {self.borrow_rate_threshold}")
        if not Decimal("0") < self.ltv_target < Decimal("1"):
            raise ValueError(f"ltv_target must be between 0 and 1 exclusive, got {self.ltv_target}")

        # Internal state
        self._VALID_STATES = frozenset(
            {"idle", "supplying", "supplied", "borrowing", "borrowed", "repaying"}
        )
        self._state = "idle"  # idle -> supplied -> borrowed -> (repaid -> supplied cycle)
        self._previous_stable_state = "idle"
        self._supplied_amount = Decimal("0")
        self._borrowed_amount = Decimal("0")
        self._borrow_cycles = 0
        self._tick_count = 0
        self._reference_price: Decimal | None = None
        self._previous_reference_price: Decimal | None = None

        logger.info(
            f"AaveSweepLending initialized: "
            f"supply={self.supply_amount} {self.supply_token}, "
            f"borrow_token={self.borrow_token}, "
            f"supply_rate_threshold={self.supply_rate_threshold}%, "
            f"borrow_rate_threshold={self.borrow_rate_threshold}%, "
            f"LTV target={self.ltv_target * 100}%, "
            f"max_cycles={self.max_borrow_cycles}"
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make lending decisions based on supply/borrow rate thresholds.

        Since the PnL backtester doesn't provide real-time Aave rates, we use
        price volatility as a proxy: |price_change| percentage is compared
        against the rate thresholds. This lets the sweep engine produce
        meaningfully different execution paths for different threshold values.

        Decision flow:
        1. idle -> supply if price volatility > supply_rate_threshold (or first tick)
        2. supplied -> borrow if price change % < borrow_rate_threshold and cycles remain
        3. borrowed -> repay if price change % > borrow_rate_threshold * 1.5
        4. transient states -> hold (waiting for confirmation)
        """
        self._tick_count += 1

        # Get supply token price
        try:
            supply_price = market.price(self.supply_token)
        except (ValueError, KeyError) as e:
            logger.warning(f"Could not get {self.supply_token} price: {e}")
            return Intent.hold(reason=f"Price data unavailable for {self.supply_token}: {e}")

        # Wait for transient states to resolve
        if self._state in ("supplying", "borrowing", "repaying"):
            return Intent.hold(reason=f"Waiting for {self._state} to confirm")

        # Step 1: Supply collateral if idle
        if self._state == "idle":
            if self._reference_price is not None:
                # After a full cycle, gate re-supply on price volatility
                price_change_pct = abs(
                    (supply_price - self._reference_price) / self._reference_price * 100
                )
                if price_change_pct < self.supply_rate_threshold:
                    return Intent.hold(
                        reason=f"Price volatility {price_change_pct:.1f}% < supply threshold {self.supply_rate_threshold}%"
                    )
            # First tick or threshold met: supply
            self._reference_price = supply_price
            self._previous_stable_state = self._state
            self._state = "supplying"
            logger.info(
                f"[tick {self._tick_count}] SUPPLY {self.supply_amount} {self.supply_token} "
                f"at ${supply_price:.2f}"
            )
            return Intent.supply(
                protocol="aave_v3",
                token=self.supply_token,
                amount=self.supply_amount,
                use_as_collateral=True,
                chain=self.chain,
            )

        # Step 2: If supplied, borrow when price movement is within threshold
        if self._state == "supplied":
            if self._borrow_cycles >= self.max_borrow_cycles:
                return Intent.hold(
                    reason=f"Max borrow cycles ({self.max_borrow_cycles}) reached"
                )

            # Use price change as a proxy for borrow rate favorability:
            # small price change -> stable market -> favorable borrow rates
            if self._reference_price is not None and self._reference_price > 0:
                price_change_pct = abs(
                    (supply_price - self._reference_price) / self._reference_price * 100
                )
                if price_change_pct > self.borrow_rate_threshold:
                    return Intent.hold(
                        reason=f"Price volatility {price_change_pct:.1f}% > borrow threshold {self.borrow_rate_threshold}%"
                    )

            try:
                borrow_price = market.price(self.borrow_token)
            except (ValueError, KeyError) as e:
                return Intent.hold(reason=f"Price data unavailable for {self.borrow_token}: {e}")

            if borrow_price <= 0:
                return Intent.hold(reason=f"Invalid {self.borrow_token} price: {borrow_price}")

            # Calculate borrow amount based on LTV target
            collateral_value = self._supplied_amount * supply_price
            borrow_value = collateral_value * self.ltv_target
            borrow_amount = (borrow_value / borrow_price).quantize(Decimal("0.01"), rounding=ROUND_DOWN)

            if borrow_amount > 0:
                self._previous_stable_state = self._state
                self._previous_reference_price = self._reference_price
                self._state = "borrowing"
                self._reference_price = supply_price
                logger.info(
                    f"[tick {self._tick_count}] BORROW {borrow_amount} {self.borrow_token} "
                    f"(LTV target={self.ltv_target * 100}%)"
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

            return Intent.hold(reason="Borrow amount too small")

        # Step 3: If borrowed, repay when price moves significantly
        if self._state == "borrowed":
            if self._reference_price is not None and self._reference_price > 0:
                price_change_pct = abs(
                    (supply_price - self._reference_price) / self._reference_price * 100
                )
                repay_threshold = self.borrow_rate_threshold * Decimal("1.5")
                if price_change_pct < repay_threshold:
                    return Intent.hold(
                        reason=f"Price volatility {price_change_pct:.1f}% < repay threshold {repay_threshold:.1f}%"
                    )

            self._previous_stable_state = self._state
            self._state = "repaying"
            logger.info(
                f"[tick {self._tick_count}] REPAY {self._borrowed_amount} {self.borrow_token} "
                f"(cycle {self._borrow_cycles}/{self.max_borrow_cycles})"
            )
            return Intent.repay(
                token=self.borrow_token,
                amount=self._borrowed_amount,
                protocol="aave_v3",
                repay_full=True,
            )

        return Intent.hold(reason=f"Holding (state={self._state}, tick={self._tick_count})")

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track lending state from execution results."""
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
                if hasattr(intent, "borrow_amount"):
                    self._borrowed_amount = Decimal(str(intent.borrow_amount))
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Borrowed {self._borrowed_amount} {self.borrow_token}",
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
                        description=f"Withdrew {self.supply_token}",
                        strategy_id=self.strategy_id,
                        details={"action": "withdraw", "token": self.supply_token},
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
            "strategy": "demo_aave_sweep_lending",
            "chain": self.chain,
            "state": self._state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
            "borrow_cycles": self._borrow_cycles,
            "tick_count": self._tick_count,
            "supply_rate_threshold": str(self.supply_rate_threshold),
            "borrow_rate_threshold": str(self.borrow_rate_threshold),
            "ltv_target": str(self.ltv_target),
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "borrowed_amount": str(self._borrowed_amount),
            "borrow_cycles": self._borrow_cycles,
            "tick_count": self._tick_count,
            "reference_price": str(self._reference_price) if self._reference_price is not None else None,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            restored = str(state["state"])
            self._state = restored if restored in self._VALID_STATES else "idle"
        if "previous_stable_state" in state:
            restored_prev = str(state["previous_stable_state"])
            self._previous_stable_state = restored_prev if restored_prev in self._VALID_STATES else "idle"
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
        if "borrowed_amount" in state:
            self._borrowed_amount = Decimal(str(state["borrowed_amount"]))
        if "borrow_cycles" in state:
            self._borrow_cycles = int(state["borrow_cycles"])
        if "tick_count" in state:
            self._tick_count = int(state["tick_count"])
        if state.get("reference_price") is not None:
            self._reference_price = Decimal(str(state["reference_price"]))
        logger.info(
            f"Restored state: state={self._state}, "
            f"supplied={self._supplied_amount}, borrowed={self._borrowed_amount}, "
            f"cycles={self._borrow_cycles}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"aave-supply-{self.supply_token}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=Decimal("0"),
                    details={
                        "action": "supply",
                        "token": self.supply_token,
                        "amount": str(self._supplied_amount),
                    },
                )
            )
        if self._borrowed_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.BORROW,
                    position_id=f"aave-borrow-{self.borrow_token}",
                    chain=self.chain,
                    protocol="aave_v3",
                    value_usd=Decimal("0"),
                    details={
                        "action": "borrow",
                        "token": self.borrow_token,
                        "amount": str(self._borrowed_amount),
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=self.strategy_id,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        from almanak.framework.teardown import TeardownMode

        intents = []
        # Repay borrow first, then withdraw supply
        if self._borrowed_amount > 0:
            intents.append(
                Intent.repay(
                    token=self.borrow_token,
                    amount=self._borrowed_amount,
                    protocol="aave_v3",
                    repay_full=True,
                )
            )
        if self._supplied_amount > 0:
            intents.append(
                Intent.withdraw(
                    protocol="aave_v3",
                    token=self.supply_token,
                    amount=self._supplied_amount,
                    withdraw_all=True,
                    chain=self.chain,
                )
            )
        return intents
