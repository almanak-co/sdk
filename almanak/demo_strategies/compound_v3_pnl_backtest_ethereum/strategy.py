"""Compound V3 PnL Backtest Strategy — Supply Rate Tracker on Ethereum.

Kitchen Loop iteration 101 strategy (VIB-1550). Validates the PnL backtester
pipeline with a lending protocol (supply/withdraw lifecycle). This is the
first PnL backtest of a lending strategy (prior backtests used RSI/LP swaps).

Strategy logic:
1. Check Compound V3 USDC supply rate via `market.lending_rate()`
2. Supply USDC when rate > entry_rate_threshold (or on first tick as fallback)
3. Hold while supplied, accruing interest
4. Withdraw USDC when rate < exit_rate_threshold (or after max_hold_ticks)

PnL backtester gap filled:
- Validates SupplyIntent + WithdrawIntent handling in `PnLBacktester`
- Tests `on_intent_executed` callback with lending receipts
- Exercises CompoundV3APYProvider for historical APY data
- First backtest of supply/withdraw lifecycle (all prior: swap-based)

Run PnL backtest:
    almanak strat backtest pnl -s demo_compound_v3_pnl_backtest_ethereum \\
        --start 2025-01-01 --end 2025-02-01 \\
        --chain ethereum --tokens USDC,ETH
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

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from almanak.framework.teardown import TeardownMode, TeardownPositionSummary


@almanak_strategy(
    name="demo_compound_v3_pnl_backtest_ethereum",
    description="Compound V3 supply rate tracker on Ethereum for PnL backtesting",
    version="1.0.0",
    author="Almanak",
    tags=["demo", "backtesting", "lending", "compound-v3", "pnl", "ethereum"],
    supported_chains=["ethereum"],
    supported_protocols=["compound_v3"],
    intent_types=["SUPPLY", "WITHDRAW", "HOLD"],
    default_chain="ethereum",
)
class CompoundV3PnLBacktestStrategy(IntentStrategy):
    """Compound V3 supply rate tracker strategy for PnL backtesting.

    Supplies USDC to Compound V3 when the supply rate exceeds a threshold.
    Withdraws when the rate falls below the exit threshold or the maximum
    hold duration is reached. Falls back to a time-based rule when live
    rate data is unavailable (e.g., in the PnL backtester context).

    Configuration Parameters (from config.json):
        supply_token: Token to supply (default: "USDC")
        supply_amount: Amount to supply (default: "10000")
        market: Compound V3 market identifier (default: "usdc")
        entry_rate_threshold: Annual supply rate to trigger entry (default: 0.03 = 3%)
        exit_rate_threshold: Annual supply rate to trigger exit (default: 0.01 = 1%)
        max_hold_ticks: Max ticks to hold before forced withdrawal (default: 30)
        force_entry_if_no_rate: Supply on first tick even when lending_rate() is unavailable
            (default: False). Set to True only in backtesting contexts where rate data is not
            wired up. NEVER set True in production — a data failure should hold, not transact.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.supply_token = self.get_config("supply_token", "USDC")
        self.supply_amount = Decimal(str(self.get_config("supply_amount", "10000")))
        self.market = self.get_config("market", "usdc")
        self.entry_rate_threshold = Decimal(str(self.get_config("entry_rate_threshold", "0.03")))
        self.exit_rate_threshold = Decimal(str(self.get_config("exit_rate_threshold", "0.01")))
        self.max_hold_ticks = int(self.get_config("max_hold_ticks", 30))
        self.force_entry_if_no_rate = bool(self.get_config("force_entry_if_no_rate", False))

        # State machine: idle -> supplying -> supplied -> withdrawing -> complete
        self._state = "idle"
        self._previous_stable_state = "idle"
        self._supplied_amount = Decimal("0")
        self._ticks_held = 0

        logger.info(
            "CompoundV3PnLBacktestStrategy initialized: "
            "supply=%s %s, market=%s, entry_rate=%.1f%%, exit_rate=%.1f%%",
            self.supply_amount,
            self.supply_token,
            self.market,
            float(self.entry_rate_threshold) * 100,
            float(self.exit_rate_threshold) * 100,
        )

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Make supply/withdraw decisions based on Compound V3 supply rate.

        When lending_rate() is unavailable and force_entry_if_no_rate=True (backtester
        context), falls back to entry on first tick and time-based exit after max_hold_ticks.
        When force_entry_if_no_rate=False (default / production), holds until rate is available.
        """
        # Get current supply rate (with fallback for backtester context)
        supply_rate, rate_available = self._get_supply_rate(market)

        if self._state == "idle":
            should_supply = supply_rate >= self.entry_rate_threshold if rate_available else self.force_entry_if_no_rate
            if should_supply:
                rate_str = f"{float(supply_rate) * 100:.2f}%" if rate_available else "N/A (fallback)"
                logger.info("SUPPLY %s %s to Compound V3 %s (rate=%s)", self.supply_amount, self.supply_token, self.market, rate_str)
                self._previous_stable_state = self._state
                self._state = "supplying"
                return Intent.supply(
                    protocol="compound_v3",
                    token=self.supply_token,
                    amount=self.supply_amount,
                    market_id=self.market,
                    chain=self.chain,
                )
            return Intent.hold(
                reason=f"Supply rate {float(supply_rate) * 100:.2f}% below entry threshold {float(self.entry_rate_threshold) * 100:.1f}%"
            )

        if self._state == "supplied":
            self._ticks_held += 1
            should_exit = (
                (rate_available and supply_rate < self.exit_rate_threshold)
                or self._ticks_held >= self.max_hold_ticks
            )

            if should_exit:
                reason = (
                    f"Rate {float(supply_rate) * 100:.2f}% < exit threshold {float(self.exit_rate_threshold) * 100:.1f}%"
                    if rate_available and supply_rate < self.exit_rate_threshold
                    else f"Max hold ticks ({self.max_hold_ticks}) reached"
                )
                logger.info("WITHDRAW %s %s from Compound V3 %s (%s)", self._supplied_amount, self.supply_token, self.market, reason)
                self._previous_stable_state = self._state
                self._state = "withdrawing"
                return Intent.withdraw(
                    protocol="compound_v3",
                    token=self.supply_token,
                    amount=self._supplied_amount,
                    withdraw_all=True,
                    market_id=self.market,
                    chain=self.chain,
                )

            rate_str = f"{float(supply_rate) * 100:.2f}%" if rate_available else "N/A"
            return Intent.hold(
                reason=f"Holding position (ticks={self._ticks_held}/{self.max_hold_ticks}, rate={rate_str})"
            )

        if self._state == "complete":
            return Intent.hold(reason="Lifecycle complete: supplied -> withdrew USDC from Compound V3")

        # Transitional states -- wait for execution confirmation
        if self._state in ("supplying", "withdrawing"):
            return Intent.hold(reason=f"Waiting for {self._state} to confirm")

        return Intent.hold(reason=f"Unexpected state: {self._state}")

    def _get_supply_rate(self, market: MarketSnapshot) -> tuple[Decimal, bool]:
        """Get Compound V3 USDC supply rate.

        Returns:
            (rate, rate_available): Rate as decimal fraction (e.g., 0.03 = 3%)
            and whether the rate was successfully retrieved.
        """
        try:
            rate = market.lending_rate("compound_v3", self.supply_token, "supply")
            if rate is not None:
                return Decimal(str(rate)), True
        except (AttributeError, NotImplementedError):
            # Expected in PnL backtester context where lending_rate() is not wired up.
            logger.debug("lending_rate() not available in this context (backtester)")
        except Exception as e:
            # Unexpected error (network, gateway, config). Hold until data recovers.
            logger.warning("Unexpected error fetching lending rate, holding: %s", e)
        return Decimal("0"), False

    def on_intent_executed(self, intent: Intent, success: bool, result: Any) -> None:
        """Track execution results and advance state machine."""
        intent_type = intent.intent_type.value

        if success:
            if intent_type == "SUPPLY":
                self._state = "supplied"
                self._supplied_amount = self.supply_amount
                self._ticks_held = 0
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Supplied {self.supply_amount} {self.supply_token} to Compound V3",
                        strategy_id=self.strategy_id,
                        details={"action": "supply", "amount": str(self.supply_amount), "protocol": "compound_v3"},
                    )
                )
                logger.info("SUPPLY confirmed: %s %s -> state=supplied", self.supply_amount, self.supply_token)

            elif intent_type == "WITHDRAW":
                self._state = "complete"
                self._supplied_amount = Decimal("0")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"Withdrew {self.supply_token} from Compound V3",
                        strategy_id=self.strategy_id,
                        details={"action": "withdraw", "protocol": "compound_v3"},
                    )
                )
                logger.info("WITHDRAW confirmed: %s -> state=complete", self.supply_token)

        else:
            revert_to = self._previous_stable_state
            logger.warning("%s failed, reverting to '%s'", intent_type, revert_to)
            self._state = revert_to

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "demo_compound_v3_pnl_backtest_ethereum",
            "chain": self.chain,
            "state": self._state,
            "supplied_amount": str(self._supplied_amount),
            "ticks_held": self._ticks_held,
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "previous_stable_state": self._previous_stable_state,
            "supplied_amount": str(self._supplied_amount),
            "ticks_held": self._ticks_held,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "state" in state:
            self._state = state["state"]
        if "previous_stable_state" in state:
            self._previous_stable_state = state["previous_stable_state"]
        if "supplied_amount" in state:
            self._supplied_amount = Decimal(str(state["supplied_amount"]))
        if "ticks_held" in state:
            self._ticks_held = int(state.get("ticks_held", 0))

    # -------------------------------------------------------------------------
    # Teardown interface
    # -------------------------------------------------------------------------

    def get_open_positions(self) -> "TeardownPositionSummary":
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions: list[PositionInfo] = []

        if self._supplied_amount > 0:
            positions.append(
                PositionInfo(
                    position_type=PositionType.SUPPLY,
                    position_id=f"compound-v3-supply-{self.supply_token}-{self.chain}",
                    chain=self.chain,
                    protocol="compound_v3",
                    value_usd=self._supplied_amount,  # USDC is 1:1
                    details={"asset": self.supply_token, "amount": str(self._supplied_amount), "market": self.market},
                )
            )

        return TeardownPositionSummary(
            strategy_id=self.STRATEGY_NAME,
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:
        """Withdraw supplied USDC from Compound V3."""
        if self._supplied_amount <= 0:
            return []

        return [
            Intent.withdraw(
                protocol="compound_v3",
                token=self.supply_token,
                amount=self._supplied_amount,
                withdraw_all=True,
                market_id=self.market,
                chain=self.chain,
            )
        ]
