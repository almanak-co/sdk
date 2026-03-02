"""
===============================================================================
Pendle RWA YT Yield Strategy -- Convex Derivative Play with PT Fallback
===============================================================================

This strategy explores YT (Yield Token) trading on Pendle for the sUSDe market
on Ethereum. YT gives leveraged exposure to yield changes -- the "convex
derivative play" from the Pendle RWA yield tokenization thesis.

HOW IT WORKS:
-------------
YT tokens represent the RIGHT to future yield until maturity. If you buy YT at
an implied yield of 5% and actual yield comes in higher, you profit. YT is the
speculative/convex bet vs PT which is the fixed/conservative bet.

STRATEGY FLOW:
--------------
1. Check sUSDe balance (the tokenMintSy for this market)
2. Attempt to buy YT-sUSDe-7MAY2026 (leveraged yield exposure)
3. YT swap will FAIL due to missing MARKET_BY_YT_TOKEN["ethereum"] registry
4. Fall back to buying PT-sUSDe-7MAY2026 (fixed yield, known working path)
5. Monitor position and track implied yield observations
6. Exit near maturity or on signal

PURPOSE:
--------
This is YAInnick Loop iteration 2. Primary goals:
- Test YT swap functionality (under-tested vs PT swaps from iteration 1)
- Exercise Ethereum chain (different from Arbitrum in iteration 1)
- Document the MARKET_BY_YT_TOKEN registry gap
- Verify PT swap on Ethereum via sUSDe input token

USAGE:
------
    almanak strat run -d strategies/incubating/pendle_rwa_yt_yield --network anvil --once

===============================================================================
"""

import logging
import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from almanak.framework.api.timeline import TimelineEvent, TimelineEventType, add_event
from almanak.framework.intents import Intent
from almanak.framework.strategies import (
    IntentStrategy,
    MarketSnapshot,
    almanak_strategy,
)

logger = logging.getLogger(__name__)


@almanak_strategy(
    name="pendle_rwa_yt_yield",
    description="RWA yield exposure via Pendle YT-sUSDe with PT fallback",
    version="1.0.0",
    author="Almanak",
    tags=["pendle", "yield", "yt", "pt", "sUSDe", "rwa", "convex"],
    supported_chains=["ethereum"],
    supported_protocols=["pendle"],
    intent_types=["SWAP", "HOLD"],
)
class PendleRWAYTYieldStrategy(IntentStrategy):
    """YT-first yield strategy with PT fallback on Pendle sUSDe market.

    State machine phases:
        idle -> entering_yt -> entering_pt -> monitoring -> exiting -> settled

    The YT attempt is expected to fail (missing MARKET_BY_YT_TOKEN["ethereum"]),
    documenting the registry gap and gracefully falling back to PT.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        def get_config(key: str, default: Any) -> Any:
            if isinstance(self.config, dict):
                return self.config.get(key, default)
            return getattr(self.config, key, default)

        # Token configuration
        self.yt_token = get_config("yt_token", "YT-sUSDe-7MAY2026")
        self.pt_token = get_config("pt_token", "PT-sUSDe-7MAY2026")
        self.pt_token_address = get_config("pt_token_address", "")
        self.yt_token_address = get_config("yt_token_address", "")
        self.pendle_market = get_config("pendle_market", "")
        self.base_token = get_config("base_token", "sUSDe")
        self.base_token_address = get_config("base_token_address", "")
        self.maturity_date = get_config("maturity_date", "2026-05-07")

        # Trading parameters
        self.max_slippage = Decimal(str(get_config("max_slippage", "0.01")))
        self.trade_size_pct = Decimal(str(get_config("trade_size_pct", "0.5")))
        self.exit_days_before_maturity = int(get_config("exit_days_before_maturity", 7))

        # State machine
        self._phase = "idle"

        # Position tracking
        self._entry_token: str | None = None  # "yt" or "pt" -- which path succeeded
        self._entry_amount = Decimal("0")
        self._initial_base_balance = Decimal("0")
        self._entry_timestamp: str | None = None

        # YT failure tracking
        self._yt_error_message: str | None = None
        self._yt_attempted = False

        logger.info(
            f"PendleRWAYTYield initialized: yt={self.yt_token}, "
            f"pt={self.pt_token}, base={self.base_token}, "
            f"market={self.pendle_market[:16]}..."
        )

    # =========================================================================
    # MAIN DECISION LOGIC
    # =========================================================================

    def decide(self, market: MarketSnapshot) -> Intent | None:
        """Advance the state machine by one step."""
        try:
            handler = {
                "idle": self._handle_idle,
                "entering_yt": self._handle_entering_yt,
                "entering_pt": self._handle_entering_pt,
                "monitoring": self._handle_monitoring,
                "exiting": self._handle_exiting,
                "settled": self._handle_settled,
            }.get(self._phase)

            if handler:
                return handler(market)

            return Intent.hold(reason=f"Unknown phase: {self._phase}")

        except Exception as e:
            logger.exception(f"Error in decide(): {e}")
            return Intent.hold(reason=f"Error: {str(e)}")

    # =========================================================================
    # STATE HANDLERS
    # =========================================================================

    def _handle_idle(self, market: MarketSnapshot) -> Intent:
        """IDLE: Check sUSDe balance and attempt YT swap."""
        base_balance = self._get_balance(market, self.base_token)

        if base_balance < Decimal("100"):
            return Intent.hold(
                reason=f"Insufficient {self.base_token}: {base_balance} < 100"
            )

        self._initial_base_balance = base_balance
        trade_amount = (base_balance * self.trade_size_pct).quantize(Decimal("0.01"))

        logger.info(
            f"Have {base_balance} {self.base_token}, "
            f"attempting YT swap with {trade_amount} ({self.trade_size_pct * 100}%)"
        )

        # Attempt YT swap first (expected to fail due to missing registry)
        self._transition("idle", "entering_yt")
        self._entry_amount = trade_amount

        return Intent.swap(
            from_token=self.base_token,
            to_token=self.yt_token,
            amount=trade_amount,
            max_slippage=self.max_slippage,
            protocol="pendle",
        )

    def _handle_entering_yt(self, market: MarketSnapshot) -> Intent:
        """ENTERING_YT: YT swap was attempted. If we're still here, it means
        on_intent_executed hasn't fired yet or YT succeeded (unlikely).
        Wait for callback."""
        if self._yt_attempted and self._entry_token == "yt":
            # YT succeeded (unexpected but great!)
            logger.info("YT swap succeeded -- staying in YT position")
            self._transition("entering_yt", "monitoring")
            return self._handle_monitoring(market)

        if self._yt_attempted and self._yt_error_message:
            # YT failed, fall back to PT
            logger.info(
                f"YT swap failed: {self._yt_error_message}. "
                f"Falling back to PT swap."
            )
            self._transition("entering_yt", "entering_pt")
            return self._build_pt_swap()

        # Still waiting for YT result
        return Intent.hold(reason="Waiting for YT swap result")

    def _handle_entering_pt(self, market: MarketSnapshot) -> Intent:
        """ENTERING_PT: PT swap was attempted as fallback."""
        if self._entry_token == "pt":
            # PT swap succeeded
            logger.info("PT swap succeeded -- position entered via fallback path")
            self._transition("entering_pt", "monitoring")
            return self._handle_monitoring(market)

        # Still waiting or need to issue the PT swap
        return Intent.hold(reason="Waiting for PT swap result")

    def _handle_monitoring(self, market: MarketSnapshot) -> Intent:
        """MONITORING: Log position value, track yield, check maturity."""
        days_to_maturity = self._estimate_days_to_maturity()

        # Check if we should exit due to maturity proximity
        if days_to_maturity is not None and days_to_maturity <= self.exit_days_before_maturity:
            logger.warning(
                f"Near maturity ({days_to_maturity} days), initiating exit"
            )
            self._transition("monitoring", "exiting")
            return self._build_exit_swap()

        # Report position status
        position_token = self.yt_token if self._entry_token == "yt" else self.pt_token
        current_balance = self._get_balance(market, self.base_token)

        return Intent.hold(
            reason=f"Monitoring {position_token} position -- "
            f"Entry: {self._entry_amount} {self.base_token}, "
            f"Remaining {self.base_token}: {current_balance}, "
            f"Days to maturity: {days_to_maturity or 'unknown'}, "
            f"Path: {'YT (direct)' if self._entry_token == 'yt' else 'PT (fallback)'}"
        )

    def _handle_exiting(self, market: MarketSnapshot) -> Intent:
        """EXITING: Sell position token back to base token."""
        if self._entry_token is None:
            self._transition("exiting", "settled")
            return self._handle_settled(market)

        return Intent.hold(reason="Waiting for exit swap confirmation")

    def _handle_settled(self, market: MarketSnapshot) -> Intent:
        """SETTLED: Position closed, report final state."""
        final_balance = self._get_balance(market, self.base_token)
        pnl = final_balance - self._initial_base_balance

        return Intent.hold(
            reason=f"Settled -- Initial: {self._initial_base_balance} {self.base_token}, "
            f"Final: {final_balance} {self.base_token}, "
            f"P&L: {pnl:+.2f} {self.base_token}, "
            f"Path used: {self._entry_token or 'none'}"
        )

    # =========================================================================
    # INTENT BUILDERS
    # =========================================================================

    def _build_pt_swap(self) -> Intent:
        """Build PT swap intent as fallback from YT failure."""
        logger.info(
            f"PT FALLBACK: {self._entry_amount} {self.base_token} -> {self.pt_token}"
        )

        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.POSITION_MODIFIED,
                description=f"PT fallback swap: {self._entry_amount} {self.base_token} -> {self.pt_token}",
                strategy_id=self.strategy_id,
                details={
                    "action": "pt_fallback_swap",
                    "amount": str(self._entry_amount),
                    "yt_error": self._yt_error_message,
                },
            )
        )

        return Intent.swap(
            from_token=self.base_token,
            to_token=self.pt_token,
            amount=self._entry_amount,
            max_slippage=self.max_slippage,
            protocol="pendle",
        )

    def _build_exit_swap(self) -> Intent:
        """Build exit swap to sell position token back to base."""
        position_token = self.yt_token if self._entry_token == "yt" else self.pt_token
        logger.info(f"EXIT: Selling {position_token} -> {self.base_token}")

        return Intent.swap(
            from_token=position_token,
            to_token=self.base_token,
            amount="all",
            max_slippage=self.max_slippage,
            protocol="pendle",
        )

    # =========================================================================
    # HELPERS
    # =========================================================================

    def _get_balance(self, market: MarketSnapshot, token: str) -> Decimal:
        """Get token balance, returning 0 on error."""
        try:
            bal = market.balance(token)
            return bal.balance if hasattr(bal, "balance") else bal
        except (ValueError, KeyError, AttributeError):
            return Decimal("0")

    def _estimate_days_to_maturity(self) -> int | None:
        """Estimate days until maturity from maturity_date config or token name."""
        # Try config date first
        try:
            maturity = datetime.strptime(self.maturity_date, "%Y-%m-%d").replace(tzinfo=UTC)
            delta = (maturity - datetime.now(UTC)).days
            return max(delta, 0)
        except (ValueError, TypeError):
            pass

        # Fall back to parsing token name
        match = re.search(r"(\d{1,2})([A-Z]{3})(\d{4})", self.pt_token)
        if not match:
            return None

        day, month_str, year = match.groups()
        months = {
            "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
            "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
        }
        month = months.get(month_str)
        if month is None:
            return None

        try:
            maturity = datetime(int(year), month, int(day), tzinfo=UTC)
            delta = (maturity - datetime.now(UTC)).days
            return max(delta, 0)
        except (ValueError, OverflowError):
            return None

    def _transition(self, old: str, new: str) -> None:
        """Transition state machine phase."""
        logger.info(f"Phase: {old.upper()} -> {new.upper()}")
        self._phase = new
        add_event(
            TimelineEvent(
                timestamp=datetime.now(UTC),
                event_type=TimelineEventType.STATE_CHANGE,
                description=f"Phase: {old.upper()} -> {new.upper()}",
                strategy_id=self.strategy_id,
                details={"old_phase": old, "new_phase": new},
            )
        )

    # =========================================================================
    # LIFECYCLE HOOKS
    # =========================================================================

    def on_intent_executed(self, intent: Any, success: bool, result: Any) -> None:
        """Update state tracking after intent execution."""
        intent_type = getattr(intent, "intent_type", None)
        if intent_type:
            intent_type = intent_type.value

        if self._phase == "entering_yt":
            self._yt_attempted = True
            if success:
                # YT swap succeeded (unexpected -- registry gap may have been fixed)
                self._entry_token = "yt"
                self._entry_timestamp = datetime.now(UTC).isoformat()
                logger.info(
                    f"YT swap SUCCEEDED: {self._entry_amount} {self.base_token} -> {self.yt_token}"
                )
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"YT swap succeeded: {self._entry_amount} {self.base_token}",
                        strategy_id=self.strategy_id,
                        details={"path": "yt", "amount": str(self._entry_amount)},
                    )
                )
            else:
                # YT swap failed (expected -- capture error for report)
                error_msg = str(result) if result else "Unknown error"
                self._yt_error_message = error_msg
                logger.warning(f"YT swap FAILED (expected): {error_msg}")
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.TRANSACTION_FAILED,
                        description=f"YT swap failed: {error_msg}",
                        strategy_id=self.strategy_id,
                        details={"path": "yt", "error": error_msg},
                    )
                )

        elif self._phase == "entering_pt":
            if success:
                self._entry_token = "pt"
                self._entry_timestamp = datetime.now(UTC).isoformat()
                logger.info(
                    f"PT fallback swap SUCCEEDED: {self._entry_amount} {self.base_token} -> {self.pt_token}"
                )
                add_event(
                    TimelineEvent(
                        timestamp=datetime.now(UTC),
                        event_type=TimelineEventType.POSITION_MODIFIED,
                        description=f"PT fallback swap succeeded: {self._entry_amount} {self.base_token}",
                        strategy_id=self.strategy_id,
                        details={"path": "pt_fallback", "amount": str(self._entry_amount)},
                    )
                )
            else:
                error_msg = str(result) if result else "Unknown error"
                logger.error(f"PT fallback swap ALSO FAILED: {error_msg}")

        elif self._phase == "exiting":
            if success:
                logger.info("Exit swap succeeded -- position closed")
                self._entry_token = None
            else:
                logger.error(f"Exit swap failed: {result}")

    # =========================================================================
    # STATUS & PERSISTENCE
    # =========================================================================

    def get_status(self) -> dict[str, Any]:
        return {
            "strategy": "pendle_rwa_yt_yield",
            "chain": self.chain,
            "wallet": self.wallet_address[:10] + "..." if self.wallet_address else "N/A",
            "config": {
                "yt_token": self.yt_token,
                "pt_token": self.pt_token,
                "base_token": self.base_token,
                "pendle_market": self.pendle_market[:20] + "...",
                "max_slippage": str(self.max_slippage),
                "trade_size_pct": str(self.trade_size_pct),
            },
            "state": {
                "phase": self._phase,
                "entry_token": self._entry_token,
                "entry_amount": str(self._entry_amount),
                "initial_base_balance": str(self._initial_base_balance),
                "yt_attempted": self._yt_attempted,
                "yt_error": self._yt_error_message,
                "days_to_maturity": self._estimate_days_to_maturity(),
            },
        }

    def get_persistent_state(self) -> dict[str, Any]:
        return {
            "phase": self._phase,
            "entry_token": self._entry_token,
            "entry_amount": str(self._entry_amount),
            "initial_base_balance": str(self._initial_base_balance),
            "entry_timestamp": self._entry_timestamp,
            "yt_attempted": self._yt_attempted,
            "yt_error_message": self._yt_error_message,
        }

    def load_persistent_state(self, state: dict[str, Any]) -> None:
        if "phase" in state:
            self._phase = state["phase"]
        if "entry_token" in state:
            self._entry_token = state["entry_token"]
        if "entry_amount" in state:
            self._entry_amount = Decimal(str(state["entry_amount"]))
        if "initial_base_balance" in state:
            self._initial_base_balance = Decimal(str(state["initial_base_balance"]))
        if "entry_timestamp" in state:
            self._entry_timestamp = state["entry_timestamp"]
        if "yt_attempted" in state:
            self._yt_attempted = state["yt_attempted"]
        if "yt_error_message" in state:
            self._yt_error_message = state["yt_error_message"]
        logger.info(
            f"Restored state: phase={self._phase}, "
            f"entry_token={self._entry_token}, "
            f"entry_amount={self._entry_amount} {self.base_token}"
        )

    # =========================================================================
    # TEARDOWN
    # =========================================================================

    def supports_teardown(self) -> bool:
        return True

    def get_open_positions(self) -> "TeardownPositionSummary":  # noqa: F821
        from almanak.framework.teardown import PositionInfo, PositionType, TeardownPositionSummary

        positions = []
        if self._entry_token:
            position_token = self.yt_token if self._entry_token == "yt" else self.pt_token
            positions.append(
                PositionInfo(
                    position_type=PositionType.TOKEN,
                    position_id=f"pendle_{self._entry_token}_0",
                    chain=self.chain,
                    protocol="pendle",
                    value_usd=self._entry_amount,  # approximate
                    details={
                        "token": position_token,
                        "base_token": self.base_token,
                        "entry_amount": str(self._entry_amount),
                        "path": self._entry_token,
                    },
                )
            )
        return TeardownPositionSummary(
            strategy_id=getattr(self, "strategy_id", "pendle_rwa_yt_yield"),
            timestamp=datetime.now(UTC),
            positions=positions,
        )

    def generate_teardown_intents(self, mode: "TeardownMode", market=None) -> list[Intent]:  # noqa: F821
        from almanak.framework.teardown import TeardownMode

        if not self._entry_token:
            return []

        max_slippage = Decimal("0.05") if mode == TeardownMode.HARD else self.max_slippage
        position_token = self.yt_token if self._entry_token == "yt" else self.pt_token

        logger.info(
            f"Generating teardown: swap {position_token} -> {self.base_token} "
            f"(mode={mode.value}, slippage={max_slippage})"
        )

        return [
            Intent.swap(
                from_token=position_token,
                to_token=self.base_token,
                amount="all",
                max_slippage=max_slippage,
                protocol="pendle",
            )
        ]

    def on_teardown_started(self, mode: "TeardownMode") -> None:  # noqa: F821
        from almanak.framework.teardown import TeardownMode

        mode_name = "graceful" if mode == TeardownMode.SOFT else "emergency"
        position_token = self.yt_token if self._entry_token == "yt" else self.pt_token
        logger.info(f"Teardown ({mode_name}): selling {position_token} -> {self.base_token}")

    def on_teardown_completed(self, success: bool, recovered_usd: Decimal) -> None:
        if success:
            logger.info(f"Teardown completed. Recovered ${recovered_usd:,.2f}")
            self._phase = "settled"
            self._entry_token = None
        else:
            logger.error("Teardown failed -- manual intervention may be required")


if __name__ == "__main__":
    meta = PendleRWAYTYieldStrategy.STRATEGY_METADATA
    print("=" * 70)
    print("PendleRWAYTYieldStrategy -- Convex Yield Bet via YT with PT Fallback")
    print("=" * 70)
    print(f"\nStrategy: {PendleRWAYTYieldStrategy.STRATEGY_NAME}")
    print(f"Chains: {meta.get('supported_chains', [])}")
    print(f"Protocols: {meta.get('supported_protocols', [])}")
    print(f"Intents: {meta.get('intent_types', [])}")
